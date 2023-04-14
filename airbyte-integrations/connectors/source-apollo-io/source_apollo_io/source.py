#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from requests.auth import AuthBase
import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream

# Basic full refresh stream
class ApolloIoStream(HttpStream, ABC):
    url_base = "https://api.apollo.io/v1/"
    entities = "contacts"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        try:
            pagination = response.json()["pagination"]
            page = int(pagination["page"])
            total_pages = int(pagination["total_pages"])
            return {"page": page + 1} if total_pages > page else None
        except Exception as e:
            raise KeyError(f"error parsing pagination: {e}")
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {"per_page": 200, "page": 1}
        if next_page_token and "page" in next_page_token:
            params["page"] = next_page_token["page"]
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data = response.json()[self.entities]
        if not data:
            return
        for element in data:
            yield element


class Users(ApolloIoStream):
    primary_key = "id"
    entities = "users"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "users/search"


# Basic incremental stream
class IncrementalApolloIoStream(ApolloIoStream, ABC):
    state_checkpoint_interval = 200
    current_run_cursor = None

    @property
    def cursor_field(self) -> str:
        return "updated_at"

    @property
    def cursor_sort_by_field(self) -> str:
        return "contact_updated_at"

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        current_stream_state = current_stream_state or {}
        current_stream_state_date = current_stream_state.get(self.cursor_field, "")
        latest_record_date = latest_record.get(self.cursor_field)
        return {self.cursor_field: max(current_stream_state_date, latest_record_date)}

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        params["sort_by_field"] = self.cursor_sort_by_field
        params["sort_ascending"] = "false"
        return params

    def parse_response(self, response: requests.Response, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping]:
        if not self.current_run_cursor and stream_state:
            self.current_run_cursor = stream_state[self.cursor_field]
        data = response.json()[self.entities]
        # Api doesn't support filtering so while we need to read all
        if not data :
            return
        for element in data:
            if self.current_run_cursor is None or self.current_run_cursor <= element[self.cursor_field]:
                yield element

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        try:
            # stop paging if current page last record cursor field is
            # less than the cursor state for the run
            data = response.json()[self.entities]
            last_record_cursor = data[-1][self.cursor_field]
            if self.current_run_cursor and last_record_cursor < self.current_run_cursor:
                return None
            pagination = response.json()["pagination"]
            page = int(pagination["page"])
            total_pages = int(pagination["total_pages"])
            return {"page": page + 1} if total_pages > page else None
        except Exception as e:
            raise KeyError(f"error parsing pagination: {e}")
        return None


class Contacts(IncrementalApolloIoStream):

    primary_key = "id"
    cursor_field = "updated_at"
    cursor_sort_by_field = "contact_updated_at"

    def path(self, **kwargs) -> str:
        return "contacts/search"

    # def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
    #     """
    #     TODO: Optionally override this method to define this stream's slices. If slicing is not needed, delete this method.
    #
    #     Slices control when state is saved. Specifically, state is saved after a slice has been fully read.
    #     This is useful if the API offers reads by groups or filters, and can be paired with the state object to make reads efficient. See the "concepts"
    #     section of the docs for more information.
    #
    #     The function is called before reading any records in a stream. It returns an Iterable of dicts, each containing the
    #     necessary data to craft a request for a slice. The stream state is usually referenced to determine what slices need to be created.
    #     This means that data in a slice is usually closely related to a stream's cursor_field and stream_state.
    #
    #     An HTTP request is made for each returned slice. The same slice can be accessed in the path, request_params and request_header functions to help
    #     craft that specific request.
    #
    #     For example, if https://example-api.com/v1/employees offers a date query params that returns data for that particular day, one way to implement
    #     this would be to consult the stream state object for the last synced date, then return a slice containing each date from the last synced date
    #     till now. The request_params function would then grab the date from the stream_slice and make it part of the request by injecting it into
    #     the date query param.
    #     """
    #     raise NotImplementedError("Implement stream slices or delete this method!")


class RequestParamAuth(AuthBase):

    def __init__(self, api_key):
        self.api_key = api_key

    def __call__(self, r):
        r.url += f"&api_key={self.api_key}"
        return r


# Source
class SourceApolloIo(AbstractSource):

    # TODO: remove once we verify its working
    @property
    def per_stream_state_enabled(self) -> bool:
        return False

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            api_key = config["api_key"]
            response = requests.get(f"https://api.apollo.io/v1/users/search?api_key={api_key}&page=1&per_page=1");
            response.raise_for_status()
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = RequestParamAuth(config["api_key"])
        return [Users(authenticator=auth), Contacts(authenticator=auth)]
