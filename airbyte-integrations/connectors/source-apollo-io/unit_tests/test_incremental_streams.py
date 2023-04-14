#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from airbyte_cdk.models import SyncMode
from pytest import fixture
from source_apollo_io.source import IncrementalApolloIoStream


@fixture
def patch_incremental_base_class(mocker):
    # Mock abstract methods to enable instantiating abstract class
    mocker.patch.object(IncrementalApolloIoStream, "path", "v0/example_endpoint")
    mocker.patch.object(IncrementalApolloIoStream, "primary_key", "test_primary_key")
    mocker.patch.object(IncrementalApolloIoStream, "__abstractmethods__", set())


def test_cursor_field(patch_incremental_base_class):
    stream = IncrementalApolloIoStream()
    expected_cursor_field = 'updated_at'
    assert stream.cursor_field == expected_cursor_field


def test_get_updated_state(patch_incremental_base_class):
    stream = IncrementalApolloIoStream()
    # TODO: replace this with your input parameters
    inputs = {"current_stream_state": {"contacts": {"updated_at": "2023-04-12T08:39:49.289Z"}},
              "latest_record": {"updated_at": "2023-04-13T08:39:49.289Z"}}
    # TODO: replace this with your expected updated stream state
    expected_state = {"updated_at": "2023-04-13T08:39:49.289Z"}
    assert stream.get_updated_state(**inputs) == expected_state

def test_supports_incremental(patch_incremental_base_class, mocker):
    mocker.patch.object(IncrementalApolloIoStream, "cursor_field", "dummy_field")
    stream = IncrementalApolloIoStream()
    assert stream.supports_incremental


def test_source_defined_cursor(patch_incremental_base_class):
    stream = IncrementalApolloIoStream()
    assert stream.source_defined_cursor
