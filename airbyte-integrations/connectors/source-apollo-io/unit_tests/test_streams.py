#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from http import HTTPStatus
from unittest.mock import MagicMock

import pytest
from source_apollo_io.source import ApolloIoStream


@pytest.fixture
def patch_base_class(mocker):
    # Mock abstract methods to enable instantiating abstract class
    mocker.patch.object(ApolloIoStream, "path", "v0/example_endpoint")
    mocker.patch.object(ApolloIoStream, "primary_key", "test_primary_key")
    mocker.patch.object(ApolloIoStream, "__abstractmethods__", set())


def test_request_params(patch_base_class):
    stream = ApolloIoStream()
    inputs = {"stream_slice": None, "stream_state": None, "next_page_token": None}
    expected_params = {"page": 1, "per_page": 200}
    assert stream.request_params(**inputs) == expected_params

def test_request_params_with_next_page_token(patch_base_class):
    stream = ApolloIoStream()
    inputs = {"stream_slice": None, "stream_state": None, "next_page_token": {"page": 2}}
    expected_params = {"page": 2, "per_page": 200}
    assert stream.request_params(**inputs) == expected_params

def test_next_page_token(patch_base_class):
    stream = ApolloIoStream()
    response = MagicMock()
    response.json.return_value = {"pagination": {"page": 1, "total_pages": "3"}}
    inputs = {"response": response}
    expected_token = {"page": 2}
    assert stream.next_page_token(**inputs) == expected_token

def test_next_page_token_on_last_page(patch_base_class):
    stream = ApolloIoStream()
    response = MagicMock()
    response.json.return_value = {"pagination": {"page": 1, "total_pages": "1"}}
    inputs = {"response": response}
    expected_token = None
    assert stream.next_page_token(**inputs) == expected_token


def test_http_method(patch_base_class):
    stream = ApolloIoStream()
    expected_method = "GET"
    assert stream.http_method == expected_method


@pytest.mark.parametrize(
    ("http_status", "should_retry"),
    [
        (HTTPStatus.OK, False),
        (HTTPStatus.BAD_REQUEST, False),
        (HTTPStatus.TOO_MANY_REQUESTS, True),
        (HTTPStatus.INTERNAL_SERVER_ERROR, True),
    ],
)
def test_should_retry(patch_base_class, http_status, should_retry):
    response_mock = MagicMock()
    response_mock.status_code = http_status
    stream = ApolloIoStream()
    assert stream.should_retry(response_mock) == should_retry


def test_backoff_time(patch_base_class):
    response_mock = MagicMock()
    stream = ApolloIoStream()
    expected_backoff_time = None
    assert stream.backoff_time(response_mock) == expected_backoff_time
