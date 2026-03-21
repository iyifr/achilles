import pytest
from unittest.mock import MagicMock
from pydantic import ValidationError

from achillesdb.http.connection import _map_status_to_code, get_base_url, _parse_response
from achillesdb.errors import (
    AchillesError,
    ERROR_CONNECTION,
    ERROR_CONFLICT,
    ERROR_NOT_FOUND,
    ERROR_SERVER,
    ERROR_VALIDATION,
    ERROR_INVALID_RESPONSE,
)
from achillesdb.schemas import GetDatabasesRes


# ─────────────────────────────────────────────────────────────────────────────
# _map_status_to_code
# ─────────────────────────────────────────────────────────────────────────────

class TestMapStatusToCode:

    def test_none_maps_to_connection(self):
        assert _map_status_to_code(None) == ERROR_CONNECTION

    def test_400_maps_to_validation(self):
        assert _map_status_to_code(400) == ERROR_VALIDATION

    def test_404_maps_to_not_found(self):
        assert _map_status_to_code(404) == ERROR_NOT_FOUND

    def test_409_maps_to_conflict(self):
        assert _map_status_to_code(409) == ERROR_CONFLICT

    def test_500_maps_to_server(self):
        assert _map_status_to_code(500) == ERROR_SERVER

    def test_503_maps_to_server(self):
        assert _map_status_to_code(503) == ERROR_SERVER

    def test_599_maps_to_server(self):
        assert _map_status_to_code(599) == ERROR_SERVER

    def test_unknown_status_maps_to_server(self):
        assert _map_status_to_code(418) == ERROR_SERVER


# ─────────────────────────────────────────────────────────────────────────────
# get_base_url
# ─────────────────────────────────────────────────────────────────────────────

class TestGetBaseUrl:

    def test_http_when_ssl_false(self):
        url = get_base_url(host="localhost", port=8180, api_base_path="/api/v1", ssl=False)
        assert url.startswith("http://")

    def test_https_when_ssl_true(self):
        url = get_base_url(host="localhost", port=8180, api_base_path="/api/v1", ssl=True)
        assert url.startswith("https://")

    def test_host_and_port_included(self):
        url = get_base_url(host="localhost", port=8180, api_base_path="/api/v1", ssl=False)
        assert "localhost:8180" in url

    def test_api_base_path_included(self):
        url = get_base_url(host="localhost", port=8180, api_base_path="/api/v1", ssl=False)
        assert "api/v1" in url

    def test_trailing_slash_stripped_from_host(self):
        url = get_base_url(host="localhost/", port=8180, api_base_path="/api/v1", ssl=False)
        assert "localhost/" not in url.replace("localhost:8180", "")

    def test_leading_slash_stripped_from_path(self):
        url = get_base_url(host="localhost", port=8180, api_base_path="/api/v1", ssl=False)
        # should not have double slash between host:port and path
        assert "8180//api" not in url

    def test_full_url_format(self):
        url = get_base_url(host="localhost", port=8180, api_base_path="/api/v1", ssl=False)
        assert url == "http://localhost:8180/api/v1"

    def test_custom_host_and_port(self):
        url = get_base_url(host="myserver.com", port=9090, api_base_path="/api/v2", ssl=True)
        assert url == "https://myserver.com:9090/api/v2"


# ─────────────────────────────────────────────────────────────────────────────
# _parse_response
# ─────────────────────────────────────────────────────────────────────────────

def _make_mock_response(status_code, json_data=None, text="", content_type="application/json", headers=None):
    response = MagicMock()
    response.status_code = status_code
    response.headers = {"Content-Type": content_type, **(headers or {})}
    response.text = text
    if json_data is not None:
        response.json.return_value = json_data
    else:
        response.json.side_effect = Exception("no json")
    return response


class TestParseResponse:

    def test_success_parses_json_into_model(self):
        data = {
            "databases": [{"name": "mydb", "collectionCount": 0, "empty": True}],
            "db_count": 1,
        }
        response = _make_mock_response(200, json_data=data)
        result = _parse_response(response, GetDatabasesRes, expected_status=200)
        assert result.db_count == 1
        assert result.databases[0].name == "mydb"

    def test_success_with_no_res_type_returns_raw(self):
        data = {"key": "value"}
        response = _make_mock_response(200, json_data=data)
        result = _parse_response(response, None, expected_status=200)
        assert result == {"key": "value"}

    def test_json_parse_failure_raises_invalid_response(self):
        response = _make_mock_response(200, json_data=None, text="not json")
        with pytest.raises(AchillesError) as exc_info:
            _parse_response(response, GetDatabasesRes, expected_status=200)
        assert exc_info.value.code == ERROR_INVALID_RESPONSE

    def test_response_validation_failure_raises_invalid_response(self):
        # valid json but wrong shape for the model
        data = {"wrong_field": "oops"}
        response = _make_mock_response(200, json_data=data)
        with pytest.raises(AchillesError) as exc_info:
            _parse_response(response, GetDatabasesRes, expected_status=200)
        assert exc_info.value.code == ERROR_INVALID_RESPONSE

    def test_404_raises_not_found(self):
        response = _make_mock_response(404, json_data={"error": "not found"})
        with pytest.raises(AchillesError) as exc_info:
            _parse_response(response, GetDatabasesRes, expected_status=200)
        assert exc_info.value.code == ERROR_NOT_FOUND
        assert exc_info.value.status_code == 404

    def test_409_raises_conflict(self):
        response = _make_mock_response(409, json_data={"error": "conflict"})
        with pytest.raises(AchillesError) as exc_info:
            _parse_response(response, GetDatabasesRes, expected_status=200)
        assert exc_info.value.code == ERROR_CONFLICT

    def test_500_raises_server_error(self):
        response = _make_mock_response(500, json_data={"error": "internal error"})
        with pytest.raises(AchillesError) as exc_info:
            _parse_response(response, GetDatabasesRes, expected_status=200)
        assert exc_info.value.code == ERROR_SERVER

    def test_retry_after_header_parsed(self):
        headers = {"Retry-After": "3.5"}
        response = _make_mock_response(429, json_data={"error": "rate limited"}, headers=headers)
        with pytest.raises(AchillesError) as exc_info:
            _parse_response(response, GetDatabasesRes, expected_status=200)
        assert exc_info.value.retry_after == 3.5

    def test_invalid_retry_after_header_defaults_to_none(self):
        headers = {"Retry-After": "not-a-number"}
        response = _make_mock_response(429, json_data={"error": "rate limited"}, headers=headers)
        with pytest.raises(AchillesError) as exc_info:
            _parse_response(response, GetDatabasesRes, expected_status=200)
        assert exc_info.value.retry_after is None

    def test_non_json_content_type_logs_warning_but_still_parses(self):
        data = {
            "databases": [{"name": "mydb", "collectionCount": 0, "empty": True}],
            "db_count": 1,
        }
        response = _make_mock_response(200, json_data=data, content_type="text/plain")
        # should still succeed parsing
        result = _parse_response(response, GetDatabasesRes, expected_status=200)
        assert result.db_count == 1
