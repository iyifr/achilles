"""
tests/unit/test_http_parsing.py
================================
Unit tests for the HTTP response parsing logic in
achillesdb/http/connection.py

Covers:
  - _parse_response  (success path, error path, edge cases)
  - get_base_url     (URL construction, SSL, trailing slash handling)

No real HTTP calls are made. Response objects are simulated with MagicMock
matching the interface used by both `requests` (sync) and `httpx` (async)
responses — both expose .status_code, .headers, .json(), and .text.
"""

import pytest
from unittest.mock import MagicMock, patch
from pydantic import BaseModel

from achillesdb.http.connection import _parse_response, get_base_url
from achillesdb.errors import (
    AchillesError,
    ERROR_CONNECTION,
    ERROR_NOT_FOUND,
    ERROR_CONFLICT,
    ERROR_SERVER,
    ERROR_VALIDATION,
    ERROR_INVALID_RESPONSE,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

class _SampleModel(BaseModel):
    """Minimal Pydantic model used as resType in parse tests."""
    name: str
    value: int


def _mock_response(
    status_code: int = 200,
    json_body: dict | None = None,
    text_body: str = "",
    content_type: str = "application/json",
    headers: dict | None = None,
    json_raises: Exception | None = None,
) -> MagicMock:
    """
    Build a mock response object that mimics both requests.Response
    and httpx.Response interfaces used by _parse_response.
    """
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = text_body

    all_headers = {"Content-Type": content_type}
    if headers:
        all_headers.update(headers)
    resp.headers = all_headers

    if json_raises is not None:
        resp.json.side_effect = json_raises
    else:
        resp.json.return_value = json_body if json_body is not None else {}

    return resp


# ─────────────────────────────────────────────────────────────────────────────
# _parse_response — success path
# ─────────────────────────────────────────────────────────────────────────────

class TestParseResponseSuccess:

    def test_200_with_valid_json_returns_model(self):
        resp = _mock_response(
            status_code=200,
            json_body={"name": "test", "value": 42},
        )
        result = _parse_response(resp, _SampleModel, expected_status=200)
        assert isinstance(result, _SampleModel)
        assert result.name == "test"
        assert result.value == 42

    def test_200_without_res_type_returns_raw_dict(self):
        resp = _mock_response(status_code=200, json_body={"key": "val"})
        result = _parse_response(resp, resType=None, expected_status=200)
        assert result == {"key": "val"}

    def test_expected_status_none_accepts_any_2xx(self):
        for code in [200, 201, 204]:
            resp = _mock_response(
                status_code=code,
                json_body={"name": "x", "value": 1},
            )
            result = _parse_response(resp, _SampleModel, expected_status=None)
            assert isinstance(result, _SampleModel)

    def test_non_json_content_type_logs_warning_but_still_parses(self):
        resp = _mock_response(
            status_code=200,
            json_body={"name": "x", "value": 1},
            content_type="text/plain",
        )
        import logging
        with patch.object(logging.getLogger("achillesdb.http.connection"), "warning") as mock_warn:
            result = _parse_response(resp, _SampleModel, expected_status=200)
        assert isinstance(result, _SampleModel)
        mock_warn.assert_called_once()

    def test_malformed_json_raises_invalid_response(self):
        resp = _mock_response(
            status_code=200,
            json_raises=ValueError("bad json"),
        )
        with pytest.raises(AchillesError) as exc_info:
            _parse_response(resp, _SampleModel, expected_status=200)
        assert exc_info.value.code == ERROR_INVALID_RESPONSE

    def test_malformed_json_error_message_includes_content_type(self):
        resp = _mock_response(
            status_code=200,
            json_raises=ValueError("bad json"),
            content_type="text/html",
        )
        with pytest.raises(AchillesError) as exc_info:
            _parse_response(resp, _SampleModel, expected_status=200)
        assert "text/html" in str(exc_info.value)

    def test_schema_validation_failure_raises_invalid_response(self):
        # JSON parses fine but doesn't match the model schema
        resp = _mock_response(
            status_code=200,
            json_body={"wrong_field": "oops"},
        )
        with pytest.raises(AchillesError) as exc_info:
            _parse_response(resp, _SampleModel, expected_status=200)
        assert exc_info.value.code == ERROR_INVALID_RESPONSE

    def test_schema_validation_error_message_includes_detail(self):
        resp = _mock_response(
            status_code=200,
            json_body={"wrong_field": "oops"},
        )
        with pytest.raises(AchillesError) as exc_info:
            _parse_response(resp, _SampleModel, expected_status=200)
        assert "validation" in str(exc_info.value).lower()

    def test_status_mismatch_treated_as_error(self):
        # server returns 201 but expected_status=200 — should be an error
        resp = _mock_response(
            status_code=201,
            json_body={"error": "unexpected status"},
        )
        with pytest.raises(AchillesError):
            _parse_response(resp, _SampleModel, expected_status=200)


# ─────────────────────────────────────────────────────────────────────────────
# _parse_response — error path: status code mapping
# ─────────────────────────────────────────────────────────────────────────────

class TestParseResponseErrorCodes:

    def test_400_maps_to_validation_error(self):
        resp = _mock_response(
            status_code=400,
            json_body={"error": "bad request"},
        )
        with pytest.raises(AchillesError) as exc_info:
            _parse_response(resp, _SampleModel, expected_status=200)
        assert exc_info.value.code == ERROR_VALIDATION

    def test_404_maps_to_not_found_error(self):
        resp = _mock_response(
            status_code=404,
            json_body={"error": "not found"},
        )
        with pytest.raises(AchillesError) as exc_info:
            _parse_response(resp, _SampleModel, expected_status=200)
        assert exc_info.value.code == ERROR_NOT_FOUND

    def test_409_maps_to_conflict_error(self):
        resp = _mock_response(
            status_code=409,
            json_body={"error": "already exists"},
        )
        with pytest.raises(AchillesError) as exc_info:
            _parse_response(resp, _SampleModel, expected_status=200)
        assert exc_info.value.code == ERROR_CONFLICT

    def test_500_maps_to_server_error(self):
        resp = _mock_response(
            status_code=500,
            json_body={"error": "internal error"},
        )
        with pytest.raises(AchillesError) as exc_info:
            _parse_response(resp, _SampleModel, expected_status=200)
        assert exc_info.value.code == ERROR_SERVER

    def test_503_maps_to_server_error(self):
        resp = _mock_response(
            status_code=503,
            json_body={"error": "service unavailable"},
        )
        with pytest.raises(AchillesError) as exc_info:
            _parse_response(resp, _SampleModel, expected_status=200)
        assert exc_info.value.code == ERROR_SERVER

    def test_status_code_preserved_on_exception(self):
        resp = _mock_response(status_code=404, json_body={"error": "gone"})
        with pytest.raises(AchillesError) as exc_info:
            _parse_response(resp, _SampleModel, expected_status=200)
        assert exc_info.value.status_code == 404


# ─────────────────────────────────────────────────────────────────────────────
# _parse_response — error path: message extraction
# ─────────────────────────────────────────────────────────────────────────────

class TestParseResponseErrorMessages:

    def test_error_key_extracted_from_json_body(self):
        # This test will FAIL with current code because ErrorResponse.model_validate
        # receives bytes (response.content) instead of the parsed dict.
        # It documents the existing bug — should pass after fix.
        resp = _mock_response(
            status_code=404,
            json_body={"error": "collection not found"},
        )
        with pytest.raises(AchillesError) as exc_info:
            _parse_response(resp, _SampleModel, expected_status=200)
        assert "collection not found" in str(exc_info.value)

    def test_message_key_extracted_from_json_body(self):
        resp = _mock_response(
            status_code=400,
            json_body={"message": "invalid input provided"},
        )
        with pytest.raises(AchillesError) as exc_info:
            _parse_response(resp, _SampleModel, expected_status=200)
        assert "invalid input provided" in str(exc_info.value)

    def test_falls_back_to_http_status_when_no_error_key(self):
        resp = _mock_response(
            status_code=503,
            json_body={"unrelated": "data"},
        )
        with pytest.raises(AchillesError) as exc_info:
            _parse_response(resp, _SampleModel, expected_status=200)
        assert "503" in str(exc_info.value)

    def test_falls_back_to_response_text_when_not_json(self):
        resp = _mock_response(
            status_code=502,
            json_raises=ValueError("not json"),
            text_body="Bad Gateway",
            content_type="text/plain",
        )
        with pytest.raises(AchillesError) as exc_info:
            _parse_response(resp, _SampleModel, expected_status=200)
        assert "Bad Gateway" in str(exc_info.value)

    def test_falls_back_to_http_status_when_text_also_empty(self):
        resp = _mock_response(
            status_code=503,
            json_raises=ValueError("not json"),
            text_body="",
        )
        with pytest.raises(AchillesError) as exc_info:
            _parse_response(resp, _SampleModel, expected_status=200)
        assert "503" in str(exc_info.value)


# ─────────────────────────────────────────────────────────────────────────────
# _parse_response — Retry-After header
# ─────────────────────────────────────────────────────────────────────────────

class TestParseResponseRetryAfter:

    def test_retry_after_header_present_sets_retry_after_on_exception(self):
        resp = _mock_response(
            status_code=429,
            json_body={"error": "rate limited"},
            headers={"Retry-After": "10"},
        )
        with pytest.raises(AchillesError) as exc_info:
            _parse_response(resp, _SampleModel, expected_status=200)
        assert exc_info.value.retry_after == pytest.approx(10.0)

    def test_retry_after_float_string_parsed_correctly(self):
        resp = _mock_response(
            status_code=429,
            json_body={"error": "rate limited"},
            headers={"Retry-After": "2.5"},
        )
        with pytest.raises(AchillesError) as exc_info:
            _parse_response(resp, _SampleModel, expected_status=200)
        assert exc_info.value.retry_after == pytest.approx(2.5)

    def test_retry_after_header_absent_sets_none(self):
        resp = _mock_response(
            status_code=503,
            json_body={"error": "unavailable"},
        )
        # ensure Retry-After is not in headers
        resp.headers = {"Content-Type": "application/json"}
        with pytest.raises(AchillesError) as exc_info:
            _parse_response(resp, _SampleModel, expected_status=200)
        assert exc_info.value.retry_after is None

    def test_retry_after_non_numeric_sets_none(self):
        resp = _mock_response(
            status_code=429,
            json_body={"error": "rate limited"},
            headers={"Retry-After": "Thu, 01 Jan 2026 00:00:00 GMT"},
        )
        with pytest.raises(AchillesError) as exc_info:
            _parse_response(resp, _SampleModel, expected_status=200)
        assert exc_info.value.retry_after is None

    def test_retry_after_zero_parsed_correctly(self):
        resp = _mock_response(
            status_code=429,
            json_body={"error": "rate limited"},
            headers={"Retry-After": "0"},
        )
        with pytest.raises(AchillesError) as exc_info:
            _parse_response(resp, _SampleModel, expected_status=200)
        assert exc_info.value.retry_after == pytest.approx(0.0)

    def test_retry_after_on_success_response_not_accessed(self):
        # Retry-After on a 200 should not cause any error
        resp = _mock_response(
            status_code=200,
            json_body={"name": "ok", "value": 1},
            headers={"Retry-After": "5"},
        )
        result = _parse_response(resp, _SampleModel, expected_status=200)
        assert isinstance(result, _SampleModel)


# ─────────────────────────────────────────────────────────────────────────────
# get_base_url
# ─────────────────────────────────────────────────────────────────────────────

class TestGetBaseUrl:

    def test_ssl_true_uses_https(self):
        url = get_base_url(host="localhost", port=8180, api_base_path="/api/v1", ssl=True)
        assert url.startswith("https://")

    def test_ssl_false_uses_http(self):
        url = get_base_url(host="localhost", port=8180, api_base_path="/api/v1", ssl=False)
        assert url.startswith("http://")

    def test_port_included_in_url(self):
        url = get_base_url(host="localhost", port=8180, api_base_path="/api/v1", ssl=False)
        assert ":8180" in url

    def test_api_base_path_included(self):
        url = get_base_url(host="localhost", port=8180, api_base_path="/api/v1", ssl=False)
        assert "api/v1" in url

    def test_trailing_slash_stripped_from_host(self):
        url = get_base_url(host="localhost/", port=8180, api_base_path="/api/v1", ssl=False)
        assert "//" not in url.replace("http://", "").replace("https://", "")

    def test_trailing_slash_stripped_from_api_base_path(self):
        url = get_base_url(host="localhost", port=8180, api_base_path="/api/v1/", ssl=False)
        assert not url.endswith("/")

    def test_leading_slash_stripped_from_api_base_path(self):
        url1 = get_base_url(host="localhost", port=8180, api_base_path="/api/v1", ssl=False)
        url2 = get_base_url(host="localhost", port=8180, api_base_path="api/v1", ssl=False)
        assert url1 == url2

    def test_full_url_structure(self):
        url = get_base_url(host="myhost", port=9090, api_base_path="/api/v2", ssl=True)
        assert url == "https://myhost:9090/api/v2"

    def test_different_port(self):
        url = get_base_url(host="localhost", port=3000, api_base_path="/api/v1", ssl=False)
        assert ":3000" in url

    def test_remote_host(self):
        url = get_base_url(host="db.example.com", port=443, api_base_path="/api/v1", ssl=True)
        assert "db.example.com" in url
        assert url.startswith("https://")
