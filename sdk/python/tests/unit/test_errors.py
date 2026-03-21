import pytest
from achillesdb.errors import (
    AchillesError,
    ERROR_NOT_FOUND,
    ERROR_CONFLICT,
    ERROR_VALIDATION,
    ERROR_SERVER,
    ERROR_CONNECTION,
    ERROR_EMBEDDING,
    ERROR_INVALID_RESPONSE,
    ERROR_VERSION_MISMATCH,
    ERROR_UNKNOWN,
)


class TestAchillesErrorFields:

    def test_message_stored(self):
        err = AchillesError(message="something went wrong", code=ERROR_NOT_FOUND)
        assert str(err.args[0]) == "something went wrong"

    def test_code_stored(self):
        err = AchillesError(message="msg", code=ERROR_NOT_FOUND)
        assert err.code == ERROR_NOT_FOUND

    def test_status_code_stored(self):
        err = AchillesError(message="msg", code=ERROR_SERVER, status_code=500)
        assert err.status_code == 500

    def test_status_code_defaults_to_none(self):
        err = AchillesError(message="msg", code=ERROR_CONNECTION)
        assert err.status_code is None

    def test_details_stored(self):
        details = {"field": "embeddings", "reason": "empty"}
        err = AchillesError(message="msg", code=ERROR_VALIDATION, details=details)
        assert err.details == details

    def test_details_defaults_to_empty_dict(self):
        err = AchillesError(message="msg", code=ERROR_VALIDATION)
        assert err.details == {}

    def test_retry_after_stored(self):
        err = AchillesError(message="msg", code=ERROR_SERVER, retry_after=2.5)
        assert err.retry_after == 2.5

    def test_retry_after_defaults_to_none(self):
        err = AchillesError(message="msg", code=ERROR_SERVER)
        assert err.retry_after is None

    def test_is_exception_subclass(self):
        err = AchillesError(message="msg", code=ERROR_UNKNOWN)
        assert isinstance(err, Exception)

    def test_can_be_raised_and_caught(self):
        with pytest.raises(AchillesError):
            raise AchillesError(message="boom", code=ERROR_CONNECTION)


class TestAchillesErrorStr:

    def test_str_includes_message(self):
        err = AchillesError(message="not found", code=ERROR_NOT_FOUND)
        assert "not found" in str(err)

    def test_str_includes_code(self):
        err = AchillesError(message="not found", code=ERROR_NOT_FOUND)
        assert ERROR_NOT_FOUND in str(err)

    def test_str_includes_status_code_when_present(self):
        err = AchillesError(message="server error", code=ERROR_SERVER, status_code=500)
        assert "500" in str(err)

    def test_str_excludes_http_when_no_status_code(self):
        err = AchillesError(message="connection failed", code=ERROR_CONNECTION)
        assert "HTTP" not in str(err)

    def test_str_format_all_fields(self):
        err = AchillesError(message="conflict", code=ERROR_CONFLICT, status_code=409)
        result = str(err)
        assert "conflict" in result
        assert ERROR_CONFLICT in result
        assert "409" in result


class TestErrorConstants:

    def test_error_constants_are_strings(self):
        constants = [
            ERROR_NOT_FOUND,
            ERROR_CONFLICT,
            ERROR_VALIDATION,
            ERROR_SERVER,
            ERROR_CONNECTION,
            ERROR_EMBEDDING,
            ERROR_INVALID_RESPONSE,
            ERROR_VERSION_MISMATCH,
            ERROR_UNKNOWN,
        ]
        for c in constants:
            assert isinstance(c, str)

    def test_error_constants_are_unique(self):
        constants = [
            ERROR_NOT_FOUND,
            ERROR_CONFLICT,
            ERROR_VALIDATION,
            ERROR_SERVER,
            ERROR_CONNECTION,
            ERROR_EMBEDDING,
            ERROR_INVALID_RESPONSE,
            ERROR_VERSION_MISMATCH,
            ERROR_UNKNOWN,
        ]
        assert len(constants) == len(set(constants))
