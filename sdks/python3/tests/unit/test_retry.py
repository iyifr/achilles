import asyncio
import pytest
from unittest.mock import MagicMock, patch

from achillesdb.http.retry import _backoff, _should_retry, with_retry, with_retry_async
from achillesdb.errors import (
    AchillesError,
    ERROR_CONNECTION,
    ERROR_SERVER,
    ERROR_NOT_FOUND,
)


# ─────────────────────────────────────────────────────────────────────────────
# _backoff
# ─────────────────────────────────────────────────────────────────────────────

class TestBackoff:

    def test_returns_retry_after_when_set(self):
        delay = _backoff(attempt=1, retry_after=5.0)
        assert delay == 5.0

    def test_returns_retry_after_zero(self):
        delay = _backoff(attempt=1, retry_after=0.0)
        assert delay == 0.0

    def test_returns_float_when_no_retry_after(self):
        delay = _backoff(attempt=1, retry_after=None)
        assert isinstance(delay, float)

    def test_delay_within_ceiling_attempt_1(self):
        # ceiling = min(0.5 * 2^0, 30) = 0.5
        for _ in range(20):
            delay = _backoff(attempt=1, retry_after=None)
            assert 0.0 <= delay <= 0.5

    def test_delay_within_ceiling_attempt_2(self):
        # ceiling = min(0.5 * 2^1, 30) = 1.0
        for _ in range(20):
            delay = _backoff(attempt=2, retry_after=None)
            assert 0.0 <= delay <= 1.0

    def test_ceiling_caps_at_30(self):
        # at high attempt numbers ceiling should not exceed 30
        for _ in range(20):
            delay = _backoff(attempt=20, retry_after=None)
            assert delay <= 30.0


# ─────────────────────────────────────────────────────────────────────────────
# _should_retry
# ─────────────────────────────────────────────────────────────────────────────

class TestShouldRetry:

    def test_returns_false_when_max_attempts_reached(self):
        err = AchillesError("fail", code=ERROR_CONNECTION)
        assert _should_retry(err, attempt=3, max_attempts=3, method="GET") is False

    def test_returns_false_for_non_achilles_error(self):
        err = ValueError("not an achilles error")
        assert _should_retry(err, attempt=1, max_attempts=3, method="GET") is False

    def test_returns_false_for_non_idempotent_method_post(self):
        err = AchillesError("fail", code=ERROR_CONNECTION)
        assert _should_retry(err, attempt=1, max_attempts=3, method="POST") is False

    def test_returns_false_for_non_idempotent_method_put(self):
        err = AchillesError("fail", code=ERROR_CONNECTION)
        assert _should_retry(err, attempt=1, max_attempts=3, method="PUT") is False

    def test_returns_false_for_non_idempotent_method_patch(self):
        err = AchillesError("fail", code=ERROR_CONNECTION)
        assert _should_retry(err, attempt=1, max_attempts=3, method="PATCH") is False

    def test_returns_true_for_connection_error_on_get(self):
        err = AchillesError("fail", code=ERROR_CONNECTION)
        assert _should_retry(err, attempt=1, max_attempts=3, method="GET") is True

    def test_returns_true_for_connection_error_on_delete(self):
        err = AchillesError("fail", code=ERROR_CONNECTION)
        assert _should_retry(err, attempt=1, max_attempts=3, method="DELETE") is True

    def test_returns_true_for_connection_error_on_head(self):
        err = AchillesError("fail", code=ERROR_CONNECTION)
        assert _should_retry(err, attempt=1, max_attempts=3, method="HEAD") is True

    def test_returns_true_for_retryable_status_429(self):
        err = AchillesError("rate limited", code=ERROR_SERVER, status_code=429)
        assert _should_retry(err, attempt=1, max_attempts=3, method="GET") is True

    def test_returns_true_for_retryable_status_503(self):
        err = AchillesError("unavailable", code=ERROR_SERVER, status_code=503)
        assert _should_retry(err, attempt=1, max_attempts=3, method="GET") is True

    def test_returns_false_for_non_retryable_status_404(self):
        err = AchillesError("not found", code=ERROR_NOT_FOUND, status_code=404)
        assert _should_retry(err, attempt=1, max_attempts=3, method="GET") is False

    def test_method_check_is_case_insensitive(self):
        err = AchillesError("fail", code=ERROR_CONNECTION)
        assert _should_retry(err, attempt=1, max_attempts=3, method="get") is True


# ─────────────────────────────────────────────────────────────────────────────
# with_retry (sync)
# ─────────────────────────────────────────────────────────────────────────────

class TestWithRetry:

    def test_succeeds_on_first_try(self):
        fn = MagicMock(return_value="ok")
        result = with_retry(fn, max_attempts=3)
        assert result == "ok"
        assert fn.call_count == 1

    def test_raises_immediately_for_non_retryable_error(self):
        err = ValueError("not retryable")
        fn = MagicMock(side_effect=err)
        with pytest.raises(ValueError):
            with_retry(fn, max_attempts=3)
        assert fn.call_count == 1

    def test_retries_and_succeeds(self):
        connection_err = AchillesError("conn fail", code=ERROR_CONNECTION)
        fn = MagicMock(side_effect=[connection_err, connection_err, "ok"])

        with patch("achillesdb.http.retry._should_retry", side_effect=[True, True, False]):
            with patch("achillesdb.http.retry.time.sleep"):
                with patch("achillesdb.http.retry._backoff", return_value=0):
                    # manually test the retry loop behavior
                    pass

        # simpler direct test: patch sleep to avoid waiting
        call_count = 0
        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise AchillesError("conn", code=ERROR_CONNECTION)
            return "ok"

        with patch("achillesdb.http.retry.time.sleep"):
            with patch("achillesdb.http.retry._backoff", return_value=0):
                with patch("achillesdb.http.retry._should_retry", side_effect=[True, True]):
                    result = with_retry(flaky, max_attempts=3)
        assert result == "ok"
        assert call_count == 3

    def test_raises_after_max_attempts(self):
        err = AchillesError("conn fail", code=ERROR_CONNECTION)
        fn = MagicMock(side_effect=err)

        with patch("achillesdb.http.retry.time.sleep"):
            with patch("achillesdb.http.retry._backoff", return_value=0):
                with patch("achillesdb.http.retry._should_retry", side_effect=[True, False]):
                    with pytest.raises(AchillesError):
                        with_retry(fn, max_attempts=2)


# ─────────────────────────────────────────────────────────────────────────────
# with_retry_async
# ─────────────────────────────────────────────────────────────────────────────

class TestWithRetryAsync:

    @pytest.mark.asyncio
    async def test_succeeds_on_first_try(self):
        async def fn():
            return "ok"

        result = await with_retry_async(fn, max_attempts=3)
        assert result == "ok"

    @pytest.mark.asyncio
    async def test_raises_immediately_for_non_retryable_error(self):
        async def fn():
            raise ValueError("not retryable")

        with pytest.raises(ValueError):
            await with_retry_async(fn, max_attempts=3)

    @pytest.mark.asyncio
    async def test_retries_and_succeeds(self):
        call_count = 0

        async def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise AchillesError("conn", code=ERROR_CONNECTION)
            return "ok"

        with patch("achillesdb.http.retry.asyncio.sleep"):
            with patch("achillesdb.http.retry._backoff", return_value=0):
                with patch("achillesdb.http.retry._should_retry", side_effect=[True, True]):
                    result = await with_retry_async(flaky, max_attempts=3)
        assert result == "ok"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_raises_after_max_attempts(self):
        async def fn():
            raise AchillesError("conn fail", code=ERROR_CONNECTION)

        with patch("achillesdb.http.retry.asyncio.sleep"):
            with patch("achillesdb.http.retry._backoff", return_value=0):
                with patch("achillesdb.http.retry._should_retry", side_effect=[True, False]):
                    with pytest.raises(AchillesError):
                        await with_retry_async(fn, max_attempts=2)
