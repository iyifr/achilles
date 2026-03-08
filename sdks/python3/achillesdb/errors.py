from __future__ import annotations

from typing import Any


class AchillesError(Exception):
    """Base exception for all AchillesDB SDK errors."""

    def __init__(
        self,
        message: str,
        code: str,
        status_code: int | None = None,
        details: dict[str, Any] | None = None,
        retry_after: float | None = None,
    ):
        self.code = code
        self.status_code = status_code
        self.details = details or {}
        self.retry_after = retry_after
        super().__init__(message)

    def __str__(self):
        parts = [self.args[0]]
        if self.code:
            parts.append(f"(code: {self.code})")
        if self.status_code:
            parts.append(f"[HTTP {self.status_code}]")
        return " ".join(parts)


ERROR_NOT_FOUND = "NOT_FOUND"
ERROR_CONFLICT = "CONFLICT"
ERROR_VALIDATION = "VALIDATION"
ERROR_SERVER = "SERVER_ERROR"
ERROR_CONNECTION = "CONNECTION"
ERROR_EMBEDDING = "EMBEDDING"
ERROR_INVALID_RESPONSE = "INVALID_RESPONSE"
ERROR_VERSION_MISMATCH = "VERSION_MISMATCH"
ERROR_UNKNOWN = "UNKNOWN"
