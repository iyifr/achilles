from typing import Optional


class AchillesError(Exception):
    """Base exception for all AchillesDB SDK errors."""

    def __init__(
        self,
        message: str,
        code: str,
        status_code: Optional[int] = None,
        details: Optional[dict] = None,
    ):
        self.code = code
        self.status_code = status_code
        self.details = details or {}
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
