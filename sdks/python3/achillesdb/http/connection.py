from __future__ import annotations

import logging
from typing import Any, Literal, Optional, TypeVar

from pydantic import BaseModel, ValidationError

from .retry import with_retry, with_retry_async

from ..schemas import ErrorResponse

from ..config import ConnectionConfig, get_config
from ..version import __version__
from ..errors import (
    AchillesError,
    ERROR_CONNECTION,
    ERROR_CONFLICT,
    ERROR_NOT_FOUND,
    ERROR_SERVER,
    ERROR_VALIDATION,
    ERROR_INVALID_RESPONSE,
)

logger = logging.getLogger(__name__)
cfg = get_config()


ReqModel = TypeVar("ReqModel", bound=BaseModel)
ResModel = TypeVar("ResModel", bound=BaseModel)


def _map_status_to_code(status: Optional[int]) -> str:
    if status is None:
        return ERROR_CONNECTION
    if status == 400:
        return ERROR_VALIDATION
    if status == 404:
        return ERROR_NOT_FOUND
    if status == 409:
        return ERROR_CONFLICT
    if 500 <= status < 600:
        return ERROR_SERVER
    return ERROR_SERVER


def _parse_response(
    response: Any,
    resType: Optional[type[ResModel]] = None,
    expected_status: Optional[int] = None,
) -> ResModel:
    status_code: int = response.status_code

    is_success = (
        expected_status is not None and status_code == expected_status
    ) or (
        expected_status is None and 200 <= status_code < 300
    )

    if is_success:
        content_type = response.headers.get("Content-Type", "")
        if "application/json" not in content_type:
            logger.warning(
                "Expected application/json but got '%s' — attempting to parse anyway.",
                content_type,
            )
        try:
            raw = response.json()
        except Exception as e:
            raise AchillesError(
                message=f"Failed to parse response as JSON (Content-Type: {content_type}): {e}",
                code=ERROR_INVALID_RESPONSE,
            ) from e

        if resType is None:
            return raw

        try:
            return resType.model_validate(raw)
        except ValidationError as e:
            raise AchillesError(
                message=f"Response validation failed: {e}",
                code=ERROR_INVALID_RESPONSE,
            ) from e

    message: str = f"HTTP {status_code}"
    details: dict = {}

    try:
        error_body = response.json()
        message = error_body.get("error") or error_body.get("message") or message
        details = error_body
    except Exception:
        try:
            message = response.text or message
        except Exception:
            pass
    try:
        retry_after_raw = response.headers.get("Retry-After")
        retry_after = float(retry_after_raw) if retry_after_raw is not None else None
    except (ValueError, TypeError):
        retry_after = None

    raise AchillesError(
        message=message,
        code=_map_status_to_code(status_code),
        status_code=status_code,
        details=details,
        retry_after=retry_after,
    )


def get_base_url(
    host: str = cfg.default_host,
    port: int = cfg.default_port,
    api_base_path: str = cfg.default_api_base_path,
    ssl: bool = cfg.default_ssl,
) -> str:
    protocol = "https" if ssl else "http"
    host = host.strip("/")
    api_base_path = api_base_path.strip("/")
    return f"{protocol}://{host}:{port}/{api_base_path}"


class _HTTPClient:
    def __init__(
        self,
        host: str = cfg.default_host,
        port: int = cfg.default_port,
        api_base_path: str = cfg.default_api_base_path,
        ssl: bool = cfg.default_ssl,
        mode: Literal["sync", "async"] = "sync",
        timeout: Optional[float] = None,
        connection_config: Optional[ConnectionConfig] = None,
        logger: Optional[logging.Logger] = None,
        max_retries: int = 3,
    ):
        self.base_url = get_base_url(
            host=host,
            port=port,
            api_base_path=api_base_path,
            ssl=ssl,
        )
        self.mode = mode
        self.timeout = timeout if timeout is not None else float(cfg.default_timeout)
        self._conn_cfg = connection_config or cfg.connection
        self.logger: logging.Logger = logger or logging.getLogger(__name__)
        self._max_retries = max_retries
        self.ssl = ssl

        if not ssl:
            logger.warning(
                "SSL is disabled for %s:%s — not recommended outside localhost.",
                host, port,
            )

        self.default_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": f"achillesdb-python-sdk/{__version__}",
        }

        if self.mode == "async":
            self._init_async()
        else:
            self._init_sync()

    def _init_sync(self) -> None:
        try:
            import requests
            import requests.adapters
        except ImportError as exc:
            raise ImportError(
                "requests is required for sync mode. Install with: pip install requests"
            ) from exc

        self._sync_session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=self._conn_cfg.session_pool_connections,
            pool_maxsize=self._conn_cfg.session_pool_maxsize,
            max_retries=0,
        )
        self._sync_session.mount("http://", adapter)
        self._sync_session.mount("https://", adapter)

    def _init_async(self) -> None:
        try:
            import httpx
        except ImportError as exc:
            raise ImportError(
                "httpx is required for async mode. Install with: pip install httpx"
            ) from exc

        self._async_client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=self.timeout,
            limits=httpx.Limits(
                max_connections=self._conn_cfg.session_pool_connections,
                max_keepalive_connections=self._conn_cfg.session_pool_maxsize,
                keepalive_expiry=float(self._conn_cfg.session_pool_timeout),
            ),
        )

    def _make_url(self, path: str) -> str:
        return self.base_url.rstrip("/") + "/" + path.lstrip("/")

    def request(
        self,
        method: str,
        path: str,
        resType: type[ResModel] | None = None,
        json: Any = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        timeout: float | None = None,
        expected_status: int | None = None,
        retry: bool = True,
    ) -> ResModel:
        if self.mode == "async":
            return self._request_async_with_retry(  # type: ignore[return-value]
                method, path, json=json, params=params,
                headers=headers, timeout=timeout,
                expected_status=expected_status, resType=resType
            ) if retry else self._request_async(  # type: ignore[return-value]
                method, path, json=json, params=params,
                headers=headers, timeout=timeout,
                expected_status=expected_status, resType=resType
            )
        return with_retry(
            lambda: self._request_sync(
                method, path, json=json, params=params,
                headers=headers, timeout=timeout,
                expected_status=expected_status, resType=resType
            ),
            max_attempts=self._max_retries,
        ) if retry else self._request_sync(
            method, path, json=json, params=params,
            headers=headers, timeout=timeout,
            expected_status=expected_status, resType=resType
        )

    async def _request_async_with_retry(self, method, path, **kwargs):
        return await with_retry_async(
            lambda: self._request_async(method, path, **kwargs),
            max_attempts=self._max_retries,
        )

    def _request_sync(
        self,
        method: str,
        path: str,
        resType: type[ResModel] | None = None,
        json: Any = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        timeout: float | None = None,
        expected_status: int | None = None,
    ) -> ResModel:
        import requests as req_lib

        url = self._make_url(path)
        merged_headers = {**self.default_headers, **(headers or {})}
        effective_timeout = timeout if timeout is not None else self.timeout

        self.logger.debug("→ %s %s", method, url)

        try:
            resp = self._sync_session.request(
                method,
                url,
                json=json,
                params=params,
                headers=merged_headers,
                timeout=effective_timeout,
            )
        except req_lib.exceptions.Timeout as exc:
            raise AchillesError(message=f"Request timed out: {method} {url}", code=ERROR_CONNECTION) from exc
        except req_lib.exceptions.ConnectionError as exc:
            raise AchillesError(message=f"Connection failed: {url}", code=ERROR_CONNECTION) from exc
        except req_lib.exceptions.RequestException as exc:
            raise AchillesError(message=str(exc), code=ERROR_CONNECTION) from exc

        self.logger.debug("← %s %s  status=%d", method, url, resp.status_code)
        return _parse_response(resp, resType, expected_status)

    async def _request_async(
        self,
        method: str,
        path: str,
        resType: type[ResModel] | None = None,
        json: Any = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        timeout: float | None = None,
        expected_status: int | None = None,
    ) -> ResModel:
        import httpx

        merged_headers = {**self.default_headers, **(headers or {})}

        self.logger.debug("→ %s %s", method, path)

        try:
            resp = await self._async_client.request(
                method,
                path,
                json=json,
                params=params,
                headers=merged_headers,
                timeout=timeout if timeout is not None else self.timeout,
            )
        except httpx.TimeoutException as exc:
            raise AchillesError(message=f"Request timed out: {method} {path}", code=ERROR_CONNECTION) from exc
        except httpx.ConnectError as exc:
            raise AchillesError(message=f"Connection failed: {path}", code=ERROR_CONNECTION) from exc
        except httpx.RequestError as exc:
            raise AchillesError(message=str(exc), code=ERROR_CONNECTION) from exc

        self.logger.debug("← %s %s  status=%d", method, path, resp.status_code)
        return _parse_response(resp, resType, expected_status)

    def close(self) -> None:
        if self.mode == "sync":
            self._sync_session.close()
        else:
            raise ValueError("this is an sync client session mathod and not async")

    async def aclose(self) -> None:
        if self.mode == "async":
            await self._async_client.aclose()
        else:
            raise ValueError("this is an async client session mathod and not sync")


class SyncHttpClient(_HTTPClient):
    def __init__(self, **kwargs):
        kwargs.setdefault("mode", "sync")
        super().__init__(**kwargs)

    def get(
        self,
        path: str,
        resType: type[ResModel],
        params: Optional[dict] = None,
        expected_status: Optional[int] = None,
        retry: bool = True,
    ) -> ResModel:
        return self.request(
            "GET", path, resType, params=params,
            expected_status=expected_status,
            retry=retry,
        )

    def post(
        self,
        path: str,
        resType: type[ResModel],
        json: Any = None,
        params: Optional[dict] = None,
        expected_status: Optional[int] = None,
        retry: bool = False,
    ) -> ResModel:
        return self.request(
            "POST", path, resType, params=params,
            json=json, expected_status=expected_status,
            retry=retry,
        )

    def put(
        self,
        path: str,
        resType: type[ResModel],
        json: Any = None,
        params: Optional[dict] = None,
        expected_status: Optional[int] = None,
        retry: bool = False,
    ) -> ResModel:
        return self.request(
            "PUT", path, resType, params=params,
            json=json, expected_status=expected_status,
            retry=retry,
        )

    def patch(
        self,
        path: str,
        resType: type[ResModel],
        json: Any = None,
        params: Optional[dict] = None,
        expected_status: Optional[int] = None,
        retry: bool = True,
    ) -> ResModel:
        return self.request(
            "PATCH", path, resType, params=params,
            json=json, expected_status=expected_status,
            retry=retry,
        )

    def delete(
        self,
        path: str,
        resType: type[ResModel],
        json: Any = None,
        params: Optional[dict] = None,
        expected_status: Optional[int] = None,
        retry: bool = True,
    ) -> ResModel:
        return self.request(
            "DELETE", path, resType, params=params,
            json=json, expected_status=expected_status,
            retry=retry,
        )

    def head(
        self,
        path: str,
        resType: type[ResModel],
        params: Optional[dict] = None,
        expected_status: Optional[int] = None,
        retry: bool = True,
    ) -> ResModel:
        return self.request(
            "HEAD", path, resType, params=params,
            expected_status=expected_status,
            retry=retry,
        )


class AsyncHttpClient(_HTTPClient):
    def __init__(self, **kwargs):
        kwargs.setdefault("mode", "async")
        super().__init__(**kwargs)

    async def get(
        self,
        path: str,
        resType: type[ResModel],
        params: Optional[dict] = None,
        expected_status: Optional[int] = None,
        retry: bool = True,
    ) -> ResModel:
        return await self.request(  # type: ignore[misc]
            "GET", path, resType, params=params,
            expected_status=expected_status,
            retry=retry,
        )

    async def post(
        self,
        path: str,
        resType: type[ResModel],
        json: Any = None,
        params: Optional[dict] = None,
        expected_status: Optional[int] = None,
        retry: bool = False,
    ) -> ResModel:
        return await self.request(  # type: ignore[misc]
            "POST", path, resType, params=params,
            json=json, expected_status=expected_status,
            retry=retry,
        )

    async def put(
        self,
        path: str,
        resType: type[ResModel],
        json: Any = None,
        params: Optional[dict] = None,
        expected_status: Optional[int] = None,
        retry: bool = False,
    ) -> ResModel:
        return await self.request(  # type: ignore[misc]
            "PUT", path, resType, params=params,
            json=json, expected_status=expected_status,
            retry=retry,
        )

    async def patch(
        self,
        path: str,
        resType: type[ResModel],
        json: Any = None,
        params: Optional[dict] = None,
        expected_status: Optional[int] = None,
        retry: bool = True,
    ) -> ResModel:
        return await self.request(  # type: ignore[misc]
            "PATCH", path, resType, params=params,
            json=json, expected_status=expected_status,
            retry=retry,
        )

    async def delete(
        self,
        path: str,
        resType: type[ResModel],
        json: Any = None,
        params: Optional[dict] = None,
        expected_status: Optional[int] = None,
        retry: bool = True,
    ) -> ResModel:
        return await self.request(  # type: ignore[misc]
            "DELETE", path, resType, params=params,
            json=json, expected_status=expected_status,
            retry=retry,
        )

    async def head(
        self,
        path: str,
        resType: type[ResModel],
        params: Optional[dict] = None,
        expected_status: Optional[int] = None,
        retry: bool = True,
    ) -> ResModel:
        return await self.request(  # type: ignore[misc]
            "HEAD", path, resType, params=params,
            expected_status=expected_status,
            retry=retry,
        )
