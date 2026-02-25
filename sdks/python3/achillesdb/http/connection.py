from __future__ import annotations

import logging
from typing import Any, Literal, Optional
from urllib.parse import urljoin

from ..config import ConnectionConfig, get_config
from ..errors import (
    AchillesError,
    ERROR_CONNECTION,
    ERROR_CONFLICT,
    ERROR_NOT_FOUND,
    ERROR_SERVER,
    ERROR_VALIDATION,
)

logger = logging.getLogger(__name__)
cfg = get_config()


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


def _parse_response(response: Any) -> Any:
    """
    Parse a response object into a Python value.
    Returns parsed JSON if Content-Type is application/json, else raw text.
    Raises AchillesError on non-2xx.
    """
    status_code: int = response.status_code

    if 200 <= status_code < 300:
        content_type = response.headers.get("Content-Type", "")
        if "application/json" in content_type:
            return response.json()
        return response.text

    # Non-2xx — extract server message if possible
    message: str = f"HTTP {status_code}"
    details: dict = {}
    try:
        details = response.json()
        message = details.get("detail") or details.get("message") or message
    except Exception:
        try:
            message = response.text or message
        except Exception:
            pass

    raise AchillesError(
        message=message,
        code=_map_status_to_code(status_code),
        status_code=status_code,
        details=details,
    )


class _HTTPClient:
    """
    Main Http Wrapper
    """

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
        self.logger = logger or logging.getLogger(__name__)

        self.default_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "achillesdb-python-sdk/0.1",
        }

        # initialise the appropriate underlying client
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
            max_retries=0
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
        return urljoin(self.base_url + "/", path.lstrip("/"))


    def request(
        self,
        method: str,
        path: str,
        json: Any = None,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        timeout: Optional[float] = None,
    ) -> Any:
        if self.mode == "async":
            return self._request_async(method, path, json=json, params=params, headers=headers, timeout=timeout)

        return self._request_sync(method, path, json=json, params=params, headers=headers, timeout=timeout)

    def _request_sync(
        self,
        method: str,
        path: str,
        json: Any = None,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        timeout: Optional[float] = None,
    ) -> Any:
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
            raise AchillesError(
                message=f"Request timed out: {method} {url}",
                code=ERROR_CONNECTION,
            ) from exc
        except req_lib.exceptions.ConnectionError as exc:
            raise AchillesError(
                message=f"Connection failed: {url}",
                code=ERROR_CONNECTION,
            ) from exc
        except req_lib.exceptions.RequestException as exc:
            raise AchillesError(
                message=str(exc),
                code=ERROR_CONNECTION,
            ) from exc

        self.logger.debug("← %s %s  status=%d", method, url, resp.status_code)
        return _parse_response(resp)

    async def _request_async(
        self,
        method: str,
        path: str,
        json: Any = None,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        timeout: Optional[float] = None,
    ) -> Any:
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
            raise AchillesError(
                message=f"Request timed out: {method} {path}",
                code=ERROR_CONNECTION,
            ) from exc
        except httpx.ConnectError as exc:
            raise AchillesError(
                message=f"Connection failed: {path}",
                code=ERROR_CONNECTION,
            ) from exc
        except httpx.RequestError as exc:
            raise AchillesError(
                message=str(exc),
                code=ERROR_CONNECTION,
            ) from exc

        self.logger.debug("← %s %s  status=%d", method, path, resp.status_code)
        return _parse_response(resp)

    def close(self) -> None:
        """Close the sync session. For async, use `await aclose()`."""
        if self.mode == "sync":
            self._sync_session.close()

    async def aclose(self) -> None:
        """Close the async client."""
        if self.mode == "async":
            await self._async_client.aclose()


class SyncHttpClient(_HTTPClient):
    pass


class AsyncHttpClient(_HTTPClient):
    pass
