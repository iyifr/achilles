from __future__ import annotations

from achillesdb.schemas import CreateDatabaseRes, GetDatabasesRes, MessageResponse

import logging
from typing import Awaitable, Literal, Optional, Union

from ..config import get_config
from ..http.connection import SyncHttpClient, AsyncHttpClient
from ..util import validate_name


cfg = get_config()


class _DatabaseApi:
    def __init__(
        self,
        http_client: Union[SyncHttpClient, AsyncHttpClient],
        logger: Optional[logging.Logger] = None,
        mode: Literal["sync", "async"] = "sync",
    ):
        self._logger = logger or logging.getLogger(__name__)
        self._mode = mode
        self._http = http_client

    def _create_database(self, name: str):
        validate_name(name, "Database name")
        return self._http.post(
            "/database",
            json={"name": name},
            resType=CreateDatabaseRes,
            expected_status=200,
        )

    def _list_databases(self) -> GetDatabasesRes | Awaitable[GetDatabasesRes]:
        return self._http.get(
            "/databases",
            GetDatabasesRes,
            expected_status=200,
        )

    def _delete_database(self, name: str) -> MessageResponse:
        validate_name(name, "Database name")
        return self._http.delete(
            f"/database/{name}",
            MessageResponse,
            expected_status=200,
        )


class SyncDatabaseApi(_DatabaseApi):

    def __init__(
        self,
        http_client: SyncHttpClient,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(
            http_client=http_client,
            logger=logger,
            mode="sync"
        )

    def create_database(self, name: str):
        return self._create_database(name)

    def list_databases(self) -> GetDatabasesRes:
        return self._list_databases()

    def delete_database(self, name: str) -> None:
        self._delete_database(name)


class AsyncDatabaseApi(_DatabaseApi):

    def __init__(
        self, http_client: AsyncHttpClient,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(
            http_client=http_client,
            logger=logger, mode="async"
        )

    async def create_database(self, name: str):
        return await self._create_database(name)

    async def list_databases(self):
        return await self._list_databases()

    async def delete_database(self, name: str) -> None:
        await self._delete_database(name)
