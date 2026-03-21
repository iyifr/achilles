from __future__ import annotations

from achillesdb.schemas import CreateDatabaseRes, DeleteDatabaseRes, GetDatabasesRes

import logging
from typing import Awaitable, Literal, cast

from ..config import get_config
from ..http.connection import SyncHttpClient, AsyncHttpClient
from ..validators import validate_name


cfg = get_config()


class _DatabaseApi:
    def __init__(
        self,
        http_client: SyncHttpClient | AsyncHttpClient,
        logger: logging.Logger | None = None,
        mode: Literal["sync", "async"] = "sync",
    ):
        self._logger = logger or logging.getLogger(__name__)
        self._mode = mode
        self._http = http_client

    def _create_database(self, name: str) -> CreateDatabaseRes | Awaitable[CreateDatabaseRes]:
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

    def _delete_database(self, name: str) -> DeleteDatabaseRes | Awaitable[DeleteDatabaseRes]:
        validate_name(name, "Database name")
        return self._http.delete(
            f"/database/{name}",
            DeleteDatabaseRes,
            expected_status=200,
        )


class SyncDatabaseApi(_DatabaseApi):

    def __init__(
        self,
        http_client: SyncHttpClient,
        logger: logging.Logger | None = None,
    ):
        super().__init__(
            http_client=http_client,
            logger=logger,
            mode="sync"
        )

    def create_database(self, name: str) -> CreateDatabaseRes:
        return cast(CreateDatabaseRes, self._create_database(name))

    def list_databases(self) -> GetDatabasesRes:
        return cast(GetDatabasesRes, self._list_databases())

    def delete_database(self, name: str) -> None:
        self._delete_database(name)


class AsyncDatabaseApi(_DatabaseApi):

    def __init__(
        self, http_client: AsyncHttpClient,
        logger: logging.Logger | None = None,
    ):
        super().__init__(
            http_client=http_client,
            logger=logger, mode="async"
        )

    async def create_database(self, name: str) -> CreateDatabaseRes:
        return await cast(Awaitable[CreateDatabaseRes], self._create_database(name))

    async def list_databases(self) -> GetDatabasesRes:
        return await cast(Awaitable[GetDatabasesRes], self._list_databases())

    async def delete_database(self, name: str) -> DeleteDatabaseRes:
        return await cast(Awaitable[DeleteDatabaseRes], self._delete_database(name))
