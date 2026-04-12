from __future__ import annotations


import asyncio
import threading

from achillesdb.api.database import AsyncDatabaseApi, SyncDatabaseApi
from achillesdb.errors import AchillesError
from achillesdb.schemas import CreateDatabaseRes, DeleteDatabaseRes, GetDatabasesRes
from achillesdb.types import EmbeddingFn

import logging
from typing import Awaitable, Literal, cast

from .config import ConnectionConfig, get_config
from .http.connection import SyncHttpClient, AsyncHttpClient

from .database import SyncDatabase, AsyncDatabase

cfg = get_config()


class _AchillesClient:
    def __init__(
        self,
        host: str = cfg.default_host,
        port: int = cfg.default_port,
        api_base_path: str = cfg.default_api_base_path,
        ssl: bool = cfg.default_ssl,
        embedding_function: EmbeddingFn | None = None,
        timeout: float | None = None,
        max_retries: int = 3,
        connection_config: ConnectionConfig | None = None,
        logger: logging.Logger | None = None,
        mode: Literal["sync", "async"] = "sync",
    ):
        self._host = host
        self._port = port
        self._embedding_function = embedding_function
        self._logger = logger or logging.getLogger(__name__)
        self._mode = mode
        self._db_cache: dict[str, SyncDatabase | AsyncDatabase] = {}
        self._db_cache_lock = threading.Lock()

        http_cls = AsyncHttpClient if mode == "async" else SyncHttpClient
        self._http = http_cls(
            host=host,
            port=port,
            api_base_path=api_base_path,
            ssl=ssl,
            timeout=timeout,
            max_retries=max_retries,
            connection_config=connection_config,
            logger=self._logger,
        )
        if mode == "async":
            self.database_api = AsyncDatabaseApi(self._http, logger=self._logger)  # type: ignore[arg-type]
        else:
            self.database_api = SyncDatabaseApi(self._http, logger=self._logger)  # type: ignore[arg-type, assignment]

    def __str__(self) -> str:
        return f"<{self.__class__.__name__} host={self._host} port={self._port}>"

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} host={self._host} port={self._port}>"

    def _make_database(self, name: str) -> SyncDatabase | AsyncDatabase:
        with self._db_cache_lock:
            if name not in self._db_cache:
                cls = AsyncDatabase if self._mode == "async" else SyncDatabase
                self._db_cache[name] = cls(
                    name=name, http=self._http,
                    embedding_function=self._embedding_function,
                    logger=self._logger,
                )
            return self._db_cache[name]

    def _ping(self) -> bool | Awaitable[bool]:
        if self._mode == "async":
            async def __ping() -> bool:
                try:
                    return True if await self.database_api.list_databases() else False
                except AchillesError as e:
                    self._logger.error(e)
                    return False
            return __ping()  # type: ignore[return-value]
        else:
            try:
                return True if self.database_api.list_databases() else False
            except AchillesError as e:
                self._logger.error(e)
                return False

    def _create_database(self, name: str) -> CreateDatabaseRes:
        return self.database_api.create_database(name)  # type: ignore[return-value]

    def _list_databases(self) -> GetDatabasesRes:
        return self.database_api.list_databases()  # type: ignore[return-value]

    def _database(self, name: str = "default") -> SyncDatabase | AsyncDatabase:
        return self._make_database(name)

    def _delete_database(self, name: str) -> DeleteDatabaseRes:
        return self.database_api.delete_database(name)  # type: ignore[return-value]

    def _close(self):
        if self._mode == "async":
            return self._http.aclose()  # type: ignore[return-value]
        else:
            return self._http.close()


class AchillesClient(_AchillesClient):

    def __init__(
        self,
        host: str = cfg.default_host,
        port: int = cfg.default_port,
        embedding_function: EmbeddingFn | None = None,
        timeout: float | None = None,
        max_retries: int = 3,
        logger: logging.Logger | None = None,
    ):
        super().__init__(
            host=host,
            port=port,
            embedding_function=embedding_function,
            timeout=timeout,
            max_retries=max_retries,
            logger=logger,
            mode="sync",
        )

    def ping(self) -> bool:
        return cast(bool, self._ping())

    def create_database(self, name: str) -> SyncDatabase:
        cast(CreateDatabaseRes, self._create_database(name))
        return self.database(name)

    def list_databases(self) -> list[SyncDatabase]:
        database = self._list_databases()
        return [
            self._make_database(db.name) for db in database.databases
        ]

    def database(self, name: str = "default") -> SyncDatabase:
        return cast(SyncDatabase, self._database(name))

    def delete_database(self, name: str) -> None:
        self._delete_database(name)

    def close(self) -> None:
        self._close()

    def __enter__(self) -> AchillesClient:
        return self

    def __exit__(self, *_) -> None:
        self.close()


class AsyncAchillesClient(_AchillesClient):
    def __init__(
        self,
        host: str = cfg.default_host,
        port: int = cfg.default_port,
        embedding_function: EmbeddingFn | None = None,
        timeout: float | None = None,
        max_retries: int = 3,
        logger: logging.Logger | None = None,
    ):
        super().__init__(
            host=host,
            port=port,
            embedding_function=embedding_function,
            timeout=timeout,
            max_retries=max_retries,
            logger=logger,
            mode="async",
        )

    async def ping(self) -> bool:
        return await cast(Awaitable[bool], self._ping())

    async def create_database(self, name: str) -> AsyncDatabase:
        await cast(Awaitable[CreateDatabaseRes], self._create_database(name))
        return self.database(name)

    async def list_databases(self) -> list[AsyncDatabase]:
        database = await cast(Awaitable[GetDatabasesRes], self._list_databases())
        return [
            self._make_database(db.name) for db in database.databases
        ]

    def database(self, name: str = "default") -> AsyncDatabase:
        return cast(AsyncDatabase, self._database(name))

    async def delete_database(self, name: str) -> None:
        await cast(Awaitable[None], self._delete_database(name))

    async def close(self) -> None:
        await cast(Awaitable[None], self._close())

    async def __aenter__(self) -> AsyncAchillesClient:
        return self

    async def __aexit__(self, *_) -> None:
        await cast(Awaitable[None], self._close())
