from __future__ import annotations
from functools import lru_cache

import threading

from achillesdb.api.database import AsyncDatabaseApi, SyncDatabaseApi
from achillesdb.errors import AchillesError
from achillesdb.schemas import GetDatabasesRes, MessageResponse

import logging
from typing import Awaitable, Callable, Literal, Optional, Union

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
        # default_db: Optional[str] = None,
        embedding_function: Optional[Callable] = None,
        timeout: Optional[float] = None,
        connection_config: Optional[ConnectionConfig] = None,
        logger: Optional[logging.Logger] = None,
        mode: Literal["sync", "async"] = "sync",
    ):
        self._host = host
        self._port = port
        # self._default_db = default_db
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
            connection_config=connection_config,
            logger=self._logger,
        )
        if mode == "async":
            self.database_api = AsyncDatabaseApi(self._http, logger=self._logger)
        else:
            self.database_api = SyncDatabaseApi(self._http, logger=self._logger)


    def _make_database(self, name: str):
        with self._db_cache_lock:
            if name not in self._db_cache:
                cls = AsyncDatabase if self._mode == "async" else SyncDatabase
                self._db_cache[name] = cls(
                    name=name, http=self._http,
                    embedding_function=self._embedding_function,
                    logger=self._logger,
                )
            return self._db_cache[name]

    def _ping(self) -> MessageResponse:
        if self._mode == "async":
            async def __ping():
                try:
                    return True if await self.database_api.list_databases() else False
                except AchillesError as e:
                    self._logger.error(e)
                    return False
            return __ping()
        else:
            try:
                return True if self.database_api.list_databases() else False
            except AchillesError as e:
                self._logger.error(e)
                return False

    def _create_database(self, name: str):
        return self.database_api.create_database(name)

    def _list_databases(self) -> Union[GetDatabasesRes, Awaitable[GetDatabasesRes]]:
        return self.database_api.list_databases()

    def _database(self, name: str):
        return self._make_database(name)

    def _delete_database(self, name: str) -> MessageResponse:
        return self.database_api.delete_database(name)

    def _close(self) -> None:
        if self._mode == "async":
            return self._http.aclose()
        else:
            return self._http.close()


class AchillesClient(_AchillesClient):

    def __init__(
        self,
        host: str = cfg.default_host,
        port: int = cfg.default_port,
        # default_db: Optional[str] = None,
        embedding_function: Optional[Callable] = None,
        timeout: Optional[float] = None,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(
            host=host,
            port=port,
            # default_db=default_db,
            embedding_function=embedding_function,
            timeout=timeout,
            logger=logger,
            mode="sync",
        )

    def ping(self) -> bool:
        return self._ping()

    def create_database(self, name: str):
        return self._create_database(name)

    def list_databases(self):
        return self._list_databases()

    def database(self, name: str):
        return self._database(name)

    def delete_database(self, name: str) -> None:
        self._delete_database(name)

    def close(self) -> None:
        self._close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class AsyncAchillesClient(_AchillesClient):
    def __init__(
        self,
        host: str = cfg.default_host,
        port: int = cfg.default_port,
        # default_db: Optional[str] = None,
        embedding_function: Optional[Callable] = None,
        timeout: Optional[float] = None,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(
            host=host,
            port=port,
            # default_db=default_db,
            embedding_function=embedding_function,
            timeout=timeout,
            logger=logger,
            mode="async",
        )

    async def ping(self) -> bool:
        return await self._ping()

    async def create_database(self, name: str):
        return await self._create_database(name)

    async def list_databases(self):
        return await self._list_databases()

    def database(self, name: str):
        return self._database(name)

    async def delete_database(self, name: str) -> None:
        await self._delete_database(name)

    async def close(self) -> None:
        await self._close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self._close()
