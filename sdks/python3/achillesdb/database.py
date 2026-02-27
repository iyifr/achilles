import logging
import threading
from typing import Callable, Literal, Optional, Union

from achillesdb.api.collection import AsyncCollectionApi, SyncCollectionApi
from achillesdb.collection import SyncCollection, AsyncCollection
from achillesdb.http.connection import AsyncHttpClient, SyncHttpClient
from achillesdb.schemas import CreateCollectionReqInput


class DatabaseImpl:
    def __init__(
        self,
        name: str,
        http: Union[SyncHttpClient, AsyncHttpClient],
        embedding_function: Optional[Callable] = None,
        logger: Optional[logging.Logger] = None,
        mode: Literal["sync", "async"] = "sync",
    ):
        self.name = name
        self._http = http
        self._embedding_function = embedding_function
        self._logger = logger
        self._collections = None
        self._collections_mutex = threading.Lock()
        self._mode = mode

        if mode == "async":
            self._collection_api = AsyncCollectionApi(
                self._http, database_name=self.name, logger=self._logger
            )
        else:
            self._collection_api = SyncCollectionApi(
                self._http, database_name=self.name, logger=self._logger
            )

    # sopos to use get_collection to populate collection
    # def _make_collection(self, name):
    #     if self._mode == "async":
    #         return AsyncCollection(
    #             name=name,
    #             http=self._http,
    #             embedding_function=self._embedding_function,
    #             logger=self._logger,
    #         )
    #
    #     return SyncCollection(
    #         name=name,
    #         http=self._http,
    #         embedding_function=self._embedding_function,
    #         logger=self._logger,
    #     )

    def _create_collection(self, name):
        return self._collection_api.create_collection(name)

    def _list_collections(self):
        return self._collection_api.list_collections()

    def _collection(self, name):
        ...

    def _get_collection(self, name):
        return self._collection_api.get_collection(name)

    def _delete_collection(self, name):
        return self._collection_api.delete_collection(name)

    def _query_collections(self, opts=None):
        ...

class AsyncDatabase(DatabaseImpl):
    def __init__(
        self,
        name: str,
        http: Union[SyncHttpClient, AsyncHttpClient],
        embedding_function: Optional[Callable] = None,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(
            name=name,
            http=http,
            embedding_function=embedding_function,
            logger=logger,
            mode="async",
        )

    async def create_collection(self, name):
        input = CreateCollectionReqInput(name=name)
        return await self._create_collection(input)

    async def list_collections(self):
        return await self._list_collections()

    async def get_collection(self, name):
        return await self._get_collection(name)

    async def delete_collection(self, name):
        return await self._delete_collection(name)

    # TODO: implement quering from multiple collections at once
    async def query_collections(self, opts=None):
        return await self._query_collections(opts)

class SyncDatabase(DatabaseImpl):
    def __init__(
        self,
        name: str,
        http: Union[SyncHttpClient, AsyncHttpClient],
        embedding_function: Optional[Callable] = None,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(
            name=name,
            http=http,
            embedding_function=embedding_function,
            logger=logger,
            mode="sync",
        )

    def create_collection(self, name):
        input = CreateCollectionReqInput(name=name)
        return self._create_collection(input)

    def list_collections(self):
        return self._list_collections()

    def get_collection(self, name):
        return self._get_collection(name)

    def delete_collection(self, name):
        return self._delete_collection(name)

    # TODO: implement quering from multiple collections at once
    def query_collections(self, opts=None):
        return self._query_collections(opts)
