from __future__ import annotations

import asyncio
import logging
import threading
from typing import Any, Awaitable, Callable, Literal, cast

from achillesdb.api.collection import AsyncCollectionApi, SyncCollectionApi
from achillesdb.collection import SyncCollection, AsyncCollection
from achillesdb.errors import ERROR_INVALID_RESPONSE, AchillesError
from achillesdb.http.connection import AsyncHttpClient, SyncHttpClient
from achillesdb.schemas import (
    CreateCollectionReqInput, CreateCollectionRes,
    DeleteCollectionRes, Document,
    GetCollectionRes, GetCollectionsRes,
    QueryRes, WhereClause,
)


class DatabaseImpl:
    def __init__(
        self,
        name: str,
        http: SyncHttpClient | AsyncHttpClient,
        embedding_function: Callable | None = None,
        logger: logging.Logger | None = None,
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
            self._collection_api: AsyncCollectionApi | SyncCollectionApi = AsyncCollectionApi(
                cast(AsyncHttpClient, self._http), database_name=self.name, logger=self._logger
            )
        else:
            self._collection_api = SyncCollectionApi(
                cast(SyncHttpClient, self._http), database_name=self.name, logger=self._logger
            )

    # sopos to use get_collection to populate collection
    def _make_collection(self, name: str, id: str) -> SyncCollection | AsyncCollection:
        if self._mode == "async":
            return AsyncCollection(
                id=id,
                name=name,
                database=self.name,
                http_client=self._http,
                embedding_function=self._embedding_function,
                logger=self._logger,
            )

        return SyncCollection(
            id=id,
            name=name,
            database=self.name,
            http_client=self._http,
            embedding_function=self._embedding_function,
            logger=self._logger,
        )

    def _create_collection(
        self, name: CreateCollectionReqInput
    ) -> CreateCollectionRes | Awaitable[CreateCollectionRes]:
        return self._collection_api.create_collection(name)

    def _list_collections(self) -> GetCollectionsRes | Awaitable[GetCollectionsRes]:
        return self._collection_api.list_collections()

    def _collection(self, name: str) -> SyncCollection | AsyncCollection | Awaitable[AsyncCollection]:
        if self._mode == "async":
            async def _coll(name: str) -> AsyncCollection:
                collection = await cast(Awaitable[GetCollectionRes], self._get_collection(name))
                return cast(AsyncCollection, self._make_collection(
                    name=name,
                    id=collection.collection.id,
                ))
            return _coll(name)
        else:
            collection = cast(GetCollectionRes, self._get_collection(name))
            return self._make_collection(
                name=name,
                id=collection.collection.id,
            )

    def _get_collection(self, name: str) -> GetCollectionRes | Awaitable[GetCollectionRes]:
        return self._collection_api.get_collection(name)

    def _delete_collection(self, name: str) -> DeleteCollectionRes | Awaitable[DeleteCollectionRes]:
        return self._collection_api.delete_collection(name)

    def _query_collections(
        self,
        collection_names: list[str],
        query: str | None,
        query_embedding: list[float] | None,
        top_k: int,
        where: dict[str, Any] | WhereClause | None,
    ) -> list[Document] | Awaitable[list[Document]]:
        if isinstance(where, dict):
            where = WhereClause(**where)
        # NOTE: API: need endpoint to get multiple collections details
        try:
            collections = [
                self._collection(name) for name in collection_names
            ]
        except Exception as e:
            raise AchillesError(
                message=f"Failed to get collections: {e}",
                code=ERROR_INVALID_RESPONSE,
            ) from e

        if self._mode == "async":
            async def _aquery_collections() -> list[Document]:
                _collections = await asyncio.gather(
                    *cast(list[Awaitable[AsyncCollection]], collections)
                )
                # get the results from each collection
                results = [
                    collection.query_documents(query, query_embedding, top_k, where)
                    for collection in _collections
                ]
                results: list[QueryRes] = await asyncio.gather(
                    *cast(list[Awaitable[QueryRes]], results)
                )
                docs = [doc for docs in results for doc in docs.documents]

                # slice the results and rerank
                return sorted(docs, key=lambda d: d.distance)[:top_k]
            return _aquery_collections()

        # get the results from each collection
        results: list[QueryRes] = [
            collection.query_documents(query, query_embedding, top_k, where)
            for collection in cast(list[SyncCollection], collections)
        ]
        docs = [doc for docs in results for doc in docs.documents]
        return sorted(docs, key=lambda d: d.distance)[:top_k]


class AsyncDatabase(DatabaseImpl):
    def __init__(
        self,
        name: str,
        http: SyncHttpClient | AsyncHttpClient,
        embedding_function: Callable | None = None,
        logger: logging.Logger | None = None,
    ):
        super().__init__(
            name=name,
            http=http,
            embedding_function=embedding_function,
            logger=logger,
            mode="async",
        )

    async def create_collection(self, name: str) -> CreateCollectionRes:
        _input = CreateCollectionReqInput(name=name)
        return await cast(Awaitable[CreateCollectionRes], self._create_collection(_input))

    async def list_collections(self) -> GetCollectionsRes:
        return await cast(Awaitable[GetCollectionsRes], self._list_collections())

    async def get_collection(self, name: str) -> GetCollectionRes:
        return await cast(Awaitable[GetCollectionRes], self._get_collection(name))

    async def delete_collection(self, name: str) -> DeleteCollectionRes:
        return await cast(Awaitable[DeleteCollectionRes], self._delete_collection(name))

    # TODO: implement quering from multiple collections at once
    # NOTE: API: no direct endpoint to handle this
    async def query_collections(
        self,
        collection_names: list[str],
        query: str | None,
        query_embedding: list[float] | None,
        top_k: int,
        where: dict[str, Any] | WhereClause | None,
    ) -> list[Document]:
        return await cast(Awaitable[list[Document]], self._query_collections(
            collection_names=collection_names,
            query=query,
            query_embedding=query_embedding,
            top_k=top_k,
            where=where,
        ))

    async def collection(self, name: str) -> AsyncCollection:
        return await cast(Awaitable[AsyncCollection], self._collection(name))


class SyncDatabase(DatabaseImpl):
    def __init__(
        self,
        name: str,
        http: SyncHttpClient | AsyncHttpClient,
        embedding_function: Callable | None = None,
        logger: logging.Logger | None = None,
    ):
        super().__init__(
            name=name,
            http=http,
            embedding_function=embedding_function,
            logger=logger,
            mode="sync",
        )

    def create_collection(self, name: str) -> CreateCollectionRes:
        _input = CreateCollectionReqInput(name=name)
        return cast(CreateCollectionRes, self._create_collection(_input))

    def list_collections(self) -> GetCollectionsRes:
        return cast(GetCollectionsRes, self._list_collections())

    def get_collection(self, name: str) -> GetCollectionRes:
        return cast(GetCollectionRes, self._get_collection(name))

    def delete_collection(self, name: str) -> DeleteCollectionRes:
        return cast(DeleteCollectionRes, self._delete_collection(name))

    # TODO: implement quering from multiple collections at once
    def query_collections(
        self,
        collection_names: list[str],
        query: str | None,
        query_embedding: list[float] | None,
        top_k: int,
        where: dict[str, Any] | WhereClause | None,
    ) -> list[Document]:
        return cast(list[Document], self._query_collections(
            collection_names=collection_names,
            query=query,
            query_embedding=query_embedding,
            top_k=top_k,
            where=where,
        ))

    def collection(self, name: str) -> SyncCollection:
        return cast(SyncCollection, self._collection(name))
