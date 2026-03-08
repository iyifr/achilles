from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable, Literal, cast


from achillesdb.api.collection import AsyncCollectionApi, SyncCollectionApi
from achillesdb.api.document import AsyncDocumentApi, SyncDocumentApi
from achillesdb.errors import ERROR_VALIDATION, AchillesError
from achillesdb.http.connection import AsyncHttpClient, SyncHttpClient
from achillesdb.schemas import (
    DeleteDocumentsReqInput, DeleteDocumentsRes,
    Document, GetCollectionRes, GetDocumentsRes,
    InsertDocumentReqInput, InsertDocumentsRes,
    QueryReqInput, QueryRes,
    UpdateDocumentsReqInput, UpdateDocumentsRes,
    WhereClause,
)
from achillesdb.types import EmbeddingFn


class CollectionImpl:
    def __init__(
        self,
        id: str,
        name: str,
        database: str,
        http_client: SyncHttpClient | AsyncHttpClient,
        embedding_function: EmbeddingFn | None = None,
        logger: logging.Logger | None = None,
        mode: Literal["sync", "async"] = "sync",
    ):
        self.id = id
        self.name = name
        self.database = database
        self._http_client = http_client
        self.embedding_function = embedding_function
        self.logger = logger
        self.mode = mode

        if self.mode == "async":
            self._documents_api: AsyncDocumentApi | SyncDocumentApi = AsyncDocumentApi(
                cast(AsyncHttpClient, self._http_client), self.database, self.name, logger=self.logger
            )
            self._collection_api: AsyncCollectionApi | SyncCollectionApi = AsyncCollectionApi(
                cast(AsyncHttpClient, self._http_client), database_name=self.database, logger=self.logger
            )
        else:
            self._documents_api = SyncDocumentApi(
                cast(SyncHttpClient, self._http_client), self.database, self.name, logger=self.logger
            )
            self._collection_api = SyncCollectionApi(
                cast(SyncHttpClient, self._http_client), database_name=self.database, logger=self.logger
            )

    def _count(self) -> int | Awaitable[int]:
        if self.mode == "async":
            async def _count() -> int:
                result = await self._collection_api.get_collection(self.name)  # type: ignore[misc]
                return result.stats.doc_count
            return _count()
        else:
            res = self._collection_api.get_collection(self.name)
            assert isinstance(res, GetCollectionRes)
            return res.stats.doc_count

    # TODO: implement doc embedding
    def _add_documents(
        self,
        ids: list[str],
        documents: list[str],
        embeddings: list[list[float]] | None,
        metadatas: list[dict[str, Any]] | None,
        before_insert: Callable[[list[str]], list[str]] | None = None,
    ) -> InsertDocumentsRes | Awaitable[InsertDocumentsRes]:
        # TODO: review error handling
        # TODO: implement doc embedding
        # FIX: API: the endpoint seems to be accepting duplicate ids

        if embeddings is None:
            if self.embedding_function is None:
                raise AchillesError(
                    "Either provide embeddings or set embedding_function on the client",
                    code=ERROR_VALIDATION,
                )
            _embeddings = self.embedding_function(documents)
            if self.mode == "async":
                async def _get_docs():
                    docs_data = InsertDocumentReqInput(
                        ids=ids,
                        documents=before_insert(documents) if before_insert else documents,
                        embeddings=await cast(Awaitable[list[list[float]]], _embeddings),
                        metadatas=metadatas or [],
                    )
                    return await cast(
                        Awaitable[InsertDocumentsRes],
                        self._documents_api.insert_documents(docs_data)
                    )
                return _get_docs()  # type: ignore[return-value]
            embeddings = cast(list[list[float]], _embeddings)
        docs_data = InsertDocumentReqInput(
            ids=ids,
            documents=before_insert(documents) if before_insert else documents,
            embeddings=embeddings,
            metadatas=metadatas or [],
        )
        return self._documents_api.insert_documents(docs_data)

    def _get_documents(self) -> GetDocumentsRes | Awaitable[GetDocumentsRes]:
        return self._documents_api.get_documents()

    def _update_document(
        self,
        document_id: str,
        where: dict[str, Any],
        updates: dict[str, Any],
    ) -> UpdateDocumentsRes | Awaitable[UpdateDocumentsRes]:
        return self._documents_api.update_documents(
            UpdateDocumentsReqInput(
                document_id=document_id,
                where=where,
                updates=updates,
            )
        )

    def _delete_documents(
        self,
        document_ids: list[str],
    ) -> DeleteDocumentsRes | Awaitable[DeleteDocumentsRes]:
        # TODO: API: implement partial delete handling: need to return deleted ids
        # and maybe retry deleting the rest of the ids
        return self._documents_api.delete_documents(
            DeleteDocumentsReqInput(
                document_ids=document_ids,
            )
        )

    def _query(
        self, query: str | None, query_embedding: list[float] | None,
        top_k: int, where: dict[str, Any] | WhereClause | None
    ) -> QueryRes | Awaitable[QueryRes]:
        if query_embedding is None:
            if query is None or self.embedding_function is None:
                raise AchillesError(
                    "Provide query_embedding or set embedding_function on the client",
                    code=ERROR_VALIDATION,
                )
            # TODO: set default embedding function
            if self.mode == "async":
                async def _get_embeddings():
                    embeddings = await self.embedding_function(query)  # type: ignore[misc]
                    embeddings = embeddings[0]

                    return QueryReqInput(
                        query_embedding=embeddings, top_k=top_k, where=where
                    )

                return _get_embeddings()  # type: ignore[return-value]
            else:
                query_embedding = self.embedding_function([query])[0]  # type: ignore[index]
        return self._documents_api.query_documents(
            QueryReqInput(
                query_embedding=query_embedding, top_k=top_k,
                where=cast(WhereClause | None, where)
            )
        )

    def _peek(self, n: int = 5) -> list[Document]:
        results: GetDocumentsRes = self._get_documents()  # type: ignore[assignment]
        return results.documents[:n]


class SyncCollection(CollectionImpl):
    def __init__(
        self,
        id: str,
        name: str,
        database: str,
        http_client: SyncHttpClient | AsyncHttpClient,
        embedding_function: EmbeddingFn | None = None,
        logger: logging.Logger | None = None,
    ):
        super().__init__(
            id=id,
            name=name,
            database=database,
            http_client=http_client,
            embedding_function=embedding_function,
            logger=logger,
            mode="sync",
        )

    def count(self) -> int:
        return cast(int, self._count())

    def add_documents(
        self,
        ids: list[str],
        documents: list[str],
        embeddings: list[list[float]] | None,
        metadatas: list[dict[str, Any]],
        before_insert: Callable[[list[str]], list[str]] | None = None,
    ) -> InsertDocumentsRes:
        return cast(InsertDocumentsRes, self._add_documents(
            ids, documents, embeddings, metadatas, before_insert
        ))

    def get_documents(self) -> GetDocumentsRes:
        return cast(GetDocumentsRes, self._get_documents())

    def update_documents(
        self,
        document_id: str,
        where: dict[str, Any],
        updates: dict[str, Any],
    ) -> UpdateDocumentsRes:
        return cast(UpdateDocumentsRes, self._update_document(
            document_id, where, updates
        ))

    def delete_documents(
        self,
        document_ids: list[str],
    ) -> DeleteDocumentsRes:
        return cast(DeleteDocumentsRes, self._delete_documents(document_ids))

    def query_documents(
        self,
        query: str | None,
        query_embedding: list[float] | None,
        top_k: int,
        where: dict[str, Any] | WhereClause | None,
    ) -> QueryRes:
        if isinstance(where, dict):
            where = WhereClause(**where)  # type: ignore[arg-type]
        return cast(QueryRes, self._query(
            query, query_embedding, top_k, where
        ))

    def peek(self, n: int = 5) -> list[Document]:
        return self._peek(n)


class AsyncCollection(CollectionImpl):
    def __init__(
        self,
        id: str,
        name: str,
        database: str,
        http_client: SyncHttpClient | AsyncHttpClient,
        embedding_function: EmbeddingFn | None = None,
        logger: logging.Logger | None = None,
    ):
        super().__init__(
            id=id,
            name=name,
            database=database,
            http_client=http_client,
            embedding_function=embedding_function,
            logger=logger,
            mode="async",
        )

    async def count(self) -> int:
        return await cast(Awaitable[int], self._count())

    async def add_documents(
        self,
        ids: list[str],
        documents: list[str],
        embeddings: list[list[float]] | None,
        metadatas: list[dict[str, Any]],
        before_insert: Callable[[list[str]], list[str]] | None = None,
    ) -> InsertDocumentsRes:
        return await cast(Awaitable[InsertDocumentsRes], self._add_documents(
            ids, documents, embeddings, metadatas, before_insert
        ))

    async def get_documents(self) -> GetDocumentsRes:
        return await cast(Awaitable[GetDocumentsRes], self._get_documents())

    async def update_documents(
        self,
        document_id: str,
        where: dict[str, Any],
        updates: dict[str, Any],
    ) -> UpdateDocumentsRes:
        return await cast(Awaitable[UpdateDocumentsRes], self._update_document(
            document_id, where, updates
        ))

    async def delete_documents(
        self,
        document_ids: list[str],
    ) -> DeleteDocumentsRes:
        return await cast(Awaitable[DeleteDocumentsRes], self._delete_documents(document_ids))

    async def query_documents(
        self,
        query: str | None,
        query_embedding: list[float] | None,
        top_k: int,
        where: dict[str, Any] | WhereClause | None,
    ) -> QueryRes:
        return await cast(Awaitable[QueryRes], self._query(
            query, query_embedding, top_k, where
        ))

    async def peek(self, n: int = 5) -> list[Document]:
        return await cast(Awaitable[list[Document]], self._peek(n))
