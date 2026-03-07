import logging
from typing import Any, Awaitable, Callable, List, Literal, Optional, Union, cast

import uuid

from achillesdb.api.collection import AsyncCollectionApi, SyncCollectionApi
from achillesdb.api.document import AsyncDocumentApi, SyncDocumentApi
from achillesdb.errors import ERROR_VALIDATION, AchillesError
from achillesdb.http.connection import AsyncHttpClient, SyncHttpClient
from achillesdb.schemas import DeleteDocumentsReqInput, GetDocumentsRes, InsertDocumentReqInput, InsertDocumentsRes, QueryReqInput, UpdateDocumentsReqInput, WhereClause
from achillesdb.types import EmbeddingFn


class CollectionImpl:
    def __init__(
        self,
        id,
        name: str,
        database: str,
        http_client: Union[SyncHttpClient, AsyncHttpClient],
        embedding_function: Optional[EmbeddingFn] = None,
        logger: Optional[logging.Logger] = None,
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
            self._documents_api = AsyncDocumentApi(
                self._http_client, self.database, self.name, logger=self.logger
            )
            self._collection_api = AsyncCollectionApi(
                self._http_client, database_name=self.database, logger=self.logger
            )
        else:
            self._documents_api = SyncDocumentApi(
                self._http_client, self.database, self.name, logger=self.logger
            )
            self._collection_api = SyncCollectionApi(
                self._http_client, database_name=self.database, logger=self.logger
            )

    def _count(self):
        if self.mode == "async":
            async def _count():
                result = await self._collection_api.get_collection(self.name)
                return result.stats.doc_count
            return _count()
        else:
            return self._collection_api.get_collection(self.name).stats.doc_count


    # TODO: implement doc embedding
    def _add_documents(
        self,
        ids: list[str],
        documents: list[str],
        embeddings: Optional[list[list[float]]],
        metadatas: Optional[list[dict[str, Any]]],
        before_insert: Optional[Callable[[List[str]], List[str]]] = None,
    ):
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
                        metadatas=metadatas,
                    )
                    return await cast(
                        Awaitable[InsertDocumentsRes],
                        self._documents_api.insert_documents(docs_data)
                    )
                return _get_docs()
            embeddings = cast(list[list[float]], _embeddings)
        docs_data = InsertDocumentReqInput(
            ids=ids,
            documents=before_insert(documents) if before_insert else documents,
            embeddings=embeddings,
            metadatas=metadatas,
        )
        return self._documents_api.insert_documents(docs_data)

    def _get_documents(self):
        return self._documents_api.get_documents()

    def _update_document(
        self,
        document_id: str,
        where: dict[str, Any],
        updates: dict[str, Any],
    ):
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
    ):
        # TODO: API: implement partial delete handling: need to return deleted ids
        # and maybe retry deleting the rest of the ids
        return self._documents_api.delete_documents(
            DeleteDocumentsReqInput(
                document_ids=document_ids,
            )
        )

    def _query(
        self, query: Optional[str], query_embedding: Optional[List[float]],
        top_k: int, where: dict
    ):
        if query_embedding is None:
            if query is None or self.embedding_function is None:
                raise AchillesError(
                    "Provide query_embedding or set embedding_function on the client",
                    code=ERROR_VALIDATION,
                )
            # TODO: set default embedding function
            if self.mode == "async":
                async def _get_embeddings():
                    embeddings = await self.embedding_function(query)
                    embeddings = embeddings[0]

                    return QueryReqInput(
                        query_embedding=embeddings, top_k=top_k, where=where
                    )

                return _get_embeddings()
            else:
                query_embedding = self.embedding_function([query])[0]
        return self._documents_api.query_documents(
            QueryReqInput(
                query_embedding=query_embedding, top_k=top_k, where=where
            )
        )

    def _peek(self, n: int = 5):
        results: GetDocumentsRes = self._get_documents()
        return results.documents[:n]


class SyncCollection(CollectionImpl):
    def __init__(
        self,
        id,
        name: str,
        database: str,
        http_client: Union[SyncHttpClient, AsyncHttpClient],
        embedding_function: Optional[Callable] = None,
        logger: Optional[logging.Logger] = None,
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

    def count(self):
        return self._count()

    def add_documents(
        self,
        ids: list[str],
        documents: list[str],
        embeddings: Optional[list[list[float]]],
        metadatas: list[dict[str, Any]],
        before_insert: Optional[Callable[[List[str]], List[str]]] = None,
    ):
        return self._add_documents(
            ids, documents, embeddings, metadatas, before_insert
        )

    def get_documents(self):
        return self._get_documents()

    def update_documents(
        self,
        document_id: str,
        where: dict[str, Any],
        updates: dict[str, Any],
    ):
        return self._update_document(
            document_id, where, updates
        )

    def delete_documents(
        self,
        document_ids: list[str],
    ):
        return self._delete_documents(
            document_ids
        )

    def query_documents(
        self,
        query: str,
        query_embedding: Optional[List[float]],
        top_k: int,
        where: Optional[Union[dict[str, Any], WhereClause]],
    ):
        if isinstance(where, dict):
            where = WhereClause(**where)
        return self._query(
            query, query_embedding, top_k, where
        )

    def peek(self, n: int = 5):
        return self._peek(n)


class AsyncCollection(CollectionImpl):
    def __init__(
        self,
        id,
        name: str,
        database: str,
        http_client: Union[SyncHttpClient, AsyncHttpClient],
        embedding_function: Optional[Callable] = None,
        logger: Optional[logging.Logger] = None,
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
    async def count(self):
        return await self._count()

    async def add_documents(
        self,
        ids: list[str],
        documents: list[str],
        embeddings: Optional[list[list[float]]],
        metadatas: list[dict[str, Any]],
        before_insert: Optional[Callable[[List[str]], List[str]]] = None,
    ):
        return await self._add_documents(
            ids, documents, embeddings, metadatas, before_insert
        )

    async def get_documents(self):
        return await self._get_documents()

    async def update_documents(
        self,
        document_id: str,
        where: dict[str, Any],
        updates: dict[str, Any],
    ):
        return await self._update_document(
            document_id, where, updates
        )

    async def delete_documents(
        self,
        document_ids: list[str],
    ):
        return await self._delete_documents(
            document_ids
        )

    async def query_documents(
        self,
        query: str,
        query_embedding: Optional[List[float]],
        top_k: int,
        where: dict[str, Any],
    ):
        return await self._query(
            query, query_embedding, top_k, where
        )

    async def peek(self, n: int = 5):
        return await self._peek(n)
