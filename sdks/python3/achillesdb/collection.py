import logging
from typing import Any, Callable, Literal, Optional, Union

from achillesdb.api.collection import AsyncCollectionApi, SyncCollectionApi
from achillesdb.api.document import AsyncDocumentApi, SyncDocumentApi
from achillesdb.http.connection import AsyncHttpClient, SyncHttpClient
from achillesdb.schemas import DeleteDocumentsReqInput, InsertDocumentReqInput, QueryReqInput, UpdateDocumentsReqInput


class CollectionImpl:
    def __init__(
        self,
        id,
        name: str,
        database: str,
        http_client: Union[SyncHttpClient, AsyncHttpClient],
        embedding_function: Optional[Callable] = None,
        logger: Optional[logging.Logger] = None,
        mode: Literal["sync", "async"] = "sync",
    ):
        self.id = id
        self.name = name
        self.database = database
        self._http_client = http_client
        self.embedding_function = embedding_function
        self.logger = logger

        if mode == "async":
            self._documents_api = AsyncDocumentApi(
                self._http_client, self.database, self.name, logger=self.logger
            )
            self._collection_api = AsyncCollectionApi(
                self._http_client, database_name=self.name, logger=self.logger
            )
        else:
            self._documents_api = SyncDocumentApi(
                self._http_client, self.database, self.name, logger=self.logger
            )
            self._collection_api = SyncCollectionApi(
                self._http_client, database_name=self.name, logger=self.logger
            )

    def _count(self):
        return self._collection_api.get_collection(self.name).stats.doc_count


    # TODO: implement doc embedding
    def _add_documents(
        self,
        ids: list[str],
        documents: list[str],
        embeddings: list[list[float]],
        metadatas: list[dict[str, Any]],
        before_insert: Optional[Callable] = None,
    ):
        # TODO: review error handling
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
        return self._documents_api.delete_documents(
            DeleteDocumentsReqInput(
                document_ids=document_ids,
            )
        )

    def _query(
        self,
        query: str,
        top_k: int,
        where: dict[str, Any],
    ):
        return self._documents_api.query_documents(
            QueryReqInput(
                query=query,
                top_k=top_k,
                where=where,
            )
        )

    def _peek(self, n: int = 5):
        return self._get_documents()[:n]


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
        embeddings: list[list[float]],
        metadatas: list[dict[str, Any]],
        before_insert: Optional[Callable] = None,
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
        top_k: int,
        where: dict[str, Any],
    ):
        return self._query(
            query, top_k, where
        )

    def peek(self, n: int = 5):
        return self._peek(n)


class AsyncCollection(CollectionImpl):
    async def count(self):
        return await  self._count()

    async def add_documents(
        self,
        ids: list[str],
        documents: list[str],
        embeddings: list[list[float]],
        metadatas: list[dict[str, Any]],
        before_insert: Optional[Callable] = None,
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
        top_k: int,
        where: dict[str, Any],
    ):
        return await self._query(
            query, top_k, where
        )

    async def peek(self, n: int = 5):
        return await self._peek(n)
