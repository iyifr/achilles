from __future__ import annotations

import logging
from typing import Awaitable, Literal, Optional, cast

from achillesdb.http.connection import AsyncHttpClient, SyncHttpClient
from achillesdb.schemas import (
    DeleteDocumentsReqInput, DeleteDocumentsRes,
    GetDocumentsRes,
    InsertDocumentReqInput, InsertDocumentsRes,
    QueryReqInput, QueryRes,
    UpdateDocumentsReqInput, UpdateDocumentsRes,
)


class _DocumentApiBase:
    def __init__(
        self,
        http_client: SyncHttpClient | AsyncHttpClient,
        database_name: str,
        collection_name: str,
        logger: Optional[logging.Logger] = None,
        mode: Literal["sync", "async"] = "sync",
    ):
        self._logger = logger or logging.getLogger(__name__)
        self._mode = mode
        self._http = http_client
        self._database_name = database_name
        self._collection_name = collection_name

    def _get_documents(self) -> GetDocumentsRes | Awaitable[GetDocumentsRes]:
        return self._http.get(
            f"/database/{self._database_name}/collections/{self._collection_name}/documents",
            GetDocumentsRes,
            expected_status=200,
        )

    def _insert_documents(self, input: InsertDocumentReqInput) -> InsertDocumentsRes | Awaitable[InsertDocumentsRes]:
        return self._http.post(
            f"/database/{self._database_name}/collections/{self._collection_name}/documents",
            InsertDocumentsRes,
            json=input.model_dump(exclude_unset=True),
            expected_status=200,
        )

    def _update_documents(self, input: UpdateDocumentsReqInput) -> UpdateDocumentsRes | Awaitable[UpdateDocumentsRes]:
        return self._http.put(
            f"/database/{self._database_name}/collections/{self._collection_name}/documents",
            UpdateDocumentsRes,
            json=input.model_dump(exclude_unset=True),
            expected_status=200,
        )

    def _delete_documents(self, input: DeleteDocumentsReqInput) -> DeleteDocumentsRes | Awaitable[DeleteDocumentsRes]:
        return self._http.delete(
            f"/database/{self._database_name}/collections/{self._collection_name}/documents",
            DeleteDocumentsRes,
            json=input.model_dump(exclude_unset=True),
            expected_status=200,
        )

    def _query(self, input: QueryReqInput) -> QueryRes | Awaitable[QueryRes]:
        return self._http.post(
            f"/database/{self._database_name}/collections/{self._collection_name}/documents/query",
            QueryRes,
            json=input.model_dump(exclude_unset=True),
            expected_status=200,
        )


class SyncDocumentApi(_DocumentApiBase):
    def __init__(
        self,
        http_client: SyncHttpClient,
        database_name: str,
        collection_name: str,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(
            http_client=http_client,
            database_name=database_name,
            collection_name=collection_name,
            logger=logger,
            mode="sync",
        )

    def get_documents(self) -> GetDocumentsRes:
        return cast(GetDocumentsRes, self._get_documents())

    def insert_documents(self, input: InsertDocumentReqInput) -> InsertDocumentsRes:
        return cast(InsertDocumentsRes, self._insert_documents(input))

    def update_documents(self, input: UpdateDocumentsReqInput) -> UpdateDocumentsRes:
        return cast(UpdateDocumentsRes, self._update_documents(input))

    def delete_documents(self, input: DeleteDocumentsReqInput) -> DeleteDocumentsRes:
        return cast(DeleteDocumentsRes, self._delete_documents(input))

    def query(self, input: QueryReqInput) -> QueryRes:
        return cast(QueryRes, self._query(input))


class AsyncDocumentApi(_DocumentApiBase):
    def __init__(
        self,
        http_client: AsyncHttpClient,
        database_name: str,
        collection_name: str,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(
            http_client=http_client,
            database_name=database_name,
            collection_name=collection_name,
            logger=logger,
            mode="async",
        )

    async def get_documents(self) -> GetDocumentsRes:
        return await cast(Awaitable[GetDocumentsRes], self._get_documents())

    async def insert_documents(self, input: InsertDocumentReqInput) -> InsertDocumentsRes:
        return await cast(Awaitable[InsertDocumentsRes], self._insert_documents(input))

    async def update_documents(self, input: UpdateDocumentsReqInput) -> UpdateDocumentsRes:
        return await cast(Awaitable[UpdateDocumentsRes], self._update_documents(input))

    async def delete_documents(self, input: DeleteDocumentsReqInput) -> DeleteDocumentsRes:
        return await cast(Awaitable[DeleteDocumentsRes], self._delete_documents(input))

    async def query(self, input: QueryReqInput) -> QueryRes:
        return await cast(Awaitable[QueryRes], self._query(input))
