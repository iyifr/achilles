import logging
from typing import Literal, Optional, Union

from achillesdb.http.connection import AsyncHttpClient, SyncHttpClient
from achillesdb.schemas import DeleteDocumentsReqInput, DeleteDocumentsRes, GetDocumentsRes, InsertDocumentReqInput, InsertDocumentsRes, QueryReqInput, QueryRes, UpdateDocumentsReqInput, UpdateDocumentsRes
from achillesdb.validators import validate_name


class _DocumentApiBase:
    def __init__(
        self,
        http_client: Union[SyncHttpClient, AsyncHttpClient],
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

    def _get_documents(self):
        return self._http.get(
            f"/database/{self._database_name}/collections/{self._collection_name}/documents",
            GetDocumentsRes,
            expected_status=200,
        )

    def _insert_documents(self, input: InsertDocumentReqInput):
        return self._http.post(
            f"/database/{self._database_name}/collections/{self._collection_name}/documents",
            InsertDocumentsRes,
            json=input.model_dump(exclude_unset=True),
            expected_status=200,
        )

    def _update_documents(self, input: UpdateDocumentsReqInput):
        return self._http.put(
            f"/database/{self._database_name}/collections/{self._collection_name}/documents",
            UpdateDocumentsRes,
            json=input.model_dump(exclude_unset=True),
            expected_status=200,
        )

    def _delete_documents(self, input: DeleteDocumentsReqInput):
        return self._http.delete(
            f"/database/{self._database_name}/collections/{self._collection_name}/documents",
            DeleteDocumentsRes,
            json=input.model_dump(exclude_unset=True),
            expected_status=200,
        )

    def _query_documents(self, input: QueryReqInput):
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

    def get_documents(self):
        return self._get_documents()

    def insert_documents(self, input: InsertDocumentReqInput):
        return self._insert_documents(input)

    def update_documents(self, input: UpdateDocumentsReqInput):
        return self._update_documents(input)

    def delete_documents(self, input: DeleteDocumentsReqInput):
        return self._delete_documents(input)

    def query_documents(self, input: QueryReqInput):
        return self._query_documents(input)


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

    async def get_documents(self):
        return await self._get_documents()

    async def insert_documents(self, input: InsertDocumentReqInput):
        return await self._insert_documents(input)

    async def update_documents(self, input: UpdateDocumentsReqInput):
        return await self._update_documents(input)

    async def delete_documents(self, input: DeleteDocumentsReqInput):
        return await self._delete_documents(input)

    async def query_documents(self, input: QueryReqInput):
        return await self._query_documents(input)
