import logging
from typing import Awaitable, Literal, Optional, Union

from achillesdb.http.connection import AsyncHttpClient, SyncHttpClient
from achillesdb.schemas import CreateCollectionReqInput, CreateCollectionRes, DeleteCollectionRes, GetCollectionRes, GetCollectionsRes
from achillesdb.validators import validate_name


class _CollectionApiBase:
    def __init__(
        self,
        http_client: Union[SyncHttpClient, AsyncHttpClient],
        database_name: str,
        logger: Optional[logging.Logger] = None,
        mode: Literal["sync", "async"] = "sync",
    ):
        self._logger = logger or logging.getLogger(__name__)
        self._mode = mode
        self._http = http_client
        self._database_name = database_name

    def _get_collection(self, collection_name: str) -> Union[None, Awaitable[None]]:
        validate_name(collection_name, "Collection name")
        return self._http.get(
            f"/database/{self._database_name}/collections/{collection_name}",
            GetCollectionRes,
            expected_status=200,
        )

    def _create_collection(self, input: CreateCollectionReqInput):
        validate_name(input.name, "Collection name")
        return self._http.post(
            f"/database/{self._database_name}/collections",
            CreateCollectionRes,
            json=input.model_dump(exclude_unset=True),
            expected_status=200,
        )

    # BUG: API: there is a problem with the deletion of collections
    # when a collection is deleted and then you try to create it again,
    # it says that the collection exists but does not create the collection
    def _delete_collection(self, collection_name: str):
        validate_name(collection_name, "Collection name")
        return self._http.delete(
            f"/database/{self._database_name}/collections/{collection_name}",
            DeleteCollectionRes,
            expected_status=200,
        )

    def _list_collections(self):
        return self._http.get(
            f"/database/{self._database_name}/collections",
            GetCollectionsRes,
            expected_status=200,
        )


class AsyncCollectionApi(_CollectionApiBase):
    def __init__(
        self,
        http_client: AsyncHttpClient,
        database_name: str,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(
            http_client=http_client,
            database_name=database_name,
            logger=logger,
            mode="async"
        )
    async def create_collection(
        self,
        input: CreateCollectionReqInput
    ):
        return await self._create_collection(
            input
        )

    async def delete_collection(self, collection_name: str):
        return await self._delete_collection(collection_name)

    async def list_collections(self):
        return await self._list_collections()

    async def get_collection(self, collection_name: str):
        return await self._get_collection(collection_name)


class SyncCollectionApi(_CollectionApiBase):
    def __init__(
        self,
        http_client: SyncHttpClient,
        database_name: str,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(
            http_client=http_client,
            database_name=database_name,
            logger=logger,
            mode="sync"
        )

    def create_collection(
        self, input: CreateCollectionReqInput
    ):
        return self._create_collection(
            input
        )

    def delete_collection(self, collection_name: str):
        return self._delete_collection(collection_name)

    def list_collections(self):
        return self._list_collections()

    def get_collection(self, collection_name: str):
        return self._get_collection(collection_name)
