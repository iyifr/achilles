from typing import Awaitable, Union


class _ColectionApiBase:
    def __init__(self, http_client):
        self.http_client = http_client


    def _get_collection(self, collection_name: str) -> Union[None, Awaitable[None]]:
        pass

    def _create_collection(self, collection_name: str):
        pass

    def _delete_collection(self, collection_name: str):
        pass

    def _list_collections(self):
        pass


class AsyncCollectionApi(_ColectionApiBase):
    async def get_collection(self, collection_name: str):
        return await self._get_collection(collection_name)

    async def create_collection(self, collection_name: str):
        return await self._create_collection(collection_name)

    async def delete_collection(self, collection_name: str):
        return await self._delete_collection(collection_name)

    async def list_collections(self):
        return await self._list_collections()


class SyncCollectionApi(_ColectionApiBase):
    def get_collection(self, collection_name: str):
        return self._get_collection(collection_name)

    def create_collection(self, collection_name: str):
        return self._create_collection(collection_name)

    def delete_collection(self, collection_name: str):
        return self._delete_collection(collection_name)

    def list_collections(self):
        return self._list_collections()
