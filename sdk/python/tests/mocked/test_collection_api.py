import pytest
from unittest.mock import MagicMock, AsyncMock

from achillesdb.api.collection import SyncCollectionApi, AsyncCollectionApi
from achillesdb.schemas import CreateCollectionReqInput
from achillesdb.errors import AchillesError


DB_NAME = "test_db"
COLL_NAME = "test_collection"


# ─────────────────────────────────────────────────────────────────────────────
# SyncCollectionApi
# ─────────────────────────────────────────────────────────────────────────────

class TestSyncCollectionApi:

    @pytest.fixture
    def api(self, mock_sync_http):
        return SyncCollectionApi(
            http_client=mock_sync_http,
            database_name=DB_NAME,
        )

    def test_get_collection_calls_correct_url(self, api, mock_sync_http, fake_get_collection_res):
        mock_sync_http.get.return_value = fake_get_collection_res
        api.get_collection(COLL_NAME)
        mock_sync_http.get.assert_called_once()
        call_args = mock_sync_http.get.call_args
        assert f"/database/{DB_NAME}/collections/{COLL_NAME}" in call_args[0][0]

    def test_get_collection_returns_response(self, api, mock_sync_http, fake_get_collection_res):
        mock_sync_http.get.return_value = fake_get_collection_res
        result = api.get_collection(COLL_NAME)
        assert result == fake_get_collection_res

    def test_get_collection_validates_name(self, api):
        with pytest.raises(ValueError):
            api.get_collection("")

    def test_get_collection_validates_invalid_name(self, api):
        with pytest.raises(ValueError):
            api.get_collection("bad-name")

    def test_create_collection_calls_correct_url(self, api, mock_sync_http, fake_create_collection_res):
        mock_sync_http.post.return_value = fake_create_collection_res
        api.create_collection(CreateCollectionReqInput(name=COLL_NAME))
        mock_sync_http.post.assert_called_once()
        call_args = mock_sync_http.post.call_args
        assert f"/database/{DB_NAME}/collections" in call_args[0][0]

    def test_create_collection_sends_correct_payload(self, api, mock_sync_http, fake_create_collection_res):
        mock_sync_http.post.return_value = fake_create_collection_res
        api.create_collection(CreateCollectionReqInput(name=COLL_NAME))
        call_kwargs = mock_sync_http.post.call_args[1]
        assert call_kwargs["json"]["name"] == COLL_NAME

    def test_create_collection_validates_name(self, api):
        with pytest.raises(ValueError):
            api.create_collection(CreateCollectionReqInput(name="bad name"))

    def test_delete_collection_calls_correct_url(self, api, mock_sync_http, fake_delete_collection_res):
        mock_sync_http.delete.return_value = fake_delete_collection_res
        api.delete_collection(COLL_NAME)
        mock_sync_http.delete.assert_called_once()
        call_args = mock_sync_http.delete.call_args
        assert f"/database/{DB_NAME}/collections/{COLL_NAME}" in call_args[0][0]

    def test_delete_collection_validates_name(self, api):
        with pytest.raises(ValueError):
            api.delete_collection("")

    def test_list_collections_calls_correct_url(self, api, mock_sync_http, fake_get_collections_res):
        mock_sync_http.get.return_value = fake_get_collections_res
        api.list_collections()
        call_args = mock_sync_http.get.call_args
        assert f"/database/{DB_NAME}/collections" in call_args[0][0]

    def test_list_collections_returns_response(self, api, mock_sync_http, fake_get_collections_res):
        mock_sync_http.get.return_value = fake_get_collections_res
        result = api.list_collections()
        assert result == fake_get_collections_res


# ─────────────────────────────────────────────────────────────────────────────
# AsyncCollectionApi
# ─────────────────────────────────────────────────────────────────────────────

class TestAsyncCollectionApi:

    @pytest.fixture
    def api(self, mock_async_http):
        return AsyncCollectionApi(
            http_client=mock_async_http,
            database_name=DB_NAME,
        )

    @pytest.mark.asyncio
    async def test_get_collection_calls_correct_url(self, api, mock_async_http, fake_get_collection_res):
        mock_async_http.get.return_value = fake_get_collection_res
        await api.get_collection(COLL_NAME)
        mock_async_http.get.assert_called_once()
        call_args = mock_async_http.get.call_args
        assert f"/database/{DB_NAME}/collections/{COLL_NAME}" in call_args[0][0]

    @pytest.mark.asyncio
    async def test_get_collection_validates_name(self, api):
        with pytest.raises(ValueError):
            await api.get_collection("")

    @pytest.mark.asyncio
    async def test_create_collection_calls_correct_url(self, api, mock_async_http, fake_create_collection_res):
        mock_async_http.post.return_value = fake_create_collection_res
        await api.create_collection(CreateCollectionReqInput(name=COLL_NAME))
        call_args = mock_async_http.post.call_args
        assert f"/database/{DB_NAME}/collections" in call_args[0][0]

    @pytest.mark.asyncio
    async def test_create_collection_sends_correct_payload(self, api, mock_async_http, fake_create_collection_res):
        mock_async_http.post.return_value = fake_create_collection_res
        await api.create_collection(CreateCollectionReqInput(name=COLL_NAME))
        call_kwargs = mock_async_http.post.call_args[1]
        assert call_kwargs["json"]["name"] == COLL_NAME

    @pytest.mark.asyncio
    async def test_delete_collection_calls_correct_url(self, api, mock_async_http, fake_delete_collection_res):
        mock_async_http.delete.return_value = fake_delete_collection_res
        await api.delete_collection(COLL_NAME)
        call_args = mock_async_http.delete.call_args
        assert f"/database/{DB_NAME}/collections/{COLL_NAME}" in call_args[0][0]

    @pytest.mark.asyncio
    async def test_list_collections_calls_correct_url(self, api, mock_async_http, fake_get_collections_res):
        mock_async_http.get.return_value = fake_get_collections_res
        await api.list_collections()
        call_args = mock_async_http.get.call_args
        assert f"/database/{DB_NAME}/collections" in call_args[0][0]
