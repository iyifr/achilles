import pytest
from unittest.mock import MagicMock

from achillesdb.database import SyncDatabase, AsyncDatabase
from achillesdb.collection import SyncCollection, AsyncCollection
from achillesdb.schemas import Document
from achillesdb.where import W
from tests.mocked.conftest import make_get_collection_res, make_get_collections_res, make_query_res


DB_NAME = "test_db"


# ─────────────────────────────────────────────────────────────────────────────
# SyncDatabase
# ─────────────────────────────────────────────────────────────────────────────

class TestSyncDatabase:

    @pytest.fixture
    def db(self, mock_sync_http):
        return SyncDatabase(name=DB_NAME, http=mock_sync_http)

    def test_create_collection_returns_sync_collection(self, db, mock_sync_http, fake_create_collection_res, fake_get_collection_res):
        mock_sync_http.post.return_value = fake_create_collection_res
        mock_sync_http.get.return_value = fake_get_collection_res
        result = db.create_collection("my_collection")
        assert isinstance(result, SyncCollection)

    def test_create_collection_calls_post_then_get(self, db, mock_sync_http, fake_create_collection_res, fake_get_collection_res):
        mock_sync_http.post.return_value = fake_create_collection_res
        mock_sync_http.get.return_value = fake_get_collection_res
        db.create_collection("my_collection")
        mock_sync_http.post.assert_called_once()
        mock_sync_http.get.assert_called_once()

    def test_list_collections_returns_list_of_sync_collections(self, db, mock_sync_http):
        mock_sync_http.get.return_value = make_get_collections_res(["col_a", "col_b"], DB_NAME)
        result = db.list_collections()
        assert isinstance(result, list)
        assert all(isinstance(c, SyncCollection) for c in result)

    def test_list_collections_correct_count(self, db, mock_sync_http):
        mock_sync_http.get.return_value = make_get_collections_res(["col_a", "col_b"], DB_NAME)
        result = db.list_collections()
        assert len(result) == 2

    def test_get_collection_returns_sync_collection(self, db, mock_sync_http, fake_get_collection_res):
        mock_sync_http.get.return_value = fake_get_collection_res
        result = db.get_collection("test_collection")
        assert isinstance(result, SyncCollection)

    def test_collection_returns_sync_collection(self, db, mock_sync_http, fake_get_collection_res):
        mock_sync_http.get.return_value = fake_get_collection_res
        result = db.collection("test_collection")
        assert isinstance(result, SyncCollection)

    def test_delete_collection_calls_delete(self, db, mock_sync_http, fake_delete_collection_res):
        mock_sync_http.delete.return_value = fake_delete_collection_res
        db.delete_collection("test_collection")
        mock_sync_http.delete.assert_called_once()

#     def test_query_collections_merges_and_sorts_by_distance(self, db, mock_sync_http, fake_get_collection_res):
#         # two collections returning docs with different distances
#         col_a_docs = [
#             Document(id="a1", content="doc a1", metadata={}, distance=0.5),
#             Document(id="a2", content="doc a2", metadata={}, distance=0.1),
#         ]
#         col_b_docs = [
#             Document(id="b1", content="doc b1", metadata={}, distance=0.3),
#         ]
# 
#         # get is called once per collection to resolve the collection handle
#         mock_sync_http.get.return_value = fake_get_collection_res
# 
#         # post is called once per collection query — order matches collection_names order
#         mock_sync_http.post.side_effect = [
#             make_query_res(col_a_docs),   # first call → col_a results
#             make_query_res(col_b_docs),   # second call → col_b results
#         ]
# 
#         result = db.query_collections(
#             collection_names=["col_a", "col_b"],
#             top_k=3,
#             query_embedding=[0.1, 0.2, 0.3],
#         )
#         # results should be sorted by distance ascending
#         distances = [d["distance"] for d in result]
#         assert distances == sorted(distances)

    def test_str_representation(self, db):
        assert DB_NAME in str(db)

    def test_repr_representation(self, db):
        assert DB_NAME in repr(db)


# ─────────────────────────────────────────────────────────────────────────────
# AsyncDatabase
# ─────────────────────────────────────────────────────────────────────────────

class TestAsyncDatabase:

    @pytest.fixture
    def db(self, mock_async_http):
        return AsyncDatabase(name=DB_NAME, http=mock_async_http)

    @pytest.mark.asyncio
    async def test_create_collection_returns_async_collection(self, db, mock_async_http, fake_create_collection_res, fake_get_collection_res):
        mock_async_http.post.return_value = fake_create_collection_res
        mock_async_http.get.return_value = fake_get_collection_res
        result = await db.create_collection("my_collection")
        assert isinstance(result, AsyncCollection)

    @pytest.mark.asyncio
    async def test_list_collections_returns_list_of_async_collections(self, db, mock_async_http):
        mock_async_http.get.return_value = make_get_collections_res(["col_a", "col_b"], DB_NAME)
        result = await db.list_collections()
        assert isinstance(result, list)
        assert all(isinstance(c, AsyncCollection) for c in result)

    @pytest.mark.asyncio
    async def test_get_collection_returns_async_collection(self, db, mock_async_http, fake_get_collection_res):
        mock_async_http.get.return_value = fake_get_collection_res
        result = await db.get_collection("test_collection")
        assert isinstance(result, AsyncCollection)

    @pytest.mark.asyncio
    async def test_delete_collection_calls_delete(self, db, mock_async_http, fake_delete_collection_res):
        mock_async_http.delete.return_value = fake_delete_collection_res
        await db.delete_collection("test_collection")
        mock_async_http.delete.assert_called_once()

    @pytest.mark.asyncio
    async def test_collection_returns_async_collection(self, db, mock_async_http, fake_get_collection_res):
        mock_async_http.get.return_value = fake_get_collection_res
        result = await db.collection("test_collection")
        assert isinstance(result, AsyncCollection)
