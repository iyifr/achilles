import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from achillesdb.collection import SyncCollection, AsyncCollection
from achillesdb.errors import AchillesError, ERROR_VALIDATION
from achillesdb.where import W


DB_NAME = "test_db"
COLL_NAME = "test_collection"
COLL_ID = "abc123"


def make_sync_collection(mock_sync_http, embedding_function=None):
    return SyncCollection(
        id=COLL_ID,
        name=COLL_NAME,
        database=DB_NAME,
        http_client=mock_sync_http,
        embedding_function=embedding_function,
    )


def make_async_collection(mock_async_http, embedding_function=None):
    return AsyncCollection(
        id=COLL_ID,
        name=COLL_NAME,
        database=DB_NAME,
        http_client=mock_async_http,
        embedding_function=embedding_function,
    )


# ─────────────────────────────────────────────────────────────────────────────
# SyncCollection
# ─────────────────────────────────────────────────────────────────────────────

class TestSyncCollectionDocuments:

    def test_add_documents_happy_path(self, mock_sync_http, fake_insert_res, fake_get_collection_res):
        mock_sync_http.post.return_value = fake_insert_res
        mock_sync_http.get.return_value = fake_get_collection_res
        coll = make_sync_collection(mock_sync_http)
        coll.add_documents(
            ids=["doc-1", "doc-2"],
            documents=["first", "second"],
            embeddings=[[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]],
            metadatas=[{}, {}],
        )
        mock_sync_http.post.assert_called_once()


    def test_add_documents_no_embeddings_no_fn_raises(self, mock_sync_http):
        coll = make_sync_collection(mock_sync_http)
        with pytest.raises(AchillesError) as exc_info:
            coll.add_documents(
                ids=["doc-1"],
                documents=["hello"],
                embeddings=None,
                metadatas=[{}],
            )
        assert exc_info.value.code == ERROR_VALIDATION

    def test_add_documents_with_embedding_function(self, mock_sync_http, fake_insert_res):
        mock_sync_http.post.return_value = fake_insert_res
        embedding_fn = MagicMock(return_value=[[0.1, 0.2, 0.3]])
        coll = make_sync_collection(mock_sync_http, embedding_function=embedding_fn)
        coll.add_documents(
            ids=["doc-1"],
            documents=["hello"],
            embeddings=None,
            metadatas=[{}],
        )
        embedding_fn.assert_called_once_with(["hello"])
        mock_sync_http.post.assert_called_once()

    def test_get_documents_returns_list_of_dicts(self, mock_sync_http, fake_get_documents_res):
        mock_sync_http.get.return_value = fake_get_documents_res
        coll = make_sync_collection(mock_sync_http)
        result = coll.get_documents()
        assert isinstance(result, list)
        assert all(isinstance(d, dict) for d in result)

    def test_get_documents_contains_expected_fields(self, mock_sync_http, fake_get_documents_res):
        mock_sync_http.get.return_value = fake_get_documents_res
        coll = make_sync_collection(mock_sync_http)
        result = coll.get_documents()
        assert "id" in result[0]
        assert "content" in result[0]

    def test_count_returns_int(self, mock_sync_http, fake_get_collection_res):
        mock_sync_http.get.return_value = fake_get_collection_res
        coll = make_sync_collection(mock_sync_http)
        result = coll.count()
        assert isinstance(result, int)

    def test_peek_returns_list(self, mock_sync_http, fake_get_documents_res):
        mock_sync_http.get.return_value = fake_get_documents_res
        coll = make_sync_collection(mock_sync_http)
        result = coll.peek(n=2)
        assert isinstance(result, list)
        assert len(result) <= 2

    def test_query_documents_with_embedding(self, mock_sync_http, fake_query_res):
        mock_sync_http.post.return_value = fake_query_res
        coll = make_sync_collection(mock_sync_http)
        result = coll.query_documents(top_k=2, query_embedding=[0.1, 0.2, 0.3])
        assert isinstance(result, list)
        mock_sync_http.post.assert_called_once()

    def test_query_documents_with_where(self, mock_sync_http, fake_query_res):
        mock_sync_http.post.return_value = fake_query_res
        coll = make_sync_collection(mock_sync_http)
        result = coll.query_documents(
            top_k=2,
            query_embedding=[0.1, 0.2, 0.3],
            where=W.eq("category", "fruit"),
        )
        assert isinstance(result, list)

    def test_query_documents_no_embedding_no_fn_raises(self, mock_sync_http):
        coll = make_sync_collection(mock_sync_http)
        with pytest.raises(AchillesError) as exc_info:
            coll.query_documents(top_k=1)
        assert exc_info.value.code == ERROR_VALIDATION

    def test_update_documents_calls_put(self, mock_sync_http, fake_update_res):
        mock_sync_http.put.return_value = fake_update_res
        coll = make_sync_collection(mock_sync_http)
        coll.update_documents(
            document_id="doc-1",
            where={},
            updates={"popular": False},
        )
        mock_sync_http.put.assert_called_once()

    def test_delete_documents_calls_delete(self, mock_sync_http, fake_delete_documents_res):
        mock_sync_http.delete.return_value = fake_delete_documents_res
        coll = make_sync_collection(mock_sync_http)
        coll.delete_documents(document_ids=["doc-1", "doc-2"])
        mock_sync_http.delete.assert_called_once()


# ─────────────────────────────────────────────────────────────────────────────
# AsyncCollection
# ─────────────────────────────────────────────────────────────────────────────

class TestAsyncCollectionDocuments:

    @pytest.mark.asyncio
    async def test_add_documents_happy_path(self, mock_async_http, fake_insert_res):
        mock_async_http.post.return_value = fake_insert_res
        coll = make_async_collection(mock_async_http)
        await coll.add_documents(
            ids=["doc-1"],
            documents=["hello"],
            embeddings=[[0.1, 0.2, 0.3]],
            metadatas=[{}],
        )
        mock_async_http.post.assert_called_once()

    @pytest.mark.asyncio
    async def test_add_documents_no_embeddings_no_fn_raises(self, mock_async_http):
        coll = make_async_collection(mock_async_http)
        with pytest.raises(AchillesError) as exc_info:
            await coll.add_documents(
                ids=["doc-1"],
                documents=["hello"],
                embeddings=None,
                metadatas=[{}],
            )
        assert exc_info.value.code == ERROR_VALIDATION


    @pytest.mark.asyncio
    async def test_get_documents_returns_list_of_dicts(self, mock_async_http, fake_get_documents_res):
        mock_async_http.get.return_value = fake_get_documents_res
        coll = make_async_collection(mock_async_http)
        result = await coll.get_documents()
        assert isinstance(result, list)
        assert all(isinstance(d, dict) for d in result)

    @pytest.mark.asyncio
    async def test_query_documents_with_embedding(self, mock_async_http, fake_query_res):
        mock_async_http.post.return_value = fake_query_res
        coll = make_async_collection(mock_async_http)
        result = await coll.query_documents(top_k=2, query_embedding=[0.1, 0.2, 0.3])
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_query_documents_no_embedding_no_fn_raises(self, mock_async_http):
        coll = make_async_collection(mock_async_http)
        with pytest.raises(AchillesError) as exc_info:
            await coll.query_documents(top_k=1)
        assert exc_info.value.code == ERROR_VALIDATION

    @pytest.mark.asyncio
    async def test_update_documents_calls_put(self, mock_async_http, fake_update_res):
        mock_async_http.put.return_value = fake_update_res
        coll = make_async_collection(mock_async_http)
        await coll.update_documents(
            document_id="doc-1",
            where={},
            updates={"popular": False},
        )
        mock_async_http.put.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_documents_calls_delete(self, mock_async_http, fake_delete_documents_res):
        mock_async_http.delete.return_value = fake_delete_documents_res
        coll = make_async_collection(mock_async_http)
        await coll.delete_documents(document_ids=["doc-1"])
        mock_async_http.delete.assert_called_once()
