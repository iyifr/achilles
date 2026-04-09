import pytest
from unittest.mock import MagicMock

from achillesdb.api.document import SyncDocumentApi, AsyncDocumentApi
from achillesdb.schemas import (
    InsertDocumentReqInput,
    UpdateDocumentsReqInput,
    DeleteDocumentsReqInput,
    QueryReqInput,
)
from achillesdb.where import W


DB_NAME = "test_db"
COLL_NAME = "test_collection"
BASE_PATH = f"/database/{DB_NAME}/collections/{COLL_NAME}/documents"
QUERY_PATH = f"{BASE_PATH}/query"


# ── input factories ───────────────────────────────────────────────────────────

def make_insert_input():
    return InsertDocumentReqInput(
        ids=["doc-1", "doc-2"],
        documents=["first doc", "second doc"],
        embeddings=[[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]],
        metadatas=[{"year": 2021}, {"year": 2022}],
    )


def make_update_input():
    return UpdateDocumentsReqInput(
        document_id="doc-1",
        updates={"popular": False, "reviewed": True},
    )


def make_delete_input():
    return DeleteDocumentsReqInput(document_ids=["doc-1", "doc-2"])


def make_query_input(where=None):
    return QueryReqInput(
        query_embedding=[0.1, 0.2, 0.3],
        top_k=5,
        where=where,
    )


# ─────────────────────────────────────────────────────────────────────────────
# SyncDocumentApi
# ─────────────────────────────────────────────────────────────────────────────

class TestSyncDocumentApi:

    @pytest.fixture
    def api(self, mock_sync_http):
        return SyncDocumentApi(
            http_client=mock_sync_http,
            database_name=DB_NAME,
            collection_name=COLL_NAME,
        )

    # ── get_documents ─────────────────────────────────────────────────────────

    def test_get_documents_calls_correct_url(self, api, mock_sync_http, fake_get_documents_res):
        mock_sync_http.get.return_value = fake_get_documents_res
        api.get_documents()
        assert mock_sync_http.get.call_args[0][0] == BASE_PATH

    def test_get_documents_uses_get_verb(self, api, mock_sync_http, fake_get_documents_res):
        mock_sync_http.get.return_value = fake_get_documents_res
        api.get_documents()
        mock_sync_http.get.assert_called_once()
        mock_sync_http.post.assert_not_called()

    def test_get_documents_passes_expected_status(self, api, mock_sync_http, fake_get_documents_res):
        mock_sync_http.get.return_value = fake_get_documents_res
        api.get_documents()
        assert mock_sync_http.get.call_args[1].get("expected_status") == 200

    def test_get_documents_returns_response(self, api, mock_sync_http, fake_get_documents_res):
        mock_sync_http.get.return_value = fake_get_documents_res
        assert api.get_documents() == fake_get_documents_res

    # ── insert_documents ──────────────────────────────────────────────────────

    def test_insert_documents_calls_correct_url(self, api, mock_sync_http, fake_insert_res):
        mock_sync_http.post.return_value = fake_insert_res
        api.insert_documents(make_insert_input())
        assert mock_sync_http.post.call_args[0][0] == BASE_PATH

    def test_insert_documents_uses_post_verb(self, api, mock_sync_http, fake_insert_res):
        mock_sync_http.post.return_value = fake_insert_res
        api.insert_documents(make_insert_input())
        mock_sync_http.post.assert_called_once()
        mock_sync_http.put.assert_not_called()

    def test_insert_documents_passes_expected_status(self, api, mock_sync_http, fake_insert_res):
        mock_sync_http.post.return_value = fake_insert_res
        api.insert_documents(make_insert_input())
        assert mock_sync_http.post.call_args[1].get("expected_status") == 200

    def test_insert_documents_payload_contains_ids(self, api, mock_sync_http, fake_insert_res):
        mock_sync_http.post.return_value = fake_insert_res
        api.insert_documents(make_insert_input())
        assert mock_sync_http.post.call_args[1]["json"]["ids"] == ["doc-1", "doc-2"]

    def test_insert_documents_payload_contains_documents(self, api, mock_sync_http, fake_insert_res):
        mock_sync_http.post.return_value = fake_insert_res
        api.insert_documents(make_insert_input())
        assert mock_sync_http.post.call_args[1]["json"]["documents"] == ["first doc", "second doc"]

    def test_insert_documents_payload_contains_embeddings(self, api, mock_sync_http, fake_insert_res):
        mock_sync_http.post.return_value = fake_insert_res
        api.insert_documents(make_insert_input())
        assert mock_sync_http.post.call_args[1]["json"]["embeddings"] == [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]]

    def test_insert_documents_payload_contains_metadatas(self, api, mock_sync_http, fake_insert_res):
        mock_sync_http.post.return_value = fake_insert_res
        api.insert_documents(make_insert_input())
        assert mock_sync_http.post.call_args[1]["json"]["metadatas"] == [{"year": 2021}, {"year": 2022}]

    def test_insert_documents_returns_response(self, api, mock_sync_http, fake_insert_res):
        mock_sync_http.post.return_value = fake_insert_res
        assert api.insert_documents(make_insert_input()) == fake_insert_res

    # ── update_documents ──────────────────────────────────────────────────────

    def test_update_documents_calls_correct_url(self, api, mock_sync_http, fake_update_res):
        mock_sync_http.put.return_value = fake_update_res
        api.update_documents(make_update_input())
        assert mock_sync_http.put.call_args[0][0] == BASE_PATH

    def test_update_documents_uses_put_verb(self, api, mock_sync_http, fake_update_res):
        mock_sync_http.put.return_value = fake_update_res
        api.update_documents(make_update_input())
        mock_sync_http.put.assert_called_once()
        mock_sync_http.post.assert_not_called()

    def test_update_documents_passes_expected_status(self, api, mock_sync_http, fake_update_res):
        mock_sync_http.put.return_value = fake_update_res
        api.update_documents(make_update_input())
        assert mock_sync_http.put.call_args[1].get("expected_status") == 200

    def test_update_documents_payload_contains_document_id(self, api, mock_sync_http, fake_update_res):
        mock_sync_http.put.return_value = fake_update_res
        api.update_documents(make_update_input())
        assert mock_sync_http.put.call_args[1]["json"]["document_id"] == "doc-1"

    def test_update_documents_payload_contains_updates(self, api, mock_sync_http, fake_update_res):
        mock_sync_http.put.return_value = fake_update_res
        api.update_documents(make_update_input())
        assert mock_sync_http.put.call_args[1]["json"]["updates"] == {"popular": False, "reviewed": True}

    def test_update_documents_payload_does_not_contain_where(self, api, mock_sync_http, fake_update_res):
        mock_sync_http.put.return_value = fake_update_res
        api.update_documents(make_update_input())
        assert "where" not in mock_sync_http.put.call_args[1]["json"]

    def test_update_documents_returns_response(self, api, mock_sync_http, fake_update_res):
        mock_sync_http.put.return_value = fake_update_res
        assert api.update_documents(make_update_input()) == fake_update_res

    # ── delete_documents ──────────────────────────────────────────────────────

    def test_delete_documents_calls_correct_url(self, api, mock_sync_http, fake_delete_documents_res):
        mock_sync_http.delete.return_value = fake_delete_documents_res
        api.delete_documents(make_delete_input())
        assert mock_sync_http.delete.call_args[0][0] == BASE_PATH

    def test_delete_documents_uses_delete_verb(self, api, mock_sync_http, fake_delete_documents_res):
        mock_sync_http.delete.return_value = fake_delete_documents_res
        api.delete_documents(make_delete_input())
        mock_sync_http.delete.assert_called_once()
        mock_sync_http.post.assert_not_called()

    def test_delete_documents_passes_expected_status(self, api, mock_sync_http, fake_delete_documents_res):
        mock_sync_http.delete.return_value = fake_delete_documents_res
        api.delete_documents(make_delete_input())
        assert mock_sync_http.delete.call_args[1].get("expected_status") == 200

    def test_delete_documents_payload_contains_document_ids(self, api, mock_sync_http, fake_delete_documents_res):
        mock_sync_http.delete.return_value = fake_delete_documents_res
        api.delete_documents(make_delete_input())
        assert mock_sync_http.delete.call_args[1]["json"]["document_ids"] == ["doc-1", "doc-2"]

    def test_delete_documents_returns_response(self, api, mock_sync_http, fake_delete_documents_res):
        mock_sync_http.delete.return_value = fake_delete_documents_res
        assert api.delete_documents(make_delete_input()) == fake_delete_documents_res

    # ── query ───────────────────────────────────────────────────────

    def test_query_calls_correct_url(self, api, mock_sync_http, fake_query_res):
        mock_sync_http.post.return_value = fake_query_res
        api.query(make_query_input())
        assert mock_sync_http.post.call_args[0][0] == QUERY_PATH

    def test_query_uses_post_verb(self, api, mock_sync_http, fake_query_res):
        mock_sync_http.post.return_value = fake_query_res
        api.query(make_query_input())
        mock_sync_http.post.assert_called_once()
        mock_sync_http.get.assert_not_called()

    def test_query_passes_expected_status(self, api, mock_sync_http, fake_query_res):
        mock_sync_http.post.return_value = fake_query_res
        api.query(make_query_input())
        assert mock_sync_http.post.call_args[1].get("expected_status") == 200

    def test_query_payload_contains_query_embedding(self, api, mock_sync_http, fake_query_res):
        mock_sync_http.post.return_value = fake_query_res
        api.query(make_query_input())
        assert mock_sync_http.post.call_args[1]["json"]["query_embedding"] == [0.1, 0.2, 0.3]

    def test_query_payload_contains_top_k(self, api, mock_sync_http, fake_query_res):
        mock_sync_http.post.return_value = fake_query_res
        api.query(make_query_input())
        assert mock_sync_http.post.call_args[1]["json"]["top_k"] == 5

    def test_query_payload_where_none_when_not_set(self, api, mock_sync_http, fake_query_res):
        mock_sync_http.post.return_value = fake_query_res
        api.query(make_query_input())
        assert mock_sync_http.post.call_args[1]["json"]["where"] is None

    def test_query_payload_where_serialized_correctly(self, api, mock_sync_http, fake_query_res):
        mock_sync_http.post.return_value = fake_query_res
        api.query(make_query_input(where=W.eq("category", "fruit")))
        assert mock_sync_http.post.call_args[1]["json"]["where"] == {"category": {"$eq": "fruit"}}

    def test_query_returns_response(self, api, mock_sync_http, fake_query_res):
        mock_sync_http.post.return_value = fake_query_res
        assert api.query(make_query_input()) == fake_query_res


# ─────────────────────────────────────────────────────────────────────────────
# AsyncDocumentApi
# ─────────────────────────────────────────────────────────────────────────────

class TestAsyncDocumentApi:

    @pytest.fixture
    def api(self, mock_async_http):
        return AsyncDocumentApi(
            http_client=mock_async_http,
            database_name=DB_NAME,
            collection_name=COLL_NAME,
        )

    # ── get_documents ─────────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_get_documents_calls_correct_url(self, api, mock_async_http, fake_get_documents_res):
        mock_async_http.get.return_value = fake_get_documents_res
        await api.get_documents()
        assert mock_async_http.get.call_args[0][0] == BASE_PATH

    @pytest.mark.asyncio
    async def test_get_documents_uses_get_verb(self, api, mock_async_http, fake_get_documents_res):
        mock_async_http.get.return_value = fake_get_documents_res
        await api.get_documents()
        mock_async_http.get.assert_called_once()
        mock_async_http.post.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_documents_passes_expected_status(self, api, mock_async_http, fake_get_documents_res):
        mock_async_http.get.return_value = fake_get_documents_res
        await api.get_documents()
        assert mock_async_http.get.call_args[1].get("expected_status") == 200

    @pytest.mark.asyncio
    async def test_get_documents_returns_response(self, api, mock_async_http, fake_get_documents_res):
        mock_async_http.get.return_value = fake_get_documents_res
        assert await api.get_documents() == fake_get_documents_res

    # ── insert_documents ──────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_insert_documents_calls_correct_url(self, api, mock_async_http, fake_insert_res):
        mock_async_http.post.return_value = fake_insert_res
        await api.insert_documents(make_insert_input())
        assert mock_async_http.post.call_args[0][0] == BASE_PATH

    @pytest.mark.asyncio
    async def test_insert_documents_uses_post_verb(self, api, mock_async_http, fake_insert_res):
        mock_async_http.post.return_value = fake_insert_res
        await api.insert_documents(make_insert_input())
        mock_async_http.post.assert_called_once()
        mock_async_http.put.assert_not_called()

    @pytest.mark.asyncio
    async def test_insert_documents_payload_contains_ids(self, api, mock_async_http, fake_insert_res):
        mock_async_http.post.return_value = fake_insert_res
        await api.insert_documents(make_insert_input())
        assert mock_async_http.post.call_args[1]["json"]["ids"] == ["doc-1", "doc-2"]

    @pytest.mark.asyncio
    async def test_insert_documents_payload_contains_embeddings(self, api, mock_async_http, fake_insert_res):
        mock_async_http.post.return_value = fake_insert_res
        await api.insert_documents(make_insert_input())
        assert mock_async_http.post.call_args[1]["json"]["embeddings"] == [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]]

    @pytest.mark.asyncio
    async def test_insert_documents_returns_response(self, api, mock_async_http, fake_insert_res):
        mock_async_http.post.return_value = fake_insert_res
        assert await api.insert_documents(make_insert_input()) == fake_insert_res

    # ── update_documents ──────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_update_documents_calls_correct_url(self, api, mock_async_http, fake_update_res):
        mock_async_http.put.return_value = fake_update_res
        await api.update_documents(make_update_input())
        assert mock_async_http.put.call_args[0][0] == BASE_PATH

    @pytest.mark.asyncio
    async def test_update_documents_uses_put_verb(self, api, mock_async_http, fake_update_res):
        mock_async_http.put.return_value = fake_update_res
        await api.update_documents(make_update_input())
        mock_async_http.put.assert_called_once()
        mock_async_http.post.assert_not_called()

    @pytest.mark.asyncio
    async def test_update_documents_payload_contains_document_id(self, api, mock_async_http, fake_update_res):
        mock_async_http.put.return_value = fake_update_res
        await api.update_documents(make_update_input())
        assert mock_async_http.put.call_args[1]["json"]["document_id"] == "doc-1"

    @pytest.mark.asyncio
    async def test_update_documents_payload_contains_updates(self, api, mock_async_http, fake_update_res):
        mock_async_http.put.return_value = fake_update_res
        await api.update_documents(make_update_input())
        assert mock_async_http.put.call_args[1]["json"]["updates"] == {"popular": False, "reviewed": True}

    @pytest.mark.asyncio
    async def test_update_documents_returns_response(self, api, mock_async_http, fake_update_res):
        mock_async_http.put.return_value = fake_update_res
        assert await api.update_documents(make_update_input()) == fake_update_res

    # ── delete_documents ──────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_delete_documents_calls_correct_url(self, api, mock_async_http, fake_delete_documents_res):
        mock_async_http.delete.return_value = fake_delete_documents_res
        await api.delete_documents(make_delete_input())
        assert mock_async_http.delete.call_args[0][0] == BASE_PATH

    @pytest.mark.asyncio
    async def test_delete_documents_uses_delete_verb(self, api, mock_async_http, fake_delete_documents_res):
        mock_async_http.delete.return_value = fake_delete_documents_res
        await api.delete_documents(make_delete_input())
        mock_async_http.delete.assert_called_once()
        mock_async_http.post.assert_not_called()

    @pytest.mark.asyncio
    async def test_delete_documents_payload_contains_document_ids(self, api, mock_async_http, fake_delete_documents_res):
        mock_async_http.delete.return_value = fake_delete_documents_res
        await api.delete_documents(make_delete_input())
        assert mock_async_http.delete.call_args[1]["json"]["document_ids"] == ["doc-1", "doc-2"]

    @pytest.mark.asyncio
    async def test_delete_documents_returns_response(self, api, mock_async_http, fake_delete_documents_res):
        mock_async_http.delete.return_value = fake_delete_documents_res
        assert await api.delete_documents(make_delete_input()) == fake_delete_documents_res

    # ── query ───────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_query_calls_correct_url(self, api, mock_async_http, fake_query_res):
        mock_async_http.post.return_value = fake_query_res
        await api.query(make_query_input())
        assert mock_async_http.post.call_args[0][0] == QUERY_PATH

    @pytest.mark.asyncio
    async def test_query_uses_post_verb(self, api, mock_async_http, fake_query_res):
        mock_async_http.post.return_value = fake_query_res
        await api.query(make_query_input())
        mock_async_http.post.assert_called_once()
        mock_async_http.get.assert_not_called()

    @pytest.mark.asyncio
    async def test_query_payload_contains_query_embedding(self, api, mock_async_http, fake_query_res):
        mock_async_http.post.return_value = fake_query_res
        await api.query(make_query_input())
        assert mock_async_http.post.call_args[1]["json"]["query_embedding"] == [0.1, 0.2, 0.3]

    @pytest.mark.asyncio
    async def test_query_payload_contains_top_k(self, api, mock_async_http, fake_query_res):
        mock_async_http.post.return_value = fake_query_res
        await api.query(make_query_input())
        assert mock_async_http.post.call_args[1]["json"]["top_k"] == 5

    @pytest.mark.asyncio
    async def test_query_payload_where_serialized_correctly(self, api, mock_async_http, fake_query_res):
        mock_async_http.post.return_value = fake_query_res
        await api.query(make_query_input(where=W.eq("category", "fruit")))
        assert mock_async_http.post.call_args[1]["json"]["where"] == {"category": {"$eq": "fruit"}}

    @pytest.mark.asyncio
    async def test_query_returns_response(self, api, mock_async_http, fake_query_res):
        mock_async_http.post.return_value = fake_query_res
        assert await api.query(make_query_input()) == fake_query_res
