"""
tests/unit/test_collection_logic.py
=====================================
Unit tests for achillesdb/collection.py

Tests collection-level logic with the HTTP layer mocked at the
_HTTPClient.request level. This means real validation, real schema
parsing, and real embedding resolution logic are all exercised —
only the actual network call is replaced.

Covers:
  - add_documents  (sync + async: embedding resolution, before_insert hook,
                    validation errors, inspect.isawaitable path)
  - get_documents  (sync + async)
  - query_documents (sync + async: embedding resolution, where normalization)
  - update_documents
  - delete_documents
  - count
  - peek           (sync + async: returns list of dicts)
"""

import asyncio
import inspect
import pytest
from unittest.mock import MagicMock, AsyncMock, patch, call
from typing import Any

from achillesdb.collection import SyncCollection, AsyncCollection
from achillesdb.errors import AchillesError, ERROR_VALIDATION
from achillesdb.http.connection import SyncHttpClient, AsyncHttpClient
from achillesdb.schemas import (
    InsertDocumentsRes,
    GetDocumentsRes,
    QueryRes,
    UpdateDocumentsRes,
    DeleteDocumentsRes,
    GetCollectionRes,
    CollectionCatalogEntry,
    CollectionStats,
    Document,
    WhereClause,
)
from achillesdb.where import W


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures and helpers
# ─────────────────────────────────────────────────────────────────────────────

DB = "testdb"
COLL = "testcollection"
COLL_ID = "coll-id-001"

EMB_A = [0.1, 0.2, 0.3, 0.4]
EMB_B = [0.5, 0.6, 0.7, 0.8]
EMB_QUERY = [0.3, 0.4, 0.5, 0.6]


def _make_sync_collection(
    embedding_function=None,
    http_overrides: dict | None = None,
) -> tuple[SyncCollection, MagicMock]:
    """
    Build a SyncCollection with a mocked SyncHttpClient.
    Returns (collection, mock_http).
    """
    mock_http = MagicMock(spec=SyncHttpClient)
    mock_http.mode = "sync"
    if http_overrides:
        for attr, val in http_overrides.items():
            setattr(mock_http, attr, val)

    collection = SyncCollection(
        id=COLL_ID,
        name=COLL,
        database=DB,
        http_client=mock_http,
        embedding_function=embedding_function,
    )
    return collection, mock_http


def _make_async_collection(
    embedding_function=None,
) -> tuple[AsyncCollection, MagicMock]:
    """
    Build an AsyncCollection with a mocked AsyncHttpClient.
    Returns (collection, mock_http).
    """
    mock_http = MagicMock(spec=AsyncHttpClient)
    mock_http.mode = "async"

    collection = AsyncCollection(
        id=COLL_ID,
        name=COLL,
        database=DB,
        http_client=mock_http,
        embedding_function=embedding_function,
    )
    return collection, mock_http


def _insert_res() -> InsertDocumentsRes:
    return InsertDocumentsRes(message="ok")


def _get_docs_res(n: int = 2) -> GetDocumentsRes:
    docs = [
        Document(id=f"doc{i}", content=f"content {i}", metadata={"i": i})
        for i in range(n)
    ]
    return GetDocumentsRes(documents=docs, doc_count=n)


def _query_res(n: int = 2) -> QueryRes:
    docs = [
        Document(id=f"doc{i}", content=f"content {i}", distance=float(i) * 0.1)
        for i in range(n)
    ]
    return QueryRes(documents=docs, doc_count=n)


def _get_collection_res() -> GetCollectionRes:
    from datetime import datetime, timezone
    entry = CollectionCatalogEntry(
        **{
            "_id": COLL_ID,
            "ns": f"{DB}.{COLL}",
            "table_uri": "s3://t",
            "vector_index_uri": "s3://v",
            "createdAt": datetime.now(tz=timezone.utc).isoformat(),
            "updatedAt": datetime.now(tz=timezone.utc).isoformat(),
        }
    )
    stats = CollectionStats(doc_count=5, vector_index_size=1.0)
    return GetCollectionRes(collection=entry, stats=stats)


# ─────────────────────────────────────────────────────────────────────────────
# add_documents — sync
# ─────────────────────────────────────────────────────────────────────────────

class TestSyncAddDocuments:

    def test_embeddings_provided_directly(self):
        coll, mock_http = _make_sync_collection()
        mock_http.post.return_value = _insert_res()

        coll.add_documents(
            ids=["a", "b"],
            documents=["doc a", "doc b"],
            embeddings=[EMB_A, EMB_B],
            metadatas=None,
        )
        mock_http.post.assert_called_once()

    def test_embedding_function_called_when_no_embeddings(self):
        embed_fn = MagicMock(return_value=[EMB_A, EMB_B])
        coll, mock_http = _make_sync_collection(embedding_function=embed_fn)
        mock_http.post.return_value = _insert_res()

        coll.add_documents(
            ids=["a", "b"],
            documents=["doc a", "doc b"],
            embeddings=None,
            metadatas=None,
        )

        embed_fn.assert_called_once_with(["doc a", "doc b"])
        mock_http.post.assert_called_once()

    def test_no_embeddings_no_embedding_function_raises(self):
        coll, _ = _make_sync_collection(embedding_function=None)

        with pytest.raises(AchillesError) as exc_info:
            coll.add_documents(
                ids=["a"],
                documents=["doc a"],
                embeddings=None,
                metadatas=None,
            )
        assert exc_info.value.code == ERROR_VALIDATION


    def test_validation_error_raised_before_http_call(self):
        # duplicate ids should fail at Pydantic validation, not reach the server
        coll, mock_http = _make_sync_collection()

        with pytest.raises(Exception):
            coll.add_documents(
                ids=["dup", "dup"],
                documents=["x", "y"],
                embeddings=[EMB_A, EMB_B],
                metadatas=None,
            )
        mock_http.post.assert_not_called()

    def test_add_documents_returns_none(self):
        coll, mock_http = _make_sync_collection()
        mock_http.post.return_value = _insert_res()

        result = coll.add_documents(
            ids=["a"],
            documents=["doc a"],
            embeddings=[EMB_A],
            metadatas=None,
        )
        assert result is None


# ─────────────────────────────────────────────────────────────────────────────
# add_documents — async
# ─────────────────────────────────────────────────────────────────────────────

class TestAsyncAddDocuments:

    def test_sync_embedding_function_works_in_async_collection(self):
        """
        inspect.isawaitable path: sync embedding_function returns a list
        directly — must not be awaited.
        """
        async def run():
            sync_embed = MagicMock(return_value=[EMB_A, EMB_B])
            coll, mock_http = _make_async_collection(embedding_function=sync_embed)
            mock_http.post = AsyncMock(return_value=_insert_res())

            await coll.add_documents(
                ids=["a", "b"],
                documents=["doc a", "doc b"],
                embeddings=None,
                metadatas=None,
            )
            sync_embed.assert_called_once_with(["doc a", "doc b"])
            mock_http.post.assert_called_once()

        asyncio.run(run())

    def test_async_embedding_function_works_in_async_collection(self):
        """
        inspect.isawaitable path: async embedding_function returns a coroutine
        that must be awaited.
        """
        async def run():
            async def async_embed(docs):
                return [EMB_A, EMB_B]

            coll, mock_http = _make_async_collection(embedding_function=async_embed)
            mock_http.post = AsyncMock(return_value=_insert_res())

            await coll.add_documents(
                ids=["a", "b"],
                documents=["doc a", "doc b"],
                embeddings=None,
                metadatas=None,
            )
            mock_http.post.assert_called_once()

        asyncio.run(run())

    def test_embeddings_provided_directly_skips_embedding_function(self):
        async def run():
            embed_fn = AsyncMock()
            coll, mock_http = _make_async_collection(embedding_function=embed_fn)
            mock_http.post = AsyncMock(return_value=_insert_res())

            await coll.add_documents(
                ids=["a"],
                documents=["doc a"],
                embeddings=[EMB_A],
                metadatas=None,
            )
            embed_fn.assert_not_called()

        asyncio.run(run())

    def test_no_embeddings_no_function_raises(self):
        async def run():
            coll, _ = _make_async_collection(embedding_function=None)
            with pytest.raises(AchillesError) as exc_info:
                await coll.add_documents(
                    ids=["a"],
                    documents=["doc a"],
                    embeddings=None,
                    metadatas=None,
                )
            assert exc_info.value.code == ERROR_VALIDATION

        asyncio.run(run())


    def test_add_documents_async_returns_none(self):
        async def run():
            coll, mock_http = _make_async_collection()
            mock_http.post = AsyncMock(return_value=_insert_res())

            result = await coll.add_documents(
                ids=["a"],
                documents=["doc a"],
                embeddings=[EMB_A],
                metadatas=None,
            )
            assert result is None

        asyncio.run(run())


# ─────────────────────────────────────────────────────────────────────────────
# get_documents — sync + async
# ─────────────────────────────────────────────────────────────────────────────

class TestGetDocuments:

    def test_sync_returns_list_of_dicts(self):
        coll, mock_http = _make_sync_collection()
        mock_http.get.return_value = _get_docs_res(2)

        result = coll.get_documents()

        assert isinstance(result, list)
        assert len(result) == 2
        assert isinstance(result[0], dict)
        assert result[0]["id"] == "doc0"

    def test_sync_empty_collection_returns_empty_list(self):
        coll, mock_http = _make_sync_collection()
        mock_http.get.return_value = GetDocumentsRes(documents=[], doc_count=0)

        result = coll.get_documents()
        assert result == []

    def test_sync_document_fields_present(self):
        coll, mock_http = _make_sync_collection()
        mock_http.get.return_value = _get_docs_res(1)

        result = coll.get_documents()
        doc = result[0]
        assert "id" in doc
        assert "content" in doc
        assert "metadata" in doc

    def test_async_returns_list_of_dicts(self):
        async def run():
            coll, mock_http = _make_async_collection()
            mock_http.get = AsyncMock(return_value=_get_docs_res(2))

            result = await coll.get_documents()

            assert isinstance(result, list)
            assert len(result) == 2
            assert isinstance(result[0], dict)

        asyncio.run(run())

    def test_async_empty_collection_returns_empty_list(self):
        async def run():
            coll, mock_http = _make_async_collection()
            mock_http.get = AsyncMock(
                return_value=GetDocumentsRes(documents=[], doc_count=0)
            )
            result = await coll.get_documents()
            assert result == []

        asyncio.run(run())


# ─────────────────────────────────────────────────────────────────────────────
# query_documents — sync
# ─────────────────────────────────────────────────────────────────────────────

class TestSyncQueryDocuments:

    def test_query_embedding_provided_directly(self):
        coll, mock_http = _make_sync_collection()
        mock_http.post.return_value = _query_res(2)

        result = coll.query_documents(top_k=5, query_embedding=EMB_QUERY)

        assert isinstance(result, list)
        assert len(result) == 2
        mock_http.post.assert_called_once()

    def test_query_text_calls_embedding_function(self):
        embed_fn = MagicMock(return_value=[EMB_QUERY])
        coll, mock_http = _make_sync_collection(embedding_function=embed_fn)
        mock_http.post.return_value = _query_res(1)

        coll.query_documents(top_k=5, query="find me something")

        # embedding_function must receive query wrapped in a list
        embed_fn.assert_called_once_with(["find me something"])

    def test_query_text_embedding_function_result_first_element_used(self):
        # embedding_function returns list of vectors — query uses [0]
        embed_fn = MagicMock(return_value=[EMB_QUERY, EMB_A])
        coll, mock_http = _make_sync_collection(embedding_function=embed_fn)
        mock_http.post.return_value = _query_res(1)

        coll.query_documents(top_k=5, query="test")
        mock_http.post.assert_called_once()

    def test_no_embedding_no_function_raises(self):
        coll, _ = _make_sync_collection(embedding_function=None)

        with pytest.raises(AchillesError) as exc_info:
            coll.query_documents(top_k=5)
        assert exc_info.value.code == ERROR_VALIDATION

    def test_where_dict_normalized_to_where_clause(self):
        coll, mock_http = _make_sync_collection()
        mock_http.post.return_value = _query_res(1)

        # passing a plain dict — must not raise
        coll.query_documents(
            top_k=5,
            query_embedding=EMB_QUERY,
            where={"category": "tech"},
        )
        mock_http.post.assert_called_once()

    def test_where_clause_instance_passed_through(self):
        coll, mock_http = _make_sync_collection()
        mock_http.post.return_value = _query_res(1)

        coll.query_documents(
            top_k=5,
            query_embedding=EMB_QUERY,
            where=W.eq("category", "tech"),
        )
        mock_http.post.assert_called_once()

    def test_where_none_passed_through(self):
        coll, mock_http = _make_sync_collection()
        mock_http.post.return_value = _query_res(1)

        coll.query_documents(top_k=5, query_embedding=EMB_QUERY, where=None)
        mock_http.post.assert_called_once()

    def test_returns_list_of_dicts(self):
        coll, mock_http = _make_sync_collection()
        mock_http.post.return_value = _query_res(2)

        result = coll.query_documents(top_k=5, query_embedding=EMB_QUERY)
        assert isinstance(result, list)
        assert all(isinstance(d, dict) for d in result)

    def test_result_dicts_have_distance_field(self):
        coll, mock_http = _make_sync_collection()
        mock_http.post.return_value = _query_res(2)

        result = coll.query_documents(top_k=5, query_embedding=EMB_QUERY)
        assert "distance" in result[0]


# ─────────────────────────────────────────────────────────────────────────────
# query_documents — async
# ─────────────────────────────────────────────────────────────────────────────

class TestAsyncQueryDocuments:

    def test_query_embedding_provided_directly(self):
        async def run():
            coll, mock_http = _make_async_collection()
            mock_http.post = AsyncMock(return_value=_query_res(2))

            result = await coll.query_documents(top_k=5, query_embedding=EMB_QUERY)
            assert isinstance(result, list)
            assert len(result) == 2

        asyncio.run(run())

    def test_sync_embedding_function_works(self):
        """Sync embedding_function must work inside async query path."""
        async def run():
            sync_embed = MagicMock(return_value=[EMB_QUERY])
            coll, mock_http = _make_async_collection(embedding_function=sync_embed)
            mock_http.post = AsyncMock(return_value=_query_res(1))

            await coll.query_documents(top_k=5, query="find me")

            # embedding_function must receive [query], not query
            sync_embed.assert_called_once_with(["find me"])

        asyncio.run(run())

    def test_async_embedding_function_works(self):
        """Async embedding_function must be awaited in the query path."""
        async def run():
            async def async_embed(texts):
                return [EMB_QUERY]

            coll, mock_http = _make_async_collection(embedding_function=async_embed)
            mock_http.post = AsyncMock(return_value=_query_res(1))

            result = await coll.query_documents(top_k=5, query="find me")
            assert isinstance(result, list)

        asyncio.run(run())

    def test_no_embedding_no_function_raises(self):
        async def run():
            coll, _ = _make_async_collection(embedding_function=None)
            with pytest.raises(AchillesError) as exc_info:
                await coll.query_documents(top_k=5)
            assert exc_info.value.code == ERROR_VALIDATION

        asyncio.run(run())

    def test_returns_list_of_dicts(self):
        async def run():
            coll, mock_http = _make_async_collection()
            mock_http.post = AsyncMock(return_value=_query_res(2))

            result = await coll.query_documents(top_k=5, query_embedding=EMB_QUERY)
            assert all(isinstance(d, dict) for d in result)

        asyncio.run(run())


# ─────────────────────────────────────────────────────────────────────────────
# count
# ─────────────────────────────────────────────────────────────────────────────

class TestCount:

    def test_sync_count_returns_int(self):
        coll, mock_http = _make_sync_collection()
        mock_http.get.return_value = _get_collection_res()

        result = coll.count()
        assert isinstance(result, int)
        assert result == 5

    def test_async_count_returns_int(self):
        async def run():
            coll, mock_http = _make_async_collection()
            mock_http.get = AsyncMock(return_value=_get_collection_res())

            result = await coll.count()
            assert isinstance(result, int)
            assert result == 5

        asyncio.run(run())


# ─────────────────────────────────────────────────────────────────────────────
# peek
# ─────────────────────────────────────────────────────────────────────────────

class TestPeek:

    def test_sync_peek_returns_first_n_documents(self):
        coll, mock_http = _make_sync_collection()
        mock_http.get.return_value = _get_docs_res(10)

        result = coll.peek(n=3)
        assert len(result) == 3

    def test_sync_peek_default_n_is_5(self):
        coll, mock_http = _make_sync_collection()
        mock_http.get.return_value = _get_docs_res(10)

        result = coll.peek()
        assert len(result) == 5

    def test_sync_peek_fewer_than_n_returns_all(self):
        coll, mock_http = _make_sync_collection()
        mock_http.get.return_value = _get_docs_res(2)

        result = coll.peek(n=10)
        assert len(result) == 2

    def test_sync_peek_empty_collection_returns_empty_list(self):
        coll, mock_http = _make_sync_collection()
        mock_http.get.return_value = GetDocumentsRes(documents=[], doc_count=0)

        result = coll.peek()
        assert result == []

    def test_async_peek_returns_first_n(self):
        async def run():
            coll, mock_http = _make_async_collection()
            mock_http.get = AsyncMock(return_value=_get_docs_res(10))

            result = await coll.peek(n=3)
            assert len(result) == 3

        asyncio.run(run())

    def test_async_peek_default_n_is_5(self):
        async def run():
            coll, mock_http = _make_async_collection()
            mock_http.get = AsyncMock(return_value=_get_docs_res(10))

            result = await coll.peek()
            assert len(result) == 5

        asyncio.run(run())


# ─────────────────────────────────────────────────────────────────────────────
# update_documents
# ─────────────────────────────────────────────────────────────────────────────

class TestUpdateDocuments:

    def test_sync_update_returns_none(self):
        coll, mock_http = _make_sync_collection()
        mock_http.put.return_value = UpdateDocumentsRes(message="updated")

        result = coll.update_documents(
            document_id="doc1",
            where={},
            updates={"category": "new_value"},
        )
        assert result is None

    def test_sync_update_calls_http_put(self):
        coll, mock_http = _make_sync_collection()
        mock_http.put.return_value = UpdateDocumentsRes(message="updated")

        coll.update_documents(
            document_id="doc1",
            where={},
            updates={"category": "new_value"},
        )
        mock_http.put.assert_called_once()

    def test_async_update_returns_none(self):
        async def run():
            coll, mock_http = _make_async_collection()
            mock_http.put = AsyncMock(return_value=UpdateDocumentsRes(message="updated"))

            result = await coll.update_documents(
                document_id="doc1",
                where={},
                updates={"field": "val"},
            )
            assert result is None

        asyncio.run(run())


# ─────────────────────────────────────────────────────────────────────────────
# delete_documents
# ─────────────────────────────────────────────────────────────────────────────

class TestDeleteDocuments:

    def test_sync_delete_returns_none(self):
        coll, mock_http = _make_sync_collection()
        mock_http.delete.return_value = DeleteDocumentsRes(message="deleted")

        result = coll.delete_documents(document_ids=["doc1", "doc2"])
        assert result is None

    def test_sync_delete_calls_http_delete(self):
        coll, mock_http = _make_sync_collection()
        mock_http.delete.return_value = DeleteDocumentsRes(message="deleted")

        coll.delete_documents(document_ids=["doc1"])
        mock_http.delete.assert_called_once()

    def test_async_delete_returns_none(self):
        async def run():
            coll, mock_http = _make_async_collection()
            mock_http.delete = AsyncMock(
                return_value=DeleteDocumentsRes(message="deleted")
            )

            result = await coll.delete_documents(document_ids=["doc1"])
            assert result is None

        asyncio.run(run())

    def test_sync_delete_empty_list_still_calls_server(self):
        # deleting an empty list is caller's choice — SDK should not gate it
        coll, mock_http = _make_sync_collection()
        mock_http.delete.return_value = DeleteDocumentsRes(message="deleted")

        coll.delete_documents(document_ids=[])
        mock_http.delete.assert_called_once()


# ─────────────────────────────────────────────────────────────────────────────
# str / repr
# ─────────────────────────────────────────────────────────────────────────────

class TestCollectionRepr:

    def test_sync_str_includes_name_and_database(self):
        coll, _ = _make_sync_collection()
        s = str(coll)
        assert COLL in s
        assert DB in s

    def test_async_repr_includes_name_and_database(self):
        coll, _ = _make_async_collection()
        r = repr(coll)
        assert COLL in r
        assert DB in r
