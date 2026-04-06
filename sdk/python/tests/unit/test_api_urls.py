"""
tests/unit/test_api_urls.py
============================
Unit tests for the API layer:
  - achillesdb/api/database.py
  - achillesdb/api/collection.py
  - achillesdb/api/document.py

What we test here and why
--------------------------
The API classes are thin wrappers that do two things:
  1. Construct the correct URL path for each endpoint
  2. Call validate_name() before making the HTTP call (where applicable)

URL correctness is NOT covered by test_collection_logic.py — those tests
only assert that mock_http.post was called, not what URL it was called with.
A regression that changes `/database/{db}/collections/{col}/documents`
to `/database/{col}/documents` would pass all collection logic tests but
fail here immediately.

validate_name() delegation is also tested here — it is the API layer's
responsibility to reject bad names before an HTTP call goes out.

Mock strategy
-------------
Each API class is constructed with a mocked SyncHttpClient / AsyncHttpClient.
We inspect the first positional argument to each HTTP method call — that is
always the URL path.
"""

import asyncio
import pytest
from unittest.mock import MagicMock, AsyncMock, call

from achillesdb.api.database import SyncDatabaseApi, AsyncDatabaseApi
from achillesdb.api.collection import SyncCollectionApi, AsyncCollectionApi
from achillesdb.api.document import SyncDocumentApi, AsyncDocumentApi
from achillesdb.http.connection import SyncHttpClient, AsyncHttpClient
from achillesdb.schemas import (
    CreateCollectionReqInput,
    CreateDatabaseRes,
    DeleteDatabaseRes,
    GetDatabasesRes,
    DatabaseInfo,
    CreateCollectionRes,
    DeleteCollectionRes,
    GetCollectionRes,
    GetCollectionsRes,
    InsertDocumentReqInput,
    InsertDocumentsRes,
    GetDocumentsRes,
    UpdateDocumentsReqInput,
    UpdateDocumentsRes,
    DeleteDocumentsReqInput,
    DeleteDocumentsRes,
    QueryReqInput,
    QueryRes,
    CollectionCatalogEntry,
    CollectionStats,
)
from achillesdb.errors import AchillesError


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

DB = "mydb"
COLL = "mycollection"


def _sync_http() -> MagicMock:
    mock = MagicMock(spec=SyncHttpClient)
    mock.mode = "sync"
    return mock


def _async_http() -> MagicMock:
    mock = MagicMock(spec=AsyncHttpClient)
    mock.mode = "async"
    return mock


def _url_arg(mock_method) -> str:
    """Extract the URL path from the first call to a mock HTTP method."""
    return mock_method.call_args[0][0]


def _get_databases_res() -> GetDatabasesRes:
    return GetDatabasesRes(
        databases=[DatabaseInfo(name=DB, collectionCount=0, empty=True)],
        db_count=1,
    )


def _get_collections_res() -> GetCollectionsRes:
    from datetime import datetime, timezone
    entry = CollectionCatalogEntry(**{
        "_id": "cid1",
        "ns": f"{DB}.{COLL}",
        "table_uri": "s3://t",
        "vector_index_uri": "s3://v",
        "createdAt": datetime.now(tz=timezone.utc).isoformat(),
        "updatedAt": datetime.now(tz=timezone.utc).isoformat(),
    })
    return GetCollectionsRes(collections=[entry], collection_count=1)


def _get_collection_res() -> GetCollectionRes:
    from datetime import datetime, timezone
    entry = CollectionCatalogEntry(**{
        "_id": "cid1",
        "ns": f"{DB}.{COLL}",
        "table_uri": "s3://t",
        "vector_index_uri": "s3://v",
        "createdAt": datetime.now(tz=timezone.utc).isoformat(),
        "updatedAt": datetime.now(tz=timezone.utc).isoformat(),
    })
    return GetCollectionRes(
        collection=entry,
        stats=CollectionStats(doc_count=0, vector_index_size=0.0),
    )


def _insert_input() -> InsertDocumentReqInput:
    return InsertDocumentReqInput(
        ids=["d1"],
        documents=["content"],
        embeddings=[[0.1, 0.2]],
        metadatas=[{}],
    )


def _update_input() -> UpdateDocumentsReqInput:
    return UpdateDocumentsReqInput(
        document_id="d1",
        updates={"field": "value"},
    )


def _delete_input() -> DeleteDocumentsReqInput:
    return DeleteDocumentsReqInput(document_ids=["d1"])


def _query_input() -> QueryReqInput:
    return QueryReqInput(query_embedding=[0.1, 0.2], top_k=5)


# ─────────────────────────────────────────────────────────────────────────────
# SyncDatabaseApi — URL paths
# ─────────────────────────────────────────────────────────────────────────────

class TestSyncDatabaseApiUrls:

    def setup_method(self):
        self.http = _sync_http()
        self.api = SyncDatabaseApi(http_client=self.http)

    def test_create_database_url(self):
        self.http.post.return_value = CreateDatabaseRes(message="ok")
        self.api.create_database(DB)
        assert _url_arg(self.http.post) == "/database"

    def test_list_databases_url(self):
        self.http.get.return_value = _get_databases_res()
        self.api.list_databases()
        assert _url_arg(self.http.get) == "/databases"

    def test_delete_database_url_includes_name(self):
        self.http.delete.return_value = DeleteDatabaseRes(message="ok")
        self.api.delete_database(DB)
        assert _url_arg(self.http.delete) == f"/database/{DB}"

    def test_create_database_uses_post(self):
        self.http.post.return_value = CreateDatabaseRes(message="ok")
        self.api.create_database(DB)
        self.http.post.assert_called_once()
        self.http.get.assert_not_called()

    def test_list_databases_uses_get(self):
        self.http.get.return_value = _get_databases_res()
        self.api.list_databases()
        self.http.get.assert_called_once()
        self.http.post.assert_not_called()

    def test_delete_database_uses_delete(self):
        self.http.delete.return_value = DeleteDatabaseRes(message="ok")
        self.api.delete_database(DB)
        self.http.delete.assert_called_once()

    # ── validate_name called before HTTP ─────────────────────────────────────

    def test_create_database_empty_name_raises_before_http(self):
        with pytest.raises(ValueError):
            self.api.create_database("")
        self.http.post.assert_not_called()

    def test_create_database_invalid_name_raises_before_http(self):
        with pytest.raises(ValueError):
            self.api.create_database("bad-name")
        self.http.post.assert_not_called()

    def test_delete_database_empty_name_raises_before_http(self):
        with pytest.raises(ValueError):
            self.api.delete_database("")
        self.http.delete.assert_not_called()

    def test_delete_database_invalid_name_raises_before_http(self):
        with pytest.raises(ValueError):
            self.api.delete_database("bad name")
        self.http.delete.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# AsyncDatabaseApi — URL paths
# ─────────────────────────────────────────────────────────────────────────────

class TestAsyncDatabaseApiUrls:

    def test_create_database_url(self):
        async def run():
            http = _async_http()
            http.post = AsyncMock(return_value=CreateDatabaseRes(message="ok"))
            api = AsyncDatabaseApi(http_client=http)
            await api.create_database(DB)
            assert _url_arg(http.post) == "/database"
        asyncio.run(run())

    def test_list_databases_url(self):
        async def run():
            http = _async_http()
            http.get = AsyncMock(return_value=_get_databases_res())
            api = AsyncDatabaseApi(http_client=http)
            await api.list_databases()
            assert _url_arg(http.get) == "/databases"
        asyncio.run(run())

    def test_delete_database_url_includes_name(self):
        async def run():
            http = _async_http()
            http.delete = AsyncMock(return_value=DeleteDatabaseRes(message="ok"))
            api = AsyncDatabaseApi(http_client=http)
            await api.delete_database(DB)
            assert _url_arg(http.delete) == f"/database/{DB}"
        asyncio.run(run())

    def test_create_database_invalid_name_raises_before_http(self):
        async def run():
            http = _async_http()
            api = AsyncDatabaseApi(http_client=http)
            with pytest.raises(ValueError):
                await api.create_database("bad-name")
            http.post.assert_not_called()
        asyncio.run(run())


# ─────────────────────────────────────────────────────────────────────────────
# SyncCollectionApi — URL paths
# ─────────────────────────────────────────────────────────────────────────────

class TestSyncCollectionApiUrls:

    def setup_method(self):
        self.http = _sync_http()
        self.api = SyncCollectionApi(http_client=self.http, database_name=DB)

    def test_list_collections_url(self):
        self.http.get.return_value = _get_collections_res()
        self.api.list_collections()
        assert _url_arg(self.http.get) == f"/database/{DB}/collections"

    def test_get_collection_url_includes_collection_name(self):
        self.http.get.return_value = _get_collection_res()
        self.api.get_collection(COLL)
        assert _url_arg(self.http.get) == f"/database/{DB}/collections/{COLL}"

    def test_create_collection_url(self):
        self.http.post.return_value = CreateCollectionRes(message="ok")
        self.api.create_collection(CreateCollectionReqInput(name=COLL))
        assert _url_arg(self.http.post) == f"/database/{DB}/collections"

    def test_delete_collection_url_includes_collection_name(self):
        self.http.delete.return_value = DeleteCollectionRes(message="ok")
        self.api.delete_collection(COLL)
        assert _url_arg(self.http.delete) == f"/database/{DB}/collections/{COLL}"

    def test_list_collections_uses_get(self):
        self.http.get.return_value = _get_collections_res()
        self.api.list_collections()
        self.http.get.assert_called_once()

    def test_create_collection_uses_post(self):
        self.http.post.return_value = CreateCollectionRes(message="ok")
        self.api.create_collection(CreateCollectionReqInput(name=COLL))
        self.http.post.assert_called_once()

    def test_delete_collection_uses_delete(self):
        self.http.delete.return_value = DeleteCollectionRes(message="ok")
        self.api.delete_collection(COLL)
        self.http.delete.assert_called_once()

    # ── validate_name called before HTTP ─────────────────────────────────────

    def test_get_collection_empty_name_raises_before_http(self):
        with pytest.raises(ValueError):
            self.api.get_collection("")
        self.http.get.assert_not_called()

    def test_get_collection_invalid_name_raises_before_http(self):
        with pytest.raises(ValueError):
            self.api.get_collection("bad-name")
        self.http.get.assert_not_called()

    def test_create_collection_empty_name_raises_before_http(self):
        with pytest.raises(ValueError):
            self.api.create_collection(CreateCollectionReqInput(name=""))
        with pytest.raises(ValueError):
            self.api.create_collection(CreateCollectionReqInput(name="bad name"))
        self.http.post.assert_not_called()

    def test_delete_collection_empty_name_raises_before_http(self):
        with pytest.raises(ValueError):
            self.api.delete_collection("")
        self.http.delete.assert_not_called()

    def test_delete_collection_invalid_name_raises_before_http(self):
        with pytest.raises(ValueError):
            self.api.delete_collection("bad name")
        self.http.delete.assert_not_called()

    # ── database_name embedded correctly in all paths ─────────────────────────

    def test_database_name_in_list_collections_url(self):
        self.http.get.return_value = _get_collections_res()
        self.api.list_collections()
        assert DB in _url_arg(self.http.get)

    def test_database_name_in_get_collection_url(self):
        self.http.get.return_value = _get_collection_res()
        self.api.get_collection(COLL)
        url = _url_arg(self.http.get)
        assert DB in url
        assert COLL in url

    def test_different_database_names_produce_different_urls(self):
        api_other = SyncCollectionApi(http_client=self.http, database_name="otherdb")
        self.http.get.return_value = _get_collections_res()
        api_other.list_collections()
        url = _url_arg(self.http.get)
        assert "otherdb" in url
        assert DB not in url


# ─────────────────────────────────────────────────────────────────────────────
# AsyncCollectionApi — URL paths
# ─────────────────────────────────────────────────────────────────────────────

class TestAsyncCollectionApiUrls:

    def test_list_collections_url(self):
        async def run():
            http = _async_http()
            http.get = AsyncMock(return_value=_get_collections_res())
            api = AsyncCollectionApi(http_client=http, database_name=DB)
            await api.list_collections()
            assert _url_arg(http.get) == f"/database/{DB}/collections"
        asyncio.run(run())

    def test_get_collection_url(self):
        async def run():
            http = _async_http()
            http.get = AsyncMock(return_value=_get_collection_res())
            api = AsyncCollectionApi(http_client=http, database_name=DB)
            await api.get_collection(COLL)
            assert _url_arg(http.get) == f"/database/{DB}/collections/{COLL}"
        asyncio.run(run())

    def test_create_collection_url(self):
        async def run():
            http = _async_http()
            http.post = AsyncMock(return_value=CreateCollectionRes(message="ok"))
            api = AsyncCollectionApi(http_client=http, database_name=DB)
            await api.create_collection(CreateCollectionReqInput(name=COLL))
            assert _url_arg(http.post) == f"/database/{DB}/collections"
        asyncio.run(run())

    def test_delete_collection_url(self):
        async def run():
            http = _async_http()
            http.delete = AsyncMock(return_value=DeleteCollectionRes(message="ok"))
            api = AsyncCollectionApi(http_client=http, database_name=DB)
            await api.delete_collection(COLL)
            assert _url_arg(http.delete) == f"/database/{DB}/collections/{COLL}"
        asyncio.run(run())

    def test_invalid_collection_name_raises_before_http(self):
        async def run():
            http = _async_http()
            api = AsyncCollectionApi(http_client=http, database_name=DB)
            with pytest.raises(ValueError):
                await api.get_collection("bad-name")
            http.get.assert_not_called()
        asyncio.run(run())


# ─────────────────────────────────────────────────────────────────────────────
# SyncDocumentApi — URL paths
# ─────────────────────────────────────────────────────────────────────────────

class TestSyncDocumentApiUrls:

    def setup_method(self):
        self.http = _sync_http()
        self.api = SyncDocumentApi(
            http_client=self.http,
            database_name=DB,
            collection_name=COLL,
        )
        self.base = f"/database/{DB}/collections/{COLL}/documents"

    def test_get_documents_url(self):
        self.http.get.return_value = GetDocumentsRes(documents=[], doc_count=0)
        self.api.get_documents()
        assert _url_arg(self.http.get) == self.base

    def test_insert_documents_url(self):
        self.http.post.return_value = InsertDocumentsRes(message="ok")
        self.api.insert_documents(_insert_input())
        assert _url_arg(self.http.post) == self.base

    def test_update_documents_url(self):
        self.http.put.return_value = UpdateDocumentsRes(message="ok")
        self.api.update_documents(_update_input())
        assert _url_arg(self.http.put) == self.base

    def test_delete_documents_url(self):
        self.http.delete.return_value = DeleteDocumentsRes(deleted_count=1, deleted_ids=["mock_doc_id"])
        self.api.delete_documents(_delete_input())
        assert _url_arg(self.http.delete) == self.base

    def test_query_url(self):
        self.http.post.return_value = QueryRes(documents=[], doc_count=0)
        self.api.query(_query_input())
        assert _url_arg(self.http.post) == f"{self.base}/query"

    # ── correct HTTP methods ──────────────────────────────────────────────────

    def test_get_documents_uses_get(self):
        self.http.get.return_value = GetDocumentsRes(documents=[], doc_count=0)
        self.api.get_documents()
        self.http.get.assert_called_once()
        self.http.post.assert_not_called()

    def test_insert_documents_uses_post(self):
        self.http.post.return_value = InsertDocumentsRes(message="ok")
        self.api.insert_documents(_insert_input())
        self.http.post.assert_called_once()
        self.http.put.assert_not_called()

    def test_update_documents_uses_put(self):
        self.http.put.return_value = UpdateDocumentsRes(message="ok")
        self.api.update_documents(_update_input())
        self.http.put.assert_called_once()
        self.http.post.assert_not_called()

    def test_delete_documents_uses_delete(self):
        self.http.delete.return_value = DeleteDocumentsRes(deleted_count=1, deleted_ids=["mock_doc_id"])
        self.api.delete_documents(_delete_input())
        self.http.delete.assert_called_once()

    def test_query_uses_post(self):
        self.http.post.return_value = QueryRes(documents=[], doc_count=0)
        self.api.query(_query_input())
        self.http.post.assert_called_once()

    # ── db and collection names embedded correctly ────────────────────────────

    def test_db_and_collection_name_in_all_urls(self):
        """All document endpoints must embed both db and collection name."""
        self.http.get.return_value = GetDocumentsRes(documents=[], doc_count=0)
        self.http.post.return_value = InsertDocumentsRes(message="ok")
        self.http.put.return_value = UpdateDocumentsRes(message="ok")
        self.http.delete.return_value = DeleteDocumentsRes(deleted_count=1, deleted_ids=["mock_doc_id"])

        self.api.get_documents()
        self.api.insert_documents(_insert_input())
        self.api.update_documents(_update_input())
        self.api.delete_documents(_delete_input())

        for method, mock in [
            ("get", self.http.get),
            ("post", self.http.post),
            ("put", self.http.put),
            ("delete", self.http.delete),
        ]:
            url = _url_arg(mock)
            assert DB in url, f"{method} URL missing database name: {url}"
            assert COLL in url, f"{method} URL missing collection name: {url}"

    def test_different_collection_produces_different_url(self):
        api_other = SyncDocumentApi(
            http_client=self.http,
            database_name=DB,
            collection_name="othercoll",
        )
        self.http.get.return_value = GetDocumentsRes(documents=[], doc_count=0)
        api_other.get_documents()
        url = _url_arg(self.http.get)
        assert "othercoll" in url
        assert COLL not in url

    # ── no validate_name in document api ─────────────────────────────────────

    def test_document_api_does_not_validate_name_on_get(self):
        """
        _DocumentApiBase deliberately does not call validate_name —
        it trusts the caller (CollectionImpl) to have validated already.
        Verify this by checking the API accepts any string without raising.
        """
        api = SyncDocumentApi(
            http_client=self.http,
            database_name="any-db",     # would fail validate_name
            collection_name="any-coll", # would fail validate_name
        )
        self.http.get.return_value = GetDocumentsRes(documents=[], doc_count=0)
        # should not raise even with invalid names — no validate_name here
        api.get_documents()
        self.http.get.assert_called_once()


# ─────────────────────────────────────────────────────────────────────────────
# AsyncDocumentApi — URL paths
# ─────────────────────────────────────────────────────────────────────────────

class TestAsyncDocumentApiUrls:

    def _make_api(self, http):
        return AsyncDocumentApi(
            http_client=http,
            database_name=DB,
            collection_name=COLL,
        )

    def test_get_documents_url(self):
        async def run():
            http = _async_http()
            http.get = AsyncMock(return_value=GetDocumentsRes(documents=[], doc_count=0))
            api = self._make_api(http)
            await api.get_documents()
            assert _url_arg(http.get) == f"/database/{DB}/collections/{COLL}/documents"
        asyncio.run(run())

    def test_insert_documents_url(self):
        async def run():
            http = _async_http()
            http.post = AsyncMock(return_value=InsertDocumentsRes(message="ok"))
            api = self._make_api(http)
            await api.insert_documents(_insert_input())
            assert _url_arg(http.post) == f"/database/{DB}/collections/{COLL}/documents"
        asyncio.run(run())

    def test_query_url(self):
        async def run():
            http = _async_http()
            http.post = AsyncMock(return_value=QueryRes(documents=[], doc_count=0))
            api = self._make_api(http)
            await api.query(_query_input())
            base = f"/database/{DB}/collections/{COLL}/documents"
            assert _url_arg(http.post) == f"{base}/query"
        asyncio.run(run())

    def test_update_documents_uses_put(self):
        async def run():
            http = _async_http()
            http.put = AsyncMock(return_value=UpdateDocumentsRes(message="ok"))
            api = self._make_api(http)
            await api.update_documents(_update_input())
            http.put.assert_called_once()
        asyncio.run(run())

    def test_delete_documents_uses_delete(self):
        async def run():
            http = _async_http()
            http.delete = AsyncMock(return_value=DeleteDocumentsRes(deleted_count=1, deleted_ids=["mock_doc_id"]))
            api = self._make_api(http)
            await api.delete_documents(_delete_input())
            http.delete.assert_called_once()
        asyncio.run(run())
