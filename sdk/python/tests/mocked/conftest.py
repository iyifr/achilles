import pytest
from unittest.mock import MagicMock, AsyncMock
from datetime import datetime

from achillesdb.http.connection import SyncHttpClient, AsyncHttpClient
from achillesdb.schemas import (
    CollectionCatalogEntry,
    CollectionStats,
    CreateCollectionRes,
    CreateDatabaseRes,
    DeleteCollectionRes,
    DeleteDatabaseRes,
    DeleteDocumentsRes,
    GetCollectionRes,
    GetCollectionsRes,
    GetDatabasesRes,
    GetDocumentsRes,
    InsertDocumentsRes,
    UpdateDocumentsRes,
    QueryRes,
    DatabaseInfo,
    Document,
)


# ── fake data builders ────────────────────────────────────────────────────────

def make_collection_entry(name="test_collection", db="test_db"):
    return CollectionCatalogEntry(
        **{
            "_id": "abc123",
            "ns": f"{db}.{name}",
            "table_uri": "s3://table",
            "vector_index_uri": "s3://index",
            "createdAt": datetime.now(),
            "updatedAt": datetime.now(),
        }
    )


def make_get_collection_res(name="test_collection", db="test_db"):
    return GetCollectionRes(
        collection=make_collection_entry(name, db),
        stats=CollectionStats(doc_count=0, vector_index_size=0.0),
    )


def make_get_collections_res(names=None, db="test_db"):
    names = names or ["test_collection"]
    return GetCollectionsRes(
        collections=[make_collection_entry(n, db) for n in names],
        collection_count=len(names),
    )


def make_get_documents_res(docs=None):
    docs = docs or [
        Document(id="doc-1", content="hello", metadata={"year": 2021}),
        Document(id="doc-2", content="world", metadata={"year": 2022}),
    ]
    return GetDocumentsRes(documents=docs, doc_count=len(docs))


def make_query_res(docs=None):
    docs = docs or [
        Document(id="doc-1", content="hello", metadata={}, distance=0.1),
        Document(id="doc-2", content="world", metadata={}, distance=0.2),
    ]
    return QueryRes(documents=docs, doc_count=len(docs))


# ── sync http client mock ─────────────────────────────────────────────────────

@pytest.fixture
def mock_sync_http():
    client = MagicMock(spec=SyncHttpClient)
    client.get = MagicMock()
    client.post = MagicMock()
    client.put = MagicMock()
    client.delete = MagicMock()
    return client


# ── async http client mock ────────────────────────────────────────────────────

@pytest.fixture
def mock_async_http():
    client = MagicMock(spec=AsyncHttpClient)
    client.get = AsyncMock()
    client.post = AsyncMock()
    client.put = AsyncMock()
    client.delete = AsyncMock()
    return client


# ── pre-built fake responses ──────────────────────────────────────────────────

@pytest.fixture
def fake_get_collection_res():
    return make_get_collection_res()


@pytest.fixture
def fake_get_collections_res():
    return make_get_collections_res()


@pytest.fixture
def fake_get_documents_res():
    return make_get_documents_res()


@pytest.fixture
def fake_query_res():
    return make_query_res()


@pytest.fixture
def fake_create_collection_res():
    return CreateCollectionRes(message="collection created")


@pytest.fixture
def fake_delete_collection_res():
    return DeleteCollectionRes(message="collection deleted")


@pytest.fixture
def fake_create_database_res():
    return CreateDatabaseRes(message="database created")


@pytest.fixture
def fake_get_databases_res():
    return GetDatabasesRes(
        databases=[DatabaseInfo(name="test_db", collectionCount=1, empty=False)],
        db_count=1,
    )


@pytest.fixture
def fake_insert_res():
    return InsertDocumentsRes(message="documents inserted")


@pytest.fixture
def fake_update_res():
    return UpdateDocumentsRes(message="documents updated")


@pytest.fixture
def fake_delete_documents_res():
    return DeleteDocumentsRes(message="documents deleted")
