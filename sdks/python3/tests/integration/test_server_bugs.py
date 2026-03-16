"""
tests/integration/test_server_bugs.py
======================================
This file contains tests that reproduce identified bugs on the AchillesDB server API.
These tests use the internal HTTP client to bypass the SDK validations and normalization
to demonstrate the raw behavior of the server.

They are skipped by default. To run them and check if the upstream server has fixed them:
    uv run pytest tests/integration/test_server_bugs.py --run-server-bugs
"""

import pytest
import uuid

from achillesdb.errors import AchillesError, ERROR_CONFLICT
from tests.integration.conftest import skip_if_no_server


@skip_if_no_server
@pytest.mark.integration
@pytest.mark.server_bug
class TestAchillesServerBugs:

    def test_ghost_collection_conflict(self, sync_client, test_db_name):
        """
        Bug 1: Deleting a collection leaves a "ghost", causing 409 Conflict
        when trying to recreate a collection with the exact same name.
        """
        db = sync_client.create_database(test_db_name)
        
        # 1. Create a collection
        collection_name = f"ghost_test_{uuid.uuid4().hex[:8]}"
        coll_api = db._collection_api
        
        coll_api.create_collection(
            input=type("DummyReq", (), {"name": collection_name, "model_dump": lambda *a, **kw: {"name": collection_name}})()
        )
        
        # 2. Delete the collection
        coll_api.delete_collection(collection_name)
        
        # 3. Try to create the collection again immediately
        # The server currently incorrectly returns 409 CONFLICT
        with pytest.raises(AchillesError) as exc_info:
            coll_api.create_collection(
                input=type("DummyReq", (), {"name": collection_name, "model_dump": lambda *a, **kw: {"name": collection_name}})()
            )
        
        assert exc_info.value.code == ERROR_CONFLICT

    def test_null_array_in_get_collections(self, sync_client, test_db_name):
        """
        Bug 2: GET /collections returns 'null' instead of '[]' when empty.
        """
        # Create an empty db
        db = sync_client.create_database(test_db_name)
        
        # Use the raw HTTP client to bypass SDK normalization
        http = sync_client._http
        
        # Hit the raw endpoint
        response = http._request_sync(
            "GET", 
            f"/database/{test_db_name}/collections", 
            resType=None, 
            expected_status=200
        )
        
        # Expecting {"collections": null, "collection_count": 0}
        assert "collections" in response
        assert response["collections"] is None  # This should ideally be []

    def test_duplicate_document_ids_silently_accepted(self, sync_client, test_db_name):
        """
        Bug 3: POST /documents silently accepts duplicate IDs instead of rejecting them.
        """
        db = sync_client.create_database(test_db_name)
        collection_name = f"dup_test_{uuid.uuid4().hex[:8]}"
        
        # create collection through raw http
        http = sync_client._http
        http.post(
            f"/database/{test_db_name}/collections",
            resType=None,
            json={"name": collection_name},
            expected_status=200
        )
        
        # Prepare payload with exactly the same ID repeated
        payload = {
            "ids": ["duplicate-1", "duplicate-1"],
            "documents": ["first instance", "second instance"],
            "embeddings": [[0.1, 0.2], [0.1, 0.2]],
            "metadatas": [{"a": 1}, {"b": 2}]
        }
        
        # The server accepts this payload and returns 200 OK
        # Ideally it should return 400 Bad Request
        response = http.post(
            f"/database/{test_db_name}/collections/{collection_name}/documents",
            resType=None,
            json=payload,
            expected_status=200
        )
        
        assert "message" in response
