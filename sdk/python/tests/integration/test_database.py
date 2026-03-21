import pytest
from achillesdb.collection import SyncCollection
from achillesdb.errors import AchillesError
from tests.integration.conftest import skip_if_no_server


@skip_if_no_server
@pytest.mark.integration
class TestSyncDatabaseIntegration:

    def test_list_collections_returns_list(self, sync_db):
        result = sync_db.list_collections()
        assert isinstance(result, list)

    def test_create_collection_returns_sync_collection(self, sync_db):
        import uuid
        name = f"my_collection_{uuid.uuid4().hex[:8]}"
        coll = sync_db.create_collection(name)
        assert isinstance(coll, SyncCollection)
        assert coll.name == name

    def test_create_collection_appears_in_list(self, sync_db):
        import uuid
        name = f"listed_collection_{uuid.uuid4().hex[:8]}"
        sync_db.create_collection(name)
        collections = sync_db.list_collections()
        names = [c.name for c in collections]
        assert name in names

    def test_get_collection_returns_sync_collection(self, sync_db):
        import uuid
        name = f"fetched_collection_{uuid.uuid4().hex[:8]}"
        sync_db.create_collection(name)
        coll = sync_db.get_collection(name)
        assert isinstance(coll, SyncCollection)
        assert coll.name == name

    def test_get_collection_not_found_raises(self, sync_db):
        with pytest.raises(AchillesError):
            sync_db.get_collection("does_not_exist_xyz")

    def test_delete_collection_removes_it(self, sync_db):
        import uuid
        name = f"to_delete_{uuid.uuid4().hex[:8]}"
        sync_db.create_collection(name)
        sync_db.delete_collection(name)
        collections = sync_db.list_collections()
        names = [c.name for c in collections]
        assert name not in names

    def test_collection_handle_returns_sync_collection(self, sync_db):
        import uuid
        name = f"handle_collection_{uuid.uuid4().hex[:8]}"
        sync_db.create_collection(name)
        coll = sync_db.collection(name)
        assert isinstance(coll, SyncCollection)
