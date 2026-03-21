import pytest
from achillesdb import AchillesClient
from achillesdb.database import SyncDatabase
from achillesdb.errors import AchillesError
from tests.integration.conftest import skip_if_no_server, TEST_DB_PREFIX


@skip_if_no_server
@pytest.mark.integration
class TestSyncClientIntegration:

    def test_ping_returns_true(self, sync_client):
        assert sync_client.ping() is True

    def test_list_databases_returns_list(self, sync_client):
        result = sync_client.list_databases()
        assert isinstance(result, list)

    def test_create_database_returns_sync_database(self, sync_client, test_db_name):
        try:
            db = sync_client.create_database(test_db_name)
            assert isinstance(db, SyncDatabase)
            assert db.name == test_db_name
        finally:
            try:
                sync_client.delete_database(test_db_name)
            except AchillesError:
                pass

    def test_create_database_appears_in_list(self, sync_client, test_db_name):
        try:
            sync_client.create_database(test_db_name)
            databases = sync_client.list_databases()
            names = [db.name for db in databases]
            assert test_db_name in names
        finally:
            try:
                sync_client.delete_database(test_db_name)
            except AchillesError:
                pass

    def test_delete_database_removes_it_from_list(self, sync_client, test_db_name):
        sync_client.create_database(test_db_name)
        sync_client.delete_database(test_db_name)
        databases = sync_client.list_databases()
        names = [db.name for db in databases]
        assert test_db_name not in names

    def test_database_handle_no_network_call(self, sync_client):
        # database() should return a handle without making any server calls
        db = sync_client.database("some_db")
        assert isinstance(db, SyncDatabase)

    def test_database_handle_is_cached(self, sync_client):
        db1 = sync_client.database("cache_test_db")
        db2 = sync_client.database("cache_test_db")
        assert db1 is db2
