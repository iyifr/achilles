import pytest
from unittest.mock import MagicMock, patch, AsyncMock

from achillesdb.client import AchillesClient, AsyncAchillesClient
from achillesdb.database import SyncDatabase, AsyncDatabase
from achillesdb.errors import AchillesError, ERROR_CONNECTION


# ─────────────────────────────────────────────────────────────────────────────
# SyncClient
# ─────────────────────────────────────────────────────────────────────────────

class TestSyncClient:

    @pytest.fixture
    def client(self, mock_sync_http):
        with patch("achillesdb.client.SyncHttpClient", return_value=mock_sync_http):
            c = AchillesClient(host="localhost", port=8180)
            c._http = mock_sync_http
            c.database_api = MagicMock()
            yield c

    def test_ping_returns_true_on_success(self, client, fake_get_databases_res):
        client.database_api.list_databases.return_value = fake_get_databases_res
        assert client.ping() is True

    def test_ping_returns_false_on_error(self, client):
        client.database_api.list_databases.side_effect = AchillesError(
            "conn failed", code=ERROR_CONNECTION
        )
        assert client.ping() is False

    def test_create_database_returns_sync_database(self, client, fake_create_database_res):
        client.database_api.create_database.return_value = fake_create_database_res
        result = client.create_database("mydb")
        assert isinstance(result, SyncDatabase)

    def test_list_databases_returns_list_of_sync_databases(self, client, fake_get_databases_res):
        client.database_api.list_databases.return_value = fake_get_databases_res
        result = client.list_databases()
        assert isinstance(result, list)
        assert all(isinstance(db, SyncDatabase) for db in result)

    def test_database_returns_sync_database(self, client):
        result = client.database("mydb")
        assert isinstance(result, SyncDatabase)

    def test_database_returns_cached_instance(self, client):
        db1 = client.database("mydb")
        db2 = client.database("mydb")
        assert db1 is db2

    def test_database_different_names_different_instances(self, client):
        db1 = client.database("db1")
        db2 = client.database("db2")
        assert db1 is not db2

    def test_delete_database_calls_api(self, client):
        client.database_api.delete_database.return_value = None
        client.delete_database("mydb")
        client.database_api.delete_database.assert_called_once_with("mydb")

    def test_context_manager_calls_close(self, mock_sync_http):
        with patch("achillesdb.client.SyncHttpClient", return_value=mock_sync_http):
            with AchillesClient(host="localhost", port=8180) as c:
                c._http = mock_sync_http
            mock_sync_http.close.assert_called_once()

    def test_str_includes_host_and_port(self, client):
        result = str(client)
        assert "localhost" in result
        assert "8180" in result


# ─────────────────────────────────────────────────────────────────────────────
# AsyncClient
# ─────────────────────────────────────────────────────────────────────────────

class TestAsyncClient:

    @pytest.fixture
    def client(self, mock_async_http):
        with patch("achillesdb.client.AsyncHttpClient", return_value=mock_async_http):
            c = AsyncAchillesClient(host="localhost", port=8180)
            c._http = mock_async_http
            c.database_api = MagicMock()
            c.database_api.list_databases = AsyncMock()
            c.database_api.create_database = AsyncMock()
            c.database_api.delete_database = AsyncMock()
            yield c

    @pytest.mark.asyncio
    async def test_ping_returns_true_on_success(self, client, fake_get_databases_res):
        client.database_api.list_databases.return_value = fake_get_databases_res
        result = await client.ping()
        assert result is True

    @pytest.mark.asyncio
    async def test_ping_returns_false_on_error(self, client):
        client.database_api.list_databases.side_effect = AchillesError(
            "conn failed", code=ERROR_CONNECTION
        )
        result = await client.ping()
        assert result is False

    @pytest.mark.asyncio
    async def test_create_database_returns_async_database(self, client, fake_create_database_res):
        client.database_api.create_database.return_value = fake_create_database_res
        result = await client.create_database("mydb")
        assert isinstance(result, AsyncDatabase)

    @pytest.mark.asyncio
    async def test_list_databases_returns_list_of_async_databases(self, client, fake_get_databases_res):
        client.database_api.list_databases.return_value = fake_get_databases_res
        result = await client.list_databases()
        assert isinstance(result, list)
        assert all(isinstance(db, AsyncDatabase) for db in result)

    def test_database_returns_async_database(self, client):
        result = client.database("mydb")
        assert isinstance(result, AsyncDatabase)

    def test_database_returns_cached_instance(self, client):
        db1 = client.database("mydb")
        db2 = client.database("mydb")
        assert db1 is db2

    @pytest.mark.asyncio
    async def test_delete_database_calls_api(self, client):
        client.database_api.delete_database.return_value = None
        await client.delete_database("mydb")
        client.database_api.delete_database.assert_called_once_with("mydb")

    @pytest.mark.asyncio
    async def test_async_context_manager_calls_aclose(self, mock_async_http):
        mock_async_http.aclose = AsyncMock()
        with patch("achillesdb.client.AsyncHttpClient", return_value=mock_async_http):
            async with AsyncAchillesClient(host="localhost", port=8180) as c:
                c._http = mock_async_http
            mock_async_http.aclose.assert_called_once()
