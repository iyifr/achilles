import pytest
from achillesdb import AchillesClient, AsyncAchillesClient
from achillesdb.errors import AchillesError


# ── pytest mark ───────────────────────────────────────────────────────────────

def pytest_addoption(parser):
    parser.addoption(
        "--run-server-bugs", action="store_true", default=False, help="run tests that reproduce known server bugs"
    )

def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "integration: marks tests as integration tests requiring a live AchillesDB server"
    )
    config.addinivalue_line(
        "markers",
        "server_bug: marks tests that verify known server bugs (run with --run-server-bugs)"
    )

def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-server-bugs"):
        return
    skip_server_bug = pytest.mark.skip(reason="need --run-server-bugs option to run server bug tests")
    for item in items:
        if "server_bug" in item.keywords:
            item.add_marker(skip_server_bug)


# ── connection settings ───────────────────────────────────────────────────────

HOST = "localhost"
PORT = 8180

# prefix for all test databases — makes teardown safe and predictable
TEST_DB_PREFIX = "inttest_"


# ── helpers ───────────────────────────────────────────────────────────────────

def _is_server_available() -> bool:
    try:
        client = AchillesClient(host=HOST, port=PORT)
        return client.ping()
    except Exception:
        return False


def _cleanup_test_databases(client: AchillesClient):
    """Delete all databases created during a test run."""
    try:
        databases = client.list_databases()
        for db in databases:
            if db.name.startswith(TEST_DB_PREFIX):
                try:
                    client.delete_database(db.name)
                except AchillesError:
                    pass
    except Exception:
        pass


# ── skip marker ───────────────────────────────────────────────────────────────

skip_if_no_server = pytest.mark.skipif(
    not _is_server_available(),
    reason="AchillesDB server not available at localhost:8180"
)


# ── sync client fixture ───────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def sync_client():
    if not _is_server_available():
        pytest.skip("AchillesDB server not available")
    client = AchillesClient(host=HOST, port=PORT)
    yield client
    _cleanup_test_databases(client)
    client.close()


# ── async client fixture ──────────────────────────────────────────────────────

@pytest.fixture(scope="session")
async def async_client():
    if not _is_server_available():
        pytest.skip("AchillesDB server not available")
    async with AsyncAchillesClient(host=HOST, port=PORT) as client:
        yield client


# ── per-test database fixtures ────────────────────────────────────────────────

@pytest.fixture
def test_db_name(request):
    """Unique database name per test, using test name as suffix."""
    safe_name = request.node.name.replace("[", "_").replace("]", "_").replace("-", "_")
    return f"{TEST_DB_PREFIX}{safe_name}"[:50]  # keep name valid identifier length


@pytest.fixture
def sync_db(sync_client, test_db_name):
    """Create a fresh database for the test, delete it after."""
    try:
        db = sync_client.create_database(test_db_name)
    except AchillesError:
        db = sync_client.database(test_db_name)
    yield db
    try:
        sync_client.delete_database(test_db_name)
    except AchillesError:
        pass


# ── sample data ───────────────────────────────────────────────────────────────

@pytest.fixture
def sample_embeddings():
    return {
        "apple":  [0.1, 0.2, 0.3, 0.4],
        "banana": [0.5, 0.6, 0.7, 0.8],
        "cherry": [0.9, 0.8, 0.7, 0.6],
        "query":  [0.3, 0.4, 0.5, 0.6],
    }
