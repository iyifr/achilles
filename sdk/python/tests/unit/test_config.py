import pytest
from pydantic import ValidationError
from achillesdb.config import BasicConfig, ConnectionConfig, get_config


class TestConnectionConfig:

    def test_defaults(self):
        cfg = ConnectionConfig()
        assert cfg.session_pool_connections == 20
        assert cfg.session_pool_maxsize == 100
        assert cfg.session_pool_timeout == 5

    def test_custom_values(self):
        cfg = ConnectionConfig(
            session_pool_connections=5,
            session_pool_maxsize=50,
            session_pool_timeout=10,
        )
        assert cfg.session_pool_connections == 5
        assert cfg.session_pool_maxsize == 50
        assert cfg.session_pool_timeout == 10

    def test_strict_mode_rejects_wrong_type(self):
        # strict=True means no coercion — passing a string should fail
        with pytest.raises(ValidationError):
            ConnectionConfig(session_pool_connections="twenty")

    def test_strict_mode_rejects_float_for_int(self):
        with pytest.raises(ValidationError):
            ConnectionConfig(session_pool_connections=5.5)


class TestBasicConfig:

    def test_default_host(self):
        cfg = BasicConfig()
        assert cfg.default_host == "localhost"

    def test_default_port(self):
        cfg = BasicConfig()
        assert cfg.default_port == 8180

    def test_default_ssl(self):
        cfg = BasicConfig()
        assert cfg.default_ssl is False

    def test_default_timeout(self):
        cfg = BasicConfig()
        assert cfg.default_timeout == 30

    def test_default_api_base_path(self):
        cfg = BasicConfig()
        assert cfg.default_api_base_path == "/api/v1"

    def test_default_db(self):
        cfg = BasicConfig()
        assert cfg.default_db == "mydb"

    def test_default_connection_config(self):
        cfg = BasicConfig()
        assert isinstance(cfg.connection, ConnectionConfig)


class TestGetConfig:

    def test_returns_basic_config(self):
        cfg = get_config()
        assert isinstance(cfg, BasicConfig)

    def test_is_cached(self):
        # lru_cache means the same object is returned each time
        cfg1 = get_config()
        cfg2 = get_config()
        assert cfg1 is cfg2
