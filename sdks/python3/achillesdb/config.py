from pydantic import BaseModel, ConfigDict
from pydantic_settings import SettingsConfigDict, BaseSettings
from functools import lru_cache


class ConnectionConfig(BaseModel):
    model_config = ConfigDict(strict=True)
    session_pool_connections: int = 20
    session_pool_maxsize: int = 100
    session_pool_timeout: int = 5


class EnvConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )


class BasicConfig(BaseSettings):
    default_db: str = "mydb"  # DEFAULT_DB
    default_host: str = "localhost"  # DEFAULT_HOST
    default_port: int = 8180  # DEFAULT_PORT
    default_timeout: int = 30  # DEFAULT_TIMEOUT
    default_ssl: bool = False  # DEFAULT_SSL
    default_api_base_path: str = "/api/v1"  # DEFAULT_API_BASE_PATH

    connection: ConnectionConfig = ConnectionConfig()


@lru_cache
def get_config():
    return BasicConfig()
