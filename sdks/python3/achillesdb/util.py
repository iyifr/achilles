from achillesdb.config import get_config


cfg = get_config()


def get_base_url(
    host: str = cfg.default_host,
    port: int = cfg.default_port,
    api_base_path: str = cfg.default_api_base_path,
    ssl: bool = cfg.default_ssl,
) -> str:
    protocol = "https" if ssl else "http"
    return f"{protocol}://{host}{port}/{api_base_path}"
