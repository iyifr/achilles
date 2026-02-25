import logging


from .http.connection import HttpClient
from typing import Literal, Optional

class _AchillesClient:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 8180,
        mode: Literal["sync", "async"] = "sync",
        timeout: Optional[float] = None
    ):
        base_url = f"http://{host}:{port}"
        self.http = HttpClient(base_url=base_url, mode=mode, timeout=timeout)

    def create_database(self, name: str):
        # This single line handles both sync and async delivery
        return self.http.request("POST", "/databases", json={"name": name})

    def list_databases(self):
        return self.http.request("GET", "/databases")


class AchillesClienti(_AchillesClient):
    def __init__(
        self,
        host="http://localhost",
        port=8180,
        default_db="mydb",
        embedding_function=None, #sync version of my_embed
        timeout=30,
        logger=logging.getLogger(),
    ):
        pass


class AsyncAchillesClient(_AchillesClient):
    def __init__(
        self,
        host: str | None = None,
        port=8180,
        default_db="mydb",
        embedding_function=None, #async version of my_embed
        timeout=30,
        logger=logging.getLogger(),
    ):
        pass
