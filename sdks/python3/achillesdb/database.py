import logging
import threading
from typing import Callable, Optional, Union

from achillesdb.http.connection import AsyncHttpClient, SyncHttpClient


class DatabaseImpl:
    def __init__(
        self,
        name: str,
        http: Union[SyncHttpClient, AsyncHttpClient],
        embedding_function: Optional[Callable] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.name = name
        self._http = http
        self._embedding_function = embedding_function
        self._logger = logger
        self._collections = None
        self._collections_mutex = threading.Lock()

    def _createCollection(self, name):
        ...

    def _listCollections(self):
        ...

    def _collection(self, name):
        ...

    def _deleteCollection(self, name):
        ...

    def _queryCollections(self, opts=None):
        ...

class AsyncDatabase(DatabaseImpl):
    pass

class SyncDatabase(DatabaseImpl):
    pass
