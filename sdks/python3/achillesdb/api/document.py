from typing import Literal, Optional, Union

from achillesdb.http.connection import AsyncHttpClient, SyncHttpClient
from achillesdb.schemas import GetDocumentsRes, InsertDocumentReqInput, InsertDocumentsRes, UpdateDocumentsReqInput
from achillesdb.util import validate_name


class _DocumentApiBase:
    def __init__(
        self,
        http_client: Union[SyncHttpClient, AsyncHttpClient],
        database_name: str,
        collection_name: str,
        logger: Optional[logging.Logger] = None,
        mode: Literal["sync", "async"] = "sync",
    ):
        self._logger = logger or logging.getLogger(__name__)
        self._mode = mode
        self._http = http_client
        self._database_name = database_name
        self._collection_name = collection_name

    def _get_documents(self):
        return self._http.get(
            f"/database/{self._database_name}/collections/{self._collection_name}/documents",
            GetDocumentsRes,
            expected_status=200,
        )

    def _insert_documents(self, input: InsertDocumentReqInput):
        return self._http.post(
            f"/database/{self._database_name}/collections/{self._collection_name}/documents",
            InsertDocumentsRes,
            json=input.dict(exclude_unset=True),
            expected_status=200,
        )

    def _update_documents(self, input: UpdateDocumentsReqInput):
        return self._http.put(
            f"/database/{self._database_name}/collections/{self._collection_name}/documents",
            InsertDocumentsRes,
            json=input.dict(exclude_unset=True),
            expected_status=200,
        )

