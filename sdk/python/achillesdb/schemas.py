from __future__ import annotations

from datetime import datetime
import math
from typing import Any
from pydantic import BaseModel, Field, model_validator

from achillesdb.validators import validate_equal_lengths


Scalar = str | int | float | bool


class ErrorResponse(BaseModel):
    error: str = ""


class MessageResponse(BaseModel):
    message: str = Field(description="Success message")


class CreateDatabaseReq(BaseModel):
    """
    POST /database
    path vars: CreateDatabaseReq
    """
    name: str = Field(default="default", description="Database name (defaults to 'default')")


class CreateDatabaseRes(BaseModel):
    """
    POST /database
    """
    message: str = Field(description="Success message")


class DatabaseInfo(BaseModel):
    name: str = Field(description="Database name")
    collectionCount: int = Field(description="Number of collections in this database")
    empty: bool = Field(description="Whether the database has no collections")


class GetDatabasesReq(BaseModel):
    """
    GET /databases
    """
    pass


class GetDatabasesRes(BaseModel):
    """
    GET /databases
    """
    databases: list[DatabaseInfo]
    db_count: int = Field(description="Total number of databases")


class DeleteDatabaseReq(BaseModel):
    """
    DELETE /database/{database_name}
    path vars: DeleteDatabaseReq
    """
    name: str = Field(description="Database name")


class DeleteDatabaseRes(BaseModel):
    """
    DELETE /database/{database_name}
    """
    message: str = Field(description="Success message")


class CollectionCatalogEntry(BaseModel):
    id: str = Field(alias="_id")
    ns: str = Field(description="Namespace in database.collection format")
    table_uri: str
    vector_index_uri: str
    createdAt: datetime
    updatedAt: datetime

    model_config = {"populate_by_name": True}


class GetCollectionsReq(BaseModel):
    """
    GET /database/{database_name}/collections
    path vars: ListCollectionsReq
    """
    database_name: str


class GetCollectionsRes(BaseModel):
    """
    GET /database/{database_name}/collections
    """
    # FIX: API: endpoint returns None if no collections instead of empty list
    collections: list[CollectionCatalogEntry] | None
    collection_count: int

    @model_validator(mode='after')
    def normalize_collections(self) -> GetCollectionsRes:
        if self.collections is None:
            self.collections = []
        return self


class CreateCollectionReqInput(BaseModel):
    """
    POST /database/{database_name}/collections
    payload: CreateCollectionReq
    """
    name: str = Field(description="Collection name")


class CreateCollectionRes(BaseModel):
    """
    POST /database/{database_name}/collections
    """
    message: str = Field(description="Success message")


class CollectionStats(BaseModel):
    doc_count: int
    vector_index_size: float


class Document(BaseModel):
    id: str
    content: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    # NOTE: distance is not available in a normal get_documents response
    distance: float | None = Field(
        description="Distance from query embedding", default=None
    )


class GetCollectionReq(BaseModel):
    """
    GET /database/{database_name}/collections/{collection_name}
    path vars: GetCollectionReq
    """
    database_name: str
    collection_name: str


class GetCollectionRes(BaseModel):
    """
    GET /database/{database_name}/collections/{collection_name}
    """
    collection: CollectionCatalogEntry
    # documents: List[Document]
    stats: CollectionStats


class DeleteCollectionReq(BaseModel):
    """
    DELETE /database/{database_name}/collections/{collection_name}
    path vars: DeleteCollectionReq
    """
    database_name: str
    collection_name: str


class DeleteCollectionRes(BaseModel):
    """
    DELETE /database/{database_name}/collections/{collection_name}
    """
    message: str = Field(description="Success message")


class GetDocumentsReq(BaseModel):
    """
    GET /database/{database_name}/collections/{collection_name}/documents
    path vars: GetDocumentsReq
    """
    database_name: str
    collection_name: str


class GetDocumentsRes(BaseModel):
    """
    GET /database/{database_name}/collections/{collection_name}/documents
    """
    documents: list[Document] | None
    doc_count: int

    @model_validator(mode='after')
    def normalize_documents(self) -> GetDocumentsRes:
        if self.documents is None:
            self.documents = []
        return self

class InsertDocumentsReq(BaseModel):
    """
    POST /database/{database_name}/collections/{collection_name}/documents
    path vars: InsertDocumentsReq
    """
    database_name: str
    collection_name: str


class InsertDocumentReqInput(BaseModel):
    """
    POST /database/{database_name}/collections/{collection_name}/documents
    payload: InsertDocumentReqInput
    """
    ids: list[str] = Field(description="Unique document ID")
    documents: list[str] = Field(description="Document content/text")
    embeddings: list[list[float]] = Field(description="Document embedding vector")
    metadatas: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Arbitrary metadata key-value pairs"
    )

    @model_validator(mode='after')
    def check_validations(self) -> InsertDocumentReqInput:
        # validate embeddings is not empty
        if not self.embeddings and len(self.ids) > 0:
            raise ValueError("Embeddings must be non-empty")

        if any(not math.isfinite(v) for emb in self.embeddings for v in emb):
            raise ValueError("Embeddings must not contain NaN or Inf values")

        # validate dimensions
        if self.embeddings:
            dim = len(self.embeddings[0])
            for i, emb in enumerate(self.embeddings[1:], 1):
                if len(emb) != dim:
                    raise ValueError(
                        f"Embedding dimension mismatch: expected {dim}, got {len(emb)} at index {i}"
                    )

        # validate no duplicates in ids
        if len(self.ids) != len(set(self.ids)):
            dupes = [id for id in self.ids if self.ids.count(id) > 1]
            raise ValueError(f"ids array contains duplicates: {set(dupes)}")

        # validate equal lengths
        validate_equal_lengths(
            ids=self.ids,
            documents=self.documents,
            embeddings=self.embeddings,
            metadatas=self.metadatas,
        )
        return self


class InsertDocumentsRes(MessageResponse):
    ...


# class InsertDocumentsRequest(BaseModel):
#     ids: List[str] = Field(description="Array of unique document IDs")
#     documents: List[str] = Field(description="Array of document content/text")
#     embeddings: List[List[float]] = Field(description="Array of embedding vectors (one per document)")
#     metadatas: List[Dict[str, Any]] = Field(description="Array of metadata objects (one per document)")


class UpdateDocumentsReq(BaseModel):
    """
    PUT /database/{database_name}/collections/{collection_name}/documents
    path vars: UpdateDocumentsReq
    """
    database_name: str
    collection_name: str


class UpdateDocumentsReqInput(BaseModel):
    """
    PUT /database/{database_name}/collections/{collection_name}/documents
    payload: UpdateDocumentsReqInput
    """
    document_id: str = Field(description="ID of the document to update")
    updates: dict[str, Any] = Field(description="Metadata fields to update")


class UpdateDocumentsRes(MessageResponse):
    """
    PUT /database/{database_name}/collections/{collection_name}/documents
    """
    ...


class DeleteDocumentsReq(BaseModel):
    """
    DELETE /database/{database_name}/collections/{collection_name}/documents
    path vars: DeleteDocumentsReq
    """
    database_name: str
    collection_name: str


class DeleteDocumentsReqInput(BaseModel):
    """
    DELETE /database/{database_name}/collections/{collection_name}/documents
    payload: DeleteDocumentsReqInput
    """
    document_ids: list[str] = Field(description="List of document IDs to delete")


class DeleteDocumentsRes(BaseModel):
    """
    DELETE /database/{database_name}/collections/{collection_name}/documents
    """
    deleted_count: int = Field(description="Number of deleted documents")
    deleted_ids: list[str] = Field(default_factory=list, description="IDs of deleted documents")


class QueryReq(BaseModel):
    """
    POST /database/{database_name}/collections/{collection_name}/documents/query
    path vars: QueryReq
    """
    database_name: str
    collection_name: str


class QueryReqInput(BaseModel):
    """
    POST /database/{database_name}/collections/{collection_name}/documents/query
    payload: QueryReqInput
    """
    query_embedding: list[float] = Field(description="Query embedding vector")
    top_k: int = Field(default=10, description="Number of results to return")
    where: WhereClause | None = Field(default=None, description="Metadata filter conditions")

    def model_dump(self, **kwargs: Any) -> dict[str, Any]:
        # serializes with aliases convert W.eq("category", "tech") to {"category": "tech"}
        d = super().model_dump(**kwargs)
        if self.where is not None:
            d["where"] = self.where.to_dict()
        return d

    @model_validator(mode='after')
    def check_validations(self) -> QueryReqInput:
        # validate embeddings is not empty
        if not self.query_embedding:
            raise ValueError("Embeddings must be non-empty")
        return self


class QueryRes(BaseModel):
    """
    POST /database/{database_name}/collections/{collection_name}/documents/query
    """
    documents: list[Document] | None
    doc_count: int

    @model_validator(mode='after')
    def normalize_documents(self) -> GetDocumentsRes:
        if self.documents is None:
            self.documents = []
        return self

# Comparison operators
class ComparisonOp(BaseModel):
    # greater than
    gt: int | float | None = Field(None, alias="$gt")
    # greater than or equal
    gte: int | float | None = Field(None, alias="$gte")
    # less than
    lt: int | float | None = Field(None, alias="$lt")
    # less than or equal
    lte: int | float | None = Field(None, alias="$lte")
    # not equal
    eq: Scalar | None = Field(None, alias="$eq")
    # not equal
    ne: Scalar | None = Field(None, alias="$ne")

    model_config = {"populate_by_name": True}

    @model_validator(mode="after")
    def at_least_one(self):
        vals = [self.gt, self.gte, self.lt, self.lte, self.eq, self.ne]
        if not any(v is not None for v in vals):
            raise ValueError("ComparisonOp must have at least one operator")
        return self


# $in operator
class InOp(BaseModel):
    in_: list[Scalar] = Field(alias="$in")
    model_config = {"populate_by_name": True}


# $arrContains operator
class ArrContainsOp(BaseModel):
    arr_contains: list[Scalar] = Field(alias="$arrContains")
    model_config = {"populate_by_name": True}


# A field value is either a scalar (equality shorthand) or an operator object
FieldValue = Scalar | ComparisonOp | InOp | ArrContainsOp


class WhereClause(BaseModel):
    model_config = {"populate_by_name": True, "extra": "allow"}

    and_: list[WhereClause] | None = Field(None, alias="$and")
    or_: list[WhereClause] | None = Field(None, alias="$or")

    # extra fields are the user's metadata field conditions
    # e.g. {"category": "tech", "year": {"$gt": 2022}}

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(
            by_alias=True,
            exclude_none=True,
        )


WhereClause.model_rebuild()
