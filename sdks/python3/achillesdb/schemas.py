from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, RootModel, model_validator

from achillesdb.validators import validate_equal_lengths


class ErrorResponse(BaseModel):
    error: str

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
    databases: List[DatabaseInfo]
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
    # FIX: endpoint returns None if no collections instead of empty list
    collections: Optional[list[CollectionCatalogEntry]]
    collection_count: int


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
    metadata: Dict[str, Any] = {}


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
    documents: List[Document]
    doc_count: int


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
    embeddings: list[List[float]] = Field(description="Document embedding vector")
    metadatas: list[Dict[str, Any]] = Field(default=[], description="Arbitrary metadata key-value pairs")

    @model_validator(mode='after')
    def check_equal_lengths(self) -> 'InsertDocumentReqInput':
        validate_equal_lengths(
            ids=self.idis,
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
    where: Dict[str, Any] = Field(default={}, description="Filter conditions (reserved for future use)")
    updates: Dict[str, Any] = Field(description="Metadata fields to update")




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
    document_ids: List[str] = Field(description="List of document IDs to delete")


class DeleteDocumentsRes(MessageResponse):
    """
    DELETE /database/{database_name}/collections/{collection_name}/documents
    """
    ...


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
    query_embedding: List[float] = Field(description="Query embedding vector")
    top_k: int = Field(default=10, description="Number of results to return")
    where: Dict[str, Any] = Field(default={}, description="Metadata filter conditions")


class QueryRes(BaseModel):
    """
    POST /database/{database_name}/collections/{collection_name}/documents/query
    """
    documents: List[Document]
    doc_count: int
