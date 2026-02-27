from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, RootModel


class ErrorResponse(BaseModel):
    error: str

class MessageResponse(BaseModel):
    message: str

class CreateDatabaseReq(BaseModel):
    """
    POST /database
    prama: CreateDatabaseReq
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
    prama: DeleteDatabaseReq
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
    prama: ListCollectionsReq
    """
    database_name: str


class GetCollectionsRes(BaseModel):
    """
    GET /database/{database_name}/collections
    """
    collections: list[CollectionCatalogEntry]
    collection_count: int


class CreateCollectionReq(BaseModel):
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
    prama: GetCollectionReq
    """
    database_name: str
    collection_name: str


class GetCollectionRes(BaseModel):
    """
    GET /database/{database_name}/collections/{collection_name}
    """
    collection: CollectionCatalogEntry
    documents: List[Document]
    stats: CollectionStats


class DeleteCollectionReq(BaseModel):
    """
    DELETE /database/{database_name}/collections/{collection_name}
    prama: DeleteCollectionReq
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
    prama: GetDocumentsReq
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
    prama: InsertDocumentsReq
    """
    database_name: str
    collection_name: str


class InsertDocumentReqInput(BaseModel):
    """
    POST /database/{database_name}/collections/{collection_name}/documents
    payload: InsertDocumentReqInput
    """
    id: str = Field(description="Unique document ID")
    content: Optional[str] = Field(default=None, description="Document content/text")
    embedding: List[float] = Field(description="Document embedding vector")
    metadata: Dict[str, Any] = Field(default={}, description="Arbitrary metadata key-value pairs")


class InsertDocumentsRes(BaseModel):
    message: str = Field(description="Success message")


# class InsertDocumentsRequest(BaseModel):
#     ids: List[str] = Field(description="Array of unique document IDs")
#     documents: List[str] = Field(description="Array of document content/text")
#     embeddings: List[List[float]] = Field(description="Array of embedding vectors (one per document)")
#     metadatas: List[Dict[str, Any]] = Field(description="Array of metadata objects (one per document)")


class UpdateDocumentsReq(BaseModel):
    """
    PUT /database/{database_name}/collections/{collection_name}/documents
    prama: UpdateDocumentsReq
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


class DeleteDocumentsReq(BaseModel):
    """
    DELETE /database/{database_name}/collections/{collection_name}/documents
    prama: DeleteDocumentsReq
    """
    database_name: str
    collection_name: str


class DeleteDocumentsReqInput(BaseModel):
    """
    DELETE /database/{database_name}/collections/{collection_name}/documents
    payload: DeleteDocumentsReqInput
    """
    document_ids: List[str] = Field(description="List of document IDs to delete")


class QueryReq(BaseModel):
    """
    POST /database/{database_name}/collections/{collection_name}/documents/query
    prama: QueryReq
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
