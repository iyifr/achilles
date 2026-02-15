# AchillesDB SDK Specification

## Overview

The AchillesDB SDK provides client libraries for interacting with AchillesDB vector database. Available in Python (sync/async) and TypeScript.

---

## Client Architecture

### HttpClient (Base)

Base HTTP client for communicating with AchillesDB server.

**Properties:**
* `host<str>`: Hostname of the AchillesDB server.
* `port<int>`: HTTP port of the AchillesDB server.
* `ssl<bool>`: Whether to enable SSL for the connection.
* `headers<Optional[Dict[str, str]]>`: Optional headers to send with each request.
* `database<str>`: Database name to use for requests.

### SyncHttpClient

Synchronous HTTP client implementation.

**Properties:**
* `host<str>`: Hostname of the AchillesDB server.
* `port<int>`: HTTP port of the AchillesDB server.
* `ssl<bool>`: Whether to enable SSL for the connection.
* `headers<Optional[Dict[str, str]]>`: Optional headers to send with each request.
* `database<str>`: Database name to use for requests.

### AsyncHttpClient

Asynchronous HTTP client implementation for non-blocking operations.

**Properties:**
* `host<str>`: Hostname of the AchillesDB server.
* `port<int>`: HTTP port of the AchillesDB server.
* `ssl<bool>`: Whether to enable SSL for the connection.
* `headers<Optional[Dict[str, str]]>`: Optional headers to send with each request.
* `database<str>`: Database name to use for requests.

---

## AdminClient

Administrative client for managing databases. Does not require a database context.

**Properties:**
* `host<str>`: Hostname of the AchillesDB server.
* `port<int>`: HTTP port of the AchillesDB server.
* `ssl<bool>`: Whether to enable SSL for the connection.
* `headers<Optional[Dict[str, str]]>`: Optional headers to send with each request.

### AdminClient Methods

#### create_database

Create a new database.

**Parameters:**
* `name<str>`: The name of the database to create.

**Returns:** Database object.

**Raises:**
* `ResourceExistsError`: If the database already exists.

---

#### get_database

Get an existing database.

**Parameters:**
* `name<str>`: The name of the database to retrieve.

**Returns:** Database object.

**Raises:**
* `DatabaseNotFoundError`: If the database does not exist.

---

#### delete_database

Delete a database.

**Parameters:**
* `name<str>`: The name of the database to delete.

**Returns:** None.

**Raises:**
* `DatabaseNotFoundError`: If the database does not exist.

---

#### list_databases

List all databases.

**Parameters:**
* `limit<Optional[int]>`: The maximum number of database entries to return.
* `offset<Optional[int]>`: The number of entries to skip before returning.

**Returns:** List of database names.

---

## Client

Main client for interacting with collections within a database context.

**Properties:**
* `host<str>`: Hostname of the AchillesDB server.
* `port<int>`: HTTP port of the AchillesDB server.
* `ssl<bool>`: Whether to enable SSL for the connection.
* `headers<Optional[Dict[str, str]]>`: Optional headers to send with each request.
* `database<str>`: Database name to use for requests.

### Client Methods

#### list_collections

List all collections in the database.

**Parameters:**
* `limit<Optional[int]>`: The maximum number of entries to return.
* `offset<Optional[int]>`: The number of entries to skip before returning.

**Returns:** A list of Collection objects.

---

#### count_collections

Count the number of collections in the database.

**Returns:** The number of collections as an integer.

---

#### create_collection

Create a new collection with the given name and metadata.

**Parameters:**
* `name<str>`: The name of the collection to create.
* `metadata<Optional[Dict[str, Any]]>`: Optional metadata to associate with the collection.
* `embedding_function<Optional[EmbeddingFunction[Optional[Embeddings]]]>`: Optional function to use to embed documents.
* `data_loader<Optional[DataLoader[Optional[Embeddings]]]>`: Optional function to use to load records (documents, images, etc.)
* `get_or_create<bool>`: If True, return the existing collection if it exists. Default: False.

**Returns:** The newly created Collection object.

**Raises:**
* `ResourceExistsError`: If the collection already exists and get_or_create is False.
* `InvalidInputError`: If the collection name is invalid.

---

#### get_collection

Get a collection with the given name.

**Parameters:**
* `name<str>`: The name of the collection to get.
* `embedding_function<Optional[EmbeddingFunction[Optional[Embeddings]]]>`: Optional function to use to embed documents.
* `data_loader<Optional[DataLoader[Optional[Embeddings]]]>`: Optional function to use to load records (documents, images, etc.)

**Returns:** The Collection object.

**Raises:**
* `CollectionNotFoundError`: If the collection does not exist.

---

#### get_or_create_collection

Get or create a collection with the given name and metadata.

**Parameters:**
* `name<str>`: The name of the collection to get or create.
* `metadata<Optional[Dict[str, Any]]>`: Optional metadata to associate with the collection.
* `embedding_function<Optional[EmbeddingFunction[Optional[Embeddings]]]>`: Optional function to use to embed documents.
* `data_loader<Optional[DataLoader[Optional[Embeddings]]]>`: Optional function to use to load records (documents, images, etc.).

**Returns:** The Collection object.

---

#### delete_collection

Delete a collection with the given name.

**Parameters:**
* `name<str>`: The name of the collection to delete.

**Returns:** None.

**Raises:**
* `CollectionNotFoundError`: If the collection does not exist.

---

#### reset

Resets the database. This will delete all collections and entries.

**Returns:** True if the database was reset successfully.

---

#### get_version

Get the version of AchillesDB.

**Returns:** The version of AchillesDB as a string.

---

## Collection

Represents a collection of records with vector embeddings.

**Properties:**
* `name<str>`: The name of the collection.
* `id<str>`: The unique identifier of the collection.
* `metadata<Dict[str, Any]>`: Metadata associated with the collection.

### Collection Methods

#### count

Return the number of records in the collection.

**Returns:** The number of records in the collection as an integer.

---

#### add

Add records to the collection.

**Parameters:**
* `ids<Union[str, IDs]>`: Record IDs to add. Can be a single ID string or list of IDs.
* `embeddings<Optional[Embeddings]>`: Embeddings to add. If None, embeddings are computed using the collection's embedding function.
* `metadatas<Union[Optional[Metadatas], List[Optional[Metadatas]], None]>`: Optional metadata for each record.
* `documents<Union[str, IDs, None]>`: Optional documents for each record. Documents will be embedded if embedding function is set.
* `images<Optional[Embeddings]>`: Optional image embeddings to add.
* `uris<Union[str, IDs, None]>`: Optional URIs for each record.

**Returns:** None.

**Raises:**
* `ValidationError`: If both embeddings and documents are missing.
* `ValidationError`: If both embeddings and documents are provided.
* `ValidationError`: If lengths of provided fields do not match.
* `ResourceExistsError`: If an ID already exists in the collection.

---

#### get

Retrieve records from the collection.

**Parameters:**
* `ids<Union[str, IDs, None]>`: If provided, only return records with these IDs.
* `where<Optional[Dict[Union[str, Literal["$and"], Literal["$or"]], Where]]>`: A Where filter used to filter based on metadata values.
* `limit<Optional[int]>`: Maximum number of results to return.
* `offset<Optional[int]>`: Number of results to skip before returning.
* `include<List[Literal["documents", "embeddings", "metadatas", "distances", "data"]]>`: Fields to include in results. Can contain "embeddings", "metadatas", "documents", "uris". Defaults to ["metadatas", "documents"].

**Returns:** GetResult object containing retrieved records and requested fields.

---

#### peek

Return the first limit records from the collection.

**Parameters:**
* `limit<int>`: Maximum number of records to return. Default: 10.

**Returns:** GetResult object containing retrieved records.

---

#### query

Query for the K nearest neighbor records in the collection using vector similarity search.

**Parameters:**
* `query_embeddings<Optional[Embeddings]>`: Raw embeddings to query for.
* `query_texts<Union[str, IDs, None]>`: Documents to embed and query against.
* `query_images<Optional[Embeddings]>`: Images to embed and query against.
* `query_uris<Union[str, IDs, None]>`: URIs to be loaded and embedded.
* `ids<Union[str, IDs, None]>`: If provided, only search within records with these IDs.
* `n_results<int>`: Number of neighbors to return per query. Default: 10.
* `where<Optional[Dict[Union[str, Literal["$and"], Literal["$or"]], Where]]>`: Metadata filter.
* `include<List[Literal["documents", "embeddings", "metadatas", "distances", "uris", "data"]]>`: Fields to include in results. Can contain "embeddings", "metadatas", "documents", "uris", "distances". Defaults to ["metadatas", "documents", "distances"].

**Returns:** QueryResult object containing nearest neighbor results.

**Raises:**
* `ValidationError`: If no query input is provided.
* `ValidationError`: If multiple query input types are provided.

---

#### modify

Update collection name, metadata, or configuration.

**Parameters:**
* `name<Optional[str]>`: New collection name.
* `metadata<Optional[Dict[str, Any]]>`: New metadata for the collection.

**Returns:** The updated Collection object.

---

#### update

Update existing records by ID.

**Parameters:**
* `ids<Union[str, IDs]>`: Record IDs to update.
* `embeddings<Optional[Embeddings]>`: Updated embeddings. If None, embeddings are computed from documents.
* `metadatas<Union[Optional[Metadatas], List[Optional[Metadatas]], None]>`: Updated metadata.
* `documents<Union[str, IDs, None]>`: Updated documents.
* `images<Optional[Embeddings]>`: Updated images.
* `uris<Union[str, IDs, None]>`: Updated URIs for loading images.

**Returns:** None.

**Raises:**
* `ValidationError`: If both embeddings and documents are missing.
* `ValidationError`: If both embeddings and documents are provided.
* `ValidationError`: If lengths of provided fields do not match.
* `DocumentNotFoundError`: If a record with the given ID does not exist.

---

#### upsert

Create or update records by ID. If a record exists, it will be updated. If it does not exist, it will be created.

**Parameters:**
* `ids<Union[str, IDs]>`: Record IDs to upsert.
* `embeddings<Optional[Embeddings]>`: Embeddings to add or update. If None, embeddings are computed.
* `metadatas<Union[Optional[Metadatas], List[Optional[Metadatas]], None]>`: Metadata to add or update.
* `documents<Union[str, IDs, None]>`: Documents to add or update.
* `images<Optional[Embeddings]>`: Images to add or update.
* `uris<Union[str, IDs, None]>`: URIs for loading images.

**Returns:** None.

**Raises:**
* `ValidationError`: If both embeddings and documents are missing.
* `ValidationError`: If both embeddings and documents are provided.
* `ValidationError`: If lengths of provided fields do not match.

---

#### delete

Delete records by ID or filters.

**Parameters:**
* `ids<Optional[IDs]>`: Record IDs to delete.
* `where<Optional[Dict[Union[str, Literal["$and"], Literal["$or"]], Where]]>`: Metadata filter to target records for deletion.

**Returns:** None.

**Raises:**
* `ValidationError`: If neither IDs nor filters are provided.

**Note:** For detailed type definitions, schemas, and error types, see [TYPES.md](TYPES.md).

---

## Embedding Functions

### EmbeddingFunction

Abstract interface for embedding generation.

**Methods:**

#### embed

Generate embeddings from input data.

**Parameters:**
* `input<Any>`: Input data to embed (documents, images, etc.).

**Returns:** `Embeddings` - List of embedding vectors.

---

## Data Loaders

### DataLoader

Abstract interface for loading external data (images, files, etc.).

**Methods:**

#### load

Load data from URIs.

**Parameters:**
* `uris<List[str]>`: URIs to load data from.

**Returns:** `List[Any]` - Loaded data objects.

**Note:** For detailed error types and exception hierarchy, see [TYPES.md](TYPES.md).

---

## Configuration

### Environment Variables

* `ACHILLESDB_HOST` - Default: "localhost"
* `ACHILLESDB_PORT` - Default: 8180
* `ACHILLESDB_SSL` - Default: False
* `ACHILLESDB_DATABASE` - Default: "default"
* `ACHILLESDB_TIMEOUT` - Default: 30
* `ACHILLESDB_BATCH_SIZE` - Default: 100

---

## Batching Behavior

When adding, updating, or upserting large numbers of records:

1. SDK automatically splits operations into batches based on `batch_size` parameter.
2. Default batch size: 100 records per request.
3. Batches are processed sequentially in sync client.
4. Batches are processed concurrently in async client (up to max concurrent limit).
5. If any batch fails, remaining batches are not processed and error is raised.

---



## Metadata Filtering

The `where` parameter supports complex queries with multiple operators:

**Simple equality:**
```
{"status": "active"}
```

**Comparison operators ($gt, $gte, $lt, $lte):**
```
{"year": {"$gt": 2022}}
{"price": {"$gte": 100, "$lte": 500}}
```

**Equality operators ($eq, $ne):**
```
{"status": {"$eq": "published"}}
{"category": {"$ne": "archived"}}
```

**Array membership ($in, $nin):**
```
{"author": {"$in": ["jane", "john", "alice"]}}
{"status": {"$nin": ["draft", "deleted"]}}
```

**Array field checking ($arrContains):**
```
{"allowed_acls": {"$arrContains": ["acl-readers", "acl-admin-42"]}}
```

**Logical AND:**
```
{
  "$and": [
    {"status": "active"},
    {"priority": "high"}
  ]
}
```

**Logical OR:**
```
{
  "$or": [
    {"status": "active"},
    {"status": "pending"}
  ]
}
```

**Nested conditions:**
```
{
  "$and": [
    {"category": "tech"},
    {
      "$or": [
        {"priority": "high"},
        {"priority": "critical"}
      ]
    }
  ]
}
```

**Complex example (multiple operators):**
```
{
  "$and": [
    {"category": "tech"},
    {"year": {"$gt": 2022}},
    {"author": {"$in": ["jane", "john"]}},
    {"allowed_acls": {"$arrContains": ["acl-readers"]}}
  ]
}
```

**Supported Operators:**
* Simple equality: `{"field": value}`
* `$gt`, `$gte`, `$lt`, `$lte`: Numeric/date comparisons
* `$eq`, `$ne`: Explicit equality/inequality
* `$in`, `$nin`: Array membership checks
* `$arrContains`: Check if array field contains specified values
* `$and`, `$or`: Logical operators for combining filters

---

## ChromaDB Compatibility

AchillesDB SDK maintains API compatibility with ChromaDB where possible:

**Compatible:**
* Collection methods: `add`, `get`, `query`, `update`, `upsert`, `delete`, `peek`, `count`
* Parameter names and types
* Result object structure
* Embedding function interface

**Differences:**
* AchillesDB has explicit `AdminClient` for database management
* AchillesDB `Client` requires database context
* Additional `data_loader` parameter for loading external resources
* Extended metadata filtering with `$and`/`$or` operators

---

## Migration from ChromaDB

To migrate from ChromaDB to AchillesDB:

1. Replace `chromadb.Client()` with `AchillesClient(database="db_name")`
2. Add database creation step using `AdminClient` if needed
3. Collection operations remain unchanged
4. Update imports to use `achillesdb` package

Existing ChromaDB collection code will work with minimal changes.
