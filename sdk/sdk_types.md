# AchillesDB SDK Types & Errors

## List Collections Output

Returns a list of collection metadata objects.

**Fields:**
* `id<str>`: Unique identifier of the collection.
* `name<str>`: Name of the collection.
* `metadata<Dict[str, Any]>`: User-defined metadata.
* `created_at<str>`: ISO 8601 timestamp of creation.
* `updated_at<str>`: ISO 8601 timestamp of last update.

---

## GetResult

Object returned by `get()` and `peek()` operations.

**Fields:**
* `ids<List[str]>`: Record IDs.
* `embeddings<Optional[List[List[float]]]>`: Embedding vectors (if included).
* `metadatas<Optional[List[Dict[str, Any]]]>`: Metadata dictionaries (if included).
* `documents<Optional[List[str]]>`: Document strings (if included).
* `uris<Optional[List[str]]>`: URIs (if included).
* `data<Optional[List[Any]]>`: Loaded data (if included).

---

## QueryResult

Object returned by `query()` operations. All fields are nested lists where outer list corresponds to each query.

**Fields:**
* `ids<List[List[str]]>`: Record IDs for each query.
* `embeddings<Optional[List[List[List[float]]]]>`: Embedding vectors (if included).
* `metadatas<Optional[List[List[Dict[str, Any]]]]>`: Metadata dictionaries (if included).
* `documents<Optional[List[List[str]]]>`: Document strings (if included).
* `uris<Optional[List[List[str]]]>`: URIs (if included).
* `distances<Optional[List[List[float]]]>`: Distance scores from query (if included).
* `data<Optional[List[List[Any]]]>`: Loaded data (if included).

---

## Error Types

### AchillesDBException
Base exception for all SDK errors.

### ConnectionError
Failed to connect to server.

### TimeoutError
Request timed out.

### ValidationError
Input validation failed.

### InvalidDimensionError
Embedding dimension mismatch.

### InvalidInputError
Invalid input parameters.

### ResourceError
Resource operation failed.

### DatabaseNotFoundError
Database does not exist.

### CollectionNotFoundError
Collection does not exist.

### DocumentNotFoundError
Document does not exist.

### ResourceExistsError
Resource already exists.

### APIError
Server API error. Includes `status_code<int>` and `response<dict>`.
