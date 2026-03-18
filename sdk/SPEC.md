# AchillesDB SDK Specification (v1)

## Overview

Official TypeScript and Python SDKs for AchillesDB (powered by GlowstickDB). Both SDKs communicate with the AchillesDB server over HTTP, targeting `localhost:8180` by default.

**v1 scope**: TypeScript SDK first, Python SDK fast-follow. Core CRUD + single-collection query.

### Core DX Principles

1. **Reduce cognitive load** — familiar patterns (ChromaDB SOA format, MongoDB-style filters)
2. **Incremental complexity** — works with minimal config, power-user features are opt-in
3. **Language-native idioms** — camelCase in TS, snake_case in Python
4. **Fail fast** — strict client-side validation with clear error messages

---

## Package Distribution

| Language   | Package Name         | Registry |
| ---------- | -------------------- | -------- |
| TypeScript | `@achillesdb/client` | npm      |
| Python     | `achillesdb`         | PyPI     |

- **Repo location**: `sdks/typescript/` and `sdks/python/` inside the GlowstickDB monorepo
- **TypeScript module format**: ESM only
- **TypeScript HTTP client**: native `fetch`
- **Python HTTP client**: `httpx` for both sync (`httpx.Client`) and async (`httpx.AsyncClient`)

---

## Client Initialization

### TypeScript

```ts
import { AchillesClient } from "@achillesdb/client";

const client = new AchillesClient({
  host: "http://localhost",    // default: "http://localhost"
  port: 8180,                  // default: 8180
  defaultDb: "mydb",           // optional — used when db not specified
  embeddingFunction: async (texts: string[]) => number[][], // optional
  timeout: 30000,              // default: 30000ms
  logger: console,             // optional — any object with .debug(), .info(), .warn(), .error()
});
```

### Python (Sync)

```python
from achillesdb import AchillesClient

client = AchillesClient(
    host="http://localhost",       # default
    port=8180,                     # default
    default_db="mydb",             # optional
    embedding_function=my_embed,   # optional: Callable[[list[str]], list[list[float]]]
    timeout=30,                    # default: 30 seconds
    logger=logging.getLogger(),    # optional: standard logging.Logger
)
```

### Python (Async)

```python
from achillesdb import AsyncAchillesClient

client = AsyncAchillesClient(
    host="http://localhost",
    port=8180,
    default_db="mydb",
    embedding_function=my_async_embed,  # async Callable[[list[str]], list[list[float]]]
    timeout=30,
    logger=logging.getLogger(),
)
```

### Configuration Defaults

| Parameter           | Default                | Notes                                     |
| ------------------- | ---------------------- | ----------------------------------------- |
| `host`              | `"http://localhost"`   |                                           |
| `port`              | `8180`                 |                                           |
| `defaultDb`         | `undefined` / `None`   | If unset, `db` param required on methods  |
| `embeddingFunction` | `undefined` / `None`   | Required for text-based query if no queryEmbedding provided |
| `timeout`           | `30000` ms / `30` sec  | Applies to all HTTP requests              |
| `logger`            | `undefined` / `None`   | No logging if unset                       |

### Connection Validation

- **Lazy by default** — no server contact on client creation
- **Explicit ping available** — `client.ping()` / `await client.ping()` returns `true` or throws

```ts
// TypeScript
const alive = await client.ping(); // true or throws AchillesError

// Python
alive = client.ping()  # True or raises AchillesError
```

---

## Embedding Function Contract

A single signature: batch-in, batch-out.

### TypeScript

```ts
type EmbeddingFunction = (texts: string[]) => Promise<number[][]>;
```

### Python (Sync)

```python
EmbeddingFunction = Callable[[list[str]], list[list[float]]]
```

### Python (Async)

```python
AsyncEmbeddingFunction = Callable[[list[str]], Awaitable[list[list[float]]]]
```

### Behavior

- The SDK always passes an array of texts, even for single documents
- The function must return one embedding per input text, in the same order
- All embeddings must have the same dimensionality
- The SDK validates output length matches input length before proceeding

---

## Error Handling

### Single Error Type with Code

All SDK errors are instances of a single `AchillesError` class with a `code` field.

### TypeScript

```ts
class AchillesError extends Error {
  code: ErrorCode;
  statusCode: number | null; // HTTP status, null for client-side errors
  constructor(message: string, code: ErrorCode, statusCode?: number);
}

enum ErrorCode {
  NOT_FOUND = "NOT_FOUND",           // 404 — db, collection, or document not found
  CONFLICT = "CONFLICT",             // 409 — resource already exists
  VALIDATION = "VALIDATION",         // 400 — invalid input (server or client-side)
  SERVER_ERROR = "SERVER_ERROR",     // 500 — internal server error
  CONNECTION = "CONNECTION",         // null — network/timeout error
  EMBEDDING = "EMBEDDING",          // null — embedding function failed
}
```

### Python

```python
class AchillesError(Exception):
    code: str          # "NOT_FOUND", "CONFLICT", "VALIDATION", "SERVER_ERROR", "CONNECTION", "EMBEDDING"
    status_code: int | None
    message: str
```

### Retry Policy

- **No automatic retries.** All errors fail immediately.
- Users handle retry logic externally.

---

## Client-Side Validation

The SDK validates before sending to the server. Throws `AchillesError` with `code: "VALIDATION"`:

| Check                                         | Error Message                                          |
| --------------------------------------------- | ------------------------------------------------------ |
| `ids` array is empty                          | `"ids array must not be empty"`                        |
| `ids` contains duplicates                     | `"ids array contains duplicates: {id}"`                |
| Array lengths don't match (ids, documents, metadatas, embeddings) | `"Array length mismatch: ids({n}) != documents({m})"` |
| Embedding dimensions inconsistent             | `"Embedding dimension mismatch: expected {d}, got {d2} at index {i}"` |
| Required ID missing from a document           | `"id is required for every document"`                  |
| No embedding function and no embeddings provided | `"Either provide embeddings or set embeddingFunction on the client"` |
| Empty collection/database name                | `"Name must not be empty"`                             |

---

## Database Operations

### TypeScript

```ts
// Create database — returns a Database handle
const db = await client.createDatabase("mydb");

// List databases
const databases: DatabaseInfo[] = await client.listDatabases();
// DatabaseInfo: { name: string, collectionCount: number, empty: boolean }

// Get existing database handle (no server call)
const db = client.database("mydb");

// Delete database
await client.deleteDatabase("mydb");
```

### Python (Sync)

```python
db = client.create_database("mydb")

databases: list[DatabaseInfo] = client.list_databases()
# DatabaseInfo: name: str, collection_count: int, empty: bool

db = client.database("mydb")

client.delete_database("mydb")
```

### Database Handle

`client.database(name)` returns a `Database` object without hitting the server. It's a lightweight namespace handle. Operations on it will fail with `NOT_FOUND` if the database doesn't exist on the server.

---

## Collection Operations

### TypeScript

```ts
// Create collection — returns a Collection handle
const collection = await db.createCollection("actors");

// Get existing collection handle (no server call)
const collection = db.collection("actors");

// List collections
const collections: CollectionInfo[] = await db.listCollections();
// CollectionInfo: { name: string, ns: string, createdAt: string, updatedAt: string }

// Delete collection
await db.deleteCollection("actors");

// Count documents in collection
const count: number = await collection.count();
```

### Python (Sync)

```python
collection = db.create_collection("actors")

collection = db.collection("actors")

collections: list[CollectionInfo] = db.list_collections()

db.delete_collection("actors")

count: int = collection.count()
```

---

## Document Operations

### Insert — ChromaDB SOA Format

#### TypeScript

```ts
const result = await collection.addDocuments({
  ids: ["doc1", "doc2", "doc3"],
  documents: ["First document", "Second document", "Third document"],
  embeddings: [                 // optional if embeddingFunction is set
    [0.1, 0.2, 0.3],
    [0.4, 0.5, 0.6],
    [0.7, 0.8, 0.9],
  ],
  metadatas: [                  // optional
    { author: "John", year: 2024 },
    { author: "Jane", year: 2023 },
    { author: "Bob", year: 2024 },
  ],
});
// result: { ids: ["doc1", "doc2", "doc3"], count: 3 }
```

#### Python (Sync)

```python
result = collection.add_documents(
    ids=["doc1", "doc2", "doc3"],
    documents=["First document", "Second document", "Third document"],
    embeddings=[                 # optional if embedding_function is set
        [0.1, 0.2, 0.3],
        [0.4, 0.5, 0.6],
        [0.7, 0.8, 0.9],
    ],
    metadatas=[                  # optional
        {"author": "John", "year": 2024},
        {"author": "Jane", "year": 2023},
        {"author": "Bob", "year": 2024},
    ],
)
# result: InsertResult(ids=["doc1", "doc2", "doc3"], count=3)
```

#### Embedding Resolution

1. If `embeddings` provided → use them directly
2. If `embeddings` omitted and `embeddingFunction` set on client → call `embeddingFunction(documents)`
3. If neither → throw `AchillesError(code: "VALIDATION")`

### Get Documents

```ts
// TypeScript
const docs = await collection.getDocuments();
// { documents: Document[], count: number }
// Document: { id: string, content: string, metadata: Record<string, any> }
```

```python
# Python
docs = collection.get_documents()
# GetResult(documents=[Document(id, content, metadata)], count=int)
```

### Update Documents

Updates metadata only. Supports batch via a single server request.

```ts
// TypeScript — single document
await collection.updateDocument({
  id: "doc1",
  metadata: { category: "updated", newField: "value" },
});

// TypeScript — batch (single PUT request)
await collection.updateDocuments({
  ids: ["doc1", "doc2"],
  metadatas: [
    { category: "updated" },
    { category: "also_updated" },
  ],
});
```

```python
# Python — single
collection.update_document(id="doc1", metadata={"category": "updated"})

# Python — batch (single PUT request)
collection.update_documents(
    ids=["doc1", "doc2"],
    metadatas=[{"category": "updated"}, {"category": "also_updated"}],
)
```

### Delete Documents

By IDs only.

```ts
// TypeScript
await collection.deleteDocuments({ ids: ["doc1", "doc2"] });
```

```python
# Python
collection.delete_documents(ids=["doc1", "doc2"])
```

---

## Query

### Single Collection Query

```ts
// TypeScript
const results = await collection.query({
  query: "action hero",           // text — requires embeddingFunction
  // OR
  queryEmbedding: [0.1, 0.2, ...], // pre-computed vector
  topK: 10,                       // required
  where: {                        // optional — metadata filters
    year: { $gte: 2024 },
    category: { $in: ["action", "drama"] },
  },
});
// QueryResult: { documents: QueryDocument[], count: number }
// QueryDocument: { id, content, metadata, distance }
```

```python
# Python
results = collection.query(
    query="action hero",           # OR query_embedding=[0.1, 0.2, ...]
    top_k=10,
    where={"year": {"$gte": 2024}},
)
# QueryResult(documents=[QueryDocument(id, content, metadata, distance)], count=int)
```

#### Query Embedding Resolution

1. If `queryEmbedding` provided → use it directly
2. If `query` (text) provided and `embeddingFunction` set → call `embeddingFunction([query])`, take first result
3. If both provided → `queryEmbedding` takes precedence
4. If neither → throw `AchillesError(code: "VALIDATION")`

---

## Filter Syntax

Plain objects using MongoDB-style operators. Passed via the `where` parameter.

### Comparison Operators

| Operator       | Example                                | Meaning                   |
| -------------- | -------------------------------------- | ------------------------- |
| (implicit `$eq`) | `{ category: "tech" }`              | Equals                    |
| `$eq`          | `{ category: { $eq: "tech" } }`       | Equals (explicit)         |
| `$ne`          | `{ category: { $ne: "tech" } }`       | Not equals                |
| `$gt`          | `{ year: { $gt: 2023 } }`             | Greater than              |
| `$gte`         | `{ year: { $gte: 2024 } }`            | Greater than or equal     |
| `$lt`          | `{ price: { $lt: 100 } }`             | Less than                 |
| `$lte`         | `{ price: { $lte: 99 } }`             | Less than or equal        |

### Set Operators

| Operator       | Example                                         | Meaning                       |
| -------------- | ----------------------------------------------- | ----------------------------- |
| `$in`          | `{ category: { $in: ["tech", "science"] } }`   | Value in array                |
| `$nin`         | `{ category: { $nin: ["spam"] } }`              | Value not in array            |
| `$arrContains` | `{ tags: { $arrContains: ["python", "go"] } }`  | Array field contains values   |

### Logical Operators

| Operator | Example                                                |
| -------- | ------------------------------------------------------ |
| `$and`   | `{ $and: [{ category: "tech" }, { year: { $gte: 2024 } }] }` |
| `$or`    | `{ $or: [{ category: "tech" }, { category: "science" }] }`   |

---

## Logging

Both SDKs accept a pluggable logger.

### TypeScript

Any object conforming to:

```ts
interface Logger {
  debug(message: string, ...args: any[]): void;
  info(message: string, ...args: any[]): void;
  warn(message: string, ...args: any[]): void;
  error(message: string, ...args: any[]): void;
}
```

`console` satisfies this interface. Pass `logger: console` for quick debugging.

### Python

A standard `logging.Logger` instance.

```python
import logging
logger = logging.getLogger("achillesdb")
client = AchillesClient(logger=logger)
```

### What Gets Logged

| Level   | Events                                           |
| ------- | ------------------------------------------------ |
| `debug` | HTTP request/response details, embedding timings |
| `info`  | Database/collection creation, document counts    |
| `warn`  | Deprecated usage, slow operations                |
| `error` | Failed requests, validation errors               |

---

## Response Types

All methods return parsed domain objects. No raw HTTP metadata exposed.

### TypeScript Types

```ts
// Databases
interface DatabaseInfo {
  name: string;
  collectionCount: number;
  empty: boolean;
}

// Collections
interface CollectionInfo {
  name: string;
  ns: string;
  createdAt: string;
  updatedAt: string;
}

// Documents
interface Document {
  id: string;
  content: string;
  metadata: Record<string, any>;
}

interface QueryDocument extends Document {
  distance: number;
}

// Operation results
interface InsertResult {
  ids: string[];
  count: number;
}

interface GetResult {
  documents: Document[];
  count: number;
}

interface QueryResult {
  documents: QueryDocument[];
  count: number;
}
```

### Python Types

Equivalent dataclasses/TypedDicts with snake_case naming.

---

## Complete API Surface

### AchillesClient / AsyncAchillesClient

| Method                                  | Returns                 | Server Endpoint                              |
| --------------------------------------- | ----------------------- | -------------------------------------------- |
| `ping()`                                | `boolean` / `bool`      | `GET /api/v1/databases` (lightweight check)  |
| `createDatabase(name)`                  | `Database`              | `POST /api/v1/database`                      |
| `listDatabases()`                       | `DatabaseInfo[]`        | `GET /api/v1/databases`                      |
| `database(name)`                        | `Database`              | (no server call)                             |
| `deleteDatabase(name)`                  | `void` / `None`         | `DELETE /api/v1/database/{name}`             |

### Database

| Method                                  | Returns                 | Server Endpoint                                       |
| --------------------------------------- | ----------------------- | ----------------------------------------------------- |
| `createCollection(name)`                | `Collection`            | `POST /api/v1/database/{db}/collections`              |
| `listCollections()`                     | `CollectionInfo[]`      | `GET /api/v1/database/{db}/collections`               |
| `collection(name)`                      | `Collection`            | (no server call)                                      |
| `deleteCollection(name)`               | `void` / `None`         | `DELETE /api/v1/database/{db}/collections/{name}`     |

### Collection

| Method                                  | Returns                 | Server Endpoint                                                |
| --------------------------------------- | ----------------------- | -------------------------------------------------------------- |
| `count()`                               | `number` / `int`        | `GET /api/v1/database/{db}/collections/{col}`                  |
| `addDocuments({...})`                   | `InsertResult`          | `POST /api/v1/database/{db}/collections/{col}/documents`       |
| `getDocuments()`                        | `GetResult`             | `GET /api/v1/database/{db}/collections/{col}/documents`        |
| `updateDocument({id, metadata})`        | `void` / `None`         | `PUT /api/v1/database/{db}/collections/{col}/documents`        |
| `updateDocuments({ids, metadatas})`     | `void` / `None`         | `PUT /api/v1/database/{db}/collections/{col}/documents`        |
| `deleteDocuments({ids})`                | `void` / `None`         | `DELETE /api/v1/database/{db}/collections/{col}/documents`     |
| `query({...})`                          | `QueryResult`           | `POST /api/v1/database/{db}/collections/{col}/documents/query` |
| `peek(n?)`                              | `Document[]`            | `GET .../documents` → slice first N (default 5)                |

---

## Project Structure

```
sdks/
  typescript/
    package.json          # @achillesdb/client, type: "module"
    tsconfig.json
    src/
      index.ts            # public exports
      client.ts           # AchillesClient class
      database.ts         # Database class
      collection.ts       # Collection class
      types.ts            # all interfaces and types
      errors.ts           # AchillesError + ErrorCode
      validation.ts       # client-side validation helpers
      http.ts             # fetch wrapper with timeout, error mapping
    tests/
      client.test.ts
      database.test.ts
      collection.test.ts
      validation.test.ts
  python/
    pyproject.toml        # achillesdb
    achillesdb/
      __init__.py         # public exports
      client.py           # AchillesClient (sync, using httpx.Client)
      async_client.py     # AsyncAchillesClient (using httpx.AsyncClient)
      database.py         # Database / AsyncDatabase
      collection.py       # Collection / AsyncCollection
      types.py            # dataclasses
      errors.py           # AchillesError
      validation.py       # client-side validation
      http.py             # httpx wrapper (shared sync/async logic)
    tests/
      test_client.py
      test_database.py
      test_collection.py
      test_validation.py
```

---

## Example: Full Workflow

### TypeScript

```ts
import { AchillesClient } from "@achillesdb/client";

const client = new AchillesClient({
  port: 8180,
  embeddingFunction: async (texts) => {
    // call your embedding API
    return texts.map(t => Array(384).fill(0).map(() => Math.random()));
  },
});

// Create db and collection
const db = await client.createDatabase("movies_db");
const collection = await db.createCollection("reviews");

// Insert documents
const result = await collection.addDocuments({
  ids: ["r1", "r2"],
  documents: ["Great movie!", "Terrible plot."],
  metadatas: [
    { rating: 5, genre: "action" },
    { rating: 1, genre: "drama" },
  ],
});
console.log(result); // { ids: ["r1", "r2"], count: 2 }

// Query with filters
const results = await collection.query({
  query: "best action movie",
  topK: 5,
  where: { genre: "action", rating: { $gte: 4 } },
});
```

### Python (Sync)

```python
from achillesdb import AchillesClient

client = AchillesClient(
    port=8180,
    embedding_function=lambda texts: [[0.0] * 384 for _ in texts],
)

db = client.create_database("movies_db")
collection = db.create_collection("reviews")

result = collection.add_documents(
    ids=["r1", "r2"],
    documents=["Great movie!", "Terrible plot."],
    metadatas=[
        {"rating": 5, "genre": "action"},
        {"rating": 1, "genre": "drama"},
    ],
)

results = collection.query(
    query="best action movie",
    top_k=5,
    where={"genre": "action", "rating": {"$gte": 4}},
)
```

---

## Future Roadmap

Features deferred from v1 — will be added based on user feedback:

- **Pre-insert hooks** — `beforeInsert` / `beforeInsertBatch` transforms on documents before server send
- **Cross-collection query** — query multiple collections in one call (planned as a backend endpoint, not SDK-side fan-out)
- **Pagination** — cursor-based pagination for `getDocuments()` (requires backend support with WiredTiger cursors)
