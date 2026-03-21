# AchillesDB Python SDK Specification

This document defines the modern "Fluent" API for the AchillesDB Python SDK. It covers both Synchronous and Asynchronous clients.

## Core Concepts

The SDK is organized into three primary layers:
1. **Client (`AchillesClient` / `AsyncAchillesClient`)**: Handles connection and database-level operations.
2. **Database (`SyncDatabase` / `AsyncDatabase`)**: Handles collection-level operations and cross-collection queries.
3. **Collection (`SyncCollection` / `AsyncCollection`)**: Handles document-level operations (CRUD + Search).

---

## 1. Client

### Initialization
```python
from achillesdb import AchillesClient, AsyncAchillesClient

# Sync
client = AchillesClient(host="localhost", port=8180)

# Async
async_client = AsyncAchillesClient(host="localhost", port=8180)
```

### Methods
| Method | Returns | Description |
| :--- | :--- | :--- |
| `ping()` | `bool` | Check server connectivity. |
| `create_database(name)` | `Database` | Creates and returns a Database object. |
| `list_databases()` | `list[Database]` | Returns a list of Database objects. |
| `database(name)` | `Database` | Returns a Database handle (no network call). |
| `delete_database(name)` | `None` | Deletes a database. |

---

## 2. Database

### Methods
| Method | Returns | Description |
| :--- | :--- | :--- |
| `create_collection(name)` | `Collection` | Creates and returns a Collection object. |
| `list_collections()` | `list[Collection]` | Returns a list of Collection objects. |
| `get_collection(name)`| `Collection` | Fetches and returns an existing Collection. |
| `collection(name)` | `Collection` | Returns a Collection handle (local or fetches metadata). |
| `query_collections(...)`| `list[dict]` | Cross-collection vector search. |

---

## 3. Collection

### Document Operations
| Method | Returns | Description |
| :--- | :--- | :--- |
| `add_documents(...)` | `None` | Insert documents with IDs, text, and embeddings. |
| `get_documents()` | `list[dict]` | Retrieve all documents in the collection. |
| `query_documents(...)` | `list[dict]` | Vector search within the collection. |
| `update_documents(...)`| `None` | Update metadata for a specific document. |
| `delete_documents(...)`| `None` | Delete documents by ID list. |
| `count()` | `int` | Get total document count. |
| `peek(n=5)` | `list[dict]` | Quickly view the first `n` documents. |

---

## 4. Metadata Filtering (`W` Helpers)

Import the `W` helper for a clean syntax when filtering:
```python
from achillesdb.where import W

# Examples
where = W.eq("category", "fruit")
where = W.and_(W.gt("year", 2020), W.ne("popular", False))
where = W.in_("tags", ["new", "sale"])
```

---
