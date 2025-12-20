# AchillesDB

Vector-document database built with Go, WiredTiger, and FAISS.

Note: Not production ready.

## Quick Start

```bash
docker run -d -p 8180:8180 ghcr.io/iyifr/achilles:latest
```

## API

| Method | Endpoint                                                       | Description       |
| ------ | -------------------------------------------------------------- | ----------------- |
| POST   | `/api/v1/database`                                             | Create database   |
| POST   | `/api/v1/database/{db}/collections`                            | Create collection |
| POST   | `/api/v1/database/{db}/collections/{col_name}/documents`       | Insert documents  |
| POST   | `/api/v1/database/{db}/collections/{col_name}/documents/query` | Query by vector   |
| GET    | `/api/v1/database/{db}/collections/{col_name}/documents`       | Get documents     |
| PUT    | `/api/v1/database/{db}/collections/{col_name}/documents`       | Update documents  |

## Usage

```bash
# Create database
curl -X POST localhost:8180/api/v1/database \
  -H "Content-Type: application/json" \
  -d '{"name": "mydb"}'

# Create collection
curl -X POST localhost:8180/api/v1/database/mydb/collections \
  -H "Content-Type: application/json" \
  -d '{"name": "articles"}'

# Insert documents with metadata
curl -X POST localhost:8180/api/v1/database/mydb/collections/articles/documents \
  -H "Content-Type: application/json" \
  -d '{
    "documents": [
      {
        "id": "article-1",
        "content": "Introduction to machine learning",
        "embedding": [0.12, -0.34, 0.56, ...],
        "metadata": {"category": "tech", "author": "jane", "year": 2024}
      },
      {
        "id": "article-2",
        "content": "Cooking with seasonal ingredients",
        "embedding": [0.78, 0.23, -0.45, ...],
        "metadata": {"category": "food", "author": "john", "year": 2023}
      }
    ]
  }'
```

### Vector Query with Metadata Filtering

Achilles supports expressive metadata filtering using the `where` clause.
You can combine semantic vector search with powerful filters:

```bash
# Find similar documents, filtered by category
curl -X POST localhost:8180/api/v1/database/mydb/collections/articles/documents/query \
  -H "Content-Type: application/json" \
  -d '{
    "top_k": 10,
    "query_embedding": [0.15, -0.32, 0.51, ...],
    "where": {"category": "tech"}
  }'

# Filter by multiple fields
curl -X POST localhost:8180/api/v1/database/mydb/collections/articles/documents/query \
  -H "Content-Type: application/json" \
  -d '{
    "top_k": 5,
    "query_embedding": [0.15, -0.32, 0.51, ...],
    "where": {"category": "tech", "year": 2024}
  }'

# Numeric Comparison: Find tech articles after 2022
curl -X POST localhost:8180/api/v1/database/mydb/collections/articles/documents/query \
  -H "Content-Type: application/json" \
  -d '{
    "top_k": 5,
    "query_embedding": [0.2, -0.1, 0.5, ...],
    "where": {"category": "tech", "year": {"$gt": 2022}}
  }'

# $in Operator: Return articles by author in given list
curl -X POST localhost:8180/api/v1/database/mydb/collections/articles/documents/query \
  -H "Content-Type: application/json" \
  -d '{
    "top_k": 3,
    "query_embedding": [0.45, -0.12, 0.28, ...],
    "where": {"author": {"$in": ["jane", "john"]}}
  }'

# $and / $or Logical Operators: Nested and/or conditions
curl -X POST localhost:8180/api/v1/database/mydb/collections/articles/documents/query \
  -H "Content-Type: application/json" \
  -d '{
    "top_k": 2,
    "query_embedding": [0.98, 0.12, -0.21, ...],
    "where": {
      "$or": [
        {"category": "food"},
        {"year": {"$lt": 2024}}
      ]
    }
  }'

# Complex Nested Condition: articles in tech from 2024 OR (food from 2023)
curl -X POST localhost:8180/api/v1/database/mydb/collections/articles/documents/query \
  -H "Content-Type: application/json" \
  -d '{
    "top_k": 8,
    "query_embedding": [0.23, 0.91, -0.46, ...],
    "where": {
      "$or": [
        {"category": "tech", "year": 2024},
        {"category": "food", "year": 2023}
      ]
    }
  }'

# Achillesdb supports arrays as metadata values as well as objects.
# Suppose you have already determined the list of ACL IDs the user is part of:
# For example, user_acls = ["acl-readers", "acl-admin-42"]
curl -X POST localhost:8180/api/v1/database/mydb/collections/articles/documents/query \
  -H "Content-Type: application/json" \
  -d '{
    "top_k": 10,
    "query_embedding": [...],
    "where": {
      "allowed_acls": { "$arrContains": ["acl-readers", "acl-admin-42"] }
    }
  }'
# This query will only return documents where the "allowed_acls" array field contains
# at least one of the ACLs the user belongs to.

// You can combine this RBAC restriction with other retrieval criteria as needed,
// e.g., filter to "tech" category with access check:
curl -X POST localhost:8180/api/v1/database/mydb/collections/articles/documents/query \
  -H "Content-Type: application/json" \
  -d '{
    "top_k": 10,
    "query_embedding": [...],
    "where": {
      "category": "tech",
      "allowed_acls": { "$arrContains": ["acl-readers", "acl-admin-42"] }
    }
  }'

```

> **Supported Operators:**
>
> - Simple equality: `{"field": value}`
> - $gt, $gte, $lt, $lte (numbers/dates): `{"field": {"$gt": 10}}`
> - $eq, $ne (explicit equality/inequality)
> - $in, $nin: `{"field": {"$in": [a, b, c]}}`
> - $and, $or for combining filters
> - $arrContains: for checking items inside array fields

### Update Documents

```bash
# Update by document ID
curl -X PUT localhost:8180/api/v1/database/mydb/collections/articles/documents \
  -H "Content-Type: application/json" \
  -d '{
    "document_id": "article-1",
    "updates": {"category": "ai", "featured": true}
  }'

# Update by filter
curl -X PUT localhost:8180/api/v1/database/mydb/collections/articles/documents \
  -H "Content-Type: application/json" \
  -d '{
    "where": {"author": "jane"},
    "updates": {"reviewed": true}
  }'
```

### Architecture

```
    ╔═══════════════════════════════════════════════════════════════════════════╗
    ║                                                                           ║
    ║                         ⚡ FastHTTP API Layer ⚡                           ║
    ║                     Minimal allocation overhead                           ║
    ║                                                                           ║
    ╠═══════════════════════════════════════════════════════════════════════════╣
    ║                                                                           ║
    ║                          🔧 Service Layer                                 ║
    ║                     REST Handlers & Routing                               ║
    ║                                                                           ║
    ╠═══════════════════════════════════════════════════════════════════════════╣
    ║                                                                           ║
    ║                       📊 Database Service Layer                           ║
    ║                                                                           ║
    ╠═══════════════════════════════╦═══════════════════════════════════════════╣
    ║                               ║                                           ║
    ║   📁 Collection Management    ║    🔍 Vector Search Orchestration         ║
    ║                               ║                                           ║
    ╠═══════════════════════════════╩═══════════════════════════════════════════╣
    ║                                                                           ║
    ║                    💾 Storage Layer: Persistence & Indexing               ║
    ║                                                                           ║
    ╠═══════════════════════╦═══════════════════════╦═══════════════════════════╣
    ║                       ║                       ║                           ║
    ║     WiredTiger        ║         FAISS         ║      📦 BSON              ║
    ║                       ║                       ║                           ║
    ║   KV + B-tree Tables  ║   Vector Indexing     ║   Serialization Layer     ║
    ║                       ║   Flat│HNSW│IVF       ║   Efficient binary JSON   ║
    ║                       ║                       ║                           ║
    ╚═══════════════════════╩═══════════════════════╩═══════════════════════════╝
```

<details>
<summary><b>📖 Component Details</b></summary>

| Component      | Role                                                                                       |
| -------------- | ------------------------------------------------------------------------------------------ |
| **WiredTiger** | Document persistence—metadata, content, and collection catalogs stored in B-tree tables    |
| **FAISS**      | Vector similarity search—each collection maintains its own index file for fast ANN queries |
| **BSON**       | Serialization layer—efficient binary encoding for complex nested documents                 |
| **FastHTTP**   | HTTP server—high-performance, low-allocation request handling                              |

</details>

## License

MIT

```

```
