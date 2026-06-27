# AchillesDB
A chromadb alternative for low volume vector databases.

Built with primitives for document db land (MongoDB), namely:
- Wiredtiger (via CGO)
- BSON (Binary storage format for documents data)

## Quick Start

```bash
docker run -d -p 8180:8180 ghcr.io/iyifr/achilles:latest
```

Python client ([`achillesdb`](sdk/python)):

```bash
pip install achillesdb
# or from this repo:
# pip install -e ./sdk/python
```

## Usage

Examples use the sync client from [`sdk/python`](sdk/python). Pass `embedding_function` on the client if you omit embeddings on insert or use text `query` on search; otherwise supply `query_embedding` / per-document embeddings explicitly.

```python
from achillesdb import AchillesClient

def embed(texts: list[str]) -> list[list[float]]:
    return [[0.12, -0.34, 0.56], [0.78, 0.23, -0.45]]  # replace with your embedding model

client = AchillesClient(host="localhost", port=8180, embedding_function=embed)

db = client.create_database("mydb")
collection = db.create_collection("articles")

collection.add_documents(
    ids=["article-1", "article-2"],
    documents=[
        "Introduction to machine learning",
        "Cooking with seasonal ingredients",
    ],
    embeddings=[
        [0.12, -0.34, 0.56],
        [0.78, 0.23, -0.45],
    ],
    metadatas=[
        {"category": "tech", "author": "jane", "year": 2024},
        {"category": "food", "author": "john", "year": 2023},
    ],
)
```

### Metadata Filtering

Use the `where` argument on the query function to filter results based on metadata. The python SDK offers two ways to build filters - plain dicts or the `W` helper, W database.

#### Plain dicts
Pass a Python `dict` with the same structure as the `where` object in `POST .../documents/query`. For filters stored as JSON text, use `json.loads` and pass the resulting dict.

```python
import json

article_collection.query(
    top_k=10,
    query_embedding=[0.15, -0.32, 0.51],
    where={"category": "tech"},
)
 
article_collection.query(
    top_k=5,
    query_embedding=[0.15, -0.32, 0.51],
    where={"category": "tech", "year": 2024},
)

# Comparisons and $in
article_collection.query(
    top_k=5,
    query_embedding=[0.2, -0.1, 0.5],
    where={"category": "tech", "year": {"$gt": 2022}},
)
article_collection.query(
    top_k=3,
    query_embedding=[0.45, -0.12, 0.28],
    where={"author": {"$in": ["jane", "john"]}},
)

# $or
collection.query(
    top_k=2,
    query_embedding=[0.98, 0.12, -0.21],
    where={
        "$or": [
            {"category": "food"},
            {"year": {"$lt": 2024}},
        ]
    },
)

# Nested $or: (tech and 2024) or (food and 2023)
collection.query(
    top_k=8,
    query_embedding=[0.23, 0.91, -0.46],
    where={
        "$or": [
            {"$and": [{"category": "tech"}, {"year": 2024}]},
            {"$and": [{"category": "food"}, {"year": 2023}]},
        ]
    },
)

# $arrContains and combining with $and
collection.query(
    top_k=10,
    query_embedding=[...],  # your query vector
    where={
        "$and": [
            {"category": "tech"},
            {"allowed_acls": {"$arrContains": ["acl-readers", "acl-admin-42"]}},
        ]
    },
)

# From a JSON string
collection.query(
    top_k=5,
    query_embedding=[0.2, -0.1, 0.5],
    where=json.loads('{"category": "tech", "year": {"$gt": 2022}}'),
)
```

#### `W` helpers (`W.eq`, `W.or_`, …)
Optional filter builder.

```python
from achillesdb import W

collection.query(
    top_k=3,
    query_embedding=[0.45, -0.12, 0.28],
    where=W.in_("author", ["jane", "john"]),
)

collection.query(
    top_k=2,
    query_embedding=[0.98, 0.12, -0.21],
    where=W.or_(
        W.eq("category", "food"),
        W.lt("year", 2024),
    ),
)

collection.query(
    top_k=8,
    query_embedding=[0.23, 0.91, -0.46],
    where=W.or_(
        W.and_(W.eq("category", "tech"), W.eq("year", 2024)),
        W.and_(W.eq("category", "food"), W.eq("year", 2023)),
    ),
)

collection.query(
    top_k=10,
    query_embedding=[...],
    where=W.arr_contains("allowed_acls", ["acl-readers", "acl-admin-42"]),
)

collection.query(
    top_k=10,
    query_embedding=[...],
    where=W.and_(
        W.eq("category", "tech"),
        W.arr_contains("allowed_acls", ["acl-readers", "acl-admin-42"]),
    ),
)
```

> **Supported operators (plain-dict / JSON shape):**
>
> - Simple equality: `{"field": value}`
> - $gt, $gte, $lt, $lte (numbers/dates): `{"field": {"$gt": 10}}`
> - $eq, $ne (explicit equality/inequality)
> - $in, $nin: `{"field": {"$in": [a, b, c]}}`
> - $and, $or for combining filters
> - $arrContains: for checking items inside array fields
>
> The `W` helpers map to these same operators (for example `W.gt` → `$gt`, `W.or_` → `$or`).

### Update Documents

```python
# Single document by id (returns number updated, usually 1)
n = collection.update_document(
    document_id="article-1",
    updates={"category": "ai", "featured": True},
)

# Bulk: merge metadata into every document matching the filter
n = collection.bulk_update_documents(
    where={"category": "tech"},
    updates={"featured": False},
)
```

### Architecture
AchillesDB has two core components:

1. **Document Store**: Persistent KV store holds binary document data.
2. **Vector Search**: FAISS serves as the vector index allowing efficient search and embedding storage.

## License
MIT
