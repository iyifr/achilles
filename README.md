# AchillesDB

A performant single node vector database.

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

### Vector Query with Metadata Filtering

Achilles supports expressive metadata filtering using the `where` argument to `query`. The first argument is always `top_k`. You can pass raw dicts or build clauses with `W` from `achillesdb`.

```python
from achillesdb import W

# Similar documents, filtered by category
collection.query(
    top_k=10,
    query_embedding=[0.15, -0.32, 0.51],
    where={"category": "tech"},
)

# Filter by multiple fields
collection.query(
    top_k=5,
    query_embedding=[0.15, -0.32, 0.51],
    where={"category": "tech", "year": 2024},
)

# Numeric comparison: tech articles after 2022
collection.query(
    top_k=5,
    query_embedding=[0.2, -0.1, 0.5],
    where={"category": "tech", "year": {"$gt": 2022}},
)

# $in: authors in a list (raw dict or W.in_)
collection.query(
    top_k=3,
    query_embedding=[0.45, -0.12, 0.28],
    where=W.in_("author", ["jane", "john"]),
)

# $or: food category OR year before 2024
collection.query(
    top_k=2,
    query_embedding=[0.98, 0.12, -0.21],
    where=W.or_(
        W.eq("category", "food"),
        W.lt("year", 2024),
    ),
)

# Nested $or: (tech and 2024) or (food and 2023)
collection.query(
    top_k=8,
    query_embedding=[0.23, 0.91, -0.46],
    where=W.or_(
        W.and_(W.eq("category", "tech"), W.eq("year", 2024)),
        W.and_(W.eq("category", "food"), W.eq("year", 2023)),
    ),
)

# $arrContains: array field must contain at least one value
collection.query(
    top_k=10,
    query_embedding=[...],  # your query vector
    where=W.arr_contains("allowed_acls", ["acl-readers", "acl-admin-42"]),
)

# Combine category with ACL check
collection.query(
    top_k=10,
    query_embedding=[...],
    where=W.and_(
        W.eq("category", "tech"),
        W.arr_contains("allowed_acls", ["acl-readers", "acl-admin-42"]),
    ),
)
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

The SDK sends `document_id` and `updates` (metadata patches) to the PUT endpoint. There is no `where`-based bulk update in the client yet; use the REST API if you need that.

```python
collection.update_document(
    document_id="article-1",
    updates={"category": "ai", "featured": True},
)
```

### Architecture

AchillesDB has two core components:

1. **Document Store**: WiredTiger KV store holds BSON bytes (on disk json document representation).
2. **Vector Search**: FAISS vector search toolkit for efficient vector search.

The system connects the two layers through an intermediary LABEL_ID --> DOC_ID key value table that maps embedding IDs to document IDs.

For detailed architecture diagrams and data flow, see [ARCHITECTURE.md](ARCHITECTURE.md).

## License

MIT
