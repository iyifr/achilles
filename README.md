# AchillesDB

Vector-document database built with Go, WiredTiger, and FAISS.

## Quick Start

```bash
docker run -d -p 8180:8180 ghcr.io/iyifr/achilles:latest
```

## API

| Method | Endpoint                                                  | Description       |
| ------ | --------------------------------------------------------- | ----------------- |
| POST   | `/api/v1/database`                                        | Create database   |
| POST   | `/api/v1/database/{db}/collections`                       | Create collection |
| POST   | `/api/v1/database/{db}/collections/{col}/documents`       | Insert documents  |
| POST   | `/api/v1/database/{db}/collections/{col}/documents/query` | Query by vector   |
| GET    | `/api/v1/database/{db}/collections/{col}/documents`       | Get documents     |
| PUT    | `/api/v1/database/{db}/collections/{col}/documents`       | Update documents  |

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

Combine semantic similarity search with metadata filters using the `where` clause:

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
```

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

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   HTTP API (FastHTTP)               │
├─────────────────────────────────────────────────────┤
│                   Database Service                  │
├────────────────────────┬────────────────────────────┤
│      WiredTiger        │          FAISS             │
│   (Document Storage)   │    (Vector Indexes)        │
└────────────────────────┴────────────────────────────┘
```

**WiredTiger** handles document persistence—metadata, content, and collection catalogs are stored in B-tree tables. Documents are serialized using BSON for efficient storage and retrieval.

**FAISS** handles vector similarity search. Each collection maintains its own index file (persisted to disk) for fast approximate nearest neighbor queries.

**FastHTTP** serves the REST API with minimal allocation overhead.

### Design Tradeoffs

| Choice                       | Tradeoff                                                                                                                                                          |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Separate storage engines** | Optimized for each workload (documents vs vectors), but requires coordination between systems                                                                     |
| **FAISS flat indexes**       | Ultimate simplicity (still researching Faiss Indexes), accurate results. Scales to ~1M vectors per collection before needing IVF for non-exhaustive vector search |
| **Post-filter metadata**     | Vector search runs first, then filters. Fast for selective filters, slower for broad queries with strict filters                                                  |
| **Single-node**              | No distributed complexity. Vertical scaling only—add RAM for larger indexes                                                                                       |
| **CGO bindings**             | Direct C library access for Wiredtiger & Faiss.. (no external packages) and cross-compilation                                                                     |

### When to Use AchillesDB

**Good fit:**

- Semantic search over document collections
- RAG applications needing fast retrieval
- Prototypes!

## Build from Source

```bash
# macOS
brew install wiredtiger faiss
go build -o achillesdb .

# Docker
docker build -t achillesdb .
```

## License

MIT
