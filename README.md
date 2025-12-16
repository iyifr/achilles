# AchillesDB

A high-performance vector-document database built with Go, WiredTiger, and FAISS.

## Features

- **Vector Similarity Search** - Fast approximate nearest neighbor search using FAISS
- **Document Storage** - Persistent document storage with WiredTiger
- **REST API** - Simple HTTP API for all operations
- **Metadata Filtering** - Filter query results by document metadata

## Quick Start with Docker

```bash
# Pull and run the latest image
docker pull ghcr.io/iyifr/achillesdb:latest
docker run -d -p 8180:8180 -v achilles_data:/data ghcr.io/iyifr/achillesdb:latest

# Or use docker-compose
docker-compose up -d
```

## API Endpoints

| Method | Endpoint                                                         | Description         |
| ------ | ---------------------------------------------------------------- | ------------------- |
| POST   | `/api/v1/database`                                               | Create a database   |
| POST   | `/api/v1/database/{db}/collections`                              | Create a collection |
| GET    | `/api/v1/database/{db}/collections`                              | List collections    |
| GET    | `/api/v1/database/{db}/collections/{collection}`                 | Get collection info |
| POST   | `/api/v1/database/{db}/collections/{collection}/documents`       | Insert documents    |
| GET    | `/api/v1/database/{db}/collections/{collection}/documents`       | Get all documents   |
| POST   | `/api/v1/database/{db}/collections/{collection}/documents/query` | Query documents     |
| PUT    | `/api/v1/database/{db}/collections/{collection}/documents`       | Update documents    |

## Example Usage

```bash
# Create a database
curl -X POST http://localhost:8180/api/v1/database -H "Content-Type: application/json" -d '{"name": "mydb"}'

# Create a collection
curl -X POST http://localhost:8180/api/v1/database/mydb/collections -H "Content-Type: application/json" -d '{"name": "docs"}'

# Insert documents with embeddings
curl -X POST http://localhost:8180/api/v1/database/mydb/collections/docs/documents \
  -H "Content-Type: application/json" \
  -d '{
    "documents": [
      {
        "id": "doc1",
        "content": "Hello world",
        "embedding": [0.1, 0.2, 0.3, ...],
        "metadata": {"category": "greeting"}
      }
    ]
  }'

# Query similar documents
curl -X POST http://localhost:8180/api/v1/database/mydb/collections/docs/documents/query \
  -H "Content-Type: application/json" \
  -d '{
    "top_k": 5,
    "query_embedding": [0.1, 0.2, 0.3, ...],
    "where": {"category": "greeting"}
  }'
```

## Building from Source

### Prerequisites

- Go 1.24+
- WiredTiger (C library)
- FAISS with C API

### macOS

```bash
brew install wiredtiger faiss
go build -o glowstickdb .
```

### Linux

See [Dockerfile](./Dockerfile) for complete build instructions.

## Docker Build

```bash
# Build the image
docker build -t achillesdb:latest .

# Run with persistent storage
docker run -d \
  -p 8180:8180 \
  -v $(pwd)/data/wiredtiger:/data/wiredtiger \
  -v $(pwd)/data/vectors:/data/vectors \
  --name achillesdb \
  achillesdb:latest
```

## Environment Variables

| Variable       | Default           | Description               |
| -------------- | ----------------- | ------------------------- |
| `WT_HOME`      | `volumes/WT_HOME` | WiredTiger data directory |
| `VECTORS_HOME` | `volumes/vectors` | FAISS vectors directory   |

## License

MIT
