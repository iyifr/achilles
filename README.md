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

## Usage

```bash
# Create database
curl -X POST localhost:8180/api/v1/database \
  -H "Content-Type: application/json" \
  -d '{"name": "mydb"}'

# Create collection
curl -X POST localhost:8180/api/v1/database/mydb/collections \
  -H "Content-Type: application/json" \
  -d '{"name": "docs"}'

# Insert documents
curl -X POST localhost:8180/api/v1/database/mydb/collections/docs/documents \
  -H "Content-Type: application/json" \
  -d '{"documents": [{"id": "1", "content": "hello", "embedding": [0.1, 0.2, ...]}]}'

# Query
curl -X POST localhost:8180/api/v1/database/mydb/collections/docs/documents/query \
  -H "Content-Type: application/json" \
  -d '{"top_k": 5, "query_embedding": [0.1, 0.2, ...]}'
```

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
