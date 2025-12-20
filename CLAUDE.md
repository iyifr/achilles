# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run Commands

```bash
# Build
go build .

# Run server (port 8180)
go run .

# Run tests
go test ./...
go test ./pkgs/db_service/   # specific package

# Docker
docker build -t achillesdb:latest .
docker-compose up
```

## Architecture Overview

GlowstickDB is a hybrid vector-document database combining:

- **WiredTiger** - B-tree key-value storage for documents (via CGO)
- **FAISS** - Vector similarity search (via CGO)
- **BSON** - Binary serialization for complex documents
- **FastHTTP** - High-performance HTTP server

### Layer Structure

```
HTTP API (server/router.go)
    ↓
Database Service (pkgs/db_service/)
    ↓
Storage: WiredTiger + FAISS (pkgs/wiredtiger/, pkgs/faiss/)
```

### Key Packages

**pkgs/db_service/** - Core database logic

- `interface.go` - `DBService` interface, data types, table URIs
- `impl.go` - CRUD operations, vector search, collection management
- `filter.go` - Metadata filtering ($gt, $in, $or, etc.)
- `errors.go` - `DBError` type with HTTP status mapping

**pkgs/wiredtiger/** - WiredTiger Go bindings

- `wt_service.go` - `WTService` interface
- `wt_service_cgo.go` - CGO implementation
- `wt_service_nocgo.go` - Stub for non-CGO builds

**pkgs/faiss/** - FAISS vector search bindings

- `faiss_service.go` - `FAISSService` interface
- `faiss_service_cgo.go` - CGO implementation

**server/** - HTTP layer

- `server.go` - Server initialization, signal handling
- `router.go` - FastHTTP routes and handlers
- `openapi.go` - OpenAPI spec at `/docs`

### Data Flow

1. HTTP request → `router.go` handler
2. Handler creates `DBService` via `DatabaseService(DbParams{...})`
3. Service reads/writes via `WTService` (documents) and `FAISSService` (vectors)
4. Documents stored as BSON in WiredTiger tables
5. Embeddings stored in FAISS index files under `VECTORS_HOME`

### Key Tables (WiredTiger)

- `table:_catalog` - Database and collection metadata
- `table:_stats` - Collection statistics (doc count, index size)
- `table:label_docID` - Maps FAISS vector labels → document IDs
- `table:collection-{name}-{db}` - Per-collection document storage

### Environment Variables

- `WT_HOME` - WiredTiger data directory (default: `volumes/WT_HOME`)
- `VECTORS_HOME` - Vector index files (default: `volumes/vectors`)

## API Endpoints

All endpoints under `/api/v1`:

| Method | Path                                               | Purpose            |
| ------ | -------------------------------------------------- | ------------------ |
| POST   | `/database`                                        | Create database    |
| GET    | `/databases`                                       | List all databases |
| DELETE | `/database/{db}`                                   | Delete database    |
| POST   | `/database/{db}/collections`                       | Create collection  |
| GET    | `/database/{db}/collections`                       | List collections   |
| DELETE | `/database/{db}/collections/{col}`                 | Delete collection  |
| POST   | `/database/{db}/collections/{col}/documents`       | Insert documents   |
| GET    | `/database/{db}/collections/{col}/documents`       | Get all documents  |
| DELETE | `/database/{db}/collections/{col}/documents`       | Delete by IDs      |
| POST   | `/database/{db}/collections/{col}/documents/query` | Vector search      |

## Testing Notes

Tests create temporary WiredTiger directories and set `VECTORS_HOME` per test. Cleanup happens via `t.Cleanup()`.

```go
// Test pattern
tmpDir := t.TempDir()
os.Setenv("VECTORS_HOME", filepath.Join(tmpDir, "vectors"))
```

You can start the server locally with `go run .` and test API changes with curl.

## CGO Requirements

Both WiredTiger and FAISS require CGO. The `*_nocgo.go` files provide stubs for syntax checking without native libraries. Full functionality requires:

- WiredTiger library installed
- FAISS library with BLAS/LAPACK
- `CGO_ENABLED=1`
