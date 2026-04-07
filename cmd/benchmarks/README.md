# Benchmarks and Profiling

Benchmarks for all core database operations using `go test -bench`.

## Quick Start

```bash
# Run all benchmarks
go test -bench=. -benchmem ./cmd/benchmarks/

# Run a specific benchmark
go test -bench=BenchmarkInsert/100docs_1536dim -benchmem ./cmd/benchmarks/

# Stable results with multiple iterations
go test -bench=. -benchmem -count=5 ./cmd/benchmarks/
```


## Profiling

```bash
# CPU + memory profiles
go test -bench=BenchmarkInsert/100docs_1536dim \
    -cpuprofile=cpu.prof \
    -memprofile=mem.prof \
    -benchmem \
    ./cmd/benchmarks/

# Block + mutex profiles (contention investigation)
go test -bench=BenchmarkInsert/100docs_1536dim \
    -blockprofile=block.prof \
    -mutexprofile=mutex.prof \
    -benchmem \
    ./cmd/benchmarks/

# Web UI (recommended for exploration)
go tool pprof -http=:8080 cpu.prof

# Interactive CLI
go tool pprof cpu.prof
# (pprof) top10             — top functions by time
# (pprof) top20 -cum        — top by cumulative time
# (pprof) list InsertDocuments — annotated source
# (pprof) web               — graph in browser

# Compare before/after
go tool pprof -base=before.prof after.prof
```

## Benchmarks

| Benchmark | File | What it measures |
|-----------|------|-----------------|
| `BenchmarkInsert` | insert_test.go | Insert with various doc counts and embedding dimensions |
| `BenchmarkInsertLargePayload` | insert_test.go | Insert with large document content (1KB-16KB) |
| `BenchmarkInsertBatchSizes` | insert_test.go | Optimal batch size for inserts |
| `BenchmarkInsertHTTPParsing` | insert_http_test.go | HTTP handler overhead (JSON parse, validation, flattening) |
| `BenchmarkQuery` | query_test.go | Vector search with varying collection size and top_k |
| `BenchmarkQueryWithFilters` | query_test.go | Vector search with metadata filters |
| `BenchmarkGetDocuments` | get_test.go | Full collection scan at various sizes |
| `BenchmarkDeleteDocuments` | delete_test.go | Document deletion with varying batch sizes |
| `BenchmarkUpdateDocuments` | update_test.go | Single-document update by ID |

## Profile Types

| Profile | Flag | What it measures |
|---------|------|-----------------|
| CPU | `-cpuprofile` | Where time is spent |
| Memory | `-memprofile` | Heap allocations |
| Block | `-blockprofile` | Goroutine blocking (mutex, channel, I/O) |
| Mutex | `-mutexprofile` | Mutex contention |
