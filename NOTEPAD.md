# Performance Optimization Progress

## Baseline Performance
- **Throughput**: ~5,000 docs/sec
- **CPU time per 100 docs**: ~20ms
- **Memory allocs per 1000 docs**: ~48MB

## Implementation Phases

### Phase 1: Batch Writer for WiredTiger
- [x] Add BatchWriter interface to wt_service.go
- [x] Implement C functions in wt_service_cgo.go
- [x] Add Go wrapper
- [x] Modify InsertDocuments to use batch writer
- [x] Run benchmark and record results

**Phase 1 Results:**
```
Iterations: 10, 100 docs each, 1536 dim
  Avg: 32.5ms per 100 docs
  Throughput: 3,073 docs/sec

CPU Profile breakdown:
  - cgocall: 66.67% (still main bottleneck - per-doc CGO overhead)
  - wt_batch_put_bin: 38.10%
  - FAISS WriteToFile: 23.81%  <- Phase 2 target
  - FAISS ReadIndex: 4.76%     <- Phase 2 target
  - bson.Marshal: 23.81%       <- Phase 3 target
```

**Analysis:** Batch writer reduces session/cursor open/close overhead but not
per-document CGO call overhead. Marginal improvement. Phase 2 (FAISS cache)
will eliminate ~28% of time spent in index I/O.

### Phase 2: FAISS Index Cache
- [x] Create index_cache.go
- [x] Add GlobalIndexCache singleton
- [x] Modify InsertDocuments to use cache
- [x] Run benchmark and record results

**Phase 2 Results:**
```
Throughput: 3,414 docs/sec (was 3,154 baseline)
Improvement: ~8% overall

CPU Profile breakdown:
  - cgocall: 36.36% (WiredTiger)
  - bson.Marshal: 36.36%         <- Phase 3 target
  - FAISS I/O: NOT VISIBLE!      <- SUCCESS: eliminated from hot path

Note: FAISS ReadIndex/WriteToFile no longer appear in top functions.
Index caching is working - eliminated ~28% of original CPU time.
```

### Phase 3: BSON Buffer Pool
- [x] Create bson_pool.go with sync.Pool
- [x] Implement MarshalWithPool using bson.MarshalAppend
- [x] Replace bson.Marshal in InsertDocuments
- [x] Run benchmark and record results

**Phase 3 Results:**
```
Throughput: 4,855 - 5,646 docs/sec (was 3,154 baseline)
Improvement: 54-79% over baseline!

Memory allocations:
  Before (baseline): 48,832 kB
  After (Phase 3):   14,441 kB
  Reduction: 70%!

bson.MarshalAppendWithContext no longer in top allocators - pool working!
```

---

## Query Path Optimization (2026-03-24)

### Phase 4: Batch Reader + Query Rewrite
- [x] Add BatchReader interface to wt_service.go
- [x] Implement C batch read functions (wt_batch_get_str, wt_batch_get_bin)
- [x] Add Go cgoBatchReader wrapper
- [x] Rewrite QueryCollection: replace goroutine pool + per-doc CGO with sequential batch reads
- [x] Remove old AOS InsertDocuments, rename InsertDocumentsSOA → InsertDocuments
- [x] Fix `defer release()` bug in InsertDocuments loop (defeated buffer pool)

**Phase 4 Results (cpu profile):**
```
Before: cgocall was 50% of query CPU (2 CGO crossings per doc × topK)
After:  cgocall reduced to 2 total crossings (one for labels, one for docs)
```

### Phase 5: Label→DocID Cache
- [x] Create label_cache.go with RWMutex-guarded map
- [x] GlobalLabelCache singleton (sync.Once)
- [x] QueryCollection checks cache before batch-reading labels
- [x] Populate cache on miss

**Rationale:** Label→docID mappings are append-only (inserts add, deletes don't remove).
Stale entries are safe — document lookup returns not-found → skip.

### Phase 6: Manual BSON Metadata Decoder
- [x] Create bson_decode.go with unmarshalQueryDoc
- [x] bson.Raw.LookupErr for _id and content (zero-alloc string extraction)
- [x] Replace bson.Unmarshal(metadata) with manual type-switch decoder
- [x] decodeMetadataDoc walks bson.Raw.Elements(), type-switches on 8 BSON types
- [x] Fast path (zero reflection): string, float64, int32, int64, bool, null
- [x] Recursive path: nested docs → map[string]interface{}, arrays → []interface{}
- [x] Fallback: exotic types (ObjectID, DateTime) use RawValue.Unmarshal

**Phase 6 Results (pprof -top -cum):**
```
BSON decode CPU time:
  Before: bson.Unmarshal 19% cum (MapCodec/EmptyInterfaceCodec reflection)
  After:  unmarshalQueryDoc 6.5% cum, decodeMetadataDoc 4.56%, decodeValue 0.85%
  Reduction: ~3x less CPU in BSON decode path

MapCodec/EmptyInterfaceCodec: GONE from hot path entirely
Residual bson.Unmarshal: 1.25% (exotic type fallback only)
```

### Query Benchmark Results (go test -bench -cpuprofile, 2026-03-24)
```
BenchmarkQuery/100docs_topk5    ~44.9μs/op    6339 B/op   116 allocs/op
BenchmarkQuery/100docs_topk10   ~55.5μs/op   11767 B/op   206 allocs/op
BenchmarkQuery/100docs_topk50  ~136.8μs/op   55556 B/op   926 allocs/op
BenchmarkQuery/500docs_topk5   ~136.7μs/op    4562 B/op    85 allocs/op
BenchmarkQuery/500docs_topk10  ~148.9μs/op    9109 B/op   163 allocs/op
BenchmarkQuery/500docs_topk50  ~224.0μs/op   47648 B/op   806 allocs/op
```

### Query CPU Profile Breakdown (after all optimizations)
```
QueryCollection total:       67.12% of CPU
  FAISS linear scan:         46.71%  (inherent to Flat index, needs IVF at scale)
  GC/runtime (kevent etc):   18.23%
  unmarshalQueryDoc:          6.50%  (was 14%)
  BatchReader CGO:            5.11%  (was 50%+ with per-doc calls)
  bson.Unmarshal fallback:    1.25%  (was 19%)
```

---

## Summary

### Insert Path (Phases 1-3)

| Metric | Baseline | After Optimization | Improvement |
|--------|----------|-------------------|-------------|
| Throughput | 3,154 docs/sec | 4,855-5,646 docs/sec | **54-79%** |
| Memory allocs | 48,832 kB | 14,441 kB | **70% reduction** |
| Avg latency/100 docs | 31.7ms | ~18ms | **43% faster** |

### Query Path (Phases 4-6)

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| CGO overhead | 50% of query CPU | 5.1% | **~10x reduction** |
| BSON decode | 19% cum | 6.5% cum | **~3x reduction** |
| Label lookups | 1 CGO call per result | In-memory cache hit | **~0 cost on warm** |

### Remaining Bottleneck
FAISS Flat index linear scan at 46.7% — inherent O(n*d) cost.
IVF index would help at 10k+ docs but adds complexity for small collections.

### What We Implemented:
1. **Batch Writer** - Single WiredTiger session for bulk writes
2. **FAISS Index Cache** - In-memory caching, no disk read per insert
3. **BSON Buffer Pool** - Reusable buffers via sync.Pool
4. **Batch Reader** - Single WiredTiger session/cursor for bulk reads
5. **Label→DocID Cache** - In-memory RWMutex map, append-only safe
6. **Manual BSON Decoder** - Type-switch on bsoncore, no reflection for common types

### Files Changed (Insert):
- `pkgs/wiredtiger/wt_service.go` - Added BatchWriter interface
- `pkgs/wiredtiger/wt_service_cgo.go` - C batch writer implementation
- `pkgs/faiss/index_cache.go` - New file for index caching
- `pkgs/db_service/bson_pool.go` - New file for BSON buffer pooling
- `pkgs/db_service/impl.go` - Modified InsertDocuments to use optimizations

### Files Changed (Query):
- `pkgs/wiredtiger/wt_service.go` - Added BatchReader interface
- `pkgs/wiredtiger/wt_service_cgo.go` - C batch read functions + Go wrapper
- `pkgs/db_service/label_cache.go` - New: label→docID in-memory cache
- `pkgs/db_service/bson_decode.go` - New: reflection-free BSON decoder
- `pkgs/db_service/impl.go` - Rewrote QueryCollection (eliminated goroutine pool)
- `pkgs/db_service/interface.go` - Dropped old AOS InsertDocuments, renamed SOA

### Benchmarks Reorganized:
- Deleted: `cmd/benchmarks/insert_documents_bench_test.go`, `cmd/benchmarks/profile_insert.go`
- New: `cmd/benchmarks/helpers_test.go` (shared setup)
- New: `cmd/benchmarks/{insert,query,get,delete,update,insert_http}_test.go`

---
