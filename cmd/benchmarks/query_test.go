package benchmarks

import (
	dbservice "achillesdb/pkgs/db_service"
	"fmt"
	"testing"
)

// BenchmarkQuery benchmarks QueryCollection with varying collection sizes and top_k values.
//
//	go test -bench=BenchmarkQuery -benchmem ./cmd/benchmarks/
//	go test -bench=BenchmarkQuery/500docs -cpuprofile=cpu.prof -memprofile=mem.prof ./cmd/benchmarks/
func BenchmarkQuery(b *testing.B) {
	collectionSizes := []int{100, 500}

	for _, numDocs := range collectionSizes {
		// Setup once per collection size — outside b.Run so it's excluded from timing
		wtService, dbSvc, cleanup := setupBenchCollection(b, fmt.Sprintf("query_%d", numDocs), "bench_col")

		soa := generateSOA(numDocs, 1536)
		if err := dbSvc.InsertDocuments("bench_col", soa); err != nil {
			wtService.Close()
			cleanup()
			b.Fatalf("InsertDocuments failed: %v", err)
		}

		queryEmbedding := genNormalizedEmbeddings(1536)

		for _, topK := range []int{5, 10, 50} {
			name := fmt.Sprintf("%ddocs_topk%d", numDocs, topK)
			query := dbservice.QueryStruct{
				TopK:           int32(topK),
				QueryEmbedding: queryEmbedding,
			}

			b.Run(name, func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					if _, err := dbSvc.QueryCollection("bench_col", query); err != nil {
						b.Fatalf("QueryCollection failed: %v", err)
					}
				}
			})
		}

		wtService.Close()
		cleanup()
	}
}

// BenchmarkQueryWithFilters benchmarks vector search with metadata filters applied.
//
//	go test -bench=BenchmarkQueryWithFilters -benchmem ./cmd/benchmarks/
func BenchmarkQueryWithFilters(b *testing.B) {
	wtService, dbSvc, cleanup := setupBenchCollection(b, "query_filters", "bench_col")
	defer cleanup()
	defer wtService.Close()

	soa := generateSOA(500, 1536)
	if err := dbSvc.InsertDocuments("bench_col", soa); err != nil {
		b.Fatalf("InsertDocuments failed: %v", err)
	}

	queryEmbedding := genNormalizedEmbeddings(1536)

	filterCases := []struct {
		name    string
		filters map[string]any
	}{
		{"no_filter", nil},
		{"eq_filter", map[string]any{"type": "benchmark"}},
		{"gt_filter", map[string]any{"index": map[string]any{"$gt": 250}}},
		{"and_filter", map[string]any{
			"type":  "benchmark",
			"index": map[string]any{"$gt": 100},
		}},
	}

	for _, fc := range filterCases {
		b.Run(fc.name, func(b *testing.B) {
			query := dbservice.QueryStruct{
				TopK:           10,
				QueryEmbedding: queryEmbedding,
				Filters:        fc.filters,
			}

			for i := 0; i < b.N; i++ {
				if _, err := dbSvc.QueryCollection("bench_col", query); err != nil {
					b.Fatalf("QueryCollection failed: %v", err)
				}
			}
		})
	}
}
