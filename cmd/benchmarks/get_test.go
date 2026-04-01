package benchmarks

import (
	"fmt"
	"testing"
)

// BenchmarkGetDocuments benchmarks GetDocuments with varying collection sizes.
//
//	go test -bench=BenchmarkGetDocuments -benchmem ./cmd/benchmarks/
func BenchmarkGetDocuments(b *testing.B) {
	cases := []struct {
		name    string
		numDocs int
	}{
		{"10docs", 10},
		{"50docs", 50},
		{"100docs", 100},
		{"500docs", 500},
	}

	for _, tc := range cases {
		// Setup once per case — outside b.Run
		wtService, dbSvc, cleanup := setupBenchCollection(b, fmt.Sprintf("get_%s", tc.name), "bench_col")

		soa := generateSOA(tc.numDocs, 1536)
		if err := dbSvc.InsertDocuments("bench_col", soa); err != nil {
			wtService.Close()
			cleanup()
			b.Fatalf("InsertDocuments failed: %v", err)
		}

		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				docs, err := dbSvc.GetDocuments("bench_col")
				if err != nil {
					b.Fatalf("GetDocuments failed: %v", err)
				}
				if len(docs) != tc.numDocs {
					b.Fatalf("expected %d docs, got %d", tc.numDocs, len(docs))
				}
			}
		})

		wtService.Close()
		cleanup()
	}
}
