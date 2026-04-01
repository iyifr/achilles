package benchmarks

import (
	"fmt"
	"testing"
)

// BenchmarkDeleteDocuments benchmarks DeleteDocuments with varying batch sizes.
//
//	go test -bench=BenchmarkDeleteDocuments -benchmem ./cmd/benchmarks/
func BenchmarkDeleteDocuments(b *testing.B) {
	cases := []struct {
		name       string
		totalDocs  int
		deleteBatch int
		dim        int
	}{
		{"delete_1_of_50", 50, 1, 1536},
		{"delete_10_of_50", 50, 10, 1536},
		{"delete_25_of_100", 100, 25, 1536},
		{"delete_50_of_100", 100, 50, 1536},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				wtService, dbSvc, cleanup := setupBenchCollection(b, fmt.Sprintf("del_%s_%d", tc.name, i), "bench_col")
				soa := generateSOA(tc.totalDocs, tc.dim)

				if err := dbSvc.InsertDocuments("bench_col", soa); err != nil {
					wtService.Close()
					cleanup()
					b.Fatalf("InsertDocuments failed: %v", err)
				}

				// Pick the first N IDs to delete
				idsToDelete := soa.Ids[:tc.deleteBatch]

				b.StartTimer()
				_, err := dbSvc.DeleteDocuments("bench_col", idsToDelete)
				b.StopTimer()

				if err != nil {
					b.Fatalf("DeleteDocuments failed: %v", err)
				}

				wtService.Close()
				cleanup()
			}
		})
	}
}
