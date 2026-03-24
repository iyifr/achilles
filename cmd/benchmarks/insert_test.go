package benchmarks

import (
	"fmt"
	"testing"
)

// BenchmarkInsert benchmarks InsertDocuments with various doc counts and embedding dimensions.
//
//	go test -bench=BenchmarkInsert -benchmem ./cmd/benchmarks/
//	go test -bench=BenchmarkInsert/100docs_1536dim -cpuprofile=cpu.prof -memprofile=mem.prof -benchmem ./cmd/benchmarks/
func BenchmarkInsert(b *testing.B) {
	cases := []struct {
		name     string
		docs     int
		dim      int
	}{
		{"10docs_128dim", 10, 128},
		{"10docs_512dim", 10, 512},
		{"10docs_1536dim", 10, 1536},
		{"50docs_1536dim", 50, 1536},
		{"100docs_1536dim", 100, 1536},
		{"500docs_1536dim", 500, 1536},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				wtService, dbSvc, cleanup := setupBenchCollection(b, fmt.Sprintf("ins_%s_%d", tc.name, i), "bench_col")
				soa := generateSOA(tc.docs, tc.dim)
				b.StartTimer()

				if err := dbSvc.InsertDocuments("bench_col", soa); err != nil {
					b.Fatalf("InsertDocuments failed: %v", err)
				}

				b.StopTimer()
				wtService.Close()
				cleanup()
			}
		})
	}
}

// BenchmarkInsertLargePayload benchmarks inserting documents with large content bodies.
//
//	go test -bench=BenchmarkInsertLargePayload -benchmem ./cmd/benchmarks/
func BenchmarkInsertLargePayload(b *testing.B) {
	contentSizes := []int{1024, 4096, 16384} // 1KB, 4KB, 16KB

	for _, contentSize := range contentSizes {
		name := fmt.Sprintf("%dKB_content_50docs", contentSize/1024)
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				wtService, dbSvc, cleanup := setupBenchCollection(b, fmt.Sprintf("ins_large_%d_%d", contentSize, i), "bench_col")
				soa := generateSOAWithContentSize(50, 1536, contentSize)
				b.StartTimer()

				if err := dbSvc.InsertDocuments("bench_col", soa); err != nil {
					b.Fatalf("InsertDocuments failed: %v", err)
				}

				b.StopTimer()
				wtService.Close()
				cleanup()
			}
		})
	}
}

// BenchmarkInsertBatchSizes benchmarks different batch sizes to find optimal insertion size.
//
//	go test -bench=BenchmarkInsertBatchSizes -benchmem ./cmd/benchmarks/
func BenchmarkInsertBatchSizes(b *testing.B) {
	batchSizes := []int{1, 10, 25, 50, 100, 200}

	for _, batchSize := range batchSizes {
		name := fmt.Sprintf("batch_%d", batchSize)
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				wtService, dbSvc, cleanup := setupBenchCollection(b, fmt.Sprintf("ins_batch_%d_%d", batchSize, i), "bench_col")
				soa := generateSOA(batchSize, 1536)
				b.StartTimer()

				if err := dbSvc.InsertDocuments("bench_col", soa); err != nil {
					b.Fatalf("InsertDocuments failed: %v", err)
				}

				b.StopTimer()
				wtService.Close()
				cleanup()
			}
		})
	}
}
