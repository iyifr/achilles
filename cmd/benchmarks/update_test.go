package benchmarks

import (
	dbservice "achillesdb/pkgs/db_service"
	"fmt"
	"testing"
)

// BenchmarkUpdateDocuments benchmarks UpdateDocuments by document ID.
//
//	go test -bench=BenchmarkUpdateDocuments -benchmem ./cmd/benchmarks/
func BenchmarkUpdateDocuments(b *testing.B) {
	cases := []struct {
		name      string
		totalDocs int
	}{
		{"50docs", 50},
		{"100docs", 100},
	}

	for _, tc := range cases {
		// Setup once per case — outside b.Run
		wtService, dbSvc, cleanup := setupBenchCollection(b, fmt.Sprintf("upd_%s", tc.name), "bench_col")

		soa := generateSOA(tc.totalDocs, 1536)
		if err := dbSvc.InsertDocuments("bench_col", soa); err != nil {
			wtService.Close()
			cleanup()
			b.Fatalf("InsertDocuments failed: %v", err)
		}

		targetID := soa.Ids[0]
		updates := map[string]any{"content": "updated content", "metadata": map[string]any{"updated": true}}

		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				payload := &dbservice.DocUpdatePayload{
					DocumentId: targetID,
					Updates:    updates,
				}
				if err := dbSvc.UpdateDocuments("bench_col", payload); err != nil {
					b.Fatalf("UpdateDocuments failed: %v", err)
				}
			}
		})

		wtService.Close()
		cleanup()
	}
}
