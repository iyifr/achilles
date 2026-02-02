package benchmarks

import (
	dbservice "achillesdb/pkgs/db_service"
	"achillesdb/pkgs/faiss"
	wt "achillesdb/pkgs/wiredtiger"
	"fmt"
	"math"
	"math/rand/v2"
	"os"

	"runtime"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// BenchmarkInsertDocuments runs benchmarks for InsertDocuments with various configurations.
// Run with: go test -bench=. -benchmem ./cmd/benchmarks/
// With pprof: go test -bench=BenchmarkInsertDocuments -cpuprofile=cpu.prof -memprofile=mem.prof ./cmd/benchmarks/
func BenchmarkInsertDocuments(b *testing.B) {
	benchCases := []struct {
		name         string
		docCount     int
		embeddingDim int
	}{
		{"10docs_128dim", 10, 128},
		{"10docs_512dim", 10, 512},
		{"10docs_1536dim", 10, 1536},
		{"50docs_1536dim", 50, 1536},
		{"100docs_1536dim", 100, 1536},
		{"500docs_1536dim", 500, 1536},
	}

	for _, bc := range benchCases {
		b.Run(bc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				wtService, dbSvc, cleanup := setupBenchDB(b, fmt.Sprintf("bench_%s_%d", bc.name, i))
				collName := "bench_collection"

				if err := dbSvc.CreateDB(); err != nil {
					b.Fatalf("Failed to create DB: %v", err)
				}
				if err := dbSvc.CreateCollection(collName); err != nil {
					b.Fatalf("Failed to create collection: %v", err)
				}

				docs := generateDocuments(bc.docCount, bc.embeddingDim)

				b.StartTimer()
				err := dbSvc.InsertDocuments(collName, docs)
				b.StopTimer()

				if err != nil {
					b.Fatalf("InsertDocuments failed: %v", err)
				}

				wtService.Close()
				cleanup()
			}
		})
	}
}

// BenchmarkInsertDocumentsLargePayload benchmarks inserting documents with large content.
func BenchmarkInsertDocumentsLargePayload(b *testing.B) {
	contentSizes := []int{1024, 4096, 16384} // 1KB, 4KB, 16KB

	for _, contentSize := range contentSizes {
		name := fmt.Sprintf("%dKB_content_50docs", contentSize/1024)
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				wtService, dbSvc, cleanup := setupBenchDB(b, fmt.Sprintf("bench_large_%d_%d", contentSize, i))
				collName := "bench_collection"

				if err := dbSvc.CreateDB(); err != nil {
					b.Fatalf("Failed to create DB: %v", err)
				}
				if err := dbSvc.CreateCollection(collName); err != nil {
					b.Fatalf("Failed to create collection: %v", err)
				}

				docs := generateDocumentsWithContentSize(50, 1536, contentSize)

				b.StartTimer()
				err := dbSvc.InsertDocuments(collName, docs)
				b.StopTimer()

				if err != nil {
					b.Fatalf("InsertDocuments failed: %v", err)
				}

				wtService.Close()
				cleanup()
			}
		})
	}
}

// BenchmarkInsertDocumentsBatchSizes benchmarks different batch sizes to find optimal insertion size.
func BenchmarkInsertDocumentsBatchSizes(b *testing.B) {
	batchSizes := []int{1, 10, 25, 50, 100, 200}

	for _, batchSize := range batchSizes {
		name := fmt.Sprintf("batch_%d", batchSize)
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				wtService, dbSvc, cleanup := setupBenchDB(b, fmt.Sprintf("bench_batch_%d_%d", batchSize, i))
				collName := "bench_collection"

				if err := dbSvc.CreateDB(); err != nil {
					b.Fatalf("Failed to create DB: %v", err)
				}
				if err := dbSvc.CreateCollection(collName); err != nil {
					b.Fatalf("Failed to create collection: %v", err)
				}

				docs := generateDocuments(batchSize, 1536)

				b.StartTimer()
				err := dbSvc.InsertDocuments(collName, docs)
				b.StopTimer()

				if err != nil {
					b.Fatalf("InsertDocuments failed: %v", err)
				}

				wtService.Close()
				cleanup()
			}
		})
	}
}

// BenchmarkInsertDocumentsSOA benchmarks the new SOA-based insertion method.
func BenchmarkInsertDocumentsSOA(b *testing.B) {
	benchCases := []struct {
		name         string
		docCount     int
		embeddingDim int
	}{
		{"10docs_128dim_SOA", 10, 128},
		{"10docs_512dim_SOA", 10, 512},
		{"10docs_1536dim_SOA", 10, 1536},
		{"50docs_1536dim_SOA", 50, 1536},
		{"100docs_1536dim_SOA", 100, 1536},
		{"500docs_1536dim_SOA", 500, 1536},
	}

	for _, bc := range benchCases {
		b.Run(bc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				wtService, dbSvc, cleanup := setupBenchDB(b, fmt.Sprintf("bench_%s_%d", bc.name, i))
				collName := "bench_collection_soa"

				if err := dbSvc.CreateDB(); err != nil {
					b.Fatalf("Failed to create DB: %v", err)
				}
				if err := dbSvc.CreateCollection(collName); err != nil {
					b.Fatalf("Failed to create collection: %v", err)
				}

				soa := generateSOADocuments(bc.docCount, bc.embeddingDim)

				b.StartTimer()
				err := dbSvc.InsertDocumentsSOA(collName, soa)
				b.StopTimer()

				if err != nil {
					b.Fatalf("InsertDocumentsSOA failed: %v", err)
				}

				wtService.Close()
				cleanup()
			}
		})
	}
}

// generateSOADocuments creates test documents in SOA format for benchmarking.
func generateSOADocuments(count, embeddingDim int) *dbservice.GlowstickDocumentSOA {
	soa := &dbservice.GlowstickDocumentSOA{
		Ids:        make([]string, count),
		Contents:   make([]string, count),
		Embeddings: make([]float32, count*embeddingDim),
		Metadatas:  make([]map[string]interface{}, count),
	}

	for i := 0; i < count; i++ {
		soa.Ids[i] = primitive.NewObjectID().Hex()
		soa.Contents[i] = fmt.Sprintf("Benchmark SOA document number %d with some additional content for testing purposes.", i+1)
		soa.Metadatas[i] = map[string]interface{}{"type": "benchmark_soa", "index": i + 1, "category": "test"}

		embedding := genNormalizedEmbeddings(embeddingDim)
		copy(soa.Embeddings[i*embeddingDim:(i+1)*embeddingDim], embedding)
	}

	return soa
}

// setupBenchDB creates a temporary WiredTiger database for benchmarking.
func setupBenchDB(b *testing.B, dirName string) (wt.WTService, dbservice.DBService, func()) {
	b.Helper()

	wtService := wt.WiredTiger()

	timestamp := time.Now().UnixNano()
	testDir := fmt.Sprintf("/tmp/glowstick_bench_%s_%d", dirName, timestamp)
	vectorsDir := fmt.Sprintf("/tmp/glowstick_bench_vectors_%s_%d", dirName, timestamp)

	if err := os.MkdirAll(testDir, 0755); err != nil {
		b.Fatalf("failed to create test dir: %v", err)
	}
	if err := os.MkdirAll(vectorsDir, 0755); err != nil {
		b.Fatalf("failed to create vectors dir: %v", err)
	}

	os.Setenv("VECTORS_HOME", vectorsDir)

	if err := wtService.Open(testDir, getWiredTigerConfig()); err != nil {
		b.Fatalf("Failed to open WiredTiger: %v", err)
	}

	if err := dbservice.InitTablesHelper(wtService); err != nil {
		wtService.Close()
		os.RemoveAll(testDir)
		os.RemoveAll(vectorsDir)
		b.Fatalf("Failed to init tables: %v", err)
	}

	params := dbservice.DbParams{
		Name:      "benchdb",
		KvService: wtService,
	}

	cleanup := func() {
		os.RemoveAll(testDir)
		os.RemoveAll(vectorsDir)
	}

	return wtService, dbservice.DatabaseService(params), cleanup
}

func generateDocuments(count, embeddingDim int) []dbservice.GlowstickDocument {
	docs := make([]dbservice.GlowstickDocument, count)
	for i := 0; i < count; i++ {
		docs[i] = dbservice.GlowstickDocument{
			Id:        primitive.NewObjectID().Hex(),
			Content:   fmt.Sprintf("Benchmark document number %d with some additional content for testing purposes.", i+1),
			Embedding: genNormalizedEmbeddings(embeddingDim),
			Metadata:  map[string]any{"type": "benchmark", "index": i + 1, "category": "test"},
		}
	}
	return docs
}

func generateDocumentsWithContentSize(count, embeddingDim, contentSize int) []dbservice.GlowstickDocument {
	docs := make([]dbservice.GlowstickDocument, count)
	content := generateRandomContent(contentSize)

	for i := 0; i < count; i++ {
		docs[i] = dbservice.GlowstickDocument{
			Id:        primitive.NewObjectID().Hex(),
			Content:   content,
			Embedding: genNormalizedEmbeddings(embeddingDim),
			Metadata:  map[string]any{"type": "benchmark", "index": i + 1, "size": contentSize},
		}
	}
	return docs
}

func generateRandomContent(size int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "
	b := make([]byte, size)
	for i := range b {
		b[i] = charset[rand.IntN(len(charset))]
	}
	return string(b)
}

func genNormalizedEmbeddings(dim int) []float32 {
	fs := faiss.FAISS()
	randVec := make([]float32, dim)
	for i := 0; i < dim; i++ {
		randVec[i] = rand.Float32()
	}
	return fs.NormalizeBatch(randVec, dim)
}

func getWiredTigerConfig() string {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	availableRAM := m.Sys
	ramGB := math.Ceil(float64(availableRAM) / (1024 * 1024 * 1024))

	cachePercent := 45
	cacheSizeGB := int(ramGB * float64(cachePercent) / 100)
	if cacheSizeGB < 1 {
		cacheSizeGB = 1
	}

	return fmt.Sprintf("create,cache_size=%dGB,eviction_trigger=90,eviction_dirty_target=10,eviction_dirty_trigger=30,eviction=(threads_max=8)", cacheSizeGB)
}
