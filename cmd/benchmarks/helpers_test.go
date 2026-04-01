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

// setupBenchCollection creates a DB and collection, returning the service ready for operations.
func setupBenchCollection(b *testing.B, tag string, collName string) (wt.WTService, dbservice.DBService, func()) {
	b.Helper()
	wtService, dbSvc, cleanup := setupBenchDB(b, tag)

	if err := dbSvc.CreateDB(); err != nil {
		wtService.Close()
		cleanup()
		b.Fatalf("Failed to create DB: %v", err)
	}
	if err := dbSvc.CreateCollection(collName); err != nil {
		wtService.Close()
		cleanup()
		b.Fatalf("Failed to create collection: %v", err)
	}

	return wtService, dbSvc, cleanup
}

// generateSOA creates test documents in SOA format for benchmarking.
func generateSOA(count, embeddingDim int) *dbservice.GlowstickDocumentSOA {
	soa := &dbservice.GlowstickDocumentSOA{
		Ids:        make([]string, count),
		Contents:   make([]string, count),
		Embeddings: make([]float32, count*embeddingDim),
		Metadatas:  make([]map[string]interface{}, count),
	}

	for i := 0; i < count; i++ {
		soa.Ids[i] = primitive.NewObjectID().Hex()
		soa.Contents[i] = fmt.Sprintf("Benchmark document %d with test content.", i+1)
		soa.Metadatas[i] = map[string]interface{}{"type": "benchmark", "index": i + 1, "category": "test"}

		embedding := genNormalizedEmbeddings(embeddingDim)
		copy(soa.Embeddings[i*embeddingDim:(i+1)*embeddingDim], embedding)
	}

	return soa
}

// generateSOAWithContentSize creates SOA documents with a specific content size per document.
func generateSOAWithContentSize(count, embeddingDim, contentSize int) *dbservice.GlowstickDocumentSOA {
	content := generateRandomContent(contentSize)
	soa := &dbservice.GlowstickDocumentSOA{
		Ids:        make([]string, count),
		Contents:   make([]string, count),
		Embeddings: make([]float32, count*embeddingDim),
		Metadatas:  make([]map[string]interface{}, count),
	}

	for i := 0; i < count; i++ {
		soa.Ids[i] = primitive.NewObjectID().Hex()
		soa.Contents[i] = content
		soa.Metadatas[i] = map[string]interface{}{"type": "benchmark", "index": i + 1, "size": contentSize}

		embedding := genNormalizedEmbeddings(embeddingDim)
		copy(soa.Embeddings[i*embeddingDim:(i+1)*embeddingDim], embedding)
	}

	return soa
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
