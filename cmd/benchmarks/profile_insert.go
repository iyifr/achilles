// +build ignore

// profile_insert.go - Detailed pprof profiling for InsertDocuments
//
// Usage:
//   go run profile_insert.go [flags]
//
// Flags:
//   -docs      Number of documents to insert (default: 100)
//   -dim       Embedding dimension (default: 1536)
//   -iterations Number of insert iterations (default: 5)
//   -cpuprofile Write CPU profile to file (default: cpu.prof)
//   -memprofile Write memory profile to file (default: mem.prof)
//   -blockprofile Write block profile to file (default: "")
//   -mutexprofile Write mutex profile to file (default: "")
//
// After running, analyze with:
//   go tool pprof cpu.prof
//   go tool pprof mem.prof
//
// Common pprof commands:
//   top10        - Show top 10 functions by time/memory
//   list funcName - Show annotated source for function
//   web          - Open interactive graph in browser
//   pdf          - Generate PDF report

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"time"

	dbservice "achillesdb/pkgs/db_service"
	"achillesdb/pkgs/faiss"
	wt "achillesdb/pkgs/wiredtiger"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

var (
	numDocs      = flag.Int("docs", 100, "Number of documents to insert per iteration")
	embDim       = flag.Int("dim", 1536, "Embedding dimension")
	iterations   = flag.Int("iterations", 5, "Number of insert iterations")
	cpuProfile   = flag.String("cpuprofile", "cpu.prof", "Write CPU profile to file")
	memProfile   = flag.String("memprofile", "mem.prof", "Write memory profile to file")
	blockProfile = flag.String("blockprofile", "", "Write block profile to file (empty=disabled)")
	mutexProfile = flag.String("mutexprofile", "", "Write mutex profile to file (empty=disabled)")
)

func main() {
	flag.Parse()

	log.Printf("InsertDocuments Profiler")
	log.Printf("========================")
	log.Printf("Documents per iteration: %d", *numDocs)
	log.Printf("Embedding dimension: %d", *embDim)
	log.Printf("Iterations: %d", *iterations)
	log.Printf("")

	// Enable block profiling if requested
	if *blockProfile != "" {
		runtime.SetBlockProfileRate(1)
	}

	// Enable mutex profiling if requested
	if *mutexProfile != "" {
		runtime.SetMutexProfileFraction(1)
	}

	// Setup database
	wtService, dbSvc, cleanup := setupProfileDB()
	defer cleanup()
	defer wtService.Close()

	collName := "profile_collection"

	if err := dbSvc.CreateDB(); err != nil {
		log.Fatalf("Failed to create DB: %v", err)
	}
	if err := dbSvc.CreateCollection(collName); err != nil {
		log.Fatalf("Failed to create collection: %v", err)
	}

	// Pre-generate all documents
	log.Printf("Generating %d documents with %d-dimensional embeddings...", *numDocs**iterations, *embDim)
	allDocs := make([][]dbservice.GlowstickDocument, *iterations)
	for i := 0; i < *iterations; i++ {
		allDocs[i] = generateDocs(*numDocs, *embDim)
	}
	log.Printf("Document generation complete")
	log.Printf("")

	// Force GC before profiling
	runtime.GC()

	// Start CPU profiling
	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatalf("Could not create CPU profile: %v", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatalf("Could not start CPU profile: %v", err)
		}
		defer pprof.StopCPUProfile()
		log.Printf("CPU profiling enabled -> %s", *cpuProfile)
	}

	// Run the profiled operations
	log.Printf("Starting InsertDocuments profiling...")
	var totalDuration time.Duration
	var durations []time.Duration

	for i := 0; i < *iterations; i++ {
		start := time.Now()
		err := dbSvc.InsertDocuments(collName, allDocs[i])
		elapsed := time.Since(start)

		if err != nil {
			log.Fatalf("InsertDocuments failed on iteration %d: %v", i+1, err)
		}

		durations = append(durations, elapsed)
		totalDuration += elapsed
		log.Printf("  Iteration %d: %v (%d docs/sec)", i+1, elapsed, int(float64(*numDocs)/elapsed.Seconds()))
	}

	log.Printf("")
	log.Printf("Results:")
	log.Printf("  Total documents: %d", *numDocs**iterations)
	log.Printf("  Total time: %v", totalDuration)
	log.Printf("  Avg per iteration: %v", totalDuration/time.Duration(*iterations))
	log.Printf("  Avg docs/sec: %.2f", float64(*numDocs**iterations)/totalDuration.Seconds())

	// Write memory profile
	if *memProfile != "" {
		f, err := os.Create(*memProfile)
		if err != nil {
			log.Fatalf("Could not create memory profile: %v", err)
		}
		defer f.Close()
		runtime.GC() // Get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatalf("Could not write memory profile: %v", err)
		}
		log.Printf("Memory profile written -> %s", *memProfile)
	}

	// Write block profile
	if *blockProfile != "" {
		f, err := os.Create(*blockProfile)
		if err != nil {
			log.Fatalf("Could not create block profile: %v", err)
		}
		defer f.Close()
		if err := pprof.Lookup("block").WriteTo(f, 0); err != nil {
			log.Fatalf("Could not write block profile: %v", err)
		}
		log.Printf("Block profile written -> %s", *blockProfile)
	}

	// Write mutex profile
	if *mutexProfile != "" {
		f, err := os.Create(*mutexProfile)
		if err != nil {
			log.Fatalf("Could not create mutex profile: %v", err)
		}
		defer f.Close()
		if err := pprof.Lookup("mutex").WriteTo(f, 0); err != nil {
			log.Fatalf("Could not write mutex profile: %v", err)
		}
		log.Printf("Mutex profile written -> %s", *mutexProfile)
	}

	log.Printf("")
	log.Printf("Analyze profiles with:")
	if *cpuProfile != "" {
		log.Printf("  go tool pprof %s", *cpuProfile)
	}
	if *memProfile != "" {
		log.Printf("  go tool pprof %s", *memProfile)
	}
}

func setupProfileDB() (wt.WTService, dbservice.DBService, func()) {
	wtService := wt.WiredTiger()

	timestamp := time.Now().UnixNano()
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("glowstick_profile_%d", timestamp))
	vectorsDir := filepath.Join(os.TempDir(), fmt.Sprintf("glowstick_profile_vectors_%d", timestamp))

	if err := os.MkdirAll(testDir, 0755); err != nil {
		log.Fatalf("failed to create test dir: %v", err)
	}
	if err := os.MkdirAll(vectorsDir, 0755); err != nil {
		log.Fatalf("failed to create vectors dir: %v", err)
	}

	os.Setenv("VECTORS_HOME", vectorsDir)

	config := "create,cache_size=1GB,eviction_trigger=90,eviction_dirty_target=10,eviction_dirty_trigger=30,eviction=(threads_max=8)"
	if err := wtService.Open(testDir, config); err != nil {
		log.Fatalf("Failed to open WiredTiger: %v", err)
	}

	if err := dbservice.InitTablesHelper(wtService); err != nil {
		wtService.Close()
		os.RemoveAll(testDir)
		os.RemoveAll(vectorsDir)
		log.Fatalf("Failed to init tables: %v", err)
	}

	params := dbservice.DbParams{
		Name:      "profiledb",
		KvService: wtService,
	}

	cleanup := func() {
		os.RemoveAll(testDir)
		os.RemoveAll(vectorsDir)
	}

	return wtService, dbservice.DatabaseService(params), cleanup
}

func generateDocs(count, dim int) []dbservice.GlowstickDocument {
	fs := faiss.FAISS()
	docs := make([]dbservice.GlowstickDocument, count)

	for i := 0; i < count; i++ {
		embedding := make([]float32, dim)
		for j := 0; j < dim; j++ {
			embedding[j] = float32(i*dim+j) / float32(count*dim)
		}
		embedding = fs.NormalizeBatch(embedding, dim)

		docs[i] = dbservice.GlowstickDocument{
			Id:        primitive.NewObjectID().Hex(),
			Content:   fmt.Sprintf("Profile document %d with test content for performance analysis.", i+1),
			Embedding: embedding,
			Metadata:  map[string]any{"type": "profile", "index": i + 1},
		}
	}
	return docs
}
