package dbservice

import (
	"achillesdb/pkgs/faiss"
	"achillesdb/pkgs/wiredtiger"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var WIREDTIGER_DIR = "volumes/WT_HOME_TEST"

func TestCreateDb(t *testing.T) {
	wtService := wiredtiger.WiredTiger()

	if _, err := os.Stat(WIREDTIGER_DIR); os.IsNotExist(err) {
		if mkErr := os.MkdirAll(WIREDTIGER_DIR, 0755); mkErr != nil {
			t.Fatalf("failed to create WT_HOME_TEST dir: %v", mkErr)
		}
	}

	if err := wtService.Open(WIREDTIGER_DIR, "create"); err != nil {
		t.Log("Err occured")
	}

	t.Cleanup(func() {
		if err := wtService.Close(); err != nil {
			fmt.Printf("Warning: failed to close connection: %v\n", err)
		}
		os.RemoveAll("volumes/WT_HOME_TEST")
	})

	name := "default"

	params := DbParams{
		Name:      name,
		KvService: wtService,
	}

	dbSvc := DatabaseService(params)

	// Create the db
	dbSvc.CreateDB()

	val, key_exists, err := wtService.GetBinaryWithStringKey(CATALOG, fmt.Sprintf("db:%s", name))

	if !key_exists {
		t.Errorf("DB value not persisted.")
	}

	if err != nil {
		t.Errorf("Error occurred in test: %v", err)
	}

	// Check if value is valid BSON and unmarshal back into struct
	if len(val) == 0 {
		t.Errorf("Returned value was empty ([]byte length == 0)")
	}
	type dbCatalog struct {
		UUID   string            `bson:"_uuid"`
		Name   string            `bson:"name"`
		Config map[string]string `bson:"config"`
	}
	var entry dbCatalog
	if err := bson.Unmarshal(val[:], &entry); err != nil {
		t.Errorf("Returned value was not valid BSON for DbCatalogEntry: %v", err)
	}

	if entry.Name != name {
		t.Errorf("Corrupted or incorrect DB name. Got: %s, want: %s", entry.Name, name)
	}

	if entry.UUID == "" {
		t.Errorf("UUID is missing in returned struct")
	}

	if entry.Config == nil || entry.Config["Index"] != "HNSW" {
		t.Errorf("Config field corrupted or missing expected value: %v", len(entry.Config))
	}

}

func TestCreateCollection(t *testing.T) {
	wtService := wiredtiger.WiredTiger()

	if _, err := os.Stat(WIREDTIGER_DIR); os.IsNotExist(err) {
		if mkErr := os.MkdirAll(WIREDTIGER_DIR, 0755); mkErr != nil {
			t.Fatalf("failed to create WT_HOME_TEST dir: %v", mkErr)
		}
	}

	if err := wtService.Open(WIREDTIGER_DIR, "create"); err != nil {
		t.Log("Err occured")
	}

	dbName := "default"
	collName := "tenant_id_1"

	params := DbParams{
		Name:      dbName,
		KvService: wtService,
	}

	dbSvc := DatabaseService(params)

	// Create the db
	err := dbSvc.CreateDB()

	if err != nil {
		t.Errorf("Failed to create Db; %s", err)
	}

	start := time.Now()
	err = dbSvc.CreateCollection(collName)
	elapsed := time.Since(start)
	t.Logf("CreateCollection took: %v\n", elapsed)

	if err != nil {
		t.Errorf("Failed to create collection: %s", err)
	}

	// Get vector index URI for cleanup
	collectionDefKey := fmt.Sprintf("%s.%s", dbName, collName)
	val, exists, err := wtService.GetBinary(CATALOG, []byte(collectionDefKey))
	var vectorIndexPath string
	if err == nil && exists {
		var catalogEntry CollectionCatalogEntry
		if bson.Unmarshal(val, &catalogEntry) == nil {
			vectorIndexPath = catalogEntry.VectorIndexUri
		}
	}

	t.Cleanup(func() {
		if err := wtService.Close(); err != nil {
			fmt.Printf("Warning: failed to close connection: %v\n", err)
		}
		if vectorIndexPath != "" {
			os.Remove(vectorIndexPath)
		}
		os.RemoveAll("volumes/WT_HOME_TEST")
	})

	fmt.Printf("URI: %s\n", fmt.Sprintf("%s.%s", dbName, collName))

	val, key_exists, err := wtService.GetBinaryWithStringKey(CATALOG, fmt.Sprintf("%s.%s", dbName, collName))

	if !key_exists {
		t.Errorf("DB value not persisted.")
	}

	if err != nil {
		t.Errorf("Error occurred in test: %v", err)
	}

	// Check if value is valid BSON and unmarshal back into struct
	if len(val) == 0 {
		t.Errorf("Returned value was empty ([]byte length == 0)")
	}

	var entry CollectionCatalogEntry
	unmarshalErr := bson.Unmarshal(val[:], &entry)
	if unmarshalErr != nil {
		t.Errorf("Failed to unmarshal BSON value: %v", unmarshalErr)
	}
	if entry.Ns != fmt.Sprintf("%s.%s", dbName, collName) {
		t.Errorf("Unmarshaled CollectionCatalogEntry Ns does not match: got %s, want %s", entry.Ns, fmt.Sprintf("%s.%s", dbName, collName))
	}

}

func TestInsertDocuments(t *testing.T) {
	wtService := wiredtiger.WiredTiger()

	if _, err := os.Stat(WIREDTIGER_DIR); os.IsNotExist(err) {
		if mkErr := os.MkdirAll(WIREDTIGER_DIR, 0755); mkErr != nil {
			t.Fatalf("failed to create WT_HOME_TEST dir: %v", mkErr)
		}
	}

	if err := wtService.Open(WIREDTIGER_DIR, getWiredTigerConfigForConnection()); err != nil {
		t.Log("Err occured")
	}

	dbName := "default"
	collName := "tenant_id_1"

	params := DbParams{
		Name:      dbName,
		KvService: wtService,
	}

	dbSvc := DatabaseService(params)

	// Create the db
	err := dbSvc.CreateDB()
	if err != nil {
		t.Errorf("Failed to create Db; %s", err)
	}

	err = dbSvc.CreateCollection(collName)
	if err != nil {
		t.Errorf("Failed to create collection: %s", err)
	}

	documents := make([]GlowstickDocument, 100)
	for i := 0; i < 100; i++ {
		documents[i] = GlowstickDocument{
			Id:        primitive.NewObjectID(),
			Content:   fmt.Sprintf("Example document %d", i+1),
			Embedding: genEmbeddings(1536),
			Metadata:  map[string]interface{}{"type": "example", "index": i + 1},
		}
	}

	start := time.Now()
	err = dbSvc.InsertDocumentsIntoCollection(collName, documents)
	duration := time.Since(start)
	fmt.Printf("InsertDocumentsIntoCollection took: %v\n", duration)
	if err != nil {
		t.Errorf("InsertDocumentsIntoCollection returned error: %v", err)
	}

	collectionDefKey := fmt.Sprintf("%s.%s", dbName, collName)
	val, exists, err := wtService.GetBinary(CATALOG, []byte(collectionDefKey))
	if err != nil {
		t.Fatalf("failed to get collection catalog entry from _catalog: %v", err)
	}
	if !exists {
		t.Fatalf("catalog entry does not exist for collection '%s'", collectionDefKey)
	}

	var catalogEntry CollectionCatalogEntry
	unmarshalErr := bson.Unmarshal(val, &catalogEntry)
	if unmarshalErr != nil {
		t.Fatalf("Failed to unmarshal catalog entry: %v", unmarshalErr)
	}
	collTableURI := catalogEntry.TableUri
	if collTableURI == "" {
		t.Fatalf("Table URI not set in catalog entry for %s", collName)
	}

	for index, doc := range documents {
		docKey := doc.Id[:]
		fmt.Printf("Index: %d, docKey: %x\n", index, docKey)

		record, found, err := wtService.GetBinary(collTableURI, docKey)
		if err != nil {
			t.Errorf("failed to read doc _id=%s from table %s: %v", doc.Id.Hex(), collTableURI, err)
		}
		if !found {
			t.Errorf("inserted doc _id=%s not found in collection physical table %s", doc.Id.Hex(), collTableURI)
		}

		var restoredDoc GlowstickDocument
		if err := bson.Unmarshal(record, &restoredDoc); err != nil {
			t.Errorf("unmarshal failed for _id=%s: %v", doc.Id.Hex(), err)
		}
		if doc.Content != restoredDoc.Content {
			t.Errorf("Retrieved content does not match document saved. Retrieved:%s, Document:%s", restoredDoc.Content, doc.Content)
		}
	}

	statsVal, statsExists, statsErr := wtService.GetBinary(STATS, []byte(collectionDefKey))
	if statsErr != nil {
		t.Errorf("Failed to retrieve _stats entry for collection %s: %v", collName, statsErr)
	}
	if !statsExists {
		t.Errorf("_stats entry missing for collection %s", collName)
	}
	var hotStats CollectionStats
	if statsErr == nil && statsExists {
		if err := bson.Unmarshal(statsVal, &hotStats); err != nil {
			t.Errorf("Unmarshal failed for hot stats: %v", err)
		}

		if int(hotStats.Doc_Count) != len(documents) {
			t.Logf("hot stats: %f", hotStats.Vector_Index_Size)
			t.Errorf("Stats Doc_Count mismatch, got %d, want %d", hotStats.Doc_Count, len(documents))
		}
	}

	t.Cleanup(func() {
		if err := wtService.Close(); err != nil {
			fmt.Printf("Warning: failed to close connection: %v\n", err)
		}
		if catalogEntry.VectorIndexUri != "" {
			os.Remove(catalogEntry.VectorIndexUri)
		}
		os.RemoveAll("volumes/WT_HOME_TEST")
	})

}

func TestBasicVectorQuery(t *testing.T) {
	wtService := wiredtiger.WiredTiger()

	if _, err := os.Stat(WIREDTIGER_DIR); os.IsNotExist(err) {
		if mkErr := os.MkdirAll(WIREDTIGER_DIR, 0755); mkErr != nil {
			t.Fatalf("failed to create WT_HOME_TEST dir: %v", mkErr)
		}
	}
	if err := wtService.Open(WIREDTIGER_DIR, "create,cache_size=4GB,eviction_trigger=95,eviction_dirty_target=5,eviction_dirty_trigger=15,eviction=(threads_max=8),checkpoint=(wait=300)"); err != nil {
		t.Log("Err occured")
	}

	dbName := "default"
	collName := "tenant_id_1"

	params := DbParams{
		Name:      dbName,
		KvService: wtService,
	}

	dbSvc := DatabaseService(params)

	// Create the db
	err := dbSvc.CreateDB()
	if err != nil {
		t.Errorf("Failed to create Db; %s", err)
	}

	err = dbSvc.CreateCollection(collName)
	if err != nil {
		t.Errorf("Failed to create collection: %s", err)
	}

	content := generateLongText()
	documents := make([]GlowstickDocument, 12000)
	for i := 0; i < 12000; i++ {
		documents[i] = GlowstickDocument{
			Id:        primitive.NewObjectID(),
			Content:   content,
			Embedding: genEmbeddings(1536),
			Metadata:  map[string]interface{}{"type": "example", "index": i + 1},
		}
	}

	err = dbSvc.InsertDocumentsIntoCollection(collName, documents)
	if err != nil {
		t.Errorf("InsertDocumentsIntoCollection returned error: %v", err)
	}

	// Get vector index URI for cleanup
	collectionDefKey := fmt.Sprintf("%s.%s", dbName, collName)
	// val, exists, err := wtService.GetBinary(CATALOG, []byte(collectionDefKey))
	// //var vectorIndexPath string
	// if err == nil && exists {
	// 	var catalogEntry CollectionCatalogEntry
	// 	if bson.Unmarshal(val, &catalogEntry) == nil {
	// 		//vectorIndexPath = catalogEntry.VectorIndexUri
	// 	}
	// }

	t.Cleanup(func() {
		if err := wtService.Close(); err != nil {
			fmt.Printf("Warning: failed to close connection: %v\n", err)
		}
		// if vectorIndexPath != "" {
		// 	os.Remove(vectorIndexPath)
		// }
		//os.RemoveAll("volumes/WT_HOME_TEST")
	})

	topK := 12

	query := QueryStruct{
		TopK:           int32(topK),
		QueryEmbedding: genEmbeddings(1536),
	}

	start := time.Now()
	docs, err := dbSvc.QueryCollection(collName, query)
	duration := time.Since(start)
	t.Logf("QueryCollection took: %v", duration)

	if err != nil {
		t.Errorf("error occured during query %v", err)
	}

	if len(docs) == 0 {
		t.Log("No docs returned")
	}
	t.Logf("Query returned %d documents:\n", len(docs))
	for i, doc := range docs {
		t.Logf("Document %d:\n", i+1)
		t.Logf("  ID: %s\n", doc.Id.Hex())
		t.Logf("  Content: %s\n", doc.Content)
		t.Logf("  Metadata: %+v\n", doc.Metadata)
		t.Logf("  Embedding length: %d\n", len(doc.Embedding))
	}

	if len(docs) != topK {
		t.Error("Returned docs don't match top K")
	}

	hot_stats, _, err := wtService.GetBinary(STATS, []byte(collectionDefKey))
	if err != nil {
		t.Errorf("failed to fetch hot stats: %v", err)
	}

	var hot_stats_doc CollectionStats
	if err := bson.Unmarshal(hot_stats, &hot_stats_doc); err != nil {
		t.Errorf("failed to unmarshal hot stats: %v", err)
	}
	t.Logf("Hot stats doc value: %+v", fmt.Sprintf("%.2f", hot_stats_doc.Vector_Index_Size))

}
func genEmbeddings(dim int) []float32 {
	fs := faiss.FAISS()
	randVec := make([]float32, dim)
	for i := 0; i < dim; i++ {
		randVec[i] = rand.Float32()
	}
	return fs.NormalizeBatch(randVec, dim)
}

func getWiredTigerConfigForConnection() string {
	// Get available RAM and round up to GB
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	availableRAM := m.Sys
	ramGB := math.Ceil(float64(availableRAM) / (1024 * 1024 * 1024))

	// Compute WiredTiger configuration string with cache size based on available RAM
	cachePercent := 45
	cacheSizeGB := int(ramGB * float64(cachePercent) / 100)
	if cacheSizeGB < 1 {
		cacheSizeGB = 1 // Minimum 1GB
	}

	return fmt.Sprintf("create,cache_size=%dGB,eviction_trigger=90,eviction_dirty_target=10,eviction_dirty_trigger=30,eviction=(threads_max=8)", cacheSizeGB)
}
func generateLongText() string {
	words := []string{
		"the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog",
		"artificial", "intelligence", "machine", "learning", "database", "vector",
		"embedding", "search", "query", "document", "collection", "index",
		"algorithm", "neural", "network", "deep", "learning", "natural",
		"language", "processing", "computer", "science", "technology", "data",
		"analysis", "information", "retrieval", "semantic", "similarity",
		"clustering", "classification", "optimization", "performance", "scalability",
	}

	// Generate text with 50-100 words (reduced range)
	numWords := 50 + rand.IntN(51) // 50-100 words instead of potentially huge numbers
	var text strings.Builder
	text.Grow(numWords * 10) // Pre-allocate capacity

	for i := 0; i < numWords; i++ {
		if i > 0 {
			text.WriteString(" ")
		}
		text.WriteString(words[rand.IntN(len(words))])
	}

	return text.String()
}
