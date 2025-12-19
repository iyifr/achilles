package dbservice

import (
	"achillesdb/pkgs/faiss"
	wt "achillesdb/pkgs/wiredtiger"
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

func setupTestDB(t *testing.T, dirName string) (wt.WTService, DBService) {
	wtService := wt.WiredTiger()

	// Ensure unique directory for each test
	timestamp := time.Now().UnixNano()
	testDir := fmt.Sprintf("volumes/WT_HOME_TEST_%s_%d", dirName, timestamp)
	vectorsDir := fmt.Sprintf("volumes/vectors_test_%s_%d", dirName, timestamp)

	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatalf("failed to create test dir: %v", err)
	}

	if err := os.MkdirAll(vectorsDir, 0755); err != nil {
		t.Fatalf("failed to create vectors dir: %v", err)
	}

	// Set VECTORS_HOME for this test
	os.Setenv("VECTORS_HOME", vectorsDir)

	if err := wtService.Open(testDir, getWiredTigerConfigForConnection()); err != nil {
		t.Fatalf("Failed to open WiredTiger: %v", err)
	}

	// Initialize tables
	if err := InitTablesHelper(wtService); err != nil {
		wtService.Close()
		os.RemoveAll(testDir)
		os.RemoveAll(vectorsDir)
		t.Fatalf("Failed to init tables: %v", err)
	}

	t.Cleanup(func() {
		if err := wtService.Close(); err != nil {
			t.Logf("Warning: failed to close connection: %v", err)
		}
		os.RemoveAll(testDir)
		os.RemoveAll(vectorsDir)
	})

	name := "default"
	params := DbParams{
		Name:      name,
		KvService: wtService,
	}

	return wtService, DatabaseService(params)
}

func TestCreateDb(t *testing.T) {
	wtService, dbSvc := setupTestDB(t, "CreateDb")

	// Create the db
	dbSvc.CreateDB()

	val, key_exists, err := wtService.GetBinaryWithStringKey(CATALOG, "db:default")

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

	if entry.Name != "default" {
		t.Errorf("Corrupted or incorrect DB name. Got: %s, want: default", entry.Name)
	}

	if entry.UUID == "" {
		t.Errorf("UUID is missing in returned struct")
	}

	if entry.Config == nil || entry.Config["Index"] != "HNSW" {
		t.Errorf("Config field corrupted or missing expected value: %v", len(entry.Config))
	}
}

func TestCreateCollection(t *testing.T) {
	wtService, dbSvc := setupTestDB(t, "CreateCollection")

	collName := "tenant_id_1"
	dbName := "default"

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

	// Verify catalog entry
	val, key_exists, err := wtService.GetBinaryWithStringKey(CATALOG, fmt.Sprintf("%s.%s", dbName, collName))

	if !key_exists {
		t.Errorf("DB value not persisted.")
	}
	if err != nil {
		t.Errorf("Error occurred in test: %v", err)
	}

	var entry CollectionCatalogEntry
	unmarshalErr := bson.Unmarshal(val[:], &entry)
	if unmarshalErr != nil {
		t.Errorf("Failed to unmarshal BSON value: %v", unmarshalErr)
	}
	if entry.Ns != fmt.Sprintf("%s.%s", dbName, collName) {
		t.Errorf("Unmarshaled CollectionCatalogEntry Ns does not match: got %s, want %s", entry.Ns, fmt.Sprintf("%s.%s", dbName, collName))
	}

	// Clean up vector index file that might have been created
	if entry.VectorIndexUri != "" {
		t.Cleanup(func() {
			os.Remove(entry.VectorIndexUri)
		})
	}
}

func TestInsertDocuments(t *testing.T) {
	wtService, dbSvc := setupTestDB(t, "InsertDocuments")
	collName := "tenant_id_1"
	dbName := "default"

	err := dbSvc.CreateDB()
	if err != nil {
		t.Fatalf("Failed to create Db; %s", err)
	}

	err = dbSvc.CreateCollection(collName)
	if err != nil {
		t.Fatalf("Failed to create collection: %s", err)
	}

	documents := make([]GlowstickDocument, 100)
	for i := 0; i < 100; i++ {
		documents[i] = GlowstickDocument{
			Id:        primitive.NewObjectID().Hex(),
			Content:   fmt.Sprintf("Example document %d", i+1),
			Embedding: genEmbeddings(1536),
			Metadata:  map[string]any{"type": "example", "index": i + 1},
		}
	}

	start := time.Now()
	err = dbSvc.InsertDocuments(collName, documents)
	duration := time.Since(start)
	t.Logf("InsertDocumentsIntoCollection took: %v\n", duration)
	if err != nil {
		t.Fatalf("InsertDocumentsIntoCollection returned error: %v", err)
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
	bson.Unmarshal(val, &catalogEntry)

	// Clean up vector index
	if catalogEntry.VectorIndexUri != "" {
		t.Cleanup(func() {
			os.Remove(catalogEntry.VectorIndexUri)
		})
	}

	collTableURI := catalogEntry.TableUri

	// Verify inserted docs
	for index, doc := range documents {
		docKey := doc.Id[:]
		record, found, err := wtService.GetBinary(collTableURI, []byte(docKey))
		if err != nil {
			t.Errorf("Index %d: failed to read doc _id=%s: %v", index, doc.Id, err)
		}
		if !found {
			t.Errorf("Index %d: inserted doc _id=%s not found", index, doc.Id)
		}

		var restoredDoc GlowstickDocument
		if err := bson.Unmarshal(record, &restoredDoc); err != nil {
			t.Errorf("unmarshal failed for _id=%s: %v", doc.Id, err)
		}
		if doc.Content != restoredDoc.Content {
			t.Errorf("Retrieved content mismatch. Got:%s, Want:%s", restoredDoc.Content, doc.Content)
		}
	}

	// Check Stats
	statsVal, statsExists, statsErr := wtService.GetBinary(STATS, []byte(collectionDefKey))
	if statsErr != nil || !statsExists {
		t.Errorf("Failed to retrieve _stats entry")
	} else {
		var hotStats CollectionStats
		if err := bson.Unmarshal(statsVal, &hotStats); err != nil {
			t.Errorf("Unmarshal failed for hot stats: %v", err)
		}
		if int(hotStats.Doc_Count) != len(documents) {
			t.Errorf("Stats Doc_Count mismatch, got %d, want %d", hotStats.Doc_Count, len(documents))
		}
	}
}

func TestBasicVectorQuery(t *testing.T) {
	_, dbSvc := setupTestDB(t, "BasicVectorQuery")
	collName := "tenant_id_1"

	dbSvc.CreateDB()
	dbSvc.CreateCollection(collName)

	content := generateLongText()
	documents := make([]GlowstickDocument, 100)
	for i := 0; i < 100; i++ {
		documents[i] = GlowstickDocument{
			Id:        primitive.NewObjectID().Hex(),
			Content:   content,
			Embedding: genEmbeddings(1536),
			Metadata:  map[string]any{"type": "example", "index": i + 1},
		}
	}

	if err := dbSvc.InsertDocuments(collName, documents); err != nil {
		t.Fatalf("InsertDocumentsIntoCollection returned error: %v", err)
	}

	t.Cleanup(func() {
		os.Remove(collName + ".index")
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
		t.Fatalf("error occured during query %v", err)
	}

	if len(docs) != topK {
		t.Errorf("Returned docs count %d, want %d", len(docs), topK)
	}
}

func TestListCollections(t *testing.T) {
	_, dbSvc := setupTestDB(t, "ListCollections")

	err := dbSvc.CreateDB()
	if err != nil {
		t.Errorf("Failed to create Db; %s", err)
	}

	colls := []string{"tenant_id_1", "tenant_id_2", "tenant_id_3"}
	for _, coll := range colls {
		err = dbSvc.CreateCollection(coll)
		if err != nil {
			t.Errorf("Failed to create collection: %s", err)
		}
	}

	// Cleanup index files
	t.Cleanup(func() {
		for _, coll := range colls {
			os.Remove(coll + ".index")
		}
	})

	collections, err := dbSvc.ListCollections()

	if err != nil {
		t.Fatalf("Failed to list collections: %v", err)
	}

	if len(collections) == 0 {
		t.Fatalf("No collections found")
	}

	// Just check if we found some
	if len(collections) != 3 {
		t.Errorf("Expected 3 collections, got %d", len(collections))
	}

	// Check content of first one
	firstColl := collections[0]
	// Order is not guaranteed in list, check if it exists in our input list
	found := false
	for _, c := range colls {
		if firstColl.Ns == fmt.Sprintf("default.%s", c) {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("First collection NS %s not found in expected list", firstColl.Ns)
	}
}

func TestMetadataFiltering(t *testing.T) {
	_, dbSvc := setupTestDB(t, "MetadataFiltering")
	collName := "filter_test_collection"

	dbSvc.CreateDB()
	dbSvc.CreateCollection(collName)

	t.Cleanup(func() {
		os.Remove(collName + ".index")
	})

	// Insert docs with diverse metadata
	documents := []GlowstickDocument{
		{
			Id:        "doc1",
			Content:   "Doc 1",
			Embedding: genEmbeddings(1536),
			Metadata:  map[string]any{"age": 25, "city": "NY", "tags": []string{"tech", "music"}},
		},
		{
			Id:        "doc2",
			Content:   "Doc 2",
			Embedding: genEmbeddings(1536),
			Metadata:  map[string]any{"age": 30, "city": "SF", "tags": []string{"tech", "food"}},
		},
		{
			Id:        "doc3",
			Content:   "Doc 3",
			Embedding: genEmbeddings(1536),
			Metadata:  map[string]any{"age": 35, "city": "NY", "tags": []string{"music", "art"}},
		},
		{
			Id:        "doc4",
			Content:   "Doc 4",
			Embedding: genEmbeddings(1536),
			Metadata:  map[string]any{"age": 40, "city": "SF", "tags": []string{"food"}},
		},
	}

	if err := dbSvc.InsertDocuments(collName, documents); err != nil {
		t.Fatalf("Failed to insert docs: %v", err)
	}

	// Helper to run query and check IDs
	runQuery := func(name string, filters map[string]any, expectedIDs []string) {
		t.Helper()
		// Using t.Run to isolate failures better
		t.Run(name, func(t *testing.T) {
			query := QueryStruct{
				TopK:           10,
				QueryEmbedding: genEmbeddings(1536),
				Filters:        filters,
			}
			docs, err := dbSvc.QueryCollection(collName, query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			var gotIDs []string
			for _, d := range docs {
				gotIDs = append(gotIDs, d.Id)
			}

			if len(gotIDs) != len(expectedIDs) {
				t.Errorf("Count mismatch. Got %d docs, want %d. Got IDs: %v, Want IDs: %v", len(gotIDs), len(expectedIDs), gotIDs, expectedIDs)
				return
			}

			// Simple set check
			expectedSet := make(map[string]bool)
			for _, id := range expectedIDs {
				expectedSet[id] = true
			}
			for _, id := range gotIDs {
				if !expectedSet[id] {
					t.Errorf("Unexpected doc ID found: %s", id)
				}
			}
		})
	}

	// 1. Simple Equality
	runQuery("Simple Eq", map[string]any{"city": "NY"}, []string{"doc1", "doc3"})

	// 2. $gt Operator
	runQuery("$gt Check", map[string]any{"age": map[string]any{"$gt": 30}}, []string{"doc3", "doc4"})

	// 3. $lte Operator
	runQuery("$lte Check", map[string]any{"age": map[string]any{"$lte": 30}}, []string{"doc1", "doc2"})

	// 4. $in Operator
	runQuery("$in Check", map[string]any{"city": map[string]any{"$in": []any{"NY", "Paris"}}}, []string{"doc1", "doc3"})

	// 5. $and Operator
	runQuery("$and Check", map[string]any{
		"$and": []any{
			map[string]any{"city": "SF"},
			map[string]any{"age": map[string]any{"$gt": 30}},
		},
	}, []string{"doc4"})

	// 6. $or Operator
	runQuery("$or Check", map[string]any{
		"$or": []any{
			map[string]any{"age": 25},
			map[string]any{"age": 40},
		},
	}, []string{"doc1", "doc4"})

	// 7. Nested Logic ( $and with $or )
	// (city=SF) AND (age < 35 OR age > 45) -> doc2 (age 30, city SF)
	runQuery("Nested Logic", map[string]any{
		"$and": []any{
			map[string]any{"city": "SF"},
			map[string]any{
				"$or": []any{
					map[string]any{"age": map[string]any{"$lt": 35}},
					map[string]any{"age": map[string]any{"$gt": 45}},
				},
			},
		},
	}, []string{"doc2"})
}

// Helpers

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

	numWords := 50 + rand.IntN(51)
	var text strings.Builder
	text.Grow(numWords * 10)

	for i := 0; i < numWords; i++ {
		if i > 0 {
			text.WriteString(" ")
		}
		text.WriteString(words[rand.IntN(len(words))])
	}

	return text.String()
}
