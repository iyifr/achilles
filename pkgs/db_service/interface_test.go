package dbservice

import (
	"achillesdb/pkgs/faiss"
	wt "achillesdb/pkgs/wiredtiger"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"runtime"
	"strconv"
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
	if err := InitTables(wtService); err != nil {
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
	err = dbSvc.InsertDocuments(collName, NewGlowstickDocumentSOA(documents))
	duration := time.Since(start)
	t.Logf("InsertDocuments took: %v\n", duration)
	if err != nil {
		t.Fatalf("InsertDocuments returned error: %v", err)
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

	// Verify inserted docs. The document table is keyed by internal id, not
	// the caller-given string id, so resolve through the alias table first.
	for index, doc := range documents {
		aliasVal, aliasFound, err := wtService.GetString(DOC_ID_ALIAS_TABLE_URI, doc.Id)
		if err != nil || !aliasFound {
			t.Errorf("Index %d: failed to resolve alias for _id=%s: %v", index, doc.Id, err)
			continue
		}
		internalId, err := strconv.ParseInt(aliasVal, 10, 64)
		if err != nil {
			t.Errorf("Index %d: corrupt alias for _id=%s: %v", index, doc.Id, err)
			continue
		}

		record, found, err := wtService.GetBinary(collTableURI, encodeInternalId(internalId))
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

func TestInsertDocumentsSOA(t *testing.T) {
	wtService, dbSvc := setupTestDB(t, "InsertDocuments_SOA")
	collName := "test_soa"
	dbName := "default"

	err := dbSvc.CreateDB()
	if err != nil {
		t.Fatalf("Failed to create DB: %s", err)
	}

	err = dbSvc.CreateCollection(collName)
	if err != nil {
		t.Fatalf("Failed to create collection: %s", err)
	}

	// Generate test data in SOA format
	numDocs := 100
	embeddingDim := 1536

	soa := &GlowstickDocumentSOA{
		Ids:        make([]string, numDocs),
		Contents:   make([]string, numDocs),
		Embeddings: make([]float32, numDocs*embeddingDim),
		Metadatas:  make([]map[string]interface{}, numDocs),
	}

	for i := 0; i < numDocs; i++ {
		soa.Ids[i] = primitive.NewObjectID().Hex()
		soa.Contents[i] = fmt.Sprintf("SOA test document %d", i+1)
		soa.Metadatas[i] = map[string]interface{}{"type": "soa_test", "index": i + 1}

		// Generate and copy embeddings
		embedding := genEmbeddings(embeddingDim)
		copy(soa.Embeddings[i*embeddingDim:(i+1)*embeddingDim], embedding)
	}

	start := time.Now()
	err = dbSvc.InsertDocuments(collName, soa)
	duration := time.Since(start)
	t.Logf("InsertDocuments took: %v", duration)

	if err != nil {
		t.Fatalf("InsertDocuments returned error: %v", err)
	}

	// Verify documents were inserted correctly
	collectionDefKey := fmt.Sprintf("%s.%s", dbName, collName)
	val, exists, err := wtService.GetBinary(CATALOG, []byte(collectionDefKey))
	if err != nil {
		t.Fatalf("failed to get collection catalog: %v", err)
	}
	if !exists {
		t.Fatalf("catalog entry does not exist")
	}

	var catalogEntry CollectionCatalogEntry
	if err := bson.Unmarshal(val, &catalogEntry); err != nil {
		t.Fatalf("failed to unmarshal catalog entry: %v", err)
	}

	if catalogEntry.VectorIndexUri != "" {
		t.Cleanup(func() {
			os.Remove(catalogEntry.VectorIndexUri)
		})
	}

	// Verify each document. The document table is keyed by internal id, not
	// the caller-given string id, so resolve through the alias table first.
	collTableURI := catalogEntry.TableUri
	for i := 0; i < numDocs; i++ {
		docKey := soa.Ids[i]
		aliasVal, aliasFound, err := wtService.GetString(DOC_ID_ALIAS_TABLE_URI, docKey)
		if err != nil || !aliasFound {
			t.Errorf("Index %d: failed to resolve alias for _id=%s: %v", i, docKey, err)
			continue
		}
		internalId, err := strconv.ParseInt(aliasVal, 10, 64)
		if err != nil {
			t.Errorf("Index %d: corrupt alias for _id=%s: %v", i, docKey, err)
			continue
		}

		record, found, err := wtService.GetBinary(collTableURI, encodeInternalId(internalId))
		if err != nil {
			t.Errorf("Index %d: failed to read doc _id=%s: %v", i, docKey, err)
		}
		if !found {
			t.Errorf("Index %d: inserted doc _id=%s not found", i, docKey)
		}

		var restoredDoc GlowstickDocument
		if err := bson.Unmarshal(record, &restoredDoc); err != nil {
			t.Errorf("unmarshal failed for _id=%s: %v", docKey, err)
		}
		if soa.Contents[i] != restoredDoc.Content {
			t.Errorf("Content mismatch. Got:%s, Want:%s", restoredDoc.Content, soa.Contents[i])
		}
	}

	// Verify stats
	statsVal, statsExists, statsErr := wtService.GetBinary(STATS, []byte(collectionDefKey))
	if statsErr != nil || !statsExists {
		t.Errorf("Failed to retrieve _stats entry")
	} else {
		var hotStats CollectionStats
		if err := bson.Unmarshal(statsVal, &hotStats); err != nil {
			t.Errorf("Unmarshal failed for hot stats: %v", err)
		}
		if hotStats.Doc_Count != numDocs {
			t.Errorf("Stats doc count mismatch. Got:%d, Want:%d", hotStats.Doc_Count, numDocs)
		}
	}
}

func TestInsertDocuments_ValidationErrors(t *testing.T) {
	_, dbSvc := setupTestDB(t, "SOA_Validation")
	collName := "test_validation"

	err := dbSvc.CreateDB()
	if err != nil {
		t.Fatalf("Failed to create DB: %s", err)
	}

	err = dbSvc.CreateCollection(collName)
	if err != nil {
		t.Fatalf("Failed to create collection: %s", err)
	}

	testCases := []struct {
		name      string
		soa       *GlowstickDocumentSOA
		expectErr string
	}{
		{
			name: "empty_ids",
			soa: &GlowstickDocumentSOA{
				Ids:        []string{},
				Contents:   []string{},
				Embeddings: []float32{},
				Metadatas:  []map[string]interface{}{},
			},
			expectErr: "ids array cannot be empty",
		},
		{
			name: "length_mismatch_contents",
			soa: &GlowstickDocumentSOA{
				Ids:        []string{"id1", "id2"},
				Contents:   []string{"content1"},
				Embeddings: make([]float32, 2*128),
				Metadatas:  []map[string]interface{}{{}, {}},
			},
			expectErr: "length mismatch",
		},
		{
			name: "wrong_embedding_length",
			soa: &GlowstickDocumentSOA{
				Ids:        []string{"id1", "id2"},
				Contents:   []string{"content1", "content2"},
				Embeddings: make([]float32, 2*128+1), // Wrong size
				Metadatas:  []map[string]interface{}{{}, {}},
			},
			expectErr: "embeddings length",
		},
		{
			name: "empty_embeddings",
			soa: &GlowstickDocumentSOA{
				Ids:        []string{"id1"},
				Contents:   []string{"content1"},
				Embeddings: []float32{},
				Metadatas:  []map[string]interface{}{{}},
			},
			expectErr: "embeddings array cannot be empty",
		},
		{
			name: "duplicate_ids_in_batch",
			soa: &GlowstickDocumentSOA{
				Ids:        []string{"dup1", "dup1"},
				Contents:   []string{"content1", "content2"},
				Embeddings: make([]float32, 2*128),
				Metadatas:  []map[string]interface{}{{}, {}},
			},
			expectErr: "duplicate ids",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := dbSvc.InsertDocuments(collName, tc.soa)
			if err == nil {
				t.Errorf("expected error containing %q, got nil", tc.expectErr)
			} else if !strings.Contains(err.Error(), tc.expectErr) {
				t.Errorf("expected error containing %q, got: %v", tc.expectErr, err)
			}
		})
	}
}

func TestInsertDocuments_RejectsExistingId(t *testing.T) {
	_, dbSvc := setupTestDB(t, "InsertDocuments_ExistingId")
	collName := "test_existing_id"

	if err := dbSvc.CreateDB(); err != nil {
		t.Fatalf("Failed to create DB: %s", err)
	}
	if err := dbSvc.CreateCollection(collName); err != nil {
		t.Fatalf("Failed to create collection: %s", err)
	}

	first := &GlowstickDocumentSOA{
		Ids:        []string{"shared-id"},
		Contents:   []string{"original content"},
		Embeddings: genEmbeddings(128),
		Metadatas:  []map[string]interface{}{{}},
	}
	if err := dbSvc.InsertDocuments(collName, first); err != nil {
		t.Fatalf("first insert failed: %v", err)
	}

	second := &GlowstickDocumentSOA{
		Ids:        []string{"shared-id"},
		Contents:   []string{"overwrite attempt"},
		Embeddings: genEmbeddings(128),
		Metadatas:  []map[string]interface{}{{}},
	}
	err := dbSvc.InsertDocuments(collName, second)
	if err == nil {
		t.Fatal("expected error re-inserting an existing id, got nil")
	}
	if !strings.Contains(err.Error(), "already exist") {
		t.Errorf("expected 'already exist' error, got: %v", err)
	}
}

func TestInsertDocuments_Upsert(t *testing.T) {
	_, dbSvc := setupTestDB(t, "InsertDocuments_Upsert")
	collName := "test_upsert"
	dim := 128

	if err := dbSvc.CreateDB(); err != nil {
		t.Fatalf("Failed to create DB: %s", err)
	}
	if err := dbSvc.CreateCollection(collName); err != nil {
		t.Fatalf("Failed to create collection: %s", err)
	}

	originalEmbedding := genEmbeddings(dim)
	first := &GlowstickDocumentSOA{
		Ids:        []string{"shared-id"},
		Contents:   []string{"original content"},
		Embeddings: originalEmbedding,
		Metadatas:  []map[string]interface{}{{"v": 1}},
	}
	if err := dbSvc.InsertDocuments(collName, first); err != nil {
		t.Fatalf("first insert failed: %v", err)
	}

	updatedEmbedding := genEmbeddings(dim)
	second := &GlowstickDocumentSOA{
		Ids:        []string{"shared-id"},
		Contents:   []string{"updated content"},
		Embeddings: updatedEmbedding,
		Metadatas:  []map[string]interface{}{{"v": 2}},
		Upsert:     true,
	}
	if err := dbSvc.InsertDocuments(collName, second); err != nil {
		t.Fatalf("upsert insert failed: %v", err)
	}

	// doc_count must not double-count an upsert.
	collEntry, err := dbSvc.GetCollection(collName)
	if err != nil {
		t.Fatalf("GetCollection failed: %v", err)
	}
	if collEntry.Stats.Doc_Count != 1 {
		t.Errorf("expected doc_count=1 after upsert, got %d", collEntry.Stats.Doc_Count)
	}

	// The vector must have been replaced, not appended: NTotal stays 1.
	indexCache := faiss.GlobalIndexCache()
	cachedIdx, err := indexCache.GetOrCreate(collEntry.Info.VectorIndexUri, dim)
	if err != nil {
		t.Fatalf("failed to get vector index: %v", err)
	}
	ntotal, err := cachedIdx.Index.NTotal()
	if err != nil {
		t.Fatalf("NTotal failed: %v", err)
	}
	if ntotal != 1 {
		t.Errorf("expected NTotal=1 after upsert, got %d", ntotal)
	}

	// Querying with the new embedding must return exactly one result with
	// the updated content -- not two (old + new vector both resolving to
	// the live, overwritten document row).
	docs, err := dbSvc.QueryCollection(collName, QueryStruct{
		TopK:           5,
		QueryEmbedding: updatedEmbedding,
	})
	if err != nil {
		t.Fatalf("QueryCollection failed: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("expected exactly 1 query result after upsert, got %d: %+v", len(docs), docs)
	}
	if docs[0].Content != "updated content" {
		t.Errorf("expected updated content, got %q", docs[0].Content)
	}
	if fmt.Sprint(docs[0].Metadata["v"]) != "2" {
		t.Errorf("expected updated metadata v=2, got %v", docs[0].Metadata["v"])
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

	if err := dbSvc.InsertDocuments(collName, NewGlowstickDocumentSOA(documents)); err != nil {
		t.Fatalf("InsertDocuments returned error: %v", err)
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

	if err := dbSvc.InsertDocuments(collName, NewGlowstickDocumentSOA(documents)); err != nil {
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

func TestUpdateDocumentsSingleAndBulk(t *testing.T) {
	_, dbSvc := setupTestDB(t, "UpdateDocuments")
	collName := "upd_coll"

	if err := dbSvc.CreateDB(); err != nil {
		t.Fatalf("CreateDB: %v", err)
	}
	if err := dbSvc.CreateCollection(collName); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	t.Cleanup(func() {
		os.Remove(collName + ".index")
	})

	documents := []GlowstickDocument{
		{Id: "doc1", Content: "a", Embedding: genEmbeddings(1536), Metadata: map[string]any{"city": "NY", "n": 1}},
		{Id: "doc2", Content: "b", Embedding: genEmbeddings(1536), Metadata: map[string]any{"city": "SF", "n": 2}},
		{Id: "doc3", Content: "c", Embedding: genEmbeddings(1536), Metadata: map[string]any{"city": "NY", "n": 3}},
	}
	if err := dbSvc.InsertDocuments(collName, NewGlowstickDocumentSOA(documents)); err != nil {
		t.Fatalf("InsertDocuments: %v", err)
	}

	n, err := dbSvc.UpdateDocuments(collName, &DocUpdatePayload{
		DocumentId: "doc1",
		Updates:    map[string]any{"flag": true},
	})
	if err != nil || n != 1 {
		t.Fatalf("single update: got n=%d err=%v", n, err)
	}

	n, err = dbSvc.UpdateDocuments(collName, &DocUpdatePayload{
		Where:   map[string]any{"city": "NY"},
		Updates: map[string]any{"bulk": true},
	})
	if err != nil || n != 2 {
		t.Fatalf("bulk update: got n=%d err=%v", n, err)
	}

	docs, err := dbSvc.GetDocuments(collName)
	if err != nil {
		t.Fatalf("GetDocuments: %v", err)
	}
	for _, d := range docs {
		switch d.Id {
		case "doc1":
			if d.Metadata["bulk"] != true || d.Metadata["flag"] != true {
				t.Errorf("doc1 metadata: %#v", d.Metadata)
			}
		case "doc3":
			if d.Metadata["bulk"] != true {
				t.Errorf("doc3 metadata: %#v", d.Metadata)
			}
		case "doc2":
			if _, ok := d.Metadata["bulk"]; ok {
				t.Errorf("doc2 should not have bulk metadata")
			}
		}
	}

	_, err = dbSvc.UpdateDocuments(collName, &DocUpdatePayload{
		DocumentId: "doc1",
		Where:      map[string]any{"city": "NY"},
		Updates:    map[string]any{"x": 1},
	})
	var dbErr *DBError
	if !errors.As(err, &dbErr) || dbErr.Code != ErrCodeInvalidInput {
		t.Fatalf("expected invalid input for id+where, got %v", err)
	}

	_, err = dbSvc.UpdateDocuments(collName, &DocUpdatePayload{
		Updates: map[string]any{"only": "updates"},
	})
	if !errors.As(err, &dbErr) || dbErr.Code != ErrCodeInvalidInput {
		t.Fatalf("expected invalid input for missing id and where, got %v", err)
	}
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
