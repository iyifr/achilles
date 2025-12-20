package dbservice

import (
	"fmt"
	"os"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestDeleteDB(t *testing.T) {
	wtService, dbSvc := setupTestDB(t, "DeleteDB")

	// Create the db
	err := dbSvc.CreateDB()
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// Verify DB exists
	_, exists, _ := wtService.GetBinaryWithStringKey(CATALOG, "db:default")
	if !exists {
		t.Fatalf("DB should exist before deletion")
	}

	// Create a collection in this DB
	collName := "test_collection"
	err = dbSvc.CreateCollection(collName)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Insert some documents
	documents := []GlowstickDocument{
		{
			Id:        primitive.NewObjectID().Hex(),
			Content:   "Test document 1",
			Embedding: genEmbeddings(128),
			Metadata:  map[string]any{"type": "test"},
		},
		{
			Id:        primitive.NewObjectID().Hex(),
			Content:   "Test document 2",
			Embedding: genEmbeddings(128),
			Metadata:  map[string]any{"type": "test"},
		},
	}

	err = dbSvc.InsertDocuments(collName, documents)
	if err != nil {
		t.Fatalf("Failed to insert documents: %v", err)
	}

	// Delete the database
	err = dbSvc.DeleteDB("default")
	if err != nil {
		t.Fatalf("DeleteDB failed: %v", err)
	}

	// Verify DB no longer exists in catalog
	_, exists, _ = wtService.GetBinaryWithStringKey(CATALOG, "db:default")
	if exists {
		t.Errorf("DB should not exist after deletion")
	}

	// Verify collection no longer exists
	collectionDefKey := fmt.Sprintf("default.%s", collName)
	_, collExists, _ := wtService.GetBinaryWithStringKey(CATALOG, collectionDefKey)
	if collExists {
		t.Errorf("Collection should not exist after DB deletion")
	}

	// Verify stats entry was cleaned up
	_, statsExists, _ := wtService.GetBinaryWithStringKey(STATS, collectionDefKey)
	if statsExists {
		t.Errorf("Stats entry should not exist after DB deletion")
	}
}

func TestDeleteDB_NotFound(t *testing.T) {
	_, dbSvc := setupTestDB(t, "DeleteDBNotFound")

	// Try to delete a non-existent database
	err := dbSvc.DeleteDB("nonexistent")
	if err == nil {
		t.Errorf("DeleteDB should return error for non-existent database")
	}

	// Check that it's a NotFound error
	dbErr, ok := err.(*DBError)
	if !ok {
		t.Errorf("Expected DBError, got %T", err)
	} else if dbErr.Code != ErrCodeNotFound {
		t.Errorf("Expected NotFound error code, got %v", dbErr.Code)
	}
}

func TestDeleteCollection(t *testing.T) {
	wtService, dbSvc := setupTestDB(t, "DeleteCollection")

	// Create DB and collection
	err := dbSvc.CreateDB()
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	collName := "test_collection"
	err = dbSvc.CreateCollection(collName)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Insert some documents
	documents := []GlowstickDocument{
		{
			Id:        primitive.NewObjectID().Hex(),
			Content:   "Test document 1",
			Embedding: genEmbeddings(128),
			Metadata:  map[string]any{"type": "test"},
		},
	}

	err = dbSvc.InsertDocuments(collName, documents)
	if err != nil {
		t.Fatalf("Failed to insert documents: %v", err)
	}

	// Verify collection exists
	collectionDefKey := fmt.Sprintf("default.%s", collName)
	_, exists, _ := wtService.GetBinaryWithStringKey(CATALOG, collectionDefKey)
	if !exists {
		t.Fatalf("Collection should exist before deletion")
	}

	// Delete the collection
	err = dbSvc.DeleteCollection(collName)
	if err != nil {
		t.Fatalf("DeleteCollection failed: %v", err)
	}

	// Verify collection no longer exists in catalog
	_, exists, _ = wtService.GetBinaryWithStringKey(CATALOG, collectionDefKey)
	if exists {
		t.Errorf("Collection should not exist in catalog after deletion")
	}

	// Verify stats entry was cleaned up
	_, statsExists, _ := wtService.GetBinaryWithStringKey(STATS, collectionDefKey)
	if statsExists {
		t.Errorf("Stats entry should not exist after collection deletion")
	}

	// Verify vector index file was deleted
	vectorsDir := os.Getenv("VECTORS_HOME")
	vectorIndexPath := fmt.Sprintf("%s/%s.index", vectorsDir, collName)
	if _, err := os.Stat(vectorIndexPath); !os.IsNotExist(err) {
		t.Errorf("Vector index file should be deleted")
	}
}

func TestDeleteCollection_NotFound(t *testing.T) {
	_, dbSvc := setupTestDB(t, "DeleteCollectionNotFound")

	// Create DB first
	err := dbSvc.CreateDB()
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// Try to delete a non-existent collection
	err = dbSvc.DeleteCollection("nonexistent")
	if err == nil {
		t.Errorf("DeleteCollection should return error for non-existent collection")
	}

	// Check that it's a NotFound error
	dbErr, ok := err.(*DBError)
	if !ok {
		t.Errorf("Expected DBError, got %T", err)
	} else if dbErr.Code != ErrCodeNotFound {
		t.Errorf("Expected NotFound error code, got %v", dbErr.Code)
	}
}

func TestDeleteCollection_EmptyName(t *testing.T) {
	_, dbSvc := setupTestDB(t, "DeleteCollectionEmpty")

	err := dbSvc.DeleteCollection("")
	if err == nil {
		t.Errorf("DeleteCollection should return error for empty name")
	}

	dbErr, ok := err.(*DBError)
	if !ok {
		t.Errorf("Expected DBError, got %T", err)
	} else if dbErr.Code != ErrCodeInvalidInput {
		t.Errorf("Expected InvalidInput error code, got %v", dbErr.Code)
	}
}

func TestDeleteDocuments(t *testing.T) {
	wtService, dbSvc := setupTestDB(t, "DeleteDocuments")

	// Create DB and collection
	err := dbSvc.CreateDB()
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	collName := "test_collection"
	err = dbSvc.CreateCollection(collName)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Insert some documents
	doc1Id := primitive.NewObjectID().Hex()
	doc2Id := primitive.NewObjectID().Hex()
	doc3Id := primitive.NewObjectID().Hex()

	documents := []GlowstickDocument{
		{
			Id:        doc1Id,
			Content:   "Test document 1",
			Embedding: genEmbeddings(128),
			Metadata:  map[string]any{"type": "test"},
		},
		{
			Id:        doc2Id,
			Content:   "Test document 2",
			Embedding: genEmbeddings(128),
			Metadata:  map[string]any{"type": "test"},
		},
		{
			Id:        doc3Id,
			Content:   "Test document 3",
			Embedding: genEmbeddings(128),
			Metadata:  map[string]any{"type": "test"},
		},
	}

	err = dbSvc.InsertDocuments(collName, documents)
	if err != nil {
		t.Fatalf("Failed to insert documents: %v", err)
	}

	// Get collection info to find table URI
	collEntry, err := dbSvc.GetCollection(collName)
	if err != nil {
		t.Fatalf("Failed to get collection: %v", err)
	}
	tableUri := collEntry.Info.TableUri

	// Verify documents exist
	_, exists1, _ := wtService.GetBinaryWithStringKey(tableUri, doc1Id)
	_, exists2, _ := wtService.GetBinaryWithStringKey(tableUri, doc2Id)
	if !exists1 || !exists2 {
		t.Fatalf("Documents should exist before deletion")
	}

	// Delete first two documents
	err = dbSvc.DeleteDocuments(collName, []string{doc1Id, doc2Id})
	if err != nil {
		t.Fatalf("DeleteDocuments failed: %v", err)
	}

	// Verify deleted documents no longer exist
	_, exists1, _ = wtService.GetBinaryWithStringKey(tableUri, doc1Id)
	_, exists2, _ = wtService.GetBinaryWithStringKey(tableUri, doc2Id)
	if exists1 {
		t.Errorf("Document 1 should not exist after deletion")
	}
	if exists2 {
		t.Errorf("Document 2 should not exist after deletion")
	}

	// Verify remaining document still exists
	_, exists3, _ := wtService.GetBinaryWithStringKey(tableUri, doc3Id)
	if !exists3 {
		t.Errorf("Document 3 should still exist after deletion of other docs")
	}

	// Verify stats were updated
	collectionDefKey := fmt.Sprintf("default.%s", collName)
	statsVal, statsExists, _ := wtService.GetBinaryWithStringKey(STATS, collectionDefKey)
	if !statsExists {
		t.Fatalf("Stats entry should exist")
	}

	var stats CollectionStats
	if err := bson.Unmarshal(statsVal, &stats); err != nil {
		t.Fatalf("Failed to unmarshal stats: %v", err)
	}

	// Should have 1 doc remaining (3 - 2 deleted)
	if stats.Doc_Count != 1 {
		t.Errorf("Expected Doc_Count to be 1, got %d", stats.Doc_Count)
	}
}

func TestDeleteDocuments_NotFound(t *testing.T) {
	_, dbSvc := setupTestDB(t, "DeleteDocumentsNotFound")

	// Create DB and collection
	err := dbSvc.CreateDB()
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	collName := "test_collection"
	err = dbSvc.CreateCollection(collName)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Try to delete non-existent documents (should not error, just skip)
	err = dbSvc.DeleteDocuments(collName, []string{"nonexistent1", "nonexistent2"})
	if err != nil {
		t.Errorf("DeleteDocuments should not error for non-existent document IDs: %v", err)
	}
}

func TestDeleteDocuments_CollectionNotFound(t *testing.T) {
	_, dbSvc := setupTestDB(t, "DeleteDocsCollNotFound")

	// Create DB but not collection
	err := dbSvc.CreateDB()
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// Try to delete documents from non-existent collection
	err = dbSvc.DeleteDocuments("nonexistent", []string{"doc1"})
	if err == nil {
		t.Errorf("DeleteDocuments should return error for non-existent collection")
	}

	dbErr, ok := err.(*DBError)
	if !ok {
		t.Errorf("Expected DBError, got %T", err)
	} else if dbErr.Code != ErrCodeNotFound {
		t.Errorf("Expected NotFound error code, got %v", dbErr.Code)
	}
}

func TestDeleteDocuments_EmptyList(t *testing.T) {
	_, dbSvc := setupTestDB(t, "DeleteDocsEmptyList")

	// Create DB and collection
	err := dbSvc.CreateDB()
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	collName := "test_collection"
	err = dbSvc.CreateCollection(collName)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Try to delete with empty list
	err = dbSvc.DeleteDocuments(collName, []string{})
	if err == nil {
		t.Errorf("DeleteDocuments should return error for empty document IDs list")
	}

	dbErr, ok := err.(*DBError)
	if !ok {
		t.Errorf("Expected DBError, got %T", err)
	} else if dbErr.Code != ErrCodeInvalidInput {
		t.Errorf("Expected InvalidInput error code, got %v", dbErr.Code)
	}
}

