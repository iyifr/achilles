package dbservice

import (
	"achillesdb/pkgs/faiss"
	wt "achillesdb/pkgs/wiredtiger"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
)

type DbCatalogEntry struct {
	UUID   string            `bson:"_uuid"`
	Name   string            `bson:"name"`
	Config map[string]string `bson:"config"`
}

// type CollectionIndex struct {
// 	Id   string                 `bson:"_id"`
// 	Key  map[string]int         `bson:"key"`
// 	Name string                 `bson:"name"`
// 	Ns   string                 `bson:"ns"`
// 	Type string                 `bson:"type"`
// 	V    int                    `bson:"v"`
// 	Opts map[string]interface{} `bson:"opts,omitempty"`
// }

type CollectionCatalogEntry struct {
	Id             primitive.ObjectID `bson:"_id" json:"_id"`
	Ns             string             `bson:"ns" json:"ns"`
	TableUri       string             `bson:"table_uri" json:"table_uri"`
	VectorIndexUri string             `bson:"vector_index_uri" json:"vector_index_uri"`
	CreatedAt      primitive.DateTime `bson:"createdAt" json:"createdAt"`
	UpdatedAt      primitive.DateTime `bson:"updatedAt" json:"updatedAt"`
}

type CollectionStats struct {
	Doc_Count         int     `bson:"doc_count" json:"doc_count"`
	Vector_Index_Size float64 `bson:"vector_index_size" json:"vector_index_size"`
}

type GDBService struct {
	Name      string
	KvService wt.WTService
	Logger    *zap.SugaredLogger
}

func (s *GDBService) CreateDB() error {
	start := time.Now()
	if s.Logger != nil {
		s.Logger.Infow("create_db_start", "db_name", s.Name)
	}

	if s.Name == "" {
		return InvalidInput_Err(ErrEmptyName)
	}

	catalogEntry := DbCatalogEntry{
		UUID:   primitive.NewObjectID().Hex(),
		Name:   s.Name,
		Config: map[string]string{"Index": "HNSW"},
	}

	doc, err := bson.Marshal(catalogEntry)
	if err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("create_db_marshal_error", "db_name", s.Name, "error", err)
		}
		return Serialization_Err(Wrap_Err(err, "failed to marshal catalog entry"))
	}

	dbKey := fmt.Sprintf("db:%s", s.Name)
	_, exists, _ := s.KvService.GetBinaryWithStringKey(CATALOG, dbKey)

	if exists {
		if s.Logger != nil {
			s.Logger.Warnw("db_already_exists", "db_name", s.Name)
		}
		return AlreadyExists_Err(ErrDatabaseExists)
	}

	err = s.KvService.PutBinaryWithStringKey(CATALOG, dbKey, doc)
	if err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("create_db_storage_error", "db_name", s.Name, "error", err)
		}
		return Storage_Err(Wrap_Err(err, "failed to write db catalog entry"))
	}

	if s.Logger != nil {
		duration := time.Since(start)
		s.Logger.Infow("create_db_complete", "db_name", s.Name, "duration_ms", duration.Milliseconds())
	}
	return nil
}
func (s *GDBService) ListDatabases() (ListDatabasesResponse, error) {
	start := time.Now()
	if s.Logger != nil {
		s.Logger.Infow("list_databases_start")
	}

	startKey := []byte("db:")
	endKey := []byte("db;") // Using semicolon as it comes after colon in ASCII

	cursor, err := s.KvService.ScanRangeBinary(CATALOG, startKey, endKey)
	if err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("list_databases_scan_error", "error", err)
		}
		return ListDatabasesResponse{}, Storage_Err(Wrap_Err(err, "failed to scan catalog for databases"))
	}
	defer cursor.Close()

	var databases []DatabaseInfo

	for cursor.Next() {
		_, value, err := cursor.Current()
		if err != nil {
			if s.Logger != nil {
				s.Logger.Errorw("list_databases_cursor_current_error", "error", err)
			}
			return ListDatabasesResponse{}, Storage_Err(Wrap_Err(err, "failed to get current database"))
		}

		var dbEntry DbCatalogEntry
		if err := bson.Unmarshal(value, &dbEntry); err != nil {
			if s.Logger != nil {
				s.Logger.Errorw("list_databases_unmarshal_error", "error", err)
			}
			return ListDatabasesResponse{}, Serialization_Err(Wrap_Err(err, "failed to unmarshal database catalog entry"))
		}

		// Count collections for this database
		collStartKey := []byte(dbEntry.Name + ".")
		collEndKey := []byte(dbEntry.Name + "/")

		collCount := 0
		collCursor, err := s.KvService.ScanRangeBinary(CATALOG, collStartKey, collEndKey)
		if err == nil {
			for collCursor.Next() {
				collCount++
			}
			collCursor.Close()
		}

		databases = append(databases, DatabaseInfo{
			Name:            dbEntry.Name,
			CollectionCount: collCount,
			Empty:           collCount == 0,
		})
	}

	if err := cursor.Err(); err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("list_databases_cursor_error", "error", err)
		}
		return ListDatabasesResponse{}, Storage_Err(Wrap_Err(err, "cursor error during iteration"))
	}

	if s.Logger != nil {
		duration := time.Since(start)
		s.Logger.Infow("list_databases_complete", "database_count", len(databases), "duration_ms", duration.Milliseconds())
	}

	return ListDatabasesResponse{
		Databases: databases,
	}, nil
}

func (s *GDBService) DeleteDB(name string) error {
	start := time.Now()
	if s.Logger != nil {
		s.Logger.Infow("delete_db_start", "db_name", name)
	}

	if name == "" {
		return InvalidInput_Err(ErrEmptyName)
	}

	kv := s.KvService

	// Check if database exists
	dbKey := fmt.Sprintf("db:%s", name)
	_, exists, err := kv.GetBinaryWithStringKey(CATALOG, dbKey)
	if err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("delete_db_check_error", "db_name", name, "error", err)
		}
		return Storage_Err(Wrap_Err(err, "failed to check if database exists"))
	}
	if !exists {
		if s.Logger != nil {
			s.Logger.Warnw("delete_db_not_found", "db_name", name)
		}
		return NotFound_Err(ErrDatabaseNotFound)
	}

	// Get all collections for this database and delete them
	startKey := []byte(name + ".")
	endKey := []byte(name + "/")

	cursor, err := kv.ScanRangeBinary(CATALOG, startKey, endKey)
	if err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("delete_db_scan_error", "db_name", name, "error", err)
		}
		return Storage_Err(Wrap_Err(err, "failed to scan catalog for collections"))
	}

	var collectionsToDelete []CollectionCatalogEntry
	for cursor.Next() {
		_, value, err := cursor.Current()
		if err != nil {
			cursor.Close()
			if s.Logger != nil {
				s.Logger.Errorw("delete_db_cursor_current_error", "db_name", name, "error", err)
			}
			return Storage_Err(Wrap_Err(err, "failed to get current collection"))
		}
		var collection CollectionCatalogEntry
		if err := bson.Unmarshal(value, &collection); err != nil {
			cursor.Close()
			if s.Logger != nil {
				s.Logger.Errorw("delete_db_unmarshal_error", "db_name", name, "error", err)
			}
			return Serialization_Err(Wrap_Err(err, "failed to unmarshal collection catalog"))
		}
		collectionsToDelete = append(collectionsToDelete, collection)
	}
	cursor.Close()

	// Delete each collection's resources
	for _, collection := range collectionsToDelete {
		// Drop the collection's WiredTiger table
		if err := kv.DeleteTable(collection.TableUri); err != nil {
			if !IsNotFoundError(err) && !IsBusyError(err) {
				if s.Logger != nil {
					s.Logger.Errorw("delete_db_drop_table_error", "db_name", name, "table_uri", collection.TableUri, "error", err)
				}
				return Storage_Err(Wrap_Err(err, "failed to drop collection table %s", collection.TableUri))
			}
		}

		// Delete vector index file
		if collection.VectorIndexUri != "" {
			os.Remove(collection.VectorIndexUri)
		}

		if err := kv.DeleteBinary(CATALOG, []byte(collection.Ns)); err != nil {
			if s.Logger != nil {
				s.Logger.Errorw("delete_db_delete_catalog_error", "db_name", name, "ns", collection.Ns, "error", err)
			}
			return Storage_Err(Wrap_Err(err, "failed to delete collection catalog entry"))
		}

		// Delete stats entry
		if err := kv.DeleteBinary(STATS, []byte(collection.Ns)); err != nil {
			if s.Logger != nil {
				s.Logger.Errorw("delete_db_delete_stats_error", "db_name", name, "ns", collection.Ns, "error", err)
			}
			return Storage_Err(Wrap_Err(err, "failed to delete collection catalog entry"))
		}
	}

	// Delete the database entry from catalog
	if err := kv.DeleteBinaryWithStringKey(CATALOG, dbKey); err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("delete_db_delete_entry_error", "db_name", name, "error", err)
		}
		return Storage_Err(Wrap_Err(err, "failed to delete database catalog entry"))
	}

	if s.Logger != nil {
		duration := time.Since(start)
		s.Logger.Infow("delete_db_complete", "db_name", name, "collections_deleted", len(collectionsToDelete), "duration_ms", duration.Milliseconds())
	}

	return nil
}

func (s *GDBService) CreateCollection(collection_name string) error {
	start := time.Now()
	if s.Logger != nil {
		s.Logger.Infow("create_collection_start", "collection", collection_name, "database", s.Name)
	}

	if len(collection_name) == 0 {
		return InvalidInput_Err(ErrEmptyName)
	}

	collectionId := primitive.NewObjectID()
	collectionTableUri := fmt.Sprintf("table:collection-%s-%s", collection_name, s.Name)
	collectionKey := fmt.Sprintf("%s.%s", s.Name, collection_name)

	catalogEntry := CollectionCatalogEntry{
		Id:             collectionId,
		Ns:             collectionKey,
		TableUri:       collectionTableUri,
		VectorIndexUri: fmt.Sprintf("%s/%s%s", GetVectorsFilePath(), collection_name, ".index"),
		CreatedAt:      primitive.NewDateTimeFromTime(time.Now()),
		UpdatedAt:      primitive.NewDateTimeFromTime(time.Now()),
	}

	exists, err := s.KvService.TableExists(collectionTableUri)
	if err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("create_collection_check_error", "collection", collection_name, "database", s.Name, "error", err)
		}
		return Storage_Err(Wrap_Err(err, "failed to check if table exists %s", collectionTableUri))
	}

	if exists {
		if s.Logger != nil {
			s.Logger.Warnw("create_collection_already_exists", "collection", collection_name, "database", s.Name)
		}
		return AlreadyExists_Err(ErrCollectionExists)
	}

	err = s.KvService.CreateTable(collectionTableUri, "key_format=u,value_format=u")
	if err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("create_collection_table_error", "collection", collection_name, "database", s.Name, "table_uri", collectionTableUri, "error", err)
		}
		return Storage_Err(Wrap_Err(err, "failed to create table %s", collectionTableUri))
	}

	doc, err := bson.Marshal(catalogEntry)
	if err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("create_collection_marshal_error", "collection", collection_name, "database", s.Name, "error", err)
		}
		return Serialization_Err(Wrap_Err(err, "failed to encode catalog entry"))
	}

	err = s.KvService.PutBinaryWithStringKey(CATALOG, collectionKey, doc)
	if err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("create_collection_catalog_error", "collection", collection_name, "database", s.Name, "error", err)
		}
		return Storage_Err(Wrap_Err(err, "failed to write catalog entry"))
	}

	statsEntry := CollectionStats{
		Doc_Count:         0,
		Vector_Index_Size: 0,
	}

	stats_doc, err := bson.Marshal(statsEntry)
	if err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("create_collection_stats_marshal_error", "collection", collection_name, "database", s.Name, "error", err)
		}
		return Serialization_Err(Wrap_Err(err, "failed to encode stats entry"))
	}

	err = s.KvService.PutBinaryWithStringKey(STATS, collectionKey, stats_doc)
	if err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("create_collection_stats_error", "collection", collection_name, "database", s.Name, "error", err)
		}
		return Storage_Err(Wrap_Err(err, "failed to write stats entry"))
	}

	if s.Logger != nil {
		duration := time.Since(start)
		s.Logger.Infow("create_collection_complete", "collection", collection_name, "database", s.Name, "duration_ms", duration.Milliseconds())
	}

	return nil
}

func (s *GDBService) DeleteCollection(collection_name string) error {
	start := time.Now()
	if s.Logger != nil {
		s.Logger.Infow("delete_collection_start", "collection", collection_name, "database", s.Name)
	}

	if len(collection_name) == 0 {
		return InvalidInput_Err(ErrEmptyName)
	}

	kv := s.KvService
	collectionDefKey := fmt.Sprintf("%s.%s", s.Name, collection_name)

	// Get collection info
	val, exists, err := kv.GetBinary(CATALOG, []byte(collectionDefKey))
	if err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("delete_collection_get_error", "collection", collection_name, "database", s.Name, "error", err)
		}
		return Storage_Err(Wrap_Err(err, "failed to get collection catalog"))
	}
	if !exists {
		if s.Logger != nil {
			s.Logger.Warnw("delete_collection_not_found", "collection", collection_name, "database", s.Name)
		}
		return NotFound_Err(ErrCollectionNotFound)
	}

	var collection CollectionCatalogEntry
	if err := bson.Unmarshal(val, &collection); err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("delete_collection_unmarshal_error", "collection", collection_name, "database", s.Name, "error", err)
		}
		return Serialization_Err(Wrap_Err(err, "failed to unmarshal collection catalog"))
	}

	// Delete vector index file
	if collection.VectorIndexUri != "" {
		os.Remove(collection.VectorIndexUri)
	}

	// Delete collection from catalog
	if err := kv.DeleteBinary(CATALOG, []byte(collectionDefKey)); err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("delete_collection_catalog_error", "collection", collection_name, "database", s.Name, "error", err)
		}
		return Storage_Err(Wrap_Err(err, "failed to delete collection catalog entry"))
	}

	// Delete stats entry
	kv.DeleteBinary(STATS, []byte(collectionDefKey))

	if s.Logger != nil {
		duration := time.Since(start)
		s.Logger.Infow("delete_collection_complete", "collection", collection_name, "database", s.Name, "duration_ms", duration.Milliseconds())
	}

	return nil
}

func (s *GDBService) ListCollections() ([]CollectionCatalogEntry, error) {
	start := time.Now()
	if s.Logger != nil {
		s.Logger.Infow("list_collections_start", "database", s.Name)
	}

	if s.Name == "" {
		return nil, InvalidInput_Err(ErrEmptyName)
	}

	startKey := []byte(s.Name + ".")
	endKey := []byte(s.Name + "/")

	cursor, err := s.KvService.ScanRangeBinary(CATALOG, startKey, endKey)
	if err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("list_collections_scan_error", "database", s.Name, "error", err)
		}
		return nil, Storage_Err(Wrap_Err(err, "failed to scan catalog"))
	}
	defer cursor.Close()

	var collections []CollectionCatalogEntry
	for cursor.Next() {
		_, value, err := cursor.Current()
		if err != nil {
			if s.Logger != nil {
				s.Logger.Errorw("list_collections_cursor_current_error", "database", s.Name, "error", err)
			}
			return nil, Storage_Err(Wrap_Err(err, "failed to get current"))
		}
		collectionValue := CollectionCatalogEntry{}
		if err := bson.Unmarshal(value, &collectionValue); err != nil {
			if s.Logger != nil {
				s.Logger.Errorw("list_collections_unmarshal_error", "database", s.Name, "error", err)
			}
			return nil, Serialization_Err(Wrap_Err(err, "failed to unmarshal collection catalog"))
		}
		collections = append(collections, collectionValue)
	}

	if err := cursor.Err(); err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("list_collections_cursor_error", "database", s.Name, "error", err)
		}
		return nil, Storage_Err(Wrap_Err(err, "cursor error during iteration"))
	}

	if s.Logger != nil {
		duration := time.Since(start)
		s.Logger.Infow("list_collections_complete", "database", s.Name, "collection_count", len(collections), "duration_ms", duration.Milliseconds())
	}

	return collections, nil
}

func (s *GDBService) GetCollection(collection_name string) (CollectionEntry, error) {
	start := time.Now()
	if s.Logger != nil {
		s.Logger.Infow("get_collection_start", "collection", collection_name, "database", s.Name)
	}

	if s.Name == "" {
		return CollectionEntry{}, InvalidInput_Err(ErrEmptyName)
	}

	collectionDefKey := fmt.Sprintf("%s.%s", s.Name, collection_name)
	val, exists, err := s.KvService.GetBinary(CATALOG, []byte(collectionDefKey))

	if err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("get_collection_catalog_error", "collection", collection_name, "database", s.Name, "error", err)
		}
		return CollectionEntry{}, Storage_Err(Wrap_Err(err, "failed to get collection catalog"))
	}

	if !exists {
		if s.Logger != nil {
			s.Logger.Warnw("get_collection_not_found", "collection", collection_name, "database", s.Name)
		}
		return CollectionEntry{}, NotFound_Err(ErrCollectionNotFound)
	}

	var collection CollectionCatalogEntry
	if err := bson.Unmarshal(val, &collection); err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("get_collection_unmarshal_catalog_error", "collection", collection_name, "database", s.Name, "error", err)
		}
		return CollectionEntry{}, Serialization_Err(Wrap_Err(err, "failed to unmarshal collection catalog"))
	}

	statsVal, statsExists, statsErr := s.KvService.GetBinary(STATS, []byte(collectionDefKey))
	var stats CollectionStats
	if statsErr == nil && statsExists {
		if err := bson.Unmarshal(statsVal, &stats); err != nil {
			if s.Logger != nil {
				s.Logger.Errorw("get_collection_unmarshal_stats_error", "collection", collection_name, "database", s.Name, "error", err)
			}
			return CollectionEntry{}, Serialization_Err(Wrap_Err(err, "failed to unmarshal collection stats"))
		}
	}

	documents := make([]GlowstickDocument, 0)

	stats.Vector_Index_Size = float64(stats.Vector_Index_Size)

	if s.Logger != nil {
		duration := time.Since(start)
		s.Logger.Infow("get_collection_complete", "collection", collection_name, "database", s.Name, "duration_ms", duration.Milliseconds())
	}

	return CollectionEntry{
		Info:      collection,
		Documents: documents,
		Stats:     stats,
	}, nil
}

func (s *GDBService) InsertDocuments(collection_name string, documents []GlowstickDocument) error {
	start := time.Now()
	if s.Logger != nil {
		s.Logger.Infow("insert_documents_start", "collection", collection_name, "doc_count", len(documents))
	}

	if len(documents) == 0 {
		return InvalidInput_Err(ErrEmptyDocuments)
	}

	kv := s.KvService

	collectionDefKey := fmt.Sprintf("%s.%s", s.Name, collection_name)
	val, exists, err := kv.GetBinary(CATALOG, []byte(collectionDefKey))

	if !exists {
		return NotFound_Err(ErrCollectionNotFound)
	}

	if err != nil {
		return Storage_Err(Wrap_Err(err, "failed to get collection catalog"))
	}

	var collection CollectionCatalogEntry
	if err := bson.Unmarshal(val, &collection); err != nil {
		return Serialization_Err(Wrap_Err(err, "failed to unmarshal collection catalog"))
	}

	vectorIndexFilePath := collection.VectorIndexUri

	// Validate first document has embedding before getting/creating index
	if len(documents[0].Embedding) == 0 {
		return InvalidInput_Err(fmt.Errorf("document with ID:%s has empty embedding", documents[0].Id))
	}

	// Use cached index (avoids disk read on every insert)
	indexCache := faiss.GlobalIndexCache()
	cachedIdx, err := indexCache.GetOrCreate(vectorIndexFilePath, len(documents[0].Embedding))
	if err != nil {
		return Internal_Err(Wrap_Err(err, "failed to get or create vector index"))
	}

	// Lock the index for exclusive access during this insert operation
	cachedIdx.Lock()
	defer cachedIdx.Unlock()

	idx := cachedIdx.Index
	destTableURI := collection.TableUri

	startLabel, err := idx.NTotal()
	if err != nil {
		return Internal_Err(Wrap_Err(err, "failed to get index size"))
	}

	// Process embeddings for vector index
	numDocs := len(documents)
	var embeddingDim int
	var embeddings []float32
	labelMappings := make([]string, numDocs)

	// Use batch writer for document inserts (single session for all writes)
	docWriter, err := s.KvService.NewBatchWriter(destTableURI, wt.VALUE_FORMAT_BINARY)
	if err != nil {
		return Storage_Err(Wrap_Err(err, "failed to create batch writer for documents"))
	}
	defer docWriter.Close()

	for i, doc := range documents {
		if len(doc.Embedding) == 0 {
			return InvalidInput_Err(fmt.Errorf("document with ID:%s has empty embedding", doc.Id))
		}

		if i == 0 {
			embeddingDim = len(doc.Embedding)
			embeddings = make([]float32, numDocs*embeddingDim)
		} else if len(doc.Embedding) != embeddingDim {
			return InvalidInput_Err(fmt.Errorf("document %s has embedding dimension %d, expected %d", doc.Id, len(doc.Embedding), embeddingDim))
		}

		doc_bytes, release, err := BsonMarshalWithPool(doc)
		defer release()

		if err != nil {
			return Serialization_Err(Wrap_Err(err, "failed to marshal document %s", doc.Id))
		}

		key := []byte(doc.Id)
		if err := docWriter.PutBinary(key, doc_bytes); err != nil {
			return Storage_Err(Wrap_Err(err, "failed to insert document %s at index %d", doc.Id, i))
		}

		// Put embedding data from current doc in embeddings array.
		// Note: We can skip this extra copy if we switch the API to use SOA pattern.
		copy(embeddings[i*embeddingDim:(i+1)*embeddingDim], doc.Embedding)
		labelMappings[i] = doc.Id
	}

	if err := docWriter.Commit(); err != nil {
		return Storage_Err(Wrap_Err(err, "failed to commit document batch"))
	}

	if err := idx.Add(embeddings, len(documents)); err != nil {
		return Internal_Err(Wrap_Err(err, "failed to add embeddings to index"))
	}

	// Use batch writer for label mappings (single session for all writes)
	labelWriter, err := s.KvService.NewBatchWriter(LABELS_TO_DOC_ID_MAPPING_TABLE_URI, wt.VALUE_FORMAT_STRING)
	if err != nil {
		return Storage_Err(Wrap_Err(err, "failed to create batch writer for labels"))
	}
	defer labelWriter.Close()

	for i, docID := range labelMappings {
		label := startLabel + int64(i)
		if err := labelWriter.PutString(fmt.Sprintf("%d", label), docID); err != nil {
			return Storage_Err(Wrap_Err(err, "failed to write label->docID mapping for label %d", label))
		}
	}

	// Commit label batch
	if err := labelWriter.Commit(); err != nil {
		return Storage_Err(Wrap_Err(err, "failed to commit label batch"))
	}

	// Write index
	if err := idx.WriteToFile(vectorIndexFilePath); err != nil {
		return Storage_Err(Wrap_Err(err, "failed to write index to file"))
	}
	cachedIdx.Dirty = false

	info, err := os.Stat(vectorIndexFilePath)
	if err != nil {
		return Storage_Err(Wrap_Err(err, "failed to stat vector index file"))
	}

	hot_stats, statsExists, err := kv.GetBinary(STATS, []byte(collectionDefKey))
	if err != nil {
		return Storage_Err(Wrap_Err(err, "failed to fetch hot stats"))
	}
	if !statsExists {
		return Storage_Err(fmt.Errorf("collection stats not found for %s", collectionDefKey))
	}

	var hot_stats_doc CollectionStats
	if err := bson.Unmarshal(hot_stats, &hot_stats_doc); err != nil {
		return Serialization_Err(Wrap_Err(err, "failed to unmarshal hot stats"))
	}

	hot_stats_doc.Doc_Count += int(len(documents))
	hot_stats_doc.Vector_Index_Size = float64(info.Size())

	hot_stats_doc_bytes, err := bson.Marshal(hot_stats_doc)
	if err != nil {
		return Serialization_Err(Wrap_Err(err, "failed to marshal hot stats"))
	}

	if err := kv.PutBinary(STATS, []byte(collectionDefKey), hot_stats_doc_bytes); err != nil {
		return Storage_Err(Wrap_Err(err, "failed to write hot stats"))
	}

	if s.Logger != nil {
		duration := time.Since(start)
		s.Logger.Infow("insert_documents_complete", "collection", collection_name, "doc_count", len(documents), "duration_ms", duration.Milliseconds())
	}
	return nil
}

func (s *GDBService) InsertDocumentsSOA(collection_name string, documents *GlowstickDocumentSOA) error {
	start := time.Now()
	if s.Logger != nil {
		s.Logger.Infow("insert_documents_soa_start", "collection", collection_name, "doc_count", documents.DocumentCount())
	}

	// Validate input structure
	if err := documents.Validate(); err != nil {
		return InvalidInput_Err(Wrap_Err(err, "invalid SOA document structure"))
	}

	kv := s.KvService
	numDocs := documents.DocumentCount()
	embeddingDim := documents.EmbeddingDimension()

	// Get collection metadata
	collectionDefKey := fmt.Sprintf("%s.%s", s.Name, collection_name)
	val, exists, err := kv.GetBinary(CATALOG, []byte(collectionDefKey))

	if !exists {
		return NotFound_Err(ErrCollectionNotFound)
	}

	if err != nil {
		return Storage_Err(Wrap_Err(err, "failed to get collection catalog"))
	}

	var collection CollectionCatalogEntry
	if err := bson.Unmarshal(val, &collection); err != nil {
		return Serialization_Err(Wrap_Err(err, "failed to unmarshal collection catalog"))
	}

	vectorIndexFilePath := collection.VectorIndexUri

	// Use cached index (avoids disk read on every insert)
	indexCache := faiss.GlobalIndexCache()
	cachedIdx, err := indexCache.GetOrCreate(vectorIndexFilePath, embeddingDim)
	if err != nil {
		return Internal_Err(Wrap_Err(err, "failed to get or create vector index"))
	}

	// Lock the index for exclusive access during this insert operation
	cachedIdx.Lock()
	defer cachedIdx.Unlock()

	idx := cachedIdx.Index
	destTableURI := collection.TableUri

	startLabel, err := idx.NTotal()
	if err != nil {
		return Internal_Err(Wrap_Err(err, "failed to get index size"))
	}

	// Use batch writer for document inserts (single session for all writes)
	docWriter, err := s.KvService.NewBatchWriter(destTableURI, wt.VALUE_FORMAT_BINARY)
	if err != nil {
		return Storage_Err(Wrap_Err(err, "failed to create batch writer for documents"))
	}
	defer docWriter.Close()

	// Insert each document into WiredTiger
	for i := 0; i < numDocs; i++ {
		doc := GlowstickDocument{
			Id:        documents.Ids[i],
			Content:   documents.Contents[i],
			Embedding: nil, // Don't store embedding in BSON (already in FAISS)
			Metadata:  documents.Metadatas[i],
		}

		doc_bytes, release, err := BsonMarshalWithPool(doc)
		defer release()

		if err != nil {
			return Serialization_Err(Wrap_Err(err, "failed to marshal document %s", doc.Id))
		}

		key := []byte(doc.Id)
		if err := docWriter.PutBinary(key, doc_bytes); err != nil {
			return Storage_Err(Wrap_Err(err, "failed to insert document %s at index %d", doc.Id, i))
		}
	}

	if err := docWriter.Commit(); err != nil {
		return Storage_Err(Wrap_Err(err, "failed to commit document batch"))
	}

	if err := idx.Add(documents.Embeddings, numDocs); err != nil {
		return Internal_Err(Wrap_Err(err, "failed to add embeddings to index"))
	}

	// Use batch writer for label mappings (single session for all writes)
	labelWriter, err := s.KvService.NewBatchWriter(LABELS_TO_DOC_ID_MAPPING_TABLE_URI, wt.VALUE_FORMAT_STRING)
	if err != nil {
		return Storage_Err(Wrap_Err(err, "failed to create batch writer for labels"))
	}
	defer labelWriter.Close()

	for i := 0; i < numDocs; i++ {
		label := startLabel + int64(i)
		if err := labelWriter.PutString(fmt.Sprintf("%d", label), documents.Ids[i]); err != nil {
			return Storage_Err(Wrap_Err(err, "failed to write label->docID mapping for label %d", label))
		}
	}

	// Commit label batch
	if err := labelWriter.Commit(); err != nil {
		return Storage_Err(Wrap_Err(err, "failed to commit label batch"))
	}

	// Write index to disk
	if err := idx.WriteToFile(vectorIndexFilePath); err != nil {
		return Storage_Err(Wrap_Err(err, "failed to write index to file"))
	}
	cachedIdx.Dirty = false

	// Update collection statistics
	info, err := os.Stat(vectorIndexFilePath)
	if err != nil {
		return Storage_Err(Wrap_Err(err, "failed to stat vector index file"))
	}

	hot_stats, statsExists, err := kv.GetBinary(STATS, []byte(collectionDefKey))
	if err != nil {
		return Storage_Err(Wrap_Err(err, "failed to fetch hot stats"))
	}
	if !statsExists {
		return Storage_Err(fmt.Errorf("collection stats not found for %s", collectionDefKey))
	}

	var hot_stats_doc CollectionStats
	if err := bson.Unmarshal(hot_stats, &hot_stats_doc); err != nil {
		return Serialization_Err(Wrap_Err(err, "failed to unmarshal hot stats"))
	}

	hot_stats_doc.Doc_Count += numDocs
	hot_stats_doc.Vector_Index_Size = float64(info.Size())

	hot_stats_doc_bytes, err := bson.Marshal(hot_stats_doc)
	if err != nil {
		return Serialization_Err(Wrap_Err(err, "failed to marshal hot stats"))
	}

	if err := kv.PutBinary(STATS, []byte(collectionDefKey), hot_stats_doc_bytes); err != nil {
		return Storage_Err(Wrap_Err(err, "failed to write hot stats"))
	}

	if s.Logger != nil {
		duration := time.Since(start)
		s.Logger.Infow("insert_documents_soa_complete", "collection", collection_name, "doc_count", numDocs, "duration_ms", duration.Milliseconds())
	}
	return nil
}

func (s *GDBService) QueryCollection(collection_name string, query QueryStruct) ([]GlowstickQueryResultSet, error) {
	start := time.Now()
	if s.Logger != nil {
		s.Logger.Infow("query_collection_start", "collection", collection_name, "top_k", query.TopK)
	}

	kv := s.KvService
	vectr_svc := faiss.FAISS()
	collectionDefKey := s.Name + "." + collection_name

	// Get collection catalog
	val, exists, err := kv.GetBinary(CATALOG, []byte(collectionDefKey))
	if err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("query_collection_catalog_error", "collection", collection_name, "error", err)
		}
		return nil, Storage_Err(Wrap_Err(err, "failed to get collection catalog"))
	}

	if !exists {
		if s.Logger != nil {
			s.Logger.Warnw("query_collection_not_found", "collection", collection_name)
		}
		return nil, NotFound_Err(ErrCollectionNotFound)
	}

	var collection CollectionCatalogEntry
	if err := bson.Unmarshal(val, &collection); err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("query_collection_unmarshal_error", "collection", collection_name, "error", err)
		}
		return nil, Serialization_Err(Wrap_Err(err, "failed to unmarshal collection catalog"))
	}

	// Parse vector index path
	vectorIndexUri := collection.VectorIndexUri
	var vectorIndexFilePath string
	if vectorIndexUri != "" {
		u, err := url.Parse(vectorIndexUri)
		if err != nil {
			if s.Logger != nil {
				s.Logger.Errorw("query_collection_parse_uri_error", "collection", collection_name, "uri", vectorIndexUri, "error", err)
			}
			return nil, Internal_Err(Wrap_Err(err, "failed to parse vector index URI"))
		}
		vectorIndexFilePath = u.Path
	}

	// Load vector index
	idx, err := vectr_svc.ReadIndex(vectorIndexFilePath)
	if err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("query_collection_read_index_error", "collection", collection_name, "path", vectorIndexFilePath, "error", err)
		}
		return nil, Storage_Err(Wrap_Err(err, "could not read vector index from file"))
	}
	defer idx.Free()

	// Search vector index
	distances, ids, err := idx.Search(query.QueryEmbedding, 1, int(query.TopK))
	if err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("query_collection_search_error", "collection", collection_name, "error", err)
		}
		return nil, Internal_Err(Wrap_Err(err, "failed to search vector index"))
	}

	docs := make([]GlowstickQueryResultSet, 0, query.TopK)
	if len(ids) == 0 {
		return docs, nil
	}

	// Helper function to process a single document
	processDoc := func(id int64, distance float32) *GlowstickQueryResultSet {
		key := strconv.FormatInt(id, 10)

		val, exists, err := kv.GetString(LABELS_TO_DOC_ID_MAPPING_TABLE_URI, key)
		if err != nil || !exists {
			return nil
		}

		docIDBytes := []byte(val)
		docBin, exists, err := kv.GetBinary(collection.TableUri, docIDBytes)
		if err != nil || !exists || len(docBin) == 0 {
			return nil
		}
		var doc GlowstickDocument
		if err := bson.Unmarshal(docBin, &doc); err != nil {
			return nil
		}

		if query.MaxDistance != 0 && distance >= query.MaxDistance {
			return nil
		}

		if query.Filters != nil {
			matches, err := matchesFilter(doc.Metadata, query.Filters)
			if err != nil || !matches {
				return nil
			}
		}

		// Convert to query result set with distance
		result := GlowstickQueryResultSet{
			Id:        doc.Id,
			Content:   doc.Content,
			Embedding: doc.Embedding,
			Metadata:  doc.Metadata,
			Distance:  distance,
		}

		return &result
	}

	// For small result sets, process sequentially
	if len(ids) <= 3 {
		for i, id := range ids {
			if doc := processDoc(int64(id), distances[i]); doc != nil {
				docs = append(docs, *doc)
			}
		}
		if s.Logger != nil {
			duration := time.Since(start)
			s.Logger.Infow("query_collection_complete", "collection", collection_name, "results", len(docs), "duration_ms", duration.Milliseconds())
		}
		return docs, nil
	}

	// For larger result sets, use parallel processing
	numWorkers := min(len(ids), 10)
	chunkSize := (len(ids) + numWorkers - 1) / numWorkers

	type docResult struct {
		doc   GlowstickQueryResultSet
		index int
	}

	resultChan := make(chan docResult, len(ids))
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		end := min(start+chunkSize, len(ids))

		if start >= len(ids) {
			break
		}

		wg.Add(1)
		go func(startIdx, endIdx int) {
			defer wg.Done()

			for j := startIdx; j < endIdx; j++ {
				if doc := processDoc(int64(ids[j]), distances[j]); doc != nil {
					resultChan <- docResult{doc: *doc, index: j}
				}
			}
		}(start, end)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	results := make(map[int]GlowstickQueryResultSet)
	for result := range resultChan {
		results[result.index] = result.doc
	}

	for i := range ids {
		if doc, exists := results[i]; exists {
			docs = append(docs, doc)
		}
	}

	if s.Logger != nil {
		duration := time.Since(start)
		s.Logger.Infow("query_collection_complete", "collection", collection_name, "results", len(docs), "duration_ms", duration.Milliseconds())
	}
	return docs, nil
}

func (s *GDBService) GetDocuments(collection_name string) ([]GlowstickDocument, error) {
	start := time.Now()
	if s.Logger != nil {
		s.Logger.Infow("get_documents_start", "collection", collection_name, "database", s.Name)
	}

	kv := s.KvService
	collectionDefKey := fmt.Sprintf("%s.%s", s.Name, collection_name)
	val, exists, err := kv.GetBinary(CATALOG, []byte(collectionDefKey))
	if err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("get_documents_catalog_error", "collection", collection_name, "error", err)
		}
		return nil, Storage_Err(Wrap_Err(err, "failed to get collection catalog"))
	}
	if !exists {
		if s.Logger != nil {
			s.Logger.Warnw("get_documents_collection_not_found", "collection", collection_name)
		}
		return nil, NotFound_Err(ErrCollectionNotFound)
	}
	var collection CollectionCatalogEntry
	if err := bson.Unmarshal(val, &collection); err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("get_documents_unmarshal_catalog_error", "collection", collection_name, "error", err)
		}
		return nil, Serialization_Err(Wrap_Err(err, "failed to unmarshal collection catalog"))
	}

	cursor, err := kv.ScanRangeBinary(collection.TableUri, []byte(""), []byte("~"))
	if err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("get_documents_scan_error", "collection", collection_name, "table_uri", collection.TableUri, "error", err)
		}
		return nil, Storage_Err(Wrap_Err(err, "failed to scan collection table"))
	}
	defer cursor.Close()

	var docs []GlowstickDocument
	for cursor.Next() {
		_, value, err := cursor.Current()
		if err != nil {
			if s.Logger != nil {
				s.Logger.Errorw("get_documents_cursor_current_error", "collection", collection_name, "error", err)
			}
			return nil, Storage_Err(Wrap_Err(err, "failed to get current document"))
		}

		var doc GlowstickDocument
		if err := bson.Unmarshal(value, &doc); err != nil {
			if s.Logger != nil {
				s.Logger.Errorw("get_documents_unmarshal_doc_error", "collection", collection_name, "error", err)
			}
			return nil, Serialization_Err(Wrap_Err(err, "failed to unmarshal document"))
		}
		docs = append(docs, doc)
	}

	if err := cursor.Err(); err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("get_documents_cursor_error", "collection", collection_name, "error", err)
		}
		return nil, Storage_Err(Wrap_Err(err, "cursor error during iteration"))
	}

	if s.Logger != nil {
		duration := time.Since(start)
		s.Logger.Infow("get_documents_complete", "collection", collection_name, "document_count", len(docs), "duration_ms", duration.Milliseconds())
	}

	return docs, nil
}

func (s *GDBService) UpdateDocuments(collection_name string, payload *DocUpdatePayload) error {
	start := time.Now()
	if s.Logger != nil {
		s.Logger.Infow("update_documents_start", "collection", collection_name, "database", s.Name, "document_id", payload.DocumentId)
	}

	kv := s.KvService
	collectionDefKey := fmt.Sprintf("%s.%s", s.Name, collection_name)
	val, exists, err := kv.GetBinary(CATALOG, []byte(collectionDefKey))

	if err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("update_documents_catalog_error", "collection", collection_name, "database", s.Name, "error", err)
		}
		return Storage_Err(Wrap_Err(err, "failed to get collection catalog"))
	}
	if !exists {
		if s.Logger != nil {
			s.Logger.Warnw("update_documents_collection_not_found", "collection", collection_name, "database", s.Name)
		}
		return NotFound_Err(ErrCollectionNotFound)
	}
	var collection CollectionCatalogEntry
	if err := bson.Unmarshal(val, &collection); err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("update_documents_unmarshal_catalog_error", "collection", collection_name, "database", s.Name, "error", err)
		}
		return Serialization_Err(Wrap_Err(err, "failed to unmarshal collection catalog"))
	}

	if len(payload.DocumentId) == 0 {
		return InvalidInput_Err(fmt.Errorf("document Id is empty"))
	}

	doc_raw, doc_exists, doc_get_err := kv.GetBinaryWithStringKey(collection.TableUri, payload.DocumentId)

	if !doc_exists {
		if s.Logger != nil {
			s.Logger.Warnw("update_documents_document_not_found", "collection", collection_name, "database", s.Name, "document_id", payload.DocumentId)
		}
		return NotFound_Err(ErrDocumentNotFound)
	}

	if doc_get_err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("update_documents_get_error", "collection", collection_name, "database", s.Name, "document_id", payload.DocumentId, "error", doc_get_err)
		}
		return Storage_Err(Wrap_Err(doc_get_err, "failed to get document"))
	}

	var doc GlowstickDocument
	if err := bson.Unmarshal(doc_raw, &doc); err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("update_documents_unmarshal_doc_error", "collection", collection_name, "database", s.Name, "document_id", payload.DocumentId, "error", err)
		}
		return Serialization_Err(Wrap_Err(err, "failed to unmarshal document"))
	}
	for key, value := range payload.Updates {
		if doc.Metadata == nil {
			doc.Metadata = make(map[string]interface{})
		}
		doc.Metadata[key] = value
	}
	updated_val, err := bson.Marshal(doc)
	if err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("update_documents_marshal_error", "collection", collection_name, "database", s.Name, "document_id", payload.DocumentId, "error", err)
		}
		return Serialization_Err(Wrap_Err(err, "failed to marshal updated document"))
	}

	if err := kv.PutBinaryWithStringKey(collection.TableUri, payload.DocumentId, updated_val); err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("update_documents_put_error", "collection", collection_name, "database", s.Name, "document_id", payload.DocumentId, "error", err)
		}
		return Storage_Err(Wrap_Err(err, "failed to update document"))
	}

	if s.Logger != nil {
		duration := time.Since(start)
		s.Logger.Infow("update_documents_complete", "collection", collection_name, "database", s.Name, "document_id", payload.DocumentId, "duration_ms", duration.Milliseconds())
	}

	return nil
}

func (s *GDBService) DeleteDocuments(collection_name string, documentIds []string) error {
	start := time.Now()
	if s.Logger != nil {
		s.Logger.Infow("delete_documents_start", "collection", collection_name, "database", s.Name, "doc_count", len(documentIds))
	}

	if len(documentIds) == 0 {
		return InvalidInput_Err(ErrEmptyDocuments)
	}

	kv := s.KvService
	collectionDefKey := fmt.Sprintf("%s.%s", s.Name, collection_name)

	// Get collection info
	val, exists, err := kv.GetBinary(CATALOG, []byte(collectionDefKey))
	if err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("delete_documents_catalog_error", "collection", collection_name, "database", s.Name, "error", err)
		}
		return Storage_Err(Wrap_Err(err, "failed to get collection catalog"))
	}
	if !exists {
		if s.Logger != nil {
			s.Logger.Warnw("delete_documents_collection_not_found", "collection", collection_name, "database", s.Name)
		}
		return NotFound_Err(ErrCollectionNotFound)
	}

	var collection CollectionCatalogEntry
	if err := bson.Unmarshal(val, &collection); err != nil {
		if s.Logger != nil {
			s.Logger.Errorw("delete_documents_unmarshal_catalog_error", "collection", collection_name, "database", s.Name, "error", err)
		}
		return Serialization_Err(Wrap_Err(err, "failed to unmarshal collection catalog"))
	}

	// Delete each document
	deletedCount := 0
	for _, docId := range documentIds {
		// Check if document exists before deleting
		_, docExists, _ := kv.GetBinaryWithStringKey(collection.TableUri, docId)
		if docExists {
			if err := kv.DeleteBinaryWithStringKey(collection.TableUri, docId); err != nil {
				if s.Logger != nil {
					s.Logger.Errorw("delete_documents_delete_error", "collection", collection_name, "database", s.Name, "document_id", docId, "error", err)
				}
				return Storage_Err(Wrap_Err(err, "failed to delete document %s", docId))
			}
			deletedCount++
		}
	}

	// Update stats if any documents were deleted
	if deletedCount > 0 {
		statsVal, statsExists, err := kv.GetBinary(STATS, []byte(collectionDefKey))
		if err != nil {
			if s.Logger != nil {
				s.Logger.Errorw("delete_documents_stats_get_error", "collection", collection_name, "database", s.Name, "error", err)
			}
			return Storage_Err(Wrap_Err(err, "failed to get stats"))
		}
		if statsExists {
			var stats CollectionStats
			if err := bson.Unmarshal(statsVal, &stats); err != nil {
				if s.Logger != nil {
					s.Logger.Errorw("delete_documents_stats_unmarshal_error", "collection", collection_name, "database", s.Name, "error", err)
				}
				return Serialization_Err(Wrap_Err(err, "failed to unmarshal stats"))
			}

			stats.Doc_Count -= deletedCount
			if stats.Doc_Count < 0 {
				stats.Doc_Count = 0
			}

			updatedStats, err := bson.Marshal(stats)
			if err != nil {
				if s.Logger != nil {
					s.Logger.Errorw("delete_documents_stats_marshal_error", "collection", collection_name, "database", s.Name, "error", err)
				}
				return Serialization_Err(Wrap_Err(err, "failed to marshal updated stats"))
			}

			if err := kv.PutBinary(STATS, []byte(collectionDefKey), updatedStats); err != nil {
				if s.Logger != nil {
					s.Logger.Errorw("delete_documents_stats_put_error", "collection", collection_name, "database", s.Name, "error", err)
				}
				return Storage_Err(Wrap_Err(err, "failed to update stats"))
			}
		}
	}

	if s.Logger != nil {
		duration := time.Since(start)
		s.Logger.Infow("delete_documents_complete", "collection", collection_name, "database", s.Name, "deleted_count", deletedCount, "duration_ms", duration.Milliseconds())
	}

	return nil
}

func InitTablesHelper(wtService wt.WTService) error {
	tables := map[string]string{
		CATALOG:                            "key_format=u,value_format=u,exclusive=true",
		STATS:                              "key_format=u,value_format=u,exclusive=true",
		LABELS_TO_DOC_ID_MAPPING_TABLE_URI: "key_format=S,value_format=S,exclusive=true",
	}

	for tableURI, config := range tables {
		exists, err := wtService.TableExists(tableURI)
		if err != nil {
			return fmt.Errorf("failed to check if table exists: %v", err)
		}

		if !exists {
			if err := wtService.CreateTable(tableURI, config); err != nil {
				return fmt.Errorf("failed to create table: %v", err)
			}
		}
	}

	return nil
}
