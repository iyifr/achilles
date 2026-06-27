package dbservice

import (
	"achillesdb/pkgs/faiss"
	wt "achillesdb/pkgs/wiredtiger"
	"errors"
	"fmt"
	"os"
	"strconv"

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
	NextInternalId    int64   `bson:"next_internal_id" json:"next_internal_id"`
}

type GDBService struct {
	Name      string
	KvService wt.WTService
	Logger    *zap.SugaredLogger
}

func (s *GDBService) databaseExists() (bool, error) {
	_, exists, err := s.KvService.GetBinaryWithStringKey(CATALOG, fmt.Sprintf("db:%s", s.Name))
	return exists, err
}

func (s *GDBService) CreateDB() error {

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

		s.Logger.Errorw("create_db_marshal_error", "db_name", s.Name, "error", err)

		return Serialization_Err(Wrap_Err(err, "failed to marshal catalog entry"))
	}

	dbRecordKey := fmt.Sprintf("db:%s", s.Name)
	_, exists, _ := s.KvService.GetBinaryWithStringKey(CATALOG, dbRecordKey)

	if exists {

		s.Logger.Warnw("db_already_exists", "db_name", s.Name)

		return AlreadyExists_Err(ErrDatabaseExists)
	}

	err = s.KvService.PutBinaryWithStringKey(CATALOG, dbRecordKey, doc)
	if err != nil {

		s.Logger.Errorw("create_db_storage_error", "db_name", s.Name, "error", err)

		return Storage_Err(Wrap_Err(err, "failed to write db catalog entry"))
	}
	return nil
}

func (s *GDBService) GetDbs() (GetDBsPayload, error) {
	start := time.Now()

	s.Logger.Infow("list_databases_start")

	startKey := []byte("db:")
	endKey := []byte("db;")

	cursor, err := s.KvService.ScanRangeBinary(CATALOG, startKey, endKey)
	if err != nil {
		s.Logger.Errorw("list_databases_scan_error", "error", err)
		return GetDBsPayload{}, Storage_Err(Wrap_Err(err, "failed to scan catalog for databases"))
	}
	defer cursor.Close()

	var databases []DatabaseInfo

	for cursor.Next() {
		_, value, err := cursor.Current()
		if err != nil {

			s.Logger.Errorw("list_databases_cursor_current_error", "error", err)

			return GetDBsPayload{}, Storage_Err(Wrap_Err(err, "failed to get current database"))
		}

		var dbEntry DbCatalogEntry
		if err := bson.Unmarshal(value, &dbEntry); err != nil {

			s.Logger.Errorw("list_databases_unmarshal_error", "error", err)

			return GetDBsPayload{}, Serialization_Err(Wrap_Err(err, "failed to unmarshal database catalog entry"))
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

		s.Logger.Errorw("list_databases_cursor_error", "error", err)

		return GetDBsPayload{}, Storage_Err(Wrap_Err(err, "cursor error during iteration"))
	}

	duration := time.Since(start)
	s.Logger.Infow("list_databases_complete", "database_count", "duration_ms", duration.Milliseconds())

	return GetDBsPayload{
		Databases: databases,
	}, nil
}

func (s *GDBService) DeleteDB(name string) error {
	start := time.Now()

	s.Logger.Infow("Deleting db:", "db_name", name)

	if name == "" {
		return InvalidInput_Err(ErrEmptyName)
	}

	kv := s.KvService

	// Check if database exists
	dbKey := fmt.Sprintf("db:%s", name)
	_, exists, err := kv.GetBinaryWithStringKey(CATALOG, dbKey)
	if err != nil {

		s.Logger.Errorw("delete_db_check_error", "db_name", name, "error", err)

		return Storage_Err(Wrap_Err(err, "failed to check if database exists"))
	}
	if !exists {

		s.Logger.Warnw("delete_db_not_found", "db_name", name)

		return NotFound_Err(ErrDatabaseNotFound)
	}

	// Get all collections for this database and delete them
	startKey := []byte(name + ".")
	endKey := []byte(name + "/")

	cursor, err := kv.ScanRangeBinary(CATALOG, startKey, endKey)
	if err != nil {

		s.Logger.Errorw("delete_db_scan_error", "db_name", name, "error", err)

		return Storage_Err(Wrap_Err(err, "failed to scan catalog for collections"))
	}

	var collectionsToDelete []CollectionCatalogEntry
	for cursor.Next() {
		_, value, err := cursor.Current()
		if err != nil {
			cursor.Close()

			s.Logger.Errorw("delete_db_cursor_current_error", "db_name", name, "error", err)

			return Storage_Err(Wrap_Err(err, "failed to get current collection"))
		}
		var collection CollectionCatalogEntry
		if err := bson.Unmarshal(value, &collection); err != nil {
			cursor.Close()

			s.Logger.Errorw("delete_db_unmarshal_error", "db_name", name, "error", err)

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
				s.Logger.Errorw("delete_db_drop_table_error", "db_name", name, "table_uri", collection.TableUri, "error", err)
				return Storage_Err(Wrap_Err(err, "failed to drop collection table %s", collection.TableUri))
			}
		}

		// Delete vector index file
		if collection.VectorIndexUri != "" {
			os.Remove(collection.VectorIndexUri)
		}

		if err := kv.DeleteBinary(CATALOG, []byte(collection.Ns)); err != nil {
			s.Logger.Errorw("delete_db_delete_catalog_error", "db_name", name, "ns", collection.Ns, "error", err)
			return Storage_Err(Wrap_Err(err, "failed to delete collection catalog entry"))
		}

		// Delete stats entry
		if err := kv.DeleteBinary(STATS, []byte(collection.Ns)); err != nil {
			s.Logger.Errorw("delete_db_delete_stats_error", "db_name", name, "ns", collection.Ns, "error", err)
			return Storage_Err(Wrap_Err(err, "failed to delete collection catalog entry"))
		}
	}

	// Delete the database entry from catalog
	if err := kv.DeleteBinaryWithStringKey(CATALOG, dbKey); err != nil {

		s.Logger.Errorw("delete_db_delete_entry_error", "db_name", name, "error", err)

		return Storage_Err(Wrap_Err(err, "failed to delete database catalog entry"))
	}

	duration := time.Since(start)
	s.Logger.Infow("delete_db_complete", "db_name", name, "collections_deleted", len(collectionsToDelete), "duration_ms", duration.Milliseconds())

	return nil
}

func (s *GDBService) CreateCollection(collection_name string) error {
	start := time.Now()

	s.Logger.Infow("create_collection_start", "collection", collection_name, "database", s.Name)

	if len(collection_name) == 0 {
		return InvalidInput_Err(ErrEmptyName)
	}

	if dbExists, err := s.databaseExists(); err != nil {
		return Storage_Err(Wrap_Err(err, "failed to check if database exists"))
	} else if !dbExists {
		return NotFound_Err(ErrDatabaseNotFound)
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

		s.Logger.Errorw("create_collection_check_error", "collection", collection_name, "database", s.Name, "error", err)

		return Storage_Err(Wrap_Err(err, "failed to check if table exists %s", collectionTableUri))
	}

	if exists {

		s.Logger.Warnw("create_collection_already_exists", "collection", collection_name, "database", s.Name)

		return AlreadyExists_Err(ErrCollectionExists)
	}

	err = s.KvService.CreateTable(collectionTableUri, "key_format=u,value_format=u")
	if err != nil {

		s.Logger.Errorw("create_collection_table_error", "collection", collection_name, "database", s.Name, "table_uri", collectionTableUri, "error", err)

		return Storage_Err(Wrap_Err(err, "failed to create table %s", collectionTableUri))
	}

	doc, err := bson.Marshal(catalogEntry)
	if err != nil {

		s.Logger.Errorw("create_collection_marshal_error", "collection", collection_name, "database", s.Name, "error", err)

		return Serialization_Err(Wrap_Err(err, "failed to encode catalog entry"))
	}

	err = s.KvService.PutBinaryWithStringKey(CATALOG, collectionKey, doc)
	if err != nil {

		s.Logger.Errorw("create_collection_catalog_error", "collection", collection_name, "database", s.Name, "error", err)

		return Storage_Err(Wrap_Err(err, "failed to write catalog entry"))
	}

	statsEntry := CollectionStats{
		Doc_Count:         0,
		Vector_Index_Size: 0,
	}

	stats_doc, err := bson.Marshal(statsEntry)
	if err != nil {

		s.Logger.Errorw("create_collection_stats_marshal_error", "collection", collection_name, "database", s.Name, "error", err)

		return Serialization_Err(Wrap_Err(err, "failed to encode stats entry"))
	}

	err = s.KvService.PutBinaryWithStringKey(STATS, collectionKey, stats_doc)
	if err != nil {

		s.Logger.Errorw("create_collection_stats_error", "collection", collection_name, "database", s.Name, "error", err)

		return Storage_Err(Wrap_Err(err, "failed to write stats entry"))
	}

	duration := time.Since(start)
	s.Logger.Infow("create_collection_complete", "collection", collection_name, "database", s.Name, "duration_ms", duration.Milliseconds())

	return nil
}

func (s *GDBService) DeleteCollection(collection_name string) error {
	start := time.Now()

	s.Logger.Infow("delete_collection_start", "collection", collection_name, "database", s.Name)

	if len(collection_name) == 0 {
		return InvalidInput_Err(ErrEmptyName)
	}

	if dbExists, err := s.databaseExists(); err != nil {
		return Storage_Err(Wrap_Err(err, "failed to check if database exists"))
	} else if !dbExists {
		return NotFound_Err(ErrDatabaseNotFound)
	}

	kv := s.KvService
	collectionDefKey := fmt.Sprintf("%s.%s", s.Name, collection_name)

	// Get collection info
	val, exists, err := kv.GetBinary(CATALOG, []byte(collectionDefKey))
	if err != nil {

		s.Logger.Errorw("delete_collection_get_error", "collection", collection_name, "database", s.Name, "error", err)

		return Storage_Err(Wrap_Err(err, "failed to get collection catalog"))
	}
	if !exists {

		s.Logger.Warnw("collection not found for delete", "collection", collection_name, "database", s.Name)

		return NotFound_Err(ErrCollectionNotFound)
	}

	var collection CollectionCatalogEntry
	if err := bson.Unmarshal(val, &collection); err != nil {
		s.Logger.Errorw("delete_collection_unmarshal_error", "collection", collection_name, "database", s.Name, "error", err)
		return Serialization_Err(Wrap_Err(err, "failed to unmarshal collection catalog"))
	}

	// Drop the WiredTiger table
	if err := kv.DeleteTable(collection.TableUri); err != nil {
		if !IsNotFoundError(err) && !IsBusyError(err) {
			s.Logger.Errorw("delete_collection_drop_table_error", "collection", collection_name, "database", s.Name, "table_uri", collection.TableUri, "error", err)
			return Storage_Err(Wrap_Err(err, "failed to drop collection table %s", collection.TableUri))
		}
	}

	// Evict the FAISS index from the cache
	if collection.VectorIndexUri != "" {
		faiss.GlobalIndexCache().Remove(collection.VectorIndexUri)
		os.Remove(collection.VectorIndexUri)
	}

	// Delete collection from catalog
	if err := kv.DeleteBinary(CATALOG, []byte(collectionDefKey)); err != nil {

		s.Logger.Errorw("delete_collection_catalog_error", "collection", collection_name, "database", s.Name, "error", err)

		return Storage_Err(Wrap_Err(err, "failed to delete collection catalog entry"))
	}

	// Delete stats entry
	kv.DeleteBinary(STATS, []byte(collectionDefKey))

	duration := time.Since(start)
	s.Logger.Infow("delete_collection_complete", "collection", collection_name, "database", s.Name, "duration_ms", duration.Milliseconds())

	return nil
}

func (s *GDBService) ListCollections() ([]CollectionCatalogEntry, error) {
	start := time.Now()

	s.Logger.Infow("list_collections_start", "database", s.Name)

	if s.Name == "" {
		return nil, InvalidInput_Err(ErrEmptyName)
	}

	if dbExists, err := s.databaseExists(); err != nil {
		return nil, Storage_Err(Wrap_Err(err, "failed to check if database exists"))
	} else if !dbExists {
		return nil, NotFound_Err(ErrDatabaseNotFound)
	}

	startKey := []byte(s.Name + ".")
	endKey := []byte(s.Name + "/")

	cursor, err := s.KvService.ScanRangeBinary(CATALOG, startKey, endKey)
	if err != nil {

		s.Logger.Errorw("list_collections_scan_error", "database", s.Name, "error", err)

		return nil, Storage_Err(Wrap_Err(err, "failed to scan catalog"))
	}
	defer cursor.Close()

	var collections []CollectionCatalogEntry
	for cursor.Next() {
		_, value, err := cursor.Current()
		if err != nil {

			s.Logger.Errorw("list_collections_cursor_current_error", "database", s.Name, "error", err)

			return nil, Storage_Err(Wrap_Err(err, "failed to get current"))
		}
		collectionValue := CollectionCatalogEntry{}
		if err := bson.Unmarshal(value, &collectionValue); err != nil {

			s.Logger.Errorw("list_collections_unmarshal_error", "database", s.Name, "error", err)

			return nil, Serialization_Err(Wrap_Err(err, "failed to unmarshal collection catalog"))
		}
		collections = append(collections, collectionValue)
	}

	if err := cursor.Err(); err != nil {

		s.Logger.Errorw("list_collections_cursor_error", "database", s.Name, "error", err)

		return nil, Storage_Err(Wrap_Err(err, "cursor error during iteration"))
	}

	duration := time.Since(start)
	s.Logger.Infow("list_collections_complete", "database", s.Name, "collection_count", len(collections), "duration_ms", duration.Milliseconds())

	return collections, nil
}

func (s *GDBService) GetCollection(collection_name string) (CollectionEntry, error) {
	start := time.Now()

	s.Logger.Infow("get_collection_start", "collection", collection_name, "database", s.Name)

	if s.Name == "" {
		return CollectionEntry{}, InvalidInput_Err(ErrEmptyName)
	}

	if dbExists, err := s.databaseExists(); err != nil {
		return CollectionEntry{}, Storage_Err(Wrap_Err(err, "failed to check if database exists"))
	} else if !dbExists {
		return CollectionEntry{}, NotFound_Err(ErrDatabaseNotFound)
	}

	collectionDefKey := fmt.Sprintf("%s.%s", s.Name, collection_name)
	val, exists, err := s.KvService.GetBinary(CATALOG, []byte(collectionDefKey))

	if err != nil {

		s.Logger.Errorw("get_collection_catalog_error", "collection", collection_name, "database", s.Name, "error", err)

		return CollectionEntry{}, Storage_Err(Wrap_Err(err, "failed to get collection catalog"))
	}

	if !exists {

		s.Logger.Warnw("get_collection_not_found", "collection", collection_name, "database", s.Name)

		return CollectionEntry{}, NotFound_Err(ErrCollectionNotFound)
	}

	var collection CollectionCatalogEntry
	if err := bson.Unmarshal(val, &collection); err != nil {

		s.Logger.Errorw("get_collection_unmarshal_catalog_error", "collection", collection_name, "database", s.Name, "error", err)

		return CollectionEntry{}, Serialization_Err(Wrap_Err(err, "failed to unmarshal collection catalog"))
	}

	statsVal, statsExists, statsErr := s.KvService.GetBinary(STATS, []byte(collectionDefKey))
	var stats CollectionStats
	if statsErr == nil && statsExists {
		if err := bson.Unmarshal(statsVal, &stats); err != nil {

			s.Logger.Errorw("get_collection_unmarshal_stats_error", "collection", collection_name, "database", s.Name, "error", err)

			return CollectionEntry{}, Serialization_Err(Wrap_Err(err, "failed to unmarshal collection stats"))
		}
	}

	documents := make([]GlowstickDocument, 0)

	stats.Vector_Index_Size = float64(stats.Vector_Index_Size)

	duration := time.Since(start)
	s.Logger.Infow("get_collection_complete", "collection", collection_name, "database", s.Name, "duration_ms", duration.Milliseconds())

	return CollectionEntry{
		Info:      collection,
		Documents: documents,
		Stats:     stats,
	}, nil
}

func (s *GDBService) InsertDocuments(collection_name string, documents *GlowstickDocumentSOA) error {
	start := time.Now()

	s.Logger.Infow("insert_documents_start", "collection", collection_name, "doc_count", documents.DocumentCount())

	// Validate input structure
	if err := documents.Validate(); err != nil {
		return InvalidInput_Err(Wrap_Err(err, "invalid SOA document structure"))
	}

	if dupes := documents.FindDuplicateIds(); len(dupes) > 0 {
		return InvalidInput_Err(Wrap_Err(ErrDuplicateDocumentIds, "duplicate ids in insert batch: %v", dupes))
	}

	if dbExists, err := s.databaseExists(); err != nil {
		return Storage_Err(Wrap_Err(err, "failed to check if database exists"))
	} else if !dbExists {
		return NotFound_Err(ErrDatabaseNotFound)
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

	// Fetch stats up front: NextInternalId is needed to mint ids for new
	// documents before any FAISS/WiredTiger writes happen.
	statsVal, statsExists, err := kv.GetBinary(STATS, []byte(collectionDefKey))
	if err != nil {
		return Storage_Err(Wrap_Err(err, "failed to fetch hot stats"))
	}
	if !statsExists {
		return Storage_Err(fmt.Errorf("collection stats not found for %s", collectionDefKey))
	}
	var stats CollectionStats
	if err := bson.Unmarshal(statsVal, &stats); err != nil {
		return Serialization_Err(Wrap_Err(err, "failed to unmarshal hot stats"))
	}

	vectorIndexFilePath := collection.VectorIndexUri
	destTableURI := collection.TableUri

	// Resolve any existing alias entries for these ids in one batched read.
	aliasReader, err := kv.NewBatchReader(DOC_ID_ALIAS_TABLE_URI, wt.VALUE_FORMAT_STRING)
	if err != nil {
		return Storage_Err(Wrap_Err(err, "failed to open alias batch reader"))
	}
	existingInternalId := make(map[string]int64, numDocs)
	for _, id := range documents.Ids {
		aliasVal, found, err := aliasReader.GetString(id)
		if err != nil {
			aliasReader.Close()
			return Storage_Err(Wrap_Err(err, "failed to look up alias for id %s", id))
		}
		if !found {
			continue
		}
		internalId, parseErr := strconv.ParseInt(aliasVal, 10, 64)
		if parseErr != nil {
			aliasReader.Close()
			return Serialization_Err(Wrap_Err(parseErr, "corrupt alias entry for id %s", id))
		}
		existingInternalId[id] = internalId
	}
	aliasReader.Close()

	if !documents.Upsert && len(existingInternalId) > 0 {
		conflicts := make([]string, 0, len(existingInternalId))
		for id := range existingInternalId {
			conflicts = append(conflicts, id)
		}
		return AlreadyExists_Err(Wrap_Err(ErrDocumentExists, "ids already exist in collection: %v", conflicts))
	}

	// Assign an internal id to every document: reuse the existing one for
	// upserts (so the alias mapping doesn't need to change), mint a fresh,
	// never-reused one for genuinely new ids.
	internalIds := make([]int64, numDocs)
	var staleIds []int64
	newDocCount := 0
	for i, id := range documents.Ids {
		if existingId, ok := existingInternalId[id]; ok {
			internalIds[i] = existingId
			staleIds = append(staleIds, existingId)
		} else {
			internalIds[i] = stats.NextInternalId
			stats.NextInternalId++
			newDocCount++
		}
	}

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

	// Upserts: remove the stale vector before adding its replacement, in
	// one batched native call covering the whole request.
	if len(staleIds) > 0 {
		if _, err := idx.RemoveIds(staleIds); err != nil {
			return Internal_Err(Wrap_Err(err, "failed to remove stale vectors for upsert"))
		}
	}

	// Use batch writer for document inserts (single session for all writes).
	docWriter, err := s.KvService.NewBatchWriter(destTableURI, wt.VALUE_FORMAT_BINARY)
	if err != nil {
		return Storage_Err(Wrap_Err(err, "failed to create batch writer for documents"))
	}
	defer docWriter.Close()

	for i := range numDocs {
		doc := GlowstickDocument{
			Id:        documents.Ids[i],
			Content:   documents.Contents[i],
			Embedding: nil, // Don't store embedding in BSON (already in FAISS)
			Metadata:  documents.Metadatas[i],
		}

		doc_bytes, release, err := BsonMarshalWithPool(doc)
		if err != nil {
			release()
			return Serialization_Err(Wrap_Err(err, "failed to marshal document %s", doc.Id))
		}

		key := encodeInternalId(internalIds[i])
		if err := docWriter.PutBinary(key, doc_bytes); err != nil {
			release()
			return Storage_Err(Wrap_Err(err, "failed to insert document %s at index %d", doc.Id, i))
		}
		release()
	}

	if err := docWriter.Commit(); err != nil {
		return Storage_Err(Wrap_Err(err, "failed to commit document batch"))
	}

	if err := idx.AddWithIds(documents.Embeddings, internalIds, numDocs); err != nil {
		return Internal_Err(Wrap_Err(err, "failed to add embeddings to index"))
	}

	// Write alias entries only for genuinely new ids -- upserted ids keep
	// their existing alias, since the internal id didn't change.
	if newDocCount > 0 {
		aliasWriter, err := s.KvService.NewBatchWriter(DOC_ID_ALIAS_TABLE_URI, wt.VALUE_FORMAT_STRING)
		if err != nil {
			return Storage_Err(Wrap_Err(err, "failed to create batch writer for alias table"))
		}
		defer aliasWriter.Close()

		for i := range numDocs {
			if _, wasUpsert := existingInternalId[documents.Ids[i]]; wasUpsert {
				continue
			}
			if err := aliasWriter.PutString(documents.Ids[i], strconv.FormatInt(internalIds[i], 10)); err != nil {
				return Storage_Err(Wrap_Err(err, "failed to write alias entry for id %s", documents.Ids[i]))
			}
		}

		if err := aliasWriter.Commit(); err != nil {
			return Storage_Err(Wrap_Err(err, "failed to commit alias batch"))
		}
	}

	// Write index to disk
	if err := idx.WriteToFile(vectorIndexFilePath); err != nil {
		return Storage_Err(Wrap_Err(err, "failed to write index to file"))
	}
	cachedIdx.Dirty = false

	info, err := os.Stat(vectorIndexFilePath)
	if err != nil {
		return Storage_Err(Wrap_Err(err, "failed to stat vector index file"))
	}

	stats.Doc_Count += newDocCount
	stats.Vector_Index_Size = float64(info.Size())

	statsBytes, err := bson.Marshal(stats)
	if err != nil {
		return Serialization_Err(Wrap_Err(err, "failed to marshal hot stats"))
	}

	if err := kv.PutBinary(STATS, []byte(collectionDefKey), statsBytes); err != nil {
		return Storage_Err(Wrap_Err(err, "failed to write hot stats"))
	}
	duration := time.Since(start)
	s.Logger.Infow("insert_documents_complete", "collection", collection_name, "doc_count", numDocs, "new_count", newDocCount, "upsert_count", len(staleIds), "duration_ms", duration.Milliseconds())

	return nil
}

func (s *GDBService) QueryCollection(collection_name string, query QueryStruct) ([]GlowstickQueryResultSet, error) {
	start := time.Now()

	s.Logger.Infow("query_collection_start", "collection", collection_name, "top_k", query.TopK)

	if dbExists, err := s.databaseExists(); err != nil {
		return nil, Storage_Err(Wrap_Err(err, "failed to check if database exists"))
	} else if !dbExists {
		return nil, NotFound_Err(ErrDatabaseNotFound)
	}

	kv := s.KvService
	collectionDefKey := s.Name + "." + collection_name

	// Get collection catalog
	val, exists, err := kv.GetBinary(CATALOG, []byte(collectionDefKey))
	if err != nil {

		s.Logger.Errorw("query_collection_catalog_error", "collection", collection_name, "error", err)

		return nil, Storage_Err(Wrap_Err(err, "failed to get collection catalog"))
	}

	if !exists {

		s.Logger.Warnw("query_collection_not_found", "collection", collection_name)

		return nil, NotFound_Err(ErrCollectionNotFound)
	}

	var collection CollectionCatalogEntry
	if err := bson.Unmarshal(val, &collection); err != nil {

		s.Logger.Errorw("query_collection_unmarshal_error", "collection", collection_name, "error", err)

		return nil, Serialization_Err(Wrap_Err(err, "failed to unmarshal collection catalog"))
	}

	// Get cached index
	vectorIndexFilePath := collection.VectorIndexUri
	indexCache := faiss.GlobalIndexCache()
	cachedIdx, err := indexCache.GetOrCreate(vectorIndexFilePath, len(query.QueryEmbedding))
	if err != nil {

		s.Logger.Errorw("query_collection_index_cache_error", "collection", collection_name, "error", err)

		return nil, Internal_Err(Wrap_Err(err, "failed to get vector index from cache"))
	}

	cachedIdx.Lock()
	defer cachedIdx.Unlock()

	idx := cachedIdx.Index

	// Search vector index. Returned ids are internal document ids -- the
	// same ids used as the document table's own keys -- so a hit can be
	// read back directly, with no separate label translation step.
	distances, ids, err := idx.Search(query.QueryEmbedding, 1, int(query.TopK))
	if err != nil {

		s.Logger.Errorw("query_collection_search_error", "collection", collection_name, "error", err)

		return nil, Internal_Err(Wrap_Err(err, "failed to search vector index"))
	}

	docs := make([]GlowstickQueryResultSet, 0, query.TopK)
	if len(ids) == 0 {
		return docs, nil
	}

	// Batch-read all matched documents (single session/cursor). FAISS pads
	// short result sets with -1, which never resolves to a document.
	docReader, err := kv.NewBatchReader(collection.TableUri, wt.VALUE_FORMAT_BINARY)
	if err != nil {
		return nil, Storage_Err(Wrap_Err(err, "failed to open document batch reader"))
	}
	defer docReader.Close()

	for i, id := range ids {
		if id < 0 {
			continue
		}
		distance := distances[i]

		// EncondeInternalId always returns the same ID so no need for lookup in cache.
		docBin, exists, err := docReader.GetBinary(encodeInternalId(id))
		if err != nil {
			return nil, Storage_Err(Wrap_Err(err, "failed to get document for internal id %d", id))
		}
		if !exists || len(docBin) == 0 {
			continue
		}

		doc, err := unmarshalQueryDoc(docBin)
		if err != nil {
			return nil, Serialization_Err(Wrap_Err(err, "failed to unmarshal document for internal id %d", id))
		}

		if query.MaxDistance != 0 && distance >= query.MaxDistance {
			continue
		}

		if query.Filters != nil {
			matches, err := matchesFilter(doc.Metadata, query.Filters)
			if err != nil || !matches {
				continue
			}
		}

		docs = append(docs, GlowstickQueryResultSet{
			Id:       doc.Id,
			Content:  doc.Content,
			Metadata: doc.Metadata,
			Distance: distance,
		})
	}

	duration := time.Since(start)
	s.Logger.Infow("query_collection_complete", "collection", collection_name, "results", len(docs), "duration_ms", duration.Milliseconds())

	return docs, nil
}

func (s *GDBService) GetDocuments(collection_name string) ([]GlowstickDocument, error) {
	start := time.Now()

	s.Logger.Infow("get_documents_start", "collection", collection_name, "database", s.Name)

	if dbExists, err := s.databaseExists(); err != nil {
		return nil, Storage_Err(Wrap_Err(err, "failed to check if database exists"))
	} else if !dbExists {
		return nil, NotFound_Err(ErrDatabaseNotFound)
	}

	kv := s.KvService
	collectionDefKey := fmt.Sprintf("%s.%s", s.Name, collection_name)
	val, exists, err := kv.GetBinary(CATALOG, []byte(collectionDefKey))
	if err != nil {

		s.Logger.Errorw("get_documents_catalog_error", "collection", collection_name, "error", err)

		return nil, Storage_Err(Wrap_Err(err, "failed to get collection catalog"))
	}
	if !exists {

		s.Logger.Warnw("get_documents_collection_not_found", "collection", collection_name)

		return nil, NotFound_Err(ErrCollectionNotFound)
	}
	var collection CollectionCatalogEntry
	if err := bson.Unmarshal(val, &collection); err != nil {

		s.Logger.Errorw("get_documents_unmarshal_catalog_error", "collection", collection_name, "error", err)

		return nil, Serialization_Err(Wrap_Err(err, "failed to unmarshal collection catalog"))
	}

	cursor, err := kv.ScanRangeBinary(collection.TableUri, minInternalIdKey(), maxInternalIdKey())
	if err != nil {

		s.Logger.Errorw("get_documents_scan_error", "collection", collection_name, "table_uri", collection.TableUri, "error", err)

		return nil, Storage_Err(Wrap_Err(err, "failed to scan collection table"))
	}
	defer cursor.Close()

	var docs []GlowstickDocument
	for cursor.Next() {
		_, value, err := cursor.Current()
		if err != nil {

			s.Logger.Errorw("get_documents_cursor_current_error", "collection", collection_name, "error", err)

			return nil, Storage_Err(Wrap_Err(err, "failed to get current document"))
		}

		var doc GlowstickDocument
		if err := bson.Unmarshal(value, &doc); err != nil {

			s.Logger.Errorw("get_documents_unmarshal_doc_error", "collection", collection_name, "error", err)

			return nil, Serialization_Err(Wrap_Err(err, "failed to unmarshal document"))
		}
		docs = append(docs, doc)
	}

	if err := cursor.Err(); err != nil {

		s.Logger.Errorw("get_documents_cursor_error", "collection", collection_name, "error", err)

		return nil, Storage_Err(Wrap_Err(err, "cursor error during iteration"))
	}

	duration := time.Since(start)
	s.Logger.Infow("get_documents_complete", "collection", collection_name, "document_count", len(docs), "duration_ms", duration.Milliseconds())

	return docs, nil
}

func (s *GDBService) UpdateDocuments(collection_name string, payload *DocUpdatePayload) (int, error) {
	start := time.Now()
	hasID := len(payload.DocumentId) > 0
	hasWhereClause := len(payload.Where) > 0

	if hasID && hasWhereClause {
		return 0, InvalidInput_Err(errors.New("specify either document_id or where, not both"))
	}
	if !hasID && !hasWhereClause {
		return 0, InvalidInput_Err(errors.New("specify document_id for a single update or where for a bulk update"))
	}

	if hasID {
		s.Logger.Infow("update_documents_start", "collection", collection_name, "database", s.Name, "document_id", payload.DocumentId)
	} else {
		s.Logger.Infow("update_documents_bulk_start", "collection", collection_name, "database", s.Name)
	}

	if dbExists, err := s.databaseExists(); err != nil {
		return 0, Storage_Err(Wrap_Err(err, "failed to check if database exists"))
	} else if !dbExists {
		return 0, NotFound_Err(ErrDatabaseNotFound)
	}

	kv := s.KvService
	collectionDefKey := fmt.Sprintf("%s.%s", s.Name, collection_name)
	val, exists, err := kv.GetBinary(CATALOG, []byte(collectionDefKey))

	if err != nil {

		s.Logger.Errorw("update_documents_catalog_error", "collection", collection_name, "database", s.Name, "error", err)

		return 0, Storage_Err(Wrap_Err(err, "failed to get collection catalog"))
	}
	if !exists {

		s.Logger.Warnw("update_documents_collection_not_found", "collection", collection_name, "database", s.Name)

		return 0, NotFound_Err(ErrCollectionNotFound)
	}
	var collection CollectionCatalogEntry
	if err := bson.Unmarshal(val, &collection); err != nil {

		s.Logger.Errorw("update_documents_unmarshal_catalog_error", "collection", collection_name, "database", s.Name, "error", err)

		return 0, Serialization_Err(Wrap_Err(err, "failed to unmarshal collection catalog"))
	}

	if hasWhereClause {
		n, err := s.updateDocumentsMatchingFilter(collection, collection_name, payload.Where, payload.Updates)
		if err != nil {
			return 0, err
		}

		duration := time.Since(start)
		s.Logger.Infow("update_documents_bulk_complete", "collection", collection_name, "database", s.Name, "updated_count", n, "duration_ms", duration.Milliseconds())

		return n, nil
	}

	aliasVal, aliasExists, aliasErr := kv.GetString(DOC_ID_ALIAS_TABLE_URI, payload.DocumentId)
	if aliasErr != nil {

		s.Logger.Errorw("update_documents_alias_lookup_error", "collection", collection_name, "database", s.Name, "document_id", payload.DocumentId, "error", aliasErr)

		return 0, Storage_Err(Wrap_Err(aliasErr, "failed to look up alias for id %s", payload.DocumentId))
	}
	if !aliasExists {

		s.Logger.Warnw("update_documents_document_not_found", "collection", collection_name, "database", s.Name, "document_id", payload.DocumentId)

		return 0, NotFound_Err(ErrDocumentNotFound)
	}
	internalId, parseErr := strconv.ParseInt(aliasVal, 10, 64)
	if parseErr != nil {
		return 0, Serialization_Err(Wrap_Err(parseErr, "corrupt alias entry for id %s", payload.DocumentId))
	}
	docKey := encodeInternalId(internalId)

	doc_raw, doc_exists, doc_get_err := kv.GetBinary(collection.TableUri, docKey)

	if !doc_exists {

		s.Logger.Warnw("update_documents_document_not_found", "collection", collection_name, "database", s.Name, "document_id", payload.DocumentId)

		return 0, NotFound_Err(ErrDocumentNotFound)
	}

	if doc_get_err != nil {

		s.Logger.Errorw("update_documents_get_error", "collection", collection_name, "database", s.Name, "document_id", payload.DocumentId, "error", doc_get_err)

		return 0, Storage_Err(Wrap_Err(doc_get_err, "failed to get document"))
	}

	var doc GlowstickDocument
	if err := bson.Unmarshal(doc_raw, &doc); err != nil {

		s.Logger.Errorw("update_documents_unmarshal_doc_error", "collection", collection_name, "database", s.Name, "document_id", payload.DocumentId, "error", err)

		return 0, Serialization_Err(Wrap_Err(err, "failed to unmarshal document"))
	}
	for key, value := range payload.Updates {
		if doc.Metadata == nil {
			doc.Metadata = make(map[string]any)
		}
		doc.Metadata[key] = value
	}
	updated_val, err := bson.Marshal(doc)
	if err != nil {

		s.Logger.Errorw("update_documents_marshal_error", "collection", collection_name, "database", s.Name, "document_id", payload.DocumentId, "error", err)

		return 0, Serialization_Err(Wrap_Err(err, "failed to marshal updated document"))
	}

	if err := kv.PutBinary(collection.TableUri, docKey, updated_val); err != nil {

		s.Logger.Errorw("update_documents_put_error", "collection", collection_name, "database", s.Name, "document_id", payload.DocumentId, "error", err)

		return 0, Storage_Err(Wrap_Err(err, "failed to update document"))
	}

	duration := time.Since(start)
	s.Logger.Infow("update_documents_complete", "collection", collection_name, "database", s.Name, "document_id", payload.DocumentId, "duration_ms", duration.Milliseconds())

	return 1, nil
}

// updateDocumentsMatchingFilter applies metadata patches to every document whose metadata matches filter.
func (s *GDBService) updateDocumentsMatchingFilter(collection CollectionCatalogEntry, collection_name string, where map[string]any, updates map[string]any) (int, error) {
	kv := s.KvService
	cursor, err := kv.ScanRangeBinary(collection.TableUri, minInternalIdKey(), maxInternalIdKey())
	if err != nil {

		s.Logger.Errorw("update_documents_bulk_scan_error", "collection", collection_name, "table_uri", collection.TableUri, "error", err)

		return 0, Storage_Err(Wrap_Err(err, "failed to scan collection table"))
	}
	defer cursor.Close()

	updated := 0
	for cursor.Next() {
		key, value, err := cursor.Current()
		if err != nil {

			s.Logger.Errorw("update_documents_bulk_cursor_current_error", "collection", collection_name, "error", err)

			return 0, Storage_Err(Wrap_Err(err, "failed to get current document"))
		}

		var doc GlowstickDocument
		if err := bson.Unmarshal(value, &doc); err != nil {

			s.Logger.Errorw("update_documents_bulk_unmarshal_doc_error", "collection", collection_name, "error", err)

			return 0, Serialization_Err(Wrap_Err(err, "failed to unmarshal document"))
		}

		meta := doc.Metadata
		if meta == nil {
			meta = make(map[string]any)
		}
		match, err := matchesFilter(meta, where)
		if err != nil {
			return 0, InvalidInput_Err(err)
		}
		if !match {
			continue
		}

		if doc.Metadata == nil {
			doc.Metadata = make(map[string]any)
		}
		for key, val := range updates {
			doc.Metadata[key] = val
		}
		updatedVal, err := bson.Marshal(doc)
		if err != nil {

			s.Logger.Errorw("update_documents_bulk_marshal_error", "collection", collection_name, "document_id", doc.Id, "error", err)

			return 0, Serialization_Err(Wrap_Err(err, "failed to marshal updated document"))
		}
		if err := kv.PutBinary(collection.TableUri, key, updatedVal); err != nil {

			s.Logger.Errorw("update_documents_bulk_put_error", "collection", collection_name, "document_id", doc.Id, "error", err)

			return 0, Storage_Err(Wrap_Err(err, "failed to update document"))
		}
		updated++
	}

	if err := cursor.Err(); err != nil {

		s.Logger.Errorw("update_documents_bulk_cursor_error", "collection", collection_name, "error", err)

		return 0, Storage_Err(Wrap_Err(err, "cursor error during iteration"))
	}

	return updated, nil
}

func (s *GDBService) DeleteDocuments(collection_name string, documentIds []string) ([]string, error) {
	start := time.Now()

	s.Logger.Infow("delete_documents_start", "collection", collection_name, "database", s.Name, "doc_count", len(documentIds))

	if len(documentIds) == 0 {
		return nil, InvalidInput_Err(ErrEmptyDocuments)
	}

	if dbExists, err := s.databaseExists(); err != nil {
		return nil, Storage_Err(Wrap_Err(err, "failed to check if database exists"))
	} else if !dbExists {
		return nil, NotFound_Err(ErrDatabaseNotFound)
	}

	kv := s.KvService
	collectionDefKey := fmt.Sprintf("%s.%s", s.Name, collection_name)

	// Get collection info
	val, exists, err := kv.GetBinary(CATALOG, []byte(collectionDefKey))
	if err != nil {

		s.Logger.Errorw("delete_documents_catalog_error", "collection", collection_name, "database", s.Name, "error", err)

		return nil, Storage_Err(Wrap_Err(err, "failed to get collection catalog"))
	}
	if !exists {

		s.Logger.Warnw("delete_documents_collection_not_found", "collection", collection_name, "database", s.Name)

		return nil, NotFound_Err(ErrCollectionNotFound)
	}

	var collection CollectionCatalogEntry
	if err := bson.Unmarshal(val, &collection); err != nil {

		s.Logger.Errorw("delete_documents_unmarshal_catalog_error", "collection", collection_name, "database", s.Name, "error", err)

		return nil, Serialization_Err(Wrap_Err(err, "failed to unmarshal collection catalog"))
	}

	// Resolve each requested id to its internal id in one batched read.
	aliasReader, err := kv.NewBatchReader(DOC_ID_ALIAS_TABLE_URI, wt.VALUE_FORMAT_STRING)
	if err != nil {
		return nil, Storage_Err(Wrap_Err(err, "failed to open alias batch reader"))
	}
	type resolvedDoc struct {
		docId      string
		internalId int64
	}
	var toDelete []resolvedDoc
	for _, docId := range documentIds {
		aliasVal, found, err := aliasReader.GetString(docId)
		if err != nil {
			aliasReader.Close()

			s.Logger.Errorw("delete_documents_alias_lookup_error", "collection", collection_name, "database", s.Name, "document_id", docId, "error", err)

			return nil, Storage_Err(Wrap_Err(err, "failed to look up alias for id %s", docId))
		}
		if !found {
			continue
		}
		internalId, parseErr := strconv.ParseInt(aliasVal, 10, 64)
		if parseErr != nil {
			aliasReader.Close()
			return nil, Serialization_Err(Wrap_Err(parseErr, "corrupt alias entry for id %s", docId))
		}
		toDelete = append(toDelete, resolvedDoc{docId: docId, internalId: internalId})
	}
	aliasReader.Close()

	deletedIds := make([]string, 0, len(toDelete))
	if len(toDelete) > 0 {
		// Remove the vectors from FAISS in a single batched native call,
		// covering every id in this request.
		indexCache := faiss.GlobalIndexCache()
		cachedIdx, err := indexCache.GetOrCreate(collection.VectorIndexUri, 0)
		if err != nil {

			s.Logger.Errorw("delete_documents_index_cache_error", "collection", collection_name, "database", s.Name, "error", err)

			return nil, Internal_Err(Wrap_Err(err, "failed to get vector index"))
		}
		cachedIdx.Lock()

		internalIds := make([]int64, len(toDelete))
		for i, r := range toDelete {
			internalIds[i] = r.internalId
		}
		if _, err := cachedIdx.Index.RemoveIds(internalIds); err != nil {
			cachedIdx.Unlock()

			s.Logger.Errorw("delete_documents_remove_vectors_error", "collection", collection_name, "database", s.Name, "error", err)

			return nil, Internal_Err(Wrap_Err(err, "failed to remove vectors from index"))
		}
		if err := cachedIdx.Index.WriteToFile(collection.VectorIndexUri); err != nil {
			cachedIdx.Unlock()

			s.Logger.Errorw("delete_documents_write_index_error", "collection", collection_name, "database", s.Name, "error", err)

			return nil, Storage_Err(Wrap_Err(err, "failed to write index to file"))
		}
		cachedIdx.Dirty = false
		cachedIdx.Unlock()

		// Delete the document row and alias entry for each resolved id.
		for _, r := range toDelete {
			if err := kv.DeleteBinary(collection.TableUri, encodeInternalId(r.internalId)); err != nil {

				s.Logger.Errorw("delete_documents_delete_error", "collection", collection_name, "database", s.Name, "document_id", r.docId, "error", err)

				return nil, Storage_Err(Wrap_Err(err, "failed to delete document %s", r.docId))
			}
			if err := kv.DeleteString(DOC_ID_ALIAS_TABLE_URI, r.docId); err != nil {

				s.Logger.Errorw("delete_documents_delete_alias_error", "collection", collection_name, "database", s.Name, "document_id", r.docId, "error", err)

				return nil, Storage_Err(Wrap_Err(err, "failed to delete alias entry for %s", r.docId))
			}
			deletedIds = append(deletedIds, r.docId)
		}
	}
	deletedCount := len(deletedIds)

	// Update stats if any documents were deleted
	if deletedCount > 0 {
		statsVal, statsExists, err := kv.GetBinary(STATS, []byte(collectionDefKey))
		if err != nil {

			s.Logger.Errorw("delete_documents_stats_get_error", "collection", collection_name, "database", s.Name, "error", err)

			return nil, Storage_Err(Wrap_Err(err, "failed to get stats"))
		}
		if statsExists {
			var stats CollectionStats
			if err := bson.Unmarshal(statsVal, &stats); err != nil {

				s.Logger.Errorw("delete_documents_stats_unmarshal_error", "collection", collection_name, "database", s.Name, "error", err)

				return nil, Serialization_Err(Wrap_Err(err, "failed to unmarshal stats"))
			}

			stats.Doc_Count -= deletedCount
			if stats.Doc_Count < 0 {
				stats.Doc_Count = 0
			}

			updatedStats, err := bson.Marshal(stats)
			if err != nil {

				s.Logger.Errorw("delete_documents_stats_marshal_error", "collection", collection_name, "database", s.Name, "error", err)

				return nil, Serialization_Err(Wrap_Err(err, "failed to marshal updated stats"))
			}

			if err := kv.PutBinary(STATS, []byte(collectionDefKey), updatedStats); err != nil {

				s.Logger.Errorw("delete_documents_stats_put_error", "collection", collection_name, "database", s.Name, "error", err)

				return nil, Storage_Err(Wrap_Err(err, "failed to update stats"))
			}
		}
	}

	duration := time.Since(start)
	s.Logger.Infow("delete_documents_complete", "collection", collection_name, "database", s.Name, "deleted_count", deletedCount, "duration_ms", duration.Milliseconds())

	if deletedIds == nil {
		deletedIds = []string{}
	}

	return deletedIds, nil
}

func InitTables(wtService wt.WTService) error {
	tables := map[string]string{
		CATALOG:                "key_format=u,value_format=u,exclusive=true",
		STATS:                  "key_format=u,value_format=u,exclusive=true",
		DOC_ID_ALIAS_TABLE_URI: "key_format=S,value_format=S,exclusive=true",
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
