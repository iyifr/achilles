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
)

type DbCatalogEntry struct {
	UUID   string            `bson:"_uuid"`
	Name   string            `bson:"name"`
	Config map[string]string `bson:"config"`
}

type CollectionIndex struct {
	Id   string                 `bson:"_id"`
	Key  map[string]int         `bson:"key"`
	Name string                 `bson:"name"`
	Ns   string                 `bson:"ns"`
	Type string                 `bson:"type"`
	V    int                    `bson:"v"`
	Opts map[string]interface{} `bson:"opts,omitempty"`
}

type CollectionCatalogEntry struct {
	Id               primitive.ObjectID `bson:"_id" json:"_id"`
	Ns               string             `bson:"ns" json:"ns"`
	TableUri         string             `bson:"table_uri" json:"table_uri"`
	VectorIndexUri   string             `bson:"vector_index_uri" json:"vector_index_uri"`
	IndexTableUriMap map[string]string  `bson:"index_table_uri_map,omitempty" json:"index_table_uri_map,omitempty"`
	Indexes          []CollectionIndex  `bson:"indexes,omitempty" json:"indexes,omitempty"`
	CreatedAt        primitive.DateTime `bson:"createdAt" json:"createdAt"`
	UpdatedAt        primitive.DateTime `bson:"updatedAt" json:"updatedAt"`
}

type CollectionStats struct {
	Doc_Count         int
	Vector_Index_Size float64
}

type GDBService struct {
	Name      string
	KvService wt.WTService
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
		return Serialization_Err(Wrap_Err(err, "failed to marshal catalog entry"))
	}

	dbKey := fmt.Sprintf("db:%s", s.Name)
	_, exists, _ := s.KvService.GetBinaryWithStringKey(CATALOG, dbKey)

	if exists {
		return AlreadyExists_Err(ErrDatabaseExists)
	}

	err = s.KvService.PutBinaryWithStringKey(CATALOG, dbKey, doc)
	if err != nil {
		return Storage_Err(Wrap_Err(err, "failed to write db catalog entry"))
	}

	return nil
}

func (s *GDBService) DeleteDB(name string) error {
	if name == "" {
		return InvalidInput_Err(ErrEmptyName)
	}

	kv := s.KvService

	// Check if database exists
	dbKey := fmt.Sprintf("db:%s", name)
	_, exists, err := kv.GetBinaryWithStringKey(CATALOG, dbKey)
	if err != nil {
		return Storage_Err(Wrap_Err(err, "failed to check if database exists"))
	}
	if !exists {
		return NotFound_Err(ErrDatabaseNotFound)
	}

	// Get all collections for this database and delete them
	startKey := []byte(name + ".")
	endKey := []byte(name + "/")

	cursor, err := kv.ScanRangeBinary(CATALOG, startKey, endKey)
	if err != nil {
		return Storage_Err(Wrap_Err(err, "failed to scan catalog for collections"))
	}

	var collectionsToDelete []CollectionCatalogEntry
	for cursor.Next() {
		_, value, err := cursor.Current()
		if err != nil {
			cursor.Close()
			return Storage_Err(Wrap_Err(err, "failed to get current collection"))
		}
		var collection CollectionCatalogEntry
		if err := bson.Unmarshal(value, &collection); err != nil {
			cursor.Close()
			return Serialization_Err(Wrap_Err(err, "failed to unmarshal collection catalog"))
		}
		collectionsToDelete = append(collectionsToDelete, collection)
	}
	cursor.Close()

	// Delete each collection's resources
	for _, collection := range collectionsToDelete {
		// Delete the collection's table (if it exists)
		tableExists, _ := kv.TableExists(collection.TableUri)
		if tableExists {
			// Note: WiredTiger doesn't have a DropTable in the interface,
			// so we'll delete all entries from the table
			// For now, we just delete the catalog and stats entries
		}

		// Delete vector index file
		if collection.VectorIndexUri != "" {
			os.Remove(collection.VectorIndexUri)
		}

		// Delete collection from catalog
		if err := kv.DeleteBinary(CATALOG, []byte(collection.Ns)); err != nil {
			return Storage_Err(Wrap_Err(err, "failed to delete collection catalog entry"))
		}

		// Delete stats entry
		if err := kv.DeleteBinary(STATS, []byte(collection.Ns)); err != nil {
			// Stats might
			//  not exist, continue
		}
	}

	// Delete the database entry from catalog
	if err := kv.DeleteBinaryWithStringKey(CATALOG, dbKey); err != nil {
		return Storage_Err(Wrap_Err(err, "failed to delete database catalog entry"))
	}

	return nil
}

func (s *GDBService) CreateCollection(collection_name string) error {
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
		return Storage_Err(Wrap_Err(err, "failed to check if table exists %s", collectionTableUri))
	}

	if exists {
		return AlreadyExists_Err(ErrCollectionExists)
	}

	err = s.KvService.CreateTable(collectionTableUri, "key_format=u,value_format=u")
	if err != nil {
		return Storage_Err(Wrap_Err(err, "failed to create table %s", collectionTableUri))
	}

	doc, err := bson.Marshal(catalogEntry)
	if err != nil {
		return Serialization_Err(Wrap_Err(err, "failed to encode catalog entry"))
	}

	err = s.KvService.PutBinaryWithStringKey(CATALOG, collectionKey, doc)
	if err != nil {
		return Storage_Err(Wrap_Err(err, "failed to write catalog entry"))
	}

	statsEntry := CollectionStats{
		Doc_Count:         0,
		Vector_Index_Size: 0,
	}

	stats_doc, err := bson.Marshal(statsEntry)
	if err != nil {
		return Serialization_Err(Wrap_Err(err, "failed to encode stats entry"))
	}

	err = s.KvService.PutBinaryWithStringKey(STATS, collectionKey, stats_doc)
	if err != nil {
		return Storage_Err(Wrap_Err(err, "failed to write stats entry"))
	}

	return nil
}

func (s *GDBService) DeleteCollection(collection_name string) error {
	if len(collection_name) == 0 {
		return InvalidInput_Err(ErrEmptyName)
	}

	kv := s.KvService
	collectionDefKey := fmt.Sprintf("%s.%s", s.Name, collection_name)

	// Get collection info
	val, exists, err := kv.GetBinary(CATALOG, []byte(collectionDefKey))
	if err != nil {
		return Storage_Err(Wrap_Err(err, "failed to get collection catalog"))
	}
	if !exists {
		return NotFound_Err(ErrCollectionNotFound)
	}

	var collection CollectionCatalogEntry
	if err := bson.Unmarshal(val, &collection); err != nil {
		return Serialization_Err(Wrap_Err(err, "failed to unmarshal collection catalog"))
	}

	// Delete vector index file
	if collection.VectorIndexUri != "" {
		os.Remove(collection.VectorIndexUri)
	}

	// Delete collection from catalog
	if err := kv.DeleteBinary(CATALOG, []byte(collectionDefKey)); err != nil {
		return Storage_Err(Wrap_Err(err, "failed to delete collection catalog entry"))
	}

	// Delete stats entry
	kv.DeleteBinary(STATS, []byte(collectionDefKey))

	return nil
}

func (s *GDBService) ListCollections() ([]CollectionCatalogEntry, error) {
	if s.Name == "" {
		return nil, InvalidInput_Err(ErrEmptyName)
	}

	startKey := []byte(s.Name + ".")
	endKey := []byte(s.Name + "/")

	cursor, err := s.KvService.ScanRangeBinary(CATALOG, startKey, endKey)
	if err != nil {
		return nil, Storage_Err(Wrap_Err(err, "failed to scan catalog"))
	}
	defer cursor.Close()

	var collections []CollectionCatalogEntry
	for cursor.Next() {
		_, value, err := cursor.Current()
		if err != nil {
			return nil, Storage_Err(Wrap_Err(err, "failed to get current"))
		}
		collectionValue := CollectionCatalogEntry{}
		if err := bson.Unmarshal(value, &collectionValue); err != nil {
			return nil, Serialization_Err(Wrap_Err(err, "failed to unmarshal collection catalog"))
		}
		collections = append(collections, collectionValue)
	}

	if err := cursor.Err(); err != nil {
		return nil, Storage_Err(Wrap_Err(err, "cursor error during iteration"))
	}

	return collections, nil
}

func (s *GDBService) GetCollection(collection_name string) (CollectionEntry, error) {
	if s.Name == "" {
		return CollectionEntry{}, InvalidInput_Err(ErrEmptyName)
	}

	collectionDefKey := fmt.Sprintf("%s.%s", s.Name, collection_name)
	val, exists, err := s.KvService.GetBinary(CATALOG, []byte(collectionDefKey))

	if err != nil {
		return CollectionEntry{}, Storage_Err(Wrap_Err(err, "failed to get collection catalog"))
	}

	if !exists {
		return CollectionEntry{}, NotFound_Err(ErrCollectionNotFound)
	}

	var collection CollectionCatalogEntry
	if err := bson.Unmarshal(val, &collection); err != nil {
		return CollectionEntry{}, Serialization_Err(Wrap_Err(err, "failed to unmarshal collection catalog"))
	}

	statsVal, statsExists, statsErr := s.KvService.GetBinary(STATS, []byte(collectionDefKey))
	var stats CollectionStats
	if statsErr == nil && statsExists {
		if err := bson.Unmarshal(statsVal, &stats); err != nil {
			return CollectionEntry{}, Serialization_Err(Wrap_Err(err, "failed to unmarshal collection stats"))
		}
	}

	documents := make([]GlowstickDocument, 0)

	stats.Vector_Index_Size = float64(stats.Vector_Index_Size)

	return CollectionEntry{
		Info:      collection,
		Documents: documents,
		Stats:     stats,
	}, nil
}
func (s *GDBService) InsertDocuments(collection_name string, documents []GlowstickDocument) error {
	if len(documents) == 0 {
		return InvalidInput_Err(ErrEmptyDocuments)
	}

	kv := s.KvService
	vectr := faiss.FAISS()

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

	idx, err := vectr.ReadIndex(vectorIndexFilePath)
	if err != nil {
		if len(documents[0].Embedding) == 0 {
			return InvalidInput_Err(fmt.Errorf("document with ID:%s has empty embedding", documents[0].Id))
		}
		const indexDesc = "Flat"
		idx, err = vectr.IndexFactory(len(documents[0].Embedding), indexDesc, faiss.MetricL2)
		if err != nil {
			return Internal_Err(Wrap_Err(err, "failed to create new vector index"))
		}
	}
	defer idx.Free()

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

	// Insert documents into KV store and validate embeddings
	for i, doc := range documents {
		// Validate embedding
		if len(doc.Embedding) == 0 {
			return InvalidInput_Err(fmt.Errorf("document with ID:%s has empty embedding", doc.Id))
		}

		if i == 0 {
			embeddingDim = len(doc.Embedding)
			embeddings = make([]float32, numDocs*embeddingDim) // Resize with correct dimension
		} else if len(doc.Embedding) != embeddingDim {
			return InvalidInput_Err(fmt.Errorf("document %s has embedding dimension %d, expected %d", doc.Id, len(doc.Embedding), embeddingDim))
		}

		doc_bytes, err := bson.Marshal(doc)
		if err != nil {
			return Serialization_Err(Wrap_Err(err, "failed to marshal document %s", doc.Id))
		}

		key := []byte(doc.Id)
		if err := s.KvService.PutBinary(destTableURI, key, doc_bytes); err != nil {
			return Storage_Err(Wrap_Err(err, "failed to insert document %s at index %d", doc.Id, i))
		}

		// Copy embedding data
		copy(embeddings[i*embeddingDim:(i+1)*embeddingDim], doc.Embedding)
		labelMappings[i] = doc.Id
	}

	if err := idx.Add(embeddings, len(documents)); err != nil {
		return Internal_Err(Wrap_Err(err, "failed to add embeddings to index"))
	}

	for i, docID := range labelMappings {
		label := startLabel + int64(i)
		if err := s.KvService.PutString(LABELS_TO_DOC_ID_MAPPING_TABLE_URI, fmt.Sprintf("%d", label), docID); err != nil {
			return Storage_Err(Wrap_Err(err, "failed to write label->docID mapping for label %d", label))
		}
	}

	if err := idx.WriteToFile(vectorIndexFilePath); err != nil {
		return Storage_Err(Wrap_Err(err, "failed to write index to file"))
	}

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

	return nil
}

func (s *GDBService) QueryCollection(collection_name string, query QueryStruct) ([]GlowstickDocument, error) {
	kv := s.KvService
	vectr_svc := faiss.FAISS()

	docs := make([]GlowstickDocument, 0, query.TopK)

	collectionDefKey := s.Name + "." + collection_name

	val, exists, err := kv.GetBinary(CATALOG, []byte(collectionDefKey))

	if !exists {
		return nil, NotFound_Err(ErrCollectionNotFound)
	}

	if err != nil {
		return nil, Storage_Err(Wrap_Err(err, "failed to get collection catalog"))
	}

	var collection CollectionCatalogEntry

	if err := bson.Unmarshal(val, &collection); err != nil {
		return nil, Serialization_Err(Wrap_Err(err, "failed to unmarshal collection catalog"))
	}

	vectorIndexUri := collection.VectorIndexUri

	var vectorIndexFilePath string
	if vectorIndexUri != "" {
		u, err := url.Parse(vectorIndexUri)
		if err != nil {
			return nil, Internal_Err(Wrap_Err(err, "failed to parse vector index URI"))
		}
		vectorIndexFilePath = u.Path
	}

	idx, err := vectr_svc.ReadIndex(vectorIndexFilePath)
	defer idx.Free()

	if err != nil {
		return nil, Storage_Err(Wrap_Err(err, "could not read vector index from file"))
	}

	distances, ids, err := idx.Search(query.QueryEmbedding, 1, int(query.TopK))

	if err != nil {
		return nil, Internal_Err(Wrap_Err(err, "failed to search vector index"))
	}

	if len(ids) == 0 {
		return docs, nil
	}

	if len(ids) <= 3 {
		for i, id := range ids {
			distance := distances[i]

			key := strconv.FormatInt(int64(id), 10)

			val, exists, err := kv.GetString(LABELS_TO_DOC_ID_MAPPING_TABLE_URI, key)
			if err != nil || !exists {
				continue
			}

			var docIDBytes = []byte(val)

			docBin, exists, err := kv.GetBinary(collection.TableUri, docIDBytes)
			if err != nil || !exists || len(docBin) == 0 {
				continue
			}

			var doc GlowstickDocument
			if err := bson.Unmarshal(docBin, &doc); err != nil {
				continue
			}

			if query.MaxDistance != 0 && distance >= query.MaxDistance {
				continue
			}

			if query.Filters != nil {
				matches, err := matchesFilter(doc.Metadata, query.Filters)
				if err != nil {
					continue
				}
				if !matches {
					continue
				}
			}

			docs = append(docs, doc)
		}
		return docs, nil
	}

	numWorkers := min(len(ids), 10)
	chunkSize := (len(ids) + numWorkers - 1) / numWorkers

	type docResult struct {
		doc   GlowstickDocument
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
				id := ids[j]
				distance := distances[j]

				key := strconv.FormatInt(int64(id), 10)

				val, exists, err := kv.GetString(LABELS_TO_DOC_ID_MAPPING_TABLE_URI, key)
				if err != nil || !exists {
					continue
				}

				var docIDBytes = []byte(val)

				docBin, exists, err := kv.GetBinary(collection.TableUri, docIDBytes)
				if err != nil || !exists || len(docBin) == 0 {
					continue
				}

				var doc GlowstickDocument
				if err := bson.Unmarshal(docBin, &doc); err != nil {
					continue
				}

				if query.MaxDistance != 0 && distance >= query.MaxDistance {
					continue
				}

				if query.Filters != nil {
					matches, err := matchesFilter(doc.Metadata, query.Filters)
					if err != nil {
						continue
					}
					if !matches {
						continue
					}
				}

				resultChan <- docResult{doc: doc, index: j}
			}
		}(start, end)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	results := make(map[int]GlowstickDocument)
	for result := range resultChan {
		results[result.index] = result.doc
	}

	for i := range ids {
		if doc, exists := results[i]; exists {
			docs = append(docs, doc)
		}
	}

	return docs, nil
}
func (s *GDBService) GetDocuments(collection_name string) ([]GlowstickDocument, error) {
	kv := s.KvService
	collectionDefKey := fmt.Sprintf("%s.%s", s.Name, collection_name)
	val, exists, err := kv.GetBinary(CATALOG, []byte(collectionDefKey))
	if err != nil {
		return nil, Storage_Err(Wrap_Err(err, "failed to get collection catalog"))
	}
	if !exists {
		return nil, NotFound_Err(ErrCollectionNotFound)
	}
	var collection CollectionCatalogEntry
	if err := bson.Unmarshal(val, &collection); err != nil {
		return nil, Serialization_Err(Wrap_Err(err, "failed to unmarshal collection catalog"))
	}

	cursor, err := kv.ScanRangeBinary(collection.TableUri, []byte(""), []byte("~"))
	if err != nil {
		return nil, Storage_Err(Wrap_Err(err, "failed to scan collection table"))
	}
	defer cursor.Close()

	var docs []GlowstickDocument
	for cursor.Next() {
		_, value, err := cursor.Current()
		if err != nil {
			return nil, Storage_Err(Wrap_Err(err, "failed to get current document"))
		}

		var doc GlowstickDocument
		if err := bson.Unmarshal(value, &doc); err != nil {
			return nil, Serialization_Err(Wrap_Err(err, "failed to unmarshal document"))
		}
		docs = append(docs, doc)
	}

	if err := cursor.Err(); err != nil {
		return nil, Storage_Err(Wrap_Err(err, "cursor error during iteration"))
	}

	return docs, nil
}

func (s *GDBService) UpdateDocuments(collection_name string, payload *DocUpdatePayload) error {
	kv := s.KvService
	collectionDefKey := fmt.Sprintf("%s.%s", s.Name, collection_name)
	val, exists, err := kv.GetBinary(CATALOG, []byte(collectionDefKey))

	if err != nil {
		return Storage_Err(Wrap_Err(err, "failed to get collection catalog"))
	}
	if !exists {
		return NotFound_Err(ErrCollectionNotFound)
	}
	var collection CollectionCatalogEntry
	if err := bson.Unmarshal(val, &collection); err != nil {
		return Serialization_Err(Wrap_Err(err, "failed to unmarshal collection catalog"))
	}

	if len(payload.DocumentId) == 0 {
		return InvalidInput_Err(fmt.Errorf("document Id is empty"))
	}

	doc_raw, doc_exists, doc_get_err := kv.GetBinaryWithStringKey(collection.TableUri, payload.DocumentId)

	if !doc_exists {
		return NotFound_Err(ErrDocumentNotFound)
	}

	if doc_get_err != nil {
		return Storage_Err(Wrap_Err(doc_get_err, "failed to get document"))
	}

	var doc GlowstickDocument
	if err := bson.Unmarshal(doc_raw, &doc); err != nil {
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
		return Serialization_Err(Wrap_Err(err, "failed to marshal updated document"))
	}

	if err := kv.PutBinaryWithStringKey(collection.TableUri, payload.DocumentId, updated_val); err != nil {
		return Storage_Err(Wrap_Err(err, "failed to update document"))
	}

	return nil
}

func (s *GDBService) DeleteDocuments(collection_name string, documentIds []string) error {
	if len(documentIds) == 0 {
		return InvalidInput_Err(ErrEmptyDocuments)
	}

	kv := s.KvService
	collectionDefKey := fmt.Sprintf("%s.%s", s.Name, collection_name)

	// Get collection info
	val, exists, err := kv.GetBinary(CATALOG, []byte(collectionDefKey))
	if err != nil {
		return Storage_Err(Wrap_Err(err, "failed to get collection catalog"))
	}
	if !exists {
		return NotFound_Err(ErrCollectionNotFound)
	}

	var collection CollectionCatalogEntry
	if err := bson.Unmarshal(val, &collection); err != nil {
		return Serialization_Err(Wrap_Err(err, "failed to unmarshal collection catalog"))
	}

	// Delete each document
	deletedCount := 0
	for _, docId := range documentIds {
		// Check if document exists before deleting
		_, docExists, _ := kv.GetBinaryWithStringKey(collection.TableUri, docId)
		if docExists {
			if err := kv.DeleteBinaryWithStringKey(collection.TableUri, docId); err != nil {
				return Storage_Err(Wrap_Err(err, "failed to delete document %s", docId))
			}
			deletedCount++
		}
	}

	// Update stats if any documents were deleted
	if deletedCount > 0 {
		statsVal, statsExists, err := kv.GetBinary(STATS, []byte(collectionDefKey))
		if err != nil {
			return Storage_Err(Wrap_Err(err, "failed to get stats"))
		}
		if statsExists {
			var stats CollectionStats
			if err := bson.Unmarshal(statsVal, &stats); err != nil {
				return Serialization_Err(Wrap_Err(err, "failed to unmarshal stats"))
			}

			stats.Doc_Count -= deletedCount
			if stats.Doc_Count < 0 {
				stats.Doc_Count = 0
			}

			updatedStats, err := bson.Marshal(stats)
			if err != nil {
				return Serialization_Err(Wrap_Err(err, "failed to marshal updated stats"))
			}

			if err := kv.PutBinary(STATS, []byte(collectionDefKey), updatedStats); err != nil {
				return Storage_Err(Wrap_Err(err, "failed to update stats"))
			}
		}
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
