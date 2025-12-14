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
	Key  map[string]int         `bson:"key"` // field name -> sort order/type (e.g., 1 for asc, -1 for desc)
	Name string                 `bson:"name"`
	Ns   string                 `bson:"ns"`             // namespace: "db.collection"
	Type string                 `bson:"type"`           // index type, e.g., "single", "2dsphere", etc.
	V    int                    `bson:"v"`              // version number
	Opts map[string]interface{} `bson:"opts,omitempty"` // additional index options, optional
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
	// Database Name
	Name string
	// WiredTiger Service
	KvService wt.WTService
}

func (s *GDBService) CreateDB() (AchillesErrorCode, error) {

	if s.Name == "" {
		return 1, fmt.Errorf("database name cannot be empty")
	}

	catalogEntry := DbCatalogEntry{
		UUID:   primitive.NewObjectID().Hex(),
		Name:   s.Name,
		Config: map[string]string{"Index": "HNSW"},
	}

	doc, err := bson.Marshal(catalogEntry)

	if err != nil {
		return 1, err
	}

	// Check if database already exists
	dbKey := fmt.Sprintf("db:%s", s.Name)
	_, exists, _ := s.KvService.GetBinaryWithStringKey(CATALOG, dbKey)

	if exists {
		return Err_Db_Exists, fmt.Errorf("database '%s' already exists", s.Name)
	}

	err = s.KvService.PutBinaryWithStringKey(CATALOG, dbKey, doc)

	if err != nil {
		return 0, fmt.Errorf("failed to write db catalog entry")
	}

	return 0, nil
}

func (s *GDBService) DeleteDB(name string) error {
	return nil
}

func (s *GDBService) CreateCollection(collection_name string) (AchillesErrorCode, error) {
	kv := s.KvService

	if len(collection_name) == 0 {
		return 1, fmt.Errorf("collection name cannot be empty")
	}

	collectionId := primitive.NewObjectID()
	collectionTableUri := fmt.Sprintf("table:collection-%s-%s", collection_name, s.Name)
	collectionKey := fmt.Sprintf("%s.%s", s.Name, collection_name)

	catalogEntry := CollectionCatalogEntry{
		Id:             collectionId,
		Ns:             collectionKey,
		TableUri:       collectionTableUri,
		VectorIndexUri: fmt.Sprintf("%s%s", collection_name, ".index"),
		CreatedAt:      primitive.NewDateTimeFromTime(time.Now()),
		UpdatedAt:      primitive.NewDateTimeFromTime(time.Now()),
	}

	// Throw err on duplicate name
	exists, err := s.KvService.TableExists(collectionTableUri)
	if err != nil {
		return 1, fmt.Errorf("[GDBSERVICE:CreateCollection] failed to check if table exists %s: %v", collectionTableUri, err)
	}

	if exists {
		return Err_Collection_Exists, fmt.Errorf("[GDBSERVICE:CreateCollection] collection '%s' already exists", collection_name)
	}

	err = s.KvService.CreateTable(collectionTableUri, "key_format=u,value_format=u")
	if err != nil {
		return 1, fmt.Errorf("[GDBSERVICE:CreateCollection] failed to create table %s: %v", collectionTableUri, err)
	}

	doc, err := bson.Marshal(catalogEntry)
	if err != nil {
		return 1, fmt.Errorf("[GDBSERVICE:CreateCollection] failed to encode catalog entry: %v", err)
	}

	err = kv.PutBinaryWithStringKey(CATALOG, collectionKey, doc)
	if err != nil {
		return 1, fmt.Errorf("[GDBSERVICE:CreateCollection] failed to write catalog entry: %v", err)
	}

	// STATS
	// Create entry in hot stats table
	statsEntry := CollectionStats{
		Doc_Count:         0,
		Vector_Index_Size: 0,
	}

	stats_doc, err := bson.Marshal(statsEntry)
	if err != nil {
		return 1, fmt.Errorf("[GDBSERVICE:CreateCollection] failed to encode stats entry: %v", err)
	}

	err = kv.PutBinaryWithStringKey(STATS, collectionKey, stats_doc)
	if err != nil {
		return 1, fmt.Errorf("[GDBSERVICE:CreateCollection] failed to write stats entry: %v", err)
	}

	return 0, nil
}

func (s *GDBService) ListCollections() ([]CollectionCatalogEntry, error) {
	if s.Name == "" {
		return nil, fmt.Errorf("[GDBSERVICE:ListCollections] database name cannot be empty")
	}

	// Use sentinel values to scan the entire table
	startKey := []byte(s.Name + ".")
	endKey := []byte(s.Name + "/")

	cursor, err := s.KvService.ScanRangeBinary(CATALOG, startKey, endKey)
	if err != nil {
		return nil, fmt.Errorf("[GDBSERVICE:ListCollections] failed to scan catalog: %v", err)
	}
	defer cursor.Close()

	var collections []CollectionCatalogEntry
	for cursor.Next() {
		_, value, err := cursor.Current()
		if err != nil {
			return nil, fmt.Errorf("[GDBSERVICE:ListCollections] failed to get current: %v", err)
		}
		collectionValue := CollectionCatalogEntry{}
		if err := bson.Unmarshal(value, &collectionValue); err != nil {
			return nil, fmt.Errorf("[GDBSERVICE:ListCollections] failed to unmarshal collection catalog: %v", err)
		}
		collections = append(collections, collectionValue)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("[GDBSERVICE:ListCollections] cursor error during iteration: %v", err)
	}

	return collections, nil
}

func (s *GDBService) GetCollection(collection_name string) (CollectionEntry, error) {
	if s.Name == "" {
		return CollectionEntry{}, fmt.Errorf("[GDBSERVICE:GetCollection] database name cannot be empty")
	}

	collectionDefKey := fmt.Sprintf("%s.%s", s.Name, collection_name)
	val, exists, err := s.KvService.GetBinary(CATALOG, []byte(collectionDefKey))

	if err != nil {
		return CollectionEntry{}, fmt.Errorf("[GDBSERVICE:GetCollection] failed to get collection catalog: %v", err)
	}

	if !exists {
		return CollectionEntry{}, fmt.Errorf("[GDBSERVICE:GetCollection] collection %s not found", collection_name)
	}

	var collection CollectionCatalogEntry
	if err := bson.Unmarshal(val, &collection); err != nil {
		return CollectionEntry{}, fmt.Errorf("[GDBSERVICE:GetCollection] failed to unmarshal collection catalog: %v", err)
	}

	// Get collection stats
	statsVal, statsExists, statsErr := s.KvService.GetBinary(STATS, []byte(collectionDefKey))
	var stats CollectionStats
	if statsErr == nil && statsExists {
		if err := bson.Unmarshal(statsVal, &stats); err != nil {
			return CollectionEntry{}, fmt.Errorf("[GDBSERVICE:GetCollection] failed to unmarshal collection stats: %v", err)
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
		return fmt.Errorf("[GDBSERVICE:InsertDocumentsIntoCollection] documents slice cannot be empty")
	}
	kv := s.KvService
	vectr := faiss.FAISS()

	collectionDefKey := fmt.Sprintf("%s.%s", s.Name, collection_name)
	val, exists, err := kv.GetBinary(CATALOG, []byte(collectionDefKey))

	if !exists {
		return fmt.Errorf("[GDBSERVICE:InsertDocumentsIntoCollection] collection %s not found", collection_name)
	}

	if err != nil {
		return fmt.Errorf("[GDBSERVICE:InsertDocumentsIntoCollection] failed to get collection catalog: %v", err)
	}

	var collection CollectionCatalogEntry
	if err := bson.Unmarshal(val, &collection); err != nil {
		return fmt.Errorf("[GDBSERVICE:InsertDocumentsIntoCollection] failed to unmarshal collection catalog: %v", err)
	}

	vectorIndexUri := collection.VectorIndexUri

	var vectorIndexFilePath string
	u, err := url.Parse(vectorIndexUri)
	if err != nil {
		return fmt.Errorf("[GDBSERVICE:InsertDocumentsIntoCollection] failed to parse vector index URI: %v", err)
	}
	vectorIndexFilePath = u.Path

	idx, err := vectr.ReadIndex(vectorIndexFilePath)
	if err != nil {
		const indexDesc = "Flat"
		idx, err = vectr.IndexFactory(len(documents[0].Embedding), indexDesc, faiss.MetricL2)
		if err != nil {
			return fmt.Errorf("[GDBSERVICE:InsertDocumentsIntoCollection] failed to create new vector index: %v", err)
		}
	}

	hot_stats, _, err := kv.GetBinary(STATS, []byte(collectionDefKey))
	if err != nil {
		return fmt.Errorf("[GDBSERVICE:InsertDocumentsIntoCollection] failed to fetch hot stats: %v", err)
	}

	var hot_stats_doc CollectionStats
	if err := bson.Unmarshal(hot_stats, &hot_stats_doc); err != nil {
		return fmt.Errorf("[GDBSERVICE:InsertDocumentsIntoCollection] failed to unmarshal hot stats: %v", err)
	}

	destTableURI := collection.TableUri

	numDocs := len(documents)
	embeddingDim := len(documents[0].Embedding)

	embeddings := make([]float32, numDocs*embeddingDim)
	docKeys := make([][]byte, numDocs)
	docBytes := make([][]byte, numDocs)
	labelMappings := make([]string, numDocs)

	for i, doc := range documents {
		doc_bytes, err := bson.Marshal(doc)
		if err != nil {
			return fmt.Errorf("[GDBSERVICE:InsertDocumentsIntoCollection] failed to marshal document %s: %v", doc.Id, err)
		}

		key := doc.Id
		docKeys[i] = []byte(key)
		docBytes[i] = doc_bytes

		copy(embeddings[i*embeddingDim:(i+1)*embeddingDim], doc.Embedding)

		labelMappings[i] = key
	}

	for i, key := range docKeys {
		if err := s.KvService.PutBinary(destTableURI, key, docBytes[i]); err != nil {
			return fmt.Errorf("[GDBSERVICE:InsertDocumentsIntoCollection] failed to insert document %x: %v", key, err)
		}
	}

	startLabel, err := idx.NTotal()
	if err != nil {
		return fmt.Errorf("[GDBSERVICE:InsertDocumentsIntoCollection] failed to get index size: %v", err)
	}

	if err := idx.Add(embeddings, len(documents)); err != nil {
		return fmt.Errorf("[GDBSERVICE:InsertDocumentsIntoCollection] failed to add embeddings to index: %v", err)
	}

	if err := idx.WriteToFile(vectorIndexFilePath); err != nil {
		return fmt.Errorf("[GDBSERVICE:InsertDocumentsIntoCollection] failed to write index to file: %v", err)
	}

	for i, docIDHex := range labelMappings {
		label := startLabel + int64(i)
		if err := s.KvService.PutString(LABELS_TO_DOC_ID_MAPPING_TABLE_URI, fmt.Sprintf("%d", label), docIDHex); err != nil {
			return fmt.Errorf("[GDBSERVICE:InsertDocumentsIntoCollection] failed to write label->docID mapping for label %d: %v", label, err)
		}
	}

	info, err := os.Stat(vectorIndexFilePath)
	if err != nil {
		return fmt.Errorf("[GDBSERVICE:InsertDocumentsIntoCollection] failed to stat vector index file: %v", err)
	}

	hot_stats_doc.Doc_Count += int(len(documents))
	hot_stats_doc.Vector_Index_Size = float64(info.Size())

	hot_stats_doc_bytes, err := bson.Marshal(hot_stats_doc)
	if err != nil {
		return fmt.Errorf("[GDBSERVICE:InsertDocumentsIntoCollection] failed to marshal hot stats: %v", err)
	}

	if err := kv.PutBinary(STATS, []byte(collectionDefKey), hot_stats_doc_bytes); err != nil {
		return fmt.Errorf("[GDBSERVICE:InsertDocumentsIntoCollection] failed to write hot stats: %v", err)
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
		return nil, fmt.Errorf("[DB_SERVICE:QueryCollection] - collection could not be found in the db")
	}

	if err != nil {
		return nil, err
	}

	var collection CollectionCatalogEntry

	bson.Unmarshal(val, &collection)

	vectorIndexUri := collection.VectorIndexUri

	var vectorIndexFilePath string
	if vectorIndexUri != "" {
		u, err := url.Parse(vectorIndexUri)
		if err != nil {
			return nil, fmt.Errorf("[DB_SERVICE:QueryCollection] - failed to parse vector index URI: %v", err)
		}
		vectorIndexFilePath = u.Path
	}

	idx, err := vectr_svc.ReadIndex(vectorIndexFilePath)
	defer idx.Free()

	if err != nil {
		return nil, fmt.Errorf("[DB_SERVICE:QueryCollection] - could not vector index after specfied file path: %v", err)
	}

	distances, ids, err := idx.Search(query.QueryEmbedding, 1, int(query.TopK))

	if err != nil {
		return nil, fmt.Errorf("[DB_SERVICE:QueryCollection] - failed to search vector index for query embedding")
	}

	if len(ids) == 0 {
		return docs, nil
	}

	if len(ids) <= 3 {
		for i, id := range ids {
			distance := distances[i]

			// Build key for label->docID mapping lookup
			key := strconv.FormatInt(int64(id), 10)

			// Get docID from label mapping
			val, exists, err := kv.GetString(LABELS_TO_DOC_ID_MAPPING_TABLE_URI, key)
			if err != nil || !exists {
				continue // Skip on error or invalid length
			}

			var docIDBytes = []byte(val)

			// Get document from collection table
			docBin, exists, err := kv.GetBinary(collection.TableUri, docIDBytes)
			if err != nil || !exists || len(docBin) == 0 {
				continue // Skip on error or missing document
			}

			var doc GlowstickDocument
			if err := bson.Unmarshal(docBin, &doc); err != nil {
				continue // Skip on unmarshal error
			}

			// Apply distance filter if specified
			if query.MaxDistance != 0 && distance >= query.MaxDistance {
				continue
			}

			// Apply metadata filters
			if query.Filters != nil {
				matches, err := matchesFilter(doc.Metadata, query.Filters)
				if err != nil {
					// Log error or treat as mismatch? Treating as mismatch for safety
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

			// Process chunk of IDs
			for j := startIdx; j < endIdx; j++ {
				id := ids[j]
				distance := distances[j]

				// Build key for label->docID mapping lookup
				key := strconv.FormatInt(int64(id), 10)

				// Get docID from label mapping
				val, exists, err := kv.GetString(LABELS_TO_DOC_ID_MAPPING_TABLE_URI, key)
				if err != nil || !exists {
					continue // Skip on error
				}

				var docIDBytes = []byte(val)

				// Get document from collection table
				docBin, exists, err := kv.GetBinary(collection.TableUri, docIDBytes)
				if err != nil || !exists || len(docBin) == 0 {
					continue // Skip on error or missing document
				}

				var doc GlowstickDocument
				if err := bson.Unmarshal(docBin, &doc); err != nil {
					continue // Skip on unmarshal error
				}

				// Apply distance filter if specified
				if query.MaxDistance != 0 && distance >= query.MaxDistance {
					continue
				}

				// Apply metadata filters
				if query.Filters != nil {
					matches, err := matchesFilter(doc.Metadata, query.Filters)
					if err != nil {
						// Log error or treat as mismatch? Treating as mismatch for safety
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

	// Goroutines may complete in any order, so we need to use a map to collect results that preserve order from faiss search results.
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
		return nil, fmt.Errorf("[GDBSERVICE:GetDocumentsFromCollection] failed to get collection catalog: %v", err)
	}
	if !exists {
		return nil, fmt.Errorf("[GDBSERVICE:GetDocumentsFromCollection] collection %s not found", collection_name)
	}
	var collection CollectionCatalogEntry
	if err := bson.Unmarshal(val, &collection); err != nil {
		return nil, fmt.Errorf("[GDBSERVICE:GetDocumentsFromCollection] failed to unmarshal collection catalog: %v", err)
	}

	// Scan all documents in the collection table using range scan
	cursor, err := kv.ScanRangeBinary(collection.TableUri, []byte(""), []byte("~"))
	if err != nil {
		return nil, fmt.Errorf("[GDBSERVICE:GetDocumentsFromCollection] failed to scan collection table: %v", err)
	}
	defer cursor.Close()

	var docs []GlowstickDocument
	for cursor.Next() {
		_, value, err := cursor.Current()
		if err != nil {
			return nil, fmt.Errorf("[GDBSERVICE:GetDocumentsFromCollection] failed to get current document: %v", err)
		}

		var doc GlowstickDocument
		if err := bson.Unmarshal(value, &doc); err != nil {
			return nil, fmt.Errorf("[GDBSERVICE:GetDocumentsFromCollection] failed to unmarshal document: %v", err)
		}
		docs = append(docs, doc)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("[GDBSERVICE:GetDocumentsFromCollection] cursor error during iteration: %v", err)
	}

	return docs, nil
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

// func float64SliceToFloat32(xs []float64) []float32 {
// 	result := make([]float32, len(xs))
// 	for i, v := range xs {
// 		result[i] = float32(v)
// 	}
// 	return result
// }
