package dbservice

import (
	"achillesdb/pkgs/faiss"
	wt "achillesdb/pkgs/wiredtiger"
	"fmt"
	"net/url"
	"os"
	"strconv"
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
	Id               primitive.ObjectID `bson:"_id"`
	Ns               string             `bson:"ns"`
	TableUri         string             `bson:"table_uri"`
	VectorIndexUri   string             `bson:"vector_index_uri"`
	IndexTableUriMap map[string]string  `bson:"index_table_uri_map,omitempty"`
	Indexes          []CollectionIndex  `bson:"indexes,omitempty"`
	CreatedAt        primitive.DateTime `bson:"createdAt"`
	UpdatedAt        primitive.DateTime `bson:"updatedAt"`
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

	err := InitTablesHelper(s.KvService)

	if err != nil {
		return err
	}

	if s.Name == "" {
		return fmt.Errorf("database name cannot be empty")
	}

	catalogEntry := DbCatalogEntry{
		UUID:   primitive.NewObjectID().Hex(),
		Name:   s.Name,
		Config: map[string]string{"Index": "HNSW"},
	}

	doc, err := bson.Marshal(catalogEntry)

	if err != nil {
		return err
	}

	err = s.KvService.PutBinaryWithStringKey(CATALOG, fmt.Sprintf("db:%s", s.Name), doc)

	if err != nil {
		return fmt.Errorf("failed to write db catalog entry")
	}

	return nil
}

func (s *GDBService) DeleteDB(name string) error {
	return nil
}

func (s *GDBService) CreateCollection(collection_name string) error {
	kv := s.KvService
	err := InitTablesHelper(kv)
	if err != nil {
		return err
	}

	if len(collection_name) == 0 {
		return fmt.Errorf("collection name cannot be empty")
	}

	collectionId := primitive.NewObjectID()
	collectionTableUri := fmt.Sprintf("table:collection-%s-%s", collectionId.Hex(), s.Name)
	collectionKey := fmt.Sprintf("%s.%s", s.Name, collection_name)

	catalogEntry := CollectionCatalogEntry{
		Id:             collectionId,
		Ns:             collectionKey,
		TableUri:       collectionTableUri,
		VectorIndexUri: fmt.Sprintf("%s%s", collection_name, ".index"),
		CreatedAt:      primitive.NewDateTimeFromTime(time.Now()),
		UpdatedAt:      primitive.NewDateTimeFromTime(time.Now()),
	}

	err = s.KvService.CreateTable(collectionTableUri, "key_format=u,value_format=u")
	if err != nil {
		return fmt.Errorf("[GDBSERVICE:CreateCollection] failed to create table %s: %v", collectionTableUri, err)
	}

	doc, err := bson.Marshal(catalogEntry)
	if err != nil {
		return fmt.Errorf("[GDBSERVICE:CreateCollection] failed to encode catalog entry: %v", err)
	}

	err = kv.PutBinaryWithStringKey(CATALOG, collectionKey, doc)
	if err != nil {
		return fmt.Errorf("[GDBSERVICE:CreateCollection] failed to write catalog entry: %v", err)
	}

	// STATS
	// Create entry in hot stats table
	statsEntry := CollectionStats{
		Doc_Count:         0,
		Vector_Index_Size: 0,
	}

	stats_doc, err := bson.Marshal(statsEntry)
	if err != nil {
		return fmt.Errorf("[GDBSERVICE:CreateCollection] failed to encode stats entry: %v", err)
	}

	err = kv.PutBinaryWithStringKey(STATS, collectionKey, stats_doc)
	if err != nil {
		return fmt.Errorf("[GDBSERVICE:CreateCollection] failed to write stats entry: %v", err)
	}

	return nil
}

func (s *GDBService) InsertDocumentsIntoCollection(collection_name string, documents []GlowstickDocument) error {
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

	var filePath string
	u, err := url.Parse(vectorIndexUri)
	if err != nil {
		return fmt.Errorf("[GDBSERVICE:InsertDocumentsIntoCollection] failed to parse vector index URI: %v", err)
	}
	filePath = u.Path

	idx, err := vectr.ReadIndex(filePath)
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

	embeddings := make([]float32, 0, len(documents)*len(documents[0].Embedding))
	docKeys := make([][]byte, 0, len(documents))
	docBytes := make([][]byte, 0, len(documents))
	labelMappings := make([]string, 0, len(documents))

	for _, doc := range documents {
		doc_bytes, err := bson.Marshal(doc)
		if err != nil {
			return fmt.Errorf("[GDBSERVICE:InsertDocumentsIntoCollection] failed to marshal document %s: %v", doc.Id.Hex(), err)
		}

		key := doc.Id[:]
		docKeys = append(docKeys, key)
		docBytes = append(docBytes, doc_bytes)

		embeddings = append(embeddings, doc.Embedding...)

		docIDHex := fmt.Sprintf("%x", key)
		labelMappings = append(labelMappings, docIDHex)
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

	for i, docIDHex := range labelMappings {
		label := startLabel + int64(i)
		if err := s.KvService.PutString(LABELS_TO_DOC_ID_MAPPING_TABLE_URI, fmt.Sprintf("%d", label), docIDHex); err != nil {
			return fmt.Errorf("[GDBSERVICE:InsertDocumentsIntoCollection] failed to write label->docID mapping for label %d: %v", label, err)
		}
	}

	if err := idx.WriteToFile(filePath); err != nil {
		return fmt.Errorf("[GDBSERVICE:InsertDocumentsIntoCollection] failed to write index to file: %v", err)
	}

	info, err := os.Stat(filePath)
	if err != nil {
		return fmt.Errorf("[GDBSERVICE:InsertDocumentsIntoCollection] failed to stat vector index file: %v", err)
	}

	hot_stats_doc.Doc_Count += int(len(documents))
	hot_stats_doc.Vector_Index_Size = float64(info.Size())

	bytes, err := bson.Marshal(hot_stats_doc)
	if err != nil {
		return fmt.Errorf("[GDBSERVICE:InsertDocumentsIntoCollection] failed to marshal hot stats: %v", err)
	}

	if err := kv.PutBinary(STATS, []byte(collectionDefKey), bytes); err != nil {
		return fmt.Errorf("[GDBSERVICE:InsertDocumentsIntoCollection] failed to write hot stats: %v", err)
	}

	return nil
}

func (s *GDBService) ListCollections() error {
	return nil
}
func (s *GDBService) QueryCollection(collection_name string, query QueryStruct) ([]GlowstickDocument, error) {
	kv := s.KvService
	vectr_svc := faiss.FAISS()

	docs := make([]GlowstickDocument, 0, query.TopK)

	collectionDefKey := fmt.Sprintf("%s.%s", s.Name, collection_name)

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

	var filePath string
	if vectorIndexUri != "" {
		u, err := url.Parse(vectorIndexUri)
		if err != nil {
			return nil, fmt.Errorf("[DB_SERVICE:QueryCollection] - failed to parse vector index URI: %v", err)
		}
		filePath = u.Path
	}

	idx, err := vectr_svc.ReadIndex(filePath)
	defer idx.Free()

	if err != nil {
		return nil, fmt.Errorf("[DB_SERVICE:QueryCollection] - could not vector index after specfied file path: %v", err)
	}

	distances, ids, err := idx.Search(query.QueryEmbedding, 1, int(query.TopK))

	if err != nil {
		return nil, fmt.Errorf("[DB_SERVICE:QueryCollection] - failed to search vector index for query embedding")
	}

	keyBuffer := make([]byte, 0, 16)

	var lastErr error
	var errorCount int

	for i, id := range ids {
		distance := distances[i]

		// id could be -1 if FAISS returned a "no result"; handle this
		if id < 0 {
			continue
		}

		keyBuffer = keyBuffer[:0]
		keyBuffer = strconv.AppendInt(keyBuffer, int64(id), 10)
		key := string(keyBuffer)

		val, _, err := kv.GetString(LABELS_TO_DOC_ID_MAPPING_TABLE_URI, key)
		if err != nil {
			errorCount++
			lastErr = err
			continue
		}

		if len(val) != 24 {
			errorCount++
			lastErr = fmt.Errorf("invalid ObjectID hex length: expected 24, got %d for '%s'", len(val), val)
			continue
		}

		objectID, err := primitive.ObjectIDFromHex(val)
		if err != nil || objectID.IsZero() {
			errorCount++
			lastErr = fmt.Errorf("failed to parse or invalid ObjectID '%s': %v", val, err)
			continue
		}

		docIDBytes := objectID[:]

		docBin, exists, err := kv.GetBinary(collection.TableUri, docIDBytes)
		if err != nil {
			errorCount++
			lastErr = err
			continue
		}

		if !exists {
			return nil, fmt.Errorf("[DB_SERVICE:QueryCollection] - failed to get document with id %v", val)
		}

		if len(docBin) > 0 {
			var doc GlowstickDocument

			if err := bson.Unmarshal(docBin, &doc); err != nil {
				errorCount++
				lastErr = err
				continue
			}

			if query.MaxDistance == 0 || distance < query.MaxDistance {
				docs = append(docs, doc)
			}
		}
	}

	if errorCount > 0 && lastErr != nil {
		fmt.Printf("[QueryCollection] Encountered %d errors during processing. Last error: %v\n", errorCount, lastErr)
	}

	return docs, lastErr
}

func InitTablesHelper(wtService wt.WTService) error {
	if _, err := os.Stat("volumes/WT_HOME"); os.IsNotExist(err) {
		if mkErr := os.MkdirAll("volumes/WT_HOME", 0755); mkErr != nil {
			return fmt.Errorf("failed to create volumes/db_files: %w", mkErr)
		}
	}

	if err := wtService.CreateTable(CATALOG, "key_format=u,value_format=u"); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	if err := wtService.CreateTable(STATS, "key_format=u,value_format=u"); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	if err := wtService.CreateTable(LABELS_TO_DOC_ID_MAPPING_TABLE_URI, "key_format=S,value_format=S"); err != nil {
		return fmt.Errorf("failed to create table: %v", err)
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
