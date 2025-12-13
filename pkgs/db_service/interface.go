package dbservice

import (
	wt "achillesdb/pkgs/wiredtiger"
)

// TABLE URIS for creating wiredtiger tables
var CATALOG = "table:_catalog"
var STATS = "table:_stats"
var LABELS_TO_DOC_ID_MAPPING_TABLE_URI = "table:label_docID"

type GlowstickDocument struct {
	Id        string                 `bson:"_id" json:"id"`
	Content   string                 `bson:"content" json:"content"`
	Embedding []float32              `bson:"embedding" json:"-"`
	Metadata  map[string]interface{} `bson:"metadata" json:"metadata"`
}

type QueryStruct struct {
	TopK           int32
	MaxDistance    float32
	QueryEmbedding []float32
	// Filters        map[string]any
}
type CollectionEntry struct {
	Info      CollectionCatalogEntry `json:"collection"`
	Documents []GlowstickDocument    `json:"documents"`
	Stats     CollectionStats        `json:"stats"`
}

type DBService interface {
	CreateDB() (AchillesErrorCode, error)
	DeleteDB(name string) error
	CreateCollection(collection_name string) (AchillesErrorCode, error)
	GetCollection(collection_name string) (CollectionEntry, error)
	ListCollections() ([]CollectionCatalogEntry, error)
	InsertDocuments(collection_name string, documents []GlowstickDocument) error
	GetDocuments(collection_name string) ([]GlowstickDocument, error)
	QueryCollection(collection_name string, query QueryStruct) ([]GlowstickDocument, error)
}

type DbParams struct {
	Name        string
	PutIfAbsent bool
	KvService   wt.WTService
}

func DatabaseService(params DbParams) DBService {
	return &GDBService{
		Name:      params.Name,
		KvService: params.KvService,
	}
}

type AchillesErrorCode int

const (
	Err_Db_Exists         AchillesErrorCode = 6767 // hHehehe
	Err_Collection_Exists AchillesErrorCode = 6768
)
