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
	Filters        map[string]any
}
type CollectionEntry struct {
	Info      CollectionCatalogEntry `json:"collection"`
	Documents []GlowstickDocument    `json:"documents"`
	Stats     CollectionStats        `json:"stats"`
}

type DocUpdatePayload struct {
	DocumentId string
	Where      map[string]any
	Updates    map[string]any
}

type DBService interface {
	CreateDB() error
	DeleteDB(name string) error
	ListCollections() ([]CollectionCatalogEntry, error)
	CreateCollection(collection_name string) error
	GetCollection(collection_name string) (CollectionEntry, error)
	InsertDocuments(collection_name string, documents []GlowstickDocument) error
	GetDocuments(collection_name string) ([]GlowstickDocument, error)
	QueryCollection(collection_name string, query QueryStruct) ([]GlowstickDocument, error)
	UpdateDocuments(collection_name string, payload *DocUpdatePayload) error
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
