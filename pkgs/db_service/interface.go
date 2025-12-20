package dbservice

import (
	wt "achillesdb/pkgs/wiredtiger"
	"os"
)

// TABLE URIS for creating wiredtiger tables
var CATALOG = "table:_catalog"
var STATS = "table:_stats"
var LABELS_TO_DOC_ID_MAPPING_TABLE_URI = "table:label_docID"

// GetVectorsFilePath returns the vectors directory path, configurable via VECTORS_HOME env var
func GetVectorsFilePath() string {
	if value := os.Getenv("VECTORS_HOME"); value != "" {
		return value
	}
	return "volumes/vectors"
}

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

type DatabaseInfo struct {
	Name            string `json:"name"`
	CollectionCount int    `json:"collectionCount"`
	Empty           bool   `json:"empty"`
}

type ListDatabasesResponse struct {
	Databases []DatabaseInfo `json:"databases"`
}

type DBService interface {
	CreateDB() error
	DeleteDB(name string) error
	ListDatabases() (ListDatabasesResponse, error)
	ListCollections() ([]CollectionCatalogEntry, error)
	CreateCollection(collection_name string) error
	DeleteCollection(collection_name string) error
	GetCollection(collection_name string) (CollectionEntry, error)
	InsertDocuments(collection_name string, documents []GlowstickDocument) error
	GetDocuments(collection_name string) ([]GlowstickDocument, error)
	DeleteDocuments(collection_name string, documentIds []string) error
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
