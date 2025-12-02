package dbservice

import (
	wt "achillesdb/pkgs/wiredtiger"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// TABLE URIS for creating wiredtiger tables
var CATALOG = "table:_catalog"
var STATS = "table:_stats"
var LABELS_TO_DOC_ID_MAPPING_TABLE_URI = "table:label_docID"

type GlowstickDocument struct {
	Id        primitive.ObjectID `bson:"_id"`
	Content   string             `bson:"content"`
	Embedding []float32          `bson:"embedding"`
	Metadata  interface{}        `bson:"metadata"` // Any BSON- and JSON-serializable type
}

type QueryStruct struct {
	TopK           int32
	MaxDistance    float32
	QueryEmbedding []float32
	Filters        map[string]interface{}
}

type DBService interface {
	CreateDB() error
	DeleteDB(name string) error
	CreateCollection(collection_name string) error
	InsertDocumentsIntoCollection(collection_name string, documents []GlowstickDocument) error
	QueryCollection(collection_name string, query QueryStruct) ([]GlowstickDocument, error)
	ListCollections() error
}

type DbParams struct {
	Name        string
	PutIfAbsent bool
	KvService   wt.WTService
}

func DatabaseService(params DbParams) DBService {
	return &GDBService{Name: params.Name, KvService: params.KvService}
}
