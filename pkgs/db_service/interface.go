package dbservice

import (
	wt "achillesdb/pkgs/wiredtiger"
	"fmt"
	"os"

	"go.uber.org/zap"
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
type GlowstickQueryResultSet struct {
	Id        string                 `bson:"_id" json:"id"`
	Content   string                 `bson:"content" json:"content"`
	Embedding []float32              `bson:"embedding" json:"-"`
	Metadata  map[string]interface{} `bson:"metadata" json:"metadata"`
	Distance  float32                `bson:"distance" json:"distance"`
}

// GlowstickDocumentSOA represents documents in ChromaDB-compatible SOA (Struct of Arrays) format.
// This format is more efficient for batch operations as embeddings are already in the flat
// layout required by FAISS, avoiding per-document copy operations.
type GlowstickDocumentSOA struct {
	Ids        []string                 `json:"ids" bson:"ids"`
	Contents   []string                 `json:"contents" bson:"contents"`
	Embeddings []float32                `json:"embeddings" bson:"embeddings"` // Flat array: [doc1_emb..., doc2_emb..., ...]
	Metadatas  []map[string]interface{} `json:"metadatas" bson:"metadatas"`
}

// Validate checks that all arrays have consistent lengths and embeddings dimension is valid.
// Returns error with detailed context if validation fails.
func (soa *GlowstickDocumentSOA) Validate() error {
	numDocs := len(soa.Ids)

	if numDocs == 0 {
		return fmt.Errorf("ids array cannot be empty")
	}

	if len(soa.Contents) != numDocs {
		return fmt.Errorf("length mismatch: ids=%d, contents=%d", numDocs, len(soa.Contents))
	}

	if len(soa.Metadatas) != numDocs {
		return fmt.Errorf("length mismatch: ids=%d, metadatas=%d", numDocs, len(soa.Metadatas))
	}

	if len(soa.Embeddings) == 0 {
		return fmt.Errorf("embeddings array cannot be empty")
	}

	if len(soa.Embeddings)%numDocs != 0 {
		return fmt.Errorf("embeddings length (%d) must be divisible by number of documents (%d)",
			len(soa.Embeddings), numDocs)
	}

	embeddingDim := len(soa.Embeddings) / numDocs
	if embeddingDim == 0 {
		return fmt.Errorf("embedding dimension cannot be zero")
	}

	return nil
}

// EmbeddingDimension returns the dimension of each embedding vector.
// Assumes Validate() has already been called.
func (soa *GlowstickDocumentSOA) EmbeddingDimension() int {
	if len(soa.Ids) == 0 {
		return 0
	}
	return len(soa.Embeddings) / len(soa.Ids)
}

// DocumentCount returns the number of documents in the SOA structure.
func (soa *GlowstickDocumentSOA) DocumentCount() int {
	return len(soa.Ids)
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
	InsertDocumentsSOA(collection_name string, documents *GlowstickDocumentSOA) error
	GetDocuments(collection_name string) ([]GlowstickDocument, error)
	DeleteDocuments(collection_name string, documentIds []string) error
	QueryCollection(collection_name string, query QueryStruct) ([]GlowstickQueryResultSet, error)
	UpdateDocuments(collection_name string, payload *DocUpdatePayload) error
}

type DbParams struct {
	Name        string
	PutIfAbsent bool
	KvService   wt.WTService
	Logger      *zap.SugaredLogger
}

func DatabaseService(params DbParams) DBService {
	return &GDBService{
		Name:      params.Name,
		KvService: params.KvService,
		Logger:    params.Logger,
	}
}
