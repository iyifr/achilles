package server

import (
	dbservice "achillesdb/pkgs/db_service"
	logger "achillesdb/pkgs/logger"
	"encoding/json"
	"fmt"

	"github.com/fasthttp/router"
	"github.com/valyala/fasthttp"
	"go.uber.org/zap"
)

type GlowstickDocumentPayload struct {
	Id        string                 `json:"id"`
	Content   string                 `json:"content"`
	Embedding []float32              `json:"embedding"`
	Metadata  map[string]interface{} `json:"metadata"` // Any JSON-serializable type
}

type QueryResponse struct {
	Documents []dbservice.GlowstickQueryResultSet `json:"documents"`
	DocCount  int                                 `json:"doc_count"`
}
type GetDocumentsResponse struct {
	Documents []dbservice.GlowstickDocument `json:"documents"`
	DocCount  int                           `json:"doc_count"`
}

// getLogger extracts logger from request context or returns global logger
func getLogger(ctx *fasthttp.RequestCtx) *zap.SugaredLogger {
	if logVal := ctx.UserValue("logger"); logVal != nil {
		return logVal.(*zap.SugaredLogger)
	}
	return logger.GetSugaredLogger()
}

// handleError handles database errors and sets appropriate HTTP responses
func handleError(ctx *fasthttp.RequestCtx, err error) {
	log := getLogger(ctx)
	var dbErr *dbservice.DBError
	if errors, ok := err.(*dbservice.DBError); ok {
		dbErr = errors
	} else {
		// If not a DBError, treat as internal error
		log.Errorw("untyped_error", "error", err)
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetContentType("application/json")
		ctx.Write([]byte(fmt.Sprintf(`{"error":"%s"}`, err.Error())))
		return
	}

	log.Errorw("db_error", "error_code", dbErr.Code, "error", dbErr.Error(), "http_status", dbErr.HTTPStatus())
	ctx.SetStatusCode(dbErr.HTTPStatus())
	ctx.SetContentType("application/json")
	ctx.Write([]byte(fmt.Sprintf(`{"error":"%s"}`, dbErr.Error())))
}

func Router() *router.Router {
	r := router.New()

	// OpenAPI documentation routes
	r.GET("/docs", LoggingMiddleware(SwaggerUIHandler))
	r.GET("/api/v1/openapi.yaml", LoggingMiddleware(OpenAPISpecHandler))

	apiV1 := r.Group("/api/v1")
	apiV1.POST("/database", LoggingMiddleware(CreateDB))
	apiV1.GET("/databases", LoggingMiddleware(ListDatabasesHandler))
	apiV1.DELETE("/database/{database_name}", LoggingMiddleware(DeleteDBHandler))
	apiV1.POST("/database/{database_name}/collections", LoggingMiddleware(CreateCollection))
	apiV1.GET("/database/{database_name}/collections", LoggingMiddleware(ListCollections))
	apiV1.DELETE("/database/{database_name}/collections/{collection_name}", LoggingMiddleware(DeleteCollectionHandler))
	apiV1.GET("/database/{database_name}/collections/{collection_name}", LoggingMiddleware(GetCollection))
	apiV1.POST("/database/{database_name}/collections/{collection_name}/documents", LoggingMiddleware(InsertDocumentsHndlr))
	apiV1.GET("/database/{database_name}/collections/{collection_name}/documents", LoggingMiddleware(GetDocumentsHandler))
	apiV1.DELETE("/database/{database_name}/collections/{collection_name}/documents", LoggingMiddleware(DeleteDocumentsHandler))
	apiV1.POST("/database/{database_name}/collections/{collection_name}/documents/query", LoggingMiddleware(QueryDocumentsHandler))
	apiV1.PUT("/database/{database_name}/collections/{collection_name}/documents", LoggingMiddleware(UpdateDocumentsHandler))
	return r
}

func CreateDB(ctx *fasthttp.RequestCtx) {
	log := getLogger(ctx)

	var requestBody struct {
		Db_name string `json:"name"`
	}

	var dbName string = "default"

	if len(ctx.Request.Body()) > 0 {
		if err := json.Unmarshal(ctx.Request.Body(), &requestBody); err != nil {
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			ctx.WriteString("Invalid JSON payload")
			return
		}

		if len(requestBody.Db_name) > 0 {
			dbName = requestBody.Db_name
		}
	}

	params := dbservice.DbParams{
		Name:      dbName,
		KvService: wtService,
		Logger:    log,
	}

	dbSvc := dbservice.DatabaseService(params)
	err := dbSvc.CreateDB()

	if err != nil {
		handleError(ctx, err)
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	ctx.Write([]byte(`{"message":"Database created successfully"}`))
}

func ListDatabasesHandler(ctx *fasthttp.RequestCtx) {
	log := getLogger(ctx)

	db := dbservice.DatabaseService(dbservice.DbParams{
		Name:      "",
		KvService: wtService,
		Logger:    log,
	})

	result, err := db.ListDatabases()
	if err != nil {
		handleError(ctx, err)
		return
	}

	databases := result.Databases
	if databases == nil {
		databases = []dbservice.DatabaseInfo{}
	}

	response := struct {
		Databases []dbservice.DatabaseInfo `json:"databases"`
		DbCount   int                      `json:"db_count"`
	}{
		Databases: databases,
		DbCount:   len(databases),
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	jsonBytes, err := json.Marshal(response)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
		return
	}
	ctx.Write(jsonBytes)
}

func ListCollections(ctx *fasthttp.RequestCtx) {
	log := getLogger(ctx)
	database_name := ctx.UserValue("database_name").(string)

	db := dbservice.DatabaseService(dbservice.DbParams{
		Name:      database_name,
		KvService: wtService,
		Logger:    log,
	})
	collections, err := db.ListCollections()
	if err != nil {
		handleError(ctx, err)
		return
	}

	response := struct {
		Collections     []dbservice.CollectionCatalogEntry `json:"collections"`
		CollectionCount int                                `json:"collection_count"`
	}{
		Collections:     collections,
		CollectionCount: len(collections),
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	jsonBytes, err := json.Marshal(response)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
		return
	}
	ctx.Write(jsonBytes)
}

func GetCollection(ctx *fasthttp.RequestCtx) {
	log := getLogger(ctx)
	database_name := ctx.UserValue("database_name").(string)
	collection_name := ctx.UserValue("collection_name").(string)

	db := dbservice.DatabaseService(dbservice.DbParams{
		Name:      database_name,
		KvService: wtService,
		Logger:    log,
	})
	collection, err := db.GetCollection(collection_name)
	if err != nil {
		handleError(ctx, err)
		return
	}
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	jsonBytes, err := json.Marshal(collection)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
		return
	}
	ctx.Write(jsonBytes)
}

func InsertDocumentsHndlr(ctx *fasthttp.RequestCtx) {
	log := getLogger(ctx)
	database_name := ctx.UserValue("database_name").(string)
	collection_name := ctx.UserValue("collection_name").(string)

	var soaRequest struct {
		Ids        []string                 `json:"ids"`
		Documents  []string                 `json:"documents"`  // Maps to Contents
		Embeddings [][]float32              `json:"embeddings"` // Array of arrays
		Metadatas  []map[string]interface{} `json:"metadatas"`
	}

	if err := json.Unmarshal(ctx.Request.Body(), &soaRequest); err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetContentType("application/json")
		ctx.Write([]byte(`{"error":"Invalid JSON: expected {ids, documents, embeddings, metadatas}"}`))
		return
	}

	// Validate array lengths
	numDocs := len(soaRequest.Ids)
	if numDocs == 0 {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetContentType("application/json")
		ctx.Write([]byte(`{"error":"ids array cannot be empty"}`))
		return
	}

	if len(soaRequest.Documents) != numDocs ||
		len(soaRequest.Embeddings) != numDocs ||
		len(soaRequest.Metadatas) != numDocs {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetContentType("application/json")
		ctx.Write([]byte(fmt.Sprintf(`{"error":"array length mismatch: ids=%d, docs=%d, emb=%d, meta=%d"}`,
			numDocs, len(soaRequest.Documents), len(soaRequest.Embeddings), len(soaRequest.Metadatas))))
		return
	}

	// Validate and flatten embeddings: [][]float32 → []float32
	if numDocs > 0 && len(soaRequest.Embeddings[0]) == 0 {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetContentType("application/json")
		ctx.Write([]byte(`{"error":"embedding vectors cannot be empty"}`))
		return
	}

	embeddingDim := len(soaRequest.Embeddings[0])
	flatEmbeddings := make([]float32, numDocs*embeddingDim)

	for i, embedding := range soaRequest.Embeddings {
		if len(embedding) != embeddingDim {
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			ctx.SetContentType("application/json")
			ctx.Write([]byte(fmt.Sprintf(`{"error":"dimension mismatch at index %d: expected %d, got %d"}`,
				i, embeddingDim, len(embedding))))
			return
		}
		copy(flatEmbeddings[i*embeddingDim:(i+1)*embeddingDim], embedding)
	}

	// Build SOA.
	soa := &dbservice.GlowstickDocumentSOA{
		Ids:        soaRequest.Ids,
		Contents:   soaRequest.Documents,
		Embeddings: flatEmbeddings,
		Metadatas:  soaRequest.Metadatas,
	}

	db := dbservice.DatabaseService(dbservice.DbParams{
		Name:      database_name,
		KvService: wtService,
		Logger:    log,
	})

	err := db.InsertDocumentsSOA(collection_name, soa)
	if err != nil {
		handleError(ctx, err)
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	ctx.Write([]byte(`{"message":"Documents inserted successfully"}`))
}
func GetDocumentsHandler(ctx *fasthttp.RequestCtx) {
	log := getLogger(ctx)
	var database = ctx.UserValue("database_name").(string)
	var collection = ctx.UserValue("collection_name").(string)

	db := dbservice.DatabaseService(dbservice.DbParams{
		Name:      database,
		KvService: wtService,
		Logger:    log,
	})

	docs, err := db.GetDocuments(collection)
	if err != nil {
		handleError(ctx, err)
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")

	response := GetDocumentsResponse{
		Documents: docs,
		DocCount:  len(docs),
	}

	jsonBytes, err := json.Marshal(response)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
		return
	}
	ctx.Write(jsonBytes)
}

func QueryDocumentsHandler(ctx *fasthttp.RequestCtx) {
	log := getLogger(ctx)
	var database = ctx.UserValue("database_name").(string)
	var collection = ctx.UserValue("collection_name").(string)

	var requestBody struct {
		TopK           int            `json:"top_k"`
		QueryEmbedding []float32      `json:"query_embedding"`
		Filters        map[string]any `json:"where"`
	}

	if err := json.Unmarshal(ctx.Request.Body(), &requestBody); err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString("Invalid JSON payload")
		return
	}

	db := dbservice.DatabaseService(dbservice.DbParams{
		Name:      database,
		KvService: wtService,
		Logger:    log,
	})

	var data = dbservice.QueryStruct{
		TopK:           int32(requestBody.TopK),
		QueryEmbedding: requestBody.QueryEmbedding,
		MaxDistance:    0,
		Filters:        requestBody.Filters,
	}
	docs, err := db.QueryCollection(collection, data)
	if err != nil {
		handleError(ctx, err)
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")

	response := QueryResponse{
		Documents: docs,
		DocCount:  len(docs),
	}

	jsonBytes, err := json.Marshal(response)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
		return
	}
	ctx.Write(jsonBytes)
}

func UpdateDocumentsHandler(ctx *fasthttp.RequestCtx) {
	log := getLogger(ctx)
	database_name := ctx.UserValue("database_name").(string)
	collection_name := ctx.UserValue("collection_name").(string)

	var requestBody struct {
		DocumentId string         `json:"document_id"`
		Where      map[string]any `json:"where"`
		Updates    map[string]any `json:"updates"`
	}

	if err := json.Unmarshal(ctx.Request.Body(), &requestBody); err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString("Invalid JSON payload")
		return
	}

	if len(requestBody.Updates) == 0 {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString("Updates field is required and cannot be empty")
		return
	}

	db := dbservice.DatabaseService(dbservice.DbParams{
		Name:      database_name,
		KvService: wtService,
		Logger:    log,
	})

	payload := &dbservice.DocUpdatePayload{
		DocumentId: requestBody.DocumentId,
		Where:      requestBody.Where,
		Updates:    requestBody.Updates,
	}

	err := db.UpdateDocuments(collection_name, payload)
	if err != nil {
		handleError(ctx, err)
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	ctx.Write([]byte(`{"message":"Documents updated successfully"}`))
}

func DeleteDBHandler(ctx *fasthttp.RequestCtx) {
	log := getLogger(ctx)
	database_name := ctx.UserValue("database_name").(string)

	db := dbservice.DatabaseService(dbservice.DbParams{
		Name:      database_name,
		KvService: wtService,
		Logger:    log,
	})

	err := db.DeleteDB(database_name)
	if err != nil {
		handleError(ctx, err)
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	ctx.Write([]byte(`{"message":"Database deleted successfully"}`))
}

func DeleteCollectionHandler(ctx *fasthttp.RequestCtx) {
	log := getLogger(ctx)
	database_name := ctx.UserValue("database_name").(string)
	collection_name := ctx.UserValue("collection_name").(string)

	db := dbservice.DatabaseService(dbservice.DbParams{
		Name:      database_name,
		KvService: wtService,
		Logger:    log,
	})

	err := db.DeleteCollection(collection_name)
	if err != nil {
		handleError(ctx, err)
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	ctx.Write([]byte(`{"message":"Collection deleted successfully"}`))
}

func DeleteDocumentsHandler(ctx *fasthttp.RequestCtx) {
	log := getLogger(ctx)
	database_name := ctx.UserValue("database_name").(string)
	collection_name := ctx.UserValue("collection_name").(string)

	var requestBody struct {
		DocumentIds []string `json:"document_ids"`
	}

	if err := json.Unmarshal(ctx.Request.Body(), &requestBody); err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString("Invalid JSON payload")
		return
	}

	db := dbservice.DatabaseService(dbservice.DbParams{
		Name:      database_name,
		KvService: wtService,
		Logger:    log,
	})

	err := db.DeleteDocuments(collection_name, requestBody.DocumentIds)
	if err != nil {
		handleError(ctx, err)
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	ctx.Write([]byte(`{"message":"Documents deleted successfully"}`))
}
