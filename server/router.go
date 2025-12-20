package server

import (
	dbservice "achillesdb/pkgs/db_service"
	"encoding/json"
	"fmt"

	"github.com/fasthttp/router"
	"github.com/valyala/fasthttp"
)

type GlowstickDocumentPayload struct {
	Id        string                 `json:"id"`
	Content   string                 `json:"content"`
	Embedding []float32              `json:"embedding"`
	Metadata  map[string]interface{} `json:"metadata"` // Any JSON-serializable type
}

type QueryResponse struct {
	Documents []dbservice.GlowstickDocument `json:"documents"`
	DocCount  int                           `json:"doc_count"`
}

// handleError handles database errors and sets appropriate HTTP responses
func handleError(ctx *fasthttp.RequestCtx, err error) {
	var dbErr *dbservice.DBError
	if errors, ok := err.(*dbservice.DBError); ok {
		dbErr = errors
	} else {
		// If not a DBError, treat as internal error
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetContentType("application/json")
		ctx.Write([]byte(fmt.Sprintf(`{"error":"%s"}`, err.Error())))
		return
	}

	ctx.SetStatusCode(dbErr.HTTPStatus())
	ctx.SetContentType("application/json")
	ctx.Write([]byte(fmt.Sprintf(`{"error":"%s"}`, dbErr.Error())))
}

func Router() *router.Router {
	r := router.New()

	// OpenAPI documentation routes
	r.GET("/docs", SwaggerUIHandler)
	r.GET("/api/v1/openapi.yaml", OpenAPISpecHandler)

	apiV1 := r.Group("/api/v1")
	apiV1.POST("/database", CreateDB)
	apiV1.GET("/databases", ListDatabasesHandler)
	apiV1.DELETE("/database/{database_name}", DeleteDBHandler)
	apiV1.POST("/database/{database_name}/collections", CreateCollection)
	apiV1.GET("/database/{database_name}/collections", ListCollections)
	apiV1.DELETE("/database/{database_name}/collections/{collection_name}", DeleteCollectionHandler)
	apiV1.GET("/database/{database_name}/collections/{collection_name}", GetCollection)
	apiV1.POST("/database/{database_name}/collections/{collection_name}/documents", InsertDocumentsHndlr)
	apiV1.GET("/database/{database_name}/collections/{collection_name}/documents", GetDocumentsHandler)
	apiV1.DELETE("/database/{database_name}/collections/{collection_name}/documents", DeleteDocumentsHandler)
	apiV1.POST("/database/{database_name}/collections/{collection_name}/documents/query", QueryDocumentsHandler)
	apiV1.PUT("/database/{database_name}/collections/{collection_name}/documents", UpdateDocumentsHandler)
	return r
}

func CreateDB(ctx *fasthttp.RequestCtx) {
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
	db := dbservice.DatabaseService(dbservice.DbParams{
		Name:      "",
		KvService: wtService,
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
	database_name := ctx.UserValue("database_name").(string)
	db := dbservice.DatabaseService(dbservice.DbParams{
		Name:      database_name,
		KvService: wtService,
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
	database_name := ctx.UserValue("database_name").(string)
	collection_name := ctx.UserValue("collection_name").(string)
	db := dbservice.DatabaseService(dbservice.DbParams{
		Name:      database_name,
		KvService: wtService,
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
	database_name := ctx.UserValue("database_name").(string)
	collection_name := ctx.UserValue("collection_name").(string)

	var requestBody struct {
		Documents []GlowstickDocumentPayload `json:"documents"`
	}

	if err := json.Unmarshal(ctx.Request.Body(), &requestBody); err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString("Invalid JSON payload")
		return
	}

	documents := make([]dbservice.GlowstickDocument, len(requestBody.Documents))
	for i, doc := range requestBody.Documents {
		documents[i] = dbservice.GlowstickDocument{
			Id:        doc.Id,
			Content:   doc.Content,
			Embedding: doc.Embedding,
			Metadata:  doc.Metadata,
		}
	}

	db := dbservice.DatabaseService(dbservice.DbParams{
		Name:      database_name,
		KvService: wtService,
	})

	err := db.InsertDocuments(collection_name, documents)
	if err != nil {
		handleError(ctx, err)
		return
	}
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	ctx.Write([]byte(`{"message":"Documents inserted into collection successfully"}`))
}
func GetDocumentsHandler(ctx *fasthttp.RequestCtx) {
	var database = ctx.UserValue("database_name").(string)
	var collection = ctx.UserValue("collection_name").(string)

	db := dbservice.DatabaseService(dbservice.DbParams{
		Name:      database,
		KvService: wtService,
	})

	docs, err := db.GetDocuments(collection)
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

func QueryDocumentsHandler(ctx *fasthttp.RequestCtx) {
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
	database_name := ctx.UserValue("database_name").(string)

	db := dbservice.DatabaseService(dbservice.DbParams{
		Name:      database_name,
		KvService: wtService,
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
	database_name := ctx.UserValue("database_name").(string)
	collection_name := ctx.UserValue("collection_name").(string)

	db := dbservice.DatabaseService(dbservice.DbParams{
		Name:      database_name,
		KvService: wtService,
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
