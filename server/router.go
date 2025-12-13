package server

import (
	dbservice "achillesdb/pkgs/db_service"
	"encoding/json"
	"fmt"

	"github.com/fasthttp/router"
	"github.com/valyala/fasthttp"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type GlowstickDocumentPayload struct {
	Id        string                 `json:"id"`
	Content   string                 `json:"content"`
	Embedding []float32              `json:"embedding"`
	Metadata  map[string]interface{} `json:"metadata"` // Any JSON-serializable type
}

func Router() *router.Router {
	r := router.New()
	apiV1 := r.Group("/api/v1")
	apiV1.POST("/database", CreateDB)
	apiV1.POST("/database/{database_name}/collections", CreateCollection)
	apiV1.GET("/database/{database_name}/collections", ListCollections)
	apiV1.GET("/database/{database_name}/collections/{collection_name}", GetCollection)
	apiV1.PUT("/database/{database_name}/collections/{collection_name}/documents", InsertDocumentsHndlr)
	apiV1.GET("/database/{database_name}/collections/{collection_name}/documents", GetDocumentsHandler)
	apiV1.POST("/database/{database_name}/collections/{collection_name}/documents", QueryDocumentsHandler)
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

	err_code, err := dbSvc.CreateDB()

	if err_code == dbservice.Err_Db_Exists {
		ctx.SetStatusCode(fasthttp.StatusConflict)
		ctx.SetContentType("application/json")
		ctx.Write([]byte(`{"error":"Database already exists"}`))
		return
	}

	if err != nil {
		fmt.Println("Error creating database: ", err)
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString("Server error")
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	ctx.Write([]byte(`{"message":"Database created successfully"}`))
}

func ListCollections(ctx *fasthttp.RequestCtx) {
	database_name := ctx.UserValue("database_name").(string)
	db := dbservice.DatabaseService(dbservice.DbParams{
		Name:      database_name,
		KvService: wtService,
	})
	collections, err := db.ListCollections()
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
		return
	}
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	jsonBytes, err := json.Marshal(collections)
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
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
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
		id := doc.Id
		// End users pass Id fields in the payload.
		// It's import to not mix ObjectId with user intended id's as the identifier of the document in the db
		// ( i'm not yet exactly sure why, but let's see! )
		// When users want to update, they can use the ID field too, but it'll be inside the metadata object.
		if id != "" {
			doc.Metadata["id"] = id
		}
		documents[i] = dbservice.GlowstickDocument{
			Id:        primitive.NewObjectID().Hex(),
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
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
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
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")

	jsonBytes, err := json.Marshal(docs)
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
		TopK           int       `json:"top_k"`
		QueryEmbedding []float32 `json:"query_embedding"`
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
	}
	docs, err := db.QueryCollection(collection, data)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")

	jsonBytes, err := json.Marshal(docs)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
		return
	}
	ctx.Write(jsonBytes)
}
