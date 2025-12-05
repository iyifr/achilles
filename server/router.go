package server

import (
	dbservice "achillesdb/pkgs/db_service"
	"encoding/json"
	"fmt"

	"github.com/fasthttp/router"
	"github.com/valyala/fasthttp"
)

func Router() *router.Router {
	r := router.New()
	apiV1 := r.Group("/api/v1")
	apiV1.POST("/database", CreateDB)
	apiV1.POST("/database/{database_name}/collections", CreateCollection)
	apiV1.GET("/database/{database_name}/collections", ListCollections)
	apiV1.GET("/database/{database_name}/collections/{collection_name}", GetCollection)
	return r
}

func CreateDB(ctx *fasthttp.RequestCtx) {

	params := dbservice.DbParams{
		Name:      "default",
		KvService: wtService,
	}

	dbSvc := dbservice.DatabaseService(params)

	err := dbSvc.CreateDB()
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
