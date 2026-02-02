package server

import (
	dbservice "achillesdb/pkgs/db_service"
	"encoding/json"

	"github.com/valyala/fasthttp"
)

func CreateCollection(ctx *fasthttp.RequestCtx) {
	log := getLogger(ctx)
	var db_name = ctx.UserValue("database_name").(string)

	var requestBody struct {
		Name string `json:"name"`
	}

	if err := json.Unmarshal(ctx.Request.Body(), &requestBody); err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString("Invalid JSON payload")
		return
	}

	collection_name := requestBody.Name
	log.Infow("creating_collection", "db_name", db_name, "collection_name", collection_name)

	db := dbservice.DatabaseService(dbservice.DbParams{
		Name:      db_name,
		KvService: wtService,
		Logger:    log,
	})
	err := db.CreateCollection(collection_name)

	if err != nil {
		handleError(ctx, err)
		return
	}

	log.Infow("collection_created", "db_name", db_name, "collection_name", collection_name)
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	ctx.Write([]byte(`{"message":"Collection created successfully"}`))
}
