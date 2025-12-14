package server

import (
	dbservice "achillesdb/pkgs/db_service"
	"encoding/json"

	"github.com/valyala/fasthttp"
)

func CreateCollection(ctx *fasthttp.RequestCtx) {
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
	db := dbservice.DatabaseService(dbservice.DbParams{
		Name:      db_name,
		KvService: wtService,
	})
	err := db.CreateCollection(collection_name)

	if err != nil {
		handleError(ctx, err)
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	ctx.Write([]byte(`{"message":"Collection created successfully"}`))
}
