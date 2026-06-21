package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

type route struct {
	Method  string
	Path    string
	Handler string
}

var routeRegex = regexp.MustCompile(`apiV1\.(GET|POST|PUT|DELETE)\("([^"]+)",\s*(?:LoggingMiddleware\(|ServerRouteHandler\()([A-Za-z_][A-Za-z0-9_]*)`)
var pathParamRegex = regexp.MustCompile(`\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// findRepoRoot walks upward from cwd until server/router.go exists (repo root), or cwd is server/ with router.go.
func findRepoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "server", "router.go")); err == nil {
			return dir, nil
		}
		if _, err := os.Stat(filepath.Join(dir, "router.go")); err == nil {
			return filepath.Dir(dir), nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("server/router.go not found")
		}
		dir = parent
	}
}

func main() {
	// go generate runs with cwd = the package dir (e.g. server/), not the repo root.
	repoRoot, err := findRepoRoot()
	if err != nil {
		fail("failed to locate repo root: %v", err)
	}

	routerPath := filepath.Join(repoRoot, "server", "router.go")
	outputPath := filepath.Join(repoRoot, "server", "openapi_gen.go")

	routes, err := parseRoutes(routerPath)
	if err != nil {
		fail("failed to parse routes: %v", err)
	}

	spec := buildOpenAPISpec(routes)
	generated := renderGeneratedFile(spec)

	if err := os.WriteFile(outputPath, []byte(generated), 0o644); err != nil {
		fail("failed to write generated file: %v", err)
	}
}

func parseRoutes(path string) ([]route, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	matches := routeRegex.FindAllStringSubmatch(string(raw), -1)
	routes := make([]route, 0, len(matches))
	for _, m := range matches {
		routes = append(routes, route{
			Method:  strings.ToUpper(m[1]),
			Path:    m[2],
			Handler: m[3],
		})
	}
	return routes, nil
}

func buildOpenAPISpec(routes []route) string {
	var b strings.Builder
	b.WriteString("openapi: 3.0.3\n")
	b.WriteString("info:\n")
	b.WriteString("  title: AchillesDB.\n")
	b.WriteString("  description: Vector database API for storing and querying documents with embeddings\n")
	b.WriteString("  version: 1.0.0\n")
	b.WriteString("servers:\n")
	b.WriteString("  - url: /api/v1\n")
	b.WriteString("    description: API v1\n")
	b.WriteString("paths:\n")

	paths := uniquePaths(routes)
	for _, path := range paths {
		b.WriteString(fmt.Sprintf("  %s:\n", path))
		pathRoutes := routesForPath(routes, path)
		slices.SortFunc(pathRoutes, func(a, c route) int {
			return methodOrder(a.Method) - methodOrder(c.Method)
		})
		for _, r := range pathRoutes {
			op := operationForRoute(r)
			writeOperation(&b, r.Method, r.Path, op)
		}
	}

	b.WriteString(staticComponents)
	return b.String()
}

func uniquePaths(routes []route) []string {
	seen := make(map[string]struct{}, len(routes))
	paths := make([]string, 0, len(routes))
	for _, r := range routes {
		if _, ok := seen[r.Path]; ok {
			continue
		}
		seen[r.Path] = struct{}{}
		paths = append(paths, r.Path)
	}
	return paths
}

func routesForPath(routes []route, path string) []route {
	filtered := make([]route, 0, 4)
	for _, r := range routes {
		if r.Path == path {
			filtered = append(filtered, r)
		}
	}
	return filtered
}

func methodOrder(method string) int {
	switch method {
	case "GET":
		return 0
	case "POST":
		return 1
	case "PUT":
		return 2
	case "DELETE":
		return 3
	default:
		return 99
	}
}

type operation struct {
	Summary        string
	Description    string
	Tag            string
	RequestSchema  string
	ResponseSchema string
	Responses      []int
}

func operationForRoute(r route) operation {
	key := r.Method + " " + r.Path
	if op, ok := operationLookup[key]; ok {
		return op
	}
	return operation{
		Summary:        strings.ReplaceAll(strings.TrimSuffix(r.Handler, "Handler"), "_", " "),
		Tag:            "Misc",
		ResponseSchema: "MessageResponse",
		Responses:      []int{200},
	}
}

func writeOperation(b *strings.Builder, method string, path string, op operation) {
	b.WriteString(fmt.Sprintf("    %s:\n", strings.ToLower(method)))
	b.WriteString(fmt.Sprintf("      summary: %s\n", op.Summary))
	if op.Description != "" {
		b.WriteString("      description: |\n")
		for _, line := range strings.Split(op.Description, "\n") {
			b.WriteString(fmt.Sprintf("        %s\n", line))
		}
	}
	b.WriteString("      tags:\n")
	b.WriteString(fmt.Sprintf("        - %s\n", op.Tag))

	parameters := pathParametersForOperation(path)
	if len(parameters) > 0 {
		b.WriteString("      parameters:\n")
		for _, p := range parameters {
			b.WriteString(fmt.Sprintf("        - name: %s\n", p))
			b.WriteString("          in: path\n")
			b.WriteString("          required: true\n")
			b.WriteString("          schema:\n")
			b.WriteString("            type: string\n")
		}
	}

	if op.RequestSchema != "" {
		b.WriteString("      requestBody:\n")
		b.WriteString("        required: true\n")
		b.WriteString("        content:\n")
		b.WriteString("          application/json:\n")
		b.WriteString("            schema:\n")
		b.WriteString(fmt.Sprintf("              $ref: '#/components/schemas/%s'\n", op.RequestSchema))
	}

	b.WriteString("      responses:\n")
	for _, code := range op.Responses {
		b.WriteString(fmt.Sprintf("        '%d':\n", code))
		b.WriteString(fmt.Sprintf("          description: %s\n", statusDescription(code)))
		b.WriteString("          content:\n")
		b.WriteString("            application/json:\n")
		b.WriteString("              schema:\n")
		b.WriteString(fmt.Sprintf("                $ref: '#/components/schemas/%s'\n", schemaForStatus(op, code)))
	}
}

func statusDescription(code int) string {
	switch code {
	case 200:
		return "Success"
	case 400:
		return "Bad request"
	case 404:
		return "Not found"
	case 409:
		return "Conflict"
	default:
		return "Response"
	}
}

func schemaForStatus(op operation, status int) string {
	if status == 200 {
		return op.ResponseSchema
	}
	return "ErrorResponse"
}

func pathParametersForOperation(path string) []string {
	matches := pathParamRegex.FindAllStringSubmatch(path, -1)
	params := make([]string, 0, len(matches))
	for _, m := range matches {
		params = append(params, m[1])
	}
	return params
}

var operationLookup = map[string]operation{
	"POST /database": {
		Summary:        "Create a database",
		Tag:            "Database",
		RequestSchema:  "CreateDatabaseReq",
		ResponseSchema: "MessageResponse",
		Responses:      []int{200, 409},
	},
	"GET /databases": {
		Summary:        "List all databases",
		Tag:            "Database",
		ResponseSchema: "ListDatabasesResponse",
		Responses:      []int{200},
	},
	"DELETE /database/{database_name}": {
		Summary:        "Delete a database",
		Tag:            "Database",
		ResponseSchema: "MessageResponse",
		Responses:      []int{200, 404},
	},
	"POST /database/{database_name}/collections": {
		Summary:        "Create a collection",
		Tag:            "Collections",
		RequestSchema:  "CreateCollectionReqInput",
		ResponseSchema: "MessageResponse",
		Responses:      []int{200, 409},
	},
	"GET /database/{database_name}/collections": {
		Summary:        "List all collections in a database",
		Tag:            "Collections",
		ResponseSchema: "ListCollectionsResponse",
		Responses:      []int{200},
	},
	"DELETE /database/{database_name}/collections/{collection_name}": {
		Summary:        "Delete a collection",
		Tag:            "Collections",
		ResponseSchema: "MessageResponse",
		Responses:      []int{200, 404},
	},
	"GET /database/{database_name}/collections/{collection_name}": {
		Summary:        "Get collection details",
		Tag:            "Collections",
		ResponseSchema: "CollectionEntry",
		Responses:      []int{200, 404},
	},
	"POST /database/{database_name}/collections/{collection_name}/documents": {
		Summary:        "Insert documents into a collection",
		Tag:            "Documents",
		RequestSchema:  "InsertDocumentReqInput",
		ResponseSchema: "MessageResponse",
		Responses:      []int{200, 400, 404, 409},
	},
	"GET /database/{database_name}/collections/{collection_name}/documents": {
		Summary:        "Get all documents in a collection",
		Tag:            "Documents",
		ResponseSchema: "GetDocumentsResponse",
		Responses:      []int{200, 404},
	},
	"DELETE /database/{database_name}/collections/{collection_name}/documents": {
		Summary:        "Delete documents from a collection",
		Tag:            "Documents",
		RequestSchema:  "DeleteDocumentsReqInput",
		ResponseSchema: "DeleteDocumentsResponse",
		Responses:      []int{200, 400, 404},
	},
	"POST /database/{database_name}/collections/{collection_name}/documents/query": {
		Summary:        "Query documents using vector similarity search",
		Tag:            "Documents",
		RequestSchema:  "QueryReqInput",
		ResponseSchema: "QueryResponse",
		Responses:      []int{200, 404},
	},
	"PUT /database/{database_name}/collections/{collection_name}/documents": {
		Summary:        "Update document metadata (single document by id or bulk by metadata filter)",
		Tag:            "Documents",
		RequestSchema:  "UpdateDocumentsReqInput",
		ResponseSchema: "UpdateDocumentsResponse",
		Responses:      []int{200, 400, 404},
	},
}

const staticComponents = `components:
  schemas:
    MessageResponse:
      type: object
      properties:
        message:
          type: string

    ErrorResponse:
      type: object
      properties:
        error:
          type: string

    CreateDatabaseReq:
      type: object
      properties:
        name:
          type: string
          description: Database name (defaults to "default")

    CreateCollectionReqInput:
      type: object
      properties:
        name:
          type: string
          description: Collection name

    InsertDocumentReqInput:
      type: object
      properties:
        ids:
          type: array
          items:
            type: string
        documents:
          type: array
          items:
            type: string
        embeddings:
          type: array
          items:
            type: array
            items:
              type: number
              format: float
        metadatas:
          type: array
          items:
            type: object
        upsert:
          type: boolean
          default: false
          description: If true, ids that already exist in the collection are replaced instead of rejected with a 409

    UpdateDocumentsReqInput:
      type: object
      required:
        - updates
      properties:
        document_id:
          type: string
          description: Target a single document by id (omit when using where)
        where:
          type: object
          description: Metadata filter for bulk update, same operators as vector query (omit when using document_id)
        updates:
          type: object
          description: Metadata fields to merge into matching document(s)

    UpdateDocumentsResponse:
      type: object
      properties:
        message:
          type: string
        updated_count:
          type: integer
          description: Number of documents updated (1 for single-document update)

    DeleteDocumentsReqInput:
      type: object
      required:
        - document_ids
      properties:
        document_ids:
          type: array
          items:
            type: string

    QueryReqInput:
      type: object
      properties:
        top_k:
          type: integer
          default: 10
        query_embedding:
          type: array
          items:
            type: number
            format: float
        where:
          type: object
      required:
        - query_embedding

    Document:
      type: object
      properties:
        id:
          type: string
        content:
          type: string
        metadata:
          type: object

    QueryDocument:
      type: object
      properties:
        id:
          type: string
        content:
          type: string
        metadata:
          type: object
        distance:
          type: number
          format: float

    QueryResponse:
      type: object
      properties:
        documents:
          type: array
          items:
            $ref: '#/components/schemas/QueryDocument'
        doc_count:
          type: integer

    GetDocumentsResponse:
      type: object
      properties:
        documents:
          type: array
          items:
            $ref: '#/components/schemas/Document'
        doc_count:
          type: integer

    CollectionCatalogEntry:
      type: object
      properties:
        _id:
          type: string
        ns:
          type: string
          description: Namespace (database.collection)
        table_uri:
          type: string
        vector_index_uri:
          type: string
        createdAt:
          type: string
          format: date-time
        updatedAt:
          type: string
          format: date-time

    CollectionStats:
      type: object
      properties:
        doc_count:
          type: integer
        vector_index_size:
          type: number

    CollectionEntry:
      type: object
      properties:
        collection:
          $ref: '#/components/schemas/CollectionCatalogEntry'
        stats:
          $ref: '#/components/schemas/CollectionStats'

    DatabaseInfo:
      type: object
      properties:
        name:
          type: string
          description: Database name
        collectionCount:
          type: integer
          description: Number of collections in this database
        empty:
          type: boolean
          description: Whether the database has no collections

    ListDatabasesResponse:
      type: object
      properties:
        databases:
          type: array
          items:
            $ref: '#/components/schemas/DatabaseInfo'
        db_count:
          type: integer
          description: Number of databases

    ListCollectionsResponse:
      type: object
      properties:
        collections:
          type: array
          items:
            $ref: '#/components/schemas/CollectionCatalogEntry'
        collection_count:
          type: integer
          description: Number of collections

    DeleteDocumentsResponse:
      type: object
      properties:
        deleted_count:
          type: integer
          description: Number of deleted documents
        deleted_ids:
          type: array
          items:
            type: string
          description: IDs of deleted documents
`

func renderGeneratedFile(spec string) string {
	return fmt.Sprintf(`// Code generated by cmd/openapi-gen; DO NOT EDIT.
package server

const openAPISpec = %q
`, spec)
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
