package server

import (
	"github.com/valyala/fasthttp"
)

const openAPISpec = `openapi: 3.0.3
info:
  title: AchillesDB.
  description: Vector database API for storing and querying documents with embeddings
  version: 1.0.0
servers:
  - url: /api/v1
    description: API v1
paths:
  /database:
    post:
      summary: Create a database
      tags:
        - Database
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                name:
                  type: string
                  description: Database name (defaults to "default")
      responses:
        '200':
          description: Database created successfully
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/MessageResponse'
        '409':
          description: Database already exists
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'

  /databases:
    get:
      summary: List all databases
      description: Returns a list of all databases with their sizes and empty status (MongoDB-style response)
      tags:
        - Database
      responses:
        '200':
          description: List of databases
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ListDatabasesResponse'

  /database/{database_name}:
    delete:
      summary: Delete a database
      tags:
        - Database
      parameters:
        - name: database_name
          in: path
          required: true
          schema:
            type: string
      responses:
        '200':
          description: Database deleted successfully
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/MessageResponse'
        '404':
          description: Database not found
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'

  /database/{database_name}/collections:
    get:
      summary: List all collections in a database
      tags:
        - Collections
      parameters:
        - name: database_name
          in: path
          required: true
          schema:
            type: string
      responses:
        '200':
          description: List of collections
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: '#/components/schemas/CollectionCatalogEntry'
    post:
      summary: Create a collection
      tags:
        - Collections
      parameters:
        - name: database_name
          in: path
          required: true
          schema:
            type: string
      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              required:
                - name
              properties:
                name:
                  type: string
                  description: Collection name
      responses:
        '200':
          description: Collection created successfully
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/MessageResponse'
        '409':
          description: Collection already exists
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'

  /database/{database_name}/collections/{collection_name}:
    get:
      summary: Get collection details
      tags:
        - Collections
      parameters:
        - name: database_name
          in: path
          required: true
          schema:
            type: string
        - name: collection_name
          in: path
          required: true
          schema:
            type: string
      responses:
        '200':
          description: Collection details
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/CollectionEntry'
        '404':
          description: Collection not found
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'
    delete:
      summary: Delete a collection
      tags:
        - Collections
      parameters:
        - name: database_name
          in: path
          required: true
          schema:
            type: string
        - name: collection_name
          in: path
          required: true
          schema:
            type: string
      responses:
        '200':
          description: Collection deleted successfully
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/MessageResponse'
        '404':
          description: Collection not found
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'

  /database/{database_name}/collections/{collection_name}/documents:
    get:
      summary: Get all documents in a collection
      tags:
        - Documents
      parameters:
        - name: database_name
          in: path
          required: true
          schema:
            type: string
        - name: collection_name
          in: path
          required: true
          schema:
            type: string
      responses:
        '200':
          description: List of documents
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/QueryResponse'
        '404':
          description: Collection not found
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'
    post:
      summary: Insert documents into a collection
      description: |
        Insert documents into AchillesDb
        All arrays (ids, documents, embeddings, metadatas) must have the same length.
        Each embedding array must have the same dimension.
      tags:
        - Documents
      parameters:
        - name: database_name
          in: path
          required: true
          schema:
            type: string
        - name: collection_name
          in: path
          required: true
          schema:
            type: string
      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              required:
                - ids
                - documents
                - embeddings
                - metadatas
              properties:
                ids:
                  type: array
                  items:
                    type: string
                  description: Array of unique document IDs
                  example: ["doc1", "doc2", "doc3"]
                documents:
                  type: array
                  items:
                    type: string
                  description: Array of document content/text
                  example: ["First document content", "Second document", "Third document"]
                embeddings:
                  type: array
                  items:
                    type: array
                    items:
                      type: number
                      format: float
                  description: Array of embedding vectors (one per document)
                  example: [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6], [0.7, 0.8, 0.9]]
                metadatas:
                  type: array
                  items:
                    type: object
                  description: Array of metadata objects (one per document)
                  example: [{"type": "test", "index": 1}, {"type": "test", "index": 2}, {"type": "test", "index": 3}]
            examples:
              upload docs :
                summary: (ChromaDB-compatible)
                value:
                  ids: ["doc1", "doc2"]
                  documents: ["First content", "Second content"]
                  embeddings: [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]]
                  metadatas: [{"source": "test"}, {"source": "test"}]
      responses:
        '200':
          description: Documents inserted successfully
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/MessageResponse'
        '400':
          description: Invalid input - array length mismatch, dimension mismatch, or empty arrays
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'
              examples:
                array_length_mismatch:
                  summary: Array length mismatch
                  value:
                    error: "array length mismatch: ids=100, docs=100, emb=99, meta=100"
                dimension_mismatch:
                  summary: Embedding dimension mismatch
                  value:
                    error: "dimension mismatch at index 42: expected 1536, got 512"
                empty_arrays:
                  summary: Empty arrays
                  value:
                    error: "ids array cannot be empty"
        '404':
          description: Collection not found
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'
    put:
      summary: Update document metadata
      tags:
        - Documents
      parameters:
        - name: database_name
          in: path
          required: true
          schema:
            type: string
        - name: collection_name
          in: path
          required: true
          schema:
            type: string
      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              required:
                - document_id
                - updates
              properties:
                document_id:
                  type: string
                  description: ID of the document to update
                where:
                  type: object
                  description: Filter conditions (reserved for future use)
                updates:
                  type: object
                  description: Metadata fields to update
      responses:
        '200':
          description: Documents updated successfully
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/MessageResponse'
        '404':
          description: Document not found
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'
    delete:
      summary: Delete documents from a collection
      tags:
        - Documents
      parameters:
        - name: database_name
          in: path
          required: true
          schema:
            type: string
        - name: collection_name
          in: path
          required: true
          schema:
            type: string
      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              required:
                - document_ids
              properties:
                document_ids:
                  type: array
                  items:
                    type: string
                  description: List of document IDs to delete
      responses:
        '200':
          description: Documents deleted successfully
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/MessageResponse'
        '400':
          description: Invalid input (empty document_ids)
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'
        '404':
          description: Collection not found
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'

  /database/{database_name}/collections/{collection_name}/documents/query:
    post:
      summary: Query documents using vector similarity search
      tags:
        - Documents
      parameters:
        - name: database_name
          in: path
          required: true
          schema:
            type: string
        - name: collection_name
          in: path
          required: true
          schema:
            type: string
      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              required:
                - query_embedding
              properties:
                top_k:
                  type: integer
                  description: Number of results to return
                  default: 10
                query_embedding:
                  type: array
                  items:
                    type: number
                    format: float
                  description: Query embedding vector
                where:
                  type: object
                  description: Metadata filter conditions
      responses:
        '200':
          description: Query results
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/QueryResponse'
        '404':
          description: Collection not found
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'

components:
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

    DocumentInput:
      type: object
      required:
        - id
        - embedding
      properties:
        id:
          type: string
          description: Unique document ID
        content:
          type: string
          description: Document content/text
        embedding:
          type: array
          items:
            type: number
            format: float
          description: Document embedding vector
        metadata:
          type: object
          description: Arbitrary metadata key-value pairs

    Document:
      type: object
      properties:
        id:
          type: string
        content:
          type: string
        metadata:
          type: object

    QueryResponse:
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
        documents:
          type: array
          items:
            $ref: '#/components/schemas/Document'
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
`

const swaggerUIHTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>AchillesDB API Documentation</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.11.0/swagger-ui.css">
    <style>
        body { margin: 0; padding: 0; }
        .topbar { display: none; }
    </style>
</head>
<body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5.11.0/swagger-ui-bundle.js"></script>
    <script>
        window.onload = function() {
            SwaggerUIBundle({
                url: "/api/v1/openapi.yaml",
                dom_id: '#swagger-ui',
                presets: [
                    SwaggerUIBundle.presets.apis,
                    SwaggerUIBundle.SwaggerUIStandalonePreset
                ],
                layout: "BaseLayout",
                deepLinking: true,
                showExtensions: true,
                showCommonExtensions: true
            });
        };
    </script>
</body>
</html>`

func OpenAPISpecHandler(ctx *fasthttp.RequestCtx) {
	ctx.SetContentType("application/x-yaml")
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.WriteString(openAPISpec)
}

func SwaggerUIHandler(ctx *fasthttp.RequestCtx) {
	ctx.SetContentType("text/html; charset=utf-8")
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.WriteString(swaggerUIHTML)
}
