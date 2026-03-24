package benchmarks

import (
	"encoding/json"
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// BenchmarkInsertHTTPParsing measures the overhead of JSON parsing, validation,
// and embedding flattening that happens in the HTTP handler before InsertDocuments.
// This isolates the handler-level cost so you can confirm it's negligible.
//
//	go test -bench=BenchmarkInsertHTTPParsing -benchmem ./cmd/benchmarks/
func BenchmarkInsertHTTPParsing(b *testing.B) {
	cases := []struct {
		name string
		docs int
		dim  int
	}{
		{"50docs_1536dim", 50, 1536},
		{"100docs_1536dim", 100, 1536},
		{"500docs_1536dim", 500, 1536},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			// Pre-build the JSON payload (simulating what arrives over HTTP)
			payload := buildHTTPPayload(tc.docs, tc.dim)
			jsonBytes, err := json.Marshal(payload)
			if err != nil {
				b.Fatalf("failed to marshal payload: %v", err)
			}
			b.SetBytes(int64(len(jsonBytes)))

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Simulate the HTTP handler parsing path
				var req struct {
					Ids        []string                 `json:"ids"`
					Documents  []string                 `json:"documents"`
					Embeddings [][]float32              `json:"embeddings"`
					Metadatas  []map[string]interface{} `json:"metadatas"`
				}

				if err := json.Unmarshal(jsonBytes, &req); err != nil {
					b.Fatalf("unmarshal failed: %v", err)
				}

				// Duplicate ID check
				numDocs := len(req.Ids)
				seen := make(map[string]struct{}, numDocs)
				for _, id := range req.Ids {
					if _, exists := seen[id]; exists {
						b.Fatalf("duplicate id: %s", id)
					}
					seen[id] = struct{}{}
				}

				// Flatten embeddings ([][]float32 → []float32)
				embeddingDim := len(req.Embeddings[0])
				flatEmbeddings := make([]float32, numDocs*embeddingDim)
				for j, embedding := range req.Embeddings {
					copy(flatEmbeddings[j*embeddingDim:(j+1)*embeddingDim], embedding)
				}
			}
		})
	}
}

type httpPayload struct {
	Ids        []string                 `json:"ids"`
	Documents  []string                 `json:"documents"`
	Embeddings [][]float32              `json:"embeddings"`
	Metadatas  []map[string]interface{} `json:"metadatas"`
}

func buildHTTPPayload(count, dim int) httpPayload {
	p := httpPayload{
		Ids:        make([]string, count),
		Documents:  make([]string, count),
		Embeddings: make([][]float32, count),
		Metadatas:  make([]map[string]interface{}, count),
	}
	for i := 0; i < count; i++ {
		p.Ids[i] = primitive.NewObjectID().Hex()
		p.Documents[i] = fmt.Sprintf("Benchmark document %d", i+1)
		p.Embeddings[i] = genNormalizedEmbeddings(dim)
		p.Metadatas[i] = map[string]interface{}{"type": "benchmark", "index": i + 1}
	}
	return p
}
