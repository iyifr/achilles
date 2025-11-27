package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"

	"achillesdb/pkgs/faiss"
	"achillesdb/pkgs/wiredtiger"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Document represents the BSON schema stored in WiredTiger.
// embedding is stored as []float64 for BSON compatibility, but is produced by
// FAISS as normalized []float32 and converted to float64 before marshaling.
type Document struct {
	ID        primitive.ObjectID `bson:"_id"`
	Text      string             `bson:"text"`
	Embedding []float64          `bson:"embedding"`
}

var LABEL_TO_ID_TABLE_URI = "table:docId_vectorId"

func generateNormalizedVector(d int, faiss faiss.FAISSService) ([]float32, []float64) {
	vec := make([]float32, d)
	for i := range vec {
		vec[i] = rand.Float32()
	}
	// Normalize in-place with FAISS utilities
	faiss.Normalize(vec)
	// Convert to float64 slice for BSON storage
	vec64 := make([]float64, d)
	for i := range vec64 {
		vec64[i] = float64(vec[i])
	}
	return vec, vec64
}

func main() {
	// Uncomment to run catalog demos
	// runCatalogDemos()
	// return

	wt := wiredtiger.WiredTiger()
	fs := faiss.FAISS()

	// Prepare WT home directory
	const home = "WT_HOME_DOCSTORE"
	if err := os.MkdirAll(home, 0755); err != nil {
		log.Fatalf("failed to create %s: %v", home, err)
	}

	// defer func() {
	// 	_ = os.RemoveAll(home)
	// }()

	// Open WiredTiger and create a binary table for BSON docs
	if err := wt.Open(home, "create"); err != nil {
		log.Fatalf("failed to open WiredTiger: %v", err)
	}
	defer wt.Close()

	const uri = "table:docs"
	// key_format=u (raw bytes ObjectID), value_format=u (raw bytes BSON)
	if err := wt.CreateTable(uri, "key_format=u,value_format=u"); err != nil {
		log.Fatalf("failed to create table: %v", err)
	}

	// Build and insert 40 documents
	const (
		nDocs = 40
		dim   = 1536
	)

	fmt.Printf("Inserting %d BSON documents with %dd normalized embeddings...\n", nDocs, dim)
	for i := 0; i < nDocs; i++ {
		id := primitive.NewObjectID()
		_, emb64 := generateNormalizedVector(dim, fs)
		doc := Document{
			ID:        id,
			Text:      fmt.Sprintf("example document %d", i+1),
			Embedding: emb64,
		}

		value, err := bson.Marshal(doc)
		if err != nil {
			log.Fatalf("failed to marshal BSON: %v", err)
		}
		if err := wt.PutBinary(uri, id[:], value); err != nil {
			log.Fatalf("failed to insert doc %d: %v", i+1, err)
		}
	}
	fmt.Println("Insert complete. Enumerating all records...")

	// Try to read an index at "coll_1.index"; if fail, create a flat index
	var index *faiss.Index
	index, err := fs.ReadIndex("coll_1.index")
	if err != nil {
		fmt.Printf("Could not read index from disk: %v\n", err)
		// Create a new Flat index as fallback
		index, err = fs.IndexFactory(dim, "Flat", faiss.MetricL2)
		if err != nil {
			log.Fatalf("failed to create Flat index: %v", err)
		}
		fmt.Println("Created new Flat index in memory.")
	} else {
		fmt.Println("Loaded FAISS index from coll_1.index")
	}

	// Enumerate values after insert
	pairs, err := wt.ScanBinary(uri)
	if err != nil {
		log.Fatalf("failed to scan records: %v", err)
	}
	for idx, kv := range pairs {
		var doc Document
		if err := bson.Unmarshal(kv.Value, &doc); err != nil {
			log.Fatalf("failed to unmarshal record %d: %v", idx+1, err)
		}

		insertDocEmbeddings(DocEmbeddingsPayload{
			DocID:       doc.ID[:],
			Embedding:   doc.Embedding,
			TableUri:    "table:docId_vectorId",
			KvService:   wt,
			vectorIndex: *index,
		})

		fmt.Printf("[%02d] _id=%s text=\"%s\" emb_len=%d\n", idx+1, doc.ID.Hex(), doc.Text, len(doc.Embedding))
	}
	fmt.Printf("Total records: %d\n", len(pairs))

	k := 10
	min_distance := 0.475
	randVec := make([]float32, dim)
	for i := 0; i < dim; i++ {
		randVec[i] = rand.Float32()
	}
	randVec = fs.NormalizeBatch(randVec, dim)

	relevantDocs := make([]Document, 0)

	searchForRelevantDocs(
		SearchForRelevantDocsPayload{
			VectorIndex:    *index,
			QueryEmbedding: randVec,
			TopK:           &k,
			DocTableURI:    uri,
			KvService:      &wt,
			Results:        &relevantDocs,
			MinDistance:    &min_distance,
		},
	)
	if err != nil {
		fmt.Printf("SearchForRelevantDocs failed: %v\n", err)
	} else {
		fmt.Printf("Top %d relevant documents for a random 1536-d vector:\n", k)
		for j, doc := range relevantDocs {
			fmt.Printf("Rank %d: DocID=%s, Text=\"%s\"\n", j+1, doc.ID.Hex(), doc.Text)
		}
	}

}

// DocEmbeddingsPayload is used as the payload for document embeddings inserts.
type DocEmbeddingsPayload struct {
	DocID       []byte    // Document ID in binary (e.g., ObjectID bytes)
	Embedding   []float64 // Embedding vector for the document
	TableUri    string    // URI of vectorLabel ---> DOCID table
	KvService   wiredtiger.WTService
	vectorIndex faiss.Index
}

func insertDocEmbeddings(payload DocEmbeddingsPayload) error {
	embedding := payload.Embedding
	Idx := payload.vectorIndex

	emb32 := float64SliceToFloat32(embedding)

	// Add embedding and retrieve the label assigned by FAISS (NTotal() - 1)
	err := Idx.Add(emb32, 1)
	var label int64 = -1
	if err == nil {
		if nTotal, nErr := Idx.NTotal(); nErr == nil {
			label = nTotal - 1
		}
	}

	if err != nil {
		return fmt.Errorf("failed to add embedding to index: %v", err)
	}

	// First, create the table if it doesn't exist.
	if err := payload.KvService.CreateTable(payload.TableUri, "key_format=S,value_format=S"); err != nil {
		return fmt.Errorf("failed to create table: %v", err)
	}
	docIDHex := fmt.Sprintf("%x", payload.DocID)
	err = payload.KvService.PutString(payload.TableUri, fmt.Sprintf("%d", label), docIDHex)

	if err != nil {
		return fmt.Errorf("failed to write label->docID mapping to table: %v", err)
	}
	fmt.Println("Updated Mappings")
	return nil
}

func float64SliceToFloat32(xs []float64) []float32 {
	result := make([]float32, len(xs))
	for i, v := range xs {
		result[i] = float32(v)
	}
	return result
}

type SearchForRelevantDocsPayload struct {
	VectorIndex    faiss.Index // vector index in memory.
	QueryEmbedding []float32
	TopK           *int
	DocTableURI    string
	Results        *[]Document // outpointer to results, a slice of User
	MinDistance    *float64    // optional out pointer to distance threshold
	KvService      *wiredtiger.WTService
}

func searchForRelevantDocs(payload SearchForRelevantDocsPayload) {
	xq := payload.QueryEmbedding

	nq := 1 // number of queries
	var k = 5
	var min_distance float64
	var kv_service wiredtiger.WTService

	if payload.TopK != nil {
		k = *payload.TopK
	}

	if payload.MinDistance != nil {
		min_distance = *payload.MinDistance
	}

	if payload.KvService == nil {
		fmt.Print("No KV service impl passed")
		return
	}

	kv_service = *payload.KvService

	distances, ids, err := payload.VectorIndex.Search(xq, nq, k)

	if err != nil {
		fmt.Println("Failed to search index")
	}

	// For each id, lookup the docID in the table, assuming KvService has a GetString(uri, key string) (val string, err error).
	for index, id := range ids {
		// id could be -1 if FAISS returned a "no result"; handle this
		if id < 0 {
			continue
		}
		key := fmt.Sprintf("%d", id)
		val, _, err := kv_service.GetString(LABEL_TO_ID_TABLE_URI, key)
		if err != nil {
			fmt.Printf("Failed to get docID for label %s: %v\n", key, err)
			continue
		}

		// Parse val as a BSON ObjectID hex string and use its raw 12-byte representation as the key
		if payload.DocTableURI != "" {
			// Validate hex string length (ObjectID should be 24 hex chars = 12 bytes)
			if len(val) != 24 {
				fmt.Printf("Invalid ObjectID hex length: expected 24, got %d for '%s'\n", len(val), val)
				continue
			}

			objectID, err := primitive.ObjectIDFromHex(val)
			if err != nil {
				fmt.Printf("Failed to parse docID '%s' as ObjectID hex: %v\n", val, err)
				continue
			}

			// Validate the ObjectID is not empty/zero
			if objectID.IsZero() {
				fmt.Printf("ObjectID is zero/empty for hex '%s'\n", val)
				continue
			}

			docIDBytes := objectID[:] // Convert ObjectID to raw [12]byte slice

			// Validate the binary key length
			if len(docIDBytes) != 12 {
				fmt.Printf("Invalid docIDBytes length: expected 12, got %d\n", len(docIDBytes))
				continue
			}

			docBin, _, err := kv_service.GetBinary(payload.DocTableURI, docIDBytes)
			if err != nil {
				fmt.Printf("Failed to get document for docID %s in table %s: %v\n", val, payload.DocTableURI, err)
				continue
			}
			if len(docBin) > 0 {
				var doc Document

				if err := bson.Unmarshal(docBin, &doc); err != nil {
					fmt.Printf("Failed to unmarshal BSON for docID %s: %v\n", val, err)
					continue
				}

				fmt.Printf("DocID: %s, Distance: %f\n", val, distances[index])

				if float64(distances[index]) < min_distance {
					*payload.Results = append(*payload.Results, doc)
				} else {
					fmt.Printf("DocID: %s, skipped\n", val)
				}
			}
		}

	}
}
