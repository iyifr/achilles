package dbservice

import (
	"bytes"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
)

// bsonBufferPool provides reusable bytes.Buffer for BSON marshaling.
// This reduces memory allocations in hot paths like InsertDocuments.
var bsonBufferPool = sync.Pool{
	New: func() any {
		// Pre-allocate 16KB buffer (typical document with embeddings)
		buf := bytes.NewBuffer(make([]byte, 0, 16*1024))
		return buf
	},
}

// MarshalWithPool marshals a value to BSON using a pooled buffer.
// Returns the marshaled bytes and a release function that must be called
// when done with the bytes to return the buffer to the pool.
//
// Usage:
//
//	data, release := MarshalWithPool(doc)
//	defer release()
//	// use data...
func BsonMarshalWithPool(v any) ([]byte, func(), error) {
	buf := bsonBufferPool.Get().(*bytes.Buffer)
	buf.Reset() // Reset buffer for reuse

	vw, err := bsonrw.NewBSONValueWriter(buf)
	if err != nil {
		bsonBufferPool.Put(buf)
		return nil, func() {}, err
	}

	enc, err := bson.NewEncoder(vw)
	if err != nil {
		bsonBufferPool.Put(buf)
		return nil, func() {}, err
	}

	if err := enc.Encode(v); err != nil {
		bsonBufferPool.Put(buf)
		return nil, func() {}, err
	}

	result := buf.Bytes()

	release := func() {
		bsonBufferPool.Put(buf)
	}

	return result, release, nil
}
