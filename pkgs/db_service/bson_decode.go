package dbservice

import (
	"go.mongodb.org/mongo-driver/bson"
)

// unmarshalQueryDoc decodes a BSON document into GlowstickDocument using
// bson.Raw for direct field lookup, avoiding full reflection-based decoding.
// This is optimized for the query path where we only need id, content, and metadata.
func unmarshalQueryDoc(data []byte) (GlowstickDocument, error) {
	var doc GlowstickDocument
	raw := bson.Raw(data)

	if val, err := raw.LookupErr("_id"); err == nil {
		doc.Id, _ = val.StringValueOK()
	}
	if val, err := raw.LookupErr("content"); err == nil {
		doc.Content, _ = val.StringValueOK()
	}
	if val, err := raw.LookupErr("metadata"); err == nil {
		if val.Type == bson.TypeEmbeddedDocument {
			doc.Metadata = decodeMetadataDoc(bson.Raw(val.Value))
		}
	}

	return doc, nil
}

// decodeMetadataDoc decodes a BSON document into map[string]interface{} without
// reflection. Type-switches on BSON type tags and extracts values directly.
//
// Produced Go types match bson.Unmarshal exactly, so filter.go compatibility
// is preserved (objectsEqual, toFloat64, evalIn all work unchanged).
func decodeMetadataDoc(raw bson.Raw) map[string]interface{} {
	elems, err := raw.Elements()
	if err != nil {
		return nil
	}
	m := make(map[string]interface{}, len(elems))
	for _, elem := range elems {
		m[elem.Key()] = decodeValue(elem.Value())
	}
	return m
}

// decodeValue extracts a Go value from a bson.RawValue without reflection.
// Fast path covers the 6 types that appear in metadata (string, numbers, bool,
// null, array, nested doc). Exotic types fall back to RawValue.Unmarshal.
func decodeValue(v bson.RawValue) interface{} {
	switch v.Type {
	case bson.TypeString:
		s, _ := v.StringValueOK()
		return s
	case bson.TypeDouble:
		f, _ := v.DoubleOK()
		return f
	case bson.TypeInt32:
		i, _ := v.Int32OK()
		return i
	case bson.TypeInt64:
		i, _ := v.Int64OK()
		return i
	case bson.TypeBoolean:
		b, _ := v.BooleanOK()
		return b
	case bson.TypeNull:
		return nil
	case bson.TypeEmbeddedDocument:
		doc, ok := v.DocumentOK()
		if !ok {
			return nil
		}
		return decodeMetadataDoc(doc)
	case bson.TypeArray:
		arr, ok := v.ArrayOK()
		if !ok {
			return nil
		}
		return decodeArray(arr)
	default:
		// Exotic types (ObjectID, DateTime, Binary, etc.) — rare in metadata.
		// Fall back to reflection-based decode for correctness.
		var out interface{}
		_ = v.Unmarshal(&out)
		return out
	}
}

// decodeArray decodes a BSON array into []interface{} (primitive.A).
// This is the type that filter.go's evalIn/evalArrContains expect.
func decodeArray(raw bson.Raw) []interface{} {
	elems, err := raw.Elements()
	if err != nil {
		return nil
	}
	out := make([]interface{}, len(elems))
	for i, elem := range elems {
		out[i] = decodeValue(elem.Value())
	}
	return out
}
