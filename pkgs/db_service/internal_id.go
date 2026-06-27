package dbservice

import "encoding/binary"

// Internal document ids are the bridge between FAISS and the WiredTiger
// document store: the same int64 serves as both the FAISS vector id
// (via AddWithIds/RemoveIds) and the document table's own key. A FAISS
// search hit is therefore usable directly as a document table key, with
// no separate label translation table on the query path.
//
// Encoded big-endian so byte-lexicographic comparison (WiredTiger's
// key_format=u) matches numeric ordering, which full-table scans rely on.

func encodeInternalId(id int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(id))
	return buf
}

func decodeInternalId(key []byte) int64 {
	return int64(binary.BigEndian.Uint64(key))
}

// minInternalIdKey and maxInternalIdKey bound a full-table scan over every
// possible internal id key.
func minInternalIdKey() []byte {
	return make([]byte, 8)
}

func maxInternalIdKey() []byte {
	buf := make([]byte, 8)
	for i := range buf {
		buf[i] = 0xFF
	}
	return buf
}
