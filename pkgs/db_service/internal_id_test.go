package dbservice

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeInternalId_RoundTrip(t *testing.T) {
	for _, id := range []int64{0, 1, 42, 1 << 40} {
		got := decodeInternalId(encodeInternalId(id))
		if got != id {
			t.Errorf("round trip mismatch: encoded/decoded %d, got %d", id, got)
		}
	}
}

func TestEncodeInternalId_PreservesNumericOrdering(t *testing.T) {
	ids := []int64{0, 1, 2, 255, 256, 65535, 65536, 1 << 32}
	for i := 1; i < len(ids); i++ {
		a := encodeInternalId(ids[i-1])
		b := encodeInternalId(ids[i])
		if bytes.Compare(a, b) >= 0 {
			t.Errorf("expected encode(%d) < encode(%d) lexicographically", ids[i-1], ids[i])
		}
	}
}

func TestInternalIdKeyBounds(t *testing.T) {
	min := minInternalIdKey()
	max := maxInternalIdKey()
	if len(min) != 8 || len(max) != 8 {
		t.Fatalf("expected 8-byte bounds, got min=%d max=%d", len(min), len(max))
	}
	if !bytes.Equal(min, encodeInternalId(0)) {
		t.Errorf("minInternalIdKey should equal encodeInternalId(0)")
	}
	if bytes.Compare(encodeInternalId(1<<62), max) >= 0 {
		t.Errorf("maxInternalIdKey should be greater than any realistic id")
	}
}
