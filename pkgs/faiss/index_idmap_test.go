//go:build cgo

package faiss

import "testing"

func TestIndexIDMap_AddSearchRemove(t *testing.T) {
	svc := FAISS()
	idx, err := svc.IndexFactory(4, "Flat", MetricL2)
	if err != nil {
		t.Fatalf("IndexFactory failed: %v", err)
	}
	defer idx.Free()

	if err := idx.WrapIDMap(); err != nil {
		t.Fatalf("WrapIDMap failed: %v", err)
	}

	vectors := []float32{
		1, 0, 0, 0, // id 10
		0, 1, 0, 0, // id 20
		0, 0, 1, 0, // id 30
	}
	ids := []int64{10, 20, 30}

	if err := idx.AddWithIds(vectors, ids, 3); err != nil {
		t.Fatalf("AddWithIds failed: %v", err)
	}

	n, err := idx.NTotal()
	if err != nil {
		t.Fatalf("NTotal failed: %v", err)
	}
	if n != 3 {
		t.Fatalf("expected NTotal=3, got %d", n)
	}

	query := []float32{0, 1, 0, 0} // matches id 20 exactly
	_, labels, err := idx.Search(query, 1, 1)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(labels) != 1 || labels[0] != 20 {
		t.Fatalf("expected top result id=20, got %v", labels)
	}

	removed, err := idx.RemoveIds([]int64{20})
	if err != nil {
		t.Fatalf("RemoveIds failed: %v", err)
	}
	if removed != 1 {
		t.Fatalf("expected 1 removed, got %d", removed)
	}

	n, err = idx.NTotal()
	if err != nil {
		t.Fatalf("NTotal after remove failed: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected NTotal=2 after remove, got %d", n)
	}

	_, labels, err = idx.Search(query, 1, 3)
	if err != nil {
		t.Fatalf("Search after remove failed: %v", err)
	}
	for _, l := range labels {
		if l == 20 {
			t.Fatalf("removed id 20 still present in search results: %v", labels)
		}
	}
}
