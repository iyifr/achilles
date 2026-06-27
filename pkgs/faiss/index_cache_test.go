//go:build cgo

package faiss

import (
	"path/filepath"
	"testing"
)

func TestIndexCache_PersistsIDMapAcrossReload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "reload.index")

	cache := NewIndexCache(10)

	cached, err := cache.GetOrCreate(path, 4)
	if err != nil {
		t.Fatalf("GetOrCreate (create) failed: %v", err)
	}
	if err := cached.Index.AddWithIds([]float32{1, 0, 0, 0}, []int64{42}, 1); err != nil {
		t.Fatalf("AddWithIds on freshly created index failed: %v", err)
	}
	if err := cached.Index.WriteToFile(path); err != nil {
		t.Fatalf("WriteToFile failed: %v", err)
	}

	// Force a real reload from disk: evict from cache and free, then fetch again.
	if err := cache.Remove(path); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	reloaded, err := cache.GetOrCreate(path, 4)
	if err != nil {
		t.Fatalf("GetOrCreate (reload) failed: %v", err)
	}

	n, err := reloaded.Index.NTotal()
	if err != nil {
		t.Fatalf("NTotal after reload failed: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected NTotal=1 after reload, got %d", n)
	}

	// If the reloaded index lost its IndexIDMap wrapping, AddWithIds would
	// fail (base Index::add_with_ids asserts/throws).
	if err := reloaded.Index.AddWithIds([]float32{0, 1, 0, 0}, []int64{99}, 1); err != nil {
		t.Fatalf("AddWithIds after reload failed (index may have lost IndexIDMap wrapping): %v", err)
	}

	removed, err := reloaded.Index.RemoveIds([]int64{42})
	if err != nil {
		t.Fatalf("RemoveIds after reload failed: %v", err)
	}
	if removed != 1 {
		t.Fatalf("expected 1 removed after reload, got %d", removed)
	}
}
