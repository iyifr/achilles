package dbservice

import "sync"

type labelKey struct {
	collection string
	label      int64
}

// labelCache provides an in-memory cache for label→docID mappings,
// namespaced by collection URI to avoid collision
//
// These mappings are append-only (new inserts add entries, deletes don't remove them),
// so caching is safe. A stale entry pointing to a deleted document is handled by
// the caller (document lookup returns not-found → skip).
type labelCache struct {
	mu      sync.RWMutex
	entries map[labelKey]string
}

var (
	globalLabelCache     *labelCache
	globalLabelCacheOnce sync.Once
)

func GlobalLabelCache() *labelCache {
	globalLabelCacheOnce.Do(func() {
		globalLabelCache = &labelCache{
			entries: make(map[labelKey]string, 4096),
		}
	})
	return globalLabelCache
}

// Get returns the docID for a label within a collection, and whether it was found.
func (c *labelCache) Get(collection string, label int64) (string, bool) {
	c.mu.RLock()
	val, ok := c.entries[labelKey{collection, label}]
	c.mu.RUnlock()
	return val, ok
}

// Put adds a label→docID mapping for a specific collection.
func (c *labelCache) Put(collection string, label int64, docID string) {
	c.mu.Lock()
	c.entries[labelKey{collection, label}] = docID
	c.mu.Unlock()
}
