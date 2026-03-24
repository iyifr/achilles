package dbservice

import "sync"

// labelCache provides an in-memory cache for label→docID mappings.
// These mappings are append-only (new inserts add entries, deletes don't remove them),
// so caching is safe. A stale entry pointing to a deleted document is handled by
// the caller (document lookup returns not-found → skip).
type labelCache struct {
	mu      sync.RWMutex
	entries map[int64]string
}

var (
	globalLabelCache     *labelCache
	globalLabelCacheOnce sync.Once
)

func GlobalLabelCache() *labelCache {
	globalLabelCacheOnce.Do(func() {
		globalLabelCache = &labelCache{
			entries: make(map[int64]string, 4096),
		}
	})
	return globalLabelCache
}

// Get returns the docID for a label, and whether it was found in cache.
func (c *labelCache) Get(label int64) (string, bool) {
	c.mu.RLock()
	val, ok := c.entries[label]
	c.mu.RUnlock()
	return val, ok
}

// Put adds a label→docID mapping to the cache.
func (c *labelCache) Put(label int64, docID string) {
	c.mu.Lock()
	c.entries[label] = docID
	c.mu.Unlock()
}
