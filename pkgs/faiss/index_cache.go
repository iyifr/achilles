package faiss

import (
	"sync"
	"time"
)

// CachedIndex wraps a FAISS index with caching metadata.
type CachedIndex struct {
	Index    *Index
	Path     string
	Dirty    bool      // Modified since last write to disk
	LastUsed time.Time
	mu       sync.Mutex
}

// Lock acquires the mutex for exclusive access to the index.
func (c *CachedIndex) Lock() {
	c.mu.Lock()
}

// Unlock releases the mutex.
func (c *CachedIndex) Unlock() {
	c.mu.Unlock()
}

// IndexCache provides thread-safe caching for FAISS indexes.
type IndexCache struct {
	indexes  map[string]*CachedIndex // path -> cached index
	mu       sync.RWMutex
	maxSize  int           // Maximum number of indexes to cache
	service  FAISSService  // For creating new indexes
}

// Global index cache singleton
var (
	globalCache     *IndexCache
	globalCacheOnce sync.Once
)

// GlobalIndexCache returns the singleton index cache.
func GlobalIndexCache() *IndexCache {
	globalCacheOnce.Do(func() {
		globalCache = NewIndexCache(100) // Cache up to 100 indexes
	})
	return globalCache
}

// NewIndexCache creates a new index cache with the specified maximum size.
func NewIndexCache(maxSize int) *IndexCache {
	return &IndexCache{
		indexes: make(map[string]*CachedIndex),
		maxSize: maxSize,
		service: FAISS(),
	}
}

// GetOrCreate returns a cached index or creates a new one.
// If the index exists on disk, it's loaded. Otherwise, a new index is created.
func (c *IndexCache) GetOrCreate(path string, dimension int) (*CachedIndex, error) {
	c.mu.RLock()
	if cached, exists := c.indexes[path]; exists {
		cached.LastUsed = time.Now()
		c.mu.RUnlock()
		return cached, nil
	}
	c.mu.RUnlock()

	// Not in cache, need to load or create
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check after acquiring write lock
	if cached, exists := c.indexes[path]; exists {
		cached.LastUsed = time.Now()
		return cached, nil
	}

	// Try to load from disk
	idx, err := c.service.ReadIndex(path)
	if err != nil {
		// Create new index
		idx, err = c.service.IndexFactory(dimension, "Flat", MetricL2)
		if err != nil {
			return nil, err
		}
	}

	// Evict if at capacity
	if len(c.indexes) >= c.maxSize {
		c.evictOldest()
	}

	cached := &CachedIndex{
		Index:    idx,
		Path:     path,
		Dirty:    false,
		LastUsed: time.Now(),
	}
	c.indexes[path] = cached

	return cached, nil
}

// MarkDirty marks an index as modified (needs to be flushed to disk).
func (c *IndexCache) MarkDirty(path string) {
	c.mu.RLock()
	if cached, exists := c.indexes[path]; exists {
		cached.Dirty = true
	}
	c.mu.RUnlock()
}

// FlushOne writes a specific index to disk if it's dirty.
func (c *IndexCache) FlushOne(path string) error {
	c.mu.RLock()
	cached, exists := c.indexes[path]
	c.mu.RUnlock()

	if !exists {
		return nil
	}

	cached.Lock()
	defer cached.Unlock()

	if !cached.Dirty {
		return nil
	}

	if err := cached.Index.WriteToFile(path); err != nil {
		return err
	}
	cached.Dirty = false
	return nil
}

// FlushAll writes all dirty indexes to disk.
func (c *IndexCache) FlushAll() error {
	c.mu.RLock()
	paths := make([]string, 0, len(c.indexes))
	for path := range c.indexes {
		paths = append(paths, path)
	}
	c.mu.RUnlock()

	for _, path := range paths {
		if err := c.FlushOne(path); err != nil {
			return err
		}
	}
	return nil
}

// Remove removes an index from the cache (flushes first if dirty).
func (c *IndexCache) Remove(path string) error {
	// Flush first
	if err := c.FlushOne(path); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if cached, exists := c.indexes[path]; exists {
		cached.Index.Free()
		delete(c.indexes, path)
	}
	return nil
}

// evictOldest removes the least recently used index (must hold write lock).
func (c *IndexCache) evictOldest() {
	var oldestPath string
	var oldestTime time.Time

	for path, cached := range c.indexes {
		if oldestPath == "" || cached.LastUsed.Before(oldestTime) {
			oldestPath = path
			oldestTime = cached.LastUsed
		}
	}

	if oldestPath != "" {
		if cached, exists := c.indexes[oldestPath]; exists {
			// Flush if dirty before evicting
			if cached.Dirty {
				cached.Index.WriteToFile(oldestPath)
			}
			cached.Index.Free()
			delete(c.indexes, oldestPath)
		}
	}
}

// Size returns the number of cached indexes.
func (c *IndexCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.indexes)
}

// Close flushes all dirty indexes and frees resources.
func (c *IndexCache) Close() error {
	if err := c.FlushAll(); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, cached := range c.indexes {
		cached.Index.Free()
	}
	c.indexes = make(map[string]*CachedIndex)
	return nil
}
