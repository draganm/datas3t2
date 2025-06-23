package diskcache

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/draganm/datas3t2/tarindex"
)

// DatarangeKey represents the unique identifier for a datarange
type DatarangeKey struct {
	Datas3tName        string
	FirstIndex         int64
	NumberOfDatapoints int64
	TotalSizeBytes     int64
}

// String returns a string representation of the datarange key for hashing
func (dk DatarangeKey) String() string {
	return fmt.Sprintf("%s:%d:%d:%d", dk.Datas3tName, dk.FirstIndex, dk.NumberOfDatapoints, dk.TotalSizeBytes)
}

// Hash returns a SHA-256 hash of the datarange key for use as filename
func (dk DatarangeKey) Hash() string {
	h := sha256.Sum256([]byte(dk.String()))
	return fmt.Sprintf("%x", h)
}

// cacheEntry represents a cached tarindex with metadata
type cacheEntry struct {
	key          DatarangeKey
	filename     string
	lastAccessed time.Time
	sizeBytes    int64
	index        *tarindex.Index // memory-mapped index, nil if not loaded
}

// IndexDiskCache provides a thread-safe, persistent disk-based cache for tarindex files
type IndexDiskCache struct {
	mu           sync.RWMutex
	cacheDir     string
	maxSizeBytes int64
	entries      map[string]*cacheEntry // filename -> entry
	totalSize    int64
}

// NewIndexDiskCache creates a new disk cache instance
func NewIndexDiskCache(cacheDir string, maxSizeBytes int64) (*IndexDiskCache, error) {
	// Create cache directory if it doesn't exist
	err := os.MkdirAll(cacheDir, 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %w", err)
	}

	cache := &IndexDiskCache{
		cacheDir:     cacheDir,
		maxSizeBytes: maxSizeBytes,
		entries:      make(map[string]*cacheEntry),
	}

	// Load existing cache entries from disk
	err = cache.loadExistingEntries()
	if err != nil {
		return nil, fmt.Errorf("failed to load existing cache entries: %w", err)
	}

	return cache, nil
}

// loadExistingEntries scans the cache directory and loads metadata for existing files
func (c *IndexDiskCache) loadExistingEntries() error {
	files, err := os.ReadDir(c.cacheDir)
	if err != nil {
		return fmt.Errorf("failed to read cache directory: %w", err)
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filename := file.Name()

		// Get file info
		info, err := file.Info()
		if err != nil {
			// Skip files we can't stat
			continue
		}

		// Create cache entry (we'll populate the key when needed)
		entry := &cacheEntry{
			filename:     filename,
			lastAccessed: info.ModTime(), // Use modification time as initial last accessed
			sizeBytes:    info.Size(),
		}

		c.entries[filename] = entry
		c.totalSize += info.Size()
	}

	return nil
}

// OnIndex provides callback-based access to cached tarindex data
// If the data is not cached, it will call the indexGenerator to create it
func (c *IndexDiskCache) OnIndex(key DatarangeKey, callback func(*tarindex.Index) error, indexGenerator func() ([]byte, error)) error {
	filename := key.Hash()
	fullPath := filepath.Join(c.cacheDir, filename)

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if we have this entry in cache
	entry, exists := c.entries[filename]
	if !exists {
		// Not in cache, generate the index
		indexData, err := indexGenerator()
		if err != nil {
			return fmt.Errorf("failed to generate index: %w", err)
		}

		// Write to disk
		err = c.writeIndexToDisk(fullPath, indexData)
		if err != nil {
			return fmt.Errorf("failed to write index to disk: %w", err)
		}

		// Create cache entry
		entry = &cacheEntry{
			key:          key,
			filename:     filename,
			lastAccessed: time.Now(),
			sizeBytes:    int64(len(indexData)),
		}

		c.entries[filename] = entry
		c.totalSize += entry.sizeBytes

		// Evict if necessary
		err = c.evictIfNeeded()
		if err != nil {
			return fmt.Errorf("failed to evict cache entries: %w", err)
		}
	}

	// Update last accessed time
	entry.lastAccessed = time.Now()
	entry.key = key // Ensure key is set (needed for entries loaded from disk)

	// Load the index if not already loaded
	if entry.index == nil {
		index, err := tarindex.OpenTarIndex(fullPath)
		if err != nil {
			return fmt.Errorf("failed to open cached index: %w", err)
		}
		entry.index = index
	}

	// Call the callback with the loaded index
	err := callback(entry.index)
	if err != nil {
		return fmt.Errorf("callback failed: %w", err)
	}

	return nil
}

// writeIndexToDisk writes index data to a file atomically
func (c *IndexDiskCache) writeIndexToDisk(path string, data []byte) error {
	// Write to temporary file first
	tempPath := path + ".tmp"

	file, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	_, err = file.Write(data)
	if err != nil {
		file.Close()
		os.Remove(tempPath)
		return fmt.Errorf("failed to write data: %w", err)
	}

	err = file.Close()
	if err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	// Atomically move temp file to final location
	err = os.Rename(tempPath, path)
	if err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

// evictIfNeeded evicts cache entries using LRU policy if cache size exceeds limit
func (c *IndexDiskCache) evictIfNeeded() error {
	if c.totalSize <= c.maxSizeBytes {
		return nil
	}

	// Sort entries by last accessed time (oldest first)
	type entryWithTime struct {
		filename string
		entry    *cacheEntry
	}

	var entries []entryWithTime
	for filename, entry := range c.entries {
		entries = append(entries, entryWithTime{filename, entry})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].entry.lastAccessed.Before(entries[j].entry.lastAccessed)
	})

	// Evict oldest entries until we're under the size limit
	for _, entryData := range entries {
		if c.totalSize <= c.maxSizeBytes {
			break
		}

		err := c.evictEntry(entryData.filename, entryData.entry)
		if err != nil {
			return fmt.Errorf("failed to evict entry %s: %w", entryData.filename, err)
		}
	}

	return nil
}

// evictEntry removes a single cache entry
func (c *IndexDiskCache) evictEntry(filename string, entry *cacheEntry) error {
	// Close the index if it's loaded
	if entry.index != nil {
		err := entry.index.Close()
		if err != nil {
			return fmt.Errorf("failed to close index: %w", err)
		}
	}

	// Remove file from disk
	fullPath := filepath.Join(c.cacheDir, filename)
	err := os.Remove(fullPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove file: %w", err)
	}

	// Remove from cache
	delete(c.entries, filename)
	c.totalSize -= entry.sizeBytes

	return nil
}

// Close closes all open index files and cleans up resources
func (c *IndexDiskCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var errs []error

	for _, entry := range c.entries {
		if entry.index != nil {
			err := entry.index.Close()
			if err != nil {
				errs = append(errs, fmt.Errorf("failed to close index %s: %w", entry.filename, err))
			}
		}
	}

	// Return first error if any occurred
	if len(errs) > 0 {
		return errs[0]
	}

	return nil
}

// Stats returns cache statistics
func (c *IndexDiskCache) Stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return CacheStats{
		EntryCount: len(c.entries),
		TotalSize:  c.totalSize,
		MaxSize:    c.maxSizeBytes,
		CacheDir:   c.cacheDir,
	}
}

// CacheStats provides information about cache state
type CacheStats struct {
	EntryCount int
	TotalSize  int64
	MaxSize    int64
	CacheDir   string
}

// Clear removes all entries from the cache
func (c *IndexDiskCache) Clear() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var errs []error

	for filename, entry := range c.entries {
		err := c.evictEntry(filename, entry)
		if err != nil {
			errs = append(errs, err)
		}
	}

	// Return first error if any occurred
	if len(errs) > 0 {
		return errs[0]
	}

	return nil
}
