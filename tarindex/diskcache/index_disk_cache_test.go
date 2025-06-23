package diskcache_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/draganm/datas3t2/tarindex"
	"github.com/draganm/datas3t2/tarindex/diskcache"
)

var _ = Describe("IndexDiskCache", func() {
	var (
		tempDir string
		cache   *diskcache.IndexDiskCache
	)

	BeforeEach(func(ctx SpecContext) {
		var err error
		tempDir, err = os.MkdirTemp("", "diskcache_test_*")
		Expect(err).NotTo(HaveOccurred())

		cache, err = diskcache.NewIndexDiskCache(tempDir, 1024*1024) // 1MB limit
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func(ctx SpecContext) {
		if cache != nil {
			err := cache.Close()
			Expect(err).NotTo(HaveOccurred())
		}

		err := os.RemoveAll(tempDir)
		Expect(err).NotTo(HaveOccurred())
	})

	Describe("DatarangeKey", func() {
		It("should generate consistent string representation", func(ctx SpecContext) {
			key := diskcache.DatarangeKey{
				Datas3tName:        "test-dataset",
				FirstIndex:         100,
				NumberOfDatapoints: 50,
				TotalSizeBytes:     2048,
			}

			str1 := key.String()
			str2 := key.String()
			Expect(str1).To(Equal(str2))
			Expect(str1).To(Equal("test-dataset:100:50:2048"))
		})

		It("should generate consistent hash", func(ctx SpecContext) {
			key := diskcache.DatarangeKey{
				Datas3tName:        "test-dataset",
				FirstIndex:         100,
				NumberOfDatapoints: 50,
				TotalSizeBytes:     2048,
			}

			hash1 := key.Hash()
			hash2 := key.Hash()
			Expect(hash1).To(Equal(hash2))
			Expect(hash1).To(HaveLen(64)) // SHA-256 hex string
		})

		It("should generate different hashes for different keys", func(ctx SpecContext) {
			key1 := diskcache.DatarangeKey{
				Datas3tName:        "test-dataset",
				FirstIndex:         100,
				NumberOfDatapoints: 50,
				TotalSizeBytes:     2048,
			}

			key2 := diskcache.DatarangeKey{
				Datas3tName:        "test-dataset",
				FirstIndex:         200,
				NumberOfDatapoints: 50,
				TotalSizeBytes:     2048,
			}

			Expect(key1.Hash()).NotTo(Equal(key2.Hash()))
		})
	})

	Describe("NewIndexDiskCache", func() {
		It("should create cache directory if it doesn't exist", func(ctx SpecContext) {
			nonExistentDir := filepath.Join(tempDir, "new_cache_dir")

			newCache, err := diskcache.NewIndexDiskCache(nonExistentDir, 1024*1024)
			Expect(err).NotTo(HaveOccurred())
			defer newCache.Close()

			_, err = os.Stat(nonExistentDir)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should load existing cache entries from disk", func(ctx SpecContext) {
			// Close current cache
			err := cache.Close()
			Expect(err).NotTo(HaveOccurred())

			// Create a dummy cache file
			dummyData := createDummyIndexData(10)
			dummyFile := filepath.Join(tempDir, "dummy_cache_file")
			err = os.WriteFile(dummyFile, dummyData, 0644)
			Expect(err).NotTo(HaveOccurred())

			// Create new cache instance
			newCache, err := diskcache.NewIndexDiskCache(tempDir, 1024*1024)
			Expect(err).NotTo(HaveOccurred())
			defer newCache.Close()

			stats := newCache.Stats()
			Expect(stats.EntryCount).To(Equal(1))
			Expect(stats.TotalSize).To(Equal(int64(len(dummyData))))
		})
	})

	Describe("OnIndex", func() {
		It("should call generator when index is not cached", func(ctx SpecContext) {
			key := diskcache.DatarangeKey{
				Datas3tName:        "test-dataset",
				FirstIndex:         0,
				NumberOfDatapoints: 10,
				TotalSizeBytes:     1024,
			}

			generatorCalled := false
			callbackCalled := false
			expectedData := createDummyIndexData(10)

			err := cache.OnIndex(key,
				func(index *tarindex.Index) error {
					callbackCalled = true
					Expect(index).NotTo(BeNil())
					Expect(index.NumFiles()).To(Equal(uint64(10)))
					return nil
				},
				func() ([]byte, error) {
					generatorCalled = true
					return expectedData, nil
				},
			)

			Expect(err).NotTo(HaveOccurred())
			Expect(generatorCalled).To(BeTrue())
			Expect(callbackCalled).To(BeTrue())

			// Verify file was written to disk
			filename := key.Hash()
			cachePath := filepath.Join(tempDir, filename)
			_, err = os.Stat(cachePath)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should use cached index when available", func(ctx SpecContext) {
			key := diskcache.DatarangeKey{
				Datas3tName:        "test-dataset",
				FirstIndex:         0,
				NumberOfDatapoints: 10,
				TotalSizeBytes:     1024,
			}

			expectedData := createDummyIndexData(10)

			// First call - should generate
			generatorCallCount := 0
			err := cache.OnIndex(key,
				func(index *tarindex.Index) error {
					return nil
				},
				func() ([]byte, error) {
					generatorCallCount++
					return expectedData, nil
				},
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(generatorCallCount).To(Equal(1))

			// Second call - should use cache
			err = cache.OnIndex(key,
				func(index *tarindex.Index) error {
					Expect(index.NumFiles()).To(Equal(uint64(10)))
					return nil
				},
				func() ([]byte, error) {
					generatorCallCount++
					return expectedData, nil
				},
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(generatorCallCount).To(Equal(1)) // Should not have called generator again
		})

		It("should handle generator errors", func(ctx SpecContext) {
			key := diskcache.DatarangeKey{
				Datas3tName:        "test-dataset",
				FirstIndex:         0,
				NumberOfDatapoints: 10,
				TotalSizeBytes:     1024,
			}

			expectedError := fmt.Errorf("generator error")

			err := cache.OnIndex(key,
				func(index *tarindex.Index) error {
					return nil
				},
				func() ([]byte, error) {
					return nil, expectedError
				},
			)

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to generate index"))
		})

		It("should handle callback errors", func(ctx SpecContext) {
			key := diskcache.DatarangeKey{
				Datas3tName:        "test-dataset",
				FirstIndex:         0,
				NumberOfDatapoints: 10,
				TotalSizeBytes:     1024,
			}

			expectedData := createDummyIndexData(10)
			expectedError := fmt.Errorf("callback error")

			err := cache.OnIndex(key,
				func(index *tarindex.Index) error {
					return expectedError
				},
				func() ([]byte, error) {
					return expectedData, nil
				},
			)

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("callback failed"))
		})
	})

	Describe("LRU Eviction", func() {
		It("should evict least recently used entries when cache exceeds size limit", func(ctx SpecContext) {
			// Create a small cache (100 bytes)
			smallCache, err := diskcache.NewIndexDiskCache(filepath.Join(tempDir, "small"), 100)
			Expect(err).NotTo(HaveOccurred())
			defer smallCache.Close()

			// Create entries that will exceed the cache size
			key1 := diskcache.DatarangeKey{Datas3tName: "test", FirstIndex: 1, NumberOfDatapoints: 5, TotalSizeBytes: 512}
			key2 := diskcache.DatarangeKey{Datas3tName: "test", FirstIndex: 2, NumberOfDatapoints: 5, TotalSizeBytes: 512}
			key3 := diskcache.DatarangeKey{Datas3tName: "test", FirstIndex: 3, NumberOfDatapoints: 5, TotalSizeBytes: 512}

			data1 := createDummyIndexData(5)
			data2 := createDummyIndexData(5)
			data3 := createDummyIndexData(5)

			// Add first entry
			err = smallCache.OnIndex(key1, func(index *tarindex.Index) error { return nil }, func() ([]byte, error) { return data1, nil })
			Expect(err).NotTo(HaveOccurred())

			// Add second entry - should still fit
			err = smallCache.OnIndex(key2, func(index *tarindex.Index) error { return nil }, func() ([]byte, error) { return data2, nil })
			Expect(err).NotTo(HaveOccurred())

			// Access first entry to make it more recently used
			time.Sleep(time.Millisecond) // Ensure different timestamps
			err = smallCache.OnIndex(key1, func(index *tarindex.Index) error { return nil }, func() ([]byte, error) { return data1, nil })
			Expect(err).NotTo(HaveOccurred())

			// Add third entry - should evict key2 (least recently used)
			time.Sleep(time.Millisecond)
			err = smallCache.OnIndex(key3, func(index *tarindex.Index) error { return nil }, func() ([]byte, error) { return data3, nil })
			Expect(err).NotTo(HaveOccurred())

			// Verify that key2 was evicted by checking if generator is called again
			generatorCalled := false
			err = smallCache.OnIndex(key2, func(index *tarindex.Index) error { return nil }, func() ([]byte, error) {
				generatorCalled = true
				return data2, nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(generatorCalled).To(BeTrue(), "key2 should have been evicted and generator should be called")
		})
	})

	Describe("Thread Safety", func() {
		It("should handle concurrent access safely", func(ctx SpecContext) {
			const numGoroutines = 10
			const numOperations = 20

			var wg sync.WaitGroup
			wg.Add(numGoroutines)

			errors := make(chan error, numGoroutines*numOperations)

			for i := 0; i < numGoroutines; i++ {
				go func(goroutineID int) {
					defer wg.Done()

					for j := 0; j < numOperations; j++ {
						key := diskcache.DatarangeKey{
							Datas3tName:        fmt.Sprintf("test-%d", goroutineID%3), // Use 3 different datasets
							FirstIndex:         int64(j),
							NumberOfDatapoints: 10,
							TotalSizeBytes:     1024,
						}

						data := createDummyIndexData(10)

						err := cache.OnIndex(key,
							func(index *tarindex.Index) error {
								// Verify the index is valid
								Expect(index.NumFiles()).To(Equal(uint64(10)))
								return nil
							},
							func() ([]byte, error) {
								return data, nil
							},
						)

						if err != nil {
							errors <- err
							return
						}
					}
				}(i)
			}

			wg.Wait()
			close(errors)

			// Check for any errors
			for err := range errors {
				Expect(err).NotTo(HaveOccurred())
			}
		})
	})

	Describe("Persistence", func() {
		It("should persist cache across restarts", func(ctx SpecContext) {
			key := diskcache.DatarangeKey{
				Datas3tName:        "test-dataset",
				FirstIndex:         0,
				NumberOfDatapoints: 10,
				TotalSizeBytes:     1024,
			}

			expectedData := createDummyIndexData(10)

			// Add entry to cache
			err := cache.OnIndex(key,
				func(index *tarindex.Index) error {
					return nil
				},
				func() ([]byte, error) {
					return expectedData, nil
				},
			)
			Expect(err).NotTo(HaveOccurred())

			// Close cache
			err = cache.Close()
			Expect(err).NotTo(HaveOccurred())

			// Create new cache instance
			newCache, err := diskcache.NewIndexDiskCache(tempDir, 1024*1024)
			Expect(err).NotTo(HaveOccurred())
			defer newCache.Close()

			// Verify the entry is still cached (generator should not be called)
			generatorCalled := false
			err = newCache.OnIndex(key,
				func(index *tarindex.Index) error {
					Expect(index.NumFiles()).To(Equal(uint64(10)))
					return nil
				},
				func() ([]byte, error) {
					generatorCalled = true
					return expectedData, nil
				},
			)

			Expect(err).NotTo(HaveOccurred())
			Expect(generatorCalled).To(BeFalse(), "Generator should not be called for persisted entry")
		})
	})

	Describe("Cache Management", func() {
		It("should provide accurate statistics", func(ctx SpecContext) {
			initialStats := cache.Stats()
			Expect(initialStats.EntryCount).To(Equal(0))
			Expect(initialStats.TotalSize).To(Equal(int64(0)))
			Expect(initialStats.MaxSize).To(Equal(int64(1024 * 1024)))
			Expect(initialStats.CacheDir).To(Equal(tempDir))

			// Add an entry
			key := diskcache.DatarangeKey{
				Datas3tName:        "test-dataset",
				FirstIndex:         0,
				NumberOfDatapoints: 10,
				TotalSizeBytes:     1024,
			}

			data := createDummyIndexData(10)
			err := cache.OnIndex(key,
				func(index *tarindex.Index) error { return nil },
				func() ([]byte, error) { return data, nil },
			)
			Expect(err).NotTo(HaveOccurred())

			stats := cache.Stats()
			Expect(stats.EntryCount).To(Equal(1))
			Expect(stats.TotalSize).To(Equal(int64(len(data))))
		})

		It("should clear all entries", func(ctx SpecContext) {
			// Add multiple entries
			for i := 0; i < 3; i++ {
				key := diskcache.DatarangeKey{
					Datas3tName:        "test-dataset",
					FirstIndex:         int64(i),
					NumberOfDatapoints: 10,
					TotalSizeBytes:     1024,
				}

				data := createDummyIndexData(10)
				err := cache.OnIndex(key,
					func(index *tarindex.Index) error { return nil },
					func() ([]byte, error) { return data, nil },
				)
				Expect(err).NotTo(HaveOccurred())
			}

			// Verify entries exist
			stats := cache.Stats()
			Expect(stats.EntryCount).To(Equal(3))

			// Clear cache
			err := cache.Clear()
			Expect(err).NotTo(HaveOccurred())

			// Verify cache is empty
			stats = cache.Stats()
			Expect(stats.EntryCount).To(Equal(0))
			Expect(stats.TotalSize).To(Equal(int64(0)))
		})
	})
})

// createDummyIndexData creates valid tarindex data for testing
func createDummyIndexData(numEntries int) []byte {
	// Each entry is 16 bytes: 8 bytes start position + 2 bytes header blocks + 6 bytes file size
	data := make([]byte, numEntries*16)

	for i := 0; i < numEntries; i++ {
		offset := i * 16

		// Start position (8 bytes, big-endian)
		startPos := int64(i * 1024) // Each file starts 1KB apart
		data[offset+0] = byte(startPos >> 56)
		data[offset+1] = byte(startPos >> 48)
		data[offset+2] = byte(startPos >> 40)
		data[offset+3] = byte(startPos >> 32)
		data[offset+4] = byte(startPos >> 24)
		data[offset+5] = byte(startPos >> 16)
		data[offset+6] = byte(startPos >> 8)
		data[offset+7] = byte(startPos)

		// Header blocks (2 bytes, big-endian) - always 1 for standard TAR
		data[offset+8] = 0
		data[offset+9] = 1

		// File size (6 bytes, big-endian)
		fileSize := int64(512) // 512 bytes per file
		data[offset+10] = byte(fileSize >> 40)
		data[offset+11] = byte(fileSize >> 32)
		data[offset+12] = byte(fileSize >> 24)
		data[offset+13] = byte(fileSize >> 16)
		data[offset+14] = byte(fileSize >> 8)
		data[offset+15] = byte(fileSize)
	}

	return data
}
