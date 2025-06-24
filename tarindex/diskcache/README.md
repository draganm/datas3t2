# TAR Index Disk Cache

This package provides a thread-safe, persistent disk-based cache for tarindex files.

## Overview

The cache stores tarindex files for dataranges from different datas3ts, enabling efficient retrieval of pre-computed index data without regenerating it each time.

## Cache Keys

Each tarindex file is identified by a unique key using the `DatarangeKey` struct containing:
- **Datas3tName**: The name of the datas3t dataset  
- **FirstIndex**: Starting position in the datarange
- **NumberOfDatapoints**: Number of datapoints in the datarange
- **TotalSizeBytes**: Total size in bytes of the datarange

The cache uses SHA-256 hashing of the key to generate unique filenames.

## Features

- **Thread-safe**: Concurrent access is supported across multiple goroutines
- **Persistent**: Cache survives application restarts  
- **Disk-based**: Uses local filesystem storage for durability
- **Automatic cleanup**: Implements LRU (Least Recently Used) eviction policy to manage disk space
- **Size-bounded**: Configurable maximum cache size in bytes that determines total disk space usage
- **Memory-mapped access**: Uses memory-mapped files for efficient index access
- **Atomic writes**: Uses temporary files and atomic renames to ensure data integrity

## Operations

### OnIndex
The primary method for accessing cached tarindex data using a callback pattern:

```go
err := cache.OnIndex(key, callback, indexGenerator)
```

- **key**: `DatarangeKey` identifying the specific datarange
- **callback**: `func(*tarindex.Index) error` - called with the loaded index
- **indexGenerator**: `func() ([]byte, error)` - called to generate index data if not cached

If the index is cached, the callback is called immediately with the cached index. If not cached, the indexGenerator is called to create the index data, which is then stored in the cache before calling the callback.

### Additional Methods

- **Stats()**: Returns `CacheStats` with information about cache state (entry count, total size, etc.)
- **Clear()**: Removes all entries from the cache
- **Close()**: Closes all open index files and cleans up resources

## Example Usage

```go
cache, err := diskcache.NewIndexDiskCache("/path/to/cache", 100*1024*1024) // 100MB limit
if err != nil {
    return err
}
defer cache.Close()

key := diskcache.DatarangeKey{
    Datas3tName:        "my-dataset",
    FirstIndex:         0,
    NumberOfDatapoints: 1000,
    TotalSizeBytes:     50*1024*1024,
}

err = cache.OnIndex(key, 
    func(index *tarindex.Index) error {
        // Use the index here
        fmt.Printf("Index has %d files\n", index.NumFiles())
        return nil
    },
    func() ([]byte, error) {
        // Generate index data if not cached
        return generateIndexData(), nil
    },
)
```
