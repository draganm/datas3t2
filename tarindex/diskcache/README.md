# TAR Index Disk Cache

This package provides a thread-safe, persistent disk-based cache for tarindex files.

## Overview

The cache stores tarindex files for dataranges from different datas3ts, enabling efficient retrieval of pre-computed index data without regenerating it each time.

## Cache Keys

Each tarindex file is identified by a unique key consisting of:
- **datas3t name**: The name of the datas3t dataset
- **datarange**: A tuple containing:
  - First index (starting position)
  - Number of datapoints
  - Total size in bytes

## Features

- **Thread-safe**: Concurrent access is supported across multiple goroutines
- **Persistent**: Cache survives application restarts
- **Disk-based**: Uses local filesystem storage for durability
- **Automatic cleanup**: Implements cache eviction policies to manage disk space
- **Size-bounded**: Each tarindex file size is proportional to the number of datapoints in the datarange. The cache is configured with a maximum byte limit that determines total disk space usage
- **Metadata-hashed**: Each tarindex filename is a SHA-256 hash of the datarange identity
- **LRU Eviction**: Uses Least Recently Used eviction policy to manage cache size

## Operations

### Get
Retrieves a cached tarindex for the specified datas3t and datarange. Returns either the instance of `tarindex.Index` or `nil` if there is no cached tarindex for the given datas3t/datarange.

### Put
Stores the byte slice representing the tarindex into the cache for the specified datas3t and datarange.
