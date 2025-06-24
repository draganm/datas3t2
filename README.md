# datas3t2

A high-performance data management system for storing, indexing, and retrieving large-scale datasets in S3-compatible object storage.

## Overview

datas3t2 is designed for efficiently managing datasets containing millions of individual files (called "datapoints"). It stores files as indexed TAR archives in S3-compatible storage, enabling fast random access without the overhead of extracting entire archives.

## Key Features

### 🗜️ **Efficient Storage**
- Packs individual files into TAR archives
- Eliminates S3 object overhead for small files
- Supports datasets with millions of datapoints

### ⚡ **Fast Random Access**
- Creates lightweight indices for TAR archives
- Enables direct access to specific files without extraction
- Disk-based caching for frequently accessed indices

### 📦 **Range-based Operations**
- Upload and download data in configurable chunks (dataranges)
- Supports partial dataset retrieval
- Parallel processing of multiple ranges

### 🔗 **Direct Client-to-Storage Transfer**
- Uses S3 presigned URLs for efficient data transfer
- Bypasses server for large file operations
- Supports multipart uploads for large datasets

### 🛡️ **Data Integrity**
- Validates TAR structure and file naming conventions
- Ensures datapoint consistency across operations
- Transactional database operations

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────────┐
│   Client/CLI    │───▶│   HTTP API       │───▶│   PostgreSQL        │
│                 │    │   Server         │    │   (Metadata)        │
└─────────────────┘    └──────────────────┘    └─────────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │  S3-Compatible   │
                       │  Object Storage  │
                       │  (TAR Archives)  │
                       └──────────────────┘
```

### Components

- **HTTP API Server**: REST API for dataset management
- **Client Library**: Go SDK for programmatic access  
- **PostgreSQL Database**: Stores metadata and indices
- **S3-Compatible Storage**: Stores TAR archives and indices
- **TAR Indexing Engine**: Creates fast-access indices
- **Disk Cache**: Local caching for performance

## Core Concepts

### Datasets (datas3ts)
Named collections of related datapoints. Each dataset is associated with an S3 bucket configuration.

### Datapoints
Individual files within a dataset, numbered sequentially:
- `00000000000000000001.txt`
- `00000000000000000002.jpg`
- `00000000000000000003.json`

### Dataranges
Contiguous chunks of datapoints stored as TAR archives:
- `datas3t/my-dataset/dataranges/00000000000000000001-00000000000000001000.tar`
- `datas3t/my-dataset/dataranges/00000000000000001001-00000000000000002000.tar`

### TAR Indices
Lightweight index files enabling fast random access:
- `datas3t/my-dataset/dataranges/00000000000000000001-00000000000000001000.index.zst`

## Quick Start

### Prerequisites

- [Nix with flakes enabled](https://nixos.wiki/wiki/Flakes) (recommended)
- Go 1.24.3+
- PostgreSQL 12+
- S3-compatible storage (AWS S3, MinIO, etc.)

### Development Setup

```bash
# Clone the repository
git clone https://github.com/draganm/datas3t2.git
cd datas3t2

# Enter development environment
nix develop

# Run tests
nix develop -c go test ./...

# Generate code
nix develop -c go generate ./...
```

### Running the Server

```bash
# Set environment variables
export DB_URL="postgres://user:password@localhost:5432/datas3t2"
export ADDR=":8080"

# Run database migrations
nix develop -c go run cmd/server/server.go

# Server will start on http://localhost:8080
```

## API Usage

### 1. Configure S3 Bucket

```bash
curl -X POST http://localhost:8080/api/bucket \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-bucket-config",
    "endpoint": "s3.amazonaws.com",
    "bucket": "my-data-bucket",
    "access_key": "ACCESS_KEY",
    "secret_key": "SECRET_KEY",
    "use_tls": true
  }'
```

### 2. Create Dataset

```bash
curl -X POST http://localhost:8080/api/datas3t \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-dataset",
    "bucket": "my-bucket-config"
  }'
```

### 3. Upload Datarange

```bash
# Start upload
curl -X POST http://localhost:8080/api/datarange/upload/start \
  -H "Content-Type: application/json" \
  -d '{
    "datas3t_name": "my-dataset",
    "first_datapoint_index": 1,
    "number_of_datapoints": 1000,
    "data_size": 1048576
  }'

# Use returned presigned URLs to upload TAR archive and index
# Then complete the upload
curl -X POST http://localhost:8080/api/datarange/upload/complete \
  -H "Content-Type: application/json" \
  -d '{
    "datarange_upload_id": 123
  }'
```

### 4. Download Datapoints

```bash
curl -X POST http://localhost:8080/api/download/presign \
  -H "Content-Type: application/json" \
  -d '{
    "datas3t_name": "my-dataset",
    "first_datapoint": 100,
    "last_datapoint": 200
  }'
```

## Client Library Usage

```go
package main

import (
    "context"
    "github.com/draganm/datas3t2/client"
)

func main() {
    // Create client
    c := client.New("http://localhost:8080")
    
    // List datasets
    datasets, err := c.ListDatas3ts(context.Background())
    if err != nil {
        panic(err)
    }
    
    // Download specific datapoints
    response, err := c.PreSignDownloadForDatapoints(context.Background(), &client.PreSignDownloadForDatapointsRequest{
        Datas3tName:    "my-dataset",
        FirstDatapoint: 1,
        LastDatapoint:  100,
    })
    if err != nil {
        panic(err)
    }
    
    // Use presigned URLs to download data directly from S3
    for _, segment := range response.DownloadSegments {
        // Download using segment.PresignedURL and segment.Range
    }
}
```

## File Naming Convention

Datapoints must follow the naming pattern `%020d.<extension>`:
- ✅ `00000000000000000001.txt`
- ✅ `00000000000000000042.jpg`  
- ✅ `00000000000000001337.json`
- ❌ `file1.txt`
- ❌ `1.txt`
- ❌ `001.txt`

## Performance Characteristics

### Storage Efficiency
- **Small Files**: 99%+ storage efficiency vs individual S3 objects
- **Large Datasets**: Linear scaling with dataset size

### Access Performance
- **Index Lookup**: O(1) file location within TAR
- **Range Queries**: Optimized byte-range requests
- **Caching**: Local disk cache for frequently accessed indices

### Scalability
- **Concurrent Operations**: Supports parallel uploads/downloads
- **Large Datasets**: Tested with millions of datapoints
- **Distributed**: Stateless server design for horizontal scaling

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes following the existing patterns
4. Run tests: `nix develop -c go test ./...`
5. Submit a pull request

### Development Guidelines

- Use the Nix development environment for consistency
- Follow Go error handling best practices
- Use the `postgresstore` package for all database queries
- Add tests for new functionality
- Update API documentation for new endpoints

## Architecture Details

### Database Schema
- **s3_buckets**: S3 configuration storage
- **datasets**: Dataset metadata
- **dataranges**: TAR archive metadata and byte ranges
- **datarange_uploads**: Temporary upload state management

### TAR Index Format
Binary format with 16-byte entries per file:
- Bytes 0-7: File position in TAR (big-endian uint64)
- Bytes 8-9: Header blocks count (big-endian uint16)  
- Bytes 10-15: File size (big-endian, 48-bit)

### Caching Strategy
- **Memory**: In-memory index objects during operations
- **Disk**: Persistent cache for TAR indices
- **LRU Eviction**: Automatic cleanup based on access patterns
- **Cache Keys**: SHA-256 hash of datarange metadata

## License

This project is licensed under the AGPLV3 License - see the [LICENSE](LICENSE) file for details.

## Support

For questions, issues, or contributions:
- Open an issue on GitHub
- Check existing documentation
- Review test files for usage examples 