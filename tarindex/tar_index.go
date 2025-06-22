package tarindex

import (
	"encoding/binary"
	"fmt"
	"os"
	"syscall"
)

type Index struct {
	file  *os.File
	Bytes []byte
}

// OpenTarIndex opens a tar index file and returns an Index object.
// name is the path to the index file.
// Returns nil and an error if the file cannot be opened.
func OpenTarIndex(name string) (*Index, error) {
	// Open the index file
	file, err := os.Open(name)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file %s: %w", name, err)
	}

	// Get file info to determine size
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat index file %s: %w", name, err)
	}

	fileSize := info.Size()
	if fileSize == 0 {
		return nil, fmt.Errorf("index file %s is empty", name)
	}

	// Memory map the file
	fd := int(file.Fd())
	data, err := syscall.Mmap(fd, 0, int(fileSize), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to mmap index file %s: %w", name, err)
	}

	if len(data)%16 != 0 {
		return nil, fmt.Errorf("index file %s is not a multiple of 16 bytes", name)
	}

	return &Index{
		file:  file,
		Bytes: data,
	}, nil
}

func (i *Index) Close() error {
	var errs []error

	// Unmap the memory if it was mapped
	if i.Bytes != nil {
		err := syscall.Munmap(i.Bytes)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to munmap: %w", err))
		}
		i.Bytes = nil // Prevent double unmapping
	}

	// Close the file
	if i.file != nil {
		err := i.file.Close()
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to close file: %w", err))
		}
		i.file = nil // Prevent double closing
	}

	// Return first error if any occurred
	if len(errs) > 0 {
		return errs[0]
	}

	return nil
}

// FileMetadata contains the metadata for a file in the index.
// Start is the offset of the file in the tar archive.
// HeaderBlocks is the number of blocks in the header of the file (each block is 512 bytes).
// Size is the size of the file in bytes.
type FileMetadata struct {
	Start        int64
	HeaderBlocks uint16
	Size         int64
}

// GetFileMetadata returns the metadata for a file in the index.
// index is the index of the file in the index.
// Returns the metadata for the file.
func (i *Index) GetFileMetadata(index uint64) (FileMetadata, error) {
	if index >= uint64(len(i.Bytes))/16 {
		return FileMetadata{}, fmt.Errorf("index out of bounds: %d", index)
	}

	offset := int64(index * 16)
	start := int64(binary.BigEndian.Uint64(i.Bytes[offset : offset+8]))
	blocks := binary.BigEndian.Uint16(i.Bytes[offset+8 : offset+10])

	// File size is stored in 6 bytes (offset 10-16), need to pad to 8 bytes for Uint64
	sizeBytes := make([]byte, 8)
	copy(sizeBytes[2:], i.Bytes[offset+10:offset+16]) // Copy 6 bytes to the last 6 positions
	size := int64(binary.BigEndian.Uint64(sizeBytes))

	return FileMetadata{
		Start:        start,
		HeaderBlocks: blocks,
		Size:         size,
	}, nil
}

func (i *Index) NumFiles() uint64 {
	return uint64(len(i.Bytes) / 16)
}
