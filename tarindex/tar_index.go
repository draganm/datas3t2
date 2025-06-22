package tarindex

import (
	"fmt"
	"os"
	"syscall"
)

type Index struct {
	file  *os.File
	Bytes []byte
}

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
