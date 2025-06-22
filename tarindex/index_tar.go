package tarindex

import (
	"archive/tar"
	"encoding/binary"
	"io"
)

func IndexTar(r io.Reader) ([]byte, error) {
	var index []byte
	tr := tar.NewReader(r)
	var position int64 = 0

	for {
		// Record current position before reading header
		headerPosition := position

		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		// Calculate header blocks (standard TAR header is always 1 block = 512 bytes)
		headerBlocks := uint16(1)

		// Get file size
		fileSize := header.Size

		// Create index entry (8 + 2 + 6 = 16 bytes per entry)
		entry := make([]byte, 16)

		// Header Position (8 bytes, big-endian)
		binary.BigEndian.PutUint64(entry[0:8], uint64(headerPosition))

		// Header Blocks (2 bytes, big-endian)
		binary.BigEndian.PutUint16(entry[8:10], headerBlocks)

		// File Size (6 bytes, big-endian)
		// We need to store a 64-bit value in 6 bytes, so we take the lower 48 bits
		fileSizeBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(fileSizeBytes, uint64(fileSize))
		copy(entry[10:16], fileSizeBytes[2:8]) // Take bytes 2-7 (6 bytes total)

		index = append(index, entry...)

		// Calculate next position
		// TAR format: header (512 bytes) + file content (rounded up to 512-byte boundary)
		contentBlocks := (fileSize + 511) / 512 // Round up to nearest 512-byte block
		position = headerPosition + 512 + (contentBlocks * 512)

		// Skip the file content in the reader
		_, err = io.CopyN(io.Discard, tr, header.Size)
		if err != nil {
			return nil, err
		}
	}

	return index, nil
}
