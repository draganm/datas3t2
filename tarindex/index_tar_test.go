package tarindex

import (
	"archive/tar"
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestIndexTar_EmptyTar(t *testing.T) {
	// Create empty TAR
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	err := tw.Close()
	if err != nil {
		t.Fatal(err)
	}

	index, err := IndexTar(&buf)
	if err != nil {
		t.Fatal(err)
	}

	if len(index) != 0 {
		t.Errorf("Expected empty index, got %d bytes", len(index))
	}
}

func TestIndexTar_SingleFile(t *testing.T) {
	// Create TAR with single file
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	header := &tar.Header{
		Name:    "test.txt",
		Size:    11,
		Mode:    0644,
		ModTime: time.Unix(1577836800, 0), // 2020-01-01
	}

	err := tw.WriteHeader(header)
	if err != nil {
		t.Fatal(err)
	}

	_, err = tw.Write([]byte("hello world"))
	if err != nil {
		t.Fatal(err)
	}

	err = tw.Close()
	if err != nil {
		t.Fatal(err)
	}

	index, err := IndexTar(&buf)
	if err != nil {
		t.Fatal(err)
	}

	// Should have one 16-byte entry
	if len(index) != 16 {
		t.Errorf("Expected 16 bytes, got %d", len(index))
	}

	// Verify the index entry
	verifyIndexEntry(t, index[0:16], 0, 1, 11)
}

func TestIndexTar_MultipleFiles(t *testing.T) {
	// Create TAR with multiple files of different sizes
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	files := []struct {
		name    string
		size    int64
		content string
	}{
		{"small.txt", 5, "hello"},
		{"medium.txt", 1024, strings.Repeat("x", 1024)},
		{"large.txt", 2048, strings.Repeat("y", 2048)},
		{"empty.txt", 0, ""},
	}

	for _, file := range files {
		header := &tar.Header{
			Name:    file.name,
			Size:    file.size,
			Mode:    0644,
			ModTime: time.Unix(1577836800, 0),
		}

		err := tw.WriteHeader(header)
		if err != nil {
			t.Fatal(err)
		}

		if file.size > 0 {
			_, err = tw.Write([]byte(file.content))
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	err := tw.Close()
	if err != nil {
		t.Fatal(err)
	}

	index, err := IndexTar(&buf)
	if err != nil {
		t.Fatal(err)
	}

	// Should have 4 entries, 16 bytes each
	expectedSize := 4 * 16
	if len(index) != expectedSize {
		t.Errorf("Expected %d bytes, got %d", expectedSize, len(index))
	}

	// Verify each entry
	var position int64 = 0
	for i, file := range files {
		entryStart := i * 16
		verifyIndexEntry(t, index[entryStart:entryStart+16], position, 1, file.size)

		// Calculate next position
		contentBlocks := (file.size + 511) / 512
		position += 512 + (contentBlocks * 512)
	}
}

func TestIndexTar_GNUFormat(t *testing.T) {
	// Create TAR with GNU format (long filename)
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	longName := strings.Repeat("verylongfilename", 10) + ".txt"
	header := &tar.Header{
		Name:    longName,
		Size:    100,
		Mode:    0644,
		ModTime: time.Unix(1577836800, 0),
		Format:  tar.FormatGNU,
	}

	err := tw.WriteHeader(header)
	if err != nil {
		t.Fatal(err)
	}

	_, err = tw.Write(bytes.Repeat([]byte("a"), 100))
	if err != nil {
		t.Fatal(err)
	}

	err = tw.Close()
	if err != nil {
		t.Fatal(err)
	}

	index, err := IndexTar(&buf)
	if err != nil {
		t.Fatal(err)
	}

	// GNU format may have additional headers for long names
	// But our implementation should still work
	if len(index) == 0 {
		t.Error("Expected non-empty index")
	}

	// Check that index length is multiple of 16
	if len(index)%16 != 0 {
		t.Errorf("Index length %d is not multiple of 16", len(index))
	}
}

func TestIndexTar_PAXFormat(t *testing.T) {
	// Create TAR with PAX format
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	header := &tar.Header{
		Name:    "test.txt",
		Size:    50,
		Mode:    0644,
		ModTime: time.Unix(1577836800, 0),
		Format:  tar.FormatPAX,
		PAXRecords: map[string]string{
			"custom.field": "custom value",
		},
	}

	err := tw.WriteHeader(header)
	if err != nil {
		t.Fatal(err)
	}

	_, err = tw.Write(bytes.Repeat([]byte("b"), 50))
	if err != nil {
		t.Fatal(err)
	}

	err = tw.Close()
	if err != nil {
		t.Fatal(err)
	}

	index, err := IndexTar(&buf)
	if err != nil {
		t.Fatal(err)
	}

	// PAX format may have additional headers
	if len(index) == 0 {
		t.Error("Expected non-empty index")
	}

	// Check that index length is multiple of 16
	if len(index)%16 != 0 {
		t.Errorf("Index length %d is not multiple of 16", len(index))
	}
}

func TestIndexTar_USTARFormat(t *testing.T) {
	// Create TAR with USTAR format
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	header := &tar.Header{
		Name:    "test.txt",
		Size:    25,
		Mode:    0644,
		ModTime: time.Unix(1577836800, 0),
		Format:  tar.FormatUSTAR,
		Uname:   "testuser",
		Gname:   "testgroup",
	}

	err := tw.WriteHeader(header)
	if err != nil {
		t.Fatal(err)
	}

	_, err = tw.Write(bytes.Repeat([]byte("c"), 25))
	if err != nil {
		t.Fatal(err)
	}

	err = tw.Close()
	if err != nil {
		t.Fatal(err)
	}

	index, err := IndexTar(&buf)
	if err != nil {
		t.Fatal(err)
	}

	// Should have one entry
	if len(index) != 16 {
		t.Errorf("Expected 16 bytes, got %d", len(index))
	}

	verifyIndexEntry(t, index[0:16], 0, 1, 25)
}

func TestIndexTar_DifferentFileTypes(t *testing.T) {
	// Create TAR with different file types
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	entries := []struct {
		name     string
		typeflag byte
		size     int64
		content  string
	}{
		{"file.txt", tar.TypeReg, 7, "regular"},
		{"dir/", tar.TypeDir, 0, ""},
		{"link.txt", tar.TypeSymlink, 0, ""},
	}

	for _, entry := range entries {
		header := &tar.Header{
			Name:     entry.name,
			Size:     entry.size,
			Mode:     0644,
			ModTime:  time.Unix(1577836800, 0),
			Typeflag: entry.typeflag,
		}

		if entry.typeflag == tar.TypeSymlink {
			header.Linkname = "file.txt"
		}

		err := tw.WriteHeader(header)
		if err != nil {
			t.Fatal(err)
		}

		if entry.size > 0 {
			_, err = tw.Write([]byte(entry.content))
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	err := tw.Close()
	if err != nil {
		t.Fatal(err)
	}

	index, err := IndexTar(&buf)
	if err != nil {
		t.Fatal(err)
	}

	// Should have 3 entries
	expectedSize := 3 * 16
	if len(index) != expectedSize {
		t.Errorf("Expected %d bytes, got %d", expectedSize, len(index))
	}
}

func TestIndexTar_LargeFiles(t *testing.T) {
	// Test with file size that requires 6 bytes (close to limit)
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	// Use a size that's close to the 6-byte limit (2^48 - 1)
	largeSize := int64(1 << 20) // 1MB for testing

	header := &tar.Header{
		Name:    "large.bin",
		Size:    largeSize,
		Mode:    0644,
		ModTime: time.Unix(1577836800, 0),
	}

	err := tw.WriteHeader(header)
	if err != nil {
		t.Fatal(err)
	}

	// Write data in chunks to avoid memory issues
	chunk := make([]byte, 1024)
	written := int64(0)
	for written < largeSize {
		toWrite := int64(len(chunk))
		if written+toWrite > largeSize {
			toWrite = largeSize - written
		}
		_, err = tw.Write(chunk[:toWrite])
		if err != nil {
			t.Fatal(err)
		}
		written += toWrite
	}

	err = tw.Close()
	if err != nil {
		t.Fatal(err)
	}

	index, err := IndexTar(&buf)
	if err != nil {
		t.Fatal(err)
	}

	if len(index) != 16 {
		t.Errorf("Expected 16 bytes, got %d", len(index))
	}

	verifyIndexEntry(t, index[0:16], 0, 1, largeSize)
}

// Helper function to verify index entry binary format
func verifyIndexEntry(t *testing.T, entry []byte, expectedPos int64, expectedBlocks uint16, expectedSize int64) {
	t.Helper()

	if len(entry) != 16 {
		t.Errorf("Entry should be 16 bytes, got %d", len(entry))
		return
	}

	// Check header position (8 bytes)
	pos := binary.BigEndian.Uint64(entry[0:8])
	if pos != uint64(expectedPos) {
		t.Errorf("Expected position %d, got %d", expectedPos, pos)
	}

	// Check header blocks (2 bytes)
	blocks := binary.BigEndian.Uint16(entry[8:10])
	if blocks != expectedBlocks {
		t.Errorf("Expected blocks %d, got %d", expectedBlocks, blocks)
	}

	// Check file size (6 bytes) - convert back from 6-byte big-endian
	sizeBytes := make([]byte, 8)
	copy(sizeBytes[2:8], entry[10:16])
	size := binary.BigEndian.Uint64(sizeBytes)
	if size != uint64(expectedSize) {
		t.Errorf("Expected size %d, got %d", expectedSize, size)
	}
}

func TestIndexTar_CorruptedTar(t *testing.T) {
	// Test with corrupted TAR data
	corruptedData := []byte("this is not a valid tar file")
	reader := bytes.NewReader(corruptedData)

	_, err := IndexTar(reader)
	if err == nil {
		t.Error("Expected error for corrupted TAR, got nil")
	}
}

func TestIndexTar_IncompleteRead(t *testing.T) {
	// Create a TAR but truncate it
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	header := &tar.Header{
		Name:    "test.txt",
		Size:    1000, // Claim 1000 bytes
		Mode:    0644,
		ModTime: time.Unix(1577836800, 0),
	}

	err := tw.WriteHeader(header)
	if err != nil {
		t.Fatal(err)
	}

	// Only write 100 bytes instead of 1000
	_, err = tw.Write(make([]byte, 100))
	if err != nil {
		t.Fatal(err)
	}

	// Don't close properly to create incomplete TAR
	data := buf.Bytes()

	// Truncate to simulate incomplete read
	truncated := data[:len(data)/2]
	reader := bytes.NewReader(truncated)

	_, err = IndexTar(reader)
	if err == nil {
		t.Error("Expected error for incomplete TAR, got nil")
	}
}

func TestIndexTar_BinaryFormatExact(t *testing.T) {
	// Test the exact binary format as specified in README
	// Example: file with header at 1024, 1 block, size 2048
	// Should produce: 00 00 00 00 00 00 04 00 00 01 00 00 00 00 08 00
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	// Add some padding to get header at position 1024
	// First add a file that will end exactly at position 1024
	paddingSize := int64(1024 - 512) // 512 bytes for first header
	header1 := &tar.Header{
		Name:    "padding.txt",
		Size:    paddingSize,
		Mode:    0644,
		ModTime: time.Unix(1577836800, 0),
	}

	err := tw.WriteHeader(header1)
	if err != nil {
		t.Fatal(err)
	}

	_, err = tw.Write(make([]byte, paddingSize))
	if err != nil {
		t.Fatal(err)
	}

	// Now add the test file that should have header at position 1024
	header2 := &tar.Header{
		Name:    "test.txt",
		Size:    2048,
		Mode:    0644,
		ModTime: time.Unix(1577836800, 0),
	}

	err = tw.WriteHeader(header2)
	if err != nil {
		t.Fatal(err)
	}

	_, err = tw.Write(make([]byte, 2048))
	if err != nil {
		t.Fatal(err)
	}

	err = tw.Close()
	if err != nil {
		t.Fatal(err)
	}

	index, err := IndexTar(&buf)
	if err != nil {
		t.Fatal(err)
	}

	// Should have 2 entries
	if len(index) != 32 {
		t.Errorf("Expected 32 bytes (2 entries), got %d", len(index))
	}

	// Check the second entry (at position 1024)
	secondEntry := index[16:32]
	expected := []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, // Position: 1024
		0x00, 0x01, // Blocks: 1
		0x00, 0x00, 0x00, 0x00, 0x08, 0x00, // Size: 2048
	}

	if !bytes.Equal(secondEntry, expected) {
		t.Errorf("Binary format mismatch.\nExpected: %v\nGot:      %v", expected, secondEntry)
	}
}

func TestIndexTar_MaxFileSizeSupport(t *testing.T) {
	// Test with a large file size that fits in 6 bytes
	// Use a size that's large but still manageable for testing
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	// Use a reasonable large size (16MB) to test 6-byte encoding
	largeSize := int64(16 << 20) // 16MB

	header := &tar.Header{
		Name:    "large.bin",
		Size:    largeSize,
		Mode:    0644,
		ModTime: time.Unix(1577836800, 0),
	}

	err := tw.WriteHeader(header)
	if err != nil {
		t.Fatal(err)
	}

	// Write the actual content in chunks
	chunk := make([]byte, 1024)
	written := int64(0)
	for written < largeSize {
		toWrite := int64(len(chunk))
		if written+toWrite > largeSize {
			toWrite = largeSize - written
		}
		_, err = tw.Write(chunk[:toWrite])
		if err != nil {
			t.Fatal(err)
		}
		written += toWrite
	}

	err = tw.Close()
	if err != nil {
		t.Fatal(err)
	}

	index, err := IndexTar(&buf)
	if err != nil {
		t.Fatal(err)
	}

	if len(index) != 16 {
		t.Errorf("Expected 16 bytes, got %d", len(index))
	}

	// Verify the size field stores the large value correctly
	entry := index[0:16]
	sizeBytes := make([]byte, 8)
	copy(sizeBytes[2:8], entry[10:16])
	size := binary.BigEndian.Uint64(sizeBytes)
	if size != uint64(largeSize) {
		t.Errorf("Expected size %d, got %d", largeSize, size)
	}

	// Also test that we can represent the theoretical maximum (just the encoding)
	maxValue := uint64((1 << 48) - 1)
	testEntry := make([]byte, 16)

	// Set position and blocks to 0 and 1
	binary.BigEndian.PutUint64(testEntry[0:8], 0)
	binary.BigEndian.PutUint16(testEntry[8:10], 1)

	// Set the maximum 6-byte value
	maxBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(maxBytes, maxValue)
	copy(testEntry[10:16], maxBytes[2:8])

	// Decode it back
	decodedSizeBytes := make([]byte, 8)
	copy(decodedSizeBytes[2:8], testEntry[10:16])
	decodedSize := binary.BigEndian.Uint64(decodedSizeBytes)

	if decodedSize != maxValue {
		t.Errorf("6-byte encoding test failed. Expected %d, got %d", maxValue, decodedSize)
	}
}

func BenchmarkIndexTar(b *testing.B) {
	// Create a TAR with multiple files of varying sizes
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	// Add files of different sizes
	for i := 0; i < 100; i++ {
		size := int64(i * 100) // Varying sizes from 0 to 9900 bytes
		header := &tar.Header{
			Name:    fmt.Sprintf("file_%d.txt", i),
			Size:    size,
			Mode:    0644,
			ModTime: time.Unix(1577836800, 0),
		}

		err := tw.WriteHeader(header)
		if err != nil {
			b.Fatal(err)
		}

		if size > 0 {
			_, err = tw.Write(make([]byte, size))
			if err != nil {
				b.Fatal(err)
			}
		}
	}

	err := tw.Close()
	if err != nil {
		b.Fatal(err)
	}

	tarData := buf.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(tarData)
		_, err := IndexTar(reader)
		if err != nil {
			b.Fatal(err)
		}
	}
}
