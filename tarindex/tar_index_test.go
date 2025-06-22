package tarindex

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestOpenTarIndex(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "tarindex_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a test TAR archive
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)

	// Add a test file to the TAR
	header := &tar.Header{
		Name:    "test.txt",
		Size:    11,
		Mode:    0644,
		ModTime: time.Unix(1577836800, 0),
	}

	err = tw.WriteHeader(header)
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

	// Create an index from the TAR
	originalIndex, err := IndexTar(&tarBuf)
	if err != nil {
		t.Fatal(err)
	}

	// Write the index to a file
	indexPath := filepath.Join(tmpDir, "test.idx")
	err = os.WriteFile(indexPath, originalIndex, 0644)
	if err != nil {
		t.Fatal(err)
	}

	// Read the index back using OpenTarIndex
	readIndex, err := OpenTarIndex(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	defer readIndex.Close()

	// Compare the original and read indexes
	if !bytes.Equal(originalIndex, readIndex.Bytes) {
		t.Errorf("Read index doesn't match original index")
		t.Errorf("Original: %v", originalIndex)
		t.Errorf("Read:     %v", readIndex.Bytes)
	}

	if len(readIndex.Bytes) != 16 {
		t.Errorf("Expected 16 bytes, got %d", len(readIndex.Bytes))
	}
}

func TestOpenTarIndex_NonExistentFile(t *testing.T) {
	// Try to open a file that doesn't exist
	_, err := OpenTarIndex("/non/existent/file.idx")
	if err == nil {
		t.Error("Expected error for non-existent file")
	}
}

func TestIndex_Close(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "tarindex_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a test index file with proper 16-byte aligned data
	indexPath := filepath.Join(tmpDir, "test.idx")
	testData := make([]byte, 16) // 16 bytes to represent one file metadata entry
	// Fill with some test data
	copy(testData, []byte("test index data"))
	err = os.WriteFile(indexPath, testData, 0644)
	if err != nil {
		t.Fatal(err)
	}

	// Open the index
	index, err := OpenTarIndex(indexPath)
	if err != nil {
		t.Fatal(err)
	}

	// Verify data is accessible
	if !bytes.Equal(index.Bytes, testData) {
		t.Errorf("Expected %v, got %v", testData, index.Bytes)
	}

	// Close the index
	err = index.Close()
	if err != nil {
		t.Errorf("Close() returned error: %v", err)
	}

	// Close again should not error (idempotent)
	err = index.Close()
	if err != nil {
		t.Errorf("Second Close() returned error: %v", err)
	}
}
