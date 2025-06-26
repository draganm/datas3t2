package client

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/draganm/datas3t2/server/dataranges"
	"github.com/draganm/datas3t2/tarindex"
	"golang.org/x/sync/errgroup"
)

// UploadOptions configures the upload behavior
type UploadOptions struct {
	MaxParallelism int // Maximum number of concurrent uploads (default: 4)
	MaxRetries     int // Maximum number of retry attempts per chunk (default: 3)
}

// DefaultUploadOptions returns sensible default options
func DefaultUploadOptions() *UploadOptions {
	return &UploadOptions{
		MaxParallelism: 4,
		MaxRetries:     3,
	}
}

// createBackoffConfig creates a backoff configuration with the specified max retries
func createBackoffConfig(maxRetries int) backoff.BackOff {
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = 1 * time.Second
	expBackoff.MaxInterval = 30 * time.Second
	expBackoff.Multiplier = 2.0
	expBackoff.RandomizationFactor = 0.5

	return backoff.WithMaxRetries(expBackoff, uint64(maxRetries))
}

func (c *Client) UploadDataRangeFile(ctx context.Context, datas3tName string, file io.ReaderAt, size int64, opts *UploadOptions) error {
	if opts == nil {
		opts = DefaultUploadOptions()
	}

	// Phase 1: Analyze TAR file to extract datapoint information
	tarInfo, err := analyzeTarFile(file, size)
	if err != nil {
		return fmt.Errorf("failed to analyze tar file: %w", err)
	}

	// Phase 2: Generate TAR index
	indexData, err := generateTarIndex(file, size)
	if err != nil {
		return fmt.Errorf("failed to generate tar index: %w", err)
	}

	// Phase 3: Start upload
	uploadReq := &dataranges.UploadDatarangeRequest{
		Datas3tName:         datas3tName,
		DataSize:            uint64(size),
		NumberOfDatapoints:  uint64(tarInfo.NumDatapoints),
		FirstDatapointIndex: uint64(tarInfo.FirstDatapointIndex),
	}

	uploadResp, err := c.StartDatarangeUpload(ctx, uploadReq)
	if err != nil {
		return fmt.Errorf("failed to start upload: %w", err)
	}

	// Phase 4: Upload data
	var uploadIDs []string
	if uploadResp.UseDirectPut {
		// Direct PUT for small files
		err = uploadDataDirectPut(ctx, uploadResp.PresignedDataPutURL, file, size, opts.MaxRetries)
		if err != nil {
			// Cancel upload on failure
			cancelReq := &dataranges.CancelUploadRequest{
				DatarangeUploadID: uploadResp.DatarangeID,
			}
			c.CancelDatarangeUpload(ctx, cancelReq) // Best effort, ignore error
			return fmt.Errorf("failed to upload data: %w", err)
		}
	} else {
		// Multipart upload for large files
		uploadIDs, err = uploadDataMultipart(ctx, uploadResp.PresignedMultipartUploadPutURLs, file, size, opts)
		if err != nil {
			// Cancel upload on failure
			cancelReq := &dataranges.CancelUploadRequest{
				DatarangeUploadID: uploadResp.DatarangeID,
			}
			c.CancelDatarangeUpload(ctx, cancelReq) // Best effort, ignore error
			return fmt.Errorf("failed to upload data: %w", err)
		}
	}

	// Phase 5: Upload index
	err = uploadIndexWithRetry(ctx, uploadResp.PresignedIndexPutURL, indexData, opts.MaxRetries)
	if err != nil {
		// Cancel upload on failure
		cancelReq := &dataranges.CancelUploadRequest{
			DatarangeUploadID: uploadResp.DatarangeID,
		}
		c.CancelDatarangeUpload(ctx, cancelReq) // Best effort, ignore error
		return fmt.Errorf("failed to upload index: %w", err)
	}

	// Phase 6: Complete upload
	completeReq := &dataranges.CompleteUploadRequest{
		DatarangeUploadID: uploadResp.DatarangeID,
		UploadIDs:         uploadIDs, // ETags for multipart, empty for direct PUT
	}

	err = c.CompleteDatarangeUpload(ctx, completeReq)
	if err != nil {
		return fmt.Errorf("failed to complete upload: %w", err)
	}

	return nil
}

// TarInfo contains metadata extracted from analyzing the TAR file
type TarInfo struct {
	FirstDatapointIndex int64
	NumDatapoints       int
}

// analyzeTarFile reads the TAR file to extract datapoint information and validate naming convention
func analyzeTarFile(file io.ReaderAt, size int64) (*TarInfo, error) {
	reader := io.NewSectionReader(file, 0, size)
	tr := tar.NewReader(reader)

	var datapointKeys []int64

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read tar entry: %w", err)
		}

		// Skip directories and other non-regular files
		if header.Typeflag != tar.TypeReg {
			continue
		}

		// Validate filename format and extract datapoint key
		datapointKey, err := extractDatapointKeyFromFileName(header.Name)
		if err != nil {
			return nil, fmt.Errorf("invalid filename '%s': %w", header.Name, err)
		}

		datapointKeys = append(datapointKeys, datapointKey)

		// Skip file content
		_, err = io.Copy(io.Discard, tr)
		if err != nil {
			return nil, fmt.Errorf("failed to skip file content: %w", err)
		}
	}

	if len(datapointKeys) == 0 {
		return nil, fmt.Errorf("no valid datapoint files found in tar archive")
	}

	// Sort the keys to ensure they're in order
	sort.Slice(datapointKeys, func(i, j int) bool {
		return datapointKeys[i] < datapointKeys[j]
	})

	// Validate there are no gaps in the sequence
	firstKey := datapointKeys[0]
	for i, key := range datapointKeys {
		expectedKey := firstKey + int64(i)
		if key != expectedKey {
			return nil, fmt.Errorf("gap in datapoint sequence: expected %d, found %d", expectedKey, key)
		}
	}

	return &TarInfo{
		FirstDatapointIndex: firstKey,
		NumDatapoints:       len(datapointKeys),
	}, nil
}

// isValidFileName checks if the filename matches the pattern %020d.<extension>
func isValidFileName(fileName string) bool {
	// Find the last dot
	dotIndex := strings.LastIndex(fileName, ".")
	if dotIndex == -1 || dotIndex == 0 {
		return false // No extension or starts with dot
	}

	// Check if the part before the extension is exactly 20 digits
	namepart := fileName[:dotIndex]
	if len(namepart) != 20 {
		return false
	}

	// Check if all characters are digits
	for _, r := range namepart {
		if r < '0' || r > '9' {
			return false
		}
	}

	return true
}

// extractDatapointKeyFromFileName extracts the numeric datapoint key from a filename
func extractDatapointKeyFromFileName(fileName string) (int64, error) {
	if !isValidFileName(fileName) {
		return 0, fmt.Errorf("filename doesn't match pattern %%020d.<extension>")
	}

	dotIndex := strings.LastIndex(fileName, ".")
	namepart := fileName[:dotIndex]

	key, err := strconv.ParseInt(namepart, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse numeric part: %w", err)
	}

	return key, nil
}

// generateTarIndex creates a tar index from the file
func generateTarIndex(file io.ReaderAt, size int64) ([]byte, error) {
	reader := io.NewSectionReader(file, 0, size)
	return tarindex.IndexTar(reader)
}

// uploadDataDirectPut handles direct PUT upload for small files
func uploadDataDirectPut(ctx context.Context, url string, file io.ReaderAt, size int64, maxRetries int) error {
	operation := func() error {
		// Create reader for entire file
		reader := io.NewSectionReader(file, 0, size)

		// Create HTTP request
		req, err := http.NewRequestWithContext(ctx, "PUT", url, reader)
		if err != nil {
			return err
		}
		req.ContentLength = size

		// Execute request
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			return nil
		}

		// Handle retryable errors
		if resp.StatusCode >= 500 || resp.StatusCode == 429 {
			return fmt.Errorf("HTTP %d", resp.StatusCode)
		}

		// Non-retryable error - wrap with Permanent to stop retrying
		return backoff.Permanent(fmt.Errorf("HTTP %d", resp.StatusCode))
	}

	b := createBackoffConfig(maxRetries)
	err := backoff.Retry(operation, backoff.WithContext(b, ctx))
	if err != nil {
		return fmt.Errorf("direct PUT failed: %w", err)
	}

	return nil
}

// uploadDataMultipart handles multipart upload for large files
func uploadDataMultipart(ctx context.Context, urls []string, file io.ReaderAt, size int64, opts *UploadOptions) ([]string, error) {
	numParts := len(urls)
	if numParts == 0 {
		return nil, fmt.Errorf("no upload URLs provided")
	}

	// Calculate chunk sizes
	chunkSize := size / int64(numParts)
	lastChunkSize := size - (chunkSize * int64(numParts-1)) // Handle remainder

	// Use errgroup with parallelism limit
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(opts.MaxParallelism)

	etags := make([]string, numParts)
	for i, url := range urls {
		i, url := i, url // capture loop variables
		g.Go(func() error {
			// Calculate this part's boundaries
			offset := int64(i) * chunkSize
			partSize := chunkSize
			if i == numParts-1 { // last part
				partSize = lastChunkSize
			}

			// Upload chunk with retry
			etag, err := uploadChunkWithRetry(ctx, url, file, offset, partSize, opts.MaxRetries)
			if err != nil {
				return fmt.Errorf("failed to upload part %d: %w", i+1, err)
			}
			etags[i] = etag
			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		return nil, fmt.Errorf("multipart upload failed: %w", err)
	}

	return etags, nil
}

// uploadChunkWithRetry uploads a single chunk with exponential backoff retry
func uploadChunkWithRetry(ctx context.Context, url string, file io.ReaderAt, offset, size int64, maxRetries int) (string, error) {
	var etag string

	operation := func() error {
		// Create section reader for this chunk
		reader := io.NewSectionReader(file, offset, size)

		// Create HTTP request
		req, err := http.NewRequestWithContext(ctx, "PUT", url, reader)
		if err != nil {
			return err
		}
		req.ContentLength = size

		// Execute request
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			etag = resp.Header.Get("ETag")
			return nil
		}

		// Handle retryable errors
		if resp.StatusCode >= 500 || resp.StatusCode == 429 {
			return fmt.Errorf("HTTP %d", resp.StatusCode)
		}

		// Non-retryable error - wrap with Permanent to stop retrying
		return backoff.Permanent(fmt.Errorf("HTTP %d", resp.StatusCode))
	}

	b := createBackoffConfig(maxRetries)
	err := backoff.Retry(operation, backoff.WithContext(b, ctx))
	if err != nil {
		return "", fmt.Errorf("chunk upload failed: %w", err)
	}

	return etag, nil
}

// uploadIndexWithRetry uploads the tar index with retry logic
func uploadIndexWithRetry(ctx context.Context, url string, indexData []byte, maxRetries int) error {
	operation := func() error {
		// Create HTTP request
		req, err := http.NewRequestWithContext(ctx, "PUT", url, bytes.NewReader(indexData))
		if err != nil {
			return err
		}
		req.ContentLength = int64(len(indexData))

		// Execute request
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			return nil
		}

		// Handle retryable errors
		if resp.StatusCode >= 500 || resp.StatusCode == 429 {
			return fmt.Errorf("HTTP %d", resp.StatusCode)
		}

		// Non-retryable error - wrap with Permanent to stop retrying
		return backoff.Permanent(fmt.Errorf("HTTP %d", resp.StatusCode))
	}

	b := createBackoffConfig(maxRetries)
	err := backoff.Retry(operation, backoff.WithContext(b, ctx))
	if err != nil {
		return fmt.Errorf("index upload failed: %w", err)
	}

	return nil
}
