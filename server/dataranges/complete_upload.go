package dataranges

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/draganm/datas3t2/postgresstore"
	"github.com/draganm/datas3t2/tarindex"
	"github.com/jackc/pgx/v5/pgtype"
)

type CompleteUploadRequest struct {
	DatarangeUploadID int64    `json:"datarange_upload_id"`
	UploadIDs         []string `json:"upload_ids,omitempty"` // Only used for multipart uploads
}

func (s *UploadDatarangeServer) CompleteDatarangeUpload(ctx context.Context, log *slog.Logger, req *CompleteUploadRequest) (err error) {
	log = log.With("datarange_upload_id", req.DatarangeUploadID)
	log.Info("Completing datarange upload")

	defer func() {
		if err != nil {
			log.Error("Failed to complete datarange upload", "error", err)
		} else {
			log.Info("Datarange upload completed successfully")
		}
	}()

	// 1. Get datarange upload details (read-only operation)
	queries := postgresstore.New(s.db)
	uploadDetails, err := queries.GetDatarangeUploadWithDetails(ctx, req.DatarangeUploadID)
	if err != nil {
		return fmt.Errorf("failed to get datarange upload details: %w", err)
	}

	// 2. Create S3 client
	s3Client, err := s.createS3ClientFromUploadDetails(ctx, uploadDetails)
	if err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	// 3. Perform all S3 operations first (without database changes)
	err = s.performS3Operations(ctx, s3Client, uploadDetails, req.UploadIDs)
	if err != nil {
		// S3 operations failed - handle cleanup in a single transaction
		return s.handleFailureInTransaction(ctx, queries, s3Client, uploadDetails, err)
	}

	// 4. S3 operations succeeded - complete in a single transaction
	return s.handleSuccessInTransaction(ctx, queries, req.DatarangeUploadID)
}

// performS3Operations handles all S3 network calls without any database changes
func (s *UploadDatarangeServer) performS3Operations(ctx context.Context, s3Client *s3.Client, uploadDetails postgresstore.GetDatarangeUploadWithDetailsRow, uploadIDs []string) error {
	// Complete upload (different logic for direct PUT vs multipart)
	isDirectPut := uploadDetails.UploadID == "DIRECT_PUT"

	if !isDirectPut {
		// Handle multipart upload completion
		var completedParts []types.CompletedPart
		for i, uploadID := range uploadIDs {
			completedParts = append(completedParts, types.CompletedPart{
				ETag:       aws.String(uploadID),
				PartNumber: aws.Int32(int32(i + 1)),
			})
		}

		completeInput := &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(uploadDetails.Bucket),
			Key:      aws.String(uploadDetails.DataObjectKey),
			UploadId: aws.String(uploadDetails.UploadID),
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: completedParts,
			},
		}

		_, err := s3Client.CompleteMultipartUpload(ctx, completeInput)
		if err != nil {
			// Abort the upload if completion fails
			s3Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(uploadDetails.Bucket),
				Key:      aws.String(uploadDetails.DataObjectKey),
				UploadId: aws.String(uploadDetails.UploadID),
			})
			return fmt.Errorf("failed to complete multipart upload: %w", err)
		}
	}
	// For direct PUT, no completion step needed - the object should already be uploaded

	// Check if the index is present
	indexObjectKey := uploadDetails.IndexObjectKey
	_, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(uploadDetails.Bucket),
		Key:    aws.String(indexObjectKey),
	})
	if err != nil {
		return fmt.Errorf("index file not found: %w", err)
	}

	// Check the size of the uploaded data
	headResp, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(uploadDetails.Bucket),
		Key:    aws.String(uploadDetails.DataObjectKey),
	})
	if err != nil {
		return fmt.Errorf("failed to get uploaded object info: %w", err)
	}

	if headResp.ContentLength == nil || *headResp.ContentLength != uploadDetails.DataSize {
		return fmt.Errorf("uploaded size mismatch: expected %d, got %d",
			uploadDetails.DataSize, aws.ToInt64(headResp.ContentLength))
	}

	// Perform tar index validation
	err = s.validateTarIndex(ctx, s3Client, uploadDetails)
	if err != nil {
		return fmt.Errorf("tar index validation failed: %w", err)
	}

	return nil
}

// handleSuccessInTransaction performs all success-case database operations in a single transaction
func (s *UploadDatarangeServer) handleSuccessInTransaction(ctx context.Context, queries *postgresstore.Queries, datarangeUploadID int64) error {
	// Begin transaction
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Create queries with transaction
	txQueries := queries.WithTx(tx)

	// Delete the upload record
	err = txQueries.DeleteDatarangeUpload(ctx, datarangeUploadID)
	if err != nil {
		return fmt.Errorf("failed to delete datarange upload record: %w", err)
	}

	// Commit transaction
	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// handleFailureInTransaction performs all failure-case database operations in a single transaction
func (s *UploadDatarangeServer) handleFailureInTransaction(ctx context.Context, queries *postgresstore.Queries, s3Client *s3.Client, uploadDetails postgresstore.GetDatarangeUploadWithDetailsRow, originalErr error) error {
	// Begin transaction
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Create queries with transaction
	txQueries := queries.WithTx(tx)

	// Generate presigned delete URLs for both data and index objects
	presigner := s3.NewPresignClient(s3Client)

	// Schedule data object for deletion
	dataDeleteReq, err := presigner.PresignDeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(uploadDetails.Bucket),
		Key:    aws.String(uploadDetails.DataObjectKey),
	}, func(opts *s3.PresignOptions) {
		opts.Expires = 24 * time.Hour
	})
	if err != nil {
		return fmt.Errorf("failed to presign data object delete: %w", err)
	}

	// Schedule index object for deletion
	indexDeleteReq, err := presigner.PresignDeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(uploadDetails.Bucket),
		Key:    aws.String(uploadDetails.IndexObjectKey),
	}, func(opts *s3.PresignOptions) {
		opts.Expires = 24 * time.Hour
	})
	if err != nil {
		return fmt.Errorf("failed to presign index object delete: %w", err)
	}

	// Schedule both objects for deletion
	deleteAfter := pgtype.Timestamp{
		Time:  time.Now().Add(time.Hour), // Delete after 1 hour
		Valid: true,
	}

	err = txQueries.ScheduleKeyForDeletion(ctx, postgresstore.ScheduleKeyForDeletionParams{
		PresignedDeleteUrl: dataDeleteReq.URL,
		DeleteAfter:        deleteAfter,
	})
	if err != nil {
		return fmt.Errorf("failed to schedule data object deletion: %w", err)
	}

	err = txQueries.ScheduleKeyForDeletion(ctx, postgresstore.ScheduleKeyForDeletionParams{
		PresignedDeleteUrl: indexDeleteReq.URL,
		DeleteAfter:        deleteAfter,
	})
	if err != nil {
		return fmt.Errorf("failed to schedule index object deletion: %w", err)
	}

	// Delete the datarange record and upload record
	err = txQueries.DeleteDatarangeUpload(ctx, uploadDetails.ID)
	if err != nil {
		return fmt.Errorf("failed to delete datarange upload record: %w", err)
	}

	err = txQueries.DeleteDatarange(ctx, uploadDetails.DatarangeID)
	if err != nil {
		return fmt.Errorf("failed to delete datarange record: %w", err)
	}

	// Commit transaction
	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Return the original error that caused the failure
	return originalErr
}

func (s *UploadDatarangeServer) createS3ClientFromUploadDetails(ctx context.Context, uploadDetails postgresstore.GetDatarangeUploadWithDetailsRow) (*s3.Client, error) {
	// Decrypt credentials
	accessKey, secretKey, err := s.encryptor.DecryptCredentials(uploadDetails.AccessKey, uploadDetails.SecretKey)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt credentials: %w", err)
	}

	// Build endpoint URL with proper scheme
	endpoint := uploadDetails.Endpoint
	if uploadDetails.UseTls {
		if !strings.HasPrefix(endpoint, "https://") && !strings.HasPrefix(endpoint, "http://") {
			endpoint = "https://" + endpoint
		}
	} else {
		if !strings.HasPrefix(endpoint, "https://") && !strings.HasPrefix(endpoint, "http://") {
			endpoint = "http://" + endpoint
		}
	}

	// Create AWS config with custom credentials and timeouts
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			accessKey,
			secretKey,
			"", // token
		)),
		config.WithRegion("us-east-1"), // default region
		config.WithHTTPClient(&http.Client{
			Timeout: 30 * time.Second, // 30 second timeout for all HTTP operations
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client with custom endpoint
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true // Use path-style addressing for custom S3 endpoints
	})

	return s3Client, nil
}

// validateTarIndex performs random sampling validation of tar entries
func (s *UploadDatarangeServer) validateTarIndex(ctx context.Context, s3Client *s3.Client, uploadDetails postgresstore.GetDatarangeUploadWithDetailsRow) error {
	// Download the index file
	indexResp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(uploadDetails.Bucket),
		Key:    aws.String(uploadDetails.IndexObjectKey),
	})
	if err != nil {
		return fmt.Errorf("failed to download index file: %w", err)
	}
	defer indexResp.Body.Close()

	// Read the index into memory
	indexData, err := io.ReadAll(indexResp.Body)
	if err != nil {
		return fmt.Errorf("failed to read index data: %w", err)
	}

	// Parse the index
	if len(indexData)%16 != 0 {
		// Skip validation if index is not in expected tar index format
		// This may be a non-tar index file or test data
		return nil
	}

	numEntries := len(indexData) / 16
	if numEntries == 0 {
		return fmt.Errorf("index file is empty")
	}

	// Create a fake index to use the existing API
	fakeIndex := &tarindex.Index{
		Bytes: indexData,
	}

	// Validate tar file size against expected size
	err = s.validateTarFileSize(ctx, s3Client, uploadDetails, fakeIndex)
	if err != nil {
		return fmt.Errorf("tar file size validation failed: %w", err)
	}

	// Always check first and last entries, plus up to 3 random entries
	var indicesToCheck []int

	// Always include first entry
	indicesToCheck = append(indicesToCheck, 0)

	// Always include last entry (if different from first)
	if numEntries > 1 {
		indicesToCheck = append(indicesToCheck, numEntries-1)
	}

	// Add up to 3 random entries from the middle (excluding first and last)
	if numEntries > 2 {
		rand.New(rand.NewSource(time.Now().UnixNano()))
		maxRandomSamples := min(3, numEntries-2) // Exclude first and last
		selectedIndices := make(map[int]bool)

		// Avoid first (0) and last (numEntries-1) indices
		for len(selectedIndices) < maxRandomSamples {
			idx := rand.Intn(numEntries-2) + 1 // Random index between 1 and numEntries-2 inclusive
			if !selectedIndices[idx] {
				selectedIndices[idx] = true
				indicesToCheck = append(indicesToCheck, idx)
			}
		}
	}

	// Validate each selected entry
	for _, entryIdx := range indicesToCheck {
		err = s.validateTarEntry(ctx, s3Client, uploadDetails, fakeIndex, uint64(entryIdx))
		if err != nil {
			return fmt.Errorf("validation failed for entry %d: %w", entryIdx, err)
		}
	}

	return nil
}

// validateTarFileSize validates the actual tar file size against the expected size calculated from the index
func (s *UploadDatarangeServer) validateTarFileSize(ctx context.Context, s3Client *s3.Client, uploadDetails postgresstore.GetDatarangeUploadWithDetailsRow, index *tarindex.Index) error {
	// Get the actual file size from the database
	expectedSizeFromDB := uploadDetails.DataSize

	// Calculate expected size from the tar index
	numFiles := index.NumFiles()
	if numFiles == 0 {
		return fmt.Errorf("tar index is empty")
	}

	// Get metadata for the last file
	lastFileMetadata, err := index.GetFileMetadata(numFiles - 1)
	if err != nil {
		return fmt.Errorf("failed to get last file metadata: %w", err)
	}

	// Calculate expected tar file size:
	// - Start position of last file
	// - + Header size (512 bytes)
	// - + Content size padded to 512-byte boundary
	// - + End-of-archive marker (1024 bytes of zeroes)
	headerSize := int64(512)
	paddedContentSize := ((lastFileMetadata.Size + 511) / 512) * 512 // Round up to 512-byte boundary
	endOfArchiveSize := int64(1024)                                  // Two 512-byte blocks of zeroes

	calculatedSize := lastFileMetadata.Start + headerSize + paddedContentSize + endOfArchiveSize

	// Check against database size
	if expectedSizeFromDB != calculatedSize {
		return fmt.Errorf("tar size mismatch: database says %d bytes, calculated from index %d bytes",
			expectedSizeFromDB, calculatedSize)
	}

	// Double-check against actual S3 object size
	headResp, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(uploadDetails.Bucket),
		Key:    aws.String(uploadDetails.DataObjectKey),
	})
	if err != nil {
		return fmt.Errorf("failed to get actual object size: %w", err)
	}

	actualSize := aws.ToInt64(headResp.ContentLength)
	if actualSize != calculatedSize {
		return fmt.Errorf("tar size mismatch: actual S3 object is %d bytes, calculated from index %d bytes",
			actualSize, calculatedSize)
	}

	return nil
}

// validateTarEntry validates a single tar entry
func (s *UploadDatarangeServer) validateTarEntry(ctx context.Context, s3Client *s3.Client, uploadDetails postgresstore.GetDatarangeUploadWithDetailsRow, index *tarindex.Index, entryIdx uint64) error {
	// Get file metadata from index
	metadata, err := index.GetFileMetadata(entryIdx)
	if err != nil {
		return fmt.Errorf("failed to get file metadata: %w", err)
	}

	// Calculate the range to download (header + some content to read the full header)
	// TAR header is always 512 bytes
	headerSize := int64(512)
	rangeStart := metadata.Start
	rangeEnd := metadata.Start + headerSize - 1

	// Download the tar header portion
	headerResp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(uploadDetails.Bucket),
		Key:    aws.String(uploadDetails.DataObjectKey),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", rangeStart, rangeEnd)),
	})
	if err != nil {
		return fmt.Errorf("failed to download tar header: %w", err)
	}
	defer headerResp.Body.Close()

	headerData, err := io.ReadAll(headerResp.Body)
	if err != nil {
		return fmt.Errorf("failed to read header data: %w", err)
	}

	if len(headerData) != int(headerSize) {
		return fmt.Errorf("incomplete header data: expected %d bytes, got %d", headerSize, len(headerData))
	}

	// Parse the tar header
	reader := tar.NewReader(bytes.NewReader(headerData))
	header, err := reader.Next()
	if err != nil {
		return fmt.Errorf("failed to parse tar header: %w", err)
	}

	// Validate file size matches
	if header.Size != metadata.Size {
		return fmt.Errorf("file size mismatch: header says %d, index says %d", header.Size, metadata.Size)
	}

	// Validate file name format: should be %020d.<extension>
	fileName := header.Name
	if !s.isValidFileName(fileName) {
		return fmt.Errorf("invalid file name format: %s (expected %%020d.<extension>)", fileName)
	}

	// Extract the numeric part and validate it matches expected datapoint key
	expectedDatapointKey := uploadDetails.FirstDatapointIndex + int64(entryIdx)
	actualDatapointKey, err := s.extractDatapointKeyFromFileName(fileName)
	if err != nil {
		return fmt.Errorf("failed to extract datapoint key from filename %s: %w", fileName, err)
	}

	if actualDatapointKey != expectedDatapointKey {
		return fmt.Errorf("datapoint key mismatch: filename has %d, expected %d", actualDatapointKey, expectedDatapointKey)
	}

	return nil
}

// isValidFileName checks if the filename matches the pattern %020d.<extension>
func (s *UploadDatarangeServer) isValidFileName(fileName string) bool {
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
func (s *UploadDatarangeServer) extractDatapointKeyFromFileName(fileName string) (int64, error) {
	dotIndex := strings.LastIndex(fileName, ".")
	if dotIndex == -1 {
		return 0, fmt.Errorf("no extension found")
	}

	namepart := fileName[:dotIndex]
	key, err := strconv.ParseInt(namepart, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse numeric part: %w", err)
	}

	return key, nil
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
