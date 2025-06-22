package uploaddatarange

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/draganm/datas3t2/postgresstore"
	"github.com/jackc/pgx/v5/pgtype"
)

type CompleteUploadRequest struct {
	DatarangeUploadID int64    `json:"datarange_upload_id"`
	UploadIDs         []string `json:"upload_ids,omitempty"` // Only used for multipart uploads
}

func (s *UploadDatarangeServer) CompleteDatarangeUpload(ctx context.Context, req *CompleteUploadRequest) error {
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

	// Create AWS config with custom credentials
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			uploadDetails.AccessKey,
			uploadDetails.SecretKey,
			"", // token
		)),
		config.WithRegion("us-east-1"), // default region
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
