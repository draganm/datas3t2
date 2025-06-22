package uploaddatarange

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/draganm/datas3t2/postgresstore"
	"github.com/jackc/pgx/v5/pgtype"
)

type CancelUploadRequest struct {
	DatarangeUploadID int64 `json:"datarange_upload_id"`
}

func (s *UploadDatarangeServer) CancelDatarangeUpload(ctx context.Context, req *CancelUploadRequest) error {
	// Start a transaction for atomic operations
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	queries := postgresstore.New(tx)
	uploadDetails, err := queries.GetDatarangeUploadWithDetails(ctx, req.DatarangeUploadID)
	if err != nil {
		return fmt.Errorf("failed to get datarange upload details: %w", err)
	}

	s3Client, err := s.createS3ClientFromUploadDetails(ctx, uploadDetails)
	if err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	if uploadDetails.UploadID == "DIRECT_PUT" {
		// For direct PUT uploads, schedule deletion of the data and index objects
		err = s.scheduleObjectsForDeletion(ctx, queries, s3Client, uploadDetails, uploadDetails.IndexObjectKey)
		if err != nil {
			return fmt.Errorf("failed to schedule cleanup for direct PUT upload: %w", err)
		}
	} else {
		// For multipart uploads, abort the upload and schedule cleanup
		_, err = s3Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(uploadDetails.Bucket),
			Key:      aws.String(uploadDetails.DataObjectKey),
			UploadId: aws.String(uploadDetails.UploadID),
		})
		if err != nil {
			return fmt.Errorf("failed to abort multipart upload: %w", err)
		}

		// Still schedule cleanup in case some parts were uploaded
		err = s.scheduleObjectsForDeletion(ctx, queries, s3Client, uploadDetails, uploadDetails.IndexObjectKey)
		if err != nil {
			return fmt.Errorf("failed to schedule cleanup for multipart upload: %w", err)
		}
	}

	// Delete the upload record from the database
	err = queries.DeleteDatarangeUpload(ctx, req.DatarangeUploadID)
	if err != nil {
		return fmt.Errorf("failed to delete datarange upload record: %w", err)
	}

	// Delete the datarange record from the database since the upload was cancelled
	err = queries.DeleteDatarange(ctx, uploadDetails.DatarangeID)
	if err != nil {
		return fmt.Errorf("failed to delete datarange record: %w", err)
	}

	// Commit transaction
	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (s *UploadDatarangeServer) scheduleObjectsForDeletion(ctx context.Context, queries *postgresstore.Queries, s3Client *s3.Client, uploadDetails postgresstore.GetDatarangeUploadWithDetailsRow, indexObjectKey string) error {
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
		Key:    aws.String(indexObjectKey),
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

	err = queries.ScheduleKeyForDeletion(ctx, postgresstore.ScheduleKeyForDeletionParams{
		PresignedDeleteUrl: dataDeleteReq.URL,
		DeleteAfter:        deleteAfter,
	})
	if err != nil {
		return fmt.Errorf("failed to schedule data object deletion: %w", err)
	}

	err = queries.ScheduleKeyForDeletion(ctx, postgresstore.ScheduleKeyForDeletionParams{
		PresignedDeleteUrl: indexDeleteReq.URL,
		DeleteAfter:        deleteAfter,
	})
	if err != nil {
		return fmt.Errorf("failed to schedule index object deletion: %w", err)
	}

	return nil
}
