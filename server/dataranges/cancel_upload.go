package dataranges

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/draganm/datas3t2/postgresstore"
	"github.com/jackc/pgx/v5/pgtype"
)

type CancelUploadRequest struct {
	DatarangeUploadID int64 `json:"datarange_upload_id"`
}

func (s *UploadDatarangeServer) CancelDatarangeUpload(
	ctx context.Context,
	log *slog.Logger,
	req *CancelUploadRequest,
) (err error) {
	log = log.With("datarange_upload_id", req.DatarangeUploadID)
	log.Info("Cancelling datarange upload")

	defer func() {
		if err != nil {
			log.Error("Failed to cancel datarange upload", "error", err)
		} else {
			log.Info("Datarange upload cancelled")
		}
	}()

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

	// Create a timeout context for S3 operations (separate from main context)
	s3Ctx, s3Cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer s3Cancel()

	if uploadDetails.UploadID == "DIRECT_PUT" {
		// For direct PUT uploads, schedule deletion of the data and index objects
		err = s.scheduleObjectsForDeletion(s3Ctx, queries, s3Client, uploadDetails, uploadDetails.IndexObjectKey)
		if err != nil {
			return fmt.Errorf("failed to schedule cleanup for direct PUT upload: %w", err)
		}
	} else {
		// For multipart uploads, abort the upload and schedule cleanup
		_, err = s3Client.AbortMultipartUpload(s3Ctx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(uploadDetails.Bucket),
			Key:      aws.String(uploadDetails.DataObjectKey),
			UploadId: aws.String(uploadDetails.UploadID),
		})
		if err != nil {
			return fmt.Errorf("failed to abort multipart upload: %w", err)
		}

		// Still schedule cleanup in case some parts were uploaded
		err = s.scheduleObjectsForDeletion(s3Ctx, queries, s3Client, uploadDetails, uploadDetails.IndexObjectKey)
		if err != nil {
			return fmt.Errorf("failed to schedule cleanup for multipart upload: %w", err)
		}
	}

	// Delete the upload record from the database
	err = queries.DeleteDatarangeUpload(s3Ctx, req.DatarangeUploadID)
	if err != nil {
		return fmt.Errorf("failed to delete datarange upload record: %w", err)
	}

	// Delete the datarange record from the database since the upload was cancelled
	err = queries.DeleteDatarange(s3Ctx, uploadDetails.DatarangeID)
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

	// Use the original context (not the S3 timeout context) for database operations
	originalCtx := context.Background()
	if deadline, ok := ctx.Deadline(); ok {
		// If the S3 context has a deadline, create a new context with a longer deadline for DB operations
		dbCtx, cancel := context.WithDeadline(originalCtx, deadline.Add(10*time.Second))
		defer cancel()
		originalCtx = dbCtx
	}

	err = queries.ScheduleKeyForDeletion(originalCtx, postgresstore.ScheduleKeyForDeletionParams{
		PresignedDeleteUrl: dataDeleteReq.URL,
		DeleteAfter:        deleteAfter,
	})
	if err != nil {
		return fmt.Errorf("failed to schedule data object deletion: %w", err)
	}

	err = queries.ScheduleKeyForDeletion(originalCtx, postgresstore.ScheduleKeyForDeletionParams{
		PresignedDeleteUrl: indexDeleteReq.URL,
		DeleteAfter:        deleteAfter,
	})
	if err != nil {
		return fmt.Errorf("failed to schedule index object deletion: %w", err)
	}

	return nil
}
