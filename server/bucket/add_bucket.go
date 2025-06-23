package bucket

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/draganm/datas3t2/postgresstore"
)

func (s *BucketServer) AddBucket(ctx context.Context, log *slog.Logger, req *BucketInfo) (err error) {

	log = log.With("bucket_name", req.Name)
	log.Info("Adding bucket")

	defer func() {
		if err != nil {
			log.Error("Failed to add bucket", "error", err)
		} else {
			log.Info("Bucket added")
		}
	}()

	err = req.Validate(ctx)
	if err != nil {
		return fmt.Errorf("failed to validate bucket info: %w", err)
	}

	queries := postgresstore.New(s.db)

	err = queries.AddBucket(ctx, postgresstore.AddBucketParams{
		Name:      req.Name,
		Endpoint:  req.Endpoint,
		Bucket:    req.Bucket,
		AccessKey: req.AccessKey,
		SecretKey: req.SecretKey,
		UseTls:    req.UseTLS,
	})

	if err != nil {
		return fmt.Errorf("failed to add bucket: %w", err)
	}

	return nil
}
