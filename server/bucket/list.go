package bucket

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/draganm/datas3t2/postgresstore"
)

func (s *BucketServer) ListBuckets(ctx context.Context, log *slog.Logger) ([]*BucketListInfo, error) {
	log.Info("Listing all buckets")

	queries := postgresstore.New(s.db)

	buckets, err := queries.ListAllBuckets(ctx)
	if err != nil {
		log.Error("Failed to list buckets from database", "error", err)
		return nil, fmt.Errorf("failed to list buckets: %w", err)
	}

	result := make([]*BucketListInfo, len(buckets))
	for i, bucket := range buckets {
		result[i] = &BucketListInfo{
			Name:     bucket.Name,
			Endpoint: bucket.Endpoint,
			Bucket:   bucket.Bucket,
			UseTLS:   bucket.UseTls,
		}
	}

	log.Info("Successfully listed buckets", "count", len(result))
	return result, nil
}
