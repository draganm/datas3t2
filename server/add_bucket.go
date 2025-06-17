package server

import (
	"context"
	"fmt"

	"github.com/draganm/datas3t2/postgresstore"
)

func (s *Server) AddBucket(ctx context.Context, req *BucketInfo) error {

	queries := postgresstore.New(s.db)

	err := queries.AddBucket(ctx, postgresstore.AddBucketParams{
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
