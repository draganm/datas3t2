package adddatas3t

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"

	"github.com/draganm/datas3t2/postgresstore"
)

type AddDatas3tRequest struct {
	Name   string `json:"name"`
	Bucket string `json:"bucket"`
}

var datas3tNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

type ValidationError error

func (r *AddDatas3tRequest) Validate(ctx context.Context) error {
	if r.Bucket == "" {
		return ValidationError(fmt.Errorf("bucket is required"))
	}
	if r.Name == "" {
		return ValidationError(fmt.Errorf("name is required"))
	}

	if !datas3tNameRegex.MatchString(r.Name) {
		return ValidationError(fmt.Errorf("name must be a valid datas3t name"))
	}

	return nil
}

func (s *AddDatas3tServer) AddDatas3t(ctx context.Context, log *slog.Logger, req *AddDatas3tRequest) (err error) {

	log = log.With("bucket", req.Bucket, "name", req.Name)
	log.Info("Adding datas3t")

	defer func() {
		if err != nil {
			log.Error("Failed to add datas3t", "error", err)
		} else {
			log.Info("Datas3t added")
		}
	}()

	err = req.Validate(ctx)
	if err != nil {
		return err
	}

	queries := postgresstore.New(s.db)

	// Check if bucket exists
	bucketExists, err := queries.BucketExists(ctx, req.Bucket)
	if err != nil {
		return fmt.Errorf("failed to check bucket existence: %w", err)
	}

	if !bucketExists {
		return fmt.Errorf("bucket '%s' does not exist", req.Bucket)
	}

	err = queries.AddDatas3t(ctx, postgresstore.AddDatas3tParams{
		DatasetName: req.Name,
		BucketName:  req.Bucket,
	})

	if err != nil {
		return fmt.Errorf("failed to add datas3t: %w", err)
	}

	return nil
}
