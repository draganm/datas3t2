package bucket

import (
	"context"
	"fmt"
	"regexp"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type BucketInfo struct {
	Name      string `json:"name"`
	Endpoint  string `json:"endpoint"`
	Bucket    string `json:"bucket"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	UseTLS    bool   `json:"use_tls"`
}

// BucketListInfo represents bucket information for listing (without sensitive credentials)
type BucketListInfo struct {
	Name     string `json:"name"`
	Endpoint string `json:"endpoint"`
	Bucket   string `json:"bucket"`
	UseTLS   bool   `json:"use_tls"`
}

var bucketNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

type ValidationError error

func (r *BucketInfo) Validate(ctx context.Context) error {
	if !bucketNameRegex.MatchString(r.Name) {
		return ValidationError(fmt.Errorf("invalid bucket name: %s", r.Name))
	}

	if r.Endpoint == "" {
		return ValidationError(fmt.Errorf("endpoint is required"))
	}

	if r.Bucket == "" {
		return ValidationError(fmt.Errorf("bucket is required"))
	}

	err := r.testConnection(ctx)
	if err != nil {
		return ValidationError(fmt.Errorf("failed to test connection: %w", err))
	}

	return nil
}

func (r *BucketInfo) testConnection(ctx context.Context) error {

	// Build endpoint URL with proper scheme based on UseTls
	endpoint := r.Endpoint
	if r.UseTLS {
		if !regexp.MustCompile(`^https?://`).MatchString(endpoint) {
			endpoint = "https://" + endpoint
		}
	} else {
		if !regexp.MustCompile(`^https?://`).MatchString(endpoint) {
			endpoint = "http://" + endpoint
		}
	}

	// Create AWS config with custom credentials
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			r.AccessKey,
			r.SecretKey,
			"", // token
		)),
		config.WithRegion("us-east-1"), // default region, can be overridden by endpoint
	)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client with custom endpoint
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true // Use path-style addressing for custom S3 endpoints
	})

	// Test connection by listing objects (with max 1 object to minimize data transfer)
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(r.Bucket),
		MaxKeys: aws.Int32(1),
	}

	_, err = s3Client.ListObjectsV2(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to connect to S3 bucket %s at %s: %w", r.Bucket, endpoint, err)
	}

	return nil
}
