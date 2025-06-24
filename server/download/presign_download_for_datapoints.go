package download

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/draganm/datas3t2/postgresstore"
	"github.com/draganm/datas3t2/tarindex"
	"github.com/draganm/datas3t2/tarindex/diskcache"
)

type PreSignDownloadForDatapointsRequest struct {
	Datas3tName    string `json:"datas3t_name"`
	FirstDatapoint uint64 `json:"first_datapoint"`
	LastDatapoint  uint64 `json:"last_datapoint"`
}

type DownloadSegment struct {
	PresignedURL string `json:"presigned_url"`
	Range        string `json:"range"`
}

type PreSignDownloadForDatapointsResponse struct {
	DownloadSegments []DownloadSegment `json:"download_segments"`
}

func (s *DownloadServer) PreSignDownloadForDatapoints(ctx context.Context, request PreSignDownloadForDatapointsRequest) (PreSignDownloadForDatapointsResponse, error) {
	// 1. Validate request
	err := request.Validate()
	if err != nil {
		return PreSignDownloadForDatapointsResponse{}, err
	}

	// 2. Get the dataranges from the database that contain the datapoints
	queries := postgresstore.New(s.pgxPool)
	dataranges, err := queries.GetDatarangesForDatapoints(ctx, postgresstore.GetDatarangesForDatapointsParams{
		Name:            request.Datas3tName,
		MinDatapointKey: int64(request.LastDatapoint),  // datarange starts before or at our last datapoint
		MaxDatapointKey: int64(request.FirstDatapoint), // datarange ends after or at our first datapoint
	})
	if err != nil {
		return PreSignDownloadForDatapointsResponse{}, fmt.Errorf("failed to get dataranges: %w", err)
	}

	if len(dataranges) == 0 {
		return PreSignDownloadForDatapointsResponse{}, fmt.Errorf("no dataranges found for datapoints %d-%d in dataset %s", request.FirstDatapoint, request.LastDatapoint, request.Datas3tName)
	}

	var downloadSegments []DownloadSegment

	// 3. For each datarange, get the index from the disk cache and create download segments
	for _, datarange := range dataranges {
		// Create S3 client for this datarange
		s3Client, err := s.createS3Client(ctx, datarange)
		if err != nil {
			return PreSignDownloadForDatapointsResponse{}, fmt.Errorf("failed to create S3 client: %w", err)
		}

		// Create disk cache key for this datarange
		cacheKey := diskcache.DatarangeKey{
			Datas3tName:        datarange.DatasetName,
			FirstIndex:         datarange.MinDatapointKey,
			NumberOfDatapoints: datarange.MaxDatapointKey - datarange.MinDatapointKey + 1,
			TotalSizeBytes:     datarange.SizeBytes,
		}

		// Get the tar index from disk cache
		err = s.diskCache.OnIndex(cacheKey, func(index *tarindex.Index) error {
			// Create download segments for the files we need
			segments, err := s.createDownloadSegments(ctx, s3Client, datarange, index, request.FirstDatapoint, request.LastDatapoint)
			if err != nil {
				return fmt.Errorf("failed to create download segments: %w", err)
			}
			downloadSegments = append(downloadSegments, segments...)
			return nil
		}, func() ([]byte, error) {
			// Index generator: download the index from S3 if not cached
			return s.downloadIndexFromS3(ctx, s3Client, datarange.Bucket, datarange.IndexObjectKey)
		})

		if err != nil {
			return PreSignDownloadForDatapointsResponse{}, fmt.Errorf("failed to get index for datarange %d: %w", datarange.ID, err)
		}
	}

	return PreSignDownloadForDatapointsResponse{
		DownloadSegments: downloadSegments,
	}, nil
}

func (r *PreSignDownloadForDatapointsRequest) Validate() error {
	if r.Datas3tName == "" {
		return fmt.Errorf("datas3t_name is required")
	}

	if r.FirstDatapoint > r.LastDatapoint {
		return fmt.Errorf("first_datapoint (%d) cannot be greater than last_datapoint (%d)", r.FirstDatapoint, r.LastDatapoint)
	}

	return nil
}

func (s *DownloadServer) createS3Client(ctx context.Context, datarange postgresstore.GetDatarangesForDatapointsRow) (*s3.Client, error) {
	// Decrypt credentials
	accessKey, secretKey, err := s.encryptor.DecryptCredentials(datarange.AccessKey, datarange.SecretKey)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt credentials: %w", err)
	}

	// Build endpoint URL with proper scheme based on UseTls
	endpoint := datarange.Endpoint
	if datarange.UseTls {
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
			accessKey,
			secretKey,
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

func (s *DownloadServer) downloadIndexFromS3(ctx context.Context, s3Client *s3.Client, bucket, indexObjectKey string) ([]byte, error) {
	resp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(indexObjectKey),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get index object from S3: %w", err)
	}
	defer resp.Body.Close()

	// Read the entire index file using io.ReadAll
	indexData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read index data: %w", err)
	}

	return indexData, nil
}

func (s *DownloadServer) createDownloadSegments(ctx context.Context, s3Client *s3.Client, datarange postgresstore.GetDatarangesForDatapointsRow, index *tarindex.Index, firstDatapoint, lastDatapoint uint64) ([]DownloadSegment, error) {
	var segments []DownloadSegment

	// Calculate the range of files we need to download
	datarangeFirst := uint64(datarange.MinDatapointKey)
	datarangeLast := uint64(datarange.MaxDatapointKey)

	// Determine the actual range we need from this datarange
	actualFirst := max(firstDatapoint, datarangeFirst)
	actualLast := min(lastDatapoint, datarangeLast)

	if actualFirst > actualLast {
		// No overlap with this datarange
		return segments, nil
	}

	// Calculate file indices within the datarange
	firstFileIndex := actualFirst - datarangeFirst
	lastFileIndex := actualLast - datarangeFirst

	if lastFileIndex >= index.NumFiles() {
		return nil, fmt.Errorf("file index %d exceeds number of files in index (%d)", lastFileIndex, index.NumFiles())
	}

	// Get metadata for the first and last files
	firstFileMetadata, err := index.GetFileMetadata(firstFileIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata for first file (index %d): %w", firstFileIndex, err)
	}

	lastFileMetadata, err := index.GetFileMetadata(lastFileIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata for last file (index %d): %w", lastFileIndex, err)
	}

	// Calculate the byte range we need
	startByte := firstFileMetadata.Start
	endByte := lastFileMetadata.Start + int64(lastFileMetadata.HeaderBlocks)*512 + lastFileMetadata.Size - 1

	// Create presigned URL for the data object with byte range
	presigner := s3.NewPresignClient(s3Client)
	req, err := presigner.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(datarange.Bucket),
		Key:    aws.String(datarange.DataObjectKey),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", startByte, endByte)),
	}, func(opts *s3.PresignOptions) {
		opts.Expires = 24 * time.Hour // URL expires in 24 hours
	})
	if err != nil {
		return nil, fmt.Errorf("failed to presign get object: %w", err)
	}

	segments = append(segments, DownloadSegment{
		PresignedURL: req.URL,
		Range:        fmt.Sprintf("bytes=%d-%d", startByte, endByte),
	})

	return segments, nil
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
