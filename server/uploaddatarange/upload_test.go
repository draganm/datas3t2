package uploaddatarange_test

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"

	"archive/tar"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/draganm/datas3t2/postgresstore"
	"github.com/draganm/datas3t2/server/addbucket"
	"github.com/draganm/datas3t2/server/adddatas3t"
	"github.com/draganm/datas3t2/server/uploaddatarange"
	"github.com/draganm/datas3t2/tarindex"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
	miniogo "github.com/minio/minio-go/v7"
	miniocreds "github.com/minio/minio-go/v7/pkg/credentials"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/minio"
	tc_postgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Helper function to perform HTTP PUT requests
func httpPut(url string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest("PUT", url, body)
	if err != nil {
		return nil, err
	}

	client := &http.Client{}
	return client.Do(req)
}

// createProperTarWithIndex creates a proper TAR archive with correctly named files and returns both the tar data and index
func createProperTarWithIndex(numFiles int, startIndex int64) ([]byte, []byte) {
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)

	// Create files with proper %020d.<extension> naming
	for i := 0; i < numFiles; i++ {
		filename := fmt.Sprintf("%020d.txt", startIndex+int64(i))
		content := fmt.Sprintf("Content of file %d", startIndex+int64(i))

		header := &tar.Header{
			Name: filename,
			Size: int64(len(content)),
			Mode: 0644,
		}

		err := tw.WriteHeader(header)
		if err != nil {
			panic(fmt.Sprintf("Failed to write tar header: %v", err))
		}

		_, err = tw.Write([]byte(content))
		if err != nil {
			panic(fmt.Sprintf("Failed to write tar content: %v", err))
		}
	}

	err := tw.Close()
	if err != nil {
		panic(fmt.Sprintf("Failed to close tar writer: %v", err))
	}

	// Create the tar index
	tarReader := bytes.NewReader(tarBuf.Bytes())
	indexData, err := tarindex.IndexTar(tarReader)
	if err != nil {
		panic(fmt.Sprintf("Failed to create tar index: %v", err))
	}

	return tarBuf.Bytes(), indexData
}

// createTarWithInvalidNames creates a TAR archive with incorrectly named files for testing validation failures
func createTarWithInvalidNames() ([]byte, []byte) {
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)

	// Create files with invalid naming (not following %020d.<extension> format)
	invalidFiles := []struct {
		name    string
		content string
	}{
		{"invalid_name.txt", "Content 1"},
		{"123.txt", "Content 2"},                       // Too short
		{"file_00000000000000000003.txt", "Content 3"}, // Wrong format
	}

	for _, file := range invalidFiles {
		header := &tar.Header{
			Name: file.name,
			Size: int64(len(file.content)),
			Mode: 0644,
		}

		err := tw.WriteHeader(header)
		if err != nil {
			panic(fmt.Sprintf("Failed to write tar header: %v", err))
		}

		_, err = tw.Write([]byte(file.content))
		if err != nil {
			panic(fmt.Sprintf("Failed to write tar content: %v", err))
		}
	}

	err := tw.Close()
	if err != nil {
		panic(fmt.Sprintf("Failed to close tar writer: %v", err))
	}

	// Create the tar index
	tarReader := bytes.NewReader(tarBuf.Bytes())
	indexData, err := tarindex.IndexTar(tarReader)
	if err != nil {
		panic(fmt.Sprintf("Failed to create tar index: %v", err))
	}

	return tarBuf.Bytes(), indexData
}

var _ = Describe("UploadDatarange", func() {
	var (
		pgContainer          *tc_postgres.PostgresContainer
		minioContainer       *minio.MinioContainer
		db                   *pgxpool.Pool
		queries              *postgresstore.Queries
		uploadSrv            *uploaddatarange.UploadDatarangeServer
		bucketSrv            *addbucket.AddBucketServer
		datasetSrv           *adddatas3t.AddDatas3tServer
		minioEndpoint        string
		minioHost            string
		minioAccessKey       string
		minioSecretKey       string
		testBucketName       string
		testBucketConfigName string
		testDatasetName      string
		s3Client             *s3.Client
		logger               *slog.Logger
	)

	BeforeEach(func(ctx SpecContext) {

		// Create logger that writes to GinkgoWriter for test visibility
		logger = slog.New(slog.NewTextHandler(GinkgoWriter, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))

		var err error

		// Start PostgreSQL container
		pgContainer, err = tc_postgres.Run(ctx,
			"postgres:16-alpine",
			tc_postgres.WithDatabase("testdb"),
			tc_postgres.WithUsername("testuser"),
			tc_postgres.WithPassword("testpass"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).
					WithStartupTimeout(30*time.Second),
			),
			testcontainers.WithLogger(log.New(GinkgoWriter, "", 0)),
		)
		Expect(err).NotTo(HaveOccurred())

		// Get PostgreSQL connection string
		connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
		Expect(err).NotTo(HaveOccurred())

		// Connect to PostgreSQL
		db, err = pgxpool.New(ctx, connStr)
		Expect(err).NotTo(HaveOccurred())

		// Initialize queries instance
		queries = postgresstore.New(db)

		// Run migrations
		connStrForMigration, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
		Expect(err).NotTo(HaveOccurred())

		m, err := migrate.New(
			"file://../../postgresstore/migrations",
			connStrForMigration)
		Expect(err).NotTo(HaveOccurred())

		err = m.Up()
		if err != nil && err != migrate.ErrNoChange {
			Expect(err).NotTo(HaveOccurred())
		}

		// Start MinIO container
		minioContainer, err = minio.Run(
			ctx,
			"minio/minio:RELEASE.2024-01-16T16-07-38Z",
			minio.WithUsername("minioadmin"),
			minio.WithPassword("minioadmin"),
			testcontainers.WithLogger(log.New(GinkgoWriter, "", 0)),
		)
		Expect(err).NotTo(HaveOccurred())

		// Get MinIO connection details
		minioEndpoint, err = minioContainer.ConnectionString(ctx)
		Expect(err).NotTo(HaveOccurred())

		// Extract host:port from the full URL for both minioHost and storage endpoint
		minioHost = strings.TrimPrefix(minioEndpoint, "http://")
		minioHost = strings.TrimPrefix(minioHost, "https://")

		// Store endpoint without protocol (consistent with bucket_info.go approach)
		minioEndpoint = minioHost

		minioAccessKey = "minioadmin"
		minioSecretKey = "minioadmin"
		testBucketName = "test-bucket"
		testBucketConfigName = "test-bucket-config"
		testDatasetName = "test-dataset"

		// Create test bucket in MinIO
		minioClient, err := miniogo.New(minioHost, &miniogo.Options{
			Creds:  miniocreds.NewStaticV4(minioAccessKey, minioSecretKey, ""),
			Secure: false,
		})
		Expect(err).NotTo(HaveOccurred())

		err = minioClient.MakeBucket(ctx, testBucketName, miniogo.MakeBucketOptions{})
		Expect(err).NotTo(HaveOccurred())

		// Create S3 client for test operations
		cfg, err := config.LoadDefaultConfig(ctx,
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				minioAccessKey,
				minioSecretKey,
				"",
			)),
			config.WithRegion("us-east-1"),
		)
		Expect(err).NotTo(HaveOccurred())

		// Build endpoint URL with proper scheme (consistent with bucket_info.go approach)
		s3Endpoint := minioEndpoint
		if !regexp.MustCompile(`^https?://`).MatchString(s3Endpoint) {
			s3Endpoint = "http://" + s3Endpoint
		}

		s3Client = s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(s3Endpoint)
			o.UsePathStyle = true
		})

		// Create server instances
		uploadSrv = uploaddatarange.NewServer(db)
		bucketSrv = addbucket.NewServer(db)
		datasetSrv = adddatas3t.NewServer(db)

		// Add test bucket configuration
		bucketInfo := &addbucket.BucketInfo{
			Name:      testBucketConfigName,
			Endpoint:  minioEndpoint,
			Bucket:    testBucketName,
			AccessKey: minioAccessKey,
			SecretKey: minioSecretKey,
			UseTLS:    false,
		}

		err = bucketSrv.AddBucket(ctx, logger, bucketInfo)
		Expect(err).NotTo(HaveOccurred())

		// Add test dataset
		datasetReq := &adddatas3t.AddDatas3tRequest{
			Bucket: testBucketConfigName,
			Name:   testDatasetName,
		}

		err = datasetSrv.AddDatas3t(ctx, logger, datasetReq)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func(ctx SpecContext) {
		if db != nil {
			db.Close()
		}
		if pgContainer != nil {
			err := pgContainer.Terminate(ctx)
			Expect(err).NotTo(HaveOccurred())
		}
		if minioContainer != nil {
			err := minioContainer.Terminate(ctx)
			Expect(err).NotTo(HaveOccurred())
		}
	})

	Context("StartDatarangeUpload", func() {
		Context("when starting a valid small upload (direct PUT)", func() {
			It("should successfully create upload with direct PUT URLs", func(ctx SpecContext) {
				req := &uploaddatarange.UploadDatarangeRequest{
					Datas3tName:         testDatasetName,
					DataSize:            1024, // Small size < 5MB
					NumberOfDatapoints:  10,
					FirstDatapointIndex: 0,
				}

				resp, err := uploadSrv.StartDatarangeUpload(ctx, logger, req)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp).NotTo(BeNil())
				Expect(resp.UseDirectPut).To(BeTrue())
				Expect(resp.PresignedDataPutURL).NotTo(BeEmpty())
				Expect(resp.PresignedIndexPutURL).NotTo(BeEmpty())
				Expect(resp.PresignedMultipartUploadPutURLs).To(BeEmpty())
				Expect(resp.DatarangeID).To(BeNumerically(">", 0))
				Expect(resp.FirstDatapointIndex).To(Equal(uint64(0)))

				// Verify database state
				dataranges, err := queries.GetAllDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())

				var datarangeCount int
				for _, datarange := range dataranges {
					datarangeCount++
					Expect(datarange.MinDatapointKey).To(Equal(int64(0)))
					Expect(datarange.MaxDatapointKey).To(Equal(int64(9)))
					Expect(datarange.SizeBytes).To(Equal(int64(1024)))
				}
				Expect(datarangeCount).To(Equal(1))

				// Verify upload record
				uploads, err := queries.GetAllDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())

				var uploadCount int
				for _, upload := range uploads {
					uploadCount++
					Expect(upload.UploadID).To(Equal("DIRECT_PUT"))
					Expect(upload.FirstDatapointIndex).To(Equal(int64(0)))
					Expect(upload.NumberOfDatapoints).To(Equal(int64(10)))
					Expect(upload.DataSize).To(Equal(int64(1024)))
				}
				Expect(uploadCount).To(Equal(1))
			})
		})

		Context("when starting a valid large upload (multipart)", func() {
			It("should successfully create upload with multipart URLs", func(ctx SpecContext) {
				req := &uploaddatarange.UploadDatarangeRequest{
					Datas3tName:         testDatasetName,
					DataSize:            10 * 1024 * 1024, // 10MB > 5MB threshold
					NumberOfDatapoints:  1000,
					FirstDatapointIndex: 100,
				}

				resp, err := uploadSrv.StartDatarangeUpload(ctx, logger, req)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp).NotTo(BeNil())
				Expect(resp.UseDirectPut).To(BeFalse())
				Expect(resp.PresignedDataPutURL).To(BeEmpty())
				Expect(resp.PresignedIndexPutURL).NotTo(BeEmpty())
				Expect(resp.PresignedMultipartUploadPutURLs).NotTo(BeEmpty())
				Expect(len(resp.PresignedMultipartUploadPutURLs)).To(Equal(2)) // 10MB / 5MB = 2 parts
				Expect(resp.DatarangeID).To(BeNumerically(">", 0))
				Expect(resp.FirstDatapointIndex).To(Equal(uint64(100)))

				// Verify database state
				dataranges, err := queries.GetDatarangeFields(ctx)
				Expect(err).NotTo(HaveOccurred())

				Expect(len(dataranges)).To(Equal(1))
				datarange := dataranges[0]
				Expect(datarange.MinDatapointKey).To(Equal(int64(100)))
				Expect(datarange.MaxDatapointKey).To(Equal(int64(1099))) // 100 + 1000 - 1
				Expect(datarange.SizeBytes).To(Equal(int64(10 * 1024 * 1024)))

				// Verify upload record
				uploadIDs, err := queries.GetDatarangeUploadIDs(ctx)
				Expect(err).NotTo(HaveOccurred())

				Expect(len(uploadIDs)).To(Equal(1))
				uploadID := uploadIDs[0]
				Expect(uploadID).NotTo(Equal("DIRECT_PUT"))
				Expect(uploadID).NotTo(BeEmpty())
			})
		})

		Context("when validation fails", func() {
			It("should reject empty dataset name", func(ctx SpecContext) {
				req := &uploaddatarange.UploadDatarangeRequest{
					Datas3tName:         "",
					DataSize:            1024,
					NumberOfDatapoints:  10,
					FirstDatapointIndex: 0,
				}

				_, err := uploadSrv.StartDatarangeUpload(ctx, logger, req)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("datas3t_name is required"))

				// Verify no database changes
				datarangeCount, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(0)))
			})

			It("should reject zero data size", func(ctx SpecContext) {
				req := &uploaddatarange.UploadDatarangeRequest{
					Datas3tName:         testDatasetName,
					DataSize:            0,
					NumberOfDatapoints:  10,
					FirstDatapointIndex: 0,
				}

				_, err := uploadSrv.StartDatarangeUpload(ctx, logger, req)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("data_size must be greater than 0"))

				// Verify no database changes
				datarangeCount, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(0)))
			})

			It("should reject zero number of datapoints", func(ctx SpecContext) {
				req := &uploaddatarange.UploadDatarangeRequest{
					Datas3tName:         testDatasetName,
					DataSize:            1024,
					NumberOfDatapoints:  0,
					FirstDatapointIndex: 0,
				}

				_, err := uploadSrv.StartDatarangeUpload(ctx, logger, req)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("number_of_datapoints must be greater than 0"))

				// Verify no database changes
				datarangeCount, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(0)))
			})

			It("should reject non-existent dataset", func(ctx SpecContext) {
				req := &uploaddatarange.UploadDatarangeRequest{
					Datas3tName:         "non-existent-dataset",
					DataSize:            1024,
					NumberOfDatapoints:  10,
					FirstDatapointIndex: 0,
				}

				_, err := uploadSrv.StartDatarangeUpload(ctx, logger, req)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("failed to find dataset 'non-existent-dataset'"))

				// Verify no database changes
				datarangeCount, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(0)))
			})
		})

		Context("when handling overlapping dataranges", func() {
			BeforeEach(func(ctx SpecContext) {
				// Create an existing datarange from 0-99
				req := &uploaddatarange.UploadDatarangeRequest{
					Datas3tName:         testDatasetName,
					DataSize:            1024,
					NumberOfDatapoints:  100,
					FirstDatapointIndex: 0,
				}

				_, err := uploadSrv.StartDatarangeUpload(ctx, logger, req)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should reject overlapping ranges", func(ctx SpecContext) {
				// Try to create overlapping range 50-149
				req := &uploaddatarange.UploadDatarangeRequest{
					Datas3tName:         testDatasetName,
					DataSize:            1024,
					NumberOfDatapoints:  100,
					FirstDatapointIndex: 50,
				}

				_, err := uploadSrv.StartDatarangeUpload(ctx, logger, req)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("datarange overlaps with existing dataranges"))

				// Verify only one datarange exists
				datarangeCount, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(1)))
			})

			It("should allow adjacent ranges", func(ctx SpecContext) {
				// Create adjacent range 100-199 (no overlap)
				req := &uploaddatarange.UploadDatarangeRequest{
					Datas3tName:         testDatasetName,
					DataSize:            1024,
					NumberOfDatapoints:  100,
					FirstDatapointIndex: 100,
				}

				_, err := uploadSrv.StartDatarangeUpload(ctx, logger, req)
				Expect(err).NotTo(HaveOccurred())

				// Verify two dataranges exist
				datarangeCount, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(2)))
			})
		})
	})

	Context("CompleteUpload", func() {
		var uploadResp *uploaddatarange.UploadDatarangeResponse
		var testData []byte
		var testIndex []byte

		BeforeEach(func(ctx SpecContext) {
			// Start an upload
			req := &uploaddatarange.UploadDatarangeRequest{
				Datas3tName:         testDatasetName,
				DataSize:            1024,
				NumberOfDatapoints:  10,
				FirstDatapointIndex: 0,
			}

			var err error
			uploadResp, err = uploadSrv.StartDatarangeUpload(ctx, logger, req)
			Expect(err).NotTo(HaveOccurred())

			// Prepare test data
			testData = make([]byte, 1024)
			for i := range testData {
				testData[i] = byte(i % 256)
			}

			testIndex = []byte("test index data")
		})

		Context("when completing a successful direct PUT upload", func() {
			It("should complete successfully with both files uploaded", func(ctx SpecContext) {
				// Upload data file
				dataResp, err := httpPut(uploadResp.PresignedDataPutURL, bytes.NewReader(testData))
				Expect(err).NotTo(HaveOccurred())
				Expect(dataResp.StatusCode).To(Equal(http.StatusOK))
				dataResp.Body.Close()

				// Upload index file
				indexResp, err := httpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(testIndex))
				Expect(err).NotTo(HaveOccurred())
				Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
				indexResp.Body.Close()

				// Complete the upload
				completeReq := &uploaddatarange.CompleteUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err = uploadSrv.CompleteDatarangeUpload(ctx, logger, completeReq)
				Expect(err).NotTo(HaveOccurred())

				// Verify upload record was deleted
				uploadCount, err := queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(int64(0)))

				// Verify datarange record still exists
				datarangeCount, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(1)))
			})
		})

		Context("when index file is missing", func() {
			It("should fail and schedule cleanup", func(ctx SpecContext) {
				// Upload only data file (no index)
				dataResp, err := httpPut(uploadResp.PresignedDataPutURL, bytes.NewReader(testData))
				Expect(err).NotTo(HaveOccurred())
				Expect(dataResp.StatusCode).To(Equal(http.StatusOK))
				dataResp.Body.Close()

				// Complete the upload (should fail)
				completeReq := &uploaddatarange.CompleteUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err = uploadSrv.CompleteDatarangeUpload(ctx, logger, completeReq)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("index file not found"))

				// Verify cleanup happened - both upload and datarange records should be deleted
				uploadCount, err := queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(int64(0)))

				datarangeCount, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(0)))

				// Verify cleanup tasks were scheduled
				cleanupTasks, err := queries.CountKeysToDelete(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(cleanupTasks).To(Equal(int64(2))) // Both data and index objects scheduled for deletion
			})
		})

		Context("when data file is missing", func() {
			It("should fail and schedule cleanup", func(ctx SpecContext) {
				// Upload only index file (no data)
				indexResp, err := httpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(testIndex))
				Expect(err).NotTo(HaveOccurred())
				Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
				indexResp.Body.Close()

				// Complete the upload (should fail)
				completeReq := &uploaddatarange.CompleteUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err = uploadSrv.CompleteDatarangeUpload(ctx, logger, completeReq)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("failed to get uploaded object info"))

				// Verify cleanup happened
				uploadCount, err := queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(int64(0)))

				// Verify cleanup tasks were scheduled
				cleanupTasks, err := queries.CountKeysToDelete(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(cleanupTasks).To(Equal(int64(2)))
			})
		})

		Context("when data size is wrong", func() {
			It("should fail and schedule cleanup", func(ctx SpecContext) {
				// Upload wrong size data
				wrongSizeData := make([]byte, 512) // Expected 1024, uploading 512
				for i := range wrongSizeData {
					wrongSizeData[i] = byte(i % 256)
				}

				dataResp, err := httpPut(uploadResp.PresignedDataPutURL, bytes.NewReader(wrongSizeData))
				Expect(err).NotTo(HaveOccurred())
				Expect(dataResp.StatusCode).To(Equal(http.StatusOK))
				dataResp.Body.Close()

				// Upload index file
				indexResp, err := httpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(testIndex))
				Expect(err).NotTo(HaveOccurred())
				Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
				indexResp.Body.Close()

				// Complete the upload (should fail)
				completeReq := &uploaddatarange.CompleteUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err = uploadSrv.CompleteDatarangeUpload(ctx, logger, completeReq)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("uploaded size mismatch"))
				Expect(err.Error()).To(ContainSubstring("expected 1024, got 512"))

				// Verify cleanup happened
				uploadCount, err := queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(int64(0)))

				// Verify cleanup tasks were scheduled
				cleanupTasks, err := queries.CountKeysToDelete(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(cleanupTasks).To(Equal(int64(2)))
			})
		})
	})

	Context("Multipart Upload Complete", func() {
		var uploadResp *uploaddatarange.UploadDatarangeResponse
		var testData []byte
		var testIndex []byte

		BeforeEach(func(ctx SpecContext) {
			// Start a large upload that requires multipart
			req := &uploaddatarange.UploadDatarangeRequest{
				Datas3tName:         testDatasetName,
				DataSize:            10 * 1024 * 1024, // 10MB
				NumberOfDatapoints:  1000,
				FirstDatapointIndex: 0,
			}

			var err error
			uploadResp, err = uploadSrv.StartDatarangeUpload(ctx, logger, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(uploadResp.UseDirectPut).To(BeFalse())

			// Prepare test data
			testData = make([]byte, 10*1024*1024)
			for i := range testData {
				testData[i] = byte(i % 256)
			}

			testIndex = []byte("test index data for multipart")
		})

		Context("when completing a successful multipart upload", func() {
			It("should complete successfully with all parts uploaded", func(ctx SpecContext) {
				// Upload all parts
				partSize := 5 * 1024 * 1024 // 5MB per part
				var etags []string

				for i, url := range uploadResp.PresignedMultipartUploadPutURLs {
					startOffset := i * partSize
					endOffset := startOffset + partSize
					if endOffset > len(testData) {
						endOffset = len(testData)
					}

					partData := testData[startOffset:endOffset]
					resp, err := httpPut(url, bytes.NewReader(partData))
					Expect(err).NotTo(HaveOccurred())
					Expect(resp.StatusCode).To(Equal(http.StatusOK))

					// Get ETag from response
					etag := resp.Header.Get("ETag")
					Expect(etag).NotTo(BeEmpty())
					etags = append(etags, etag)
					resp.Body.Close()
				}

				// Upload index file
				indexResp, err := httpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(testIndex))
				Expect(err).NotTo(HaveOccurred())
				Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
				indexResp.Body.Close()

				// Complete the upload
				completeReq := &uploaddatarange.CompleteUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
					UploadIDs:         etags,
				}

				err = uploadSrv.CompleteDatarangeUpload(ctx, logger, completeReq)
				Expect(err).NotTo(HaveOccurred())

				// Verify upload record was deleted
				uploadCount, err := queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(int64(0)))

				// Verify datarange record still exists
				datarangeCount, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(1)))

				// Verify the file was actually uploaded and accessible
				getResp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(testBucketName),
					Key:    aws.String(uploadResp.ObjectKey),
				})
				Expect(err).NotTo(HaveOccurred())
				defer getResp.Body.Close()

				downloadedData, err := io.ReadAll(getResp.Body)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(downloadedData)).To(Equal(len(testData)))
			})
		})

		Context("when multipart upload fails due to missing parts", func() {
			It("should fail to complete with incomplete parts", func(ctx SpecContext) {
				// Upload only the first part (missing second part)
				partSize := 5 * 1024 * 1024 // 5MB per part
				partData := testData[:partSize]

				resp, err := httpPut(uploadResp.PresignedMultipartUploadPutURLs[0], bytes.NewReader(partData))
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.StatusCode).To(Equal(http.StatusOK))

				etag := resp.Header.Get("ETag")
				Expect(etag).NotTo(BeEmpty())
				resp.Body.Close()

				// Upload index file
				indexResp, err := httpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(testIndex))
				Expect(err).NotTo(HaveOccurred())
				Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
				indexResp.Body.Close()

				// Try to complete with only one ETag (should fail)
				completeReq := &uploaddatarange.CompleteUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
					UploadIDs:         []string{etag}, // Missing second part
				}

				err = uploadSrv.CompleteDatarangeUpload(ctx, logger, completeReq)
				Expect(err).To(HaveOccurred())
				// When only partial data is uploaded, it fails with size mismatch before multipart completion
				Expect(err.Error()).To(ContainSubstring("uploaded size mismatch"))

				// Verify records were cleaned up (both upload and datarange should be deleted on failure)
				uploadCount, err := queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(int64(0)))

				// Verify cleanup tasks were scheduled
				cleanupTasks, err := queries.CountKeysToDelete(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(cleanupTasks).To(Equal(int64(2)))
			})
		})
	})

	Context("CancelDatarangeUpload", func() {
		Context("when cancelling a direct PUT upload", func() {
			var uploadResp *uploaddatarange.UploadDatarangeResponse

			BeforeEach(func(ctx SpecContext) {
				// Start a small upload that uses direct PUT
				req := &uploaddatarange.UploadDatarangeRequest{
					Datas3tName:         testDatasetName,
					DataSize:            1024, // Small size < 5MB
					NumberOfDatapoints:  10,
					FirstDatapointIndex: 0,
				}

				var err error
				uploadResp, err = uploadSrv.StartDatarangeUpload(ctx, logger, req)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadResp.UseDirectPut).To(BeTrue())
			})

			It("should successfully cancel upload and clean up database records", func(ctx SpecContext) {
				// Verify initial state
				uploadCount, err := queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(int64(1)))

				datarangeCount, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(1)))

				// Cancel the upload
				cancelReq := &uploaddatarange.CancelUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err = uploadSrv.CancelDatarangeUpload(ctx, logger, cancelReq)
				Expect(err).NotTo(HaveOccurred())

				// Verify upload record was deleted
				uploadCount2, err := queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount2).To(Equal(int64(0)))

				// Verify datarange record was also deleted
				datarangeCount2, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount2).To(Equal(int64(0)))

				// Verify cleanup tasks were scheduled
				cleanupTasks, err := queries.CountKeysToDelete(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(cleanupTasks).To(Equal(int64(2)))
			})
		})

		Context("when cancelling a multipart upload", func() {
			var uploadResp *uploaddatarange.UploadDatarangeResponse

			BeforeEach(func(ctx SpecContext) {
				// Start a large upload that requires multipart
				req := &uploaddatarange.UploadDatarangeRequest{
					Datas3tName:         testDatasetName,
					DataSize:            10 * 1024 * 1024, // 10MB
					NumberOfDatapoints:  1000,
					FirstDatapointIndex: 100,
				}

				var err error
				uploadResp, err = uploadSrv.StartDatarangeUpload(ctx, logger, req)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadResp.UseDirectPut).To(BeFalse())
			})

			It("should successfully cancel multipart upload and clean up database records", func(ctx SpecContext) {
				// Verify initial state
				uploadCount, err := queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(int64(1)))

				datarangeCount, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(1)))

				// Cancel the upload
				cancelReq := &uploaddatarange.CancelUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err = uploadSrv.CancelDatarangeUpload(ctx, logger, cancelReq)
				Expect(err).NotTo(HaveOccurred())

				// Verify upload record was deleted
				uploadCount2, err := queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount2).To(Equal(int64(0)))

				// Verify datarange record was also deleted
				datarangeCount2, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount2).To(Equal(int64(0)))

				// Verify cleanup tasks were scheduled
				cleanupTasks, err := queries.CountKeysToDelete(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(cleanupTasks).To(Equal(int64(2)))
			})

			It("should handle partial uploads by cancelling and cleaning up properly", func(ctx SpecContext) {
				// Upload one part to simulate partial upload
				testData := make([]byte, 5*1024*1024) // 5MB for first part
				for i := range testData {
					testData[i] = byte(i % 256)
				}

				resp, err := httpPut(uploadResp.PresignedMultipartUploadPutURLs[0], bytes.NewReader(testData))
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.StatusCode).To(Equal(http.StatusOK))
				resp.Body.Close()

				// Cancel the upload (should abort multipart and clean up)
				cancelReq := &uploaddatarange.CancelUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err = uploadSrv.CancelDatarangeUpload(ctx, logger, cancelReq)
				Expect(err).NotTo(HaveOccurred())

				// Verify database cleanup
				uploadCount, err := queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(int64(0)))

				datarangeCount, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(0)))

				// Verify cleanup tasks were scheduled
				cleanupTasks, err := queries.CountKeysToDelete(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(cleanupTasks).To(Equal(int64(2)))
			})
		})

		Context("when validation fails", func() {
			It("should reject non-existent upload ID", func(ctx SpecContext) {
				cancelReq := &uploaddatarange.CancelUploadRequest{
					DatarangeUploadID: 999999, // Non-existent ID
				}

				err := uploadSrv.CancelDatarangeUpload(ctx, logger, cancelReq)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("failed to get datarange upload details"))
			})
		})

		Context("when upload has already been cancelled", func() {
			var uploadResp *uploaddatarange.UploadDatarangeResponse

			BeforeEach(func(ctx SpecContext) {
				// Start an upload
				req := &uploaddatarange.UploadDatarangeRequest{
					Datas3tName:         testDatasetName,
					DataSize:            1024,
					NumberOfDatapoints:  10,
					FirstDatapointIndex: 0,
				}

				var err error
				uploadResp, err = uploadSrv.StartDatarangeUpload(ctx, logger, req)
				Expect(err).NotTo(HaveOccurred())

				// Cancel it once
				cancelReq := &uploaddatarange.CancelUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}
				err = uploadSrv.CancelDatarangeUpload(ctx, logger, cancelReq)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should return error when trying to cancel again", func(ctx SpecContext) {
				// Try to cancel again
				cancelReq := &uploaddatarange.CancelUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err := uploadSrv.CancelDatarangeUpload(ctx, logger, cancelReq)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("failed to get datarange upload details"))
			})
		})
	})

	Context("Tar Index Validation", func() {
		var uploadResp *uploaddatarange.UploadDatarangeResponse
		var properTarData []byte
		var properTarIndex []byte

		BeforeEach(func(ctx SpecContext) {
			// Create a proper TAR archive with correctly named files
			properTarData, properTarIndex = createProperTarWithIndex(5, 0) // 5 files starting from index 0

			// Start an upload with the correct size
			req := &uploaddatarange.UploadDatarangeRequest{
				Datas3tName:         testDatasetName,
				DataSize:            uint64(len(properTarData)),
				NumberOfDatapoints:  5,
				FirstDatapointIndex: 0,
			}

			var err error
			uploadResp, err = uploadSrv.StartDatarangeUpload(ctx, logger, req)
			Expect(err).NotTo(HaveOccurred())
		})

		Context("when tar validation succeeds", func() {
			It("should validate proper tar files with correct index", func(ctx SpecContext) {
				// Upload proper tar data
				dataResp, err := httpPut(uploadResp.PresignedDataPutURL, bytes.NewReader(properTarData))
				Expect(err).NotTo(HaveOccurred())
				Expect(dataResp.StatusCode).To(Equal(http.StatusOK))
				dataResp.Body.Close()

				// Upload proper tar index
				indexResp, err := httpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(properTarIndex))
				Expect(err).NotTo(HaveOccurred())
				Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
				indexResp.Body.Close()

				// Complete the upload - should succeed with validation
				completeReq := &uploaddatarange.CompleteUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err = uploadSrv.CompleteDatarangeUpload(ctx, logger, completeReq)
				Expect(err).NotTo(HaveOccurred())

				// Verify upload completed successfully
				uploadCount, err := queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(int64(0)))
			})
		})

		Context("when tar validation fails due to size mismatch", func() {
			It("should reject tar with incorrect size", func(ctx SpecContext) {
				// Create tar data with wrong size (truncate it)
				wrongSizeTarData := properTarData[:len(properTarData)-100]

				// Upload wrong size tar data
				dataResp, err := httpPut(uploadResp.PresignedDataPutURL, bytes.NewReader(wrongSizeTarData))
				Expect(err).NotTo(HaveOccurred())
				Expect(dataResp.StatusCode).To(Equal(http.StatusOK))
				dataResp.Body.Close()

				// Upload proper tar index (which will now be inconsistent with data)
				indexResp, err := httpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(properTarIndex))
				Expect(err).NotTo(HaveOccurred())
				Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
				indexResp.Body.Close()

				// Complete the upload - should fail during validation
				completeReq := &uploaddatarange.CompleteUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err = uploadSrv.CompleteDatarangeUpload(ctx, logger, completeReq)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("uploaded size mismatch"))
			})
		})

		Context("when tar validation fails due to invalid file names", func() {
			It("should reject tar with incorrectly named files", func(ctx SpecContext) {
				// Create tar with wrong file names
				invalidTarData, invalidTarIndex := createTarWithInvalidNames()

				// Update the upload request with correct size for the invalid tar
				err := uploadSrv.CancelDatarangeUpload(ctx, logger, &uploaddatarange.CancelUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				})
				Expect(err).NotTo(HaveOccurred())

				// Start a new upload with correct size
				req := &uploaddatarange.UploadDatarangeRequest{
					Datas3tName:         testDatasetName,
					DataSize:            uint64(len(invalidTarData)),
					NumberOfDatapoints:  3,
					FirstDatapointIndex: 0,
				}

				uploadResp, err = uploadSrv.StartDatarangeUpload(ctx, logger, req)
				Expect(err).NotTo(HaveOccurred())

				// Upload invalid tar data
				dataResp, err := httpPut(uploadResp.PresignedDataPutURL, bytes.NewReader(invalidTarData))
				Expect(err).NotTo(HaveOccurred())
				Expect(dataResp.StatusCode).To(Equal(http.StatusOK))
				dataResp.Body.Close()

				// Upload invalid tar index
				indexResp, err := httpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(invalidTarIndex))
				Expect(err).NotTo(HaveOccurred())
				Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
				indexResp.Body.Close()

				// Complete the upload - should fail during validation
				completeReq := &uploaddatarange.CompleteUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err = uploadSrv.CompleteDatarangeUpload(ctx, logger, completeReq)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("tar index validation failed"))
				Expect(err.Error()).To(ContainSubstring("invalid file name format"))
			})
		})
	})
})
