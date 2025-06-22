package uploaddatarange_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/draganm/datas3t2/server/addbucket"
	"github.com/draganm/datas3t2/server/adddatas3t"
	"github.com/draganm/datas3t2/server/uploaddatarange"
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

var _ = Describe("UploadDatarange", func() {
	var (
		ctx                  context.Context
		cancel               context.CancelFunc
		pgContainer          *tc_postgres.PostgresContainer
		minioContainer       *minio.MinioContainer
		db                   *pgxpool.Pool
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
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 300*time.Second)

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
					WithStartupTimeout(30*time.Second)),
		)
		Expect(err).NotTo(HaveOccurred())

		// Get PostgreSQL connection string
		connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
		Expect(err).NotTo(HaveOccurred())

		// Connect to PostgreSQL
		db, err = pgxpool.New(ctx, connStr)
		Expect(err).NotTo(HaveOccurred())

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
		minioContainer, err = minio.Run(ctx,
			"minio/minio:RELEASE.2024-01-16T16-07-38Z",
			minio.WithUsername("minioadmin"),
			minio.WithPassword("minioadmin"),
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

		err = bucketSrv.AddBucket(ctx, bucketInfo)
		Expect(err).NotTo(HaveOccurred())

		// Add test dataset
		datasetReq := &adddatas3t.AddDatas3tRequest{
			Bucket: testBucketConfigName,
			Name:   testDatasetName,
		}

		err = datasetSrv.AddDatas3t(ctx, datasetReq)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
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
		cancel()
	})

	Context("StartDatarangeUpload", func() {
		Context("when starting a valid small upload (direct PUT)", func() {
			It("should successfully create upload with direct PUT URLs", func() {
				req := &uploaddatarange.UploadDatarangeRequest{
					Datas3tName:         testDatasetName,
					DataSize:            1024, // Small size < 5MB
					NumberOfDatapoints:  10,
					FirstDatapointIndex: 0,
				}

				resp, err := uploadSrv.StartDatarangeUpload(ctx, req)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp).NotTo(BeNil())
				Expect(resp.UseDirectPut).To(BeTrue())
				Expect(resp.PresignedDataPutURL).NotTo(BeEmpty())
				Expect(resp.PresignedIndexPutURL).NotTo(BeEmpty())
				Expect(resp.PresignedMultipartUploadPutURLs).To(BeEmpty())
				Expect(resp.DatarangeID).To(BeNumerically(">", 0))
				Expect(resp.FirstDatapointIndex).To(Equal(uint64(0)))

				// Verify database state
				dataranges, err := db.Query(ctx, "SELECT id, dataset_id, min_datapoint_key, max_datapoint_key, size_bytes FROM dataranges")
				Expect(err).NotTo(HaveOccurred())
				defer dataranges.Close()

				var datarangeCount int
				for dataranges.Next() {
					datarangeCount++
					var id, datasetID, minKey, maxKey, sizeBytes int64
					err = dataranges.Scan(&id, &datasetID, &minKey, &maxKey, &sizeBytes)
					Expect(err).NotTo(HaveOccurred())
					Expect(minKey).To(Equal(int64(0)))
					Expect(maxKey).To(Equal(int64(9)))
					Expect(sizeBytes).To(Equal(int64(1024)))
				}
				Expect(datarangeCount).To(Equal(1))

				// Verify upload record
				uploads, err := db.Query(ctx, "SELECT id, datarange_id, upload_id, first_datapoint_index, number_of_datapoints, data_size FROM datarange_uploads")
				Expect(err).NotTo(HaveOccurred())
				defer uploads.Close()

				var uploadCount int
				for uploads.Next() {
					uploadCount++
					var id, datarangeID, firstIndex, numDatapoints, dataSize int64
					var uploadID string
					err = uploads.Scan(&id, &datarangeID, &uploadID, &firstIndex, &numDatapoints, &dataSize)
					Expect(err).NotTo(HaveOccurred())
					Expect(uploadID).To(Equal("DIRECT_PUT"))
					Expect(firstIndex).To(Equal(int64(0)))
					Expect(numDatapoints).To(Equal(int64(10)))
					Expect(dataSize).To(Equal(int64(1024)))
				}
				Expect(uploadCount).To(Equal(1))
			})
		})

		Context("when starting a valid large upload (multipart)", func() {
			It("should successfully create upload with multipart URLs", func() {
				req := &uploaddatarange.UploadDatarangeRequest{
					Datas3tName:         testDatasetName,
					DataSize:            10 * 1024 * 1024, // 10MB > 5MB threshold
					NumberOfDatapoints:  1000,
					FirstDatapointIndex: 100,
				}

				resp, err := uploadSrv.StartDatarangeUpload(ctx, req)
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
				dataranges, err := db.Query(ctx, "SELECT min_datapoint_key, max_datapoint_key, size_bytes FROM dataranges")
				Expect(err).NotTo(HaveOccurred())
				defer dataranges.Close()

				Expect(dataranges.Next()).To(BeTrue())
				var minKey, maxKey, sizeBytes int64
				err = dataranges.Scan(&minKey, &maxKey, &sizeBytes)
				Expect(err).NotTo(HaveOccurred())
				Expect(minKey).To(Equal(int64(100)))
				Expect(maxKey).To(Equal(int64(1099))) // 100 + 1000 - 1
				Expect(sizeBytes).To(Equal(int64(10 * 1024 * 1024)))

				// Verify upload record
				uploads, err := db.Query(ctx, "SELECT upload_id FROM datarange_uploads")
				Expect(err).NotTo(HaveOccurred())
				defer uploads.Close()

				Expect(uploads.Next()).To(BeTrue())
				var uploadID string
				err = uploads.Scan(&uploadID)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadID).NotTo(Equal("DIRECT_PUT"))
				Expect(uploadID).NotTo(BeEmpty())
			})
		})

		Context("when validation fails", func() {
			It("should reject empty dataset name", func() {
				req := &uploaddatarange.UploadDatarangeRequest{
					Datas3tName:         "",
					DataSize:            1024,
					NumberOfDatapoints:  10,
					FirstDatapointIndex: 0,
				}

				_, err := uploadSrv.StartDatarangeUpload(ctx, req)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("datas3t_name is required"))

				// Verify no database changes
				count, err := db.Query(ctx, "SELECT count(*) FROM dataranges")
				Expect(err).NotTo(HaveOccurred())
				defer count.Close()
				Expect(count.Next()).To(BeTrue())
				var datarangeCount int
				err = count.Scan(&datarangeCount)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(0))
			})

			It("should reject zero data size", func() {
				req := &uploaddatarange.UploadDatarangeRequest{
					Datas3tName:         testDatasetName,
					DataSize:            0,
					NumberOfDatapoints:  10,
					FirstDatapointIndex: 0,
				}

				_, err := uploadSrv.StartDatarangeUpload(ctx, req)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("data_size must be greater than 0"))

				// Verify no database changes
				count, err := db.Query(ctx, "SELECT count(*) FROM dataranges")
				Expect(err).NotTo(HaveOccurred())
				defer count.Close()
				Expect(count.Next()).To(BeTrue())
				var datarangeCount int
				err = count.Scan(&datarangeCount)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(0))
			})

			It("should reject zero number of datapoints", func() {
				req := &uploaddatarange.UploadDatarangeRequest{
					Datas3tName:         testDatasetName,
					DataSize:            1024,
					NumberOfDatapoints:  0,
					FirstDatapointIndex: 0,
				}

				_, err := uploadSrv.StartDatarangeUpload(ctx, req)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("number_of_datapoints must be greater than 0"))

				// Verify no database changes
				count, err := db.Query(ctx, "SELECT count(*) FROM dataranges")
				Expect(err).NotTo(HaveOccurred())
				defer count.Close()
				Expect(count.Next()).To(BeTrue())
				var datarangeCount int
				err = count.Scan(&datarangeCount)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(0))
			})

			It("should reject non-existent dataset", func() {
				req := &uploaddatarange.UploadDatarangeRequest{
					Datas3tName:         "non-existent-dataset",
					DataSize:            1024,
					NumberOfDatapoints:  10,
					FirstDatapointIndex: 0,
				}

				_, err := uploadSrv.StartDatarangeUpload(ctx, req)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("failed to find dataset 'non-existent-dataset'"))

				// Verify no database changes
				count, err := db.Query(ctx, "SELECT count(*) FROM dataranges")
				Expect(err).NotTo(HaveOccurred())
				defer count.Close()
				Expect(count.Next()).To(BeTrue())
				var datarangeCount int
				err = count.Scan(&datarangeCount)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(0))
			})
		})

		Context("when handling overlapping dataranges", func() {
			BeforeEach(func() {
				// Create an existing datarange from 0-99
				req := &uploaddatarange.UploadDatarangeRequest{
					Datas3tName:         testDatasetName,
					DataSize:            1024,
					NumberOfDatapoints:  100,
					FirstDatapointIndex: 0,
				}

				_, err := uploadSrv.StartDatarangeUpload(ctx, req)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should reject overlapping ranges", func() {
				// Try to create overlapping range 50-149
				req := &uploaddatarange.UploadDatarangeRequest{
					Datas3tName:         testDatasetName,
					DataSize:            1024,
					NumberOfDatapoints:  100,
					FirstDatapointIndex: 50,
				}

				_, err := uploadSrv.StartDatarangeUpload(ctx, req)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("datarange overlaps with existing dataranges"))

				// Verify only one datarange exists
				count, err := db.Query(ctx, "SELECT count(*) FROM dataranges")
				Expect(err).NotTo(HaveOccurred())
				defer count.Close()
				Expect(count.Next()).To(BeTrue())
				var datarangeCount int
				err = count.Scan(&datarangeCount)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(1))
			})

			It("should allow adjacent ranges", func() {
				// Create adjacent range 100-199 (no overlap)
				req := &uploaddatarange.UploadDatarangeRequest{
					Datas3tName:         testDatasetName,
					DataSize:            1024,
					NumberOfDatapoints:  100,
					FirstDatapointIndex: 100,
				}

				_, err := uploadSrv.StartDatarangeUpload(ctx, req)
				Expect(err).NotTo(HaveOccurred())

				// Verify two dataranges exist
				count, err := db.Query(ctx, "SELECT count(*) FROM dataranges")
				Expect(err).NotTo(HaveOccurred())
				defer count.Close()
				Expect(count.Next()).To(BeTrue())
				var datarangeCount int
				err = count.Scan(&datarangeCount)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(2))
			})
		})
	})

	Context("CompleteUpload", func() {
		var uploadResp *uploaddatarange.UploadDatarangeResponse
		var testData []byte
		var testIndex []byte

		BeforeEach(func() {
			// Start an upload
			req := &uploaddatarange.UploadDatarangeRequest{
				Datas3tName:         testDatasetName,
				DataSize:            1024,
				NumberOfDatapoints:  10,
				FirstDatapointIndex: 0,
			}

			var err error
			uploadResp, err = uploadSrv.StartDatarangeUpload(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			// Prepare test data
			testData = make([]byte, 1024)
			for i := range testData {
				testData[i] = byte(i % 256)
			}

			testIndex = []byte("test index data")
		})

		Context("when completing a successful direct PUT upload", func() {
			It("should complete successfully with both files uploaded", func() {
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

				err = uploadSrv.CompleteDatarangeUpload(ctx, completeReq)
				Expect(err).NotTo(HaveOccurred())

				// Verify upload record was deleted
				uploads, err := db.Query(ctx, "SELECT count(*) FROM datarange_uploads")
				Expect(err).NotTo(HaveOccurred())
				defer uploads.Close()
				Expect(uploads.Next()).To(BeTrue())
				var uploadCount int
				err = uploads.Scan(&uploadCount)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(0))

				// Verify datarange record still exists
				dataranges, err := db.Query(ctx, "SELECT count(*) FROM dataranges")
				Expect(err).NotTo(HaveOccurred())
				defer dataranges.Close()
				Expect(dataranges.Next()).To(BeTrue())
				var datarangeCount int
				err = dataranges.Scan(&datarangeCount)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(1))
			})
		})

		Context("when index file is missing", func() {
			It("should fail and schedule cleanup", func() {
				// Upload only data file (no index)
				dataResp, err := httpPut(uploadResp.PresignedDataPutURL, bytes.NewReader(testData))
				Expect(err).NotTo(HaveOccurred())
				Expect(dataResp.StatusCode).To(Equal(http.StatusOK))
				dataResp.Body.Close()

				// Complete the upload (should fail)
				completeReq := &uploaddatarange.CompleteUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err = uploadSrv.CompleteDatarangeUpload(ctx, completeReq)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("index file not found"))

				// Verify cleanup happened - both upload and datarange records should be deleted
				uploads, err := db.Query(ctx, "SELECT count(*) FROM datarange_uploads")
				Expect(err).NotTo(HaveOccurred())
				defer uploads.Close()
				Expect(uploads.Next()).To(BeTrue())
				var uploadCount int
				err = uploads.Scan(&uploadCount)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(0))

				dataranges, err := db.Query(ctx, "SELECT count(*) FROM dataranges")
				Expect(err).NotTo(HaveOccurred())
				defer dataranges.Close()
				Expect(dataranges.Next()).To(BeTrue())
				var datarangeCount int
				err = dataranges.Scan(&datarangeCount)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(0))

				// Verify cleanup tasks were scheduled
				cleanupTasks, err := db.Query(ctx, "SELECT count(*) FROM keys_to_delete")
				Expect(err).NotTo(HaveOccurred())
				defer cleanupTasks.Close()
				Expect(cleanupTasks.Next()).To(BeTrue())
				var cleanupCount int
				err = cleanupTasks.Scan(&cleanupCount)
				Expect(err).NotTo(HaveOccurred())
				Expect(cleanupCount).To(Equal(2)) // Both data and index objects scheduled for deletion
			})
		})

		Context("when data file is missing", func() {
			It("should fail and schedule cleanup", func() {
				// Upload only index file (no data)
				indexResp, err := httpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(testIndex))
				Expect(err).NotTo(HaveOccurred())
				Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
				indexResp.Body.Close()

				// Complete the upload (should fail)
				completeReq := &uploaddatarange.CompleteUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err = uploadSrv.CompleteDatarangeUpload(ctx, completeReq)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("failed to get uploaded object info"))

				// Verify cleanup happened
				uploads, err := db.Query(ctx, "SELECT count(*) FROM datarange_uploads")
				Expect(err).NotTo(HaveOccurred())
				defer uploads.Close()
				Expect(uploads.Next()).To(BeTrue())
				var uploadCount int
				err = uploads.Scan(&uploadCount)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(0))

				// Verify cleanup tasks were scheduled
				cleanupTasks, err := db.Query(ctx, "SELECT count(*) FROM keys_to_delete")
				Expect(err).NotTo(HaveOccurred())
				defer cleanupTasks.Close()
				Expect(cleanupTasks.Next()).To(BeTrue())
				var cleanupCount int
				err = cleanupTasks.Scan(&cleanupCount)
				Expect(err).NotTo(HaveOccurred())
				Expect(cleanupCount).To(Equal(2))
			})
		})

		Context("when data size is wrong", func() {
			It("should fail and schedule cleanup", func() {
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

				err = uploadSrv.CompleteDatarangeUpload(ctx, completeReq)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("uploaded size mismatch"))
				Expect(err.Error()).To(ContainSubstring("expected 1024, got 512"))

				// Verify cleanup happened
				uploads, err := db.Query(ctx, "SELECT count(*) FROM datarange_uploads")
				Expect(err).NotTo(HaveOccurred())
				defer uploads.Close()
				Expect(uploads.Next()).To(BeTrue())
				var uploadCount int
				err = uploads.Scan(&uploadCount)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(0))

				// Verify cleanup tasks were scheduled
				cleanupTasks, err := db.Query(ctx, "SELECT count(*) FROM keys_to_delete")
				Expect(err).NotTo(HaveOccurred())
				defer cleanupTasks.Close()
				Expect(cleanupTasks.Next()).To(BeTrue())
				var cleanupCount int
				err = cleanupTasks.Scan(&cleanupCount)
				Expect(err).NotTo(HaveOccurred())
				Expect(cleanupCount).To(Equal(2))
			})
		})
	})

	Context("Multipart Upload Complete", func() {
		var uploadResp *uploaddatarange.UploadDatarangeResponse
		var testData []byte
		var testIndex []byte

		BeforeEach(func() {
			// Start a large upload that requires multipart
			req := &uploaddatarange.UploadDatarangeRequest{
				Datas3tName:         testDatasetName,
				DataSize:            10 * 1024 * 1024, // 10MB
				NumberOfDatapoints:  1000,
				FirstDatapointIndex: 0,
			}

			var err error
			uploadResp, err = uploadSrv.StartDatarangeUpload(ctx, req)
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
			It("should complete successfully with all parts uploaded", func() {
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

				err = uploadSrv.CompleteDatarangeUpload(ctx, completeReq)
				Expect(err).NotTo(HaveOccurred())

				// Verify upload record was deleted
				uploads, err := db.Query(ctx, "SELECT count(*) FROM datarange_uploads")
				Expect(err).NotTo(HaveOccurred())
				defer uploads.Close()
				Expect(uploads.Next()).To(BeTrue())
				var uploadCount int
				err = uploads.Scan(&uploadCount)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(0))

				// Verify datarange record still exists
				dataranges, err := db.Query(ctx, "SELECT count(*) FROM dataranges")
				Expect(err).NotTo(HaveOccurred())
				defer dataranges.Close()
				Expect(dataranges.Next()).To(BeTrue())
				var datarangeCount int
				err = dataranges.Scan(&datarangeCount)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(1))

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
			It("should fail to complete with incomplete parts", func() {
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

				err = uploadSrv.CompleteDatarangeUpload(ctx, completeReq)
				Expect(err).To(HaveOccurred())
				// When only partial data is uploaded, it fails with size mismatch before multipart completion
				Expect(err.Error()).To(ContainSubstring("uploaded size mismatch"))

				// Verify records were cleaned up (both upload and datarange should be deleted on failure)
				uploads, err := db.Query(ctx, "SELECT count(*) FROM datarange_uploads")
				Expect(err).NotTo(HaveOccurred())
				defer uploads.Close()
				Expect(uploads.Next()).To(BeTrue())
				var uploadCount int
				err = uploads.Scan(&uploadCount)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(0)) // Upload record should be deleted after failed completion

				// Verify cleanup tasks were scheduled
				cleanupTasks, err := db.Query(ctx, "SELECT count(*) FROM keys_to_delete")
				Expect(err).NotTo(HaveOccurred())
				defer cleanupTasks.Close()
				Expect(cleanupTasks.Next()).To(BeTrue())
				var cleanupCount int
				err = cleanupTasks.Scan(&cleanupCount)
				Expect(err).NotTo(HaveOccurred())
				Expect(cleanupCount).To(Equal(2)) // Both data and index objects scheduled for deletion
			})
		})
	})

	Context("CancelDatarangeUpload", func() {
		Context("when cancelling a direct PUT upload", func() {
			var uploadResp *uploaddatarange.UploadDatarangeResponse

			BeforeEach(func() {
				// Start a small upload that uses direct PUT
				req := &uploaddatarange.UploadDatarangeRequest{
					Datas3tName:         testDatasetName,
					DataSize:            1024, // Small size < 5MB
					NumberOfDatapoints:  10,
					FirstDatapointIndex: 0,
				}

				var err error
				uploadResp, err = uploadSrv.StartDatarangeUpload(ctx, req)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadResp.UseDirectPut).To(BeTrue())
			})

			It("should successfully cancel upload and clean up database records", func() {
				// Verify initial state
				uploads, err := db.Query(ctx, "SELECT count(*) FROM datarange_uploads")
				Expect(err).NotTo(HaveOccurred())
				defer uploads.Close()
				Expect(uploads.Next()).To(BeTrue())
				var uploadCount int
				err = uploads.Scan(&uploadCount)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(1))

				dataranges, err := db.Query(ctx, "SELECT count(*) FROM dataranges")
				Expect(err).NotTo(HaveOccurred())
				defer dataranges.Close()
				Expect(dataranges.Next()).To(BeTrue())
				var datarangeCount int
				err = dataranges.Scan(&datarangeCount)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(1))

				// Cancel the upload
				cancelReq := &uploaddatarange.CancelUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err = uploadSrv.CancelDatarangeUpload(ctx, cancelReq)
				Expect(err).NotTo(HaveOccurred())

				// Verify upload record was deleted
				uploads2, err := db.Query(ctx, "SELECT count(*) FROM datarange_uploads")
				Expect(err).NotTo(HaveOccurred())
				defer uploads2.Close()
				Expect(uploads2.Next()).To(BeTrue())
				var uploadCount2 int
				err = uploads2.Scan(&uploadCount2)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount2).To(Equal(0))

				// Verify datarange record was also deleted
				dataranges2, err := db.Query(ctx, "SELECT count(*) FROM dataranges")
				Expect(err).NotTo(HaveOccurred())
				defer dataranges2.Close()
				Expect(dataranges2.Next()).To(BeTrue())
				var datarangeCount2 int
				err = dataranges2.Scan(&datarangeCount2)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount2).To(Equal(0))

				// Verify cleanup tasks were scheduled
				cleanupTasks, err := db.Query(ctx, "SELECT count(*) FROM keys_to_delete")
				Expect(err).NotTo(HaveOccurred())
				defer cleanupTasks.Close()
				Expect(cleanupTasks.Next()).To(BeTrue())
				var cleanupCount int
				err = cleanupTasks.Scan(&cleanupCount)
				Expect(err).NotTo(HaveOccurred())
				Expect(cleanupCount).To(Equal(2)) // Both data and index objects scheduled for deletion
			})
		})

		Context("when cancelling a multipart upload", func() {
			var uploadResp *uploaddatarange.UploadDatarangeResponse

			BeforeEach(func() {
				// Start a large upload that requires multipart
				req := &uploaddatarange.UploadDatarangeRequest{
					Datas3tName:         testDatasetName,
					DataSize:            10 * 1024 * 1024, // 10MB
					NumberOfDatapoints:  1000,
					FirstDatapointIndex: 100,
				}

				var err error
				uploadResp, err = uploadSrv.StartDatarangeUpload(ctx, req)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadResp.UseDirectPut).To(BeFalse())
			})

			It("should successfully cancel multipart upload and clean up database records", func() {
				// Verify initial state
				uploads, err := db.Query(ctx, "SELECT count(*) FROM datarange_uploads")
				Expect(err).NotTo(HaveOccurred())
				defer uploads.Close()
				Expect(uploads.Next()).To(BeTrue())
				var uploadCount int
				err = uploads.Scan(&uploadCount)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(1))

				dataranges, err := db.Query(ctx, "SELECT count(*) FROM dataranges")
				Expect(err).NotTo(HaveOccurred())
				defer dataranges.Close()
				Expect(dataranges.Next()).To(BeTrue())
				var datarangeCount int
				err = dataranges.Scan(&datarangeCount)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(1))

				// Cancel the upload
				cancelReq := &uploaddatarange.CancelUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err = uploadSrv.CancelDatarangeUpload(ctx, cancelReq)
				Expect(err).NotTo(HaveOccurred())

				// Verify upload record was deleted
				uploads2, err := db.Query(ctx, "SELECT count(*) FROM datarange_uploads")
				Expect(err).NotTo(HaveOccurred())
				defer uploads2.Close()
				Expect(uploads2.Next()).To(BeTrue())
				var uploadCount2 int
				err = uploads2.Scan(&uploadCount2)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount2).To(Equal(0))

				// Verify datarange record was also deleted
				dataranges2, err := db.Query(ctx, "SELECT count(*) FROM dataranges")
				Expect(err).NotTo(HaveOccurred())
				defer dataranges2.Close()
				Expect(dataranges2.Next()).To(BeTrue())
				var datarangeCount2 int
				err = dataranges2.Scan(&datarangeCount2)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount2).To(Equal(0))

				// Verify cleanup tasks were scheduled
				cleanupTasks, err := db.Query(ctx, "SELECT count(*) FROM keys_to_delete")
				Expect(err).NotTo(HaveOccurred())
				defer cleanupTasks.Close()
				Expect(cleanupTasks.Next()).To(BeTrue())
				var cleanupCount int
				err = cleanupTasks.Scan(&cleanupCount)
				Expect(err).NotTo(HaveOccurred())
				Expect(cleanupCount).To(Equal(2)) // Both data and index objects scheduled for deletion
			})

			It("should handle partial uploads by cancelling and cleaning up properly", func() {
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

				err = uploadSrv.CancelDatarangeUpload(ctx, cancelReq)
				Expect(err).NotTo(HaveOccurred())

				// Verify database cleanup
				uploads, err := db.Query(ctx, "SELECT count(*) FROM datarange_uploads")
				Expect(err).NotTo(HaveOccurred())
				defer uploads.Close()
				Expect(uploads.Next()).To(BeTrue())
				var uploadCount int
				err = uploads.Scan(&uploadCount)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(0))

				dataranges, err := db.Query(ctx, "SELECT count(*) FROM dataranges")
				Expect(err).NotTo(HaveOccurred())
				defer dataranges.Close()
				Expect(dataranges.Next()).To(BeTrue())
				var datarangeCount int
				err = dataranges.Scan(&datarangeCount)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(0))

				// Verify cleanup tasks were scheduled
				cleanupTasks, err := db.Query(ctx, "SELECT count(*) FROM keys_to_delete")
				Expect(err).NotTo(HaveOccurred())
				defer cleanupTasks.Close()
				Expect(cleanupTasks.Next()).To(BeTrue())
				var cleanupCount int
				err = cleanupTasks.Scan(&cleanupCount)
				Expect(err).NotTo(HaveOccurred())
				Expect(cleanupCount).To(Equal(2)) // Both data and index objects scheduled for deletion
			})
		})

		Context("when validation fails", func() {
			It("should reject non-existent upload ID", func() {
				cancelReq := &uploaddatarange.CancelUploadRequest{
					DatarangeUploadID: 999999, // Non-existent ID
				}

				err := uploadSrv.CancelDatarangeUpload(ctx, cancelReq)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("failed to get datarange upload details"))
			})
		})

		Context("when upload has already been cancelled", func() {
			var uploadResp *uploaddatarange.UploadDatarangeResponse

			BeforeEach(func() {
				// Start an upload
				req := &uploaddatarange.UploadDatarangeRequest{
					Datas3tName:         testDatasetName,
					DataSize:            1024,
					NumberOfDatapoints:  10,
					FirstDatapointIndex: 0,
				}

				var err error
				uploadResp, err = uploadSrv.StartDatarangeUpload(ctx, req)
				Expect(err).NotTo(HaveOccurred())

				// Cancel it once
				cancelReq := &uploaddatarange.CancelUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}
				err = uploadSrv.CancelDatarangeUpload(ctx, cancelReq)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should return error when trying to cancel again", func() {
				// Try to cancel again
				cancelReq := &uploaddatarange.CancelUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err := uploadSrv.CancelDatarangeUpload(ctx, cancelReq)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("failed to get datarange upload details"))
			})
		})
	})
})
