package download_test

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"archive/tar"

	"github.com/draganm/datas3t2/server/bucket"
	"github.com/draganm/datas3t2/server/dataranges"
	"github.com/draganm/datas3t2/server/datas3t"
	"github.com/draganm/datas3t2/server/download"
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

// Helper function to perform HTTP GET requests with Range header
func httpGetWithRange(url, rangeHeader string) (*http.Response, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Range", rangeHeader)

	client := &http.Client{}
	return client.Do(req)
}

// createTestTarWithIndex creates a TAR archive with correctly named files and returns both the tar data and index
func createTestTarWithIndex(numFiles int, startIndex int64) ([]byte, []byte) {
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

var _ = Describe("PresignDownloadForDatapoints", func() {
	var (
		pgContainer          *tc_postgres.PostgresContainer
		minioContainer       *minio.MinioContainer
		db                   *pgxpool.Pool
		downloadSrv          *download.DownloadServer
		uploadSrv            *dataranges.UploadDatarangeServer
		bucketSrv            *bucket.BucketServer
		datasetSrv           *datas3t.Datas3tServer
		minioEndpoint        string
		minioHost            string
		minioAccessKey       string
		minioSecretKey       string
		testBucketName       string
		testBucketConfigName string
		testDatasetName      string
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

		// Create server instances
		uploadSrv = dataranges.NewServer(db)
		bucketSrv = bucket.NewServer(db)
		datasetSrv = datas3t.NewServer(db)
		downloadSrv, err = download.NewDownloadServer(db)
		Expect(err).NotTo(HaveOccurred())

		// Add test bucket configuration
		bucketInfo := &bucket.BucketInfo{
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
		datasetReq := &datas3t.AddDatas3tRequest{
			Bucket: testBucketConfigName,
			Name:   testDatasetName,
		}

		err = datasetSrv.AddDatas3t(ctx, logger, datasetReq)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func(ctx SpecContext) {
		if downloadSrv != nil {
			downloadSrv.Close()
		}
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

	// Helper function to upload a complete datarange for testing
	uploadCompleteDatarange := func(ctx SpecContext, firstDatapoint, numDatapoints uint64) {
		// Create test data
		testData, testIndex := createTestTarWithIndex(int(numDatapoints), int64(firstDatapoint))

		// Start upload
		uploadReq := &dataranges.UploadDatarangeRequest{
			Datas3tName:         testDatasetName,
			DataSize:            uint64(len(testData)),
			NumberOfDatapoints:  numDatapoints,
			FirstDatapointIndex: firstDatapoint,
		}

		uploadResp, err := uploadSrv.StartDatarangeUpload(ctx, logger, uploadReq)
		Expect(err).NotTo(HaveOccurred())

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

		// Complete upload
		completeReq := &dataranges.CompleteUploadRequest{
			DatarangeUploadID: uploadResp.DatarangeID,
		}

		err = uploadSrv.CompleteDatarangeUpload(ctx, logger, completeReq)
		Expect(err).NotTo(HaveOccurred())
	}

	Context("when requesting download for valid datapoints", func() {
		BeforeEach(func(ctx SpecContext) {
			// Upload test dataranges
			// Datarange 1: files 0-9 (10 files)
			uploadCompleteDatarange(ctx, 0, 10)
			// Datarange 2: files 20-29 (10 files) - gap between 10-19
			uploadCompleteDatarange(ctx, 20, 10)
			// Datarange 3: files 30-49 (20 files)
			uploadCompleteDatarange(ctx, 30, 20)
		})

		It("should return download segments for a single datarange", func(ctx SpecContext) {
			req := download.PreSignDownloadForDatapointsRequest{
				Datas3tName:    testDatasetName,
				FirstDatapoint: 2,
				LastDatapoint:  7,
			}

			resp, err := downloadSrv.PreSignDownloadForDatapoints(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.DownloadSegments).To(HaveLen(1))

			segment := resp.DownloadSegments[0]
			Expect(segment.PresignedURL).NotTo(BeEmpty())
			Expect(segment.Range).To(MatchRegexp(`bytes=\d+-\d+`))

			// Test that the presigned URL actually works
			getResp, err := httpGetWithRange(segment.PresignedURL, segment.Range)
			Expect(err).NotTo(HaveOccurred())
			Expect(getResp.StatusCode).To(Equal(http.StatusPartialContent))
			getResp.Body.Close()
		})

		It("should return download segments spanning multiple dataranges", func(ctx SpecContext) {
			req := download.PreSignDownloadForDatapointsRequest{
				Datas3tName:    testDatasetName,
				FirstDatapoint: 5,
				LastDatapoint:  35,
			}

			resp, err := downloadSrv.PreSignDownloadForDatapoints(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			// Should have segments from 3 dataranges: 5-9, 20-29, 30-35
			Expect(resp.DownloadSegments).To(HaveLen(3))

			// Verify all segments have valid URLs and ranges
			for _, segment := range resp.DownloadSegments {
				Expect(segment.PresignedURL).NotTo(BeEmpty())
				Expect(segment.Range).To(MatchRegexp(`bytes=\d+-\d+`))

				// Test that the presigned URL actually works
				getResp, err := httpGetWithRange(segment.PresignedURL, segment.Range)
				Expect(err).NotTo(HaveOccurred())
				Expect(getResp.StatusCode).To(Equal(http.StatusPartialContent))
				getResp.Body.Close()
			}
		})

		It("should return download segments for exact datarange boundaries", func(ctx SpecContext) {
			req := download.PreSignDownloadForDatapointsRequest{
				Datas3tName:    testDatasetName,
				FirstDatapoint: 0,
				LastDatapoint:  9,
			}

			resp, err := downloadSrv.PreSignDownloadForDatapoints(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.DownloadSegments).To(HaveLen(1))

			segment := resp.DownloadSegments[0]
			Expect(segment.PresignedURL).NotTo(BeEmpty())
			Expect(segment.Range).To(MatchRegexp(`bytes=\d+-\d+`))
		})

		It("should return download segments for single datapoint", func(ctx SpecContext) {
			req := download.PreSignDownloadForDatapointsRequest{
				Datas3tName:    testDatasetName,
				FirstDatapoint: 25,
				LastDatapoint:  25,
			}

			resp, err := downloadSrv.PreSignDownloadForDatapoints(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.DownloadSegments).To(HaveLen(1))

			segment := resp.DownloadSegments[0]
			Expect(segment.PresignedURL).NotTo(BeEmpty())
			Expect(segment.Range).To(MatchRegexp(`bytes=\d+-\d+`))
		})

		It("should handle partial overlap with dataranges", func(ctx SpecContext) {
			req := download.PreSignDownloadForDatapointsRequest{
				Datas3tName:    testDatasetName,
				FirstDatapoint: 45,
				LastDatapoint:  55, // Extends beyond last datarange (30-49)
			}

			resp, err := downloadSrv.PreSignDownloadForDatapoints(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.DownloadSegments).To(HaveLen(1)) // Only the overlapping part (45-49)

			segment := resp.DownloadSegments[0]
			Expect(segment.PresignedURL).NotTo(BeEmpty())
			Expect(segment.Range).To(MatchRegexp(`bytes=\d+-\d+`))
		})
	})

	Context("when requesting download for invalid datapoints", func() {
		BeforeEach(func(ctx SpecContext) {
			// Upload one test datarange
			uploadCompleteDatarange(ctx, 10, 10) // files 10-19
		})

		It("should reject empty dataset name", func(ctx SpecContext) {
			req := download.PreSignDownloadForDatapointsRequest{
				Datas3tName:    "",
				FirstDatapoint: 10,
				LastDatapoint:  15,
			}

			_, err := downloadSrv.PreSignDownloadForDatapoints(ctx, req)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("datas3t_name is required"))
		})

		It("should reject first datapoint greater than last datapoint", func(ctx SpecContext) {
			req := download.PreSignDownloadForDatapointsRequest{
				Datas3tName:    testDatasetName,
				FirstDatapoint: 20,
				LastDatapoint:  10,
			}

			_, err := downloadSrv.PreSignDownloadForDatapoints(ctx, req)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("first_datapoint (20) cannot be greater than last_datapoint (10)"))
		})

		It("should reject non-existent dataset", func(ctx SpecContext) {
			req := download.PreSignDownloadForDatapointsRequest{
				Datas3tName:    "non-existent-dataset",
				FirstDatapoint: 10,
				LastDatapoint:  15,
			}

			_, err := downloadSrv.PreSignDownloadForDatapoints(ctx, req)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("no dataranges found"))
		})

		It("should reject datapoints with no overlapping dataranges", func(ctx SpecContext) {
			req := download.PreSignDownloadForDatapointsRequest{
				Datas3tName:    testDatasetName,
				FirstDatapoint: 100,
				LastDatapoint:  200,
			}

			_, err := downloadSrv.PreSignDownloadForDatapoints(ctx, req)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("no dataranges found for datapoints 100-200"))
		})
	})

	Context("when dataset has no dataranges", func() {
		It("should return error for empty dataset", func(ctx SpecContext) {
			req := download.PreSignDownloadForDatapointsRequest{
				Datas3tName:    testDatasetName,
				FirstDatapoint: 0,
				LastDatapoint:  10,
			}

			_, err := downloadSrv.PreSignDownloadForDatapoints(ctx, req)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("no dataranges found"))
		})
	})

	Context("when testing disk cache behavior", func() {
		BeforeEach(func(ctx SpecContext) {
			// Upload test datarange
			uploadCompleteDatarange(ctx, 0, 5) // files 0-4
		})

		It("should cache index files and reuse them", func(ctx SpecContext) {
			req := download.PreSignDownloadForDatapointsRequest{
				Datas3tName:    testDatasetName,
				FirstDatapoint: 1,
				LastDatapoint:  3,
			}

			// First request - should download and cache the index
			resp1, err := downloadSrv.PreSignDownloadForDatapoints(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp1.DownloadSegments).To(HaveLen(1))

			// Second request - should use cached index
			resp2, err := downloadSrv.PreSignDownloadForDatapoints(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp2.DownloadSegments).To(HaveLen(1))

			// Both responses should be identical
			Expect(resp1.DownloadSegments[0].Range).To(Equal(resp2.DownloadSegments[0].Range))
		})
	})

	Context("when handling large datapoint ranges", func() {
		BeforeEach(func(ctx SpecContext) {
			// Upload larger dataranges for testing
			uploadCompleteDatarange(ctx, 0, 100)   // files 0-99
			uploadCompleteDatarange(ctx, 100, 100) // files 100-199
		})

		It("should efficiently handle large ranges", func(ctx SpecContext) {
			req := download.PreSignDownloadForDatapointsRequest{
				Datas3tName:    testDatasetName,
				FirstDatapoint: 50,
				LastDatapoint:  150,
			}

			resp, err := downloadSrv.PreSignDownloadForDatapoints(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.DownloadSegments).To(HaveLen(2)) // Two dataranges

			// Verify segments
			for _, segment := range resp.DownloadSegments {
				Expect(segment.PresignedURL).NotTo(BeEmpty())
				Expect(segment.Range).To(MatchRegexp(`bytes=\d+-\d+`))
			}
		})
	})
})
