package datas3t2_test

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/draganm/datas3t2/client"
	"github.com/draganm/datas3t2/server/bucket"
	"github.com/draganm/datas3t2/server/datas3t"
	"github.com/draganm/datas3t2/server/download"
	"github.com/draganm/datas3t2/tarindex"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	miniogo "github.com/minio/minio-go/v7"
	miniocreds "github.com/minio/minio-go/v7/pkg/credentials"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/minio"
	tc_postgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Helper functions for HTTP requests
func httpPost(url string, body interface{}) (*http.Response, error) {
	var reqBody io.Reader
	if body != nil {
		jsonData, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reqBody = bytes.NewReader(jsonData)
	}

	req, err := http.NewRequest("POST", url, reqBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	return client.Do(req)
}

func httpPut(url string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest("PUT", url, body)
	if err != nil {
		return nil, err
	}

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

// validateTarArchive checks if the downloaded data is a valid tar archive
func validateTarArchive(data []byte) error {
	if len(data) < 1024 {
		return fmt.Errorf("data too short to be a valid tar archive")
	}

	// Check for proper tar ending (two 512-byte zero blocks)
	expectedEnd := make([]byte, 1024)
	actualEnd := data[len(data)-1024:]
	if !bytes.Equal(actualEnd, expectedEnd) {
		return fmt.Errorf("tar archive does not end with two zero blocks")
	}

	// Try to parse the tar archive
	reader := tar.NewReader(bytes.NewReader(data))
	fileCount := 0
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading tar header: %w", err)
		}

		fileCount++

		// Read file content
		content, err := io.ReadAll(reader)
		if err != nil {
			return fmt.Errorf("error reading file content: %w", err)
		}

		// Validate content matches expected pattern
		expectedContent := fmt.Sprintf("Content of file %s", strings.TrimSuffix(header.Name, ".txt"))
		if string(content) != expectedContent {
			return fmt.Errorf("file %s has unexpected content: got %q, expected %q",
				header.Name, string(content), expectedContent)
		}
	}

	if fileCount == 0 {
		return fmt.Errorf("tar archive contains no files")
	}

	return nil
}

var _ = Describe("End-to-End Server Test", func() {
	var (
		pgContainer          *tc_postgres.PostgresContainer
		minioContainer       *minio.MinioContainer
		serverCmd            *exec.Cmd
		serverBaseURL        string
		minioEndpoint        string
		minioHost            string
		minioAccessKey       string
		minioSecretKey       string
		testBucketName       string
		testBucketConfigName string
		testDatasetName      string
		logger               *slog.Logger
		tempDir              string
		datas3tClient        *client.Client
	)

	BeforeEach(func(ctx SpecContext) {
		// Create logger that writes to GinkgoWriter for test visibility
		logger = slog.New(slog.NewTextHandler(GinkgoWriter, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))

		var err error

		// Create temporary directory for cache and builds
		tempDir = GinkgoT().TempDir()

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

		// Extract host:port from the full URL
		minioHost = strings.TrimPrefix(minioEndpoint, "http://")
		minioHost = strings.TrimPrefix(minioHost, "https://")
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

		// Get PostgreSQL connection string
		connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
		Expect(err).NotTo(HaveOccurred())

		// Ensure connection string starts with postgresql: for server compatibility
		if strings.HasPrefix(connStr, "postgres://") {
			connStr = strings.Replace(connStr, "postgres://", "postgresql://", 1)
		}

		// Run migrations
		m, err := migrate.New(
			"file://postgresstore/migrations",
			connStr)
		Expect(err).NotTo(HaveOccurred())

		err = m.Up()
		if err != nil && err != migrate.ErrNoChange {
			Expect(err).NotTo(HaveOccurred())
		}

		// Compile the server
		serverBinaryPath := filepath.Join(tempDir, "datas3t2-server")
		buildCmd := exec.Command("go", "build", "-o", serverBinaryPath, "./cmd/server")
		buildCmd.Dir = "."
		buildOutput, err := buildCmd.CombinedOutput()
		if err != nil {
			logger.Error("Failed to build server", "error", err, "output", string(buildOutput))
		}
		Expect(err).NotTo(HaveOccurred())

		// Find available port for server
		listener, err := net.Listen("tcp", ":0")
		Expect(err).NotTo(HaveOccurred())
		serverPort := listener.Addr().(*net.TCPAddr).Port
		listener.Close()

		serverAddr := fmt.Sprintf(":%d", serverPort)
		serverBaseURL = fmt.Sprintf("http://localhost%s", serverAddr)
		cacheDir := filepath.Join(tempDir, "cache")

		// Start the server
		serverCmd = exec.Command(serverBinaryPath,
			"--addr", serverAddr,
			"--db-url", connStr,
			"--cache-dir", cacheDir,
			"--max-cache-size", "1073741824", // 1GB
			"--encryption-key", "dGVzdC1rZXktMzItYnl0ZXMtZm9yLXRlc3RpbmchIQ==", // test-key-32-bytes-for-testing!!
		)
		serverCmd.Stdout = GinkgoWriter
		serverCmd.Stderr = GinkgoWriter

		err = serverCmd.Start()
		Expect(err).NotTo(HaveOccurred())

		// Wait for server to be ready
		Eventually(func() error {
			_, err := http.Get(serverBaseURL + "/api/v1/buckets")
			return err
		}, 30*time.Second, 1*time.Second).Should(Succeed())

		logger.Info("Server started successfully", "url", serverBaseURL)

		// Create client instance
		datas3tClient = client.NewClient(serverBaseURL)
	})

	AfterEach(func(ctx SpecContext) {
		if serverCmd != nil && serverCmd.Process != nil {
			serverCmd.Process.Signal(syscall.SIGTERM)
			serverCmd.Wait()
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

	It("should complete full end-to-end workflow", func(ctx SpecContext) {
		// Step 1: Add bucket configuration
		logger.Info("Step 1: Adding bucket configuration")
		bucketInfo := &bucket.BucketInfo{
			Name:      testBucketConfigName,
			Endpoint:  minioEndpoint,
			Bucket:    testBucketName,
			AccessKey: minioAccessKey,
			SecretKey: minioSecretKey,
			UseTLS:    false,
		}

		err := datas3tClient.AddBucket(ctx, bucketInfo)
		Expect(err).NotTo(HaveOccurred())

		// Step 2: Add dataset
		logger.Info("Step 2: Adding dataset")
		datasetReq := &datas3t.AddDatas3tRequest{
			Name:   testDatasetName,
			Bucket: testBucketConfigName,
		}

		err = datas3tClient.AddDatas3t(ctx, datasetReq)
		Expect(err).NotTo(HaveOccurred())

		// Step 3: Upload first datarange (files 0-4)
		logger.Info("Step 3: Uploading first datarange")
		testData1, _ := createTestTarWithIndex(5, 0) // files 0-4

		// Create temporary file for upload
		tarFile1 := filepath.Join(tempDir, "test1.tar")
		err = os.WriteFile(tarFile1, testData1, 0644)
		Expect(err).NotTo(HaveOccurred())

		// Open file for upload
		file1, err := os.Open(tarFile1)
		Expect(err).NotTo(HaveOccurred())
		defer file1.Close()

		err = datas3tClient.UploadDataRangeFile(ctx, testDatasetName, file1, int64(len(testData1)), nil)
		Expect(err).NotTo(HaveOccurred())

		// Step 4: Upload second datarange (files 10-14) - gap between 5-9
		logger.Info("Step 4: Uploading second datarange")
		testData2, _ := createTestTarWithIndex(5, 10) // files 10-14

		// Create temporary file for upload
		tarFile2 := filepath.Join(tempDir, "test2.tar")
		err = os.WriteFile(tarFile2, testData2, 0644)
		Expect(err).NotTo(HaveOccurred())

		// Open file for upload
		file2, err := os.Open(tarFile2)
		Expect(err).NotTo(HaveOccurred())
		defer file2.Close()

		err = datas3tClient.UploadDataRangeFile(ctx, testDatasetName, file2, int64(len(testData2)), nil)
		Expect(err).NotTo(HaveOccurred())

		// Step 5: Download tar file containing both dataranges
		logger.Info("Step 5: Downloading combined dataranges")
		downloadReq := &download.PreSignDownloadForDatapointsRequest{
			Datas3tName:    testDatasetName,
			FirstDatapoint: 2,  // Start from file 2 in first datarange
			LastDatapoint:  12, // End at file 12 in second datarange
		}

		downloadResp, err := datas3tClient.PreSignDownloadForDatapoints(ctx, downloadReq)
		Expect(err).NotTo(HaveOccurred())

		// Should have segments from both dataranges
		Expect(downloadResp.DownloadSegments).To(HaveLen(2))

		// Download and process each segment separately
		expectedFiles := []string{"00000000000000000002.txt", "00000000000000000003.txt", "00000000000000000004.txt",
			"00000000000000000010.txt", "00000000000000000011.txt", "00000000000000000012.txt"}
		actualFiles := []string{}
		totalDataSize := 0

		for i, segment := range downloadResp.DownloadSegments {
			logger.Info("Downloading segment", "index", i, "range", segment.Range)

			segmentResp, err := httpGetWithRange(segment.PresignedURL, segment.Range)
			Expect(err).NotTo(HaveOccurred())
			Expect(segmentResp.StatusCode).To(Equal(http.StatusPartialContent))

			segmentData, err := io.ReadAll(segmentResp.Body)
			Expect(err).NotTo(HaveOccurred())
			segmentResp.Body.Close()

			totalDataSize += len(segmentData)

			// Step 6: Validate this segment as tar archive data
			logger.Info("Validating segment as tar archive", "segment", i, "size", len(segmentData))
			Expect(segmentData).NotTo(BeEmpty())

			// Parse this segment's tar data and verify contents
			reader := tar.NewReader(bytes.NewReader(segmentData))

			for {
				header, err := reader.Next()
				if err == io.EOF {
					break
				}
				Expect(err).NotTo(HaveOccurred())

				actualFiles = append(actualFiles, header.Name)

				// Read and validate content
				content, err := io.ReadAll(reader)
				Expect(err).NotTo(HaveOccurred())

				// Extract file number from filename (remove leading zeros and .txt extension)
				fileName := strings.TrimSuffix(header.Name, ".txt")
				// Convert filename to actual number for content comparison
				fileNum := strings.TrimLeft(fileName, "0")
				if fileNum == "" {
					fileNum = "0" // Handle case where filename is all zeros
				}
				expectedContent := fmt.Sprintf("Content of file %s", fileNum)
				Expect(string(content)).To(Equal(expectedContent))
			}
		}

		// Verify we got the expected files from all segments
		Expect(actualFiles).To(ConsistOf(expectedFiles))

		logger.Info("End-to-end test completed successfully",
			"files_downloaded", len(actualFiles),
			"segments_processed", len(downloadResp.DownloadSegments),
			"total_data_size", totalDataSize)
	})
})

// httpGetWithRange performs a GET request with range header for downloading segments
func httpGetWithRange(url, rangeHeader string) (*http.Response, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", rangeHeader)

	client := &http.Client{}
	return client.Do(req)
}
