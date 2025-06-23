package datas3t_test

import (
	"context"
	"log"
	"log/slog"
	"strings"
	"time"

	"github.com/draganm/datas3t2/postgresstore"
	"github.com/draganm/datas3t2/server/bucket"
	"github.com/draganm/datas3t2/server/datas3t"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/minio"
	tc_postgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

var _ = Describe("AddDatas3t", func() {
	var (
		ctx                  context.Context
		cancel               context.CancelFunc
		pgContainer          *tc_postgres.PostgresContainer
		minioContainer       *minio.MinioContainer
		db                   *pgxpool.Pool
		srv                  *datas3t.AddDatas3tServer
		bucketSrv            *bucket.BucketServer
		minioEndpoint        string
		minioHost            string
		minioAccessKey       string
		minioSecretKey       string
		testBucketName       string
		testBucketConfigName string
		logger               *slog.Logger
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 300*time.Second)

		var err error

		logger = slog.New(slog.NewTextHandler(GinkgoWriter, nil))

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
		minioContainer, err = minio.Run(ctx,
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

		minioAccessKey = "minioadmin"
		minioSecretKey = "minioadmin"
		testBucketName = "test-bucket"
		testBucketConfigName = "test-bucket-config"

		// Create test bucket in MinIO
		minioClient, err := miniogo.New(minioHost, &miniogo.Options{
			Creds:  credentials.NewStaticV4(minioAccessKey, minioSecretKey, ""),
			Secure: false,
		})
		Expect(err).NotTo(HaveOccurred())

		err = minioClient.MakeBucket(ctx, testBucketName, miniogo.MakeBucketOptions{})
		Expect(err).NotTo(HaveOccurred())

		// Create server instances
		srv = datas3t.NewServer(db)
		bucketSrv = bucket.NewServer(db)

		// Add a test bucket configuration that datasets can use
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

	Context("when adding a valid dataset", func() {
		It("should successfully add the dataset to the database", func() {
			datasetReq := &datas3t.AddDatas3tRequest{
				Bucket: testBucketConfigName,
				Name:   "test-dataset",
			}

			err := srv.AddDatas3t(ctx, logger, datasetReq)
			Expect(err).NotTo(HaveOccurred())

			// Verify dataset was added to database
			queries := postgresstore.New(db)
			datasets, err := queries.AllDatasets(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(datasets).To(ContainElement("test-dataset"))
		})

		It("should handle dataset names with allowed characters", func() {
			validNames := []string{
				"test-dataset-123",
				"test_dataset_456",
				"TestDataset789",
				"a",
				"123-test_Dataset",
			}

			for _, name := range validNames {
				datasetReq := &datas3t.AddDatas3tRequest{
					Bucket: testBucketConfigName,
					Name:   name,
				}

				err := srv.AddDatas3t(ctx, logger, datasetReq)
				Expect(err).NotTo(HaveOccurred(), "Failed for dataset name: %s", name)

				// Verify dataset was added to database
				queries := postgresstore.New(db)
				datasets, err := queries.AllDatasets(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datasets).To(ContainElement(name), "Dataset not found for name: %s", name)

				// Add a small delay to avoid potential timing issues
				time.Sleep(10 * time.Millisecond)
			}
		})

		It("should allow multiple datasets for the same bucket", func() {
			datasetReq1 := &datas3t.AddDatas3tRequest{
				Bucket: testBucketConfigName,
				Name:   "test-dataset-1",
			}

			datasetReq2 := &datas3t.AddDatas3tRequest{
				Bucket: testBucketConfigName,
				Name:   "test-dataset-2",
			}

			err := srv.AddDatas3t(ctx, logger, datasetReq1)
			Expect(err).NotTo(HaveOccurred())

			err = srv.AddDatas3t(ctx, logger, datasetReq2)
			Expect(err).NotTo(HaveOccurred())

			// Verify both datasets were added to database
			queries := postgresstore.New(db)
			datasets, err := queries.AllDatasets(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(datasets).To(ContainElement("test-dataset-1"))
			Expect(datasets).To(ContainElement("test-dataset-2"))
		})
	})

	Context("when validation fails", func() {
		It("should reject invalid dataset names", func() {
			invalidNames := []string{
				"test@dataset",
				"test dataset",
				"test.dataset",
				"test/dataset",
				"test\\dataset",
				"test+dataset",
				"test=dataset",
			}

			for _, name := range invalidNames {
				datasetReq := &datas3t.AddDatas3tRequest{
					Bucket: testBucketConfigName,
					Name:   name,
				}

				err := srv.AddDatas3t(ctx, logger, datasetReq)
				Expect(err).To(HaveOccurred(), "Should have failed for dataset name: %s", name)
				Expect(err.Error()).To(ContainSubstring("name must be a valid datas3t name"), "Wrong error for dataset name: %s", name)
			}
		})

		It("should reject empty dataset name", func() {
			datasetReq := &datas3t.AddDatas3tRequest{
				Bucket: testBucketConfigName,
				Name:   "",
			}

			err := srv.AddDatas3t(ctx, logger, datasetReq)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("name is required"))
		})

		It("should reject empty bucket name", func() {
			datasetReq := &datas3t.AddDatas3tRequest{
				Bucket: "",
				Name:   "test-dataset",
			}

			err := srv.AddDatas3t(ctx, logger, datasetReq)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("bucket is required"))
		})

		It("should reject non-existent bucket", func() {
			datasetReq := &datas3t.AddDatas3tRequest{
				Bucket: "non-existent-bucket",
				Name:   "test-dataset",
			}

			err := srv.AddDatas3t(ctx, logger, datasetReq)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("bucket 'non-existent-bucket' does not exist"))
		})
	})

	Context("when handling database constraints", func() {
		It("should reject duplicate dataset names", func() {
			datasetReq := &datas3t.AddDatas3tRequest{
				Bucket: testBucketConfigName,
				Name:   "duplicate-dataset",
			}

			// Add first dataset
			err := srv.AddDatas3t(ctx, logger, datasetReq)
			Expect(err).NotTo(HaveOccurred())

			// Try to add the same dataset name again
			err = srv.AddDatas3t(ctx, logger, datasetReq)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to add datas3t"))
		})

		It("should reject duplicate dataset names even with different buckets", func() {
			// Create another test bucket configuration
			anotherBucketName := "another-test-bucket"
			anotherBucketConfigName := "another-test-bucket-config"

			// Create another bucket in MinIO
			minioClient, err := miniogo.New(minioHost, &miniogo.Options{
				Creds:  credentials.NewStaticV4(minioAccessKey, minioSecretKey, ""),
				Secure: false,
			})
			Expect(err).NotTo(HaveOccurred())
			err = minioClient.MakeBucket(ctx, anotherBucketName, miniogo.MakeBucketOptions{})
			Expect(err).NotTo(HaveOccurred())

			// Add another bucket configuration
			bucketInfo := &bucket.BucketInfo{
				Name:      anotherBucketConfigName,
				Endpoint:  minioEndpoint,
				Bucket:    anotherBucketName,
				AccessKey: minioAccessKey,
				SecretKey: minioSecretKey,
				UseTLS:    false,
			}

			err = bucketSrv.AddBucket(ctx, logger, bucketInfo)
			Expect(err).NotTo(HaveOccurred())

			datasetReq1 := &datas3t.AddDatas3tRequest{
				Bucket: testBucketConfigName,
				Name:   "same-dataset-name",
			}

			datasetReq2 := &datas3t.AddDatas3tRequest{
				Bucket: anotherBucketConfigName,
				Name:   "same-dataset-name", // Same dataset name but different bucket
			}

			// Add first dataset
			err = srv.AddDatas3t(ctx, logger, datasetReq1)
			Expect(err).NotTo(HaveOccurred())

			// Try to add the same dataset name with different bucket
			err = srv.AddDatas3t(ctx, logger, datasetReq2)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to add datas3t"))
		})
	})
})
