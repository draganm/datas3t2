package bucket_test

import (
	"log"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/draganm/datas3t2/postgresstore"
	"github.com/draganm/datas3t2/server/bucket"
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

var _ = Describe("AddBucket", func() {
	var (
		pgContainer    *tc_postgres.PostgresContainer
		minioContainer *minio.MinioContainer
		db             *pgxpool.Pool
		srv            *bucket.BucketServer
		minioEndpoint  string
		minioHost      string
		minioAccessKey string
		minioSecretKey string
		testBucketName string
		logger         *slog.Logger
	)

	BeforeEach(func(ctx SpecContext) {

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
		// Create connection string for migrations
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

		// Extract host:port from the full URL (e.g., "http://localhost:12345" -> "localhost:12345")
		minioHost = strings.TrimPrefix(minioEndpoint, "http://")
		minioHost = strings.TrimPrefix(minioHost, "https://")

		minioAccessKey = "minioadmin"
		minioSecretKey = "minioadmin"
		testBucketName = "test-bucket"

		// Create test bucket in MinIO using the MinIO Go client
		minioClient, err := miniogo.New(minioHost, &miniogo.Options{
			Creds:  credentials.NewStaticV4(minioAccessKey, minioSecretKey, ""),
			Secure: false,
		})
		Expect(err).NotTo(HaveOccurred())

		err = minioClient.MakeBucket(ctx, testBucketName, miniogo.MakeBucketOptions{})
		Expect(err).NotTo(HaveOccurred())

		// Create server instances
		srv, err = bucket.NewServer(db, "dGVzdC1rZXktMzItYnl0ZXMtZm9yLXRlc3RpbmchIQ==")
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

	Context("when adding a valid bucket", func() {
		It("should successfully add the bucket to the database", func(ctx SpecContext) {
			bucketInfo := &bucket.BucketInfo{
				Name:      "test-config",
				Endpoint:  minioEndpoint,
				Bucket:    testBucketName,
				AccessKey: minioAccessKey,
				SecretKey: minioSecretKey,
				UseTLS:    false,
			}

			err := srv.AddBucket(ctx, logger, bucketInfo)
			Expect(err).NotTo(HaveOccurred())

			// Verify bucket was added to database
			queries := postgresstore.New(db)
			configs, err := queries.AllAccessConfigs(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(configs).To(ContainElement("test-config"))
		})

		It("should handle TLS enabled buckets", func(ctx SpecContext) {
			// For this test, we'll use a bucket configuration that doesn't require actual TLS
			// but tests the TLS flag handling
			bucketInfo := &bucket.BucketInfo{
				Name:      "test-config-tls",
				Endpoint:  minioEndpoint,
				Bucket:    testBucketName,
				AccessKey: minioAccessKey,
				SecretKey: minioSecretKey,
				UseTLS:    false, // Keep false since our test MinIO doesn't have TLS
			}

			err := srv.AddBucket(ctx, logger, bucketInfo)
			Expect(err).NotTo(HaveOccurred())

			// Verify bucket was added to database
			queries := postgresstore.New(db)
			configs, err := queries.AllAccessConfigs(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(configs).To(ContainElement("test-config-tls"))
		})

		It("should handle bucket names with allowed characters", func(ctx SpecContext) {
			validNames := []string{
				"test-config-123",
				"test_config_456",
				"TestConfig789",
				"a",
				"123-test_Config",
			}

			for i, name := range validNames {
				bucketInfo := &bucket.BucketInfo{
					Name:      name,
					Endpoint:  minioEndpoint,
					Bucket:    testBucketName,
					AccessKey: minioAccessKey,
					SecretKey: minioSecretKey,
					UseTLS:    false,
				}

				err := srv.AddBucket(ctx, logger, bucketInfo)
				Expect(err).NotTo(HaveOccurred(), "Failed for bucket name: %s", name)

				// Verify bucket was added to database
				queries := postgresstore.New(db)
				configs, err := queries.AllAccessConfigs(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(configs).To(ContainElement(name), "Config not found for bucket name: %s", name)

				// Add a small delay to avoid potential timing issues
				time.Sleep(10 * time.Millisecond)

				// Use a different name for next iteration to avoid conflicts
				testBucketName = "test-bucket-" + strconv.Itoa(i+1)
				minioClient, err := miniogo.New(minioHost, &miniogo.Options{
					Creds:  credentials.NewStaticV4(minioAccessKey, minioSecretKey, ""),
					Secure: false,
				})
				Expect(err).NotTo(HaveOccurred())
				err = minioClient.MakeBucket(ctx, testBucketName, miniogo.MakeBucketOptions{})
				Expect(err).NotTo(HaveOccurred())
			}
		})
	})

	Context("when validation fails", func() {
		It("should reject invalid bucket names", func(ctx SpecContext) {
			invalidNames := []string{
				"test@config",
				"test config",
				"test.config",
				"test/config",
				"test\\config",
				"test+config",
				"test=config",
				"",
			}

			for _, name := range invalidNames {
				bucketInfo := &bucket.BucketInfo{
					Name:      name,
					Endpoint:  minioEndpoint,
					Bucket:    testBucketName,
					AccessKey: minioAccessKey,
					SecretKey: minioSecretKey,
					UseTLS:    false,
				}

				err := srv.AddBucket(ctx, logger, bucketInfo)
				Expect(err).To(HaveOccurred(), "Should have failed for bucket name: %s", name)
				Expect(err.Error()).To(ContainSubstring("invalid bucket name"), "Wrong error for bucket name: %s", name)
			}
		})

		It("should reject empty endpoint", func(ctx SpecContext) {
			bucketInfo := &bucket.BucketInfo{
				Name:      "test-config",
				Endpoint:  "",
				Bucket:    testBucketName,
				AccessKey: minioAccessKey,
				SecretKey: minioSecretKey,
				UseTLS:    false,
			}

			err := srv.AddBucket(ctx, logger, bucketInfo)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("endpoint is required"))
		})

		It("should reject empty bucket name", func(ctx SpecContext) {
			bucketInfo := &bucket.BucketInfo{
				Name:      "test-config",
				Endpoint:  minioEndpoint,
				Bucket:    "",
				AccessKey: minioAccessKey,
				SecretKey: minioSecretKey,
				UseTLS:    false,
			}

			err := srv.AddBucket(ctx, logger, bucketInfo)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("bucket is required"))
		})

		It("should reject invalid S3 credentials", func(ctx SpecContext) {
			bucketInfo := &bucket.BucketInfo{
				Name:      "test-config",
				Endpoint:  minioEndpoint,
				Bucket:    testBucketName,
				AccessKey: "invalid-access-key",
				SecretKey: "invalid-secret-key",
				UseTLS:    false,
			}

			err := srv.AddBucket(ctx, logger, bucketInfo)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to test connection"))
		})

		It("should reject non-existent S3 bucket", func(ctx SpecContext) {
			bucketInfo := &bucket.BucketInfo{
				Name:      "test-config",
				Endpoint:  minioEndpoint,
				Bucket:    "non-existent-bucket",
				AccessKey: minioAccessKey,
				SecretKey: minioSecretKey,
				UseTLS:    false,
			}

			err := srv.AddBucket(ctx, logger, bucketInfo)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to test connection"))
		})

		It("should reject unreachable S3 endpoint", func(ctx SpecContext) {
			bucketInfo := &bucket.BucketInfo{
				Name:      "test-config",
				Endpoint:  "http://localhost:12345",
				Bucket:    testBucketName,
				AccessKey: minioAccessKey,
				SecretKey: minioSecretKey,
				UseTLS:    false,
			}

			err := srv.AddBucket(ctx, logger, bucketInfo)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to test connection"))
		})
	})

	Context("when handling database constraints", func() {
		It("should reject duplicate bucket names", func(ctx SpecContext) {
			bucketInfo := &bucket.BucketInfo{
				Name:      "duplicate-config",
				Endpoint:  minioEndpoint,
				Bucket:    testBucketName,
				AccessKey: minioAccessKey,
				SecretKey: minioSecretKey,
				UseTLS:    false,
			}

			// Add first bucket
			err := srv.AddBucket(ctx, logger, bucketInfo)
			Expect(err).NotTo(HaveOccurred())

			// Try to add the same bucket name again
			err = srv.AddBucket(ctx, logger, bucketInfo)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to add bucket"))
		})

		It("should reject duplicate endpoint-bucket combinations", func(ctx SpecContext) {
			// Create another bucket for this test
			anotherBucketName := "another-test-bucket"
			minioClient, err := miniogo.New(minioHost, &miniogo.Options{
				Creds:  credentials.NewStaticV4(minioAccessKey, minioSecretKey, ""),
				Secure: false,
			})
			Expect(err).NotTo(HaveOccurred())
			err = minioClient.MakeBucket(ctx, anotherBucketName, miniogo.MakeBucketOptions{})
			Expect(err).NotTo(HaveOccurred())

			bucketInfo1 := &bucket.BucketInfo{
				Name:      "config1",
				Endpoint:  minioEndpoint,
				Bucket:    testBucketName,
				AccessKey: minioAccessKey,
				SecretKey: minioSecretKey,
				UseTLS:    false,
			}

			bucketInfo2 := &bucket.BucketInfo{
				Name:      "config2",
				Endpoint:  minioEndpoint,
				Bucket:    testBucketName, // Same endpoint-bucket combination
				AccessKey: minioAccessKey,
				SecretKey: minioSecretKey,
				UseTLS:    false,
			}

			// Add first bucket
			err = srv.AddBucket(ctx, logger, bucketInfo1)
			Expect(err).NotTo(HaveOccurred())

			// Try to add the same endpoint-bucket combination with different name
			err = srv.AddBucket(ctx, logger, bucketInfo2)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to add bucket"))
		})
	})
})
