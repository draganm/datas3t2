package bucket_test

import (
	"log"
	"log/slog"
	"strings"
	"time"

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

var _ = Describe("ListBuckets", func() {
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

		// Create test bucket in MinIO
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

	Context("when there are no buckets", func() {
		It("should return an empty list", func(ctx SpecContext) {
			buckets, err := srv.ListBuckets(ctx, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(buckets).To(BeEmpty())
		})
	})

	Context("when there are buckets in the database", func() {
		BeforeEach(func(ctx SpecContext) {
			// Add test buckets to the database
			bucketInfo1 := &bucket.BucketInfo{
				Name:      "bucket-config-1",
				Endpoint:  minioEndpoint,
				Bucket:    testBucketName,
				AccessKey: minioAccessKey,
				SecretKey: minioSecretKey,
				UseTLS:    false,
			}

			err := srv.AddBucket(ctx, logger, bucketInfo1)
			Expect(err).NotTo(HaveOccurred())

			// Create another test bucket for the second config
			testBucketName2 := "test-bucket-2"
			minioClient, err := miniogo.New(minioHost, &miniogo.Options{
				Creds:  credentials.NewStaticV4(minioAccessKey, minioSecretKey, ""),
				Secure: false,
			})
			Expect(err).NotTo(HaveOccurred())

			err = minioClient.MakeBucket(ctx, testBucketName2, miniogo.MakeBucketOptions{})
			Expect(err).NotTo(HaveOccurred())

			bucketInfo2 := &bucket.BucketInfo{
				Name:      "bucket-config-2",
				Endpoint:  minioEndpoint,
				Bucket:    testBucketName2,
				AccessKey: minioAccessKey,
				SecretKey: minioSecretKey,
				UseTLS:    false, // Keep false since test MinIO doesn't have TLS
			}

			err = srv.AddBucket(ctx, logger, bucketInfo2)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should return all buckets without sensitive credentials", func(ctx SpecContext) {
			buckets, err := srv.ListBuckets(ctx, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(buckets).To(HaveLen(2))

			// Find buckets by name for verification
			var bucket1, bucket2 *bucket.BucketListInfo
			for _, b := range buckets {
				if b.Name == "bucket-config-1" {
					bucket1 = b
				} else if b.Name == "bucket-config-2" {
					bucket2 = b
				}
			}

			Expect(bucket1).NotTo(BeNil())
			Expect(bucket1.Name).To(Equal("bucket-config-1"))
			Expect(bucket1.Endpoint).To(Equal(minioEndpoint))
			Expect(bucket1.Bucket).To(Equal(testBucketName))
			Expect(bucket1.UseTLS).To(BeFalse())

			Expect(bucket2).NotTo(BeNil())
			Expect(bucket2.Name).To(Equal("bucket-config-2"))
			Expect(bucket2.Endpoint).To(Equal(minioEndpoint))
			Expect(bucket2.Bucket).To(Equal("test-bucket-2"))
			Expect(bucket2.UseTLS).To(BeFalse())
		})

		It("should return buckets sorted by name", func(ctx SpecContext) {
			// Add more buckets with names that will test sorting
			bucketConfigs := []struct {
				name       string
				bucketName string
			}{
				{"aaa-config", "aaa-bucket"},
				{"zzz-config", "zzz-bucket"},
				{"mmm-config", "mmm-bucket"},
			}

			minioClient, err := miniogo.New(minioHost, &miniogo.Options{
				Creds:  credentials.NewStaticV4(minioAccessKey, minioSecretKey, ""),
				Secure: false,
			})
			Expect(err).NotTo(HaveOccurred())

			for _, config := range bucketConfigs {
				err = minioClient.MakeBucket(ctx, config.bucketName, miniogo.MakeBucketOptions{})
				Expect(err).NotTo(HaveOccurred())

				bucketInfo := &bucket.BucketInfo{
					Name:      config.name,
					Endpoint:  minioEndpoint,
					Bucket:    config.bucketName,
					AccessKey: minioAccessKey,
					SecretKey: minioSecretKey,
					UseTLS:    false,
				}

				err = srv.AddBucket(ctx, logger, bucketInfo)
				Expect(err).NotTo(HaveOccurred())
			}

			buckets, err := srv.ListBuckets(ctx, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(buckets).To(HaveLen(5)) // 2 from BeforeEach + 3 new

			// Verify sorting by name
			names := make([]string, len(buckets))
			for i, b := range buckets {
				names[i] = b.Name
			}

			expectedNames := []string{
				"aaa-config",
				"bucket-config-1",
				"bucket-config-2",
				"mmm-config",
				"zzz-config",
			}

			Expect(names).To(Equal(expectedNames))
		})

		It("should not expose sensitive credentials", func(ctx SpecContext) {
			buckets, err := srv.ListBuckets(ctx, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(buckets).To(HaveLen(2))

			// Verify that BucketListInfo struct doesn't contain sensitive fields
			for _, bucket := range buckets {
				// These should be present
				Expect(bucket.Name).NotTo(BeEmpty())
				Expect(bucket.Endpoint).NotTo(BeEmpty())
				Expect(bucket.Bucket).NotTo(BeEmpty())

				// Verify the struct type doesn't have access key or secret key fields
				// by attempting to access them (this should not compile if they exist)
				// This is more of a compile-time check, but we can verify the returned
				// bucket info doesn't accidentally include sensitive data
				bucketBytes := bucket.Name + bucket.Endpoint + bucket.Bucket
				Expect(bucketBytes).NotTo(ContainSubstring(minioAccessKey))
				Expect(bucketBytes).NotTo(ContainSubstring(minioSecretKey))
			}
		})
	})

	Context("when database connection fails", func() {
		It("should handle database errors gracefully", func(ctx SpecContext) {
			// Close the database connection to simulate failure
			db.Close()

			buckets, err := srv.ListBuckets(ctx, logger)
			Expect(err).To(HaveOccurred())
			Expect(buckets).To(BeNil())
			Expect(err.Error()).To(ContainSubstring("failed to list buckets"))
		})
	})
})
