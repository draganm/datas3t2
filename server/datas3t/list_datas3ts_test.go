package datas3t_test

import (
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

var _ = Describe("ListDatas3ts", func() {
	var (
		pgContainer          *tc_postgres.PostgresContainer
		minioContainer       *minio.MinioContainer
		db                   *pgxpool.Pool
		srv                  *datas3t.Datas3tServer
		bucketSrv            *bucket.BucketServer
		minioEndpoint        string
		minioHost            string
		minioAccessKey       string
		minioSecretKey       string
		testBucketName       string
		testBucketConfigName string
		logger               *slog.Logger
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

	Context("when no datasets exist", func() {
		It("should return an empty list", func(ctx SpecContext) {
			datas3ts, err := srv.ListDatas3ts(ctx, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(datas3ts).To(BeEmpty())
		})
	})

	Context("when datasets exist without dataranges", func() {
		BeforeEach(func(ctx SpecContext) {
			// Add some test datasets
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
		})

		It("should return datasets with zero stats", func(ctx SpecContext) {
			datas3ts, err := srv.ListDatas3ts(ctx, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(datas3ts).To(HaveLen(2))

			// Check that both datasets are present
			datasetNames := make([]string, len(datas3ts))
			for i, d := range datas3ts {
				datasetNames[i] = d.DatasetName
			}
			Expect(datasetNames).To(ContainElements("test-dataset-1", "test-dataset-2"))

			// Check that all datasets have zero stats
			for _, d := range datas3ts {
				Expect(d.BucketName).To(Equal(testBucketConfigName))
				Expect(d.DatarangeCount).To(Equal(int64(0)))
				Expect(d.TotalDatapoints).To(Equal(int64(0)))
				Expect(d.LowestDatapoint).To(Equal(int64(0)))
				Expect(d.HighestDatapoint).To(Equal(int64(0)))
				Expect(d.TotalBytes).To(Equal(int64(0)))
			}
		})

		It("should return datasets ordered by name", func(ctx SpecContext) {
			datas3ts, err := srv.ListDatas3ts(ctx, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(datas3ts).To(HaveLen(2))

			// Should be ordered by dataset name
			Expect(datas3ts[0].DatasetName).To(Equal("test-dataset-1"))
			Expect(datas3ts[1].DatasetName).To(Equal("test-dataset-2"))
		})
	})

	Context("when datasets exist with dataranges", func() {
		BeforeEach(func(ctx SpecContext) {
			// Add a test dataset
			datasetReq := &datas3t.AddDatas3tRequest{
				Bucket: testBucketConfigName,
				Name:   "test-dataset-with-data",
			}

			err := srv.AddDatas3t(ctx, logger, datasetReq)
			Expect(err).NotTo(HaveOccurred())

			// Get the dataset to add dataranges to it
			queries := postgresstore.New(db)
			dataset, err := queries.GetDatasetWithBucket(ctx, "test-dataset-with-data")
			Expect(err).NotTo(HaveOccurred())

			// Add some test dataranges
			_, err = queries.CreateDatarange(ctx, postgresstore.CreateDatarangeParams{
				DatasetID:       dataset.ID,
				DataObjectKey:   "data-1",
				IndexObjectKey:  "index-1",
				MinDatapointKey: 100,
				MaxDatapointKey: 199,
				SizeBytes:       1000,
			})
			Expect(err).NotTo(HaveOccurred())

			_, err = queries.CreateDatarange(ctx, postgresstore.CreateDatarangeParams{
				DatasetID:       dataset.ID,
				DataObjectKey:   "data-2",
				IndexObjectKey:  "index-2",
				MinDatapointKey: 200,
				MaxDatapointKey: 299,
				SizeBytes:       2000,
			})
			Expect(err).NotTo(HaveOccurred())

			_, err = queries.CreateDatarange(ctx, postgresstore.CreateDatarangeParams{
				DatasetID:       dataset.ID,
				DataObjectKey:   "data-3",
				IndexObjectKey:  "index-3",
				MinDatapointKey: 50,
				MaxDatapointKey: 149,
				SizeBytes:       1500,
			})
			Expect(err).NotTo(HaveOccurred())
		})

		It("should return correct aggregated statistics", func(ctx SpecContext) {
			datas3ts, err := srv.ListDatas3ts(ctx, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(datas3ts).To(HaveLen(1))

			d := datas3ts[0]
			Expect(d.DatasetName).To(Equal("test-dataset-with-data"))
			Expect(d.BucketName).To(Equal(testBucketConfigName))
			Expect(d.DatarangeCount).To(Equal(int64(3)))

			// Total datapoints: (199-100+1) + (299-200+1) + (149-50+1) = 100 + 100 + 100 = 300
			Expect(d.TotalDatapoints).To(Equal(int64(300)))

			// Lowest datapoint: MIN(100, 200, 50) = 50
			Expect(d.LowestDatapoint).To(Equal(int64(50)))

			// Highest datapoint: MAX(199, 299, 149) = 299
			Expect(d.HighestDatapoint).To(Equal(int64(299)))

			// Total bytes: 1000 + 2000 + 1500 = 4500
			Expect(d.TotalBytes).To(Equal(int64(4500)))
		})
	})

	Context("when multiple datasets exist with different data", func() {
		BeforeEach(func(ctx SpecContext) {
			// Add multiple test datasets
			datasets := []string{"dataset-a", "dataset-b", "dataset-c"}
			for _, name := range datasets {
				datasetReq := &datas3t.AddDatas3tRequest{
					Bucket: testBucketConfigName,
					Name:   name,
				}

				err := srv.AddDatas3t(ctx, logger, datasetReq)
				Expect(err).NotTo(HaveOccurred())
			}

			queries := postgresstore.New(db)

			// Add dataranges to dataset-a
			datasetA, err := queries.GetDatasetWithBucket(ctx, "dataset-a")
			Expect(err).NotTo(HaveOccurred())

			_, err = queries.CreateDatarange(ctx, postgresstore.CreateDatarangeParams{
				DatasetID:       datasetA.ID,
				DataObjectKey:   "data-a-1",
				IndexObjectKey:  "index-a-1",
				MinDatapointKey: 0,
				MaxDatapointKey: 99,
				SizeBytes:       500,
			})
			Expect(err).NotTo(HaveOccurred())

			// Add dataranges to dataset-c (leave dataset-b empty)
			datasetC, err := queries.GetDatasetWithBucket(ctx, "dataset-c")
			Expect(err).NotTo(HaveOccurred())

			_, err = queries.CreateDatarange(ctx, postgresstore.CreateDatarangeParams{
				DatasetID:       datasetC.ID,
				DataObjectKey:   "data-c-1",
				IndexObjectKey:  "index-c-1",
				MinDatapointKey: 1000,
				MaxDatapointKey: 1999,
				SizeBytes:       10000,
			})
			Expect(err).NotTo(HaveOccurred())

			_, err = queries.CreateDatarange(ctx, postgresstore.CreateDatarangeParams{
				DatasetID:       datasetC.ID,
				DataObjectKey:   "data-c-2",
				IndexObjectKey:  "index-c-2",
				MinDatapointKey: 2000,
				MaxDatapointKey: 2499,
				SizeBytes:       5000,
			})
			Expect(err).NotTo(HaveOccurred())
		})

		It("should return correct data for each dataset", func(ctx SpecContext) {
			datas3ts, err := srv.ListDatas3ts(ctx, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(datas3ts).To(HaveLen(3))

			// Should be ordered by dataset name
			Expect(datas3ts[0].DatasetName).To(Equal("dataset-a"))
			Expect(datas3ts[1].DatasetName).To(Equal("dataset-b"))
			Expect(datas3ts[2].DatasetName).To(Equal("dataset-c"))

			// Check dataset-a
			datasetA := datas3ts[0]
			Expect(datasetA.BucketName).To(Equal(testBucketConfigName))
			Expect(datasetA.DatarangeCount).To(Equal(int64(1)))
			Expect(datasetA.TotalDatapoints).To(Equal(int64(100))) // 99-0+1
			Expect(datasetA.LowestDatapoint).To(Equal(int64(0)))
			Expect(datasetA.HighestDatapoint).To(Equal(int64(99)))
			Expect(datasetA.TotalBytes).To(Equal(int64(500)))

			// Check dataset-b (empty)
			datasetB := datas3ts[1]
			Expect(datasetB.BucketName).To(Equal(testBucketConfigName))
			Expect(datasetB.DatarangeCount).To(Equal(int64(0)))
			Expect(datasetB.TotalDatapoints).To(Equal(int64(0)))
			Expect(datasetB.LowestDatapoint).To(Equal(int64(0)))
			Expect(datasetB.HighestDatapoint).To(Equal(int64(0)))
			Expect(datasetB.TotalBytes).To(Equal(int64(0)))

			// Check dataset-c
			datasetC := datas3ts[2]
			Expect(datasetC.BucketName).To(Equal(testBucketConfigName))
			Expect(datasetC.DatarangeCount).To(Equal(int64(2)))
			Expect(datasetC.TotalDatapoints).To(Equal(int64(1500))) // (1999-1000+1) + (2499-2000+1) = 1000 + 500
			Expect(datasetC.LowestDatapoint).To(Equal(int64(1000)))
			Expect(datasetC.HighestDatapoint).To(Equal(int64(2499)))
			Expect(datasetC.TotalBytes).To(Equal(int64(15000))) // 10000 + 5000
		})
	})
})
