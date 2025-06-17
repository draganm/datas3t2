-- name: DatasetExists :one
SELECT count(*) > 0
FROM datasets;

-- name: AllDatasets :many
SELECT name
FROM datasets;

-- name: BucketExists :one
SELECT count(*) > 0
FROM s3_buckets
WHERE name = $1;

-- name: AllAccessConfigs :many
SELECT DISTINCT name
FROM s3_buckets;
-- name: AddBucket :exec
INSERT INTO s3_buckets (
        name,
        endpoint,
        bucket,
        access_key,
        secret_key,
        use_tls
    )
VALUES ($1, $2, $3, $4, $5, $6);

-- name: AddDatas3t :exec
INSERT INTO datasets (name, s3_bucket_id) 
SELECT @dataset_name, id 
FROM s3_buckets 
WHERE s3_buckets.name = @bucket_name;