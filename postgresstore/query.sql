-- name: DatasetExists :one
SELECT count(*) > 0 FROM datasets;

-- name: AllAccessConfigs :many
SELECT DISTINCT name FROM s3_buckets;


-- name: AddBucket :exec
INSERT INTO s3_buckets (name, endpoint, bucket, access_key, secret_key, use_tls) VALUES ($1, $2, $3, $4, $5, $6);