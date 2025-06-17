-- name: DatasetExists :one
SELECT count(*) > 0 FROM datasets;

-- name: AllAccessConfigs :many
SELECT DISTINCT name FROM s3_buckets;
