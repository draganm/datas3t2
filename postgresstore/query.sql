-- name: DatasetExists :one
SELECT count(*) > 0 FROM datasets;

-- name: AllAccessConfigs :many
SELECT DISTINCT access_config FROM s3_buckets;
