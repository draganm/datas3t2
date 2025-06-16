-- name: DatasetExists :one
SELECT count(*) > 0 FROM datasets;
