-- name: DatasetExists :one
SELECT count(*) > 0
FROM datasets;

-- name: AllDatasets :many
SELECT name
FROM datasets;

-- name: ListDatas3ts :many
SELECT 
    d.name as dataset_name,
    s.name as bucket_name,
    COALESCE(COUNT(dr.id), 0) as datarange_count,
    COALESCE(SUM(dr.max_datapoint_key - dr.min_datapoint_key + 1), 0) as total_datapoints,
    COALESCE(MIN(dr.min_datapoint_key), 0) as lowest_datapoint,
    COALESCE(MAX(dr.max_datapoint_key), 0) as highest_datapoint,
    COALESCE(SUM(dr.size_bytes), 0) as total_bytes
FROM datasets d
JOIN s3_buckets s ON d.s3_bucket_id = s.id
LEFT JOIN dataranges dr ON d.id = dr.dataset_id
GROUP BY d.id, d.name, s.name
ORDER BY d.name;

-- name: BucketExists :one
SELECT count(*) > 0
FROM s3_buckets
WHERE name = $1;

-- name: AllAccessConfigs :many
SELECT DISTINCT name
FROM s3_buckets;

-- name: ListAllBuckets :many
SELECT name, endpoint, bucket, use_tls
FROM s3_buckets
ORDER BY name;

-- name: GetDatasetWithBucket :one
SELECT d.id, d.name, d.s3_bucket_id, 
       s.endpoint, s.bucket, s.access_key, s.secret_key, s.use_tls
FROM datasets d
JOIN s3_buckets s ON d.s3_bucket_id = s.id
WHERE d.name = $1;

-- name: CheckDatarangeOverlap :one
SELECT count(*) > 0
FROM dataranges
WHERE dataset_id = $1
  AND min_datapoint_key < $2
  AND max_datapoint_key >= $3;

-- name: CreateDatarange :one
INSERT INTO dataranges (dataset_id, data_object_key, index_object_key, min_datapoint_key, max_datapoint_key, size_bytes)
VALUES (@dataset_id, @data_object_key, @index_object_key, @min_datapoint_key, @max_datapoint_key, @size_bytes)
RETURNING id;

-- name: CreateDatarangeUpload :one
INSERT INTO datarange_uploads (
    datarange_id, 
    upload_id,
    data_object_key,
    index_object_key,
    first_datapoint_index, 
    number_of_datapoints, 
    data_size
)
VALUES (@datarange_id, @upload_id, @data_object_key, @index_object_key, @first_datapoint_index, @number_of_datapoints, @data_size)
RETURNING id;

-- name: GetDatarangeUploadWithDetails :one
SELECT 
    du.id, 
    du.datarange_id, 
    du.upload_id, 
    du.first_datapoint_index, 
    du.number_of_datapoints, 
    du.data_size,
    dr.data_object_key, 
    dr.index_object_key,
    dr.dataset_id,
    d.name as dataset_name, 
    d.s3_bucket_id,
    s.endpoint, 
    s.bucket, 
    s.access_key, 
    s.secret_key, 
    s.use_tls
FROM datarange_uploads du
JOIN dataranges dr ON du.datarange_id = dr.id  
JOIN datasets d ON dr.dataset_id = d.id
JOIN s3_buckets s ON d.s3_bucket_id = s.id
WHERE du.id = $1;

-- name: ScheduleKeyForDeletion :exec
INSERT INTO keys_to_delete (presigned_delete_url, delete_after)
VALUES ($1, $2);

-- name: DeleteDatarangeUpload :exec
DELETE FROM datarange_uploads WHERE id = $1;

-- name: DeleteDatarange :exec
DELETE FROM dataranges WHERE id = $1;

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

-- name: AddDatarangeUpload :one
INSERT INTO datarange_uploads (datarange_id, first_datapoint_index, number_of_datapoints, data_size)
SELECT dr.id, @first_datapoint_index, @number_of_datapoints, @data_size
FROM dataranges dr
JOIN datasets d ON dr.dataset_id = d.id
WHERE d.name = @datas3t_name
  AND dr.data_object_key = @data_object_key
RETURNING id;

-- name: GetAllDataranges :many
SELECT id, dataset_id, min_datapoint_key, max_datapoint_key, size_bytes
FROM dataranges;

-- name: GetAllDatarangeUploads :many
SELECT id, datarange_id, upload_id, first_datapoint_index, number_of_datapoints, data_size
FROM datarange_uploads;

-- name: GetDatarangeFields :many
SELECT min_datapoint_key, max_datapoint_key, size_bytes
FROM dataranges;

-- name: GetDatarangeUploadIDs :many
SELECT upload_id
FROM datarange_uploads;

-- name: CountDataranges :one
SELECT count(*)
FROM dataranges;

-- name: CountDatarangeUploads :one
SELECT count(*)
FROM datarange_uploads;

-- name: CountKeysToDelete :one
SELECT count(*)
FROM keys_to_delete;

-- name: GetDatarangesForDatapoints :many
SELECT 
    dr.id,
    dr.data_object_key,
    dr.index_object_key,
    dr.min_datapoint_key,
    dr.max_datapoint_key,
    dr.size_bytes,
    d.name as dataset_name,
    s.endpoint,
    s.bucket,
    s.access_key,
    s.secret_key,
    s.use_tls
FROM dataranges dr
JOIN datasets d ON dr.dataset_id = d.id
JOIN s3_buckets s ON d.s3_bucket_id = s.id
WHERE d.name = $1
  AND dr.min_datapoint_key <= $2  -- datarange starts before or at our last datapoint
  AND dr.max_datapoint_key >= $3  -- datarange ends after or at our first datapoint
ORDER BY dr.min_datapoint_key;