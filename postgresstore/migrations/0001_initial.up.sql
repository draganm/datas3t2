CREATE TABLE IF NOT EXISTS datasets (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dataranges (
    id SERIAL PRIMARY KEY,
    dataset_name TEXT NOT NULL,
    object_key TEXT NOT NULL,
    min_datapoint_key BIGINT NOT NULL,
    max_datapoint_key BIGINT NOT NULL,
    size_bytes BIGINT NOT NULL,
    FOREIGN KEY (dataset_name) REFERENCES datasets(name) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS keys_to_delete (
    id SERIAL PRIMARY KEY,
    key TEXT NOT NULL UNIQUE,
    delete_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_keys_to_delete_delete_at ON keys_to_delete(delete_at);
CREATE INDEX IF NOT EXISTS idx_dataranges_dataset_name ON dataranges(dataset_name);
CREATE INDEX IF NOT EXISTS idx_dataranges_key_range ON dataranges(min_datapoint_key, max_datapoint_key);
