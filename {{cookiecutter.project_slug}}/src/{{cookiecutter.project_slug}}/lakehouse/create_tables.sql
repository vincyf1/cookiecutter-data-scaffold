INSTALL iceberg;
LOAD iceberg;

CREATE TABLE IF NOT EXISTS events (
    id BIGINT,
    name VARCHAR,
    ingested_at TIMESTAMP DEFAULT now()
);
