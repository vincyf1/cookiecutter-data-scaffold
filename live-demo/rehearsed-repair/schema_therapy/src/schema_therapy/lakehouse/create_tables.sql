CREATE TABLE IF NOT EXISTS events (
    id BIGINT,
    name VARCHAR,
    ingested_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw_customers (
    customer_id BIGINT PRIMARY KEY,
    full_name VARCHAR NOT NULL,
    email VARCHAR NOT NULL,
    region VARCHAR NOT NULL,
    signed_up_at TIMESTAMPTZ NOT NULL,
    customer_type VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_transactions (
    transaction_id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    amount_aud DECIMAL(12, 2) NOT NULL,
    status VARCHAR NOT NULL
);
