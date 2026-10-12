-- Source tables for the customer_revenue_daily demo.
-- Keep the source separate from Supabase's public API schema.
CREATE SCHEMA IF NOT EXISTS demo_source;

CREATE TABLE IF NOT EXISTS demo_source.customers (
    customer_id BIGINT PRIMARY KEY,
    full_name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    region TEXT NOT NULL,
    signed_up_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS demo_source.transactions (
    transaction_id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES demo_source.customers (customer_id),
    occurred_at TIMESTAMPTZ NOT NULL,
    amount_aud NUMERIC(12, 2) NOT NULL CHECK (amount_aud > 0),
    status TEXT NOT NULL CHECK (status IN ('completed', 'failed'))
);

CREATE INDEX IF NOT EXISTS transactions_occurred_at_idx
    ON demo_source.transactions (occurred_at);

CREATE INDEX IF NOT EXISTS transactions_customer_id_idx
    ON demo_source.transactions (customer_id);
