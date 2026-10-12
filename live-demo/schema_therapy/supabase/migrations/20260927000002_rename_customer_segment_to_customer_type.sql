-- Demo trigger: adopt the upstream name before downstream consumers are updated.
-- Run this once after the baseline Airflow DAG run has succeeded.
BEGIN;

ALTER TABLE demo_source.customers
    RENAME COLUMN customer_segment TO customer_type;

COMMIT;
