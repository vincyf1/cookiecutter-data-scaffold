-- Run after the migration. IDs and attributes are synthetic and repeatable.
-- Dates are anchored to the first day this seed is applied.
BEGIN;

INSERT INTO demo_source.customers (
    customer_id, full_name, email, region, signed_up_at, customer_segment
)
SELECT
    customer_number,
    'Demo Customer ' || lpad(customer_number::text, 3, '0'),
    'customer' || lpad(customer_number::text, 3, '0') || '@example.test',
    (ARRAY['NSW', 'VIC', 'QLD', 'WA'])[1 + (customer_number - 1) % 4],
    (CURRENT_DATE - 180 + ((customer_number - 1) % 60))::timestamptz,
    CASE
        WHEN customer_number % 10 = 0 THEN 'gold'
        WHEN customer_number % 10 IN (1, 2, 3) THEN 'silver'
        ELSE 'bronze'
    END
FROM generate_series(1, 200) AS generated(customer_number)
ON CONFLICT (customer_id) DO UPDATE
    SET customer_segment = EXCLUDED.customer_segment
    WHERE demo_source.customers.email = EXCLUDED.email;

INSERT INTO demo_source.transactions (
    transaction_id, customer_id, occurred_at, amount_aud, status
)
SELECT
    transaction_number,
    1 + ((transaction_number * 37) % 200),
    (CURRENT_DATE - 119 + ((transaction_number - 1) % 120))::timestamptz
        + make_interval(hours => (transaction_number * 7) % 24,
                        mins => (transaction_number * 13) % 60),
    ((500 + ((transaction_number * 97) % 49500))::numeric / 100)::numeric(12, 2),
    CASE WHEN transaction_number % 11 = 0 THEN 'failed' ELSE 'completed' END
FROM generate_series(1, 4000) AS generated(transaction_number)
ON CONFLICT (transaction_id) DO NOTHING;

COMMIT;
