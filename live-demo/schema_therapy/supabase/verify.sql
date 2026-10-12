-- Expected after seeding an empty demo_source schema: 200 customers,
-- 4,000 transactions, zero orphan or pre-signup transactions, and 120 dates.
SELECT
    (SELECT count(*) FROM demo_source.customers) AS customer_count,
    (SELECT count(*) FROM demo_source.transactions) AS transaction_count,
    (SELECT count(*) FROM demo_source.transactions AS t
     LEFT JOIN demo_source.customers AS c USING (customer_id)
     WHERE c.customer_id IS NULL) AS orphan_transaction_count,
    (SELECT count(*) FROM demo_source.transactions AS t
     JOIN demo_source.customers AS c USING (customer_id)
     WHERE t.occurred_at < c.signed_up_at) AS pre_signup_transaction_count,
    (SELECT count(*) FROM demo_source.customers
     WHERE customer_segment IS NULL) AS missing_segment_count,
    (SELECT count(DISTINCT occurred_at::date) FROM demo_source.transactions)
        AS transaction_day_count,
    (SELECT min(occurred_at::date) FROM demo_source.transactions) AS first_transaction_date,
    (SELECT max(occurred_at::date) FROM demo_source.transactions) AS last_transaction_date;

-- Expected synthetic distribution: bronze 120, silver 60, gold 20.
SELECT customer_segment, count(*) AS customer_count
FROM demo_source.customers
GROUP BY customer_segment
ORDER BY customer_segment;

-- This is the metric the later dbt model can calculate from completed sales.
SELECT
    occurred_at::date AS revenue_date,
    count(*) AS completed_transaction_count,
    sum(amount_aud) AS revenue_aud
FROM demo_source.transactions
WHERE status = 'completed'
GROUP BY occurred_at::date
ORDER BY revenue_date DESC
LIMIT 7;
