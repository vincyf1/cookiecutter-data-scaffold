{{ config(materialized='table') }}

select
    cast(t.customer_id as varchar) || '|' || cast(t.transaction_date_utc as varchar)
        as customer_day_id,
    t.customer_id,
    t.transaction_date_utc as revenue_date,
    c.customer_type,
    c.region,
    count(*) as completed_transaction_count,
    cast(sum(t.amount_aud) as decimal(18, 2)) as revenue_aud
from {{ ref('stg_transactions') }} as t
inner join {{ ref('stg_customers') }} as c
    on t.customer_id = c.customer_id
where t.status = 'completed'
group by
    t.customer_id,
    t.transaction_date_utc,
    c.customer_type,
    c.region
