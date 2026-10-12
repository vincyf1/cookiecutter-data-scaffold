-- Every completed source sale must contribute to exactly one customer/day row.
with expected as (
    select
        customer_id,
        cast(occurred_at at time zone 'UTC' as date) as revenue_date,
        count(*) as completed_transaction_count,
        sum(amount_aud) as revenue_aud
    from {{ source('lakehouse', 'raw_transactions') }}
    where status = 'completed'
    group by customer_id, cast(occurred_at at time zone 'UTC' as date)
), actual as (
    select customer_id, revenue_date, completed_transaction_count, revenue_aud
    from {{ ref('customer_revenue_daily') }}
)
select
    coalesce(expected.customer_id, actual.customer_id) as customer_id,
    coalesce(expected.revenue_date, actual.revenue_date) as revenue_date
from expected
full outer join actual
    on expected.customer_id = actual.customer_id
    and expected.revenue_date = actual.revenue_date
where expected.customer_id is null
    or actual.customer_id is null
    or expected.completed_transaction_count <> actual.completed_transaction_count
    or expected.revenue_aud <> actual.revenue_aud
