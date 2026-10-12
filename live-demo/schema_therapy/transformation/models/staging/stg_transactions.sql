select
    transaction_id,
    customer_id,
    occurred_at,
    cast(occurred_at at time zone 'UTC' as date) as transaction_date_utc,
    amount_aud,
    lower(trim(status)) as status
from {{ source('lakehouse', 'raw_transactions') }}
