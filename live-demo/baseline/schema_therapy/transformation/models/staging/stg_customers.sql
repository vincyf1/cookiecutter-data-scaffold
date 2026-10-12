select
    customer_id,
    trim(full_name) as full_name,
    lower(trim(email)) as email,
    upper(trim(region)) as region,
    lower(trim(customer_segment)) as customer_segment,
    signed_up_at
from {{ source('lakehouse', 'raw_customers') }}
