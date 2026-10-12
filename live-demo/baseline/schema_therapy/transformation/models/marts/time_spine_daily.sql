{{ config(materialized='table') }}

select cast(day as date) as date_day
from generate_series(
    current_date - interval 5 year,
    current_date + interval 1 year,
    interval 1 day
) as dates(day)
