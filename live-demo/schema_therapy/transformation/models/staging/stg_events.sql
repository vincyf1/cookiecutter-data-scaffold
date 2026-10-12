select
    id,
    name
from
    {{ source('lakehouse', 'events') }}
