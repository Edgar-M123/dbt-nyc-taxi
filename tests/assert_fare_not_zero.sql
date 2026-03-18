
select
    trip_id,
    fare_amount
from {{ ref('stg_fct_yellow_trips') }}
where fare_amount = 0
