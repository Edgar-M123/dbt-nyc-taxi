
select
    pickup_date_key,
    dropoff_date_key,
    pickup_time_key,
    dropoff_time_key
from {{ ref('stg_fct_yellow_trips') }}
where dropoff_time_key < pickup_time_key AND dropoff_date_key <= pickup_date_key
