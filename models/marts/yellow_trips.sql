
with yellow_trips as (
    select * from {{ ref('int_yellow_trips_deduplicated') }}
),

dim_date as (
    select * from {{ ref('dim_date') }}
),

dim_minute as (
    select * from {{ ref('dim_minute') }}
),

zone_data as (
    select * from {{ ref('taxi_zone_lookup') }}
)

select
    t.*,
    pz.borough as pickup_borough,
    pz.zone as pickup_zone,
    dz.borough as dropoff_borough,
    dz.zone as dropoff_zone,
    d.month_of_year as pickup_month_num,
    d.month_name as pickup_month_name,
    d.year_number as pickup_year_num,
    m.hour as pickup_hour,
    m.am_pm as pickup_am_pm
from yellow_trips t
LEFT JOIN
    dim_date d
    ON t.pickup_date_key = d.date_key
LEFT JOIN
    dim_minute m
    ON t.pickup_time_key = m.time_key
LEFT JOIN
    zone_data pz
    ON t.pickup_location_id = pz.locationid
LEFT JOIN
    zone_data dz
    ON t.dropoff_location_id = dz.locationid


