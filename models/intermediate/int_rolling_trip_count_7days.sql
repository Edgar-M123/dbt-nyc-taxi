

with grouped as (
    select distinct
        pickup_location_id,
        pickup_date_key,
        count(*) as trip_volume,
        -- count(*) over (partition by pickup_location_id order by pickup_timestamp range interval '7 day' preceding ) as rolling_count
    from {{ ref('int_yellow_trips_deduplicated') }}
    group by pickup_location_id, pickup_date_key
)

select
    pickup_location_id,
    pickup_date_key,
    trip_volume,
    sum(trip_volume) over (partition by pickup_location_id order by pickup_date_key rows 7 preceding ) as trip_volume_rolling_7days
from grouped

