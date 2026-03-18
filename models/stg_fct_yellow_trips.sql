
{{ config(materialized='table') }}

with converted_timestamps as (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['vendorid', 'tpep_pickup_datetime','tpep_dropoff_datetime', 'pulocationid', 'dolocationid']) }} as trip_id,
        {{ dbt_date.from_unixtimestamp("tpep_pickup_datetime", format="microseconds") }} as pickup_timestamp,
        {{ dbt_date.from_unixtimestamp("tpep_dropoff_datetime", format="microseconds") }} as dropoff_timestamp,
        *
    EXCLUDE (tpep_pickup_datetime, tpep_dropoff_datetime)
    FROM
        {{ source("raw_taxi", "yellow_tripdata") }}
    WHERE pickup_timestamp <= dropoff_timestamp
),

extracted_dates as (
    SELECT
        to_number( to_char( cast(pickup_timestamp as date), 'YYYYMMDD')) as pickup_date_key,
        to_number( to_char( cast(dropoff_timestamp as date), 'YYYYMMDD')) as dropoff_date_key,
        date_part(HOUR, pickup_timestamp) * 100 + date_part(MINUTE, pickup_timestamp) as pickup_time_key,
        date_part(HOUR, dropoff_timestamp) * 100 + date_part(MINUTE, dropoff_timestamp) as dropoff_time_key,
        *
    EXCLUDE (pickup_timestamp, dropoff_timestamp)
    FROM
        converted_timestamps
),

-- trips with 2 entries with a negative and positive fare amount
refunded_trips as (
    select
        trip_id,
        COUNT(trip_id),
        SUM(fare_amount)
    from extracted_dates
    GROUP BY trip_id
    HAVING SUM(fare_amount) = 0 AND COUNT(trip_id) = 2
),

exclude_refunded_trips as (
    select 
        *,
        ABS(fare_amount) as abs_fare_amount
    from extracted_dates
    where
        trip_id not in (select trip_id from refunded_trips)
        and fare_amount != 0
),

-- remove some duplicates with slightly different distances / fare amounts by getting the row with the largest distance
get_largest_distance as (
    select * from exclude_refunded_trips
    where (trip_id, trip_distance) in (
        select
            trip_id,
            MAX(trip_distance) as max_distance,
        from exclude_refunded_trips
        GROUP BY trip_id
    )
)

select * from get_largest_distance
