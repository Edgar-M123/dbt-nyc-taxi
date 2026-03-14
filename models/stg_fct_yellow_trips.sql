
{{ config(materialized='table') }}

with converted_timestamps as (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['vendorid', 'tpep_pickup_datetime', 'pulocationid']) }} as trip_id,
        {{ dbt_date.from_unixtimestamp("tpep_pickup_datetime", format="microseconds") }} as pickup_timestamp,
        {{ dbt_date.from_unixtimestamp("tpep_dropoff_datetime", format="microseconds") }} as dropoff_timestamp,
        *
    EXCLUDE (tpep_pickup_datetime, tpep_dropoff_datetime)
    FROM
        {{ source("raw_taxi", "yellow_tripdata") }}

),

extracted_dates as (
    SELECT
        to_number( to_char( cast(pickup_timestamp as date), 'YYYYMMDD')) as pickup_date_key,
        to_number( to_char( cast(dropoff_timestamp as date), 'YYYYMMDD')) as dropoff_date_key,
        *
    EXCLUDE (pickup_timestamp, dropoff_timestamp)
    FROM
        converted_timestamps
)

select * from extracted_dates
