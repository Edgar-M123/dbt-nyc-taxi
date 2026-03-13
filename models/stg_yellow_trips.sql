
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['vendorid', 'tpep_pickup_datetime', 'pulocationid']) }} as trip_id,
    {{ dbt_date.from_unixtimestamp("tpep_pickup_datetime", format="microseconds") }} as pickup_timestamp,
    {{ dbt_date.from_unixtimestamp("tpep_dropoff_datetime", format="microseconds") }} as dropoff_timestamp,
    *
EXCLUDE (tpep_pickup_datetime, tpep_dropoff_datetime)
FROM
    raw.taxi.yellow_tripdata
