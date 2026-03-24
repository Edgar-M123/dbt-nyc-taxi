
with source as (
    select * from {{ source("raw_taxi", "yellow_tripdata") }}
),

renamed as (
    SELECT
        vendorid as vendor_id,
        {{ dbt_date.from_unixtimestamp("tpep_pickup_datetime", format="microseconds") }} as pickup_timestamp,
        {{ dbt_date.from_unixtimestamp("tpep_dropoff_datetime", format="microseconds") }} as dropoff_timestamp,
        passenger_count,
        trip_distance as trip_distance_miles,
        ratecodeid as rate_code_id,
        store_and_fwd_flag,
        pulocationid as pickup_location_id,
        dolocationid as dropoff_location_id,
        payment_type,
        COALESCE(fare_amount, 0) as fare_amount,
        COALESCE(extra, 0) as extra_charges_amount,
        COALESCE(mta_tax, 0) as mta_tax_amount,
        COALESCE(tip_amount, 0) as tip_amount,
        COALESCE(tolls_amount, 0) as tolls_amount,
        COALESCE(improvement_surcharge, 0) as improvement_surcharge_amount,
        COALESCE(congestion_surcharge, 0) as congestion_surcharge_amount,
        COALESCE(airport_fee, 0) as airport_fee_amount,
        COALESCE(cbd_congestion_fee, 0) as cbd_congestion_fee,
        COALESCE(
            total_amount,
            (fare_amount + extra + mta_tax + tip_amount + tolls_amount + improvement_surcharge + congestion_surcharge + airport_fee + cbd_congestion_fee)
        ) as total_amount
    FROM source
    WHERE pickup_timestamp <= dropoff_timestamp
)

select * from renamed
