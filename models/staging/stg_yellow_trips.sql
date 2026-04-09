
with source as (
    select 
        *
    from {{ source("raw_taxi", "yellow_tripdata") }}
),

renamed as (
    SELECT
        vendorid as vendor_id,
        tpep_pickup_datetime as pickup_timestamp,
        tpep_dropoff_datetime as dropoff_timestamp,
        passenger_count,
        trip_distance as trip_distance_miles,
        ratecodeid as rate_code_id,
        store_and_fwd_flag,
        pulocationid as pickup_location_id,
        dolocationid as dropoff_location_id,
        payment_type,
        fare_amount,
        extra as extra_charges_amount,
        mta_tax as mta_tax_amount,
        tip_amount as tip_amount,
        tolls_amount as tolls_amount,
        improvement_surcharge as improvement_surcharge_amount,
        congestion_surcharge as congestion_surcharge_amount,
        airport_fee as airport_fee_amount,
        cbd_congestion_fee as cbd_congestion_fee,
        total_amount
    FROM source
)

select * from renamed
