with trips as (

    select * from {{ ref('stg_yellow_trips') }}

),

-- add surrogate key, date/time dimension keys, and filter invalid timestamps
with_keys as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'vendor_id', 'pickup_timestamp', 'dropoff_timestamp',
            'pickup_location_id', 'dropoff_location_id',
            'fare_amount', 'trip_distance_miles'
        ]) }} as trip_id,
        {{ generate_date_key('pickup_timestamp') }} as pickup_date_key,
        {{ generate_date_key('dropoff_timestamp') }} as dropoff_date_key,
        {{ generate_time_key('pickup_timestamp') }} as pickup_time_key,
        {{ generate_time_key('dropoff_timestamp') }} as dropoff_time_key,
        vendor_id,
        pickup_timestamp,
        dropoff_timestamp,
        passenger_count,
        trip_distance_miles,
        rate_code_id,
        store_and_fwd_flag,
        pickup_location_id,
        dropoff_location_id,
        payment_type,
        fare_amount,
        extra_charges_amount,
        mta_tax_amount,
        tip_amount,
        tolls_amount,
        improvement_surcharge_amount,
        congestion_surcharge_amount,
        airport_fee_amount,
        cbd_congestion_fee,
        total_amount
    from trips
    where pickup_timestamp <= dropoff_timestamp

),

-- identify refunded trips: matching positive/negative fare pairs
refunded_trips as (

    select trip_id
    from with_keys
    group by trip_id
    having sum(fare_amount) = 0 and count(*) = 2

),

-- exclude refunds and zero-fare trips
valid_trips as (

    select
        wk.trip_id,
        wk.pickup_date_key,
        wk.dropoff_date_key,
        wk.pickup_time_key,
        wk.dropoff_time_key,
        wk.vendor_id,
        wk.pickup_timestamp,
        wk.dropoff_timestamp,
        wk.passenger_count,
        wk.trip_distance_miles,
        wk.rate_code_id,
        wk.store_and_fwd_flag,
        wk.pickup_location_id,
        wk.dropoff_location_id,
        wk.payment_type,
        wk.fare_amount,
        abs(wk.fare_amount) as abs_fare_amount,
        wk.extra_charges_amount,
        wk.mta_tax_amount,
        wk.tip_amount,
        wk.tolls_amount,
        wk.improvement_surcharge_amount,
        wk.congestion_surcharge_amount,
        wk.airport_fee_amount,
        wk.cbd_congestion_fee,
        wk.total_amount
    from with_keys wk
    where not exists (
        select 1 from refunded_trips rt
        where rt.trip_id = wk.trip_id
    )
    and wk.fare_amount != 0

),

-- deterministic deduplication: keep row with longest distance, highest fare as tiebreaker
deduplicated as (

    select
        *,
        row_number() over (
            partition by trip_id
            order by trip_distance_miles desc, fare_amount desc
        ) as dedup_row_num
    from valid_trips

)

select
    trip_id,
    pickup_date_key,
    dropoff_date_key,
    pickup_time_key,
    dropoff_time_key,
    vendor_id,
    pickup_timestamp,
    dropoff_timestamp,
    passenger_count,
    trip_distance_miles,
    rate_code_id,
    store_and_fwd_flag,
    pickup_location_id,
    dropoff_location_id,
    payment_type,
    fare_amount,
    abs_fare_amount,
    extra_charges_amount,
    mta_tax_amount,
    tip_amount,
    tolls_amount,
    improvement_surcharge_amount,
    congestion_surcharge_amount,
    airport_fee_amount,
    cbd_congestion_fee,
    total_amount
from deduplicated
where dedup_row_num = 1
