with trips as (

    select * from {{ ref('int_yellow_trips_cleaned') }}

),

dim_date as (

    select * from {{ ref('dim_date') }}

),

dim_minute as (

    select * from {{ ref('dim_minute') }}

)

select
    t.trip_id,
    t.vendor_id,
    t.pickup_timestamp,
    t.dropoff_timestamp,
    t.passenger_count,
    t.trip_distance_miles,
    t.rate_code_id,
    t.store_and_fwd_flag,
    t.pickup_location_id,
    t.dropoff_location_id,
    t.payment_type,
    t.fare_amount,
    t.abs_fare_amount,
    t.extra_charges_amount,
    t.mta_tax_amount,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge_amount,
    t.congestion_surcharge_amount,
    t.airport_fee_amount,
    t.cbd_congestion_fee,
    t.total_amount,

    -- pickup dimension attributes
    t.pickup_date_key,
    t.pickup_time_key,
    pd.month_of_year as pickup_month_num,
    pd.month_name as pickup_month_name,
    pd.year_number as pickup_year_num,
    pm.hour as pickup_hour,
    pm.am_pm as pickup_am_pm,

    -- dropoff dimension attributes
    t.dropoff_date_key,
    t.dropoff_time_key,
    dd.month_of_year as dropoff_month_num,
    dd.month_name as dropoff_month_name,
    dd.year_number as dropoff_year_num,
    dm.hour as dropoff_hour,
    dm.am_pm as dropoff_am_pm

from trips t
left join dim_date pd
    on t.pickup_date_key = pd.date_key
left join dim_minute pm
    on t.pickup_time_key = pm.time_key
left join dim_date dd
    on t.dropoff_date_key = dd.date_key
left join dim_minute dm
    on t.dropoff_time_key = dm.time_key
