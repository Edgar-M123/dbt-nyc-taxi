

with minute_timestamps as (
    {{ dbt_date.get_base_dates(start_date="2025-01-01", end_date="2025-01-02", datepart="minute") }}
),

dim_minutes as (
    
    select
        hour(date_minute)*100 + minute(date_minute) as time_key,
        hour(date_minute) as hour,
        minute(date_minute) as minute,
        (case when hour > 12 then hour - 12 when hour = 0 then 12 else hour end) as hour_12_hr,
        (CASE
            WHEN hour >= 12 then 'PM'
            else 'AM'
        end) as AM_PM,
        CONCAT(LPAD(cast( hour_12_hr as text), 2, '0'), ':', LPAD( cast(minute as text), 2, '0' ), ' ', AM_PM) as time_label_12_hr,
        CONCAT(LPAD(cast( hour as text), 2, '0'), ':', LPAD( cast(minute as text), 2, '0' )) as time_label_24_hr
    from minute_timestamps
)

select *
from dim_minutes
