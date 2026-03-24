with minute_timestamps as (

    {{ dbt_date.get_base_dates(start_date="2025-01-01", end_date="2025-01-02", datepart="minute") }}

),

dim_minutes as (

    select
        hour(date_minute) * 100 + minute(date_minute) as time_key,
        hour(date_minute) as hour,
        minute(date_minute) as minute,
        case
            when hour(date_minute) > 12 then hour(date_minute) - 12
            when hour(date_minute) = 0 then 12
            else hour(date_minute)
        end as hour_12_hr,
        case
            when hour(date_minute) >= 12 then 'PM'
            else 'AM'
        end as am_pm,
        concat(
            lpad(to_char(
                case
                    when hour(date_minute) > 12 then hour(date_minute) - 12
                    when hour(date_minute) = 0 then 12
                    else hour(date_minute)
                end
            ), 2, '0'),
            ':',
            lpad(to_char(minute(date_minute)), 2, '0'),
            ' ',
            case
                when hour(date_minute) >= 12 then 'PM'
                else 'AM'
            end
        ) as time_label_12_hr,
        concat(
            lpad(to_char(hour(date_minute)), 2, '0'),
            ':',
            lpad(to_char(minute(date_minute)), 2, '0')
        ) as time_label_24_hr
    from minute_timestamps

)

select * from dim_minutes
