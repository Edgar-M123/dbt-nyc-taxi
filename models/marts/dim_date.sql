
with dates as ({{ dbt_date.get_date_dimension("2020-01-01", "2030-01-01") }})

select
    cast(strftime(date_day, '%Y%m%d') as integer) as date_key,
    *
from dates
