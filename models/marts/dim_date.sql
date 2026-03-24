
with dates as ({{ dbt_date.get_date_dimension("2020-01-01", "2030-01-01") }})

select
    to_number(to_char(date_day, 'YYYYMMDD')) as date_key,
    *
from dates
