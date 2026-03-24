with dates as (

    {{ dbt_date.get_date_dimension(var('date_spine_start'), var('date_spine_end')) }}

)

select
    {{ generate_date_key('date_day') }} as date_key,
    *
from dates
