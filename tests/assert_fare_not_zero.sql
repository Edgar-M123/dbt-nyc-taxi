-- Asserts that no trips in the cleaned intermediate model have a zero fare.
-- Zero-fare trips should have been filtered out during the cleaning step.

select
    trip_id,
    fare_amount
from {{ ref('int_yellow_trips_cleaned') }}
where fare_amount = 0
