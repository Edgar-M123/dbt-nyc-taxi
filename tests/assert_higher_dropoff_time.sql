-- Asserts that all trips have a dropoff time at or after the pickup time.
-- Trips where pickup_timestamp > dropoff_timestamp should have been
-- filtered out in the intermediate cleaning step.

select
    pickup_timestamp,
    dropoff_timestamp
from {{ ref('int_yellow_trips_cleaned') }}
where pickup_timestamp > dropoff_timestamp
