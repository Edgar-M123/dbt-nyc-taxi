
# NYC Taxi DBT Models

This project showcases how NYC Yellow Taxi data can be transformed and modelled in DBT.

This project uses **Snowflake** as the cloud database. Source yellow taxi trip data is loaded into the table `raw.taxi.yellow_tripdata`.

## Models

### Staging

The staging model `stg_yellow_trips` performs the following transformations:
* Rename column names to a consistent convention
* Convert integer unix timestamps to a SQL Timestamp data type.
* Coalesces some null values to an appropriate value.


### Intermediate

The intermediate model `int_fct_yellow_trips` performs the following transformations:
* Add surrogate key `trip_id` by hashing some columns
* Remove rows where the pickup time is after the dropoff time as these are erroneous
* Remove 'refunded' rows which are duplicate rows with negative fare amounts. Both the original row (positive amount) and refunded row (negative amount) is removed.
* Removed rows with 0 payment amount.
* Added date keys and time keys.

### Marts

The date dimension and time (minute) dimension are available in as marts.

The model `yellow_trips` joins the `int_fct_yellow_trips` data to the date and time dimensions.


## Upcoming Additions

1. Loading location dimension provided by the NYC Taxi data as seeds.


