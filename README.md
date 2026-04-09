
# NYC Taxi DBT Models

This project showcases how NYC Yellow Taxi data can be transformed and modelled in DBT.

This project uses **DuckDB** as the database. Source yellow taxi trip data is loaded into the table `raw.taxi.yellow_tripdata`.

## Getting Started

The data was initially downloaded from the [ NYC Taxi Trip Data site ](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) as a .parquet file.
It was loaded into DuckDB with the following commands:

```sql
CREATE SCHEMA taxi;

CREATE TABLE yellow_tripdata as
SELECT * FROM read_parquet('path/to/file.parquet');

```

## Models

### Staging

The staging model `stg_yellow_trips` performs the following transformations:
* Rename column names to a consistent convention

### Intermediate

The intermediate model `int_yellow_trips_deduplicated` performs the following transformations:
* Add surrogate key `trip_id` by hashing some columns
* Remove rows where the pickup time is after the dropoff time as these are erroneous
* Remove 'refunded' rows which are duplicate rows with negative fare amounts. Both the original row (positive amount) and refunded row (negative amount) is removed.
* Removed rows with 0 payment amount.
* Added date keys and time keys.

### Marts

The date dimension and time (minute) dimension are available in as marts.

The model `yellow_trips` joins the `int_yellow_trips_deduplicated` data to the date and time dimensions.


## Upcoming Additions

1. Loading location dimension provided by the NYC Taxi data as seeds.


