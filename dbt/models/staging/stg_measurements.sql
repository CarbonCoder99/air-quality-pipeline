/*
stg_measurements
-----------------
Cleans and standardises raw measurements from OpenAQ.

What this model does:
  1. Casts every column to its correct data type
     (BigQuery autodetect sometimes gets types wrong)
  2. Extracts the date from the datetime string
  3. Filters out invalid readings (nulls and negatives)
  4. Renames columns to consistent snake_case
  5. Adds a loaded_at timestamp for data lineage

What this model does NOT do:
  - No aggregations
  - No joins to other tables
  - No business logic
  Staging is purely cleaning — one row in, one row out.
*/

with source as (

    select * from {{ source('air_quality_raw', 'raw_measurements') }}

),

cleaned as (

    select
        -- ── Identifiers ───────────────────────────────────────
        cast(sensor_id   as int64)   as sensor_id,
        cast(location_id as int64)   as location_id,

        -- ── Location info ─────────────────────────────────────
        trim(location_name)          as location_name,
        upper(trim(country_iso))     as country_iso,
        trim(country_name)           as country_name,
        trim(timezone)               as timezone,

        -- ── Pollutant info ────────────────────────────────────
        lower(trim(pollutant))       as pollutant,
        trim(units)                  as units,

        -- ── Measurement values ────────────────────────────────
        cast(value     as float64)   as measurement_value,
        cast(value_min as float64)   as measurement_value_min,
        cast(value_max as float64)   as measurement_value_max,

        -- ── Datetime handling ─────────────────────────────────
        -- datetime_utc comes in as an ISO 8601 string: "2024-01-15T00:00:00Z"
        -- We cast it to TIMESTAMP for proper time-based operations
        -- Then extract just the DATE for partitioning and joining
        cast(datetime_utc as timestamp)         as measured_at_utc,
        date(cast(datetime_utc as timestamp))   as measurement_date,

        -- ── Geography ─────────────────────────────────────────
        cast(latitude  as float64)   as latitude,
        cast(longitude as float64)   as longitude,

        -- ── Pipeline metadata ─────────────────────────────────
        -- When was this record loaded into our warehouse?
        -- Useful for debugging data freshness issues
        cast(ingested_at as timestamp)   as ingested_at,
        cast(data_date   as date)        as data_date,

        -- When did dbt process this record?
        current_timestamp()              as dbt_processed_at

    from source

    where
        -- Remove null measurements
        -- A null value means the sensor reported nothing — unusable
        value is not null

        -- Remove physically impossible negative readings
        -- Discovered in our exploration step — sensors malfunction
        -- and report negatives. PM2.5 cannot be negative in reality.
        and cast(value as float64) >= 0

        -- Remove extreme outliers — likely sensor errors
        -- PM2.5 > 2000 μg/m³ has never been recorded legitimately
        -- NO2 > 5000 ppb is physically impossible at ground level
        and cast(value as float64) < 5000

        -- Ensure we have a valid date
        and datetime_utc is not null

)

select * from cleaned
