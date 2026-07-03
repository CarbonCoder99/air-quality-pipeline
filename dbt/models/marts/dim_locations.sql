/*
dim_locations
--------------
Dimension table for monitoring stations.

Joins location data from our staging model with the countries
seed to enrich with continent and region information that
OpenAQ does not provide.

One row per monitoring station.
Analysts join this to fct_daily_measurements on location_id
to get station name, country, continent, and coordinates.
*/

with locations as (

    select * from {{ ref('stg_locations') }}

),

countries as (

    -- ref() to a seed works exactly like ref() to a model
    select * from {{ ref('countries') }}

),

enriched as (

    select
        -- Station identifiers
        l.location_id,
        l.location_name,

        -- Country info from staging
        l.country_iso,
        l.country_name,

        -- Enriched from seed — OpenAQ does not provide these
        c.continent,
        c.region,

        -- Geography
        l.latitude,
        l.longitude,
        l.timezone,

        -- Classify station type based on available data
        case
            when l.latitude  is null
              or l.longitude is null
            then 'Unknown Location'
            else 'Fixed Station'
        end as station_type

    from locations l
    left join countries c
        on l.country_iso = c.iso_code

)

select * from enriched
