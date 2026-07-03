/*
stg_locations
--------------
Derives a clean, deduplicated list of monitoring stations
from the measurements data.

Why derive from measurements instead of a separate API call?
We already have location data embedded in every measurement record
that DAG 1 ingested. Deriving from there avoids needing a separate
locations ingestion pipeline.

We deduplicate using ROW_NUMBER — keeping the most recently
ingested record for each location, since location metadata
(like name or coordinates) can occasionally be updated by OpenAQ.
*/

with measurements as (

    select * from {{ ref('stg_measurements') }}

),

-- Pick one record per location — the most recently ingested
-- This handles cases where the same location appears across
-- multiple days with slightly different metadata
deduped as (

    select
        *,
        row_number() over (
            partition by location_id
            order by ingested_at desc
        ) as row_num

    from measurements

),

locations as (

    select
        location_id,
        location_name,
        country_iso,
        country_name,
        latitude,
        longitude,
        timezone
    from deduped
    where row_num = 1

)

select * from locations
