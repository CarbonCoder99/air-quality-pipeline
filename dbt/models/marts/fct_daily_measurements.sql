/*
fct_daily_measurements
-----------------------
The central fact table of our star schema.

One row per sensor per day per pollutant.

This model is:
  - INCREMENTAL: only processes new dates, not full history
  - PARTITIONED: by measurement_date (cheap date-range queries)
  - CLUSTERED: by country_iso and pollutant (cheap filter queries)
  - TESTED: surrogate_key is unique and not null

Why incremental?
We add new data every day. Re-processing all historical data
daily would be wasteful — BigQuery charges by bytes scanned.
Incremental models only touch the new partition, keeping costs low
and runs fast.

The rolling_7d_avg window function is what makes this model
analytically powerful — it lets analysts see trends without
needing to write complex SQL themselves.
*/

{{
    config(
        materialized  = 'incremental',
        partition_by  = {
            'field':       'measurement_date',
            'data_type':   'date',
            'granularity': 'day'
        },
        cluster_by    = ['country_iso', 'pollutant'],
        unique_key    = 'surrogate_key',
        on_schema_change = 'sync_all_columns'
    )
}}

with measurements as (

    select * from {{ ref('stg_measurements') }}

    -- Incremental filter: on subsequent runs, only process
    -- the last 3 days. We use 3 days (not 1) to handle late-arriving
    -- data — OpenAQ sometimes backfills data for previous days.
    {% if is_incremental() %}
        where measurement_date >= date_sub(current_date(), interval 3 day)
    {% endif %}

),

with_keys as (

    select
        -- ── Surrogate key ──────────────────────────────────────
        -- A surrogate key uniquely identifies each row.
        -- We combine sensor_id + date + pollutant because that
        -- combination is naturally unique in our data.
        -- dbt_utils.generate_surrogate_key hashes these fields
        -- into a single string we can test for uniqueness.
        {{
            dbt_utils.generate_surrogate_key([
                'sensor_id',
                'measurement_date',
                'pollutant'
            ])
        }}                                      as surrogate_key,

        -- ── Foreign keys (join to dimensions) ─────────────────
        sensor_id,
        location_id,
        measurement_date,                       -- joins to dim_dates.date_id

        -- ── Degenerate dimensions ──────────────────────────────
        -- These could be foreign keys to dimension tables
        -- but they're simple enough to keep in the fact table
        country_iso,
        pollutant,
        units,

        -- ── Measures ──────────────────────────────────────────
        measurement_value,
        measurement_value_min,
        measurement_value_max,

        -- ── AQI Category ──────────────────────────────────────
        -- Uses our custom macro — converts raw value to health label
        -- This is business logic applied once here, not in every query
        {{
            aqi_category('pollutant', 'measurement_value')
        }}                                      as aqi_category,

        -- ── Rolling 7-day average ─────────────────────────────
        -- The most analytically useful derived metric.
        -- Shows trend direction better than daily point values
        -- which can spike due to individual events.
        --
        -- PARTITION BY sensor_id, pollutant: each sensor/pollutant
        --   combo gets its own rolling window
        -- ORDER BY measurement_date: window moves forward in time
        -- ROWS BETWEEN 6 PRECEDING AND CURRENT ROW: include today
        --   and the 6 days before = 7 day window
        avg(measurement_value) over (
            partition by sensor_id, pollutant
            order by measurement_date
            rows between 6 preceding and current row
        )                                       as rolling_7d_avg,

        -- ── Month-to-date average ─────────────────────────────
        -- Useful for monthly pollution reports
        avg(measurement_value) over (
            partition by
                sensor_id,
                pollutant,
                extract(year  from measurement_date),
                extract(month from measurement_date)
            order by measurement_date
            rows between unbounded preceding and current row
        )                                       as month_to_date_avg,

        -- ── Geography ─────────────────────────────────────────
        latitude,
        longitude,
        timezone,

        -- ── Metadata ──────────────────────────────────────────
        measured_at_utc,
        ingested_at,
        dbt_processed_at

    from measurements

)

select * from with_keys
