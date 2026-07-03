/*
dim_dates
----------
A date dimension table covering every day from
project start to today.

Why do we need this?
When you query "what was the average PM2.5 each month in 2024",
you need every month represented — even months where a sensor
had no data. Without a date dimension, those months simply
don't appear in your results. With it, they appear as NULL
or 0, which is the honest answer.

This uses dbt_utils.date_spine to generate one row per calendar day.
It is materialised as a TABLE (not a view) because it is static
reference data that never changes for past dates.
*/

{{ config(materialized='table') }}

with date_spine as (

    {{
        dbt_utils.date_spine(
            datepart   = "day",
            start_date = "cast('2024-01-01' as date)",
            end_date   = "cast(current_date() as date)"
        )
    }}

),

enriched as (

    select
        cast(date_day as date)                      as date_id,

        -- Numeric components
        extract(year    from date_day)              as year,
        extract(month   from date_day)              as month,
        extract(day     from date_day)              as day_of_month,
        extract(dayofweek from date_day)            as day_of_week_num,
        extract(quarter from date_day)              as quarter,

        -- Named components
        format_date('%A', date_day)                 as day_name,
        format_date('%a', date_day)                 as day_name_short,
        format_date('%B', date_day)                 as month_name,
        format_date('%b', date_day)                 as month_name_short,

        -- Useful flags
        case
            when extract(dayofweek from date_day)
                 in (1, 7)
            then true else false
        end                                         as is_weekend,

        -- Season (Northern Hemisphere)
        case extract(month from date_day)
            when 12 then 'Winter'
            when 1  then 'Winter'
            when 2  then 'Winter'
            when 3  then 'Spring'
            when 4  then 'Spring'
            when 5  then 'Spring'
            when 6  then 'Summer'
            when 7  then 'Summer'
            when 8  then 'Summer'
            else         'Autumn'
        end                                         as season_northern,

        -- Season (Southern Hemisphere — relevant for ZA, AU, BR)
        case extract(month from date_day)
            when 12 then 'Summer'
            when 1  then 'Summer'
            when 2  then 'Summer'
            when 3  then 'Autumn'
            when 4  then 'Autumn'
            when 5  then 'Autumn'
            when 6  then 'Winter'
            when 7  then 'Winter'
            when 8  then 'Winter'
            else         'Spring'
        end                                         as season_southern,

        -- YYYY-MM format — useful for monthly aggregations
        format_date('%Y-%m', date_day)              as year_month

    from date_spine

)

select * from enriched
