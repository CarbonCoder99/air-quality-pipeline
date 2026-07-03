/*
aqi_category macro
-------------------
Converts a raw pollutant measurement into an AQI health category.

Based on US EPA AQI breakpoints — the global standard for
communicating air quality to the public.

Usage in models:
  {{ aqi_category('pollutant_column', 'value_column') }}

Why a macro instead of inline CASE?
  This logic is used in fct_daily_measurements.
  If EPA updates breakpoints (they do occasionally), we update
  this macro once and all models using it automatically reflect
  the change. Without the macro, we'd hunt through every model.
*/

{% macro aqi_category(pollutant_col, value_col) %}

    case

        -- PM2.5 breakpoints (μg/m³, 24-hour average)
        when {{ pollutant_col }} = 'pm25' then
            case
                when {{ value_col }} <= 12.0   then 'Good'
                when {{ value_col }} <= 35.4   then 'Moderate'
                when {{ value_col }} <= 55.4   then 'Unhealthy for Sensitive Groups'
                when {{ value_col }} <= 150.4  then 'Unhealthy'
                when {{ value_col }} <= 250.4  then 'Very Unhealthy'
                else                                'Hazardous'
            end

        -- PM10 breakpoints (μg/m³, 24-hour average)
        when {{ pollutant_col }} = 'pm10' then
            case
                when {{ value_col }} <= 54    then 'Good'
                when {{ value_col }} <= 154   then 'Moderate'
                when {{ value_col }} <= 254   then 'Unhealthy for Sensitive Groups'
                when {{ value_col }} <= 354   then 'Unhealthy'
                when {{ value_col }} <= 424   then 'Very Unhealthy'
                else                               'Hazardous'
            end

        -- NO2 breakpoints (ppb, 1-hour average)
        when {{ pollutant_col }} = 'no2' then
            case
                when {{ value_col }} <= 53    then 'Good'
                when {{ value_col }} <= 100   then 'Moderate'
                when {{ value_col }} <= 360   then 'Unhealthy for Sensitive Groups'
                when {{ value_col }} <= 649   then 'Unhealthy'
                when {{ value_col }} <= 1249  then 'Very Unhealthy'
                else                               'Hazardous'
            end

        -- O3 breakpoints (ppb, 8-hour average)
        when {{ pollutant_col }} = 'o3' then
            case
                when {{ value_col }} <= 54    then 'Good'
                when {{ value_col }} <= 70    then 'Moderate'
                when {{ value_col }} <= 85    then 'Unhealthy for Sensitive Groups'
                when {{ value_col }} <= 105   then 'Unhealthy'
                when {{ value_col }} <= 200   then 'Very Unhealthy'
                else                               'Hazardous'
            end

        -- CO breakpoints (ppm, 8-hour average)
        when {{ pollutant_col }} = 'co' then
            case
                when {{ value_col }} <= 4.4   then 'Good'
                when {{ value_col }} <= 9.4   then 'Moderate'
                when {{ value_col }} <= 12.4  then 'Unhealthy for Sensitive Groups'
                when {{ value_col }} <= 15.4  then 'Unhealthy'
                when {{ value_col }} <= 30.4  then 'Very Unhealthy'
                else                               'Hazardous'
            end

        -- SO2 breakpoints (ppb, 1-hour average)
        when {{ pollutant_col }} = 'so2' then
            case
                when {{ value_col }} <= 35    then 'Good'
                when {{ value_col }} <= 75    then 'Moderate'
                when {{ value_col }} <= 185   then 'Unhealthy for Sensitive Groups'
                when {{ value_col }} <= 304   then 'Unhealthy'
                when {{ value_col }} <= 604   then 'Very Unhealthy'
                else                               'Hazardous'
            end

        else 'Unknown'

    end

{% endmacro %}
