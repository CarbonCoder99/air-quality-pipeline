# OpenAQ API — Data Discovery Findings

## API Version
- Using: v3
- Base URL: https://api.openaq.org/v3
- Auth: X-API-Key header (header name: X-API-Key)

---

## Key Endpoints

| Endpoint | Purpose |
|---|---|
| GET /v3/countries | List all countries with numeric IDs |
| GET /v3/locations | Get monitoring stations per country |
| GET /v3/sensors/{id}/days | Get daily averages per sensor |

---

## Critical Discovery — Country ID Format

**The API requires NUMERIC country IDs, not ISO string codes.**

| ISO Code | Country | Numeric ID |
|---|---|---|
| NG | Nigeria | 100 |

This means our pipeline cannot use "NG" as a filter parameter.
It must first resolve ISO codes to numeric IDs via the /v3/countries
endpoint, then use those numeric IDs for all location queries.

Pipeline implication: The ingestion DAG must maintain a mapping of
ISO code -> numeric ID, built fresh from the countries endpoint at
the start of each run.

---

## Location Record Structure

Each location record contains:

| Field | Type | Notes |
|---|---|---|
| id | integer | Unique location ID |
| name | string | Station name (e.g. "SPARTAN - Ilorin University") |
| country.code | string | ISO code e.g. "NG" |
| country.id | integer | Numeric country ID e.g. 100 |
| country.name | string | Full country name |
| coordinates.latitude | float | Can be None for some stations |
| coordinates.longitude | float | Can be None for some stations |
| datetimeFirst | None/string | NULL for inactive stations |
| datetimeLast | None/string | NULL for inactive stations |
| isMobile | boolean | False = fixed station |
| isMonitor | boolean | True = reference monitor |
| timezone | string | e.g. "Africa/Lagos" |
| sensors | list | One entry per pollutant measured |
| instruments | list | Hardware info |
| provider | dict | Data provider details |

---

## Sensor Structure (inside each location)

Each sensor in the sensors array contains:

| Field | Type | Notes |
|---|---|---|
| id | integer | Unique sensor ID — used to fetch measurements |
| name | string | e.g. "pm25 μg/m³" |
| parameter.id | integer | Numeric parameter ID |
| parameter.name | string | e.g. "pm25" |
| parameter.units | string | e.g. "μg/m³" |
| parameter.displayName | string | e.g. "PM2.5" — can be None |

Key insight: one location can have multiple sensors.
We must loop through all sensors per location to capture all pollutants.

---

## Measurement Record Structure

Endpoint: GET /v3/sensors/{sensor_id}/days
Date parameters: date_from, date_to (format: YYYY-MM-DD)

Fields in each daily measurement record:

| Field | Type | Notes |
|---|---|---|
| value | float | The daily average reading |
| period.datetimeFrom.utc | string | Start of day in UTC — extract [:10] for date |
| period.datetimeFrom.local | string | Local timezone version |
| period.datetimeTo.utc | string | End of day in UTC |
| period.datetimeTo.local | string | Local timezone version |
| coverage | dict | Data completeness metrics |
| summary.min | float | Minimum reading in the day |
| summary.max | float | Maximum reading in the day |
| summary.avg | float | Same as value |

How to extract the date in our dbt staging model:
  CAST(JSON_EXTRACT_SCALAR(period, '$.datetimeFrom.utc') AS TIMESTAMP)
  Then DATE() to get just the date part.

---

## Data Quality Observations

### Nigeria (Sensor 62 — SPARTAN Ilorin University — pm25)
- Measurements for Jan 2024 : 0 records
- datetimeFirst on location : None
- datetimeLast on location  : None
- Null values               : 0 (no data at all)
- Negative values           : 0 (no data at all)
- Date gaps                 : Entire date range missing

### What This Means for the Pipeline
Nigeria has monitoring stations registered in OpenAQ but many are
inactive or report data very infrequently. This is common in African
countries where sensor maintenance is inconsistent.

**Pipeline implication:** We cannot assume every sensor returns data.
The pipeline must handle empty API responses gracefully without failing.
An empty response is valid — it just means no data for that day.

---

## Countries With Likely Better Coverage
Based on OpenAQ documentation and known data density:
- US, GB, DE, FR — dense, reliable, multiple stations per city
- IN, CN — large number of stations, generally active
- BR, MX — moderate coverage in major cities
- NG, KE, ZA — sparse, many inactive stations

Pipeline strategy: We fetch all available countries uniformly.
If a sensor returns no data, we log it and move on.
We do NOT fail the entire DAG because one country has no data.

---

## Pipeline Design Decisions

| Decision | Reason |
|---|---|
| Resolve country IDs dynamically | API requires numeric IDs, not ISO codes |
| Filter pollutants to core set | Only pm25, pm10, no2, o3, co, so2 are globally comparable |
| Handle empty responses gracefully | Many sensors are inactive — empty is valid |
| Use /days endpoint not /hours | Daily granularity suits trend analysis and reduces API calls |
| Store raw JSON in GCS | Preserves original data for reprocessing if parsing logic changes |
| Filter negatives in dbt staging | Negative readings indicate sensor malfunction |
| Filter nulls in dbt staging | Null values cannot be used in aggregations |
| Partition BigQuery by date | Queries filtering by date skip entire partitions — cost efficient |
| Cluster by country and pollutant | Most analytical queries filter by these two dimensions |
| Incremental dbt model | No need to reprocess all history daily — only new dates |

---

## Open Questions Durin Data Exploration (resolved before creating workflow)
- [x] Does the API need numeric or string country IDs? NUMERIC
- [x] Is datetime a string or nested object? NESTED OBJECT
- [x] Can sensors have null coordinates? YES
- [x] What happens when a sensor has no data? Returns empty results array
- [x] Is sparse African data a problem? No — pipeline handles empty gracefully
