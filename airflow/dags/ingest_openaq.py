"""
DAG: ingest_openaq
-------------------
Schedule : Daily at 06:00 UTC
Purpose  : Pull daily air quality measurements from OpenAQ API
           and land raw JSON files in GCS.

Data flow:
  OpenAQ API /v3/sensors/{id}/days
      → Raw JSON (newline-delimited)
      → GCS: measurements/date=YYYY-MM-DD/country=XX/data.json

Design decisions:
  - One file per country per day in GCS
  - Newline-delimited JSON (NDJSON) — each line is one measurement record
  - Airflow execution_date used as the data date (yesterday's data)
  - catchup=True enables backfilling historical data
  - Skips inactive sensors (datetimeFirst = None)
  - Handles empty API responses without failing
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import json
import logging
import sys
import os

# Make our utils module importable from within the DAG
# The dags/ folder is on the Python path in Airflow by default
# But utils/ sits one level up, so we add it explicitly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from utils.openaq_client import (
    fetch_country_id_map,
    fetch_locations,
    fetch_daily_measurements,
    extract_sensors
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

# Countries we collect data for
# We chose these for geographic diversity across all continents
TARGET_COUNTRIES = [
    "US",   # North America  — dense sensor network
    "GB",   # Europe         — dense sensor network
    "DE",   # Europe         — industrial + residential mix
    "FR",   # Europe
    "IN",   # Asia           — high pollution, many sensors
    "CN",   # Asia           — industrial pollution context
    "JP",   # Asia           — good data quality
    "BR",   # South America  — urban + forest fire pollution
    "MX",   # North America  — urban pollution
    "ZA",   # Africa         — mining + industrial
    "NG",   # Africa         — your home country
    "KE",   # Africa         — East Africa representation
    "AU",   # Oceania        — bushfire smoke context
    "PK",   # Asia           — historically poor air quality
    "ID",   # Southeast Asia — forest fires + urban pollution
]

GCS_BUCKET = os.environ.get("GCS_BUCKET_NAME", "air-quality-project-501022-air-quality-raw")


# ─────────────────────────────────────────────────────────────
# TASK FUNCTIONS
# ─────────────────────────────────────────────────────────────

def resolve_country_ids(**context):
    """
    TASK 1: Resolve country ISO codes to numeric IDs.

    Why a separate task?
    We only need to call /countries once per DAG run — not once per country.
    By making it its own task, we call it once, push the result to XCom,
    and all downstream tasks read from that shared result.

    XCom (Cross-Communication) is Airflow's mechanism for passing small
    pieces of data between tasks. Think of it as a shared notepad that
    tasks in the same DAG run can read and write.
    """
    api_key = Variable.get("openaq_api_key")

    # Fetch the full country map from OpenAQ
    country_map = fetch_country_id_map(api_key)

    # Resolve only our target countries
    resolved = {}
    for iso_code in TARGET_COUNTRIES:
        if iso_code in country_map:
            resolved[iso_code] = country_map[iso_code]
            logger.info(
                f"Resolved {iso_code} → "
                f"ID {country_map[iso_code]['id']} "
                f"({country_map[iso_code]['name']})"
            )
        else:
            logger.warning(
                f"Country {iso_code} not found in OpenAQ — skipping"
            )

    # Push to XCom so downstream tasks can read it
    # context["ti"] is the task instance — ti.xcom_push stores data
    context["ti"].xcom_push(key="country_ids", value=resolved)
    logger.info(f"Resolved {len(resolved)} of {len(TARGET_COUNTRIES)} countries")


def ingest_country(country_iso: str, **context):
    """
    TASK 2 (one per country): Fetch measurements and upload to GCS.

    This is the core task. For each country:
      1. Read the country's numeric ID from XCom
      2. Fetch all active locations in that country
      3. For each location, for each core pollutant sensor:
         - Fetch yesterday's daily measurements
         - Attach location and country metadata to each record
      4. Write everything to GCS as newline-delimited JSON

    Why newline-delimited JSON (NDJSON)?
    BigQuery's native JSON load format is NDJSON — one JSON object
    per line. It's more efficient to load than a single large JSON
    array because BigQuery can process lines in parallel.

    Why one file per country per day?
    It keeps GCS organised and makes reprocessing easy. If the US
    data for one day gets corrupted, we can reprocess just that one
    file without touching any other country's data.
    """
    from google.cloud import storage

    api_key        = Variable.get("openaq_api_key")
    execution_date = context["ds"]    # format: YYYY-MM-DD (logical date)

    # Airflow runs with execution_date = the scheduled interval START
    # For a daily DAG running at 6AM, execution_date is yesterday
    # The API data for "yesterday" is available by 6AM today
    # So execution_date is exactly the data date we want
    data_date = execution_date

    # Read country IDs resolved by Task 1 via XCom
    # xcom_pull fetches the value pushed by another task
    country_ids = context["ti"].xcom_pull(
        task_ids = "resolve_country_ids",
        key      = "country_ids"
    )

    if country_iso not in country_ids:
        logger.warning(f"{country_iso} not in resolved countries — skipping")
        return

    country_info       = country_ids[country_iso]
    country_numeric_id = country_info["id"]
    country_name       = country_info["name"]

    logger.info(
        f"Starting ingestion for {country_name} ({country_iso}) "
        f"| Date: {data_date}"
    )

    # Fetch all active monitoring locations in this country
    locations = fetch_locations(api_key, country_numeric_id)

    if not locations:
        logger.info(f"No active locations found for {country_name} — nothing to ingest")
        return

    # Collect all measurements across all locations and sensors
    all_records = []

    for location in locations:
        location_id   = location["id"]
        location_name = location.get("name", "Unknown")
        coordinates   = location.get("coordinates", {})
        latitude      = coordinates.get("latitude")  if coordinates else None
        longitude     = coordinates.get("longitude") if coordinates else None
        timezone      = location.get("timezone")

        # Get only core pollutant sensors for this location
        core_sensors = extract_sensors(location)

        if not core_sensors:
            # This location only measures weather metrics — skip it
            continue

        for sensor in core_sensors:
            sensor_id = sensor["sensor_id"]
            pollutant = sensor["pollutant"]

            # Fetch daily measurements for this sensor
            measurements = fetch_daily_measurements(
                api_key   = api_key,
                sensor_id = sensor_id,
                date_from = data_date,
                date_to   = data_date
            )

            if not measurements:
                # Sensor exists but has no data for this date — that's fine
                # We discovered this with Nigeria in our exploration step
                continue

            # Enrich each measurement with location and country context
            # The raw API measurement only has value + datetime
            # We add everything else so the record is self-contained
            for m in measurements:
                record = {
                    # Identifiers
                    "sensor_id":     sensor_id,
                    "location_id":   location_id,
                    "location_name": location_name,
                    "country_iso":   country_iso,
                    "country_name":  country_name,

                    # Pollutant info
                    "pollutant":     pollutant,
                    "units":         sensor["units"],

                    # The actual measurement
                    "value":         m.get("value"),

                    # Datetime — stored as nested object in API response
                    # We preserve the full structure and parse it in dbt
                    "datetime_utc":  (
                        m.get("period", {})
                         .get("datetimeFrom", {})
                         .get("utc")
                    ),
                    "datetime_local": (
                        m.get("period", {})
                         .get("datetimeFrom", {})
                         .get("local")
                    ),

                    # Daily statistics
                    "value_min":     m.get("summary", {}).get("min"),
                    "value_max":     m.get("summary", {}).get("max"),

                    # Geo
                    "latitude":      latitude,
                    "longitude":     longitude,
                    "timezone":      timezone,

                    # Pipeline metadata
                    # Useful for debugging: when was this record loaded?
                    "ingested_at":   datetime.utcnow().isoformat(),
                    "data_date":     data_date,
                }
                all_records.append(record)

    logger.info(
        f"{country_name}: collected {len(all_records)} measurement records"
    )

    if not all_records:
        logger.info(f"No records collected for {country_name} — skipping GCS upload")
        return

    # Convert to newline-delimited JSON (NDJSON)
    # Each line = one complete JSON record
    # BigQuery loads NDJSON natively and efficiently
    ndjson_content = "\n".join(json.dumps(record) for record in all_records)

    # Upload to GCS
    # Path pattern: measurements/date=YYYY-MM-DD/country=XX/data.json
    # The date= and country= prefixes are called Hive partitioning
    # BigQuery can auto-detect these and use them as virtual columns
    gcs_path = f"measurements/date={data_date}/country={country_iso}/data.json"

    storage_client = storage.Client()
    bucket         = storage_client.bucket(GCS_BUCKET)
    blob           = bucket.blob(gcs_path)

    blob.upload_from_string(
        data          = ndjson_content,
        content_type  = "application/json"
    )

    logger.info(
        f"Uploaded {len(all_records)} records to "
        f"gs://{GCS_BUCKET}/{gcs_path}"
    )

    # Push record count to XCom for monitoring
    context["ti"].xcom_push(
        key   = f"record_count_{country_iso}",
        value = len(all_records)
    )


def log_ingestion_summary(**context):
    """
    TASK 3: Print a summary of what was ingested across all countries.

    Why a summary task?
    When you wake up and check your Airflow UI, you want to immediately
    know: did the pipeline succeed? How much data was collected?
    Which countries had data? This task gives you that at a glance.
    """
    execution_date = context["ds"]
    logger.info(f"\n{'='*50}")
    logger.info(f"INGESTION SUMMARY — {execution_date}")
    logger.info(f"{'='*50}")

    total_records = 0
    for iso in TARGET_COUNTRIES:
        count = context["ti"].xcom_pull(
            task_ids = f"ingest_{iso}",
            key      = f"record_count_{iso}"
        )
        count = count or 0
        total_records += count
        status = "✅" if count > 0 else "⚠️ "
        logger.info(f"  {status} {iso}: {count} records")

    logger.info(f"{'='*50}")
    logger.info(f"  TOTAL: {total_records} records ingested")
    logger.info(f"{'='*50}\n")


# ─────────────────────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────────────────────

default_args = {
    "owner":           "airflow",
    "retries":         3,
    "retry_delay":     timedelta(minutes=5),
    "retry_exponential_backoff": True,   # wait longer between each retry
    "email_on_failure": False,           # set to True with SMTP configured
}

with DAG(
    dag_id             = "ingest_openaq",
    description        = "Ingest daily air quality data from OpenAQ API to GCS",
    default_args       = default_args,
    start_date         = datetime(2026, 6, 25),
    schedule  = "0 6 * * *",   # 6AM UTC daily
    catchup            = False,           # enables backfilling
    max_active_runs    = 3,              # max 3 days backfilling in parallel
    tags               = ["air-quality", "ingestion", "openaq"],
) as dag:

    # Task 1: Resolve country ISO codes to numeric IDs
    # Runs once per DAG run — result shared via XCom
    resolve_ids = PythonOperator(
        task_id         = "resolve_country_ids",
        python_callable = resolve_country_ids,
    )

    # Task 2: One ingestion task per country
    # These run in PARALLEL after Task 1 completes
    # Each country is independent — no reason to run them sequentially
    ingest_tasks = []
    for iso_code in TARGET_COUNTRIES:
        task = PythonOperator(
            task_id         = f"ingest_{iso_code}",
            python_callable = ingest_country,
            op_kwargs       = {"country_iso": iso_code},
        )
        ingest_tasks.append(task)

    # Task 3: Summary — runs after ALL country tasks complete
    summary = PythonOperator(
        task_id         = "log_ingestion_summary",
        python_callable = log_ingestion_summary,
        trigger_rule    = "all_done",   # run even if some countries failed
    )

    # Define task dependencies
    # resolve_ids must finish before any country ingestion starts
    # all country tasks must finish before summary runs
    resolve_ids >> ingest_tasks >> summary
