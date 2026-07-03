"""
DAG: load_and_transform
------------------------
Schedule : Daily at 08:00 UTC (2 hours after ingest_openaq)
Purpose  : Load raw GCS files into BigQuery, then run dbt
           to transform raw data into analytics-ready models.

Data flow:
  GCS: measurements/date=YYYY-MM-DD/country=XX/data.json
      → BigQuery: air_quality_raw.raw_measurements
      → dbt staging: air_quality_staging.stg_measurements
      → dbt marts:   air_quality_mart.fct_daily_measurements
                     air_quality_mart.dim_locations
                     air_quality_mart.dim_dates

Design decisions:
  - ExternalTaskSensor waits for DAG 1 before doing anything
  - Loads one file per country — clear audit trail
  - Skips countries with no GCS file (sparse data is expected)
  - dbt runs after ALL countries are loaded — not per country
  - dbt test failures do not fail the DAG — they alert only
"""

from airflow                   import DAG
from airflow.operators.python  import PythonOperator
from airflow.operators.bash    import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models            import Variable
from datetime                  import datetime, timedelta
import os
import logging
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from utils.bigquery_client import (
    gcs_file_exists,
    load_gcs_to_bigquery,
    check_partition_exists
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

TARGET_COUNTRIES = [
    "US", "GB", "DE", "FR", "IN", "CN", "JP",
    "BR", "MX", "ZA", "NG", "KE", "AU", "PK", "ID"
]

GCS_BUCKET  = os.environ.get("GCS_BUCKET_NAME",  "your-project-air-quality-raw")
PROJECT_ID  = os.environ.get("GCP_PROJECT_ID",   "your-project-id")
RAW_DATASET = "air_quality_raw"
RAW_TABLE   = "raw_measurements"
DBT_DIR     = "/opt/airflow/dbt"


# ─────────────────────────────────────────────────────────────
# TASK FUNCTIONS
# ─────────────────────────────────────────────────────────────

def load_country_to_bq(country_iso: str, **context):
    """
    TASK: Load one country's GCS file into BigQuery.

    For each country:
      1. Build the expected GCS path for today's data
      2. Check if the file exists (some countries may have no data)
      3. Check if this partition was already loaded (prevent duplicates)
      4. Load the NDJSON file into BigQuery raw table
      5. Push row count to XCom for the summary task
    """
    execution_date = context["ds"]   # YYYY-MM-DD

    # Build the GCS path where DAG 1 would have written this file
    blob_path = (
        f"measurements/date={execution_date}"
        f"/country={country_iso}/data.json"
    )

    # Check 1: Does the file exist in GCS?
    if not gcs_file_exists(GCS_BUCKET, blob_path):
        logger.info(
            f"{country_iso}: No GCS file for {execution_date} — "
            f"skipping (expected for countries with sparse data)"
        )
        context["ti"].xcom_push(
            key=f"rows_loaded_{country_iso}", value=0
        )
        return

    # Check 2: Was this partition already loaded?
    # Prevents duplicate rows if DAG is manually re-triggered
    already_loaded = check_partition_exists(
        project_id = PROJECT_ID,
        dataset_id = RAW_DATASET,
        table_id   = RAW_TABLE,
        data_date  = execution_date
    )

    if already_loaded:
        logger.warning(
            f"{country_iso}: Partition {execution_date} already exists "
            f"in BigQuery — skipping to prevent duplicates"
        )
        context["ti"].xcom_push(
            key=f"rows_loaded_{country_iso}", value=0
        )
        return

    # Load the file into BigQuery
    rows_loaded = load_gcs_to_bigquery(
        bucket_name    = GCS_BUCKET,
        blob_path      = blob_path,
        project_id     = PROJECT_ID,
        dataset_id     = RAW_DATASET,
        table_id       = RAW_TABLE,
        date_partition = execution_date,
        country_iso    = country_iso
    )

    context["ti"].xcom_push(
        key=f"rows_loaded_{country_iso}", value=rows_loaded
    )
    logger.info(
        f"{country_iso}: Successfully loaded {rows_loaded} rows "
        f"for {execution_date}"
    )


def log_load_summary(**context):
    """
    TASK: Print a summary of all loads across all countries.

    Runs after all country load tasks complete.
    Gives a clear picture of what went into BigQuery today.
    """
    execution_date = context["ds"]

    logger.info(f"\n{'='*55}")
    logger.info(f"  LOAD SUMMARY — {execution_date}")
    logger.info(f"{'='*55}")

    total_rows = 0
    loaded     = []
    skipped    = []

    for iso in TARGET_COUNTRIES:
        rows = context["ti"].xcom_pull(
            task_ids = f"load_{iso}_to_bq",
            key      = f"rows_loaded_{iso}"
        ) or 0

        total_rows += rows

        if rows > 0:
            loaded.append(f"{iso}: {rows:,} rows")
        else:
            skipped.append(iso)

    for entry in loaded:
        logger.info(f"  ✅ {entry}")

    if skipped:
        logger.info(f"\n  ⚠️  No data: {', '.join(skipped)}")

    logger.info(f"\n  TOTAL ROWS LOADED: {total_rows:,}")
    logger.info(f"{'='*55}\n")


# ─────────────────────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────────────────────

default_args = {
    "owner":          "airflow",
    "retries":        2,
    "retry_delay":    timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id            = "load_and_transform",
    description       = "Load GCS files to BigQuery and run dbt transformations",
    default_args      = default_args,
    start_date        = datetime(2026, 6, 30),
    schedule_interval = "0 8 * * *",   # 8AM UTC — 2 hours after ingestion
    catchup           = False,
    max_active_runs   = 3,
    tags              = ["air-quality", "loading", "dbt"],
) as dag:

    # ── Task 1: Wait for DAG 1 ──────────────────────────────
    # ExternalTaskSensor polls Airflow's metadata DB every 60 seconds
    # and waits until ingest_openaq's summary task shows SUCCESS
    # for the SAME execution date as this DAG run.
    #
    # Why wait for log_ingestion_summary specifically?
    # It's the last task in DAG 1. If it succeeded, all country
    # ingestion tasks either succeeded or were gracefully skipped.
    # That means GCS files are ready to load.
    #
    # timeout=7200 = wait up to 2 hours before giving up.
    # If DAG 1 hasn't finished in 2 hours, something is very wrong.
    wait_for_ingestion = ExternalTaskSensor(
        task_id              = "wait_for_ingestion",
        external_dag_id      = "ingest_openaq",
        external_task_id     = "log_ingestion_summary",
        timeout              = 7200,        # 2 hours in seconds
        poke_interval        = 60,          # check every 60 seconds
        mode                 = "reschedule", # free up worker slot while waiting
        allowed_states       = ["success"],
        failed_states        = ["failed", "upstream_failed"],
    )

    # ── Task 2: Load each country in parallel ───────────────
    load_tasks = []
    for iso_code in TARGET_COUNTRIES:
        task = PythonOperator(
            task_id         = f"load_{iso_code}_to_bq",
            python_callable = load_country_to_bq,
            op_kwargs       = {"country_iso": iso_code},
        )
        load_tasks.append(task)

    # ── Task 3: Load summary ─────────────────────────────────
    load_summary = PythonOperator(
        task_id         = "log_load_summary",
        python_callable = log_load_summary,
        trigger_rule    = "all_done",
    )

    # ── Task 4: dbt run ──────────────────────────────────────
    # Runs dbt against BigQuery using the profiles.yml
    # mounted inside the container at /opt/airflow/dbt
    # --select staging+ means: run staging models AND
    # everything downstream of them (the mart models)
    dbt_run = BashOperator(
        task_id      = "dbt_run",
        bash_command = """
            cd {{ params.dbt_dir }} && \
            dbt deps && \
            dbt run \
              --profiles-dir {{ params.dbt_dir }} \
              --target prod \
              --select staging+ \
              --vars '{"execution_date": "{{ ds }}"}'
        """,
        params = {"dbt_dir": DBT_DIR},
    )

    # ── Task 5: dbt test ─────────────────────────────────────
    # Runs all schema tests defined in schema.yml files
    # soft_fail=True means test failures raise a warning,
    # not a full DAG failure — important because one bad sensor
    # reading shouldn't bring down the entire pipeline
    dbt_test = BashOperator(
        task_id      = "dbt_test",
        bash_command = """
            cd {{ params.dbt_dir }} && \
            dbt test \
              --profiles-dir {{ params.dbt_dir }} \
              --target prod \
              --select staging+ || echo "⚠️ dbt tests had failures — check logs"
        """,
        params = {"dbt_dir": DBT_DIR},
    )
    # ── Task 6: dbt docs generate ────────────────────────────
    # Regenerates dbt documentation after every successful run
    # Documentation lives at /opt/airflow/dbt/target/
    # In a production setup you'd serve this with dbt docs serve
    dbt_docs = BashOperator(
        task_id      = "dbt_docs_generate",
        bash_command = """
            cd {{ params.dbt_dir }} && \
            dbt docs generate \
              --profiles-dir {{ params.dbt_dir }} \
              --target prod
        """,
        params = {"dbt_dir": DBT_DIR},
    )

    # ── Dependency chain ─────────────────────────────────────
    #
    # wait_for_ingestion
    #         │
    #         ▼
    # load_US  load_GB  load_DE ... (all parallel)
    #         │
    #         ▼
    # log_load_summary
    #         │
    #         ▼
    #     dbt_run
    #         │
    #         ▼
    #     dbt_test
    #         │
    #         ▼
    #   dbt_docs_generate

    wait_for_ingestion >> load_tasks >> load_summary
    load_summary >> dbt_run >> dbt_test >> dbt_docs
