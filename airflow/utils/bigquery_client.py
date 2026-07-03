"""
BigQuery Utility Functions
---------------------------
Handles all interactions with BigQuery for the load DAG.
Kept separate from the DAG so the DAG stays focused on
orchestration, not implementation details.
"""

import logging
from google.cloud import bigquery, storage

logger = logging.getLogger(__name__)


def get_bq_client():
    """
    Create and return an authenticated BigQuery client.
    Uses Application Default Credentials — the service account
    key we set via GOOGLE_APPLICATION_CREDENTIALS in .env
    """
    return bigquery.Client()


def get_gcs_client():
    """
    Create and return an authenticated GCS client.
    """
    return storage.Client()


def gcs_file_exists(bucket_name: str, blob_path: str) -> bool:
    """
    Check whether a specific file exists in GCS before trying to load it.

    Why check existence?
    Some countries (like Nigeria) may return zero measurements on a given
    day — DAG 1 skips uploading a file for them. If DAG 2 tries to load
    a file that doesn't exist, BigQuery throws an error.
    Checking existence first lets us skip gracefully.
    """
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob   = bucket.blob(blob_path)
    exists = blob.exists()

    if exists:
        logger.info(f"GCS file found: gs://{bucket_name}/{blob_path}")
    else:
        logger.info(f"GCS file not found: gs://{bucket_name}/{blob_path}")

    return exists


def load_gcs_to_bigquery(
    bucket_name:    str,
    blob_path:      str,
    project_id:     str,
    dataset_id:     str,
    table_id:       str,
    date_partition: str,
    country_iso:    str
) -> int:
    """
    Load a single NDJSON file from GCS into a BigQuery table.

    Key design decisions:
    - WRITE_APPEND: we never overwrite existing data — we always append
    - autodetect=True: BigQuery infers schema from the NDJSON file
    - time_partitioning on data_date: makes date-filtered queries cheaper
    - clustering on country_iso + pollutant: makes country/pollutant
      filtered queries skip irrelevant data blocks entirely

    Returns the number of rows loaded.
    """
    client = get_bq_client()

    gcs_uri         = f"gs://{bucket_name}/{blob_path}"
    table_reference = f"{project_id}.{dataset_id}.{table_id}"

    # Define how this table should be partitioned and clustered
    # These settings are applied on first load and must stay consistent
    time_partitioning = bigquery.TimePartitioning(
        type_  = bigquery.TimePartitioningType.DAY,
        field  = "data_date"   # partition on the data date column we add
    )

    clustering_fields = ["country_iso", "pollutant"]

    # Job config defines how BigQuery should load the file
    job_config = bigquery.LoadJobConfig(
        source_format        = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition    = bigquery.WriteDisposition.WRITE_APPEND,
        autodetect           = True,
        time_partitioning    = time_partitioning,
        clustering_fields    = clustering_fields,
        ignore_unknown_values = True,   # don't fail on unexpected fields
    )

    logger.info(
        f"Loading {gcs_uri} → "
        f"{table_reference}${date_partition.replace('-', '')}"
    )

    # Start the load job
    load_job = client.load_table_from_uri(
        source_uris  = gcs_uri,
        destination  = table_reference,
        job_config   = job_config
    )

    # .result() blocks until the job completes
    # This is important — without it the function returns immediately
    # and we don't know if the job succeeded or failed
    load_job.result()

    # Get the destination table to check how many rows were loaded
    table    = client.get_table(table_reference)
    rows_loaded = load_job.output_rows

    logger.info(
        f"Loaded {rows_loaded} rows into {table_reference}"
    )

    return rows_loaded


def check_partition_exists(
    project_id:  str,
    dataset_id:  str,
    table_id:    str,
    data_date:   str
) -> bool:
    """
    Check if data for a specific date already exists in BigQuery.

    Why check this?
    If DAG 2 runs twice for the same day (e.g. manual re-trigger),
    we don't want to double-load data into BigQuery.
    Checking first prevents duplicate records.

    data_date format: "YYYY-MM-DD"
    """
    client = get_bq_client()

    query = f"""
        SELECT COUNT(*) as row_count
        FROM `{project_id}.{dataset_id}.{table_id}`
        WHERE data_date = '{data_date}'
    """

    try:
        result    = client.query(query).result()
        row_count = next(iter(result)).row_count
        exists    = row_count > 0

        if exists:
            logger.warning(
                f"Partition {data_date} already has {row_count} rows "
                f"in {table_id} — will skip to prevent duplicates"
            )
        return exists

    except Exception:
        # Table might not exist yet on first run — that's fine
        return False
