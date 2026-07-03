# Tell Terraform to use the Google Cloud provider
# A "provider" is a plugin that knows how to talk to a specific cloud platform
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

# Configure the Google provider with your project and region
provider "google" {
  project = var.project_id
  region  = var.region
}

# ─────────────────────────────────────────────
# GCS BUCKET — Your Data Lake (Raw Landing Zone)
# ─────────────────────────────────────────────
# This is where Airflow drops raw JSON files from the OpenAQ API
# before anything is processed or loaded into BigQuery.
# Raw data lands here first, untouched. This is your source of truth.

resource "google_storage_bucket" "raw_lake" {
  name          = "${var.project_id}-air-quality-raw"
  location      = var.region
  force_destroy = true   # allows terraform destroy to delete even if bucket has files

  # Organise storage into classes based on access frequency
  storage_class = "STANDARD"

  # Automatically delete raw files after 90 days
  # Raw files are only needed for reprocessing — they don't need to live forever
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 90
    }
  }

  # Prevent public access — this data is private
  public_access_prevention = "enforced"

  # Versioning OFF for raw data — we don't need multiple versions of raw files
  versioning {
    enabled = false
  }
}

# ─────────────────────────────────────────────
# BIGQUERY DATASETS — Your Data Warehouse Layers
# ─────────────────────────────────────────────
# We create three separate datasets representing the three layers
# of our data pipeline. Keeping them separate gives us:
# - Clear access control (analysts only see mart, not raw)
# - Clear data lineage (you always know which layer data came from)
# - Clean dbt project structure

# Layer 1: RAW
# Holds data exactly as it came from GCS — no transformations.
# Only the pipeline writes here. Nobody should query this directly.
resource "google_bigquery_dataset" "raw" {
  dataset_id    = "air_quality_raw"
  friendly_name = "Air Quality Raw"
  description   = "Raw data loaded directly from GCS. No transformations applied."
  location      = var.location

  # Auto-delete tables in this dataset after 90 days
  # Raw BigQuery tables are disposable — GCS is the real archive
  default_table_expiration_ms = 7776000000  # 90 days in milliseconds
}

# Layer 2: STAGING
# Holds dbt staging models — cleaned, renamed, cast to correct types.
# Still one-to-one with source tables, just standardised.
resource "google_bigquery_dataset" "staging" {
  dataset_id    = "air_quality_staging"
  friendly_name = "Air Quality Staging"
  description   = "dbt staging models. Cleaned and standardised raw data."
  location      = var.location
}

# Layer 3: MART
# Holds dbt mart models — the star schema ready for analysis.
# This is what analysts, dashboards, and reports query.
resource "google_bigquery_dataset" "mart" {
  dataset_id    = "air_quality_mart"
  friendly_name = "Air Quality Mart"
  description   = "dbt mart models. Star schema ready for analysis."
  location      = var.location
}
