# After terraform apply, these values get printed to your terminal.
# Useful for confirming what was created and grabbing values
# you'll need in other config files.

output "gcs_bucket_name" {
  description = "Name of the raw data lake GCS bucket"
  value       = google_storage_bucket.raw_lake.name
}

output "gcs_bucket_url" {
  description = "GCS URL for the raw bucket"
  value       = "gs://${google_storage_bucket.raw_lake.name}"
}

output "bigquery_raw_dataset" {
  description = "BigQuery raw dataset ID"
  value       = google_bigquery_dataset.raw.dataset_id
}

output "bigquery_staging_dataset" {
  description = "BigQuery staging dataset ID"
  value       = google_bigquery_dataset.staging.dataset_id
}

output "bigquery_mart_dataset" {
  description = "BigQuery mart dataset ID"
  value       = google_bigquery_dataset.mart.dataset_id
}

output "project_id" {
  description = "GCP Project ID"
  value       = var.project_id
}
