variable "project_id" {
  description = "Your GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP region for all resources"
  type        = string
  default     = "US"
}

variable "location" {
  description = "GCP location for BigQuery datasets"
  type        = string
  default     = "US"
}
