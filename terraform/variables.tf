variable "credentials" {
  description = "Path to the GCP credentials JSON file"
  default     = "/workspaces/London-Network-Analysis/terraform/keys/la_creds.json"
}

variable "project" {
  description = "The GCP project ID"
  default     = "london-analytics"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "The location for the BigQuery dataset"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "london_analytics_dataset"
}

variable "gcs_bucket_name" {
  description = "My GCS Bucket Name"
  default     = "london-analytics-bucket"
}

variable "gcs_storage_class" {
  description = "The storage class of the GCS bucket"
  default     = "STANDARD"
}

