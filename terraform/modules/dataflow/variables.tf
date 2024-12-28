variable "project_id" {
  description = "The GCP Project ID"
  type        = string
}

variable "environment" {
  description = "Environment (dev/prod)"
  type        = string
}

variable "enabled_subscribers" {
  description = "List of enabled subscribers"
  type        = list(string)
}

variable "temp_gcs_location" {
  description = "GCS location for temporary files"
  type        = string
}

variable "template_path" {
  description = "GCS path to the Dataflow template"
  type        = string
  default     = "gs://dataflow-templates/latest/PubSub_to_BigQuery"
}

variable "input_subscription" {
  description = "Path to the input Pub/Sub subscription"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}
