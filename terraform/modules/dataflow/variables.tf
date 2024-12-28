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

variable "dataflow_template_path" {
  description = "GCS path to the Dataflow template"
  type        = string
}

variable "temp_gcs_location" {
  description = "GCS location for temporary files"
  type        = string
}

variable "bigtable_subscription_path" {
  description = "Full path to the BigTable PubSub subscription"
  type        = string
  default     = ""
}

variable "bigquery_subscription_path" {
  description = "Full path to the BigQuery PubSub subscription"
  type        = string
  default     = ""
}

variable "gcs_subscription_path" {
  description = "Full path to the GCS PubSub subscription"
  type        = string
  default     = ""
}

variable "alloydb_subscription_path" {
  description = "Full path to the AlloyDB PubSub subscription"
  type        = string
  default     = ""
}

variable "cloudsql_subscription_path" {
  description = "Full path to the CloudSQL PubSub subscription"
  type        = string
  default     = ""
}
