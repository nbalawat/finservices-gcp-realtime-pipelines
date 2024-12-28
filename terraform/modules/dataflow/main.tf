resource "google_dataflow_job" "bigtable_job" {
  count              = contains(var.enabled_subscribers, "bigtable") ? 1 : 0
  name               = "dataflow-bigtable-${var.environment}"
  template_gcs_path  = var.dataflow_template_path
  temp_gcs_location  = var.temp_gcs_location
  parameters = {
    inputSubscription = var.bigtable_subscription_path
  }
}

resource "google_dataflow_job" "bigquery_job" {
  count              = contains(var.enabled_subscribers, "bigquery") ? 1 : 0
  name               = "dataflow-bigquery-${var.environment}"
  template_gcs_path  = var.dataflow_template_path
  temp_gcs_location  = var.temp_gcs_location
  parameters = {
    inputSubscription = var.bigquery_subscription_path
  }
}

resource "google_dataflow_job" "gcs_job" {
  count              = contains(var.enabled_subscribers, "gcs") ? 1 : 0
  name               = "dataflow-gcs-${var.environment}"
  template_gcs_path  = var.dataflow_template_path
  temp_gcs_location  = var.temp_gcs_location
  parameters = {
    inputSubscription = var.gcs_subscription_path
  }
}

resource "google_dataflow_job" "alloydb_job" {
  count              = contains(var.enabled_subscribers, "alloydb") ? 1 : 0
  name               = "dataflow-alloydb-${var.environment}"
  template_gcs_path  = var.dataflow_template_path
  temp_gcs_location  = var.temp_gcs_location
  parameters = {
    inputSubscription = var.alloydb_subscription_path
  }
}

resource "google_dataflow_job" "cloudsql_job" {
  count              = contains(var.enabled_subscribers, "cloudsql") ? 1 : 0
  name               = "dataflow-cloudsql-${var.environment}"
  template_gcs_path  = var.dataflow_template_path
  temp_gcs_location  = var.temp_gcs_location
  parameters = {
    inputSubscription = var.cloudsql_subscription_path
  }
}
