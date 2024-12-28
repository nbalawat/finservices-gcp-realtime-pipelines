resource "google_storage_bucket" "dataflow_temp" {
  name     = "dataflow-temp-${var.project_id}-${var.environment}"
  location = var.region
  project  = var.project_id
}

resource "google_bigquery_dataset" "main" {
  count         = contains(var.enabled_subscribers, "bigquery") ? 1 : 0
  dataset_id    = "pipeline_data_${var.environment}"
  friendly_name = "Pipeline Data"
  location      = var.region
  project       = var.project_id
}

# Example table for BigQuery
resource "google_bigquery_table" "example" {
  count         = contains(var.enabled_subscribers, "bigquery") ? 1 : 0
  dataset_id    = google_bigquery_dataset.main[0].dataset_id
  table_id      = "example_table"
  project       = var.project_id

  schema = jsonencode([
    {
      name = "timestamp",
      type = "TIMESTAMP",
      mode = "REQUIRED",
      description = "Event timestamp"
    },
    {
      name = "data",
      type = "STRING",
      mode = "REQUIRED",
      description = "Event data"
    }
  ])
}
