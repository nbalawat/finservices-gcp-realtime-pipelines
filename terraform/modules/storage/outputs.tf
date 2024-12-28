output "temp_bucket_url" {
  description = "The URL of the temporary GCS bucket"
  value       = "gs://${google_storage_bucket.dataflow_temp.name}"
}

output "bigquery_dataset_id" {
  description = "The ID of the BigQuery dataset"
  value       = try(google_bigquery_dataset.main[0].dataset_id, "")
}
