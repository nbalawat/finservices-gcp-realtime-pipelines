output "temp_bucket_url" {
  description = "The URL of the GCS bucket for temporary files"
  value       = "gs://${google_storage_bucket.dataflow_temp.name}"
}

output "staging_bucket_name" {
  description = "The name of the GCS bucket for staging files"
  value       = google_storage_bucket.dataflow_staging.name
}

output "staging_bucket_url" {
  description = "The URL of the GCS bucket for staging files"
  value       = "gs://${google_storage_bucket.dataflow_staging.name}"
}
