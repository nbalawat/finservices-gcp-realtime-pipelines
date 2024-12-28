output "bigtable_instance_name" {
  description = "The name of the BigTable instance"
  value       = google_bigtable_instance.pipeline_instance.name
}

output "bigquery_dataset_id" {
  description = "The ID of the BigQuery dataset"
  value       = google_bigquery_dataset.pipeline_data.dataset_id
}

output "bigquery_payments_table_id" {
  description = "The ID of the BigQuery payments table"
  value       = google_bigquery_table.payments.table_id
}

output "bigquery_errors_table_id" {
  description = "The ID of the BigQuery errors table"
  value       = google_bigquery_table.errors.table_id
}
