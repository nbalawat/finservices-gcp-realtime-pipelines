# PubSub outputs
output "topic_id" {
  description = "The ID of the Pub/Sub topic"
  value       = module.pubsub.topic_id
}

output "subscription_path" {
  description = "The full path of the Pub/Sub subscription"
  value       = module.pubsub.subscription_path
}

# BigTable outputs
output "bigtable_instance_name" {
  description = "The name of the BigTable instance"
  value       = module.databases.bigtable_instance_name
}

# BigQuery outputs
output "bigquery_dataset_id" {
  description = "The ID of the BigQuery dataset"
  value       = module.databases.bigquery_dataset_id
}

output "bigquery_payments_table_id" {
  description = "The ID of the BigQuery payments table"
  value       = module.databases.bigquery_payments_table_id
}

output "bigquery_errors_table_id" {
  description = "The ID of the BigQuery errors table"
  value       = module.databases.bigquery_errors_table_id
}
