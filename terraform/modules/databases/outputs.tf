output "bigtable_instance_name" {
  description = "The name of the BigTable instance"
  value       = try(google_bigtable_instance.instance[0].name, "")
}

output "cloudsql_instance_name" {
  description = "The name of the CloudSQL instance"
  value       = try(google_sql_database_instance.instance[0].name, "")
}
