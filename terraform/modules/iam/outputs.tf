output "service_account_email" {
  description = "The email of the service account"
  value       = google_service_account.pipeline_service_account.email
}

output "service_account_name" {
  description = "The fully-qualified name of the service account"
  value       = google_service_account.pipeline_service_account.name
}
