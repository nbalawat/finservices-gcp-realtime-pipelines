# Create service account for the pipeline
resource "google_service_account" "pipeline_service_account" {
  account_id   = "payment-pipeline-sa-${var.environment}"
  display_name = "Payment Pipeline Service Account ${var.environment}"
  project      = var.project_id
}

# Grant roles to the service account
resource "google_project_iam_member" "pipeline_roles" {
  for_each = toset([
    "roles/pubsub.publisher",
    "roles/pubsub.subscriber",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/bigtable.user",
    "roles/dataflow.worker",
    "roles/storage.objectViewer"
  ])
  
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.pipeline_service_account.email}"
}

# Grant service account user role to Compute Engine default service account
resource "google_project_iam_member" "compute_service_account" {
  project = var.project_id
  role    = "roles/iam.serviceAccountUser"
  member  = "serviceAccount:${data.google_project.project.number}-compute@developer.gserviceaccount.com"
}

# Get project information
data "google_project" "project" {
  project_id = var.project_id
}

# Create a custom role for the pipeline
resource "google_project_iam_custom_role" "pipeline_custom_role" {
  role_id     = "paymentPipelineRole${var.environment}"
  title       = "Payment Pipeline Custom Role ${var.environment}"
  description = "Custom role for the payment pipeline"
  permissions = [
    "bigquery.tables.create",
    "bigquery.tables.updateData",
    "bigquery.tables.get",
    "bigquery.tables.update",
    "bigtable.tables.readRows",
    "bigtable.tables.mutateRows",
    "pubsub.topics.publish",
    "pubsub.subscriptions.consume"
  ]
}

# Grant the custom role to the service account
resource "google_project_iam_member" "pipeline_custom_role" {
  project = var.project_id
  role    = google_project_iam_custom_role.pipeline_custom_role.id
  member  = "serviceAccount:${google_service_account.pipeline_service_account.email}"
}
