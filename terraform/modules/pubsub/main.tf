resource "google_pubsub_topic" "main" {
  name    = "data-pipeline-topic-${var.environment}"
  project = var.project_id
}

resource "google_pubsub_subscription" "bigtable" {
  count   = contains(var.enabled_subscribers, "bigtable") ? 1 : 0
  name    = "bigtable-subscription-${var.environment}"
  topic   = google_pubsub_topic.main.name
  project = var.project_id
}

resource "google_pubsub_subscription" "bigquery" {
  count   = contains(var.enabled_subscribers, "bigquery") ? 1 : 0
  name    = "bigquery-subscription-${var.environment}"
  topic   = google_pubsub_topic.main.name
  project = var.project_id
}

resource "google_pubsub_subscription" "gcs" {
  count   = contains(var.enabled_subscribers, "gcs") ? 1 : 0
  name    = "gcs-subscription-${var.environment}"
  topic   = google_pubsub_topic.main.name
  project = var.project_id
}

resource "google_pubsub_subscription" "alloydb" {
  count   = contains(var.enabled_subscribers, "alloydb") ? 1 : 0
  name    = "alloydb-subscription-${var.environment}"
  topic   = google_pubsub_topic.main.name
  project = var.project_id
}

resource "google_pubsub_subscription" "cloudsql" {
  count   = contains(var.enabled_subscribers, "cloudsql") ? 1 : 0
  name    = "cloudsql-subscription-${var.environment}"
  topic   = google_pubsub_topic.main.name
  project = var.project_id
}
