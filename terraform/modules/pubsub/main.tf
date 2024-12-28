resource "google_pubsub_topic" "payments" {
  name    = "payments-${var.environment}"
  project = var.project_id
}

resource "google_pubsub_subscription" "payments" {
  name    = "payments-sub-${var.environment}"
  topic   = google_pubsub_topic.payments.name
  project = var.project_id

  # Configure message retention
  message_retention_duration = "604800s"  # 7 days
  retain_acked_messages = false

  # Configure acknowledgement deadline
  ack_deadline_seconds = 60

  # Enable message ordering if needed
  enable_message_ordering = true

  # Configure expiration policy
  expiration_policy {
    ttl = "2592000s"  # 30 days
  }

  # Configure retry policy
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"  # 10 minutes
  }
}

# Grant publisher access to service account
resource "google_pubsub_topic_iam_member" "publisher" {
  project = var.project_id
  topic   = google_pubsub_topic.payments.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${var.service_account}"
}

# Grant subscriber access to service account
resource "google_pubsub_subscription_iam_member" "subscriber" {
  project      = var.project_id
  subscription = google_pubsub_subscription.payments.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${var.service_account}"
}
