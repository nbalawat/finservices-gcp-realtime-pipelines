output "topic_id" {
  description = "The ID of the Pub/Sub topic"
  value       = google_pubsub_topic.payments.id
}

output "subscription_path" {
  description = "The full path of the Pub/Sub subscription"
  value       = google_pubsub_subscription.payments.id
}
