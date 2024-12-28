output "topic_id" {
  description = "The ID of the Pub/Sub topic"
  value       = google_pubsub_topic.main.id
}

output "topic_name" {
  description = "The name of the Pub/Sub topic"
  value       = google_pubsub_topic.main.name
}
