variable "project_id" {
  description = "The GCP Project ID"
  type        = string
}

variable "environment" {
  description = "Environment (dev/prod)"
  type        = string
}

variable "service_account" {
  description = "Service account email to grant access to"
  type        = string
}
