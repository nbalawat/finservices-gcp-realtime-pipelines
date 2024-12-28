variable "project_id" {
  description = "The GCP Project ID"
  type        = string
}

variable "environment" {
  description = "Environment (dev/prod)"
  type        = string
}

variable "enabled_subscribers" {
  description = "List of enabled subscribers"
  type        = list(string)
}
