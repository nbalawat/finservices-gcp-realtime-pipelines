variable "project_id" {
  description = "The GCP Project ID"
  type        = string
}

variable "region" {
  description = "The default region for resources"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Environment (dev/prod)"
  type        = string
  default     = "dev"
}

variable "enabled_subscribers" {
  description = "List of enabled subscribers"
  type        = list(string)
  default     = ["bigtable"] # Start with just BigTable
}
