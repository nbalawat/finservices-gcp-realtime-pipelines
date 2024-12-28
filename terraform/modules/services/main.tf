# Define required services
locals {
  required_services = {
    # Core services
    "cloudresourcemanager" = "cloudresourcemanager.googleapis.com"
    "serviceusage"         = "serviceusage.googleapis.com"
    "iam"                  = "iam.googleapis.com"
    
    # Storage services
    "storage"             = "storage.googleapis.com"
    "bigquery"           = "bigquery.googleapis.com"
    "bigtable"           = "bigtable.googleapis.com"
    
    # Messaging services
    "pubsub"             = "pubsub.googleapis.com"
    
    # Processing services
    "dataflow"           = "dataflow.googleapis.com"
    "compute"            = "compute.googleapis.com"
  }
}

# Enable required APIs
resource "google_project_service" "required_apis" {
  for_each = local.required_services
  
  project = var.project_id
  service = each.value

  disable_dependent_services = true
  disable_on_destroy        = false
}
