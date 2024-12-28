locals {
  required_services = {
    "pubsub"    = "pubsub.googleapis.com"
    "dataflow"  = "dataflow.googleapis.com"
    "bigtable"  = "bigtable.googleapis.com"
    "bigquery"  = "bigquery.googleapis.com"
    "cloudsql"  = "sql-component.googleapis.com"
    "alloydb"   = "alloydb.googleapis.com"
    "compute"   = "compute.googleapis.com"  # Required for networks
    "storage"   = "storage.googleapis.com"  # Required for GCS
    "servicenetworking" = "servicenetworking.googleapis.com"  # Required for private services
  }

  # Filter services based on enabled subscribers
  enabled_services = merge(
    # Always enable these core services
    {
      "pubsub"    = local.required_services["pubsub"]
      "dataflow"  = local.required_services["dataflow"]
      "compute"   = local.required_services["compute"]
      "storage"   = local.required_services["storage"]
      "servicenetworking" = local.required_services["servicenetworking"]
    },
    # Conditionally enable subscriber-specific services
    contains(var.enabled_subscribers, "bigtable") ? { "bigtable" = local.required_services["bigtable"] } : {},
    contains(var.enabled_subscribers, "bigquery") ? { "bigquery" = local.required_services["bigquery"] } : {},
    contains(var.enabled_subscribers, "cloudsql") ? { "cloudsql" = local.required_services["cloudsql"] } : {},
    contains(var.enabled_subscribers, "alloydb") ? { "alloydb" = local.required_services["alloydb"] } : {}
  )
}

resource "google_project_service" "services" {
  for_each = local.enabled_services
  
  project = var.project_id
  service = each.value

  disable_dependent_services = false
  disable_on_destroy        = false

  timeouts {
    create = "30m"
    update = "40m"
  }
}
