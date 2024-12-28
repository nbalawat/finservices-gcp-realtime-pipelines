resource "google_bigtable_instance" "instance" {
  count         = contains(var.enabled_subscribers, "bigtable") ? 1 : 0
  name          = "bt-instance-${var.environment}"
  project       = var.project_id
  
  cluster {
    cluster_id   = "bt-cluster-${var.environment}"
    num_nodes    = 1
    storage_type = "HDD"
    zone         = "${var.region}-a"
  }

  deletion_protection = false  # Set to true for production
}

resource "google_bigtable_table" "table" {
  count          = contains(var.enabled_subscribers, "bigtable") ? 1 : 0
  name           = "data-table"
  instance_name  = google_bigtable_instance.instance[0].name
  project        = var.project_id

  column_family {
    family = "cf1"
  }
}

# Example CloudSQL instance
resource "google_sql_database_instance" "instance" {
  count            = contains(var.enabled_subscribers, "cloudsql") ? 1 : 0
  name             = "sql-instance-${var.environment}"
  database_version = "POSTGRES_13"
  region          = var.region
  project         = var.project_id

  settings {
    tier = "db-f1-micro"
  }

  deletion_protection = false  # Set to true for production
}

# Example CloudSQL database
resource "google_sql_database" "database" {
  count     = contains(var.enabled_subscribers, "cloudsql") ? 1 : 0
  name      = "pipeline_data"
  instance  = google_sql_database_instance.instance[0].name
  project   = var.project_id
}

# Note: AlloyDB resources would be added here when needed
# Currently, AlloyDB doesn't have official Terraform resources
# You would need to use null_resource with custom scripts or wait for official support
