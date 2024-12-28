# Create a bucket for temporary files
resource "google_storage_bucket" "dataflow_temp" {
  name     = "dataflow-temp-${var.project_id}-${var.environment}"
  location = "US"
  project  = var.project_id

  uniform_bucket_level_access = true
  force_destroy              = true

  # Add versioning for better data protection
  versioning {
    enabled = true
  }

  # Add lifecycle rules to clean up old files
  lifecycle_rule {
    condition {
      age = 7  # 7 days
    }
    action {
      type = "Delete"
    }
  }
}

# Create a bucket for staging files
resource "google_storage_bucket" "dataflow_staging" {
  name     = "dataflow-staging-${var.project_id}-${var.environment}"
  location = "US"
  project  = var.project_id

  uniform_bucket_level_access = true
  force_destroy              = true

  # Add versioning for better data protection
  versioning {
    enabled = true
  }

  # Add lifecycle rules to clean up old files
  lifecycle_rule {
    condition {
      age = 7  # 7 days
    }
    action {
      type = "Delete"
    }
  }
}

# Grant access to service account
resource "google_storage_bucket_iam_member" "temp_bucket_access" {
  bucket = google_storage_bucket.dataflow_temp.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${var.service_account}"
}

resource "google_storage_bucket_iam_member" "staging_bucket_access" {
  bucket = google_storage_bucket.dataflow_staging.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${var.service_account}"
}
