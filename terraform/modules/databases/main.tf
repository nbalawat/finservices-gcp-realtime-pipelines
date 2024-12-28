# Create BigTable instance
resource "google_bigtable_instance" "pipeline_instance" {
  name                = "pipeline-${var.environment}"
  project            = var.project_id
  deletion_protection = false  # Explicitly set to false to allow destruction
  
  cluster {
    cluster_id   = "pipeline-${var.environment}-cluster"
    num_nodes    = 1
    storage_type = "SSD"
    zone         = "us-central1-a"
  }
}

# Create BigQuery dataset
resource "google_bigquery_dataset" "pipeline_data" {
  dataset_id    = "pipeline_data_${var.environment}"
  friendly_name = "Pipeline Data"
  location      = "US"
  project       = var.project_id

  delete_contents_on_destroy = true  # Allow deletion of dataset and contents
  default_table_expiration_ms = null

  # Add access controls
  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }
  access {
    role          = "READER"
    special_group = "projectReaders"
  }
  access {
    role          = "WRITER"
    special_group = "projectWriters"
  }
  
  # Grant access to service account
  access {
    role          = "WRITER"
    user_by_email = var.service_account
  }
}

# Create BigQuery table for payments
resource "google_bigquery_table" "payments" {
  dataset_id    = google_bigquery_dataset.pipeline_data.dataset_id
  table_id      = "payments"
  project       = var.project_id
  deletion_protection  = false


  schema = jsonencode([
    {
      name = "transaction_id",
      type = "STRING",
      mode = "REQUIRED",
      description = "Unique transaction identifier"
    },
    {
      name = "payment_type",
      type = "STRING",
      mode = "REQUIRED",
      description = "Type of payment (ACH, WIRE, etc.)"
    },
    {
      name = "customer_id",
      type = "STRING",
      mode = "REQUIRED",
      description = "Customer identifier"
    },
    {
      name = "amount",
      type = "FLOAT",
      mode = "REQUIRED",
      description = "Payment amount"
    },
    {
      name = "currency",
      type = "STRING",
      mode = "REQUIRED",
      description = "Payment currency"
    },
    {
      name = "sender_account",
      type = "STRING",
      mode = "REQUIRED",
      description = "Sender account information"
    },
    {
      name = "receiver_account",
      type = "STRING",
      mode = "REQUIRED",
      description = "Receiver account information"
    },
    {
      name = "timestamp",
      type = "TIMESTAMP",
      mode = "REQUIRED",
      description = "Transaction timestamp"
    },
    {
      name = "status",
      type = "STRING",
      mode = "REQUIRED",
      description = "Payment status"
    },
    {
      name = "metadata",
      type = "STRING",
      mode = "NULLABLE",
      description = "Additional payment metadata"
    }
  ])
}

# Create BigQuery table for errors
resource "google_bigquery_table" "errors" {
  dataset_id    = google_bigquery_dataset.pipeline_data.dataset_id
  table_id      = "payment_errors"
  project       = var.project_id
  deletion_protection  = false

  schema = jsonencode([
    {
      name = "error_type",
      type = "STRING",
      mode = "REQUIRED",
      description = "Type of error"
    },
    {
      name = "error_message",
      type = "STRING",
      mode = "REQUIRED",
      description = "Error message"
    },
    {
      name = "raw_data",
      type = "STRING",
      mode = "REQUIRED",
      description = "Raw data that caused the error"
    },
    {
      name = "timestamp",
      type = "TIMESTAMP",
      mode = "REQUIRED",
      description = "Error timestamp"
    }
  ])
}
