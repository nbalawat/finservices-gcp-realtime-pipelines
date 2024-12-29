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

# Create or update BigQuery table for payments
resource "google_bigquery_table" "payments" {
  dataset_id          = "pipeline_data_${var.environment}"
  friendly_name       = "Payment Records"
  table_id           = "payments"
  project            = var.project_id
  deletion_protection = false
  description        = "Records of all payment transactions processed by the pipeline"

  schema = jsonencode([
    {
      name = "transaction_id"
      type = "STRING"
      mode = "REQUIRED"
      description = "Unique identifier for the transaction"
    },
    {
      name = "payment_type"
      type = "STRING"
      mode = "REQUIRED"
      description = "Type of payment (WIRE, ACH, RTP)"
    },
    {
      name = "customer_id"
      type = "STRING"
      mode = "REQUIRED"
      description = "Customer identifier"
    },
    {
      name = "amount"
      type = "FLOAT"
      mode = "REQUIRED"
      description = "Payment amount"
    },
    {
      name = "currency"
      type = "STRING"
      mode = "REQUIRED"
      description = "Currency code"
    },
    {
      name = "sender_account"
      type = "STRING"
      mode = "REQUIRED"
      description = "Sender account information"
    },
    {
      name = "receiver_account"
      type = "STRING"
      mode = "REQUIRED"
      description = "Receiver account information"
    },
    {
      name = "timestamp"
      type = "TIMESTAMP"
      mode = "REQUIRED"
      description = "Transaction timestamp"
    },
    {
      name = "status"
      type = "STRING"
      mode = "REQUIRED"
      description = "Payment status"
    },
    {
      name = "error_message"
      type = "STRING"
      mode = "NULLABLE"
      description = "Error message if payment failed"
    },
    {
      name = "metadata"
      type = "STRING"
      mode = "NULLABLE"
      description = "Additional payment metadata in JSON format"
    }
  ])

  time_partitioning {
    type  = "DAY"
    field = "timestamp"
  }

  lifecycle {
    prevent_destroy = false
    ignore_changes = [
      schema, # Allow schema to be managed by the application
      labels  # Ignore changes to labels
    ]
  }
}

# Create BigQuery table for errors
resource "google_bigquery_table" "errors" {
  dataset_id          = "pipeline_data_${var.environment}"
  table_id           = "payment_errors"
  project            = var.project_id
  deletion_protection = false

  schema = jsonencode([
    {
      name = "error_type"
      type = "STRING"
      mode = "REQUIRED"
      description = "Type of error"
    },
    {
      name = "error_message"
      type = "STRING"
      mode = "REQUIRED"
      description = "Error message"
    },
    {
      name = "raw_data"
      type = "STRING"
      mode = "REQUIRED"
      description = "Raw data that caused the error"
    },
    {
      name = "timestamp"
      type = "TIMESTAMP"
      mode = "REQUIRED"
      description = "Error timestamp"
    }
  ])
}
