# Create a single Dataflow job that handles all destinations
resource "google_dataflow_job" "payment_pipeline" {
  name              = "payment-processing-${var.environment}"
  template_gcs_path = var.template_path
  temp_gcs_location = var.temp_gcs_location
  
  parameters = {
    input_subscription = var.input_subscription
    project           = var.project_id
    environment       = var.environment
    region           = var.region
  }
  
  on_delete = "cancel"
}
