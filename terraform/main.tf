provider "google" {
  project = var.project_id
  region  = var.region
}

# Enable required GCP services first
module "services" {
  source             = "./modules/services"
  project_id         = var.project_id
  enabled_subscribers = var.enabled_subscribers
}

module "pubsub" {
  source             = "./modules/pubsub"
  project_id         = var.project_id
  environment        = var.environment
  enabled_subscribers = var.enabled_subscribers
  
  depends_on = [module.services]
}

module "storage" {
  source             = "./modules/storage"
  project_id         = var.project_id
  environment        = var.environment
  region             = var.region
  enabled_subscribers = var.enabled_subscribers
  
  depends_on = [module.services]
}

module "databases" {
  source             = "./modules/databases"
  project_id         = var.project_id
  environment        = var.environment
  region             = var.region
  enabled_subscribers = var.enabled_subscribers
  
  depends_on = [module.services]
}

module "dataflow" {
  source                    = "./modules/dataflow"
  project_id               = var.project_id
  environment              = var.environment
  enabled_subscribers      = var.enabled_subscribers
  temp_gcs_location       = module.storage.temp_bucket_url
  dataflow_template_path  = "gs://dataflow-templates/latest/PubSub_to_BigTable"  # Example template
  bigtable_subscription_path = try(module.pubsub.bigtable_subscription_path, "")
  bigquery_subscription_path = try(module.pubsub.bigquery_subscription_path, "")
  gcs_subscription_path      = try(module.pubsub.gcs_subscription_path, "")
  alloydb_subscription_path  = try(module.pubsub.alloydb_subscription_path, "")
  cloudsql_subscription_path = try(module.pubsub.cloudsql_subscription_path, "")
  
  depends_on = [module.services, module.pubsub, module.storage, module.databases]
}
