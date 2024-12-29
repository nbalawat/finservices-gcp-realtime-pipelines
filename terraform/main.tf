provider "google" {
  project = var.project_id
  region  = var.region
}

# Enable required GCP services first
module "services" {
  source             = "./modules/services"
  project_id         = var.project_id
}

# Create IAM resources
module "iam" {
  source = "./modules/iam"
  project_id = var.project_id
  environment = var.environment
  depends_on = [module.services]
}

module "pubsub" {
  source             = "./modules/pubsub"
  project_id         = var.project_id
  environment        = var.environment
  service_account = module.iam.service_account_email
  
  depends_on = [module.services, module.iam]
}

module "storage" {
  source             = "./modules/storage"
  project_id         = var.project_id
  environment        = var.environment
  service_account = module.iam.service_account_email
  
  depends_on = [module.services, module.iam]
}

module "databases" {
  source             = "./modules/databases"
  project_id         = var.project_id
  environment        = var.environment
  service_account = module.iam.service_account_email
  
  depends_on = [module.services, module.iam]
}

# We'll add Dataflow module later
# module "dataflow" {
#   source             = "./modules/dataflow"
#   project_id         = var.project_id
#   environment        = var.environment
#   service_account    = module.iam.service_account_email
#   region            = var.region
#   depends_on = [module.services, module.iam]
# }
