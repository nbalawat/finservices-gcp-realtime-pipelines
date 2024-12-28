#!/usr/bin/env python3
"""Script to set up GCP infrastructure for payment processing pipeline."""

import argparse
import logging
from google.cloud import pubsub_v1
from google.cloud import bigquery
from google.cloud import bigtable
from google.cloud import bigtable_admin_v2
from google.cloud.bigtable import column_family
from google.api_core import retry
from google.cloud import service_usage_v1
from google.cloud import resource_manager_v3
from google.cloud import iam_v1
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class InfrastructureSetup:
    """Set up GCP infrastructure for payment processing pipeline."""

    def __init__(self, project_id: str, region: str = "us-central1"):
        self.project_id = project_id
        self.region = region
        self.project_number = self._get_project_number()
        self.pubsub_publisher = pubsub_v1.PublisherClient()
        self.pubsub_subscriber = pubsub_v1.SubscriberClient()
        self.bigquery_client = bigquery.Client(project=project_id)
        self.bigtable_client = bigtable.Client(project=project_id, admin=True)
        self.bigtable_instance_admin_client = bigtable_admin_v2.BigtableInstanceAdminClient()
        self.service_usage_client = service_usage_v1.ServiceUsageClient()
        self.iam_client = iam_v1.IAMClient()

    def _get_project_number(self) -> str:
        """Get project number from project ID."""
        client = resource_manager_v3.ProjectsClient()
        project = client.get_project(name=f"projects/{self.project_id}")
        return project.name.split('/')[-1]

    def enable_apis(self):
        """Enable required GCP APIs."""
        required_apis = [
            "dataflow.googleapis.com",
            "pubsub.googleapis.com",
            "bigquery.googleapis.com",
            "bigtable.googleapis.com",
            "storage.googleapis.com",  # Required for Dataflow temp storage
            "logging.googleapis.com",
            "monitoring.googleapis.com"
        ]

        parent = f"projects/{self.project_number}"
        
        for api in required_apis:
            request = service_usage_v1.EnableServiceRequest(
                name=f"{parent}/services/{api}"
            )
            
            try:
                operation = self.service_usage_client.enable_service(request=request)
                operation.result()  # Wait for operation to complete
                logger.info(f"Enabled {api}")
            except Exception as e:
                logger.info(f"API {api} already enabled or error: {e}")

    def create_service_account(self, name: str):
        """Create service account for Dataflow pipeline."""
        client = iam_v1.IAMClient()
        
        # Create service account
        service_account_path = f"projects/{self.project_id}"
        service_account = iam_v1.ServiceAccount()
        service_account.display_name = "Dataflow Payment Pipeline"
        service_account.description = "Service account for payment processing Dataflow pipeline"
        
        try:
            service_account = client.create_service_account(
                request={
                    "name": service_account_path,
                    "account_id": name,
                    "service_account": service_account,
                }
            )
            logger.info(f"Created service account: {service_account.email}")
        except Exception as e:
            logger.info(f"Service account already exists or error: {e}")
            return f"{name}@{self.project_id}.iam.gserviceaccount.com"

        # Grant necessary roles
        required_roles = [
            "roles/dataflow.worker",
            "roles/bigquery.dataEditor",
            "roles/pubsub.publisher",
            "roles/pubsub.subscriber",
            "roles/bigtable.user",
            "roles/storage.objectViewer"
        ]

        for role in required_roles:
            policy = {
                "bindings": [
                    {
                        "role": role,
                        "members": [f"serviceAccount:{service_account.email}"]
                    }
                ]
            }
            
            try:
                client.set_iam_policy(request={
                    "resource": f"projects/{self.project_id}",
                    "policy": policy
                })
                logger.info(f"Granted {role} to {service_account.email}")
            except Exception as e:
                logger.info(f"Role {role} already granted or error: {e}")

        return service_account.email

    def create_pubsub_topic_and_subscription(self, topic_id: str, subscription_id: str):
        """Create Pub/Sub topic and subscription."""
        topic_path = self.pubsub_publisher.topic_path(self.project_id, topic_id)
        subscription_path = self.pubsub_subscriber.subscription_path(
            self.project_id, subscription_id
        )

        try:
            topic = self.pubsub_publisher.create_topic(request={"name": topic_path})
            logger.info(f"Created topic: {topic.name}")
        except Exception as e:
            logger.info(f"Topic already exists or error: {e}")

        try:
            subscription = self.pubsub_subscriber.create_subscription(
                request={
                    "name": subscription_path,
                    "topic": topic_path,
                    "ack_deadline_seconds": 60,
                    "message_retention_duration": {"seconds": 604800},  # 7 days
                    "enable_message_ordering": True
                }
            )
            logger.info(f"Created subscription: {subscription.name}")
        except Exception as e:
            logger.info(f"Subscription already exists or error: {e}")

    def create_bigquery_dataset_and_tables(self, dataset_id: str):
        """Create BigQuery dataset and tables."""
        dataset_ref = self.bigquery_client.dataset(dataset_id)
        
        try:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = self.region
            dataset = self.bigquery_client.create_dataset(dataset)
            logger.info(f"Created dataset: {dataset.dataset_id}")
        except Exception as e:
            logger.info(f"Dataset already exists or error: {e}")

        # Create payments table
        payments_schema = [
            bigquery.SchemaField("transaction_id", "STRING"),
            bigquery.SchemaField("payment_type", "STRING"),
            bigquery.SchemaField("customer_id", "STRING"),
            bigquery.SchemaField("amount", "FLOAT"),
            bigquery.SchemaField("currency", "STRING"),
            bigquery.SchemaField("sender_account", "STRING"),
            bigquery.SchemaField("receiver_account", "STRING"),
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("metadata", "STRING")
        ]

        table_ref = dataset_ref.table("payments")
        table = bigquery.Table(table_ref, schema=payments_schema)
        
        try:
            table = self.bigquery_client.create_table(table)
            logger.info(f"Created table: {table.table_id}")
        except Exception as e:
            logger.info(f"Table already exists or error: {e}")

        # Create error table
        error_schema = [
            bigquery.SchemaField("error_type", "STRING"),
            bigquery.SchemaField("error_message", "STRING"),
            bigquery.SchemaField("raw_data", "STRING"),
            bigquery.SchemaField("timestamp", "TIMESTAMP")
        ]

        error_table_ref = dataset_ref.table("payment_errors")
        error_table = bigquery.Table(error_table_ref, schema=error_schema)
        
        try:
            error_table = self.bigquery_client.create_table(error_table)
            logger.info(f"Created error table: {error_table.table_id}")
        except Exception as e:
            logger.info(f"Error table already exists or error: {e}")

    def create_bigtable_instance_and_tables(self, instance_id: str, cluster_id: str):
        """Create BigTable instance and tables."""
        instance = self.bigtable_client.instance(instance_id)
        
        if not instance.exists():
            logger.info(f"Creating BigTable instance: {instance_id}")
            cluster = bigtable.cluster.Cluster(
                cluster_id,
                instance,
                location_id=self.region,
                serve_nodes=1,
                default_storage_type=bigtable_admin_v2.StorageType.SSD
            )
            instance.create(clusters=[cluster])
            logger.info(f"Created BigTable instance: {instance_id}")
        
        # Define column families
        column_families_config = {
            'payment_info': column_family.MaxVersionsGCRule(1),
            'accounts': column_family.MaxVersionsGCRule(1),
            'metadata': column_family.MaxVersionsGCRule(1)
        }

        # Create tables with different row key strategies
        table_suffixes = [
            'by_customer_time_type',
            'by_customer_time',
            'by_time_customer',
            'by_transaction',
            'by_customer_type'
        ]

        for suffix in table_suffixes:
            table_id = f"payments_{suffix}"
            table = instance.table(table_id)
            
            if not table.exists():
                logger.info(f"Creating table: {table_id}")
                table.create(column_families=column_families_config)
                logger.info(f"Created table: {table_id}")
            else:
                logger.info(f"Table already exists: {table_id}")

def main():
    parser = argparse.ArgumentParser(description="Set up GCP infrastructure")
    parser.add_argument("--project-id", required=True, help="GCP Project ID")
    parser.add_argument("--region", default="us-central1", help="GCP Region")
    parser.add_argument("--topic-id", default="payment-events", help="Pub/Sub Topic ID")
    parser.add_argument("--subscription-id", default="payment-subscription", help="Pub/Sub Subscription ID")
    parser.add_argument("--dataset-id", default="payment_processing", help="BigQuery Dataset ID")
    parser.add_argument("--bigtable-instance", default="payment-processing", help="BigTable Instance ID")
    parser.add_argument("--bigtable-cluster", default="payment-processing-cluster", help="BigTable Cluster ID")
    parser.add_argument("--service-account", default="payment-pipeline-sa", help="Service Account ID")

    args = parser.parse_args()

    setup = InfrastructureSetup(args.project_id, args.region)

    # Enable required APIs
    logger.info("Enabling required APIs...")
    setup.enable_apis()

    # Create service account
    logger.info("Creating service account...")
    service_account_email = setup.create_service_account(args.service_account)
    logger.info(f"Using service account: {service_account_email}")

    # Create all infrastructure components
    logger.info("Setting up Pub/Sub...")
    setup.create_pubsub_topic_and_subscription(args.topic_id, args.subscription_id)

    logger.info("Setting up BigQuery...")
    setup.create_bigquery_dataset_and_tables(args.dataset_id)

    logger.info("Setting up BigTable...")
    setup.create_bigtable_instance_and_tables(args.bigtable_instance, args.bigtable_cluster)

    logger.info("Infrastructure setup complete!")
    logger.info(f"\nSetup Summary:")
    logger.info(f"- Service Account: {service_account_email}")
    logger.info(f"- Pub/Sub Topic: projects/{args.project_id}/topics/{args.topic_id}")
    logger.info(f"- Pub/Sub Subscription: projects/{args.project_id}/subscriptions/{args.subscription_id}")
    logger.info(f"- BigQuery Dataset: {args.project_id}:{args.dataset_id}")
    logger.info(f"- BigTable Instance: {args.bigtable_instance}")

if __name__ == "__main__":
    main()
