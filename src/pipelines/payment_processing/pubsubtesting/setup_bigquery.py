from google.cloud import bigquery
from google.api_core import exceptions
import logging
from typing import List

class BigQuerySetup:
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)

    def get_schema(self) -> List[bigquery.SchemaField]:
        """Define BigQuery table schema"""
        return [
            bigquery.SchemaField("transaction_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("payment_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("amount", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("currency", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("sender_account", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("receiver_account", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("error_message", "STRING"),
            bigquery.SchemaField("metadata", "STRING"),
        ]

    def create_dataset(self, dataset_id: str) -> bool:
        """Create BigQuery dataset if it doesn't exist"""
        dataset_ref = f"{self.project_id}.{dataset_id}"
        
        try:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            self.client.create_dataset(dataset, exists_ok=True)
            logging.info(f"Dataset {dataset_id} is ready")
            return True
            
        except exceptions.PermissionDenied:
            logging.error(f"Permission denied creating dataset {dataset_id}. Check IAM roles.")
            raise
        except Exception as e:
            logging.error(f"Error creating dataset {dataset_id}: {str(e)}")
            raise

    def create_table(self, dataset_id: str, table_id: str) -> bool:
        """Create BigQuery table with schema"""
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        
        try:
            # Check if table exists
            try:
                existing_table = self.client.get_table(table_ref)
                logging.warning(f"Table {table_ref} already exists")
                
                # Verify schema matches
                required_fields = {field.name: field.field_type for field in self.get_schema()}
                existing_fields = {field.name: field.field_type for field in existing_table.schema}
                
                if required_fields != existing_fields:
                    raise RuntimeError(
                        f"Table {table_ref} exists but schema doesn't match. "
                        f"Expected: {required_fields}, "
                        f"Found: {existing_fields}"
                    )
                
                logging.info(f"Table {table_ref} exists with correct schema")
                return True
                
            except exceptions.NotFound:
                # Create new table
                table = bigquery.Table(table_ref, schema=self.get_schema())
                
                # Set time partitioning
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="timestamp"
                )
                
                self.client.create_table(table)
                logging.info(f"Created table {table_ref}")
                return True
                
        except exceptions.PermissionDenied:
            logging.error(f"Permission denied creating table {table_ref}. Check IAM roles.")
            raise
        except Exception as e:
            logging.error(f"Error creating table {table_ref}: {str(e)}")
            raise

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # Configuration
    PROJECT_ID = 'agentic-experiments-446019'
    DATASET_ID = 'pipeline_data_test'
    TABLE_ID = 'payments'

    try:
        setup = BigQuerySetup(PROJECT_ID)
        setup.create_dataset(DATASET_ID)
        setup.create_table(DATASET_ID, TABLE_ID)
        logging.info("BigQuery setup completed successfully")
    except Exception as e:
        logging.error(f"BigQuery setup failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()