"""Utility to explicitly create BigQuery dataset and table."""

from google.cloud import bigquery
from google.api_core import exceptions
import logging
import time

def create_dataset():
    """Create BigQuery dataset and table explicitly"""
    client = bigquery.Client(project='agentic-experiments-446019')
    dataset_id = 'pipeline_data_test'
    
    print("\n=== Creating BigQuery Dataset and Table ===")
    
    # Construct dataset reference
    dataset = bigquery.Dataset(f"{client.project}.{dataset_id}")
    dataset.location = "US"
    
    try:
        # First try to get the dataset
        try:
            existing_dataset = client.get_dataset(dataset)
            print(f"\nFound existing dataset: {existing_dataset.dataset_id}")
            print("Deleting existing dataset...")
            
            # Delete if exists
            client.delete_dataset(
                dataset.dataset_id,
                delete_contents=True,
                not_found_ok=True
            )
            print("✅ Deleted existing dataset")
            
            # Wait a bit for deletion to propagate
            time.sleep(2)
            
        except exceptions.NotFound:
            print("\nNo existing dataset found")
        
        # Create new dataset
        dataset = client.create_dataset(dataset, exists_ok=True)
        print(f"✅ Created dataset: {client.project}.{dataset_id}")
        
        # Wait a bit for creation to propagate
        time.sleep(2)
        
        # Define schema for the table
        schema = [
            bigquery.SchemaField("transaction_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("payment_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("amount", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("currency", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("sender_account", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("receiver_account", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("metadata", "STRING", mode="NULLABLE")
        ]
        
        # Create table with time partitioning
        table_id = f"{client.project}.{dataset_id}.payments"
        table = bigquery.Table(table_id, schema=schema)
        
        # Set time partitioning
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="timestamp"
        )
        
        # Create the table
        table = client.create_table(table, exists_ok=True)
        print(f"✅ Created table: {table_id}")
        print(f"  Schema: {[field.name for field in table.schema]}")
        print(f"  Partitioning: {table.time_partitioning.type_} on {table.time_partitioning.field}")
        
        # Verify we can query the table
        query = f"""
        SELECT COUNT(*) as count
        FROM `{table_id}`
        """
        
        query_job = client.query(query)
        results = list(query_job.result())
        print(f"✅ Successfully queried table: {results[0].count} rows")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    if create_dataset():
        print("\n✨ Dataset and table created successfully!")
    else:
        print("\n❌ Failed to create dataset and table")
