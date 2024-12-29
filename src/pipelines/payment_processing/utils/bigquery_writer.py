"""Simple BigQuery write pipeline."""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging
from google.cloud import bigquery
from google.api_core import exceptions
import time

class BigQueryWritePipeline:
    def __init__(self, project_id='agentic-experiments-446019', 
                 dataset='pipeline_data_test',
                 table='payments'):
        self.project_id = project_id
        self.dataset = dataset
        self.table = table
        
        # Create proper BigQuery references
        self.client = bigquery.Client(project=project_id)
        self.dataset_ref = self.client.dataset(dataset)
        self.table_ref = self.dataset_ref.table(table)
        self.table_id = f"{project_id}.{dataset}.{table}"
        
    def setup_dataset(self):
        """Create dataset if it doesn't exist"""
        try:
            # List all datasets first
            logging.info("Listing all datasets:")
            datasets = list(self.client.list_datasets())
            for dataset in datasets:
                logging.info(f"Found dataset: {dataset.dataset_id}")
            
            # Create dataset if it doesn't exist
            dataset = bigquery.Dataset(self.dataset_ref)
            dataset.location = "US"
            dataset = self.client.create_dataset(dataset, exists_ok=True)
            logging.info(f"Dataset {self.dataset_ref.dataset_id} is ready")
            
            # Wait a bit for propagation
            time.sleep(2)
            
            # Try to get the dataset to verify
            dataset = self.client.get_dataset(self.dataset_ref)
            logging.info(f"Successfully verified dataset: {dataset.dataset_id}")
            
            # Create table schema
            schema = [
                bigquery.SchemaField("transaction_id", "STRING"),
                bigquery.SchemaField("payment_type", "STRING"),
                bigquery.SchemaField("customer_id", "STRING"),
                bigquery.SchemaField("amount", "FLOAT"),
                bigquery.SchemaField("currency", "STRING"),
                bigquery.SchemaField("sender_account", "STRING"),
                bigquery.SchemaField("receiver_account", "STRING"),
                bigquery.SchemaField("timestamp", "TIMESTAMP"),
                bigquery.SchemaField("status", "STRING"),
                bigquery.SchemaField("error_message", "STRING"),
                bigquery.SchemaField("metadata", "STRING"),
            ]
            
            # Create table if it doesn't exist
            table = bigquery.Table(self.table_ref, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="timestamp"
            )
            table = self.client.create_table(table, exists_ok=True)
            logging.info(f"Table {self.table_ref.table_id} is ready")
            
            return True
            
        except Exception as e:
            logging.error(f"Error setting up dataset/table: {str(e)}")
            return False
        
    def run(self, data, custom_options=None):
        """Run the pipeline with explicit project configuration"""
        
        # First setup dataset and table
        if not self.setup_dataset():
            logging.error("Failed to setup dataset/table. Pipeline cannot proceed.")
            return False
            
        # Create temp directory
        import tempfile
        import os
        temp_dir = tempfile.mkdtemp()
        logging.info(f"Using temp directory: {temp_dir}")
        
        try:
            # Set up pipeline options
            options = PipelineOptions(
                runner='DirectRunner',  # Use DataflowRunner for production
                project=self.project_id,
                job_name='payment-processing-job',
                temp_location=temp_dir,
                **(custom_options or {})
            )
            
            # Create the pipeline
            with beam.Pipeline(options=options) as pipeline:
                # Add timestamp to show real-time processing
                (pipeline 
                 | 'Create Events' >> beam.Create(data)
                 | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                    table=self.table_ref,
                    schema='transaction_id:STRING,payment_type:STRING,customer_id:STRING,'
                           'amount:FLOAT,currency:STRING,sender_account:STRING,'
                           'receiver_account:STRING,timestamp:TIMESTAMP,status:STRING,'
                           'error_message:STRING,metadata:STRING',
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    method="STREAMING_INSERTS"
                 ))
                 
            logging.info("Pipeline completed successfully")
            return True
            
        except Exception as e:
            logging.error(f"Pipeline failed: {str(e)}")
            return False
            
        finally:
            # Clean up temp directory
            try:
                import shutil
                shutil.rmtree(temp_dir)
                logging.info(f"Cleaned up temp directory: {temp_dir}")
            except Exception as e:
                logging.warning(f"Failed to cleanup temp directory: {str(e)}")

# Usage example
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Sample data
    test_data = [
        {
            'transaction_id': 'TEST001',
            'payment_type': 'CREDIT',
            'customer_id': 'CUST001',
            'amount': 100.0,
            'currency': 'USD',
            'sender_account': 'ACC001',
            'receiver_account': 'ACC002',
            'timestamp': '2024-01-01T00:00:00Z',
            'status': 'COMPLETED',
            'error_message': '',
            'metadata': '{}'
        }
    ]
    
    # Run pipeline
    pipeline = BigQueryWritePipeline()
    pipeline.run(test_data)
