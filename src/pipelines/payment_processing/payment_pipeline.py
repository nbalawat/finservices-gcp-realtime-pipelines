"""Main payment processing pipeline."""

import apache_beam as beam
import logging
import os
import json
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, SetupOptions
from .common.base_pipeline import PaymentJsonParsingDoFn
from .config import PaymentPipelineOptions
from .transformations.payment_transforms import PaymentTransform, PrepareForStorage
from src.pipelines.payment_processing.common.env import load_env, get_env_var
from .utils import verify_credentials
from .utils.bigquery_writer import BigQueryWritePipeline as BQWriter
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, Conflict

def log_element(element, prefix=""):
    """Log an element with an optional prefix."""
    logging.info(f"{prefix}: {element}")
    return element

class PaymentPipeline:
    """Unified pipeline for processing multiple payment types."""

    def __init__(self, pipeline_options=None):
        """Initialize the pipeline."""
        load_env()
        
        # Set project ID in environment
        os.environ['GOOGLE_CLOUD_PROJECT'] = get_env_var('PROJECT_ID')
        
        # Log all relevant environment variables
        logging.info(f"PROJECT_ID: {get_env_var('PROJECT_ID')}")
        logging.info(f"PUBSUB_SUBSCRIPTION: {get_env_var('PUBSUB_SUBSCRIPTION')}")
        logging.info(f"BIGQUERY_PAYMENTS_TABLE: {get_env_var('BIGQUERY_PAYMENTS_TABLE')}")
        logging.info(f"TEMP_LOCATION: {get_env_var('TEMP_LOCATION')}")
        logging.info(f"STAGING_LOCATION: {get_env_var('STAGING_LOCATION')}")
        
        # Set up pipeline options as a dictionary
        options = {
            'project': get_env_var('PROJECT_ID'),
            'temp_location': get_env_var('TEMP_LOCATION'),
            'staging_location': get_env_var('STAGING_LOCATION'),
            'runner': 'DirectRunner',
            'streaming': True
        }

        # Add service account if specified
        try:
            service_account = get_env_var('SERVICE_ACCOUNT_EMAIL')
            options['service_account_email'] = service_account
            logging.info(f"Using service account: {service_account}")
        except ValueError:
            logging.warning("No service account specified, using default credentials")

        # Create pipeline options
        self.pipeline_options = pipeline_options or PipelineOptions.from_dictionary(options)
        self.pipeline_options.view_as(StandardOptions).streaming = True
        self.pipeline_options.view_as(SetupOptions).save_main_session = True
        
        # Store important variables
        self.input_subscription = get_env_var('PUBSUB_SUBSCRIPTION')
        self.output_table = get_env_var('BIGQUERY_PAYMENTS_TABLE')
        self.error_table = get_env_var('BIGQUERY_ERRORS_TABLE')
        
        # Initialize BigQuery writer
        project_id, dataset, table = self.output_table.split('.')
        self.project_id = project_id
        self.dataset = dataset
        self.table = table
        
        logging.info(f"BigQuery output table: {self.output_table}")

    def build_and_run(self):
        """Build and run the pipeline."""
        # First verify credentials
        if not verify_credentials():
            logging.error("Failed to verify GCP credentials. Pipeline cannot proceed.")
            return False
            
        try:
            # Create test data instead of reading from Pub/Sub for now
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
            
            # Set up pipeline options
            options = PipelineOptions(
                runner='DirectRunner',  # Use DataflowRunner for production
                project=self.project_id,
                job_name='payment-processing-job',
                temp_location=f'gs://{self.project_id}-temp/temp',  # Adjust bucket name
            )
            
            # Create the pipeline
            with beam.Pipeline(options=options) as pipeline:
                # Create events from test data
                events = pipeline | 'Create Events' >> beam.Create(test_data)
                
                # Write to BigQuery
                _ = events | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                    table=f"{self.project_id}.{self.dataset}.{self.table}",
                    schema='transaction_id:STRING,payment_type:STRING,customer_id:STRING,'
                           'amount:FLOAT,currency:STRING,sender_account:STRING,'
                           'receiver_account:STRING,timestamp:TIMESTAMP,status:STRING,'
                           'error_message:STRING,metadata:STRING',
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    method="STREAMING_INSERTS",
                    additional_bq_parameters={
                        'timePartitioning': {
                            'type': 'DAY',
                            'field': 'timestamp'
                        }
                    }
                )

        except Exception as e:
            logging.error(f"Pipeline error: {str(e)}", exc_info=True)
            raise

def run():
    """Run the payment pipeline."""
    pipeline = PaymentPipeline()
    pipeline.build_and_run()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
