"""Main payment processing pipeline."""

import apache_beam as beam
import logging
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from .common.base_pipeline import PaymentJsonParsingDoFn
from .config import PaymentPipelineOptions
from .transformations.payment_transforms import PaymentTransform, PrepareForStorage
from src.pipelines.payment_processing.common.env import load_env, get_env_var

class PaymentPipeline:
    """Unified pipeline for processing multiple payment types."""

    def __init__(self):
        # Load environment variables
        load_env()
        
        # Create pipeline options
        pipeline_args = [
            f"--project={get_env_var('PROJECT_ID')}",
            f"--input_subscription={get_env_var('PUBSUB_SUBSCRIPTION')}",
            f"--output_table={get_env_var('BIGQUERY_PAYMENTS_TABLE')}",
            f"--error_table={get_env_var('BIGQUERY_ERRORS_TABLE')}",
            f"--temp_location={get_env_var('TEMP_LOCATION')}",
            f"--staging_location={get_env_var('STAGING_LOCATION')}",
            "--runner=DirectRunner",
            "--streaming"  # Enable streaming mode
        ]
        
        self.pipeline_options = PipelineOptions(pipeline_args)
        # Explicitly set streaming mode
        self.pipeline_options.view_as(StandardOptions).streaming = True
        self.custom_options = self.pipeline_options.view_as(PaymentPipelineOptions)

    def build_and_run(self):
        """Build and run the payment processing pipeline."""
        with beam.Pipeline(options=self.pipeline_options) as pipeline:
            # Read messages from Pub/Sub
            messages = pipeline | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(
                subscription=self.custom_options.input_subscription,
                with_attributes=False  # We only want the data, not the attributes
            )

            # Log raw messages for debugging
            _ = messages | "Log Raw Messages" >> beam.Map(lambda x: logging.info(f"Raw message: {x}"))

            # Parse and validate JSON messages
            parsed_messages = messages | "Parse JSON" >> beam.ParDo(PaymentJsonParsingDoFn())

            # Transform payments based on type
            transformed_payments = parsed_messages | "Transform Payments" >> beam.ParDo(PaymentTransform())

            # Prepare data for storage
            prepared_data = transformed_payments | "Prepare for Storage" >> beam.ParDo(PrepareForStorage()).with_outputs()

            # Write main output to BigQuery
            _ = (
                prepared_data.main
                | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                    self.custom_options.output_table,
                    schema='transaction_id:STRING,payment_type:STRING,customer_id:STRING,'
                           'amount:FLOAT,currency:STRING,sender_account:STRING,'
                           'receiver_account:STRING,timestamp:TIMESTAMP,status:STRING,'
                           'metadata:STRING',
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                )
            )

def run():
    """Run the payment pipeline."""
    pipeline = PaymentPipeline()
    pipeline.build_and_run()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
