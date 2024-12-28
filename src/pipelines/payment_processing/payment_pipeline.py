"""Main payment processing pipeline."""

import apache_beam as beam
import logging
from apache_beam.options.pipeline_options import PipelineOptions
from .common.base_pipeline import PaymentJsonParsingDoFn
from .config import PaymentPipelineOptions
from .transformations.payment_transforms import PaymentTransform, PrepareForStorage

class PaymentPipeline:
    """Unified pipeline for processing multiple payment types."""
    
    def __init__(self):
        self.pipeline_options = PipelineOptions()
        self.custom_options = self.pipeline_options.view_as(PaymentPipelineOptions)

    def build_and_run(self):
        """Build and run the payment processing pipeline."""
        with beam.Pipeline(options=self.pipeline_options) as pipeline:
            # Read messages from Pub/Sub
            messages = pipeline | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(
                subscription=self.custom_options.input_subscription
            )

            # Parse JSON and handle errors
            parsed_messages, errors = (
                messages
                | "Parse JSON" >> beam.ParDo(PaymentJsonParsingDoFn())
                .with_outputs('errors', main='main')
            )

            # Write parsing errors to BigQuery
            _ = (
                errors
                | "Write Parse Errors" >> beam.io.WriteToBigQuery(
                    self.custom_options.error_table,
                    schema='error_type:STRING,error_message:STRING,raw_data:STRING,timestamp:TIMESTAMP',
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                )
            )

            # Transform payments based on type
            transformed_payments = (
                parsed_messages
                | "Transform Payments" >> beam.ParDo(PaymentTransform())
                | "Filter Invalid" >> beam.Filter(lambda x: x is not None)
            )

            # Prepare data for storage systems
            prepared_data = (
                transformed_payments
                | "Prepare for Storage" >> beam.ParDo(PrepareForStorage())
                .with_outputs(
                    'customer_time_type',
                    'customer_time',
                    'time_customer',
                    'transaction',
                    'customer_type',
                    main='main'  # main output is BigQuery data
                )
            )

            # Write to BigQuery
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

            # Write to different BigTable tables using configured table names
            for output_name, table_id in self.custom_options.bigtable_tables.items():
                _ = (
                    getattr(prepared_data, output_name)
                    | f"Write to BigTable ({output_name})" >> beam.io.gcp.bigtable.WriteToBigTable(
                        project_id=self.pipeline_options.project,
                        instance_id=self.custom_options.bigtable_instance,
                        table_id=table_id
                    )
                )

def run():
    """Run the payment pipeline."""
    pipeline = PaymentPipeline()
    pipeline.build_and_run()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
