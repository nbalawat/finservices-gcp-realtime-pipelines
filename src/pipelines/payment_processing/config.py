"""Configuration for the payment processing pipeline."""

import os
from apache_beam.options.pipeline_options import PipelineOptions
from dotenv import load_dotenv

class PaymentPipelineOptions(PipelineOptions):
    """Pipeline options for payment processing."""

    @classmethod
    def _add_argparse_args(cls, parser):
        # Input configuration
        parser.add_argument(
            '--input_subscription',
            help='Input Pub/Sub subscription',
            default=os.getenv('INPUT_SUBSCRIPTION')
        )

        # BigQuery configuration
        parser.add_argument(
            '--output_table',
            help='Output BigQuery table: PROJECT:DATASET.TABLE',
            default=os.getenv('OUTPUT_TABLE')
        )
        parser.add_argument(
            '--error_table',
            help='Error BigQuery table: PROJECT:DATASET.TABLE',
            default=os.getenv('ERROR_TABLE')
        )

        # BigTable configuration
        parser.add_argument(
            '--bigtable_instance',
            help='BigTable instance ID',
            default=os.getenv('BIGTABLE_INSTANCE')
        )
        parser.add_argument(
            '--bigtable_table_prefix',
            help='Prefix for BigTable tables',
            default=os.getenv('BIGTABLE_TABLE_PREFIX', 'payments')
        )

        # BigTable table names
        parser.add_argument(
            '--table_customer_time_type',
            help='Table for customer-time-type queries',
            default=os.getenv('TABLE_CUSTOMER_TIME_TYPE', 'payments_by_customer_time_type')
        )
        parser.add_argument(
            '--table_customer_time',
            help='Table for customer-time queries',
            default=os.getenv('TABLE_CUSTOMER_TIME', 'payments_by_customer_time')
        )
        parser.add_argument(
            '--table_time_customer',
            help='Table for time-customer queries',
            default=os.getenv('TABLE_TIME_CUSTOMER', 'payments_by_time_customer')
        )
        parser.add_argument(
            '--table_transaction',
            help='Table for transaction queries',
            default=os.getenv('TABLE_TRANSACTION', 'payments_by_transaction')
        )
        parser.add_argument(
            '--table_customer_type',
            help='Table for customer-type queries',
            default=os.getenv('TABLE_CUSTOMER_TYPE', 'payments_by_customer_type')
        )

    @property
    def bigtable_tables(self):
        """Get mapping of table types to their names."""
        return {
            'customer_time_type': self.table_customer_time_type,
            'customer_time': self.table_customer_time,
            'time_customer': self.table_time_customer,
            'transaction': self.table_transaction,
            'customer_type': self.table_customer_type
        }
