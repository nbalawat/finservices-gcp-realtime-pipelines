"""Pipeline configuration."""

from apache_beam.options.pipeline_options import PipelineOptions

class PaymentPipelineOptions(PipelineOptions):
    """Custom options for the payment pipeline."""

    @classmethod
    def _add_argparse_args(cls, parser):
        # Input and output options
        parser.add_argument(
            '--input_subscription',
            help='Input Pub/Sub subscription'
        )
        parser.add_argument(
            '--output_table',
            help='Output BigQuery table'
        )
        parser.add_argument(
            '--error_table',
            help='Error BigQuery table'
        )
        parser.add_argument(
            '--bigtable_instance',
            help='BigTable instance ID'
        )

    @property
    def bigtable_tables(self):
        """Return a mapping of output names to BigTable table IDs."""
        return {
            'customer_time_type': 'payments_by_customer_time_type',
            'customer_time': 'payments_by_customer_time',
            'time_customer': 'payments_by_time_customer',
            'transaction': 'payments_by_transaction',
            'customer_type': 'payments_by_customer_type'
        }
