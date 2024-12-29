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
