import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import logging
from bigtable_setup import BigTableSetup, setup_bigtable
from payment_generator import PaymentGeneratorFn, FormatForBigTableFn
from bigtable_writer import WriteToBigtable

class PaymentPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Pipeline configs
        parser.add_argument('--project_id', required=True, help='GCP Project ID')
        parser.add_argument('--bigtable_instance', required=True, help='BigTable instance ID')
        
        # Data generation configs
        parser.add_argument('--num_records', type=int, default=200_000_000,
                          help='Number of records to generate')
        parser.add_argument('--num_customers', type=int, default=10_000,
                          help='Number of unique customers')
        parser.add_argument('--start_date', default='2024-03-01',
                          help='Start date for payment generation (YYYY-MM-DD)')
        parser.add_argument('--end_date', default='2024-03-31',
                          help='End date for payment generation (YYYY-MM-DD)')

def run_pipeline(pipeline_options: PaymentPipelineOptions):
    # Verify BigTable setup
    logger = logging.getLogger(__name__)
    logger.info("Verifying BigTable setup...")
    setup_bigtable(
        pipeline_options.project_id,
        pipeline_options.bigtable_instance
    )
    
    # Get table configurations
    logger.info("Getting BigTable configurations...")
    bt_setup = BigTableSetup(
        pipeline_options.project_id,
        pipeline_options.bigtable_instance
    )
    
    logger.info(f"Starting pipeline with {pipeline_options.num_records:,} records")
    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Generate payments
        main_data = (pipeline
                    | 'Create Events' >> beam.Create(range(pipeline_options.num_records))
                    | 'Generate Payments' >> beam.ParDo(PaymentGeneratorFn(
                        pipeline_options.num_customers,
                        pipeline_options.start_date,
                        pipeline_options.end_date
                    )))
        
        # Write to different BigTable tables
        for table_name, row_key_format in bt_setup.TABLE_CONFIGS.items():
            logger.info(f"Setting up pipeline for table: {table_name}")
            table_data = (main_data
                         | f'Format {table_name}' >> beam.ParDo(
                             FormatForBigTableFn(row_key_format))
                         | f'Write {table_name}' >> beam.ParDo(
                             WriteToBigtable(
                                 project_id=pipeline_options.project_id,
                                 instance_id=pipeline_options.bigtable_instance,
                                 table_id=table_name
                             ))
                        )

def main():
    # Parse the pipeline options
    pipeline_options = PaymentPipelineOptions()
    
    # Configure standard options
    standard_options = pipeline_options.view_as(StandardOptions)
    standard_options.streaming = False
    
    # Log pipeline configuration
    logger = logging.getLogger(__name__)
    logger.info("Pipeline Configuration:")
    logger.info(f"  Project ID: {pipeline_options.project_id}")
    logger.info(f"  BigTable Instance: {pipeline_options.bigtable_instance}")
    logger.info(f"  Number of Records: {pipeline_options.num_records:,}")
    logger.info(f"  Date Range: {pipeline_options.start_date} to {pipeline_options.end_date}")
    
    # Run the pipeline
    run_pipeline(pipeline_options)

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    main()