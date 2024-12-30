import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, SetupOptions, WorkerOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms import trigger
from typing import Dict, Any, List
import logging
from bigtable_setup import BigTableSetup, setup_bigtable
from payment_generator import PaymentGeneratorFn, FormatForBigTableFn
from bigtable_writer import WriteToBigtable
import time

class PaymentPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Pipeline configs
        parser.add_argument('--project_id', required=True, help='GCP Project ID')
        parser.add_argument('--bigtable_instance', required=True, help='BigTable instance ID')
        
        # Data generation configs
        parser.add_argument('--num_records', type=int, default=200_000_000)
        parser.add_argument('--num_customers', type=int, default=10_000)
        parser.add_argument('--start_date', default='2024-03-01')
        parser.add_argument('--end_date', default='2024-03-31')

def run_pipeline(pipeline_options: PaymentPipelineOptions):
    # Verify BigTable setup
    logger = logging.getLogger(__name__)
    start_time = time.time()
    
    try:
        logger.info("=== Pipeline Execution Started ===")
        logger.info(f"Start Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Pipeline Options: {pipeline_options.get_all_options()}")
        
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
        logger.info("Attempting to create and submit Dataflow job...")
        
        with beam.Pipeline(options=pipeline_options) as pipeline:
            logger.info("Pipeline object created successfully")
            
            # Generate payments
            logger.info("Creating main data transform...")
      
            main_data = (pipeline
                        | 'Create Events' >> beam.Create(range(pipeline_options.num_records))
                        | 'Window' >> beam.WindowInto(FixedWindows(60))  # 60-second windows
                        | 'Reshuffle' >> beam.Reshuffle()
                        | 'Generate Payments' >> beam.ParDo(PaymentGeneratorFn(
                            pipeline_options.num_customers,
                            pipeline_options.start_date,
                            pipeline_options.end_date
                        )))
            
            logger.info("Main data transform created successfully")
            
            # Write to different BigTable tables
            for table_name, row_key_format in bt_setup.TABLE_CONFIGS.items():
                logger.info(f"Setting up pipeline for table: {table_name}")
                try:
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
                    logger.info(f"Pipeline branch for table {table_name} created successfully")
                except Exception as e:
                    logger.error(f"Error setting up pipeline for table {table_name}: {str(e)}", exc_info=True)
                    raise
            
            logger.info("Pipeline definition completed successfully")
            logger.info("Starting pipeline execution...")
        
        logger.info("Pipeline execution completed")
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}", exc_info=True)
        raise
    
    end_time = time.time()
    duration = end_time - start_time
    logger.info("=== Pipeline Execution Details ===")
    logger.info(f"End Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Total Duration: {duration:.2f} seconds")
    logger.info(f"Records per Second: {pipeline_options.num_records/duration:.2f}")

def main():
    # Parse the pipeline options
    pipeline_options = PaymentPipelineOptions()
    
    # Configure standard options
    standard_options = pipeline_options.view_as(StandardOptions)
    standard_options.streaming = False
    
    # Configure worker options
    worker_options = pipeline_options.view_as(WorkerOptions)
    
    # Configure setup options
    setup_options = pipeline_options.view_as(SetupOptions)
    if pipeline_options.get_all_options().get('setup_file'):
        setup_options.setup_file = pipeline_options.get_all_options().get('setup_file')
        
    # Log pipeline configuration
    logger = logging.getLogger(__name__)
    logger.info("=== Pipeline Configuration ===")
    logger.info(f"Project ID: {pipeline_options.project_id}")
    logger.info(f"BigTable Instance: {pipeline_options.bigtable_instance}")
    logger.info(f"Number of Records: {pipeline_options.num_records:,}")
    logger.info(f"Number of Customers: {pipeline_options.num_customers:,}")
    logger.info(f"Date Range: {pipeline_options.start_date} to {pipeline_options.end_date}")
    
    # Log Dataflow-specific options
    logger.info("=== Dataflow Configuration ===")
    logger.info(f"Runner: {standard_options.runner}")
    logger.info(f"Project: {pipeline_options.get_all_options().get('project')}")
    logger.info(f"Region: {pipeline_options.get_all_options().get('region')}")
    logger.info(f"Temp Location: {pipeline_options.get_all_options().get('temp_location')}")
    logger.info(f"Staging Location: {pipeline_options.get_all_options().get('staging_location')}")
    logger.info(f"Setup File: {setup_options.setup_file}")
    
    # Run the pipeline
    run_pipeline(pipeline_options)

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    main()