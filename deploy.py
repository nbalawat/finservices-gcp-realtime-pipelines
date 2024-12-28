import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from src.pipelines.payment_processing.transformations.ach_pipeline import ACHPipeline
from src.pipelines.payment_processing.config import PipelineConfig
import sys

def deploy_pipeline(args):
    """Deploy a specific pipeline with the given configuration"""
    try:
        # Initialize configuration
        config = PipelineConfig(args.env_file)
        
        # Validate configuration
        config.validate()
        
        # Override configuration with command-line arguments if provided
        if args.project:
            config.project_id = args.project
        if args.region:
            config.region = args.region
        if args.bucket:
            config.gcs_bucket = args.bucket
        
        # Get pipeline options for the specific payment type
        pipeline_options = PipelineOptions(
            flags=[],
            **config.get_pipeline_options(args.payment_type)
        )

        # Initialize and run the appropriate pipeline
        if args.payment_type == 'ach':
            pipeline = ACHPipeline(pipeline_options)
        else:
            raise ValueError(f"Unsupported payment type: {args.payment_type}")

        pipeline.build_and_run()
        
    except Exception as e:
        print(f"Error deploying pipeline: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Deploy payment processing pipeline')
    
    # Configuration source
    parser.add_argument('--env-file', help='Path to .env file')
    
    # Optional overrides
    parser.add_argument('--project', help='Override GCP project ID')
    parser.add_argument('--region', help='Override GCP region')
    parser.add_argument('--bucket', help='Override GCS bucket')
    
    # Required arguments
    parser.add_argument('--payment-type', required=True, choices=['ach'],
                      help='Payment type to process')

    args = parser.parse_args()
    deploy_pipeline(args)
