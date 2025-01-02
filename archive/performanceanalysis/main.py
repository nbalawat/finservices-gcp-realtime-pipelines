import apache_beam as beam
import logging
import argparse
from typing import Dict, Any
import json
from datetime import datetime
import pandas as pd
from pathlib import Path

from config.settings import Config
from core.client import create_bigtable_client
from analysis.processor import ResultsProcessor
from analysis.visualizer import create_visualizations

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='BigTable Query Performance Testing')
    parser.add_argument('--config', type=str, help='Path to config file')
    parser.add_argument('--output-dir', type=str, default='./results',
                       help='Directory for output files')
    parser.add_argument('--beam-runner', type=str, default='DirectRunner',
                       help='Apache Beam runner (DirectRunner or DataflowRunner)')
    return parser.parse_args()

def load_config(config_path: str) -> Config:
    """Load configuration from file or use defaults"""
    if config_path and Path(config_path).exists():
        with open(config_path, 'r') as f:
            config_dict = json.load(f)
            # Convert dict to Config object
            # Implementation depends on config structure
            return Config.from_dict(config_dict)
    return Config.get_default_config()

def setup_output_directory(output_dir: str) -> None:
    """Create output directory if it doesn't exist"""
    Path(output_dir).mkdir(parents=True, exist_ok=True)

def run_performance_test(config: Config, beam_runner: str, 
                        output_dir: str) -> None:
    """
    Run the performance test pipeline
    
    Args:
        config: Test configuration
        beam_runner: Beam runner to use
        output_dir: Directory for output files
    """
    logger.info("Starting performance test")
    
    # Create and run the pipeline
    pipeline_options = {
        'runner': beam_runner,
        'project': config.table_config.project_id,
        'job_name': f'bigtable-perf-test-{datetime.now().strftime("%Y%m%d-%H%M%S")}',
        'save_main_session': True,
        'streaming': False
    }
    
    if beam_runner == 'DataflowRunner':
        pipeline_options.update({
            'temp_location': f'gs://{config.table_config.project_id}-temp/dataflow',
            'region': 'us-central1'  # Adjust as needed
        })
    
    pipeline = create_pipeline(config)
    
    # Run the pipeline and wait for completion
    logger.info("Executing pipeline")
    result = pipeline.run()
    result.wait_until_finish()
    
    # Process metrics
    metrics_results = result.metrics().query(
        MetricsFilter().with_name('query_latency_ms'))
    
    processor = ResultsProcessor()
    results_df = processor.process_metrics(metrics_results)
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    results_df.to_csv(f"{output_dir}/results_{timestamp}.csv", index=False)
    
    with open(f"{output_dir}/summary_{timestamp}.txt", 'w') as f:
        f.write(processor.get_summary_report())
    
    # Create visualizations
    visualizations = create_visualizations(results_df)
    # Save visualizations (implementation depends on visualization library)
    
    logger.info(f"Results saved to {output_dir}")

def main():
    """Main entry point"""
    args = parse_arguments()
    
    # Load configuration
    config = load_config(args.config)
    
    # Setup output directory
    setup_output_directory(args.output_dir)
    
    try:
        # Run performance test
        run_performance_test(config, args.beam_runner, args.output_dir)
        logger.info("Performance test completed successfully")
        
    except Exception as e:
        logger.error(f"Performance test failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()