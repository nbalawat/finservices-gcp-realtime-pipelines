import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, WorkerOptions
import logging

def run():
    logging.getLogger().setLevel(logging.INFO)
    
    options = PipelineOptions([
        '--runner=DataflowRunner',
        '--project=agentic-experiments-446019',
        '--region=us-east4',
        '--temp_location=gs://agentic-experiments-446019-dataflow-temp/temp',
        '--staging_location=gs://agentic-experiments-446019-dataflow-temp/staging',
        '--job_name=test-dataflow-setup',
        '--worker_machine_type=n2-standard-2',
        '--max_num_workers=2',
        '--experiments=use_runner_v2',
        '--service_account_email=beam-local-pipeline@agentic-experiments-446019.iam.gserviceaccount.com',
        '--save_main_session'
    ])
    
    # Set standard options
    standard_options = options.view_as(StandardOptions)
    standard_options.streaming = False
    
    # Set worker options
    worker_options = options.view_as(WorkerOptions)
    
    logging.info("Creating test pipeline")
    with beam.Pipeline(options=options) as p:
        (p 
         | 'Create' >> beam.Create([1, 2, 3])
         | 'Print' >> beam.Map(print))
        
    logging.info("Pipeline execution completed")

if __name__ == '__main__':
    run()
