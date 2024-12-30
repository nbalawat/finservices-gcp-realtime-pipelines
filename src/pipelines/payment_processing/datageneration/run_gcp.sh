#!/bin/bash

export GOOGLE_APPLICATION_CREDENTIALS=/Users/nbalawat/development/apache-beam-examples/src/beam-bigquery-test.json

# Enable Dataflow debugging
export DATAFLOW_PYTHON_COMMAND_PATH="python"
export BEAM_VERBOSE_LOG=1

python datagenerator_pipeline.py \
    --project=agentic-experiments-446019 \
    --project_id=agentic-experiments-446019 \
    --bigtable_instance=payment-processing-dev \
    --num_records=200000000 \
    --num_customers=10000 \
    --start_date=2024-03-01 \
    --end_date=2024-03-31 \
    --runner=DataflowRunner \
    --region=us-east4 \
    --temp_location=gs://agentic-experiments-446019-dataflow-temp/temp \
    --staging_location=gs://agentic-experiments-446019-dataflow-temp/staging \
    --max_num_workers=50 \
    --disk_size_gb=50 \
    --worker_machine_type=n2-standard-8 \
    --experiments=use_runner_v2 \
    --setup_file=./setup.py \
    --service_account_email=beam-local-pipeline@agentic-experiments-446019.iam.gserviceaccount.com \
    --job_name=payment-data-generator-$(date +%Y%m%d-%H%M%S) \
    --save_main_session
