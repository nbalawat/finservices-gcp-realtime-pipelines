# Have the google credentials exported so that the service account can be used
export GOOGLE_APPLICATION_CREDENTIALS=/Users/nbalawat/development/apache-beam-examples/src/beam-bigquery-test.json

# run the pipeline locally to generate some sample records
python datagenerator_pipeline.py \
    --runner=DirectRunner \
    --project_id=agentic-experiments-446019 \
    --bigtable_instance=payment-processing-dev \
    --num_records=10