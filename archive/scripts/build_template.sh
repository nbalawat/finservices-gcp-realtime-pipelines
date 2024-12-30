#!/bin/bash

# Exit on error
set -e

# Get parameters
PROJECT_ID=$1
BUCKET_NAME=$2
REGION=${3:-us-central1}

if [ -z "$PROJECT_ID" ] || [ -z "$BUCKET_NAME" ]; then
    echo "Usage: $0 PROJECT_ID BUCKET_NAME [REGION]"
    exit 1
fi

# Set up Python environment
# python -m pip install --upgrade pip
# pip install -r requirements.txt

# Build and upload the template
python -m src.pipelines.payment_processing.payment_pipeline \
    --project=$PROJECT_ID \
    --region=$REGION \
    --temp_location=gs://$BUCKET_NAME/temp \
    --staging_location=gs://$BUCKET_NAME/staging \
    --template_location=gs://$BUCKET_NAME/templates/payment_pipeline \
    --setup_file=./setup.py \
    --runner=DataflowRunner \
    --input_subscription=projects/$PROJECT_ID/subscriptions/payments-sub-dev \
    --output_table=$PROJECT_ID:pipeline_data_dev.payments \
    --error_table=$PROJECT_ID:pipeline_data_dev.payment_errors \
    --bigtable_instance=bt-instance-dev

echo "Template built and uploaded to: gs://$BUCKET_NAME/templates/payment_pipeline"
