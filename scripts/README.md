# Infrastructure Setup Scripts

This directory contains scripts for setting up the required GCP infrastructure for the payment processing pipeline.

## Setup Script

The `setup_gcp_infrastructure.py` script creates:

1. **Pub/Sub**:
   - Topic for payment events
   - Subscription with message ordering enabled

2. **BigQuery**:
   - Dataset for payment processing
   - Payments table with standardized schema
   - Error table for logging issues

3. **BigTable**:
   - Instance with specified cluster
   - Five tables with different row key strategies:
     - `payments_by_customer_time_type`: Customer ID + Timestamp + Payment Type
     - `payments_by_customer_time`: Customer ID + Timestamp
     - `payments_by_time_customer`: Timestamp + Customer ID
     - `payments_by_transaction`: Transaction ID only
     - `payments_by_customer_type`: Customer ID + Payment Type

## Prerequisites

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Set up GCP authentication:
   ```bash
   gcloud auth application-default login
   ```

## Usage

Run the script with your GCP project ID:

```bash
python setup_gcp_infrastructure.py --project-id=YOUR_PROJECT_ID
```

Optional arguments:
- `--region`: GCP region (default: us-central1)
- `--topic-id`: Pub/Sub topic name (default: payment-events)
- `--subscription-id`: Pub/Sub subscription name (default: payment-subscription)
- `--dataset-id`: BigQuery dataset name (default: payment_processing)
- `--bigtable-instance`: BigTable instance name (default: payment-processing)
- `--bigtable-cluster`: BigTable cluster name (default: payment-processing-cluster)

Example with all options:
```bash
python setup_gcp_infrastructure.py \
  --project-id=my-project \
  --region=us-east1 \
  --topic-id=custom-payments \
  --subscription-id=custom-sub \
  --dataset-id=my_payments \
  --bigtable-instance=my-instance \
  --bigtable-cluster=my-cluster
```

## Notes

1. The script is idempotent - it's safe to run multiple times
2. Resources that already exist will be skipped
3. BigTable instance creation may take a few minutes
4. Make sure you have appropriate IAM permissions in your GCP project
