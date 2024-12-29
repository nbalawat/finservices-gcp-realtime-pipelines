# Payment Processing Pipeline with Apache Beam

This repository contains a real-time payment processing pipeline built using Apache Beam, Google Cloud Platform services, and Terraform for infrastructure management.

## Architecture Overview

The pipeline processes payment transactions in real-time using the following components:

- **Pub/Sub**: Ingests payment messages in real-time
- **Apache Beam/Dataflow**: Processes and transforms payment data
- **BigQuery**: Stores processed payment data and error records
- **Cloud Bigtable**: Provides low-latency access to payment state
- **Cloud Storage**: Stores Dataflow templates and temporary files

## Project Structure

```
.
├── src/
│   └── pipelines/
│       └── payment_processing/
│           ├── pipeline.py          # Main Apache Beam pipeline
│           └── transforms/          # Custom beam transforms
├── terraform/
│   ├── environments/               # Environment-specific configurations
│   │   ├── dev/
│   │   └── prod/
│   ├── modules/                    # Reusable Terraform modules
│   │   ├── databases/             # BigQuery and Bigtable resources
│   │   ├── dataflow/             # Dataflow job configurations
│   │   ├── iam/                  # IAM roles and service accounts
│   │   ├── pubsub/              # Pub/Sub topics and subscriptions
│   │   ├── services/           # GCP API services
│   │   └── storage/           # GCS bucket configurations
│   ├── main.tf                # Main Terraform configuration
│   └── variables.tf           # Input variables
└── requirements.txt           # Python dependencies
```

## Prerequisites

1. Google Cloud Platform account with billing enabled
2. Terraform >= 1.0
3. Python >= 3.8
4. Google Cloud SDK
5. Apache Beam SDK for Python

## Setup Instructions

1. **Initialize GCP Project**
   ```bash
   export PROJECT_ID=your-project-id
   gcloud config set project $PROJECT_ID
   ```

2. **Install Dependencies**
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Configure Terraform**
   ```bash
   cd terraform
   terraform init
   ```

4. **Deploy Infrastructure**
   ```bash
   # For development environment
   cd environments/dev
   terraform apply
   
   # For production environment
   cd environments/prod
   terraform apply
   ```

## Pipeline Components

### 1. Data Ingestion
- Messages are published to a Pub/Sub topic in JSON format
- Required fields: transaction_id, payment_type, amount, currency, etc.

### 2. Data Processing
- Apache Beam pipeline processes messages in real-time
- Performs validation, enrichment, and transformation
- Handles errors gracefully with dead-letter queue pattern

### 3. Data Storage
- Processed payments are written to BigQuery tables
- Error records are stored in a separate BigQuery table
- Payment state is maintained in Bigtable for quick lookups

## Infrastructure Management

### Service Account Permissions
The pipeline uses a dedicated service account with the following roles:
- Pub/Sub Publisher/Subscriber
- Dataflow Worker
- BigQuery Data Editor
- Bigtable User
- Storage Object Viewer

### Resource Naming Convention
Resources follow the pattern: `{resource-type}-{environment}-{optional-suffix}`
Example: `pipeline-dev-cluster` for Bigtable cluster in dev environment

## Monitoring and Logging

- Dataflow job metrics available in Cloud Console
- Custom metrics tracked for payment processing success/failure
- Error logs stored in BigQuery for analysis

## Development Guidelines

1. **Local Testing**
   ```bash
   # Run unit tests
   python -m pytest tests/
   
   # Run pipeline locally
   python src/pipelines/payment_processing/pipeline.py \
     --project=$PROJECT_ID \
     --runner=DirectRunner
   ```

2. **Deployment**
   ```bash
   # Deploy to Dataflow
   python src/pipelines/payment_processing/pipeline.py \
     --project=$PROJECT_ID \
     --runner=DataflowRunner \
     --region=us-central1 \
     --temp_location=gs://dataflow-temp-{project}-{env}
   ```

## Error Handling

The pipeline implements robust error handling:
1. Invalid messages are sent to error table
2. Processing errors are logged with full context
3. Failed messages can be replayed from error table

## Contributing

1. Create a new branch for features/fixes
2. Update tests as needed
3. Submit pull request with detailed description
4. Ensure CI/CD pipeline passes

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
1. Check existing GitHub issues
2. Create a new issue with detailed description
3. Include relevant logs and error messages
