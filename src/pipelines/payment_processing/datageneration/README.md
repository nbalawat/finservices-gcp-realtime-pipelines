# Payment Data Generator Pipeline

A scalable Apache Beam pipeline for generating synthetic payment data and writing it to Google Cloud BigTable. This pipeline is designed to generate large-scale payment data with configurable parameters and multiple table layouts for different query patterns.

## Features

- Generates synthetic payment data with realistic attributes
- Scales to billions of records using Apache Beam's distributed processing
- Writes to multiple BigTable tables with different row key formats for optimal querying
- Supports both local execution and Google Cloud Dataflow
- Configurable parameters for data generation
- Built-in monitoring and logging

## Prerequisites

- Python 3.8+
- Google Cloud Project with enabled services:
  - Cloud Dataflow API
  - Cloud BigTable API
  - Cloud Storage API
- Google Cloud Service Account with required permissions:
  - `roles/dataflow.worker`
  - `roles/bigtable.admin`
  - `roles/storage.admin`

## Installation

1. Set up a Python virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up Google Cloud credentials:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/service-account-key.json"
```

## BigTable Schema

The pipeline creates multiple tables with different row key formats optimized for various query patterns:

1. `payments_by_customer`: 
   - Row Key: `customerId#transaction_date#transaction_type`
   - Optimal for: Querying all transactions for a specific customer

2. `payments_by_date`:
   - Row Key: `transaction_date#transaction_type#customerId`
   - Optimal for: Time-based queries and analytics

3. `payments_by_transaction`:
   - Row Key: `transaction_id#customerId#transaction_date`
   - Optimal for: Transaction lookup and customer history

Column Families:
- `payment_header`: Basic payment information
- `amount_info`: Payment amount details
- `party_info`: Customer and counterparty information (180 days retention)
- `processing_info`: Processing status and metadata (90 days retention)
- `type_specific`: Payment type specific fields
- `metadata`: Additional metadata (30 days retention)

## Usage

### Local Execution

Run the pipeline locally using DirectRunner:
```bash
./run_local.sh
```

Configuration options in `run_local.sh`:
```bash
--num_records=5000000     # Number of payment records to generate
--num_customers=1000      # Number of unique customers
--start_date=2024-03-01   # Start date for payment dates
--end_date=2024-03-31     # End date for payment dates
```

### Cloud Execution

Run the pipeline on Google Cloud Dataflow:
```bash
./run_gcp.sh
```

Additional configuration options in `run_gcp.sh`:
```bash
--max_num_workers=50          # Maximum number of Dataflow workers
--disk_size_gb=50            # Worker disk size
--worker_machine_type=n2-standard-8  # Worker machine type
```

## Pipeline Components

1. **PaymentGeneratorFn** (`payment_generator.py`):
   - Generates synthetic payment data
   - Creates realistic payment attributes
   - Supports multiple payment types

2. **FormatForBigTableFn** (`payment_generator.py`):
   - Formats data for BigTable ingestion
   - Creates row keys based on table configurations
   - Handles data transformation

3. **WriteToBigtable** (`bigtable_writer.py`):
   - Manages BigTable writes
   - Implements batching for performance
   - Includes error handling and metrics

4. **BigTableSetup** (`bigtable_setup.py`):
   - Creates and configures BigTable tables
   - Sets up column families
   - Manages table schemas

## Performance Optimization

The pipeline includes several optimizations for handling large-scale data:

1. **Windowing**:
   - Uses 60-second fixed windows
   - Helps manage memory usage
   - Improves parallelization

2. **Batching**:
   - Implements BigTable batch mutations
   - Configurable batch sizes
   - Optimized for throughput

3. **Monitoring**:
   - Custom metrics for tracking progress
   - Detailed logging
   - Error handling and reporting

## Monitoring and Logging

The pipeline provides detailed logging and metrics:

- Record generation progress
- BigTable write statistics
- Error rates and types
- Processing throughput

View logs in Cloud Logging:
```bash
gcloud logging read "resource.type=dataflow_step"
```

## Troubleshooting

Common issues and solutions:

1. **Memory Issues**:
   - Adjust window size
   - Reduce batch size
   - Increase worker memory

2. **BigTable Errors**:
   - Check permissions
   - Verify instance/table existence
   - Review row key format

3. **Performance Issues**:
   - Increase number of workers
   - Adjust machine type
   - Review data distribution

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Key Learnings and Best Practices

### 1. Apache Beam Version Compatibility
- Use `apache-beam[gcp]>=2.61.0` for better compatibility with Google Cloud services
- Newer versions provide improved performance and stability
- Include all necessary dependencies in `requirements.txt`

### 2. Pipeline Optimization Techniques
- **Memory Management**:
  - Use `beam.WindowInto(FixedWindows(60))` for better memory distribution
  - Implement `Reshuffle()` to redistribute data across workers
  - Batch writes to BigTable for optimal throughput

- **Scaling Strategies**:
  - Start with small data volumes for testing
  - Gradually increase to production scale
  - Monitor worker utilization and adjust resources accordingly

### 3. BigTable Best Practices
- **Row Key Design**:
  - Use '#' as separator in row keys (e.g., `customerId#transaction_date#transaction_type`)
  - Design keys for optimal read patterns
  - Maintain consistent key formats across tables

- **Multiple Table Layouts**:
  ```python
  TABLE_CONFIGS = {
      'payments_by_customer': 'customerId#transaction_date#transaction_type',
      'payments_by_date': 'transaction_date#transaction_type#customerId',
      'payments_by_transaction': 'transaction_id#customerId#transaction_date'
  }
  ```

### 4. Performance Tuning
- **Dataflow Settings**:
  ```bash
  --max_num_workers=50
  --worker_machine_type=n2-standard-8
  --disk_size_gb=50
  ```

- **Pipeline Parameters**:
  ```bash
  --num_records=50000000    # Adjust based on needs
  --num_customers=10000     # Balance with data distribution
  ```

### 5. Monitoring and Debugging
- **Job Monitoring**:
  - Use `monitor_pipeline.sh` for real-time status
  - Check Cloud Console for detailed metrics
  - Monitor BigTable write throughput

- **Logging Strategy**:
  ```python
  logger.info(f"Created BigTable row key: {row_key}")
  logger.error(f"Error setting up pipeline: {str(e)}", exc_info=True)
  ```

### 6. Development Workflow
1. **Local Testing**:
   ```bash
   ./run_local.sh
   # Use small data volumes
   --num_records=1000
   --num_customers=100
   ```

2. **Production Deployment**:
   ```bash
   ./run_gcp.sh
   # Scale up gradually
   --num_records=50000000
   --num_customers=10000
   ```

### 7. Common Issues and Solutions

1. **Memory Issues**:
   - Implement windowing
   - Use Reshuffle transform
   - Adjust worker machine type

2. **BigTable Write Performance**:
   - Batch mutations
   - Monitor throttling
   - Balance row key distribution

3. **Pipeline Stuck**:
   - Check worker logs
   - Monitor step progress
   - Verify resource allocation

### 8. Code Organization
- Separate concerns into specific files:
  - `datagenerator_pipeline.py`: Main pipeline logic
  - `payment_generator.py`: Data generation
  - `bigtable_writer.py`: BigTable interaction
  - `bigtable_setup.py`: Table configuration

### 9. Testing Strategy
1. **Unit Tests**:
   - Test data generation
   - Verify row key formatting
   - Check BigTable mutations

2. **Integration Tests**:
   - Run with small data volumes
   - Verify end-to-end flow
   - Check data consistency

### 10. Dependencies and Environment
```txt
apache-beam[gcp]==2.61.0
google-cloud-bigtable==2.26.0
google-cloud-bigquery==3.13.0
python-dotenv==1.0.0
```

### 11. Security Considerations
- Use service accounts with minimal required permissions
- Store credentials securely
- Implement proper error handling
- Monitor access patterns

### 12. Future Improvements
1. **Performance**:
   - Implement custom partitioning
   - Optimize batch sizes
   - Add caching where appropriate

2. **Monitoring**:
   - Add custom metrics
   - Implement alerting
   - Enhanced logging

3. **Features**:
   - Add data validation
   - Implement cleanup jobs
   - Add data quality checks
