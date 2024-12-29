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

Below is a **Markdown** document containing various ways to “query” Cloud Bigtable using both the **`cbt` CLI** and the **Python client library**. You can copy and paste the content into a file (e.g., `bigtable_queries.md`) and download it.

```markdown
# Cloud Bigtable Query Examples

Cloud Bigtable does not provide SQL support but uses row keys, filters, and column families for querying. Below are examples using:

1. **cbt** CLI tool  
2. **Python client library**

---

## 1. cbt CLI Tool

Make sure you have:
- Installed `gcloud` and the `cbt` component.
- Configured your `~/.cbtrc` with the correct `project`, `instance`, and optionally `creds`.

**Basic structure:**
```
cbt [options] <command> <table_name> [parameters]
```

### 1.1 Read All Rows
```bash
cbt read <TABLE_NAME>
```
Reads every row in the table.

### 1.2 Limit the Number of Rows
```bash
cbt read <TABLE_NAME> count=10
```
Returns only the first 10 rows.

### 1.3 Read a Specific Row
```bash
cbt read <TABLE_NAME> start=<ROW_KEY> end=<ROW_KEY>
```
By setting `start` and `end` to the same value, you effectively read just one row.

### 1.4 Row Key Prefix
```bash
cbt read <TABLE_NAME> prefix=<PREFIX_STRING>
```
Returns rows whose key begins with `<PREFIX_STRING>`.

### 1.5 Column Families / Columns
- **Read all columns in a specific family**:
  ```bash
  cbt read <TABLE_NAME> columns=<FAMILY_NAME>
  ```
- **Read a specific column qualifier**:
  ```bash
  cbt read <TABLE_NAME> columns=<FAMILY_NAME>:<QUALIFIER>
  ```
- **Read multiple columns** (comma-separated):
  ```bash
  cbt read <TABLE_NAME> columns=family1:colA,family2:colB
  ```

### 1.6 Row Key Regex
```bash
cbt read <TABLE_NAME> regex="<REGEX_PATTERN>"
```
- Example: Row key ends with "2024":
  ```bash
  cbt read <TABLE_NAME> regex="2024$"
  ```

### 1.7 Start Key & End Key (Range)
```bash
cbt read <TABLE_NAME> start=<START_KEY> end=<END_KEY>
```
Returns rows from (including) `<START_KEY>` up to (but not including) `<END_KEY>` in lexicographical order.

### 1.8 Timestamp Range
The `cbt` tool doesn’t offer a direct parameter for timestamp range filtering. For advanced filtering by timestamps, use a client library (see below).

---

## 2. Python Client Library

Install the client:
```bash
pip install google-cloud-bigtable
```

### 2.1 Connect to Bigtable
```python
from google.cloud import bigtable

client = bigtable.Client(project='YOUR_PROJECT_ID', admin=True)
instance = client.instance('YOUR_INSTANCE_ID')
table = instance.table('YOUR_TABLE_ID')
```

### 2.2 Read All Rows
```python
all_rows = table.read_rows()
for row in all_rows:
    print("Row key:", row.row_key.decode('utf-8'))
    for family, columns in row.cells.items():
        for col_qualifier, cells in columns.items():
            for cell in cells:
                print(f"Family={family}, Qualifier={col_qualifier.decode('utf-8')}, Value={cell.value.decode('utf-8')}")
```

### 2.3 Row Prefix Filter
```python
from google.cloud.bigtable import row_filters

prefix_filter = row_filters.RowKeyRegexFilter(rb'^customer123')
rows = table.read_rows(filter_=prefix_filter)
for row in rows:
    print("Row key:", row.row_key.decode('utf-8'))
```
Only returns rows whose keys start with `"customer123"`.

### 2.4 Row Range
```python
rows = table.read_rows(
    start_key=b'customer123#2024-01-01',
    end_key=b'customer123#2025-01-01'
)
for row in rows:
    print("Row key:", row.row_key.decode('utf-8'))
```
Returns rows from `customer123#2024-01-01` up to (but not including) `customer123#2025-01-01`.

### 2.5 Column Qualifier Filter
```python
col_filter = row_filters.ColumnQualifierRegexFilter(rb'^payment_type$')
rows = table.read_rows(filter_=col_filter)
for row in rows:
    print("Row key:", row.row_key.decode('utf-8'))
    # Only "payment_type" column is retrieved
```

### 2.6 Column Family Filter
```python
family_filter = row_filters.FamilyNameRegexFilter('metadata')
rows = table.read_rows(filter_=family_filter)
for row in rows:
    print("Row key:", row.row_key.decode('utf-8'))
    # Only columns in the 'metadata' family are retrieved
```

### 2.7 Value Range Filter
If your values are stored as strings (e.g., amounts as numeric strings):
```python
value_filter = row_filters.ValueRangeFilter(start_value=b'100', end_value=b'999')
rows = table.read_rows(filter_=value_filter)
for row in rows:
    print("Row key:", row.row_key.decode('utf-8'))
    # Cells whose values are between "100" and "999"
```

### 2.8 Timestamp Range Filter
```python
import datetime

start_ts = datetime.datetime(2024, 1, 1)
end_ts   = datetime.datetime(2025, 1, 1)
timestamp_filter = row_filters.TimestampRangeFilter(start=start_ts, end=end_ts)

rows = table.read_rows(filter_=timestamp_filter)
for row in rows:
    print("Row key:", row.row_key.decode('utf-8'))
    # Only cells written in [2024-01-01, 2025-01-01)
```

### 2.9 Chaining Filters
Combine filters with **Chain** (AND), **Interleave** (OR), or **Condition** (IF-THEN-ELSE).

```python
filter_chain = row_filters.ChainFilter(
    filters=[
        row_filters.FamilyNameRegexFilter('transaction_info'),
        row_filters.ColumnQualifierRegexFilter(rb'^amount$'),
        row_filters.ValueRangeFilter(start_value=b'500', end_value=b'9999')
    ]
)

rows = table.read_rows(filter_=filter_chain)
for row in rows:
    print("Row key:", row.row_key.decode('utf-8'))
    # Only rows with family=transaction_info, column=amount, value in [500..9999]
```

---

## 3. Counting Rows

### 3.1 `cbt count`
```bash
cbt count <TABLE_NAME>
```
Returns a count of rows, but performs a full scan (can be slow for large tables).

### 3.2 Programmatic Full Scan
```python
row_count = 0
rows = table.read_rows()
for row in rows:
    row_count += 1
print(f"Total Rows: {row_count}")
```
Also a full scan.

### 3.3 Maintaining a Running Count
For large or frequently updated tables, consider:
- Using Pub/Sub or Dataflow to increment a counter as rows are added.
- Storing the count in a separate system for quick retrieval.

---

## 4. Best Practices

1. **Efficient Row Key Design**  
   Align row keys with common query patterns. For example, `customerID#YYYYMMDD#...` can help with time-based scans.

2. **Column Families**  
   Group related columns into families (e.g., `transaction_info`, `account_info`, `metadata`).

3. **Filtering**  
   Use **filters** to limit data and reduce latency or cost. For large amounts of data, avoid scanning everything unnecessarily.

4. **Timestamp Management**  
   Bigtable stores multiple versions (cells) of the same column by timestamp. If you only need the latest version, use `CellsRowLimitFilter(1)` or set a version limit in the schema.

5. **No SQL**  
   Bigtable is not SQL-based. For complex aggregations (like `AVG` or `JOIN`), consider external processing (e.g., Dataflow/Beam).