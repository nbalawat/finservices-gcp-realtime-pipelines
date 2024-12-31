# BigTable Query Performance Testing Framework

A modular Python framework for testing and analyzing BigTable query performance at scale using Apache Beam.

## Features

- Comprehensive query testing for both customer-centric and generic queries
- Parallel query execution using Apache Beam
- Detailed performance metrics collection and analysis
- Interactive visualizations and dashboards
- Configurable test parameters and execution settings
- Modular and extensible architecture

## Installation

1. Clone the repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

Create a configuration file (JSON format) with the following structure:

```json
{
  "table_config": {
    "project_id": "your-project-id",
    "instance_id": "your-instance-id",
    "tables": {
      "payments_by_customer": "customerId#transaction_date#transaction_type",
      "payments_by_date": "transaction_date#transaction_type#customerId",
      "payments_by_transaction": "transaction_id#customerId#transaction_date",
      "payments_by_id": "transaction_id",
      "payments_by_id_date": "transaction_id#transaction_date"
    },
    "column_families": ["cf1"]
  },
  "query_config": {
    "num_queries": 2500,
    "concurrent_queries": 500,
    "test_month": "2024-03",
    "sample_customers": 1000
  },
  "performance_config": {
    "timeout_seconds": 30,
    "retry_attempts": 3,
    "metrics_window_size": 100
  }
}
```

## Usage

Run the performance test:

```bash
python main.py --config config.json --output-dir ./results --beam-runner DirectRunner
```

For cloud execution using Dataflow:

```bash
python main.py --config config.json --output-dir ./results --beam-runner DataflowRunner
```

## Query Types

### Customer-Centric Queries
1. Daily Transactions by Type
2. Date Range Transactions
3. Type-Specific Transactions
4. All Customer Transactions

### Generic Queries
1. Batch Transaction Lookup
2. Daily Customer Activity
3. Date Range Activity
4. Payment Type Analysis

## Output

The framework generates:
- CSV files with detailed performance metrics
- Interactive HTML dashboard with visualizations
- Summary report with key statistics
- Raw query execution data for custom analysis

## Testing

Run the test suite:

```bash
pytest tests/
```

## Architecture

The framework follows a modular architecture:

- `config/`: Configuration management
- `core/`: Core functionality and client wrappers
- `queries/`: Query implementations
- `analysis/`: Results processing and visualization
- `tests/`: Test suite

## Performance Metrics

Collected metrics include:
- Query execution time (avg, median, p95, p99)
- Success rates
- Row counts
- Throughput
- Error rates

## Extending

To add new query types:
1. Create a new query class inheriting from `BaseQuery`
2. Implement `build_row_key` and `execute` methods
3. Add corresponding test cases
4. Update query generator to include new query type

## Best Practices

- Use appropriate table schema based on query patterns
- Monitor resource utilization during tests
- Adjust concurrent queries based on cluster capacity
- Regular test execution for performance trending
- Keep test data representative of production workload

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## License

MIT License