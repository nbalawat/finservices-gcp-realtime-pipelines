# Apache Beam Execution Patterns

This guide explains different execution patterns in Apache Beam using practical examples.

## Key Concepts

### 1. Parallel Processing
Parallel processing in Apache Beam allows multiple transformations to be executed simultaneously. This is achieved through:

- **Multiple Branches**: Creating independent processing paths that can execute concurrently
- **Fusion**: Beam's optimization that combines compatible sequential steps
- **Parallelization**: Automatic partitioning of data across workers

Example from `06_execution_patterns.py`:
```python
# Branch 1: Heavy Processing
heavy_branch = orders_pc | "Heavy Processing" >> beam.ParDo(HeavyProcessing())

# Branch 2: Light Processing (runs in parallel)
light_branch = orders_pc | "Light Processing" >> beam.ParDo(LightProcessing())
```

### 2. Serial Processing
Serial processing ensures steps are executed in a specific order. This is important when:

- Steps depend on previous results
- Resource management requires controlled execution
- Processing order matters for business logic

Example:
```python
result = (
    input_data
    | "Step 1" >> beam.ParDo(FirstStep())  # Must complete before Step 2
    | "Step 2" >> beam.ParDo(SecondStep())  # Depends on Step 1
    | "Step 3" >> beam.ParDo(ThirdStep())   # Depends on Step 2
)
```

### 3. Side Inputs
Side inputs provide additional data to your transformations. There are three patterns:

#### a. Singleton
- Used when you need a single value available to all instances
- Typically for global configuration or aggregated values
```python
result = main_data | beam.Map(
    lambda x, total: process(x, total),
    total=beam.pvalue.AsSingleton(count_pc)
)
```

#### b. Iterable
- Provides access to all elements of a PCollection
- Useful for lookups and cross-referencing
```python
result = main_data | beam.Map(
    lambda x, all_data: process(x, all_data),
    all_data=beam.pvalue.AsIter(reference_pc)
)
```

#### c. Dictionary
- Provides key-based lookup capability
- Efficient for joining and enriching data
```python
result = main_data | beam.Map(
    lambda x, lookup: process(x, lookup),
    lookup=beam.pvalue.AsDict(reference_pc)
)
```

## Best Practices

1. **Resource Management**
   - Use `setup()` and `teardown()` in DoFns
   - Clean up resources properly
   - Cache frequently used data

2. **Performance Optimization**
   - Choose appropriate side input patterns
   - Balance parallel vs serial execution
   - Consider data size when using AsIter

3. **Error Handling**
   - Implement proper logging
   - Handle missing or invalid side inputs
   - Consider retry logic for flaky operations

## Common Pitfalls

1. **Memory Usage**
   - AsIter loads entire PCollection into memory
   - Large side inputs can cause OOM errors
   - Consider windowing for large datasets

2. **Execution Order**
   - Don't assume parallel branches execute in any order
   - Use timestamps for ordering when needed
   - Consider using windowing for time-based ordering

3. **Side Input Updates**
   - Side inputs are fixed once pipeline starts
   - Use windowing for dynamic updates
   - Consider refresh strategies for long-running pipelines

## Example Usage

Run the example:
```bash
python 06_execution_patterns.py
```

This will demonstrate:
- Parallel processing of orders
- Serial enrichment with inventory data
- Three side input patterns
- Proper resource management
- Logging and error handling
