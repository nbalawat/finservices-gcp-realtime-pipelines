import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def run_basic_operations():
    """Demonstrates basic Apache Beam operations"""
    with beam.Pipeline(options=PipelineOptions()) as pipeline:
        # Create a simple PCollection
        numbers = pipeline | "Create numbers" >> beam.Create([1, 2, 3, 4, 5])

        # Map (ParDo with a simple function)
        squared = numbers | "Square numbers" >> beam.Map(lambda x: x * x)

        # Filter
        even_squares = squared | "Filter even" >> beam.Filter(lambda x: x % 2 == 0)

        # FlatMap (one-to-many mapping)
        repeated = even_squares | "Repeat numbers" >> beam.FlatMap(
            lambda x: [x] * (x // 4)  # Repeat each number x/4 times
        )

        # ParDo (more complex processing)
        class AddIndexDoFn(beam.DoFn):
            def process(self, element, index=beam.DoFn.IndexParam):
                yield f"Index {index}: {element}"

        indexed = repeated | "Add indices" >> beam.ParDo(AddIndexDoFn())

        # Print results
        indexed | "Print" >> beam.Map(print)

if __name__ == '__main__':
    run_basic_operations()
