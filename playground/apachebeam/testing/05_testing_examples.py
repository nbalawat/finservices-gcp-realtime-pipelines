import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, is_not_empty
import unittest

class WordCount(beam.PTransform):
    """A transform that counts words in a PCollection of text."""

    def expand(self, pcoll):
        return (
            pcoll
            | 'Split words' >> beam.FlatMap(lambda x: x.split())
            | 'Filter empty' >> beam.Filter(bool)
            | 'To lower' >> beam.Map(str.lower)
            | 'Count words' >> beam.CombinePerElement(lambda x: 1)
            | 'Sum counts' >> beam.CombinePerKey(sum)
        )

class DataCleaning(beam.DoFn):
    """A DoFn that cleans and validates data."""

    def process(self, element):
        if not isinstance(element, dict):
            return []

        # Clean and validate the data
        try:
            cleaned = {
                'name': element.get('name', '').strip(),
                'age': int(element.get('age', 0)),
                'email': element.get('email', '').lower().strip()
            }

            # Validation
            if not cleaned['name'] or cleaned['age'] <= 0:
                return []

            yield cleaned
        except (ValueError, TypeError):
            return []

class BeamTestCases(unittest.TestCase):
    """Test cases demonstrating Apache Beam testing patterns."""

    def test_word_count(self):
        """Test the WordCount transform."""
        with TestPipeline() as pipeline:
            # Create test input
            input_data = ['Hello World', 'Hello Beam', 'Apache Beam']
            
            # Apply the transform
            output = (
                pipeline
                | 'Create input' >> beam.Create(input_data)
                | 'Count words' >> WordCount()
            )

            # Assert on the results
            expected_output = [
                ('hello', 2),
                ('world', 1),
                ('beam', 2),
                ('apache', 1)
            ]

            assert_that(output, equal_to(expected_output))

    def test_data_cleaning(self):
        """Test the DataCleaning DoFn."""
        with TestPipeline() as pipeline:
            # Test input with various cases
            input_data = [
                {'name': 'Alice', 'age': '25', 'email': 'ALICE@EXAMPLE.COM'},
                {'name': '  Bob  ', 'age': 30, 'email': 'bob@example.com'},
                {'name': '', 'age': 0, 'email': 'invalid'},  # Should be filtered out
                {'name': 'Charlie', 'age': 'invalid', 'email': 'charlie@example.com'},  # Should be filtered out
                'invalid input'  # Should be filtered out
            ]

            # Apply the transform
            output = (
                pipeline
                | 'Create input' >> beam.Create(input_data)
                | 'Clean data' >> beam.ParDo(DataCleaning())
            )

            # Expected output after cleaning
            expected_output = [
                {'name': 'Alice', 'age': 25, 'email': 'alice@example.com'},
                {'name': 'Bob', 'age': 30, 'email': 'bob@example.com'}
            ]

            assert_that(output, equal_to(expected_output))

    def test_empty_input(self):
        """Test behavior with empty input."""
        with TestPipeline() as pipeline:
            # Create empty input
            input_data = []
            
            # Apply transforms
            output = (
                pipeline
                | 'Create empty input' >> beam.Create(input_data)
                | 'Count words' >> WordCount()
            )

            # Assert that the output is empty
            assert_that(output, equal_to([]))

    def test_data_presence(self):
        """Test that output is not empty when given valid input."""
        with TestPipeline() as pipeline:
            input_data = [{'name': 'Alice', 'age': '25', 'email': 'alice@example.com'}]
            
            output = (
                pipeline
                | 'Create input' >> beam.Create(input_data)
                | 'Clean data' >> beam.ParDo(DataCleaning())
            )

            # Assert that the output is not empty
            assert_that(output, is_not_empty())

if __name__ == '__main__':
    unittest.main()
