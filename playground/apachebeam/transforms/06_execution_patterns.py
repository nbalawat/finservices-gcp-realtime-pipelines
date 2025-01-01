"""
Apache Beam Execution Patterns Example

This example demonstrates different execution patterns in Apache Beam:
1. Parallel Processing: Processing multiple branches simultaneously
2. Serial Processing: Ensuring sequential processing of steps
3. Side Inputs: Using additional data sources in transformations

Key Concepts:
- Parallel vs Serial execution
- Side inputs (as singletons, iterables, and dictionaries)
- Custom DoFns with setup and teardown
- Resource management
- Performance optimization patterns

Note: In a real distributed environment, the actual parallelization will depend on:
- Runner capabilities (DirectRunner vs DataflowRunner)
- Available resources
- Data size and distribution
- Pipeline configuration
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.trigger import AfterWatermark
from apache_beam.transforms import window
from typing import Dict, List, Any, Iterable, Generator
import time
import logging
import json
import random
from datetime import datetime, timezone

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EnrichOrdersWithInventory(beam.DoFn):
    """
    A DoFn that enriches orders with inventory information using side inputs.
    Demonstrates proper resource management and side input usage.
    """
    
    def setup(self):
        """Called before processing any elements."""
        logger.info("Setting up EnrichOrdersWithInventory DoFn")
        self.cache = {}
    
    def teardown(self):
        """Called after processing all elements."""
        logger.info("Tearing down EnrichOrdersWithInventory DoFn")
        self.cache.clear()
    
    def process(self, element: Dict, inventory_data: Dict) -> Generator[Dict, None, None]:
        """
        Process each order by enriching it with inventory data.
        Args:
            element: The order data
            inventory_data: Side input containing inventory information
        """
        product_id = element['product_id']
        
        # Use side input to enrich the order
        if product_id in inventory_data:
            yield {
                **element,
                'inventory_status': inventory_data[product_id]['status'],
                'warehouse': inventory_data[product_id]['warehouse'],
                'processing_time': datetime.now(timezone.utc).isoformat()
            }
        else:
            logger.warning(f"No inventory data found for product {product_id}")
            yield element

class HeavyProcessing(beam.DoFn):
    """Simulates a computationally intensive operation."""
    
    def process(self, element: Dict) -> Generator[Dict, None, None]:
        time.sleep(0.1)  # Simulate heavy processing
        element['heavy_processing_complete'] = True
        yield element

class LightProcessing(beam.DoFn):
    """Simulates a light processing operation."""
    
    def process(self, element: Dict) -> Generator[Dict, None, None]:
        element['light_processing_complete'] = True
        yield element

def run_execution_patterns():
    """Demonstrates different execution patterns in Apache Beam."""
    
    # Sample data
    orders = [
        {'order_id': f'ORD{i}', 'product_id': f'PROD{i%3}', 'quantity': random.randint(1, 10)}
        for i in range(20)
    ]
    
    inventory_data = {
        'PROD0': {'status': 'IN_STOCK', 'warehouse': 'WEST'},
        'PROD1': {'status': 'LOW_STOCK', 'warehouse': 'EAST'},
        'PROD2': {'status': 'OUT_OF_STOCK', 'warehouse': 'NORTH'}
    }
    
    with beam.Pipeline(options=PipelineOptions()) as pipeline:
        # Create the main input PCollection
        orders_pc = pipeline | "Create orders" >> beam.Create(orders)
        
        # Create side input PCollection
        inventory_pc = (pipeline 
                       | "Create inventory" >> beam.Create([inventory_data])
                       | "Convert to dict" >> beam.combiners.ToDict())
        
        # 1. Parallel Processing Example
        # Process orders through two independent branches simultaneously
        logger.info("Setting up parallel processing branches")
        
        # Branch 1: Heavy Processing
        heavy_branch = (
            orders_pc
            | "Heavy Processing" >> beam.ParDo(HeavyProcessing())
            | "Window Heavy" >> beam.WindowInto(window.GlobalWindows())
        )
        
        # Branch 2: Light Processing
        light_branch = (
            orders_pc
            | "Light Processing" >> beam.ParDo(LightProcessing())
            | "Window Light" >> beam.WindowInto(window.GlobalWindows())
        )
        
        # 2. Serial Processing Example
        # Process enriched orders sequentially
        logger.info("Setting up serial processing chain")
        
        enriched_orders = (
            orders_pc
            | "Enrich with Inventory" >> beam.ParDo(
                EnrichOrdersWithInventory(), inventory_data=beam.pvalue.AsSingleton(inventory_pc)
            )
        )
        
        # 3. Side Input Patterns
        logger.info("Demonstrating different side input patterns")
        
        # Pattern 1: Side input as singleton
        singleton_result = (
            enriched_orders
            | "Process with Singleton" >> beam.Map(
                lambda x, total: {**x, 'total_orders': total},
                total=beam.pvalue.AsSingleton(enriched_orders | beam.combiners.Count.Globally())
            )
        )
        
        # Pattern 2: Side input as iterable
        def process_with_iterable(element: Dict, all_orders: Iterable[Dict]) -> Dict:
            related_orders = [
                order for order in all_orders
                if order['product_id'] == element['product_id']
                and order['order_id'] != element['order_id']
            ]
            return {
                **element,
                'related_orders': len(related_orders)
            }
        
        iterable_result = (
            enriched_orders
            | "Process with Iterable" >> beam.Map(
                process_with_iterable,
                all_orders=beam.pvalue.AsIter(enriched_orders)
            )
        )
        
        # Pattern 3: Side input as dictionary
        def process_with_dict(element: Dict, product_stats: Dict) -> Dict:
            return {
                **element,
                'product_stats': product_stats.get(element['product_id'], {})
            }
        
        # Create product statistics
        product_stats = (
            enriched_orders
            | "Group by product" >> beam.GroupBy(lambda x: x['product_id'])
            | "Calculate stats" >> beam.Map(
                lambda x: (x[0], {'total_orders': len(x[1])})
            )
            | "To dict" >> beam.combiners.ToDict()
        )
        
        dict_result = (
            enriched_orders
            | "Process with Dict" >> beam.Map(
                process_with_dict,
                product_stats=beam.pvalue.AsSingleton(product_stats)
            )
        )
        
        # Output results
        def format_output(element: Dict) -> str:
            return json.dumps(element, indent=2)
        
        singleton_result | "Print Singleton" >> beam.Map(
            lambda x: logger.info(f"Singleton Result:\n{format_output(x)}")
        )
        
        iterable_result | "Print Iterable" >> beam.Map(
            lambda x: logger.info(f"Iterable Result:\n{format_output(x)}")
        )
        
        dict_result | "Print Dict" >> beam.Map(
            lambda x: logger.info(f"Dict Result:\n{format_output(x)}")
        )

if __name__ == '__main__':
    run_execution_patterns()
