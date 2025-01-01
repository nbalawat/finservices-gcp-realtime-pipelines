import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window
from apache_beam.transforms.trigger import AfterWatermark, AfterCount
import datetime
import random

def create_sample_events(num_events=100):
    """Create sample timestamped events"""
    base_time = datetime.datetime.now()
    events = []
    
    for i in range(num_events):
        # Create events spread over the last hour
        timestamp = base_time + datetime.timedelta(
            minutes=random.randint(-60, 0),
            seconds=random.randint(0, 59)
        )
        
        events.append({
            'id': i,
            'value': random.randint(1, 100),
            'timestamp': timestamp.isoformat()
        })
    
    return events

def add_timestamps(element):
    """Add timestamps to elements for windowing"""
    return beam.window.TimestampedValue(
        element,
        datetime.datetime.fromisoformat(element['timestamp']).timestamp()
    )

def run_windowing_example():
    """Demonstrates different windowing strategies in Apache Beam"""
    
    with beam.Pipeline(options=PipelineOptions()) as pipeline:
        # Create sample events
        events = pipeline | "Create events" >> beam.Create(create_sample_events())
        
        # Add timestamps to events
        timestamped_events = events | "Add timestamps" >> beam.Map(add_timestamps)
        
        # Fixed windows example
        fixed_windows = (
            timestamped_events
            | "Fixed windows" >> beam.WindowInto(
                window.FixedWindows(60),  # 1-minute windows
                trigger=AfterWatermark(early=AfterCount(10)),  # Early firing after 10 elements
                accumulation_mode=beam.transforms.trigger.AccumulationMode.DISCARDING
            )
            | "Count per window" >> beam.CombineGlobally(beam.combiners.CountCombineFn()).without_defaults()
            | "Format fixed" >> beam.Map(
                lambda x: f"Fixed window count: {x}"
            )
        )
        
        # Sliding windows example
        sliding_windows = (
            timestamped_events
            | "Sliding windows" >> beam.WindowInto(
                window.SlidingWindows(60, 30)  # 1-minute windows, sliding every 30 seconds
            )
            | "Sum per window" >> beam.CombineGlobally(
                beam.combiners.MeanCombineFn()
            ).without_defaults()
            | "Format sliding" >> beam.Map(
                lambda x: f"Sliding window mean: {x:.2f}"
            )
        )
        
        # Session windows example
        session_windows = (
            timestamped_events
            | "Session windows" >> beam.WindowInto(
                window.Sessions(30),  # 30-second gap between sessions
                trigger=AfterWatermark()
            )
            | "Count sessions" >> beam.CombineGlobally(
                beam.combiners.CountCombineFn()
            ).without_defaults()
            | "Format sessions" >> beam.Map(
                lambda x: f"Session count: {x}"
            )
        )
        
        # Print results
        fixed_windows | "Print fixed" >> beam.Map(print)
        sliding_windows | "Print sliding" >> beam.Map(print)
        session_windows | "Print sessions" >> beam.Map(print)

if __name__ == '__main__':
    run_windowing_example()
