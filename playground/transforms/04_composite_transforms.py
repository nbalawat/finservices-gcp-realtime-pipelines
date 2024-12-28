import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from typing import Dict, Any, List, Tuple
import json

class EnrichUserData(beam.PTransform):
    """A composite transform that enriches user data with additional information"""

    def __init__(self, user_interests: List[Dict[str, Any]]):
        super().__init__()
        self.user_interests = user_interests

    def expand(self, users):
        interests_pc = self.pipeline | "Create interests" >> beam.Create(self.user_interests)

        # Key both PCollections
        keyed_users = users | "Key users" >> beam.Map(lambda x: (x['id'], x))
        keyed_interests = interests_pc | "Key interests" >> beam.Map(lambda x: (x['user_id'], x))

        # Join and format
        return ({
            'users': keyed_users,
            'interests': keyed_interests
        }
        | "Group by key" >> beam.CoGroupByKey()
        | "Format enriched data" >> beam.Map(self.format_enriched_data)
        )

    @staticmethod
    def format_enriched_data(element: Tuple[int, Dict[str, List]]) -> Dict[str, Any]:
        user_id, grouped = element
        user = grouped['users'][0] if grouped['users'] else None
        interests = [i['interest'] for i in grouped['interests']]

        if not user:
            return None

        return {
            'id': user_id,
            'name': user['name'],
            'email': user['email'],
            'interests': interests
        }

class AnalyzeUserActivity(beam.PTransform):
    """A composite transform that analyzes user activity patterns"""

    def expand(self, user_activities):
        # Calculate activity counts per user
        activity_counts = (
            user_activities
            | "Extract user and activity" >> beam.Map(
                lambda x: (x['user_id'], x['activity_type'])
            )
            | "Count per user" >> beam.CombinePerKey(beam.combiners.CountCombineFn())
        )

        # Calculate most common activity
        most_common_activity = (
            user_activities
            | "Get activity types" >> beam.Map(lambda x: x['activity_type'])
            | "Count activities" >> beam.CombineGlobally(
                beam.combiners.CountCombineFn()
            ).without_defaults()
        )

        return {
            'activity_counts': activity_counts,
            'most_common': most_common_activity
        }

def run_composite_transform_example():
    """Demonstrates the use of composite transforms"""

    # Sample data
    users = [
        {'id': 1, 'name': 'Alice', 'email': 'alice@example.com'},
        {'id': 2, 'name': 'Bob', 'email': 'bob@example.com'},
        {'id': 3, 'name': 'Charlie', 'email': 'charlie@example.com'},
    ]

    user_interests = [
        {'user_id': 1, 'interest': 'Python'},
        {'user_id': 1, 'interest': 'Data Science'},
        {'user_id': 2, 'interest': 'Java'},
        {'user_id': 3, 'interest': 'Python'},
        {'user_id': 3, 'interest': 'Machine Learning'},
    ]

    user_activities = [
        {'user_id': 1, 'activity_type': 'login', 'timestamp': '2024-01-01T10:00:00'},
        {'user_id': 1, 'activity_type': 'search', 'timestamp': '2024-01-01T10:05:00'},
        {'user_id': 2, 'activity_type': 'login', 'timestamp': '2024-01-01T11:00:00'},
        {'user_id': 3, 'activity_type': 'search', 'timestamp': '2024-01-01T12:00:00'},
    ]

    with beam.Pipeline(options=PipelineOptions()) as pipeline:
        # Create initial PCollection
        users_pc = pipeline | "Create users" >> beam.Create(users)
        activities_pc = pipeline | "Create activities" >> beam.Create(user_activities)

        # Apply composite transforms
        enriched_users = users_pc | "Enrich users" >> EnrichUserData(user_interests)
        activity_analysis = activities_pc | "Analyze activities" >> AnalyzeUserActivity()

        # Format and print results
        enriched_users | "Format users" >> beam.Map(
            lambda x: f"Enriched user: {json.dumps(x, indent=2)}"
        ) | "Print users" >> beam.Map(print)

        activity_analysis['activity_counts'] | "Format counts" >> beam.Map(
            lambda x: f"User {x[0]} activity count: {x[1]}"
        ) | "Print counts" >> beam.Map(print)

        activity_analysis['most_common'] | "Format most common" >> beam.Map(
            lambda x: f"Total activities: {x}"
        ) | "Print most common" >> beam.Map(print)

if __name__ == '__main__':
    run_composite_transform_example()
