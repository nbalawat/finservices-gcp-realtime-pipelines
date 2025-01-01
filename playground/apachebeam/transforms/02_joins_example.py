import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from typing import Tuple, List, Dict
import json

def run_joins_example():
    """Demonstrates different types of joins in Apache Beam"""
    
    # Sample data
    users = [
        {"id": 1, "name": "Alice", "department_id": 100},
        {"id": 2, "name": "Bob", "department_id": 101},
        {"id": 3, "name": "Charlie", "department_id": 100},
        {"id": 4, "name": "David", "department_id": 102},
    ]

    departments = [
        {"id": 100, "name": "Engineering"},
        {"id": 101, "name": "Sales"},
        {"id": 103, "name": "Marketing"},  # Note: No users in Marketing
    ]

    with beam.Pipeline(options=PipelineOptions()) as pipeline:
        # Create PCollections
        users_pc = pipeline | "Create users" >> beam.Create(users)
        departments_pc = pipeline | "Create departments" >> beam.Create(departments)

        # Prepare data for joins
        keyed_users = users_pc | "Key users" >> beam.Map(
            lambda user: (user["department_id"], user)
        )
        keyed_departments = departments_pc | "Key departments" >> beam.Map(
            lambda dept: (dept["id"], dept)
        )

        # CoGroupByKey example (similar to full outer join)
        grouped = {"users": keyed_users, "departments": keyed_departments} | beam.CoGroupByKey()

        def format_group(element: Tuple[int, Dict[str, List]]) -> str:
            dept_id, groups = element
            users_list = groups["users"]
            depts_list = groups["departments"]
            
            return json.dumps({
                "department_id": dept_id,
                "department": depts_list[0]["name"] if depts_list else "Unknown",
                "users": [u["name"] for u in users_list]
            }, indent=2)

        # Process and print results
        formatted = grouped | "Format groups" >> beam.Map(format_group)
        formatted | "Print groups" >> beam.Map(print)

        # Inner join example
        def join_user_dept(element: Tuple[int, Dict[str, List]]) -> List[Dict]:
            dept_id, groups = element
            users_list = groups["users"]
            depts_list = groups["departments"]
            
            if not depts_list or not users_list:
                return []
            
            return [{
                "user_name": user["name"],
                "department_name": depts_list[0]["name"]
            } for user in users_list]

        joined = (
            grouped
            | "Inner join" >> beam.FlatMap(join_user_dept)
            | "Format joined" >> beam.Map(json.dumps, indent=2)
        )
        
        joined | "Print joined" >> beam.Map(print)

if __name__ == '__main__':
    run_joins_example()
