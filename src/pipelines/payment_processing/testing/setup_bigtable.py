from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable.table import Table
from google.api_core import exceptions
import logging
from datetime import timedelta
from typing import Dict, Optional, Set

class BigtableSetup:
    def __init__(self, project_id: str, instance_id: str):
        self.project_id = project_id
        self.instance_id = instance_id
        self.client = bigtable.Client(project=project_id, admin=True)
        self.instance = self.client.instance(instance_id)

    def validate_instance(self) -> bool:
        """Validate if BigTable instance exists"""
        try:
            self.instance.reload()
            logging.info(f"BigTable instance {self.instance_id} exists")
            return True
        except exceptions.NotFound:
            logging.error(f"BigTable instance {self.instance_id} not found")
            return False
        except Exception as e:
            logging.error(f"Error validating BigTable instance: {str(e)}")
            return False

    def get_column_families(self) -> Dict[str, Optional[int]]:
        """Define column families and their retention periods in days"""
        return {
            'transaction_info': None,  # Core transaction data - no expiry
            'account_info': None,      # Account related data - 180 days retention
            'metadata': None            # Enriched data - 90 days retention
        }

    def modify_column_family(self, table: Table, cf_id: str, retention_days: Optional[int]):
        """Create or modify a single column family"""
        try:
            cf = table.column_family(cf_id)
            
            if retention_days:
                # Set GC rule with retention
                gc_rule = column_family.MaxAgeGCRule(timedelta(days=retention_days))
                cf = table.column_family(cf_id, gc_rule=gc_rule)
                cf.create()
                logging.info(f"Created column family: {cf_id} with retention: {retention_days} days")
            else:
                # Create without GC rule
                cf.create()
                logging.info(f"Created column family: {cf_id} with retention: infinite")
                
        except exceptions.AlreadyExists:
            logging.info(f"Column family {cf_id} already exists")
        except Exception as e:
            logging.error(f"Failed to create column family {cf_id}: {str(e)}")
            raise

    def add_missing_column_families(self, table: Table, missing_cfs: Set[str]):
        """Add missing column families to existing table"""
        required_cfs = self.get_column_families()
        
        for cf_id in missing_cfs:
            try:
                retention_days = required_cfs[cf_id]
                self.modify_column_family(table, cf_id, retention_days)
                logging.info(f"Added missing column family: {cf_id}")
            except Exception as e:
                logging.error(f"Error adding column family {cf_id}: {str(e)}")
                raise

    def create_table(self, table_id: str) -> bool:
        """Create BigTable table with column families or add missing ones"""
        if not self.validate_instance():
            raise RuntimeError(f"BigTable instance {self.instance_id} not available")

        table = self.instance.table(table_id)

        try:
            if table.exists():
                logging.info(f"Table {table_id} exists")
                # Verify column families
                existing_cfs = set(table.list_column_families().keys())
                required_cfs = set(self.get_column_families().keys())
                
                missing_cfs = required_cfs - existing_cfs
                if missing_cfs:
                    logging.info(f"Adding missing column families: {missing_cfs}")
                    self.add_missing_column_families(table, missing_cfs)
                    logging.info(f"Added all missing column families to table {table_id}")
                else:
                    logging.info(f"Table {table_id} has all required column families")
                return True

            # Create new table
            table.create()
            logging.info(f"Created new table: {table_id}")

            # Create all column families for new table
            for cf_id, retention_days in self.get_column_families().items():
                self.modify_column_family(table, cf_id, retention_days)

            logging.info(f"Created table {table_id} with all column families")
            return True

        except exceptions.AlreadyExists:
            logging.warning(f"Table {table_id} already exists")
            return False
        except exceptions.PermissionDenied:
            logging.error(f"Permission denied accessing table {table_id}. Check IAM roles.")
            raise
        except Exception as e:
            logging.error(f"Error creating/modifying table {table_id}: {str(e)}")
            raise

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # Configuration
    PROJECT_ID = 'agentic-experiments-446019'
    INSTANCE_ID = 'pipeline-dev'
    TABLE_ID = 'payments'

    try:
        setup = BigtableSetup(PROJECT_ID, INSTANCE_ID)
        setup.create_table(TABLE_ID)
        logging.info("BigTable setup completed successfully")
    except Exception as e:
        logging.error(f"BigTable setup failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()