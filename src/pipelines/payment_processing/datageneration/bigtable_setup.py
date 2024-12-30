from google.cloud import bigtable
from google.cloud.bigtable import column_family
from typing import Dict
import logging
from datetime import timedelta

# Configure logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BigTableSetup:
    TABLE_CONFIGS = {
        'payments_by_customer': 'customerId#transaction_date#transaction_type',
        'payments_by_date': 'transaction_date#transaction_type#customerId',
        'payments_by_transaction': 'transaction_id#customerId#transaction_date',
        'payments_by_id': 'transaction_id',
        'payments_by_id_date': 'transaction_id#transaction_date'
    }

    COLUMN_FAMILIES = {
        'payment_header': None,
        'amount_info': None,
        'party_info': 180,  # 180 days retention
        'processing_info': 90,  # 90 days retention
        'type_specific': None,
        'metadata': 30  # 30 days retention
    }

    def __init__(self, project_id: str, instance_id: str):
        self.project_id = project_id
        self.instance_id = instance_id
        logger.info(f"Initializing BigTableSetup for project: {project_id}, instance: {instance_id}")
        self.client = bigtable.Client(project=project_id, admin=True)
        self.instance = self.client.instance(instance_id)

    def create_tables(self) -> None:
        """Create all required tables with column families"""
        logger.info(f"Starting table creation for {len(self.TABLE_CONFIGS)} tables")
        for table_id in self.TABLE_CONFIGS.keys():
            logger.info(f"Processing table: {table_id}")
            self._create_table(table_id)
        logger.info("Table creation completed")

    def _create_table(self, table_id: str) -> None:
        """Create single table with column families"""
        table = self.instance.table(table_id)
        
        if not table.exists():
            logger.info(f"Creating new table: {table_id}")
            table.create()
            create_all_column_families = True
        else:
            logger.info(f"Table {table_id} exists, checking for missing column families")
            existing_cfs = set(table.list_column_families().keys())
            required_cfs = set(self.COLUMN_FAMILIES.keys())
            missing_cfs = required_cfs - existing_cfs
            
            if missing_cfs:
                logger.info(f"Found missing column families: {missing_cfs}")
                self.COLUMN_FAMILIES = {cf: retention for cf, retention in self.COLUMN_FAMILIES.items() if cf in missing_cfs}
                create_all_column_families = True
            else:
                logger.info(f"All required column families exist in table {table_id}")
                create_all_column_families = False
        
        if create_all_column_families:
            for cf_name, retention_days in self.COLUMN_FAMILIES.items():
                logger.info(f"Creating column family: {cf_name} for table: {table_id}")
                if retention_days:
                    logger.info(f"Setting retention period of {retention_days} days for {cf_name}")
                    gc_rule = column_family.MaxAgeGCRule(timedelta(days=retention_days))
                    cf = table.column_family(cf_name, gc_rule=gc_rule)
                    cf.create()
                else:
                    cf = table.column_family(cf_name)
                    logger.info(f"Creating column family {cf_name} without retention")
                    cf.create()
                logger.info(f"Successfully created column family {cf_name} for table {table_id}")

    def verify_tables(self) -> bool:
        """Verify all tables exist with correct column families"""
        logger.info("Starting table verification")
        for table_id in self.TABLE_CONFIGS.keys():
            logger.info(f"Verifying table: {table_id}")
            table = self.instance.table(table_id)
            if not table.exists():
                logger.error(f"Verification failed: Table {table_id} does not exist")
                return False
                
            existing_cfs = set(table.list_column_families().keys())
            required_cfs = set(self.COLUMN_FAMILIES.keys())
            
            logger.info(f"Checking column families for table {table_id}")
            logger.info(f"Required column families: {required_cfs}")
            logger.info(f"Existing column families: {existing_cfs}")
            
            if not required_cfs.issubset(existing_cfs):
                missing_cfs = required_cfs - existing_cfs
                logger.error(f"Verification failed: Missing column families in table {table_id}: {missing_cfs}")
                return False
                
        logger.info("Table verification completed successfully")
        return True

def setup_bigtable(project_id: str, instance_id: str) -> None:
    """Setup BigTable instance and tables"""
    logger.info(f"=== Starting BigTable Setup ===")
    logger.info(f"Project ID: {project_id}")
    logger.info(f"Instance ID: {instance_id}")
    
    setup = BigTableSetup(project_id, instance_id)
    setup.create_tables()
    if setup.verify_tables():
        logger.info("=== BigTable Setup Completed Successfully ===")
    else:
        logger.error("=== BigTable Setup Failed ===")
        raise RuntimeError("BigTable setup failed")

if __name__ == "__main__":
    logger.info("=== BigTable Setup Script Started ===")
    setup_bigtable("your-project-id", "your-instance-id")
    logger.info("=== BigTable Setup Script Completed ===")