import apache_beam as beam
from google.cloud import bigtable
import logging
import json
from datetime import datetime

logger = logging.getLogger(__name__)

class WriteToBigtable(beam.DoFn):
    """Write payment data to BigTable"""
    
    def __init__(self, project_id: str, instance_id: str, table_id: str):
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self.client = None
        self.table = None
        
    def setup(self):
        """Initialize BigTable client"""
        logger.info(f"Setting up BigTable client for table: {self.table_id}")
        self.client = bigtable.Client(project=self.project_id, admin=True)
        instance = self.client.instance(self.instance_id)
        self.table = instance.table(self.table_id)
        
    def process(self, element):
        try:
            if not self.table:
                self.setup()
            
            row_key = element['row_key']
            payment_data = element['data']
            
            # Get the row
            row = self.table.direct_row(row_key)
            
            # Add payment header information
            header = payment_data['payment_header']
            for key, value in header.items():
                row.set_cell(
                    'payment_header',
                    key.encode(),
                    str(value).encode(),
                    timestamp=datetime.now()
                )
            
            # Add amount information
            amount = payment_data['amount']
            for key, value in amount.items():
                row.set_cell(
                    'amount_info',
                    key.encode(),
                    str(value).encode(),
                    timestamp=datetime.now()
                )
            
            # Add party information
            parties = payment_data['parties']
            for party_type, party_info in parties.items():
                for key, value in party_info.items():
                    if isinstance(value, dict):
                        value = json.dumps(value)
                    row.set_cell(
                        'party_info',
                        f'{party_type}:{key}'.encode(),
                        str(value).encode(),
                        timestamp=datetime.now()
                    )
            
            # Add type-specific data if present
            if 'type_specific_data' in payment_data:
                for key, value in payment_data['type_specific_data'].items():
                    if isinstance(value, (list, dict)):
                        value = json.dumps(value)
                    row.set_cell(
                        'type_specific',
                        key.encode(),
                        str(value).encode(),
                        timestamp=datetime.now()
                    )
            
            # Commit the row
            row.commit()
            logger.info(f"Successfully wrote payment to BigTable: {row_key}")
            
            # Return element for further processing if needed
            yield element
            
        except Exception as e:
            logger.error(f"Error writing to BigTable: {str(e)}")
            logger.error(f"Failed payment: {json.dumps(element, indent=2)}")
            raise  # Re-raise the exception to mark the element as failed
