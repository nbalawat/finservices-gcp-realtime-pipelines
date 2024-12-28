from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any

@dataclass
class StandardizedPayment:
    """Common format for all payment types after standardization"""
    transaction_id: str
    payment_type: str
    amount: float
    currency: str
    sender_account: str
    receiver_account: str
    timestamp: datetime
    status: str
    metadata: Dict[str, Any]

class PaymentTypeRegistry:
    """Registry of supported payment types and their schemas"""
    
    ACH_SCHEMA = {
        'required_fields': {
            'ach_transaction_id': str,
            'amount': float,
            'receiver_account_number': str,
            'receiver_routing_number': str,
            'sender_account_number': str,
            'sender_routing_number': str,
        },
        'optional_fields': {
            'memo': str,
            'sec_code': str,
            'company_entry_description': str
        }
    }

    WIRE_SCHEMA = {
        'required_fields': {
            'wire_id': str,
            'amount': float,
            'currency': str,
            'beneficiary_account': str,
            'beneficiary_bank_code': str,
            'originator_account': str,
            'originator_bank_code': str
        },
        'optional_fields': {
            'correspondent_bank_code': str,
            'purpose_of_payment': str,
            'charges_details': str
        }
    }

    RTP_SCHEMA = {
        'required_fields': {
            'rtp_id': str,
            'amount': float,
            'currency': str,
            'creditor_account': str,
            'debtor_account': str,
            'settlement_date': str
        },
        'optional_fields': {
            'purpose_code': str,
            'remittance_information': str,
            'end_to_end_id': str
        }
    }

    @classmethod
    def validate_payment(cls, payment_type: str, data: Dict[str, Any]) -> bool:
        """Validate payment data against its schema"""
        schema = getattr(cls, f"{payment_type.upper()}_SCHEMA", None)
        if not schema:
            return False

        # Check required fields
        for field, field_type in schema['required_fields'].items():
            if field not in data:
                return False
            if not isinstance(data[field], field_type):
                return False

        # Check optional fields if present
        for field, field_type in schema['optional_fields'].items():
            if field in data and not isinstance(data[field], field_type):
                return False

        return True
