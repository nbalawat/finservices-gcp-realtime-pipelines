"""Generate sample payment messages for testing."""

import json
import random
from datetime import datetime, timezone
from typing import Dict, Any
import uuid

def generate_customer_id() -> str:
    """Generate a random customer ID."""
    return f"CUST{random.randint(1000, 9999)}"

def generate_ach_message() -> Dict[str, Any]:
    """Generate a sample ACH payment message."""
    return {
        'ach_transaction_id': f"ACH{uuid.uuid4()}",
        'payment_type': 'ACH',
        'customer_id': generate_customer_id(),
        'amount': round(random.uniform(100, 10000), 2),
        'sender_routing_number': f"{random.randint(100000000, 999999999)}",
        'sender_account_number': f"{random.randint(1000000, 9999999)}",
        'receiver_routing_number': f"{random.randint(100000000, 999999999)}",
        'receiver_account_number': f"{random.randint(1000000, 9999999)}",
        'sec_code': random.choice(['PPD', 'CCD', 'WEB', 'TEL']),
        'memo': f"Payment for invoice #{random.randint(1000, 9999)}",
        'company_entry_description': 'PAYMENT',
        'customer_type': random.choice(['INDIVIDUAL', 'BUSINESS'])
    }

def generate_wire_message() -> Dict[str, Any]:
    """Generate a sample wire transfer message."""
    return {
        'wire_id': f"WIRE{uuid.uuid4()}",
        'payment_type': 'WIRE',
        'customer_id': generate_customer_id(),
        'amount': round(random.uniform(1000, 100000), 2),
        'currency': random.choice(['USD', 'EUR', 'GBP', 'JPY']),
        'sender_bank_code': f"BANK{random.randint(100, 999)}",
        'sender_account': f"ACCT{random.randint(1000000, 9999999)}",
        'receiver_bank_code': f"BANK{random.randint(100, 999)}",
        'receiver_account': f"ACCT{random.randint(1000000, 9999999)}",
        'correspondent_bank': f"CORR{random.randint(100, 999)}",
        'purpose_code': random.choice(['SUPP', 'SALA', 'TRAD', 'COMM']),
        'instruction_code': random.choice(['CHQB', 'CORT', 'PHOB', 'REPA']),
        'customer_type': random.choice(['INDIVIDUAL', 'BUSINESS'])
    }

def generate_rtp_message() -> Dict[str, Any]:
    """Generate a sample RTP (Real-Time Payment) message."""
    return {
        'rtp_id': f"RTP{uuid.uuid4()}",
        'payment_type': 'RTP',
        'customer_id': generate_customer_id(),
        'amount': round(random.uniform(10, 5000), 2),
        'sender_bank_id': f"BANK{random.randint(100, 999)}",
        'sender_account': f"ACCT{random.randint(1000000, 9999999)}",
        'receiver_bank_id': f"BANK{random.randint(100, 999)}",
        'receiver_account': f"ACCT{random.randint(1000000, 9999999)}",
        'settlement_speed': random.choice(['INSTANT', 'SAME_DAY', 'NEXT_DAY']),
        'purpose': random.choice(['PERSONAL', 'BUSINESS', 'INVOICE', 'TRANSFER']),
        'remittance_info': f"RMT{random.randint(1000, 9999)}",
        'customer_type': random.choice(['INDIVIDUAL', 'BUSINESS'])
    }

def generate_sample_messages(num_messages: int = 10) -> str:
    """Generate a batch of sample messages."""
    messages = []
    generators = [generate_ach_message, generate_wire_message, generate_rtp_message]
    
    for _ in range(num_messages):
        message = random.choice(generators)()
        messages.append(message)
    
    return json.dumps(messages, indent=2)

if __name__ == '__main__':
    print(generate_sample_messages())
