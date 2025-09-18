"""Airtable client for storing parsed transaction data."""

import json
import logging
import requests
from typing import Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class AirtableClient:
    """Handle Airtable API operations."""
    
    def __init__(self, config):
        self.config = config
        self.base_id = config.get('AIRTABLE_BASE_ID')
        self.api_key = config.get('AIRTABLE_API_KEY')
        self.base_url = f"https://api.airtable.com/v0/{self.base_id}"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
    def create_purchase(self, data: Dict) -> Optional[Dict]:
        """Create a purchase record in Airtable."""
        table_name = self.config.get('AIRTABLE_PURCHASES_TABLE', 'Purchases')
        
        # Extract parse metadata
        parse_metadata = data.get('parse_metadata', {})
        parse_result = data.get('parse_result', {})
        
        # Transform data for Airtable
        record = {
            "fields": {
                "Order Number": data.get('order_number'),
                "Date": data.get('date'),
                "Vendor": data.get('vendor_name'),
                "Items": json.dumps(data.get('items', [])),
                "Subtotal": data.get('subtotal', 0),
                "Taxes": data.get('taxes', 0),
                "Shipping": data.get('shipping', 0),
                "Total": data.get('total', 0),
                "Email UID": data.get('email_uid'),
                "Processed At": datetime.now().isoformat(),
                # Review fields
                "Requires Review": data.get('requires_review', False),
                "Confidence Score": data.get('confidence_score', 0),
                "Missing Fields": ', '.join(parse_result.get('missing_fields', [])),
                "Parse Status": parse_metadata.get('status', 'unknown'),
                "Parse Warnings": json.dumps(parse_result.get('warnings', [])),
                "Review Notes": self._generate_review_notes(data, parse_result)
            }
        }
        
        try:
            response = requests.post(
                f"{self.base_url}/{table_name}",
                json={"records": [record]},
                headers=self.headers
            )
            response.raise_for_status()
            
            result = response.json()
            logger.info(f"Created purchase record in Airtable: {result['records'][0]['id']}")
            
            if data.get('requires_review'):
                logger.info(f"Record marked for review: {result['records'][0]['id']}")
                
            return result['records'][0]
            
        except Exception as e:
            logger.error(f"Failed to create Airtable purchase record: {str(e)}")
            raise
            
    def create_sale(self, data: Dict) -> Optional[Dict]:
        """Create a sale record in Airtable."""
        table_name = self.config.get('AIRTABLE_SALES_TABLE', 'Sales')
        
        # Extract parse metadata
        parse_metadata = data.get('parse_metadata', {})
        parse_result = data.get('parse_result', {})
        
        # Transform data for Airtable
        record = {
            "fields": {
                "Order Number": data.get('order_number'),
                "Date": data.get('date'),
                "Channel": data.get('channel'),
                "Customer Email": data.get('customer_email'),
                "Items": json.dumps(data.get('items', [])),
                "Subtotal": data.get('subtotal', 0),
                "Taxes": data.get('taxes', 0),
                "Fees": data.get('fees', 0),
                "Total": data.get('total', 0),
                "Email UID": data.get('email_uid'),
                "Processed At": datetime.now().isoformat(),
                # Review fields
                "Requires Review": data.get('requires_review', False),
                "Confidence Score": data.get('confidence_score', 0),
                "Missing Fields": ', '.join(parse_result.get('missing_fields', [])),
                "Parse Status": parse_metadata.get('status', 'unknown'),
                "Parse Warnings": json.dumps(parse_result.get('warnings', [])),
                "Review Notes": self._generate_review_notes(data, parse_result)
            }
        }
        
        try:
            response = requests.post(
                f"{self.base_url}/{table_name}",
                json={"records": [record]},
                headers=self.headers
            )
            response.raise_for_status()
            
            result = response.json()
            logger.info(f"Created sale record in Airtable: {result['records'][0]['id']}")
            
            if data.get('requires_review'):
                logger.info(f"Record marked for review: {result['records'][0]['id']}")
                
            return result['records'][0]
            
        except Exception as e:
            logger.error(f"Failed to create Airtable sale record: {str(e)}")
            raise
            
    def _generate_review_notes(self, data: Dict, parse_result: Dict) -> str:
        """Generate review notes for records requiring manual review."""
        notes = []
        
        # Add missing field notes
        missing_fields = parse_result.get('missing_fields', [])
        if missing_fields:
            notes.append(f"Missing fields: {', '.join(missing_fields)}")
            
        # Add warning notes
        warnings = parse_result.get('warnings', [])
        if warnings:
            notes.append(f"Warnings: {'; '.join(warnings[:3])}")
            
        # Add specific item issues
        items = data.get('items', [])
        for i, item in enumerate(items):
            if item.get('needs_sku'):
                notes.append(f"Item {i+1} needs SKU: {item.get('name', 'Unknown')}")
                
        # Add total mismatch note
        if data.get('total_mismatch'):
            notes.append(f"Total mismatch: calculated ${data.get('total_calculated', 0):.2f} vs stated ${data.get('total', 0):.2f}")
            
        return ' | '.join(notes) if notes else ""