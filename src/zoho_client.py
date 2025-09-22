"""Main Zoho client - refactored with modular architecture."""

import logging
from typing import Dict
from datetime import datetime

from .zoho.base_client import ZohoBaseClient
from .zoho.entities import ZohoEntityManager
from .zoho.workflows import ZohoWorkflowProcessor

logger = logging.getLogger(__name__)


class ZohoClient:
    """Main Zoho client with proper Purchase Order and Sales Order workflows."""
    
    def __init__(self, config):
        """Initialize the modular Zoho client."""
        self.config = config
        
        # Initialize components
        self.base_client = ZohoBaseClient(config)
        self.entity_manager = ZohoEntityManager(self.base_client)
        self.workflow_processor = ZohoWorkflowProcessor(self.base_client, self.entity_manager)
        
        # Expose commonly used properties from base client
        self.use_proper_workflows = self.workflow_processor.use_proper_workflows
        self.auto_create_bills = self.workflow_processor.auto_create_bills
        self.auto_create_invoices = self.workflow_processor.auto_create_invoices
        self.auto_create_shipments = self.workflow_processor.auto_create_shipments
        self.allow_direct_adjustments = self.workflow_processor.allow_direct_adjustments
        
        logger.info(f"🔧 Zoho client initialized with modular architecture:")
        logger.info(f"   - Use Proper Workflows: {self.use_proper_workflows}")
        logger.info(f"   - Auto Create Bills: {self.auto_create_bills}")
        logger.info(f"   - Auto Create Invoices: {self.auto_create_invoices}")
        logger.info(f"   - Auto Create Shipments: {self.auto_create_shipments}")
        logger.info(f"   - Allow Direct Adjustments: {self.allow_direct_adjustments}")

    @property
    def is_available(self) -> bool:
        """Check if Zoho API is available."""
        return self.base_client._ensure_connection()

    def process_complete_data(self, clean_data: Dict, transaction_type: str) -> Dict:
        """
        Process clean data from Airtable through proper Zoho workflows.
        
        Args:
            clean_data: Clean, validated data from Airtable
            transaction_type: 'purchase' or 'sale'
            
        Returns:
            Dict with processing results
        """
        return self.workflow_processor.process_complete_data(clean_data, transaction_type)

    def test_connection(self) -> bool:
        """Test Zoho API connection and return status."""
        return self.base_client.test_connection()

    # ===========================================
    # VALIDATION METHODS
    # ===========================================

    def validate_inventory_adjustments_empty(self) -> Dict:
        """Check that inventory adjustments tab is empty (except manual adjustments)."""
        try:
            response = self.base_client._make_api_request('GET', 'inventoryadjustments')
            adjustments = response.get('inventory_adjustments', [])
            
            # Filter out manual adjustments (those with specific reasons)
            auto_adjustments = [
                adj for adj in adjustments 
                if 'Purchase - Stock Received' in adj.get('reason', '') or 
                   'Sale - Stock Reduced' in adj.get('reason', '')
            ]
            
            return {
                'total_adjustments': len(adjustments),
                'auto_adjustments': len(auto_adjustments),
                'is_clean': len(auto_adjustments) == 0,
                'auto_adjustment_ids': [adj.get('inventory_adjustment_id') for adj in auto_adjustments]
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to validate adjustments: {e}")
            return {'error': str(e)}

    def generate_inventory_sync_report(self) -> Dict:
        """Compare Airtable vs Zoho stock levels."""
        # This would need integration with AirtableClient
        # For now, return placeholder structure
        return {
            'timestamp': datetime.now().isoformat(),
            'items_compared': 0,
            'discrepancies': [],
            'total_value_difference': 0
        }