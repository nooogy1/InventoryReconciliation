"""Enhanced Airtable client with three-table architecture for inventory management."""

import json
import logging
import requests
import hashlib
from typing import Dict, Optional, List, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)


class AirtableClient:
    """Handle Airtable API operations with three-table inventory architecture."""
    
    def __init__(self, config):
        self.config = config
        self.base_id = config.get('AIRTABLE_BASE_ID')
        self.api_key = config.get('AIRTABLE_API_KEY')
        self.base_url = f"https://api.airtable.com/v0/{self.base_id}"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        # Table names from environment variables
        self.purchases_table = config.get('AIRTABLE_PURCHASES_TABLE', 'InventoryPurchases')
        self.sales_table = config.get('AIRTABLE_SALES_TABLE', 'InventorySales')
        self.inventory_table = config.get('AIRTABLE_INVENTORY_TABLE', 'InventoryStock')
        
        logger.info(f"🗃️ Airtable client initialized:")
        logger.info(f"   - Purchases: {self.purchases_table}")
        logger.info(f"   - Sales: {self.sales_table}")
        logger.info(f"   - Inventory: {self.inventory_table}")
        
    def process_transaction(self, data: Dict, transaction_type: str) -> Dict:
        """
        Process a complete transaction through the three-table workflow.
        
        Args:
            data: Parsed transaction data
            transaction_type: 'purchase' or 'sale'
            
        Returns:
            Processing result with record IDs and inventory updates
        """
        logger.info(f"📊 Processing {transaction_type} transaction: {data.get('order_number', 'N/A')}")
        
        result = {
            'success': False,
            'transaction_record_id': None,
            'inventory_updates': [],
            'items_processed': [],
            'items_failed': [],
            'errors': [],
            'warnings': []
        }
        
        try:
            # Step 1: Process each item through inventory management
            processed_items = []
            
            for i, item in enumerate(data.get('items', []), 1):
                logger.info(f"   📦 [{i}/{len(data.get('items', []))}] Processing item: {item.get('name', 'Unknown')}")
                
                try:
                    # Get or create SKU and update inventory
                    inventory_result = self._process_item_inventory(item, transaction_type, data)
                    
                    if inventory_result['success']:
                        # Add SKU to item data for transaction record
                        item_with_sku = item.copy()
                        item_with_sku['sku'] = inventory_result['sku']
                        processed_items.append(item_with_sku)
                        
                        result['items_processed'].append({
                            'name': item.get('name'),
                            'sku': inventory_result['sku'],
                            'quantity': item.get('quantity'),
                            'inventory_record_id': inventory_result.get('inventory_record_id')
                        })
                        
                        result['inventory_updates'].append(inventory_result)
                        
                        logger.info(f"      ✅ Item processed: SKU {inventory_result['sku']}")
                        
                    else:
                        result['items_failed'].append({
                            'name': item.get('name'),
                            'errors': inventory_result.get('errors', [])
                        })
                        result['warnings'].extend(inventory_result.get('errors', []))
                        
                        logger.error(f"      ❌ Item failed: {'; '.join(inventory_result.get('errors', []))}")
                        
                except Exception as e:
                    error_msg = f"Failed to process item {item.get('name')}: {str(e)}"
                    result['items_failed'].append({
                        'name': item.get('name'),
                        'errors': [error_msg]
                    })
                    result['warnings'].append(error_msg)
                    logger.error(f"      💥 Item error: {e}")
            
            # Step 2: Create transaction record with clean data
            if processed_items:
                # Update data with processed items (now with SKUs)
                clean_data = data.copy()
                clean_data['items'] = processed_items
                clean_data['processing_status'] = 'airtable_complete'
                clean_data['inventory_items_count'] = len(processed_items)
                
                if transaction_type == 'purchase':
                    transaction_record = self.create_purchase(clean_data)
                else:
                    transaction_record = self.create_sale(clean_data)
                    
                result['transaction_record_id'] = transaction_record.get('id')
                result['success'] = True
                
                logger.info(f"   ✅ Transaction record created: {result['transaction_record_id']}")
                
            else:
                result['errors'].append("No items could be processed successfully")
                logger.error("   ❌ No items processed successfully")
                
        except Exception as e:
            result['errors'].append(f"Transaction processing error: {str(e)}")
            logger.error(f"💥 Transaction processing failed: {e}")
            
        return result
        
    def _process_item_inventory(self, item: Dict, transaction_type: str, transaction_data: Dict) -> Dict:
        """
        Process a single item through inventory management.
        
        Args:
            item: Item data from parsed email
            transaction_type: 'purchase' or 'sale'
            transaction_data: Full transaction context
            
        Returns:
            Result with SKU and inventory record info
        """
        result = {
            'success': False,
            'sku': None,
            'inventory_record_id': None,
            'action': None,  # 'created', 'updated', 'found'
            'previous_quantity': 0,
            'new_quantity': 0,
            'errors': []
        }
        
        try:
            # Step 1: Find or create inventory record
            inventory_record = self._find_or_create_inventory_item(item)
            
            if not inventory_record:
                result['errors'].append(f"Could not find or create inventory record for {item.get('name')}")
                return result
            
            result['sku'] = inventory_record['sku']
            result['inventory_record_id'] = inventory_record['record_id']
            result['previous_quantity'] = inventory_record.get('current_quantity', 0)
            result['action'] = inventory_record.get('action', 'found')
            
            # Step 2: Calculate new quantity based on transaction type
            quantity_change = item.get('quantity', 0)
            if transaction_type == 'sale':
                quantity_change = -quantity_change  # Sales reduce inventory
            
            new_quantity = max(0, result['previous_quantity'] + quantity_change)
            result['new_quantity'] = new_quantity
            
            # Step 3: Update inventory quantity
            update_success = self._update_inventory_quantity(
                inventory_record['record_id'],
                new_quantity,
                transaction_data.get('order_number', ''),
                transaction_type
            )
            
            if update_success:
                result['success'] = True
                logger.info(f"      📈 Inventory updated: {result['previous_quantity']} → {new_quantity}")
            else:
                result['errors'].append("Failed to update inventory quantity")
                
        except Exception as e:
            result['errors'].append(f"Inventory processing error: {str(e)}")
            
        return result
        
    def _find_or_create_inventory_item(self, item: Dict) -> Optional[Dict]:
        """
        Find existing inventory item or create new one with SKU generation.
        
        Args:
            item: Item data with name, UPC, etc.
            
        Returns:
            Dictionary with SKU, record_id, current_quantity, action
        """
        item_name = item.get('name', '').strip()
        if not item_name:
            logger.error("      ❌ No item name provided")
            return None
            
        logger.info(f"      🔍 Looking up inventory item: {item_name}")
        
        # Step 1: Try to find by existing identifiers
        existing_record = None
        
        # Try by SKU first
        if item.get('sku'):
            existing_record = self._find_inventory_by_sku(item['sku'])
            if existing_record:
                logger.info(f"      ✅ Found by SKU: {item['sku']}")
                return existing_record
        
        # Try by UPC
        if item.get('upc'):
            existing_record = self._find_inventory_by_upc(item['upc'])
            if existing_record:
                logger.info(f"      ✅ Found by UPC: {item['upc']}")
                return existing_record
        
        # Try by name
        existing_record = self._find_inventory_by_name(item_name)
        if existing_record:
            logger.info(f"      ✅ Found by name: {item_name}")
            return existing_record
        
        # Step 2: Create new inventory item
        logger.info(f"      🆕 Creating new inventory item")
        return self._create_new_inventory_item(item)
        
    def _find_inventory_by_sku(self, sku: str) -> Optional[Dict]:
        """Find inventory item by SKU."""
        try:
            params = {
                'filterByFormula': f"{{SKU}} = '{sku}'"
            }
            
            response = requests.get(
                f"{self.base_url}/{self.inventory_table}",
                headers=self.headers,
                params=params
            )
            response.raise_for_status()
            
            records = response.json().get('records', [])
            if records:
                record = records[0]
                return {
                    'record_id': record['id'],
                    'sku': record['fields'].get('SKU', ''),
                    'current_quantity': record['fields'].get('Quantity', 0),
                    'action': 'found'
                }
                
        except Exception as e:
            logger.debug(f"      Error finding by SKU {sku}: {e}")
            
        return None
        
    def _find_inventory_by_upc(self, upc: str) -> Optional[Dict]:
        """Find inventory item by UPC (UPC would be stored as SKU)."""
        try:
            # Since UPC becomes the SKU, search by SKU field
            params = {
                'filterByFormula': f"{{SKU}} = '{upc}'"
            }
            
            response = requests.get(
                f"{self.base_url}/{self.inventory_table}",
                headers=self.headers,
                params=params
            )
            response.raise_for_status()
            
            records = response.json().get('records', [])
            if records:
                record = records[0]
                return {
                    'record_id': record['id'],
                    'sku': record['fields'].get('SKU', ''),
                    'current_quantity': record['fields'].get('Quantity', 0),
                    'action': 'found'
                }
                
        except Exception as e:
            logger.debug(f"      Error finding by UPC {upc}: {e}")
            
        return None
        
    def _find_inventory_by_name(self, name: str) -> Optional[Dict]:
        """Find inventory item by name (exact match)."""
        try:
            # Escape single quotes in name for formula
            escaped_name = name.replace("'", "\\'")
            params = {
                'filterByFormula': f"{{Item Name}} = '{escaped_name}'"
            }
            
            response = requests.get(
                f"{self.base_url}/{self.inventory_table}",
                headers=self.headers,
                params=params
            )
            response.raise_for_status()
            
            records = response.json().get('records', [])
            if records:
                record = records[0]
                return {
                    'record_id': record['id'],
                    'sku': record['fields'].get('SKU', ''),
                    'current_quantity': record['fields'].get('Quantity', 0),
                    'action': 'found'
                }
                
        except Exception as e:
            logger.debug(f"      Error finding by name {name}: {e}")
            
        return None
        
    def _create_new_inventory_item(self, item: Dict) -> Optional[Dict]:
        """Create new inventory item with generated SKU."""
        try:
            # Generate SKU
            sku = self._generate_sku(item)
            
            # Create record with simplified schema
            record_data = {
                "fields": {
                    "SKU": sku,
                    "Item Name": item.get('name', ''),
                    "Quantity": 0,  # Start with 0, will be updated by transaction
                    "Created At": datetime.now().isoformat(),
                    "Last Updated": datetime.now().isoformat(),
                    "Last Transaction": "Initial creation"
                }
            }
            
            response = requests.post(
                f"{self.base_url}/{self.inventory_table}",
                json={"records": [record_data]},
                headers=self.headers
            )
            response.raise_for_status()
            
            result = response.json()
            new_record = result['records'][0]
            
            logger.info(f"      🎉 Created inventory item: {sku}")
            
            return {
                'record_id': new_record['id'],
                'sku': sku,
                'current_quantity': 0,
                'action': 'created'
            }
            
        except Exception as e:
            logger.error(f"      ❌ Failed to create inventory item: {e}")
            return None
            
    def _generate_sku(self, item: Dict) -> str:
        """Generate a unique SKU for an item."""
        # Use provided SKU if available
        if item.get('sku'):
            return item['sku'].upper()
            
        # Use UPC if available
        if item.get('upc'):
            return f"UPC-{item['upc']}"
            
        # Use product ID if available
        if item.get('product_id'):
            return f"ID-{item['product_id']}"
            
        # Generate from name
        name = item.get('name', 'UNKNOWN')
        
        # Clean name for SKU
        clean_name = ''.join(c for c in name.upper() if c.isalnum() or c in [' ', '-'])
        words = clean_name.split()
        
        # Take first letter of each word (max 4 words)
        if len(words) > 1:
            prefix = ''.join(w[0] for w in words[:4])
        else:
            # Use first 4 characters if single word
            prefix = clean_name[:4]
            
        # Add hash for uniqueness
        name_hash = hashlib.md5(name.encode()).hexdigest()[:6].upper()
        
        return f"AUTO-{prefix}-{name_hash}"
        
    def _update_inventory_quantity(self, record_id: str, new_quantity: int, 
                                  order_number: str, transaction_type: str) -> bool:
        """Update inventory quantity for a specific record."""
        try:
            update_data = {
                "fields": {
                    "Quantity": new_quantity,
                    "Last Updated": datetime.now().isoformat(),
                    "Last Transaction": f"{transaction_type.title()}: {order_number}"
                }
            }
            
            response = requests.patch(
                f"{self.base_url}/{self.inventory_table}/{record_id}",
                json=update_data,
                headers=self.headers
            )
            response.raise_for_status()
            
            return True
            
        except Exception as e:
            logger.error(f"      ❌ Failed to update inventory quantity: {e}")
            return False
            
    def create_purchase(self, data: Dict) -> Optional[Dict]:
        """Create a purchase record in Airtable."""
        logger.info(f"💾 Creating purchase record in Airtable...")
        
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
                "Email Seq Num": data.get('email_seq_num'),
                "Processed At": datetime.now().isoformat(),
                "Processing Status": data.get('processing_status', 'airtable_complete'),
                "Inventory Items Count": data.get('inventory_items_count', 0),
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
                f"{self.base_url}/{self.purchases_table}",
                json={"records": [record]},
                headers=self.headers
            )
            response.raise_for_status()
            
            result = response.json()
            logger.info(f"✅ Created purchase record: {result['records'][0]['id']}")
            
            return result['records'][0]
            
        except Exception as e:
            logger.error(f"❌ Failed to create purchase record: {str(e)}")
            raise
            
    def create_sale(self, data: Dict) -> Optional[Dict]:
        """Create a sale record in Airtable."""
        logger.info(f"💾 Creating sale record in Airtable...")
        
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
                "Email Seq Num": data.get('email_seq_num'),
                "Processed At": datetime.now().isoformat(),
                "Processing Status": data.get('processing_status', 'airtable_complete'),
                "Inventory Items Count": data.get('inventory_items_count', 0),
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
                f"{self.base_url}/{self.sales_table}",
                json={"records": [record]},
                headers=self.headers
            )
            response.raise_for_status()
            
            result = response.json()
            logger.info(f"✅ Created sale record: {result['records'][0]['id']}")
            
            return result['records'][0]
            
        except Exception as e:
            logger.error(f"❌ Failed to create sale record: {str(e)}")
            raise
            
    def get_records_ready_for_zoho_sync(self, transaction_type: str, limit: int = 10) -> List[Dict]:
        """
        Get records that are ready to be synced to Zoho.
        
        Args:
            transaction_type: 'purchase' or 'sale'
            limit: Maximum number of records to return
            
        Returns:
            List of records ready for Zoho sync
        """
        try:
            table_name = self.purchases_table if transaction_type == 'purchase' else self.sales_table
            
            params = {
                'filterByFormula': "{Processing Status} = 'airtable_complete'",
                'maxRecords': limit
            }
            
            response = requests.get(
                f"{self.base_url}/{table_name}",
                headers=self.headers,
                params=params
            )
            response.raise_for_status()
            
            records = response.json().get('records', [])
            
            # Transform records for Zoho processing
            processed_records = []
            for record in records:
                fields = record['fields']
                
                # Parse items JSON
                items = []
                if fields.get('Items'):
                    try:
                        items = json.loads(fields['Items'])
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid items JSON in record {record['id']}")
                
                processed_record = {
                    'airtable_record_id': record['id'],
                    'order_number': fields.get('Order Number'),
                    'date': fields.get('Date'),
                    'vendor_name': fields.get('Vendor'),
                    'channel': fields.get('Channel'),
                    'customer_email': fields.get('Customer Email'),
                    'items': items,
                    'subtotal': fields.get('Subtotal', 0),
                    'taxes': fields.get('Taxes', 0),
                    'shipping': fields.get('Shipping', 0),
                    'fees': fields.get('Fees', 0),
                    'total': fields.get('Total', 0),
                    'type': transaction_type
                }
                
                processed_records.append(processed_record)
            
            return processed_records
            
        except Exception as e:
            logger.error(f"Failed to get records ready for Zoho sync: {e}")
            return []
            
    def mark_record_synced_to_zoho(self, record_id: str, table_type: str, 
                                   zoho_adjustment_id: str = None, errors: List[str] = None) -> bool:
        """
        Mark a record as successfully synced to Zoho.
        
        Args:
            record_id: Airtable record ID
            table_type: 'purchase' or 'sale'
            zoho_adjustment_id: Zoho adjustment ID if successful
            errors: List of errors if sync failed
            
        Returns:
            Success status
        """
        try:
            table_name = self.purchases_table if table_type == 'purchase' else self.sales_table
            
            if errors:
                # Mark as failed
                update_data = {
                    "fields": {
                        "Processing Status": "zoho_sync_failed",
                        "Zoho Sync Errors": json.dumps(errors),
                        "Last Sync Attempt": datetime.now().isoformat()
                    }
                }
            else:
                # Mark as successful
                update_data = {
                    "fields": {
                        "Processing Status": "zoho_synced",
                        "Zoho Adjustment ID": zoho_adjustment_id,
                        "Synced to Zoho At": datetime.now().isoformat()
                    }
                }
            
            response = requests.patch(
                f"{self.base_url}/{table_name}/{record_id}",
                json=update_data,
                headers=self.headers
            )
            response.raise_for_status()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to mark record as synced: {e}")
            return False
            
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