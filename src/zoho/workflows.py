"""Zoho workflow processors for purchases and sales."""

import logging
from typing import Dict, List
from datetime import datetime

logger = logging.getLogger(__name__)


class ZohoWorkflowProcessor:
    """Handles proper Zoho workflows for purchases and sales."""
    
    def __init__(self, base_client, entity_manager):
        self.base_client = base_client
        self.entity_manager = entity_manager
        self.config = base_client.config
        
        # Workflow configuration
        self.use_proper_workflows = self.config.get_bool('ZOHO_USE_PROPER_WORKFLOWS', True)
        self.auto_receive_po = self.config.get_bool('ZOHO_AUTO_RECEIVE_PO', True)
        self.auto_create_bills = self.config.get_bool('ZOHO_AUTO_CREATE_BILLS', True)
        self.auto_create_invoices = self.config.get_bool('ZOHO_AUTO_CREATE_INVOICES', True)
        self.auto_create_shipments = self.config.get_bool('ZOHO_AUTO_CREATE_SHIPMENTS', True)
        self.allow_direct_adjustments = self.config.get_bool('ZOHO_ALLOW_DIRECT_ADJUSTMENTS', False)

    def process_complete_data(self, clean_data: Dict, transaction_type: str) -> Dict:
        """Process clean data from Airtable through proper Zoho workflows."""
        result = {
            'success': False,
            'purchase_order_id': None,
            'sales_order_id': None,
            'bill_id': None,
            'invoice_id': None,
            'shipment_id': None,
            'items_processed': [],
            'revenue': 0,
            'cogs': 0,
            'errors': [],
            'workflow_steps': []
        }
        
        # Lazy connection
        if not self.base_client._ensure_connection():
            result['errors'].append("Zoho API is not available")
            return result
            
        try:
            if self.use_proper_workflows:
                if transaction_type == 'purchase':
                    return self._process_purchase_with_proper_workflow(clean_data)
                elif transaction_type == 'sale':
                    return self._process_sale_with_proper_workflow(clean_data)
                else:
                    result['errors'].append(f"Unknown transaction type: {transaction_type}")
            else:
                logger.warning("⚠️ Using legacy direct adjustment workflow - DEPRECATED")
                if not self.allow_direct_adjustments:
                    result['errors'].append("Direct adjustments are disabled. Enable ZOHO_USE_PROPER_WORKFLOWS.")
                    return result
                
                result['errors'].append("Legacy workflows not implemented - use proper workflows")
                
        except Exception as e:
            result['errors'].append(f"Processing error: {e}")
            logger.error(f"💥 Failed to process {transaction_type}: {e}", exc_info=True)
            
        return result

    def _process_purchase_with_proper_workflow(self, airtable_data: Dict) -> Dict:
        """Process purchase using proper Purchase Order → Bill workflow."""
        result = {
            'success': False,
            'purchase_order_id': None,
            'bill_id': None,
            'items_processed': [],
            'errors': [],
            'workflow_steps': []
        }
        
        order_number = airtable_data.get('order_number', 'UNKNOWN')
        vendor_name = airtable_data.get('vendor_name', 'Unknown Vendor')
        
        logger.info(f"🔄 Processing purchase with proper workflow: {order_number}")
        logger.info(f"   - Vendor: {vendor_name}")
        logger.info(f"   - Items: {len(airtable_data.get('items', []))}")
        
        try:
            # Step 1: Find or create vendor
            logger.info("👥 Step 1: Finding/creating vendor...")
            vendor_id = self.entity_manager.find_or_create_vendor(vendor_name, airtable_data)
            result['workflow_steps'].append(f"Customer resolved: {channel} (ID: {customer_id})")
            
            # Step 2: Validate inventory and prepare items
            logger.info("📦 Step 2: Validating inventory...")
            processed_items = []
            total_revenue = 0
            
            for item in airtable_data.get('items', []):
                item_id = self.entity_manager.ensure_item_exists_in_zoho(item.get('sku'), item.get('name'))
                
                # Check available stock
                item_details = self.entity_manager.get_item_details(item_id)
                available_stock = item_details.get('available_stock', 0)
                requested_qty = item.get('quantity', 0)
                sale_price = item.get('sale_price', 0)
                
                if available_stock < requested_qty:
                    logger.warning(f"⚠️ Insufficient stock for {item.get('sku')}: {available_stock} < {requested_qty}")
                
                item_revenue = requested_qty * sale_price
                total_revenue += item_revenue
                
                processed_items.append({
                    'item_id': item_id,
                    'sku': item.get('sku'),
                    'name': item.get('name'),
                    'quantity': requested_qty,
                    'sale_price': sale_price,
                    'revenue': item_revenue,
                    'available_stock': available_stock
                })
            
            result['items_processed'] = processed_items
            result['revenue'] = total_revenue
            result['workflow_steps'].append(f"Items validated: {len(processed_items)}")
            
            # Step 3: Create Sales Order
            logger.info("📋 Step 3: Creating Sales Order...")
            so_data = self._build_sales_order_data(customer_id, processed_items, airtable_data)
            so_response = self.base_client._make_api_request('POST', 'salesorders', so_data)
            
            so_id = so_response.get('salesorder', {}).get('salesorder_id')
            so_number = so_response.get('salesorder', {}).get('salesorder_number')
            result['sales_order_id'] = so_id
            result['workflow_steps'].append(f"Sales Order created: {so_number}")
            
            logger.info(f"✅ Sales Order created: {so_number} (ID: {so_id})")
            
            # Step 4: Create Invoice (commits the sale)
            if self.auto_create_invoices:
                logger.info("🧾 Step 4: Creating Invoice from Sales Order...")
                invoice_response = self.base_client._make_api_request('POST', f'salesorders/{so_id}/convertto/invoice')
                
                invoice_id = invoice_response.get('invoice', {}).get('invoice_id')
                invoice_number = invoice_response.get('invoice', {}).get('invoice_number')
                result['invoice_id'] = invoice_id
                result['workflow_steps'].append(f"Invoice created: {invoice_number}")
                
                logger.info(f"✅ Invoice created: {invoice_number} (ID: {invoice_id})")
            
            # Step 5: Create Shipment (reduces inventory)
            if self.auto_create_shipments:
                logger.info("📦 Step 5: Creating Shipment...")
                shipment_data = self._build_shipment_data(so_id, processed_items)
                shipment_response = self.base_client._make_api_request('POST', f'salesorders/{so_id}/shipments', shipment_data)
                
                shipment_id = shipment_response.get('shipment', {}).get('shipment_id')
                shipment_number = shipment_response.get('shipment', {}).get('shipment_number')
                result['shipment_id'] = shipment_id
                result['workflow_steps'].append(f"Shipment created: {shipment_number} (inventory reduced)")
                
                logger.info(f"✅ Shipment created: {shipment_number} (ID: {shipment_id})")
                
                # Calculate COGS from shipment
                result['cogs'] = self._calculate_cogs_from_shipment(processed_items)
            
            result['success'] = True
            logger.info(f"🎉 Sales workflow completed successfully for {order_number}")
            
        except Exception as e:
            error_msg = f"Sales workflow failed: {e}"
            result['errors'].append(error_msg)
            logger.error(f"❌ {error_msg}", exc_info=True)
            
            # Cleanup on failure
            if result.get('sales_order_id'):
                self._cleanup_failed_sale(result['sales_order_id'], result.get('invoice_id'), result.get('shipment_id'))
                
        return result

    # ===========================================
    # DATA BUILDERS
    # ===========================================

    def _build_purchase_order_data(self, vendor_id: str, items: List[Dict], airtable_data: Dict) -> Dict:
        """Build purchase order data structure."""
        po_data = {
            'vendor_id': vendor_id,
            'date': airtable_data.get('date', datetime.now().strftime('%Y-%m-%d')),
            'reference_number': airtable_data.get('order_number', ''),
            'notes': f"Auto-generated from email parsing - Order: {airtable_data.get('order_number', '')}",
            'line_items': []
        }
        
        for item in items:
            line_item = {
                'item_id': item['item_id'],
                'quantity': item['quantity'],
                'rate': item['unit_price'],
                'description': item.get('name', '')
            }
            po_data['line_items'].append(line_item)
        
        # Add tax if present
        if airtable_data.get('taxes', 0) > 0:
            po_data['tax_total'] = airtable_data['taxes']
        
        return po_data

    def _build_receive_data(self, po_id: str, items: List[Dict]) -> Dict:
        """Build receive data for marking PO as received."""
        return {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'line_items': [
                {
                    'item_id': item['item_id'],
                    'quantity': item['quantity']
                }
                for item in items
            ]
        }

    def _build_sales_order_data(self, customer_id: str, items: List[Dict], airtable_data: Dict) -> Dict:
        """Build sales order data structure."""
        so_data = {
            'customer_id': customer_id,
            'date': airtable_data.get('date', datetime.now().strftime('%Y-%m-%d')),
            'reference_number': airtable_data.get('order_number', ''),
            'notes': f"Auto-generated from email parsing - Order: {airtable_data.get('order_number', '')}",
            'line_items': []
        }
        
        for item in items:
            line_item = {
                'item_id': item['item_id'],
                'quantity': item['quantity'],
                'rate': item['sale_price'],
                'description': item.get('name', '')
            }
            so_data['line_items'].append(line_item)
        
        # Add tax if present
        if airtable_data.get('taxes', 0) > 0:
            so_data['tax_total'] = airtable_data['taxes']
        
        return so_data

    def _build_shipment_data(self, so_id: str, items: List[Dict]) -> Dict:
        """Build shipment data structure."""
        return {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'delivery_method': 'Standard',
            'line_items': [
                {
                    'item_id': item['item_id'],
                    'quantity': item['quantity']
                }
                for item in items
            ]
        }

    def _calculate_cogs_from_shipment(self, items: List[Dict]) -> float:
        """Calculate COGS from shipped items using Zoho's FIFO method."""
        total_cogs = 0
        
        for item in items:
            try:
                item_details = self.entity_manager.get_item_details(item['item_id'])
                stock_rate = item_details.get('stock_rate', 0)
                quantity = item['quantity']
                
                item_cogs = stock_rate * quantity
                total_cogs += item_cogs
                
                logger.debug(f"COGS calculation: {item['sku']} - {quantity} × ${stock_rate} = ${item_cogs}")
                
            except Exception as e:
                logger.warning(f"⚠️ Could not calculate COGS for item {item.get('sku')}: {e}")
        
        return total_cogs

    # ===========================================
    # CLEANUP METHODS
    # ===========================================

    def _cleanup_failed_purchase(self, po_id: str, bill_id: str = None):
        """Clean up failed purchase workflow."""
        try:
            if bill_id:
                logger.info(f"🧹 Cleaning up failed bill: {bill_id}")
                self.base_client._make_api_request('DELETE', f'bills/{bill_id}')
            
            if po_id:
                logger.info(f"🧹 Cleaning up failed purchase order: {po_id}")
                self.base_client._make_api_request('DELETE', f'purchaseorders/{po_id}')
                
        except Exception as e:
            logger.warning(f"⚠️ Cleanup failed: {e}")

    def _cleanup_failed_sale(self, so_id: str, invoice_id: str = None, shipment_id: str = None):
        """Clean up failed sales workflow."""
        try:
            if shipment_id:
                logger.info(f"🧹 Cleaning up failed shipment: {shipment_id}")
                self.base_client._make_api_request('DELETE', f'shipments/{shipment_id}')
            
            if invoice_id:
                logger.info(f"🧹 Cleaning up failed invoice: {invoice_id}")
                self.base_client._make_api_request('DELETE', f'invoices/{invoice_id}')
            
            if so_id:
                logger.info(f"🧹 Cleaning up failed sales order: {so_id}")
                self.base_client._make_api_request('DELETE', f'salesorders/{so_id}')
                
        except Exception as e:
            logger.warning(f"⚠️ Cleanup failed: {e}")
            
            # Step 2: Ensure all items exist in Zoho
            logger.info("📦 Step 2: Ensuring items exist...")
            processed_items = []
            for item in airtable_data.get('items', []):
                item_id = self.entity_manager.ensure_item_exists_in_zoho(item.get('sku'), item.get('name'))
                processed_items.append({
                    'item_id': item_id,
                    'sku': item.get('sku'),
                    'name': item.get('name'),
                    'quantity': item.get('quantity', 0),
                    'unit_price': item.get('unit_price', 0)
                })
            
            result['items_processed'] = processed_items
            result['workflow_steps'].append(f"Items validated: {len(processed_items)}")
            
            # Step 3: Create Purchase Order
            logger.info("📋 Step 3: Creating Purchase Order...")
            po_data = self._build_purchase_order_data(vendor_id, processed_items, airtable_data)
            po_response = self.base_client._make_api_request('POST', 'purchaseorders', po_data)
            
            po_id = po_response.get('purchaseorder', {}).get('purchaseorder_id')
            po_number = po_response.get('purchaseorder', {}).get('purchaseorder_number')
            result['purchase_order_id'] = po_id
            result['workflow_steps'].append(f"Purchase Order created: {po_number}")
            
            logger.info(f"✅ Purchase Order created: {po_number} (ID: {po_id})")
            
            # Step 4: Mark PO as received (updates inventory)
            if self.auto_receive_po:
                logger.info("📥 Step 4: Marking Purchase Order as received...")
                receive_data = self._build_receive_data(po_id, processed_items)
                receive_response = self.base_client._make_api_request('POST', f'purchaseorders/{po_id}/receive', receive_data)
                result['workflow_steps'].append("Purchase Order marked as received (inventory updated)")
                
                logger.info("✅ Purchase Order marked as received - inventory updated")
            
            # Step 5: Create Bill (for accounting consistency)
            if self.auto_create_bills:
                logger.info("🧾 Step 5: Creating Bill from Purchase Order...")
                bill_response = self.base_client._make_api_request('POST', f'purchaseorders/{po_id}/convertto/bill')
                
                bill_id = bill_response.get('bill', {}).get('bill_id')
                bill_number = bill_response.get('bill', {}).get('bill_number')
                result['bill_id'] = bill_id
                result['workflow_steps'].append(f"Bill created: {bill_number}")
                
                logger.info(f"✅ Bill created: {bill_number} (ID: {bill_id})")
            
            result['success'] = True
            logger.info(f"🎉 Purchase workflow completed successfully for {order_number}")
            
        except Exception as e:
            error_msg = f"Purchase workflow failed: {e}"
            result['errors'].append(error_msg)
            logger.error(f"❌ {error_msg}", exc_info=True)
            
            # Cleanup on failure
            if result.get('purchase_order_id'):
                self._cleanup_failed_purchase(result['purchase_order_id'], result.get('bill_id'))
                
        return result

    def _process_sale_with_proper_workflow(self, airtable_data: Dict) -> Dict:
        """Process sale using proper Sales Order → Invoice → Shipment workflow."""
        result = {
            'success': False,
            'sales_order_id': None,
            'invoice_id': None,
            'shipment_id': None,
            'items_processed': [],
            'revenue': 0,
            'cogs': 0,
            'errors': [],
            'workflow_steps': []
        }
        
        order_number = airtable_data.get('order_number', 'UNKNOWN')
        channel = airtable_data.get('channel', 'Direct Sales')
        
        logger.info(f"🔄 Processing sale with proper workflow: {order_number}")
        logger.info(f"   - Channel: {channel}")
        logger.info(f"   - Items: {len(airtable_data.get('items', []))}")
        
        try:
            # Step 1: Find or create customer
            logger.info("👥 Step 1: Finding/creating customer...")
            customer_id = self.entity_manager.find_or_create_customer(channel, airtable_data.get('customer_email'))
            result['workflow_steps'].append(f