"""Zoho Inventory client with proper bill/invoice handling for inventory updates."""

import logging
import requests
import json
from typing import Dict, Optional, List, Tuple
from datetime import datetime, timedelta
from functools import lru_cache
from threading import Lock

logger = logging.getLogger(__name__)


class ZohoClient:
    """Handle Zoho Inventory API operations with proper inventory movement."""
    
    def __init__(self, config):
        self.config = config
        self.organization_id = config.get('ZOHO_ORGANIZATION_ID')
        self.access_token = None
        self.base_url = "https://inventory.zoho.com/api/v1"
        self.api_region = config.get('ZOHO_API_REGION', 'com')
        
        # Adjust base URL for region
        if self.api_region != 'com':
            self.base_url = f"https://inventory.zoho.{self.api_region}/api/v1"
            
        # Cache for entities to avoid repeated lookups
        self._cache = {
            'items': {},
            'vendors': {},
            'customers': {},
            'taxes': {}
        }
        self._cache_lock = Lock()
        
        # Configuration flags
        self.auto_create_bill = config.get_bool('ZOHO_AUTO_CREATE_BILL', True)
        self.auto_create_invoice = config.get_bool('ZOHO_AUTO_CREATE_INVOICE', True)
        self.auto_ship_sales = config.get_bool('ZOHO_AUTO_SHIP_SALES', True)
        self.use_zoho_wac = config.get_bool('ZOHO_USE_NATIVE_WAC', True)
        
        # Tax configuration
        self.default_tax_id = config.get('ZOHO_DEFAULT_TAX_ID')
        self.tax_inclusive = config.get_bool('ZOHO_TAX_INCLUSIVE', False)
        
        self._refresh_access_token()
        self._load_tax_configuration()
        
    def _refresh_access_token(self):
        """Refresh Zoho access token using refresh token."""
        try:
            url = f"https://accounts.zoho.{self.api_region}/oauth/v2/token"
            data = {
                "refresh_token": self.config.get('ZOHO_REFRESH_TOKEN'),
                "client_id": self.config.get('ZOHO_CLIENT_ID'),
                "client_secret": self.config.get('ZOHO_CLIENT_SECRET'),
                "grant_type": "refresh_token"
            }
            
            response = requests.post(url, data=data)
            response.raise_for_status()
            
            token_data = response.json()
            self.access_token = token_data['access_token']
            logger.info("Refreshed Zoho access token")
            
        except Exception as e:
            logger.error(f"Failed to refresh Zoho token: {str(e)}")
            raise
            
    def _get_headers(self):
        """Get headers for Zoho API requests."""
        return {
            "Authorization": f"Zoho-oauthtoken {self.access_token}",
            "Content-Type": "application/json"
        }
        
    def _make_api_request(self, method: str, endpoint: str, data: Optional[Dict] = None, 
                         params: Optional[Dict] = None, retry: bool = True) -> Dict:
        """Make API request with automatic token refresh on 401."""
        url = f"{self.base_url}/{endpoint}"
        
        if params is None:
            params = {}
        params['organization_id'] = self.organization_id
        
        # First attempt
        response = requests.request(
            method=method,
            url=url,
            json=data,
            params=params,
            headers=self._get_headers()
        )
        
        # Handle token expiration
        if response.status_code == 401 and retry:
            logger.info("Token expired, refreshing...")
            self._refresh_access_token()
            return self._make_api_request(method, endpoint, data, params, retry=False)
            
        response.raise_for_status()
        return response.json()
        
    def _load_tax_configuration(self):
        """Load tax configuration from Zoho."""
        try:
            response = self._make_api_request('GET', 'settings/taxes')
            taxes = response.get('taxes', [])
            
            with self._cache_lock:
                self._cache['taxes'] = {tax['tax_id']: tax for tax in taxes}
                
            logger.info(f"Loaded {len(taxes)} tax configurations")
            
        except Exception as e:
            logger.warning(f"Could not load tax configuration: {e}")
            
    def process_purchase_complete(self, data: Dict, parse_metadata: Dict = None) -> Dict:
        """
        Complete purchase workflow: PO → Bill → Inventory Update
        
        Args:
            data: Parsed purchase data
            parse_metadata: Metadata about parsing quality
            
        Returns:
            Dictionary with created entity IDs
        """
        result = {
            'purchase_order_id': None,
            'bill_id': None,
            'inventory_updated': False,
            'errors': []
        }
        
        try:
            # Step 1: Create Purchase Order
            logger.info("Creating purchase order in Zoho")
            po_response = self._create_purchase_order_internal(data, parse_metadata)
            result['purchase_order_id'] = po_response['purchaseorder']['purchaseorder_id']
            
            # Step 2: Create Bill (this updates inventory)
            if self.auto_create_bill:
                logger.info("Creating bill to update inventory")
                bill_response = self._create_bill_from_po(
                    result['purchase_order_id'],
                    data,
                    parse_metadata
                )
                result['bill_id'] = bill_response['bill']['bill_id']
                result['inventory_updated'] = True
                
                # Mark bill as paid if payment info available
                if data.get('payment_status') == 'paid':
                    self._mark_bill_paid(result['bill_id'], data)
            else:
                logger.info("Auto-bill creation disabled, inventory not updated")
                
        except Exception as e:
            error_msg = f"Purchase processing error: {str(e)}"
            logger.error(error_msg)
            result['errors'].append(error_msg)
            raise
            
        return result
        
    def process_sale_complete(self, data: Dict, parse_metadata: Dict = None) -> Dict:
        """
        Complete sales workflow: SO → Invoice → Shipment → Inventory Update
        
        Args:
            data: Parsed sales data
            parse_metadata: Metadata about parsing quality
            
        Returns:
            Dictionary with created entity IDs
        """
        result = {
            'sales_order_id': None,
            'invoice_id': None,
            'shipment_id': None,
            'inventory_updated': False,
            'errors': []
        }
        
        try:
            # Step 1: Create Sales Order
            logger.info("Creating sales order in Zoho")
            so_response = self._create_sales_order_internal(data, parse_metadata)
            result['sales_order_id'] = so_response['salesorder']['salesorder_id']
            
            # Step 2: Create Invoice (commits the sale)
            if self.auto_create_invoice:
                logger.info("Creating invoice from sales order")
                invoice_response = self._create_invoice_from_so(
                    result['sales_order_id'],
                    data,
                    parse_metadata
                )
                result['invoice_id'] = invoice_response['invoice']['invoice_id']
                
                # Mark as paid if payment received
                if data.get('payment_status') == 'paid':
                    self._mark_invoice_paid(result['invoice_id'], data)
                    
            # Step 3: Create Shipment (updates inventory)
            if self.auto_ship_sales:
                logger.info("Creating shipment to update inventory")
                shipment_response = self._create_shipment(
                    result['sales_order_id'],
                    data
                )
                result['shipment_id'] = shipment_response['shipmentorder']['shipmentorder_id']
                result['inventory_updated'] = True
            else:
                logger.info("Auto-shipment disabled, inventory not updated")
                
        except Exception as e:
            error_msg = f"Sales processing error: {str(e)}"
            logger.error(error_msg)
            result['errors'].append(error_msg)
            raise
            
        return result
        
    def _create_purchase_order_internal(self, data: Dict, parse_metadata: Dict = None) -> Dict:
        """Create a purchase order with enhanced validation."""
        # Get or create vendor with caching
        vendor_id = self._get_or_create_vendor_cached(data.get('vendor_name'))
        
        # Prepare line items with tax handling
        line_items = []
        for item in data.get('items', []):
            item_id = self._get_or_create_item_cached(
                item.get('sku'),
                item.get('name')
            )
            
            line_item = {
                "item_id": item_id,
                "quantity": item.get('quantity', 1),
                "rate": item.get('unit_price', 0)
            }
            
            # Handle item-level tax if available
            if item.get('tax'):
                line_item['tax_id'] = self._get_tax_id(item.get('tax_rate'))
            elif self.default_tax_id:
                line_item['tax_id'] = self.default_tax_id
                
            line_items.append(line_item)
            
        # Build PO data with metadata
        po_data = {
            "vendor_id": vendor_id,
            "purchaseorder_number": data.get('order_number', self._generate_po_number()),
            "date": data.get('date', datetime.now().strftime('%Y-%m-%d')),
            "line_items": line_items,
            "notes": self._build_notes(data, parse_metadata),
            "is_inclusive_tax": self.tax_inclusive
        }
        
        # Add header-level adjustments
        if data.get('taxes') and not self.tax_inclusive:
            po_data['tax_total'] = data.get('taxes')
            
        if data.get('shipping'):
            po_data['adjustment'] = data.get('shipping')
            po_data['adjustment_description'] = "Shipping & Handling"
            
        # Add custom fields for tracking
        po_data['custom_fields'] = [
            {"customfield_id": "email_uid", "value": data.get('email_uid', '')},
            {"customfield_id": "confidence_score", "value": str(data.get('confidence_score', 0))},
            {"customfield_id": "requires_review", "value": str(data.get('requires_review', False))}
        ]
        
        return self._make_api_request('POST', 'purchaseorders', po_data)
        
    def _create_bill_from_po(self, po_id: str, data: Dict, parse_metadata: Dict = None) -> Dict:
        """Create a bill from purchase order to update inventory."""
        # Get PO details
        po_response = self._make_api_request('GET', f'purchaseorders/{po_id}')
        po = po_response['purchaseorder']
        
        # Build bill data
        bill_data = {
            "vendor_id": po['vendor_id'],
            "bill_number": f"BILL-{data.get('order_number', po['purchaseorder_number'])}",
            "date": data.get('date', datetime.now().strftime('%Y-%m-%d')),
            "due_date": (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d'),
            "purchaseorder_ids": [po_id],
            "line_items": po['line_items'],  # Copy from PO
            "notes": f"Auto-created from PO {po['purchaseorder_number']}"
        }
        
        # Add adjustments if present
        if po.get('adjustment'):
            bill_data['adjustment'] = po['adjustment']
            bill_data['adjustment_description'] = po.get('adjustment_description', 'Adjustment')
            
        return self._make_api_request('POST', 'bills', bill_data)
        
    def _create_sales_order_internal(self, data: Dict, parse_metadata: Dict = None) -> Dict:
        """Create a sales order with enhanced validation."""
        # Get or create customer with caching
        customer_id = self._get_or_create_customer_cached(
            data.get('customer_email', f"{data.get('channel')}_customer"),
            data.get('channel')
        )
        
        # Prepare line items
        line_items = []
        for item in data.get('items', []):
            item_id = self._get_or_create_item_cached(
                item.get('sku'),
                item.get('name')
            )
            
            line_item = {
                "item_id": item_id,
                "quantity": item.get('quantity', 1),
                "rate": item.get('sale_price', 0)
            }
            
            # Handle item-level tax
            if item.get('tax'):
                line_item['tax_id'] = self._get_tax_id(item.get('tax_rate'))
            elif self.default_tax_id:
                line_item['tax_id'] = self.default_tax_id
                
            line_items.append(line_item)
            
        # Build SO data
        so_data = {
            "customer_id": customer_id,
            "salesorder_number": data.get('order_number', self._generate_so_number()),
            "date": data.get('date', datetime.now().strftime('%Y-%m-%d')),
            "line_items": line_items,
            "notes": self._build_notes(data, parse_metadata),
            "is_inclusive_tax": self.tax_inclusive
        }
        
        # Add adjustments
        if data.get('taxes') and not self.tax_inclusive:
            so_data['tax_total'] = data.get('taxes')
            
        if data.get('fees'):
            so_data['adjustment'] = -abs(data.get('fees'))  # Negative for fees
            so_data['adjustment_description'] = f"{data.get('channel', 'Platform')} Fees"
            
        # Add custom fields
        so_data['custom_fields'] = [
            {"customfield_id": "channel", "value": data.get('channel', '')},
            {"customfield_id": "email_uid", "value": data.get('email_uid', '')},
            {"customfield_id": "confidence_score", "value": str(data.get('confidence_score', 0))}
        ]
        
        return self._make_api_request('POST', 'salesorders', so_data)
        
    def _create_invoice_from_so(self, so_id: str, data: Dict, parse_metadata: Dict = None) -> Dict:
        """Create an invoice from sales order."""
        # Get SO details
        so_response = self._make_api_request('GET', f'salesorders/{so_id}')
        so = so_response['salesorder']
        
        # Build invoice data
        invoice_data = {
            "customer_id": so['customer_id'],
            "invoice_number": f"INV-{data.get('order_number', so['salesorder_number'])}",
            "date": data.get('date', datetime.now().strftime('%Y-%m-%d')),
            "due_date": datetime.now().strftime('%Y-%m-%d'),  # Due immediately for online sales
            "salesorder_id": so_id,
            "line_items": so['line_items'],
            "notes": f"Auto-created from SO {so['salesorder_number']}"
        }
        
        # Copy adjustments
        if so.get('adjustment'):
            invoice_data['adjustment'] = so['adjustment']
            invoice_data['adjustment_description'] = so.get('adjustment_description', 'Adjustment')
            
        return self._make_api_request('POST', 'invoices', invoice_data)
        
    def _create_shipment(self, so_id: str, data: Dict) -> Dict:
        """Create a shipment to update inventory."""
        # Get SO details for line items
        so_response = self._make_api_request('GET', f'salesorders/{so_id}')
        so = so_response['salesorder']
        
        # Build shipment data
        shipment_data = {
            "salesorder_id": so_id,
            "shipment_number": f"SHIP-{so['salesorder_number']}",
            "date": data.get('ship_date', datetime.now().strftime('%Y-%m-%d')),
            "delivery_method": data.get('shipping_method', 'Standard'),
            "tracking_number": data.get('tracking_number', ''),
            "line_items": [
                {
                    "so_line_item_id": item['line_item_id'],
                    "quantity": item['quantity']
                }
                for item in so['line_items']
            ],
            "notes": f"Auto-shipped for {data.get('channel', 'online')} order"
        }
        
        return self._make_api_request('POST', 'shipmentorders', shipment_data)
        
    def _mark_bill_paid(self, bill_id: str, data: Dict):
        """Mark a bill as paid."""
        try:
            payment_data = {
                "vendor_id": data.get('vendor_id'),
                "payment_mode": data.get('payment_method', 'Bank Transfer'),
                "amount": data.get('total', 0),
                "date": data.get('payment_date', datetime.now().strftime('%Y-%m-%d')),
                "bills": [
                    {
                        "bill_id": bill_id,
                        "amount_applied": data.get('total', 0)
                    }
                ]
            }
            
            self._make_api_request('POST', 'vendorpayments', payment_data)
            logger.info(f"Marked bill {bill_id} as paid")
            
        except Exception as e:
            logger.warning(f"Could not mark bill as paid: {e}")
            
    def _mark_invoice_paid(self, invoice_id: str, data: Dict):
        """Mark an invoice as paid."""
        try:
            payment_data = {
                "customer_id": data.get('customer_id'),
                "payment_mode": data.get('payment_method', 'Online Payment'),
                "amount": data.get('total', 0),
                "date": data.get('payment_date', datetime.now().strftime('%Y-%m-%d')),
                "invoices": [
                    {
                        "invoice_id": invoice_id,
                        "amount_applied": data.get('total', 0)
                    }
                ]
            }
            
            self._make_api_request('POST', 'customerpayments', payment_data)
            logger.info(f"Marked invoice {invoice_id} as paid")
            
        except Exception as e:
            logger.warning(f"Could not mark invoice as paid: {e}")
            
    @lru_cache(maxsize=1000)
    def _get_or_create_vendor_cached(self, vendor_name: str) -> str:
        """Get or create vendor with caching."""
        with self._cache_lock:
            # Check cache first
            if vendor_name in self._cache['vendors']:
                return self._cache['vendors'][vendor_name]
                
        # Search for existing vendor (exact match)
        params = {'vendor_name': vendor_name}
        response = self._make_api_request('GET', 'vendors', params=params)
        
        vendors = response.get('vendors', [])
        for vendor in vendors:
            if vendor['vendor_name'].lower() == vendor_name.lower():
                vendor_id = vendor['vendor_id']
                with self._cache_lock:
                    self._cache['vendors'][vendor_name] = vendor_id
                return vendor_id
                
        # Create new vendor
        vendor_data = {
            "vendor_name": vendor_name,
            "contact_type": "vendor",
            "vendor_email": f"{vendor_name.lower().replace(' ', '_')}@vendor.com"
        }
        
        response = self._make_api_request('POST', 'vendors', vendor_data)
        vendor_id = response['vendor']['vendor_id']
        
        with self._cache_lock:
            self._cache['vendors'][vendor_name] = vendor_id
            
        logger.info(f"Created new vendor: {vendor_name}")
        return vendor_id
        
    @lru_cache(maxsize=1000)
    def _get_or_create_customer_cached(self, customer_identifier: str, channel: str = None) -> str:
        """Get or create customer with caching."""
        with self._cache_lock:
            cache_key = f"{customer_identifier}_{channel}"
            if cache_key in self._cache['customers']:
                return self._cache['customers'][cache_key]
                
        # Search for existing customer
        if '@' in customer_identifier:
            params = {'email': customer_identifier}
        else:
            params = {'customer_name': customer_identifier}
            
        response = self._make_api_request('GET', 'customers', params=params)
        
        customers = response.get('customers', [])
        if customers:
            # Use exact match
            for customer in customers:
                if '@' in customer_identifier:
                    if customer.get('email', '').lower() == customer_identifier.lower():
                        customer_id = customer['customer_id']
                        with self._cache_lock:
                            self._cache['customers'][cache_key] = customer_id
                        return customer_id
                else:
                    if customer.get('customer_name', '').lower() == customer_identifier.lower():
                        customer_id = customer['customer_id']
                        with self._cache_lock:
                            self._cache['customers'][cache_key] = customer_id
                        return customer_id
                        
        # Create new customer
        customer_data = {
            "customer_name": customer_identifier.split('@')[0] if '@' in customer_identifier else customer_identifier,
            "customer_type": "individual",
            "payment_terms": 0,
            "notes": f"Channel: {channel}" if channel else ""
        }
        
        if '@' in customer_identifier:
            customer_data['email'] = customer_identifier
            
        response = self._make_api_request('POST', 'customers', customer_data)
        customer_id = response['customer']['customer_id']
        
        with self._cache_lock:
            self._cache['customers'][cache_key] = customer_id
            
        logger.info(f"Created new customer: {customer_identifier}")
        return customer_id
        
    @lru_cache(maxsize=5000)
    def _get_or_create_item_cached(self, sku: str, name: str = None) -> str:
        """Get or create item with caching and exact matching."""
        if not sku:
            sku = f"UNKNOWN-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
        with self._cache_lock:
            if sku in self._cache['items']:
                return self._cache['items'][sku]
                
        # Search for existing item by SKU (exact match)
        params = {'sku': sku}
        response = self._make_api_request('GET', 'items', params=params)
        
        items = response.get('items', [])
        for item in items:
            if item.get('sku', '').upper() == sku.upper():
                item_id = item['item_id']
                with self._cache_lock:
                    self._cache['items'][sku] = item_id
                return item_id
                
        # Create new item
        item_data = {
            "name": name or sku,
            "sku": sku.upper(),
            "item_type": "inventory",
            "purchase_rate": 0,
            "selling_price": 0,
            "inventory_account_name": "Inventory Asset",
            "purchase_account_name": "Cost of Goods Sold",
            "initial_stock": 0,
            "reorder_level": 5  # Default reorder point
        }
        
        response = self._make_api_request('POST', 'items', item_data)
        item_id = response['item']['item_id']
        
        with self._cache_lock:
            self._cache['items'][sku] = item_id
            
        logger.info(f"Created new item: {sku} - {name}")
        return item_id
        
    def _get_tax_id(self, tax_rate: float = None) -> Optional[str]:
        """Get tax ID for given rate or use default."""
        if tax_rate:
            with self._cache_lock:
                for tax_id, tax in self._cache.get('taxes', {}).items():
                    if abs(tax.get('tax_percentage', 0) - tax_rate) < 0.01:
                        return tax_id
                        
        return self.default_tax_id
        
    def _build_notes(self, data: Dict, parse_metadata: Dict = None) -> str:
        """Build notes field with metadata."""
        notes = []
        
        if data.get('channel'):
            notes.append(f"Channel: {data.get('channel')}")
            
        if parse_metadata:
            if parse_metadata.get('confidence_score'):
                notes.append(f"Confidence: {parse_metadata.get('confidence_score', 0):.2f}")
            if parse_metadata.get('missing_fields'):
                notes.append(f"Missing: {', '.join(parse_metadata.get('missing_fields', []))}")
            if parse_metadata.get('requires_review'):
                notes.append("⚠️ REQUIRES REVIEW")
                
        if data.get('email_uid'):
            notes.append(f"Email UID: {data.get('email_uid')}")
            
        return ' | '.join(notes)
        
    def _generate_po_number(self) -> str:
        """Generate a unique PO number."""
        return f"PO-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
    def _generate_so_number(self) -> str:
        """Generate a unique SO number."""
        return f"SO-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
    def get_inventory_levels(self, sku: str) -> Dict:
        """Get current inventory levels for an item."""
        try:
            item_id = self._get_or_create_item_cached(sku, sku)
            response = self._make_api_request('GET', f'items/{item_id}')
            
            item = response['item']
            return {
                'sku': item.get('sku'),
                'name': item.get('name'),
                'stock_on_hand': item.get('stock_on_hand', 0),
                'available_stock': item.get('available_stock', 0),
                'actual_available_stock': item.get('actual_available_stock', 0),
                'purchase_rate': item.get('purchase_rate', 0),
                'selling_price': item.get('selling_price', 0),
                'reorder_level': item.get('reorder_level', 0)
            }
            
        except Exception as e:
            logger.error(f"Failed to get inventory levels for {sku}: {e}")
            return {}
            
    def clear_cache(self):
        """Clear the entity cache."""
        with self._cache_lock:
            self._cache = {
                'items': {},
                'vendors': {},
                'customers': {},
                'taxes': self._cache.get('taxes', {})  # Keep tax config
            }
        logger.info("Cleared entity cache")
        
    # Legacy methods for backward compatibility
    def create_purchase_order(self, data: Dict) -> Dict:
        """Legacy method - redirects to complete workflow."""
        logger.warning("Using legacy create_purchase_order - consider using process_purchase_complete")
        return self.process_purchase_complete(data)
        
    def create_sales_order(self, data: Dict) -> Dict:
        """Legacy method - redirects to complete workflow."""
        logger.warning("Using legacy create_sales_order - consider using process_sale_complete")
        return self.process_sale_complete(data)
        
    def update_item_cost(self, sku: str, quantity: float, unit_price: float, taxes: float):
        """Legacy method - Zoho handles WAC automatically with bills."""
        if not self.use_zoho_wac:
            logger.warning("Manual WAC update requested but Zoho handles this automatically with bills")
        # No-op as Zoho handles WAC through proper bill creation
        pass
        
    def apply_cogs(self, sku: str, quantity: float):
        """Legacy method - Zoho handles COGS automatically with shipments."""
        # No-op as Zoho handles COGS through proper shipment creation
        pass