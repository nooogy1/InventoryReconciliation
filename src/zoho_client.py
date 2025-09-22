"""Zoho Inventory client with proper Purchase Order and Sales Order workflows."""

import logging
import requests
import json
import hashlib
from typing import Dict, Optional, List, Tuple
from datetime import datetime, timedelta
from functools import lru_cache
from threading import Lock

logger = logging.getLogger(__name__)


class ZohoClient:
    """Handle Zoho Inventory API with proper accounting workflows."""
    
    def __init__(self, config):
        self.config = config
        self.organization_id = config.get('ZOHO_ORGANIZATION_ID')
        self.access_token = None
        
        # Correct Zoho API endpoint from official documentation
        self.base_url = "https://www.zohoapis.com/inventory/v1"
        
        self.api_region = config.get('ZOHO_API_REGION', 'com')
        self.is_available = False  # Track if Zoho is available
        
        # Adjust base URL for different regions
        if self.api_region != 'com':
            region_urls = {
                'eu': "https://www.zohoapis.eu/inventory/v1",
                'in': "https://www.zohoapis.in/inventory/v1",
                'au': "https://www.zohoapis.com.au/inventory/v1",
                'jp': "https://www.zohoapis.jp/inventory/v1",
                'ca': "https://www.zohoapis.ca/inventory/v1",
                'cn': "https://www.zohoapis.com.cn/inventory/v1",
                'sa': "https://www.zohoapis.sa/inventory/v1"
            }
            self.base_url = region_urls.get(self.api_region, self.base_url)
            
        # Cache for efficient API usage
        self._cache = {
            'items': {},  # SKU -> item_id mapping
            'vendors': {},  # vendor_name -> vendor_id mapping
            'customers': {},  # channel_name -> customer_id mapping
            'taxes': {}
        }
        self._cache_lock = Lock()
        
        # NEW: Feature flags for proper workflow vs legacy
        self.use_proper_workflows = config.get_bool('ZOHO_USE_PROPER_WORKFLOWS', True)
        self.auto_receive_po = config.get_bool('ZOHO_AUTO_RECEIVE_PO', True)
        self.auto_create_bills = config.get_bool('ZOHO_AUTO_CREATE_BILLS', True)
        self.auto_create_invoices = config.get_bool('ZOHO_AUTO_CREATE_INVOICES', True)
        self.auto_create_shipments = config.get_bool('ZOHO_AUTO_CREATE_SHIPMENTS', True)
        self.allow_direct_adjustments = config.get_bool('ZOHO_ALLOW_DIRECT_ADJUSTMENTS', False)
        
        # Account configuration
        self.default_inventory_account = config.get('ZOHO_DEFAULT_INVENTORY_ACCOUNT')
        self.default_cogs_account = config.get('ZOHO_DEFAULT_COGS_ACCOUNT')
        self.default_sales_account = config.get('ZOHO_DEFAULT_SALES_ACCOUNT')
        
        logger.info(f"🔧 Initializing Zoho client (Proper Workflows Mode)...")
        logger.info(f"   - Base URL: {self.base_url}")
        logger.info(f"   - Use Proper Workflows: {self.use_proper_workflows}")
        logger.info(f"   - Auto Create Bills: {self.auto_create_bills}")
        logger.info(f"   - Auto Create Invoices: {self.auto_create_invoices}")
        logger.info(f"   - Allow Direct Adjustments: {self.allow_direct_adjustments}")
        
        # Initialize connection
        if self._refresh_access_token():
            self.is_available = self.test_connection()
            if self.is_available:
                self._load_cache()

    def _refresh_access_token(self) -> bool:
        """Refresh OAuth2 access token using refresh token."""
        try:
            auth_url = "https://accounts.zoho.com/oauth/v2/token"
            
            data = {
                'refresh_token': self.config.get('ZOHO_REFRESH_TOKEN'),
                'client_id': self.config.get('ZOHO_CLIENT_ID'),
                'client_secret': self.config.get('ZOHO_CLIENT_SECRET'),
                'grant_type': 'refresh_token'
            }
            
            response = requests.post(auth_url, data=data, timeout=30)
            response.raise_for_status()
            
            token_data = response.json()
            self.access_token = token_data.get('access_token')
            
            if self.access_token:
                logger.info("🔑 Zoho access token refreshed successfully")
                return True
            else:
                logger.error("❌ Failed to get access token from response")
                return False
                
        except Exception as e:
            logger.error(f"❌ Failed to refresh Zoho access token: {e}")
            return False
    
    def _get_headers(self) -> Dict[str, str]:
        """Get API request headers."""
        return {
            "Authorization": f"Zoho-oauthtoken {self.access_token}",
            "Content-Type": "application/json"
        }
        
    def _make_api_request(self, method: str, endpoint: str, data: Optional[Dict] = None, 
                         params: Optional[Dict] = None, retry: bool = True) -> Dict:
        """Make API request with automatic token refresh on 401."""
        if not self.is_available:
            raise Exception("Zoho API is not available")
            
        url = f"{self.base_url}/{endpoint}"
        
        if params is None:
            params = {}
        params['organization_id'] = self.organization_id
        
        try:
            logger.debug(f"Making {method} request to: {url}")
            
            response = requests.request(
                method=method,
                url=url,
                json=data,
                params=params,
                headers=self._get_headers(),
                timeout=30
            )
            
            # Handle token expiration
            if response.status_code == 401 and retry:
                logger.info("🔑 Token expired, refreshing...")
                self._refresh_access_token()
                return self._make_api_request(method, endpoint, data, params, retry=False)
                
            response.raise_for_status()
            return response.json()
            
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, OSError) as e:
            logger.warning(f"Network issue with Zoho API: {e}")
            self.is_available = False
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error for {method} {endpoint}: {e}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response content: {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Zoho API error for {method} {endpoint}: {e}")
            raise

    def _load_cache(self):
        """Load essential data into cache."""
        try:
            # Load tax configuration
            response = self._make_api_request('GET', 'settings/taxes')
            taxes = response.get('taxes', [])
            
            with self._cache_lock:
                self._cache['taxes'] = {
                    tax['tax_id']: tax for tax in taxes
                }
            
            logger.info(f"💰 Loaded {len(taxes)} tax configurations from Zoho")
            
        except Exception as e:
            logger.warning(f"⚠️ Could not load tax configuration: {e}")

    def test_connection(self) -> bool:
        """Test Zoho API connection and return status."""
        try:
            response = self._make_api_request('GET', 'settings/taxes')
            logger.info("✅ Zoho connection test successful")
            return True
        except Exception as e:
            logger.error(f"❌ Zoho connection test failed: {e}")
            return False

    # ===========================================
    # NEW: PROPER WORKFLOW METHODS
    # ===========================================

    def process_complete_data(self, clean_data: Dict, transaction_type: str) -> Dict:
        """
        Process clean data from Airtable through proper Zoho workflows.
        
        Args:
            clean_data: Clean, validated data from Airtable
            transaction_type: 'purchase' or 'sale'
            
        Returns:
            Dict with processing results
        """
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
        
        if not self.is_available:
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
                # Legacy fallback - direct adjustments (DEPRECATED)
                logger.warning("⚠️ Using legacy direct adjustment workflow - DEPRECATED")
                if not self.allow_direct_adjustments:
                    result['errors'].append("Direct adjustments are disabled. Enable ZOHO_USE_PROPER_WORKFLOWS.")
                    return result
                    
                # Call legacy methods (keeping for migration period)
                if transaction_type == 'purchase':
                    return self._legacy_process_purchase_from_airtable(clean_data)
                elif transaction_type == 'sale':
                    return self._legacy_process_sale_from_airtable(clean_data)
                
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
            vendor_id = self._find_or_create_vendor(vendor_name, airtable_data)
            result['workflow_steps'].append(f"Vendor resolved: {vendor_name} (ID: {vendor_id})")
            
            # Step 2: Ensure all items exist in Zoho
            logger.info("📦 Step 2: Ensuring items exist...")
            processed_items = []
            for item in airtable_data.get('items', []):
                item_id = self._ensure_item_exists_in_zoho(item.get('sku'), item.get('name'))
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
            po_response = self._make_api_request('POST', 'purchaseorders', po_data)
            
            po_id = po_response.get('purchaseorder', {}).get('purchaseorder_id')
            po_number = po_response.get('purchaseorder', {}).get('purchaseorder_number')
            result['purchase_order_id'] = po_id
            result['workflow_steps'].append(f"Purchase Order created: {po_number}")
            
            logger.info(f"✅ Purchase Order created: {po_number} (ID: {po_id})")
            
            # Step 4: Mark PO as received (updates inventory)
            if self.auto_receive_po:
                logger.info("📥 Step 4: Marking Purchase Order as received...")
                receive_data = self._build_receive_data(po_id, processed_items)
                receive_response = self._make_api_request('POST', f'purchaseorders/{po_id}/receive', receive_data)
                result['workflow_steps'].append("Purchase Order marked as received (inventory updated)")
                
                logger.info("✅ Purchase Order marked as received - inventory updated")
            
            # Step 5: Create Bill (for accounting consistency)
            if self.auto_create_bills:
                logger.info("🧾 Step 5: Creating Bill from Purchase Order...")
                bill_response = self._make_api_request('POST', f'purchaseorders/{po_id}/convertto/bill')
                
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
        channel = airtable_data.get('channel', 'Unknown Channel')
        customer_email = airtable_data.get('customer_email', '')
        
        logger.info(f"🔄 Processing sale with proper workflow: {order_number}")
        logger.info(f"   - Channel: {channel}")
        logger.info(f"   - Items: {len(airtable_data.get('items', []))}")
        
        try:
            # Step 1: Find or create customer/channel
            logger.info("👤 Step 1: Finding/creating customer...")
            customer_id = self._find_or_create_customer(channel, customer_email)
            result['workflow_steps'].append(f"Customer resolved: {channel} (ID: {customer_id})")
            
            # Step 2: Validate items and stock availability
            logger.info("📦 Step 2: Validating items and stock...")
            processed_items = []
            total_revenue = 0
            
            for item in airtable_data.get('items', []):
                item_id = self._ensure_item_exists_in_zoho(item.get('sku'), item.get('name'))
                
                # Check stock availability
                item_details = self._get_item_details(item_id)
                available_stock = item_details.get('stock_on_hand', 0)
                requested_qty = item.get('quantity', 0)
                
                if available_stock < requested_qty:
                    logger.warning(f"⚠️ Insufficient stock for {item.get('name')}: {available_stock} < {requested_qty}")
                
                sale_price = item.get('sale_price', 0)
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
            so_response = self._make_api_request('POST', 'salesorders', so_data)
            
            so_id = so_response.get('salesorder', {}).get('salesorder_id')
            so_number = so_response.get('salesorder', {}).get('salesorder_number')
            result['sales_order_id'] = so_id
            result['workflow_steps'].append(f"Sales Order created: {so_number}")
            
            logger.info(f"✅ Sales Order created: {so_number} (ID: {so_id})")
            
            # Step 4: Create Invoice (commits the sale)
            if self.auto_create_invoices:
                logger.info("🧾 Step 4: Creating Invoice from Sales Order...")
                invoice_response = self._make_api_request('POST', f'salesorders/{so_id}/convertto/invoice')
                
                invoice_id = invoice_response.get('invoice', {}).get('invoice_id')
                invoice_number = invoice_response.get('invoice', {}).get('invoice_number')
                result['invoice_id'] = invoice_id
                result['workflow_steps'].append(f"Invoice created: {invoice_number}")
                
                logger.info(f"✅ Invoice created: {invoice_number} (ID: {invoice_id})")
            
            # Step 5: Create Shipment (reduces inventory)
            if self.auto_create_shipments:
                logger.info("📦 Step 5: Creating Shipment...")
                shipment_data = self._build_shipment_data(so_id, processed_items)
                shipment_response = self._make_api_request('POST', f'salesorders/{so_id}/shipments', shipment_data)
                
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
    # VENDOR MANAGEMENT
    # ===========================================

    def _find_or_create_vendor(self, vendor_name: str, vendor_data: Dict) -> str:
        """Find existing vendor or create new one."""
        standardized_name = self._standardize_vendor_name(vendor_name)
        
        # Check cache first
        with self._cache_lock:
            if standardized_name in self._cache['vendors']:
                return self._cache['vendors'][standardized_name]
        
        try:
            # Search for existing vendor
            search_response = self._make_api_request('GET', 'contacts', {
                'contact_type': 'vendor',
                'search_text': standardized_name
            })
            
            vendors = search_response.get('contacts', [])
            for vendor in vendors:
                if vendor.get('contact_name', '').lower() == standardized_name.lower():
                    vendor_id = vendor['contact_id']
                    
                    with self._cache_lock:
                        self._cache['vendors'][standardized_name] = vendor_id
                    
                    logger.info(f"✅ Found existing vendor: {standardized_name} (ID: {vendor_id})")
                    return vendor_id
            
            # Create new vendor
            vendor_create_data = {
                'contact_name': standardized_name,
                'contact_type': 'vendor',
                'company_name': standardized_name
            }
            
            create_response = self._make_api_request('POST', 'contacts', vendor_create_data)
            vendor_id = create_response.get('contact', {}).get('contact_id')
            
            with self._cache_lock:
                self._cache['vendors'][standardized_name] = vendor_id
            
            logger.info(f"✅ Created new vendor: {standardized_name} (ID: {vendor_id})")
            return vendor_id
            
        except Exception as e:
            logger.error(f"❌ Failed to find/create vendor {vendor_name}: {e}")
            raise

    def _standardize_vendor_name(self, vendor_name: str) -> str:
        """Clean and standardize vendor names."""
        if not vendor_name:
            return "Unknown Vendor"
        
        # Basic cleanup
        standardized = vendor_name.strip()
        
        # Common standardizations
        standardizations = {
            'ebay': 'eBay',
            'amazon': 'Amazon',
            'tcgplayer': 'TCGPlayer',
            'shopify': 'Shopify'
        }
        
        for original, standard in standardizations.items():
            if original in standardized.lower():
                standardized = standard
                break
        
        return standardized

    # ===========================================
    # CUSTOMER MANAGEMENT
    # ===========================================

    def _find_or_create_customer(self, channel_name: str, customer_email: str = None) -> str:
        """Find existing customer or create new one for sales channel."""
        standardized_name = self._standardize_channel_name(channel_name)
        
        # Check cache first
        with self._cache_lock:
            if standardized_name in self._cache['customers']:
                return self._cache['customers'][standardized_name]
        
        try:
            # Search for existing customer
            search_response = self._make_api_request('GET', 'contacts', {
                'contact_type': 'customer',
                'search_text': standardized_name
            })
            
            customers = search_response.get('contacts', [])
            for customer in customers:
                if customer.get('contact_name', '').lower() == standardized_name.lower():
                    customer_id = customer['contact_id']
                    
                    with self._cache_lock:
                        self._cache['customers'][standardized_name] = customer_id
                    
                    logger.info(f"✅ Found existing customer: {standardized_name} (ID: {customer_id})")
                    return customer_id
            
            # Create new customer
            customer_create_data = {
                'contact_name': standardized_name,
                'contact_type': 'customer',
                'company_name': standardized_name
            }
            
            if customer_email:
                customer_create_data['email'] = customer_email
            
            create_response = self._make_api_request('POST', 'contacts', customer_create_data)
            customer_id = create_response.get('contact', {}).get('contact_id')
            
            with self._cache_lock:
                self._cache['customers'][standardized_name] = customer_id
            
            logger.info(f"✅ Created new customer: {standardized_name} (ID: {customer_id})")
            return customer_id
            
        except Exception as e:
            logger.error(f"❌ Failed to find/create customer {channel_name}: {e}")
            raise

    def _standardize_channel_name(self, channel_name: str) -> str:
        """Clean and standardize sales channel names."""
        if not channel_name:
            return "Direct Sales"
        
        # Basic cleanup
        standardized = channel_name.strip()
        
        # Channel standardizations
        standardizations = {
            'ebay': 'eBay Sales',
            'amazon': 'Amazon Sales',
            'tcgplayer': 'TCGPlayer Sales',
            'shopify': 'Shopify Sales',
            'etsy': 'Etsy Sales',
            'facebook': 'Facebook Marketplace',
            'mercari': 'Mercari Sales'
        }
        
        for original, standard in standardizations.items():
            if original in standardized.lower():
                standardized = standard
                break
        
        return standardized

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

    # ===========================================
    # UTILITY METHODS
    # ===========================================

    def _ensure_item_exists_in_zoho(self, sku: str, item_name: str) -> str:
        """Ensure item exists in Zoho, create if missing."""
        if not sku:
            raise ValueError("SKU is required for item creation")
        
        # Check cache first
        with self._cache_lock:
            if sku in self._cache['items']:
                return self._cache['items'][sku]
        
        try:
            # Search for existing item by SKU
            search_response = self._make_api_request('GET', 'items', {'sku': sku})
            items = search_response.get('items', [])
            
            if items:
                item_id = items[0]['item_id']
                
                with self._cache_lock:
                    self._cache['items'][sku] = item_id
                
                logger.debug(f"✅ Found existing item: {sku} (ID: {item_id})")
                return item_id
            
            # Create new item
            logger.info(f"📦 Creating new item: {sku} - {item_name}")
            item_data = self._build_item_creation_data(sku, item_name)
            
            create_response = self._make_api_request('POST', 'items', item_data)
            item_id = create_response.get('item', {}).get('item_id')
            
            with self._cache_lock:
                self._cache['items'][sku] = item_id
            
            logger.info(f"✅ Created new item: {sku} (ID: {item_id})")
            return item_id
            
        except Exception as e:
            logger.error(f"❌ Failed to ensure item exists {sku}: {e}")
            raise

    def _build_item_creation_data(self, sku: str, item_name: str) -> Dict:
        """Build item creation data with proper account setup."""
        item_data = {
            'name': item_name or f"Item {sku}",
            'sku': sku,
            'is_inventory_tracked': True,
            'opening_stock': 0,
            'opening_stock_rate': 0,
            'item_type': 'inventory'
        }
        
        # Add account mappings if configured
        if self.default_inventory_account:
            item_data['inventory_account_id'] = self.default_inventory_account
        
        if self.default_cogs_account:
            item_data['account_id'] = self.default_cogs_account
        
        if self.default_sales_account:
            item_data['income_account_id'] = self.default_sales_account
        
        return item_data

    def _get_item_details(self, item_id: str) -> Dict:
        """Get detailed item information including current stock and rate."""
        try:
            response = self._make_api_request('GET', f'items/{item_id}')
            return response.get('item', {})
        except Exception as e:
            logger.warning(f"⚠️ Could not get item details for {item_id}: {e}")
            return {}

    def _calculate_cogs_from_shipment(self, items: List[Dict]) -> float:
        """Calculate COGS from shipped items using Zoho's FIFO method."""
        total_cogs = 0
        
        for item in items:
            try:
                # Get current item rate (Zoho maintains FIFO automatically)
                item_details = self._get_item_details(item['item_id'])
                current_rate = item_details.get('rate', 0)
                quantity = item['quantity']
                
                item_cogs = quantity * current_rate
                total_cogs += item_cogs
                
                logger.debug(f"COGS calculation - {item['sku']}: {quantity} × ${current_rate} = ${item_cogs}")
                
            except Exception as e:
                logger.warning(f"⚠️ Could not calculate COGS for {item.get('sku')}: {e}")
        
        return total_cogs

    # ===========================================
    # CLEANUP AND ROLLBACK METHODS
    # ===========================================

    def _cleanup_failed_purchase(self, po_id: str, bill_id: str = None):
        """Clean up failed purchase transaction."""
        try:
            if bill_id:
                logger.info(f"🧹 Cleaning up failed bill: {bill_id}")
                self._make_api_request('DELETE', f'bills/{bill_id}')
            
            logger.info(f"🧹 Cleaning up failed purchase order: {po_id}")
            self._make_api_request('DELETE', f'purchaseorders/{po_id}')
            
        except Exception as e:
            logger.error(f"⚠️ Failed to cleanup purchase transaction: {e}")

    def _cleanup_failed_sale(self, so_id: str, invoice_id: str = None, shipment_id: str = None):
        """Clean up failed sales transaction."""
        try:
            if shipment_id:
                logger.info(f"🧹 Cleaning up failed shipment: {shipment_id}")
                self._make_api_request('DELETE', f'shipments/{shipment_id}')
            
            if invoice_id:
                logger.info(f"🧹 Cleaning up failed invoice: {invoice_id}")
                self._make_api_request('DELETE', f'invoices/{invoice_id}')
            
            logger.info(f"🧹 Cleaning up failed sales order: {so_id}")
            self._make_api_request('DELETE', f'salesorders/{so_id}')
            
        except Exception as e:
            logger.error(f"⚠️ Failed to cleanup sales transaction: {e}")

    # ===========================================
    # VALIDATION METHODS
    # ===========================================

    def validate_inventory_adjustments_empty(self) -> Dict:
        """Check that inventory adjustments tab is empty (except manual adjustments)."""
        try:
            response = self._make_api_request('GET', 'inventoryadjustments')
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

    # ===========================================
    # LEGACY METHODS (DEPRECATED - For Migration Period Only)
    # ===========================================

    def _legacy_process_purchase_from_airtable(self, airtable_data: Dict) -> Dict:
        """DEPRECATED: Legacy purchase processing using direct adjustments."""
        logger.warning("⚠️ Using DEPRECATED legacy purchase processing - use proper workflows instead")
        
        result = {
            'success': False,
            'items_processed': [],
            'stock_adjusted': False,
            'adjustment_id': None,
            'errors': []
        }
        
        logger.info("🔄 Processing clean purchase data from Airtable (LEGACY)")
        logger.info(f"   - Order: {airtable_data.get('order_number', 'N/A')}")
        
        try:
            items_for_adjustment = []
            
            logger.info(f"📋 Processing {len(airtable_data.get('items', []))} items with assigned SKUs...")
            
            for i, item in enumerate(airtable_data.get('items', []), 1):
                name = item.get('name')
                sku = item.get('sku')
                quantity = item.get('quantity', 0)
                unit_price = item.get('unit_price', 0)
                
                logger.info(f"   [{i}/{len(airtable_data['items'])}] Processing: {name}")
                logger.info(f"      - SKU: {sku}")
                
                try:
                    # Ensure item exists in Zoho
                    item_id = self._ensure_item_exists_in_zoho(sku, name)
                    
                    # Get current item details for WAC calculation
                    item_details = self._get_item_details(item_id)
                    
                    items_for_adjustment.append({
                        'item_id': item_id,
                        'name': name,
                        'sku': sku,
                        'quantity_adjusted': quantity,
                        'rate': unit_price,
                        'current_stock': item_details.get('stock_on_hand', 0),
                        'current_rate': item_details.get('rate', 0)
                    })
                    
                    result['items_processed'].append({
                        'item_id': item_id,
                        'sku': sku,
                        'name': name,
                        'quantity': quantity,
                        'unit_price': unit_price
                    })
                    
                    logger.info(f"      ✅ Item processed successfully")
                    
                except Exception as e:
                    result['errors'].append(f"Failed to process item {name}: {e}")
                    logger.error(f"      ❌ Failed to process item {name}: {e}")
                    
            if items_for_adjustment:
                # Create inventory adjustment for stock increase
                try:
                    adjustment_data = {
                        'date': datetime.now().strftime('%Y-%m-%d'),
                        'reason': 'Purchase - Stock Received',
                        'adjustment_type': 'quantity',
                        'line_items': []
                    }
                    
                    # Generate reference number
                    order_ref = airtable_data.get('order_number', 'UNKNOWN')
                    adjustment_data['reference_number'] = f"PURCHASE-{order_ref}-{datetime.now().strftime('%Y%m%d')}"
                    
                    for item in items_for_adjustment:
                        adjustment_data['line_items'].append({
                            'item_id': item['item_id'],
                            'quantity_adjusted': item['quantity_adjusted'],
                            'rate': item['rate']
                        })
                    
                    logger.info(f"📈 Creating stock adjustment:")
                    logger.info(f"   - Type: Purchase (Stock Increase)")
                    logger.info(f"   - Date: {adjustment_data['date']}")
                    logger.info(f"   - Reference: {adjustment_data['reference_number']}")
                    
                    # Create the adjustment
                    adjustment_response = self._make_api_request(
                        'POST',
                        'inventoryadjustments',
                        adjustment_data
                    )
                    
                    result['adjustment_id'] = adjustment_response.get('inventory_adjustment', {}).get('inventory_adjustment_id')
                    result['stock_adjusted'] = True
                    result['success'] = True
                    
                    logger.info(f"✅ Stock adjustment created successfully:")
                    logger.info(f"   - Adjustment ID: {result['adjustment_id']}")
                    
                except Exception as e:
                    result['errors'].append(f"Failed to create stock adjustment: {e}")
                    logger.error(f"❌ Stock adjustment failed: {e}")
                    
            else:
                result['errors'].append("No items could be processed")
                logger.error("❌ No items could be processed for stock adjustment")
                
        except Exception as e:
            result['errors'].append(f"Purchase processing error: {e}")
            logger.error(f"💥 Failed to process purchase: {e}", exc_info=True)
            
        return result

    def _legacy_process_sale_from_airtable(self, airtable_data: Dict) -> Dict:
        """DEPRECATED: Legacy sale processing using direct adjustments."""
        logger.warning("⚠️ Using DEPRECATED legacy sale processing - use proper workflows instead")
        
        result = {
            'success': False,
            'items_processed': [],
            'stock_adjusted': False,
            'adjustment_id': None,
            'revenue': 0,
            'cogs': 0,
            'errors': []
        }
        
        logger.info("🔄 Processing clean sale data from Airtable (LEGACY)")
        logger.info(f"   - Order: {airtable_data.get('order_number', 'N/A')}")
        
        try:
            items_for_adjustment = []
            total_revenue = 0
            total_cogs = 0
            
            logger.info(f"📋 Processing {len(airtable_data.get('items', []))} items with assigned SKUs...")
            
            for i, item in enumerate(airtable_data.get('items', []), 1):
                name = item.get('name')
                sku = item.get('sku')
                quantity = item.get('quantity', 0)
                sale_price = item.get('sale_price', 0)
                
                logger.info(f"   [{i}/{len(airtable_data['items'])}] Processing: {name}")
                logger.info(f"      - SKU: {sku}")
                
                try:
                    # Ensure item exists in Zoho
                    item_id = self._ensure_item_exists_in_zoho(sku, name)
                    
                    # Get current item details for WAC calculation
                    item_details = self._get_item_details(item_id)
                    current_rate = item_details.get('rate', 0)
                    
                    # Calculate revenue and COGS
                    item_revenue = quantity * sale_price
                    item_cogs = quantity * current_rate
                    
                    total_revenue += item_revenue
                    total_cogs += item_cogs
                    
                    items_for_adjustment.append({
                        'item_id': item_id,
                        'name': name,
                        'sku': sku,
                        'quantity_adjusted': -quantity,  # Negative for sale
                        'rate': current_rate,  # Use WAC for COGS
                        'sale_price': sale_price,
                        'revenue': item_revenue,
                        'cogs': item_cogs
                    })
                    
                    result['items_processed'].append({
                        'item_id': item_id,
                        'sku': sku,
                        'name': name,
                        'quantity': quantity,
                        'sale_price': sale_price,
                        'cogs': item_cogs,
                        'revenue': item_revenue
                    })
                    
                    logger.info(f"      ✅ Item processed successfully")
                    
                except Exception as e:
                    result['errors'].append(f"Failed to process item {name}: {e}")
                    logger.error(f"      ❌ Failed to process item {name}: {e}")
                    
            if items_for_adjustment:
                # Create inventory adjustment for stock decrease
                try:
                    adjustment_data = {
                        'date': datetime.now().strftime('%Y-%m-%d'),
                        'reason': 'Sale - Stock Reduced',
                        'adjustment_type': 'quantity',
                        'line_items': []
                    }
                    
                    # Generate reference number
                    order_ref = airtable_data.get('order_number', 'UNKNOWN')
                    adjustment_data['reference_number'] = f"SALE-{order_ref}-{datetime.now().strftime('%Y%m%d')}"
                    
                    for item in items_for_adjustment:
                        adjustment_data['line_items'].append({
                            'item_id': item['item_id'],
                            'quantity_adjusted': item['quantity_adjusted'],  # Negative
                            'rate': item['rate']
                        })
                    
                    logger.info(f"📉 Creating stock adjustment:")
                    logger.info(f"   - Type: Sale (Stock Decrease)")
                    logger.info(f"   - Date: {adjustment_data['date']}")
                    logger.info(f"   - Reference: {adjustment_data['reference_number']}")
                    
                    # Create the adjustment
                    adjustment_response = self._make_api_request(
                        'POST',
                        'inventoryadjustments',
                        adjustment_data
                    )
                    
                    result['adjustment_id'] = adjustment_response.get('inventory_adjustment', {}).get('inventory_adjustment_id')
                    result['stock_adjusted'] = True
                    result['success'] = True
                    result['revenue'] = total_revenue
                    result['cogs'] = total_cogs
                    
                    logger.info(f"✅ Stock adjustment created successfully:")
                    logger.info(f"   - Adjustment ID: {result['adjustment_id']}")
                    logger.info(f"   - Revenue: ${total_revenue:.2f}")
                    logger.info(f"   - COGS: ${total_cogs:.2f}")
                    
                except Exception as e:
                    result['errors'].append(f"Failed to create stock adjustment: {e}")
                    logger.error(f"❌ Stock adjustment failed: {e}")
                    
            else:
                result['errors'].append("No items could be processed")
                logger.error("❌ No items could be processed for stock adjustment")
                
        except Exception as e:
            result['errors'].append(f"Sale processing error: {e}")
            logger.error(f"💥 Failed to process sale: {e}", exc_info=True)
            
        return result