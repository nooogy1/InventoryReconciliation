"""Zoho Inventory client updated for sequential Airtable → Zoho workflow."""

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
    """Handle Zoho Inventory API with simplified sequential workflow."""
    
    def __init__(self, config):
        self.config = config
        self.organization_id = config.get('ZOHO_ORGANIZATION_ID')
        self.access_token = None
        self.base_url = "https://inventory.zohoapis.com/api/v1"
        self.api_region = config.get('ZOHO_API_REGION', 'com')
        self.is_available = False  # Track if Zoho is available
        
        # Adjust base URL for region
        if self.api_region != 'com':
            self.base_url = f"https://inventory.zohoapis.{self.api_region}/api/v1"
            
        # Simplified cache - mainly for items by SKU
        self._cache = {
            'items': {},  # SKU -> item_id mapping
            'taxes': {}
        }
        self._cache_lock = Lock()
        
        # Configuration flags
        self.use_physical_stock = config.get_bool('ZOHO_USE_PHYSICAL_STOCK', True)
        
        logger.info(f"🔧 Initializing Zoho client (Sequential Mode)...")
        logger.info(f"   - Organization ID: {self.organization_id}")
        logger.info(f"   - API Region: {self.api_region}")
        logger.info(f"   - Mode: Simplified for clean Airtable data")
        
        # Try to initialize Zoho connection - don't fail if it's not available
        try:
            self._refresh_access_token()
            self._load_tax_configuration()
            self.is_available = True
            logger.info("✅ Zoho client initialized successfully")
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, OSError) as e:
            logger.warning(f"⚠️ Zoho is not available (network/DNS issue): {e}")
            logger.warning("⚠️ System will continue without Zoho integration")
            self.is_available = False
        except Exception as e:
            logger.error(f"❌ Zoho initialization failed: {e}")
            logger.warning("⚠️ System will continue without Zoho integration")
            self.is_available = False
        
    def _refresh_access_token(self):
        """Refresh Zoho access token using refresh token."""
        if not self.is_available:
            logger.debug("Zoho not available, skipping token refresh")
            return
            
        try:
            url = f"https://accounts.zohoapis.{self.api_region}/oauth/v2/token"
            data = {
                "refresh_token": self.config.get('ZOHO_REFRESH_TOKEN'),
                "client_id": self.config.get('ZOHO_CLIENT_ID'),
                "client_secret": self.config.get('ZOHO_CLIENT_SECRET'),
                "grant_type": "refresh_token"
            }
            
            response = requests.post(url, data=data, timeout=10)
            response.raise_for_status()
            
            token_data = response.json()
            self.access_token = token_data['access_token']
            logger.info("✅ Zoho access token refreshed")
            
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, OSError) as e:
            logger.warning(f"Network issue refreshing Zoho token: {e}")
            self.is_available = False
            raise
        except Exception as e:
            logger.error(f"Failed to refresh Zoho token: {str(e)}")
            self.is_available = False
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
        if not self.is_available:
            raise Exception("Zoho API is not available")
            
        url = f"{self.base_url}/{endpoint}"
        
        if params is None:
            params = {}
        params['organization_id'] = self.organization_id
        
        try:
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
        except Exception as e:
            logger.error(f"Zoho API error for {method} {endpoint}: {e}")
            raise
        
    def _load_tax_configuration(self):
        """Load tax configuration from Zoho."""
        if not self.is_available:
            logger.debug("Zoho not available, skipping tax configuration")
            return
            
        try:
            response = self._make_api_request('GET', 'settings/taxes')
            taxes = response.get('taxes', [])
            
            with self._cache_lock:
                self._cache['taxes'] = {tax['tax_id']: tax for tax in taxes}
                
            logger.info(f"✅ Loaded {len(taxes)} tax configurations")
            
        except requests.exceptions.HTTPError as e:
            if e.response and e.response.status_code == 400:
                logger.warning("⚠️ Could not load tax configuration (400 error)")
                logger.warning("   Tax operations will use default settings")
                with self._cache_lock:
                    self._cache['taxes'] = {}
            else:
                logger.warning(f"Could not load tax configuration: {e}")
                with self._cache_lock:
                    self._cache['taxes'] = {}
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, OSError) as e:
            logger.warning(f"Network issue loading tax configuration: {e}")
            self.is_available = False
            with self._cache_lock:
                self._cache['taxes'] = {}
        except Exception as e:
            logger.warning(f"Could not load tax configuration: {e}")
            with self._cache_lock:
                self._cache['taxes'] = {}
                
    def process_complete_data(self, data: Dict, transaction_type: str) -> Dict:
        """
        Process clean data from Airtable.
        
        This method now expects clean, validated data with SKUs already assigned.
        
        Args:
            data: Clean data from Airtable with guaranteed SKUs
            transaction_type: 'purchase' or 'sale'
            
        Returns:
            Dictionary with processing results
        """
        order_number = data.get('order_number', 'N/A')
        logger.info(f"🔄 Processing clean {transaction_type} data from Airtable")
        logger.info(f"   - Order: {order_number}")
        
        result = {
            'success': False,
            'stock_adjusted': False,
            'items_processed': [],
            'items_failed': [],
            'errors': [],
            'warnings': []
        }
        
        # Check if Zoho is available
        if not self.is_available:
            result['errors'].append("Zoho API is not available - network/DNS issue")
            result['warnings'].append("Data has been saved to Airtable but NOT synced to Zoho")
            logger.warning("⚠️ Zoho not available - skipping inventory sync")
            return result
        
        try:
            if transaction_type == 'purchase':
                result = self._process_purchase_from_airtable(data)
            elif transaction_type == 'sale':
                result = self._process_sale_from_airtable(data)
            else:
                result['errors'].append(f"Unknown transaction type: {transaction_type}")
                
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, OSError) as e:
            result['errors'].append(f"Network error processing {transaction_type}: {e}")
            result['warnings'].append("Zoho became unavailable during processing")
            logger.warning(f"⚠️ Network issue during Zoho processing: {e}")
            self.is_available = False
        except Exception as e:
            result['errors'].append(str(e))
            logger.error(f"Error processing clean data: {e}")
            
        return result
        
    def _process_purchase_from_airtable(self, data: Dict) -> Dict:
        """
        Process purchase using clean Airtable data.
        
        Args:
            data: Clean purchase data with SKUs assigned by Airtable
            
        Returns:
            Processing result dictionary
        """
        order_number = data.get('order_number', 'N/A')
        
        logger.info(f"📦 Processing purchase from Airtable data:")
        logger.info(f"   - Order: {order_number}")
        
        result = {
            'success': False,
            'stock_adjusted': False,
            'items_processed': [],
            'items_failed': [],
            'errors': [],
            'warnings': [],
            'adjustment_id': None
        }
        
        try:
            # Process each item with guaranteed SKUs
            adjustment_items = []
            total_cost = 0
            
            items = data.get('items', [])
            logger.info(f"📋 Processing {len(items)} items with assigned SKUs...")
            
            for i, item in enumerate(items, 1):
                item_name = item.get('name', f'Item {i}')
                item_sku = item.get('sku')  # Guaranteed to exist from Airtable
                
                logger.info(f"   [{i}/{len(items)}] Processing: {item_name}")
                logger.info(f"      - SKU: {item_sku}")
                
                if not item_sku:
                    error_msg = f"No SKU provided for item {item_name}"
                    result['items_failed'].append({
                        'name': item_name,
                        'error': error_msg
                    })
                    result['warnings'].append(error_msg)
                    logger.error(f"      ❌ {error_msg}")
                    continue
                
                try:
                    # Find or create item in Zoho
                    item_id = self._ensure_item_exists_in_zoho(item_sku, item_name)
                    
                    logger.info(f"      - Zoho Item ID: {item_id}")
                    
                    # Calculate item cost
                    quantity = item.get('quantity', 0)
                    unit_price = item.get('unit_price', 0)
                    
                    logger.info(f"      - Quantity: {quantity}")
                    logger.info(f"      - Unit price: ${unit_price:.2f}")
                    
                    # Add to adjustment
                    adjustment_items.append({
                        'item_id': item_id,
                        'quantity_adjusted': quantity,
                        'new_rate': unit_price,
                        'notes': f"Purchase from Airtable: {order_number}"
                    })
                    
                    total_cost += quantity * unit_price
                    
                    result['items_processed'].append({
                        'name': item_name,
                        'sku': item_sku,
                        'quantity': quantity,
                        'unit_cost': unit_price
                    })
                    
                    logger.info(f"      ✅ Item processed successfully")
                    
                except Exception as e:
                    error_msg = f"Failed to process item {item_name}: {e}"
                    result['items_failed'].append({
                        'name': item_name,
                        'error': str(e)
                    })
                    result['warnings'].append(error_msg)
                    logger.error(f"      ❌ {error_msg}")
                    
            # Create stock adjustment if we have items
            if adjustment_items:
                logger.info(f"📈 Creating stock adjustment with {len(adjustment_items)} items...")
                
                try:
                    adjustment_data = {
                        'date': data.get('date', datetime.now().strftime('%Y-%m-%d')),
                        'reason': 'Stock Received',
                        'description': f"Purchase from Airtable: {order_number}",
                        'adjustment_type': 'quantity',
                        'line_items': adjustment_items,
                        'reference_number': order_number,
                        'notes': f"Processed via Airtable → Zoho workflow"
                    }
                    
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
                    logger.info(f"   - Total cost: ${total_cost:.2f}")
                    
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
        
    def _process_sale_from_airtable(self, data: Dict) -> Dict:
        """
        Process sale using clean Airtable data.
        
        Args:
            data: Clean sales data with SKUs assigned by Airtable
            
        Returns:
            Processing result dictionary
        """
        order_number = data.get('order_number', 'N/A')
        
        logger.info(f"💰 Processing sale from Airtable data:")
        logger.info(f"   - Order: {order_number}")
        
        result = {
            'success': False,
            'stock_adjusted': False,
            'items_processed': [],
            'items_failed': [],
            'errors': [],
            'warnings': [],
            'adjustment_id': None,
            'revenue': 0,
            'cogs': 0
        }
        
        try:
            # Process each item with guaranteed SKUs
            adjustment_items = []
            total_revenue = 0
            total_cogs = 0
            
            items = data.get('items', [])
            logger.info(f"📋 Processing {len(items)} items with assigned SKUs...")
            
            for i, item in enumerate(items, 1):
                item_name = item.get('name', f'Item {i}')
                item_sku = item.get('sku')  # Guaranteed to exist from Airtable
                
                logger.info(f"   [{i}/{len(items)}] Processing: {item_name}")
                logger.info(f"      - SKU: {item_sku}")
                
                if not item_sku:
                    error_msg = f"No SKU provided for item {item_name}"
                    result['items_failed'].append({
                        'name': item_name,
                        'error': error_msg
                    })
                    result['warnings'].append(error_msg)
                    logger.error(f"      ❌ {error_msg}")
                    continue
                
                try:
                    # Find item in Zoho
                    item_id = self._find_item_by_sku(item_sku)
                    
                    if not item_id:
                        # Create item if it doesn't exist
                        item_id = self._ensure_item_exists_in_zoho(item_sku, item_name)
                        
                    # Get current stock and cost
                    item_details = self._get_item_details(item_id)
                    current_stock = item_details.get('stock_on_hand', 0)
                    current_cost = item_details.get('purchase_rate', 0)
                    
                    quantity = item.get('quantity', 0)
                    sale_price = item.get('sale_price', 0)
                    
                    logger.info(f"      - Zoho Item ID: {item_id}")
                    logger.info(f"      - Current stock: {current_stock}")
                    logger.info(f"      - Current cost: ${current_cost:.2f}")
                    logger.info(f"      - Sale quantity: {quantity}")
                    logger.info(f"      - Sale price: ${sale_price:.2f}")
                    
                    # Check if we have sufficient stock
                    if current_stock < quantity:
                        warning_msg = (
                            f"Insufficient stock for {item_name}: "
                            f"have {current_stock}, need {quantity}"
                        )
                        result['warnings'].append(warning_msg)
                        logger.warning(f"      ⚠️ {warning_msg}")
                        
                    # Add to adjustment (negative for sales)
                    adjustment_items.append({
                        'item_id': item_id,
                        'quantity_adjusted': -quantity,  # Negative for reduction
                        'notes': f"Sale from Airtable: {order_number}"
                    })
                    
                    # Calculate revenue and COGS
                    item_revenue = quantity * sale_price
                    item_cogs = quantity * current_cost
                    
                    total_revenue += item_revenue
                    total_cogs += item_cogs
                    
                    result['items_processed'].append({
                        'name': item_name,
                        'sku': item_sku,
                        'quantity': quantity,
                        'sale_price': sale_price,
                        'cost': current_cost,
                        'profit': item_revenue - item_cogs
                    })
                    
                    logger.info(f"      - Revenue: ${item_revenue:.2f}")
                    logger.info(f"      - COGS: ${item_cogs:.2f}")
                    logger.info(f"      - Profit: ${item_revenue - item_cogs:.2f}")
                    logger.info(f"      ✅ Item processed successfully")
                    
                except Exception as e:
                    error_msg = f"Failed to process item {item_name}: {e}"
                    result['items_failed'].append({
                        'name': item_name,
                        'error': str(e)
                    })
                    result['warnings'].append(error_msg)
                    logger.error(f"      ❌ {error_msg}")
                    
            # Create stock adjustment if we have items
            if adjustment_items:
                logger.info(f"📉 Creating stock adjustment with {len(adjustment_items)} items...")
                
                try:
                    adjustment_data = {
                        'date': data.get('date', datetime.now().strftime('%Y-%m-%d')),
                        'reason': 'Goods Sold',
                        'description': f"Sale from Airtable: {order_number}",
                        'adjustment_type': 'quantity',
                        'line_items': adjustment_items,
                        'reference_number': order_number,
                        'notes': f"Processed via Airtable → Zoho workflow"
                    }
                    
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
                    logger.info(f"   - Total revenue: ${total_revenue:.2f}")
                    logger.info(f"   - Total COGS: ${total_cogs:.2f}")
                    logger.info(f"   - Total profit: ${total_revenue - total_cogs:.2f}")
                    
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
        
    def _ensure_item_exists_in_zoho(self, sku: str, name: str) -> str:
        """
        Ensure item exists in Zoho, create if necessary.
        
        Args:
            sku: SKU assigned by Airtable
            name: Item name
            
        Returns:
            Zoho item ID
        """
        # Check if item exists
        item_id = self._find_item_by_sku(sku)
        
        if item_id:
            logger.info(f"      ✅ Found existing item in Zoho: {item_id}")
            return item_id
            
        # Create new item in Zoho
        logger.info(f"      🆕 Creating new item in Zoho")
        
        item_data = {
            'name': name,
            'sku': sku,
            'item_type': 'inventory',
            'product_type': 'goods',
            'purchase_rate': 0,
            'selling_price': 0,
            'initial_stock': 0,
            'track_inventory': True
        }
        
        try:
            response = self._make_api_request('POST', 'items', item_data)
            item_id = response['item']['item_id']
            
            # Cache the new item
            with self._cache_lock:
                self._cache['items'][sku] = item_id
                
            logger.info(f"      ✅ Created new item in Zoho: {item_id}")
            return item_id
            
        except Exception as e:
            logger.error(f"      ❌ Failed to create item in Zoho: {e}")
            raise
            
    def _find_item_by_sku(self, sku: str) -> Optional[str]:
        """Find item by SKU in Zoho."""
        # Check cache first
        with self._cache_lock:
            if sku in self._cache['items']:
                return self._cache['items'][sku]
                
        try:
            params = {'sku': sku}
            response = self._make_api_request('GET', 'items', params=params)
            
            items = response.get('items', [])
            for item in items:
                if item.get('sku', '').upper() == sku.upper():
                    item_id = item['item_id']
                    
                    # Cache it
                    with self._cache_lock:
                        self._cache['items'][sku] = item_id
                        
                    return item_id
                    
        except Exception as e:
            logger.debug(f"Error finding item by SKU {sku}: {e}")
            
        return None
        
    def _get_item_details(self, item_id: str) -> Dict:
        """Get detailed item information from Zoho."""
        try:
            response = self._make_api_request('GET', f'items/{item_id}')
            return response.get('item', {})
            
        except Exception as e:
            logger.error(f"Failed to get item details for {item_id}: {e}")
            return {}
            
    def clear_cache(self):
        """Clear the item cache."""
        with self._cache_lock:
            self._cache = {
                'items': {},
                'taxes': self._cache.get('taxes', {})  # Keep tax config
            }
        logger.info("🧹 Cleared Zoho item cache")