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
        
        # FIX: Use correct Zoho API endpoint from official documentation
        # OLD: self.base_url = "https://inventory.zohoapis.com/api/v1"
        self.base_url = "https://www.zohoapis.com/inventory/v1"
        
        self.api_region = config.get('ZOHO_API_REGION', 'com')
        self.is_available = False  # Track if Zoho is available
        
        # FIX: Adjust base URL for different regions correctly according to documentation
        if self.api_region != 'com':
            if self.api_region == 'eu':
                self.base_url = "https://www.zohoapis.eu/inventory/v1"
            elif self.api_region == 'in':
                self.base_url = "https://www.zohoapis.in/inventory/v1"
            elif self.api_region == 'au':
                self.base_url = "https://www.zohoapis.com.au/inventory/v1"
            elif self.api_region == 'jp':
                self.base_url = "https://www.zohoapis.jp/inventory/v1"
            elif self.api_region == 'ca':
                self.base_url = "https://www.zohoapis.ca/inventory/v1"
            elif self.api_region == 'cn':
                self.base_url = "https://www.zohoapis.com.cn/inventory/v1"
            elif self.api_region == 'sa':
                self.base_url = "https://www.zohoapis.sa/inventory/v1"
            else:
                # Default to .com if unknown region
                logger.warning(f"Unknown API region '{self.api_region}', defaulting to 'com'")
                self.base_url = "https://www.zohoapis.com/inventory/v1"
            
        # Simplified cache - mainly for items by SKU
        self._cache = {
            'items': {},  # SKU -> item_id mapping
            'taxes': {}
        }
        self._cache_lock = Lock()
        
        # Configuration flags
        self.use_physical_stock = config.get_bool('ZOHO_USE_PHYSICAL_STOCK', True)
        
        logger.info(f"🔧 Initializing Zoho client (Sequential Mode)...")
        logger.info(f"   - Base URL: {self.base_url}")
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
            # FIX: Use correct OAuth endpoint based on region
            if self.api_region == 'com':
                token_url = "https://accounts.zoho.com/oauth/v2/token"
            elif self.api_region == 'eu':
                token_url = "https://accounts.zoho.eu/oauth/v2/token"
            elif self.api_region == 'in':
                token_url = "https://accounts.zoho.in/oauth/v2/token"
            elif self.api_region == 'au':
                token_url = "https://accounts.zoho.com.au/oauth/v2/token"
            elif self.api_region == 'jp':
                token_url = "https://accounts.zoho.jp/oauth/v2/token"
            elif self.api_region == 'ca':
                token_url = "https://accounts.zoho.ca/oauth/v2/token"
            elif self.api_region == 'cn':
                token_url = "https://accounts.zoho.com.cn/oauth/v2/token"
            elif self.api_region == 'sa':
                token_url = "https://accounts.zoho.sa/oauth/v2/token"
            else:
                token_url = "https://accounts.zoho.com/oauth/v2/token"
            
            data = {
                "refresh_token": self.config.get('ZOHO_REFRESH_TOKEN'),
                "client_id": self.config.get('ZOHO_CLIENT_ID'),
                "client_secret": self.config.get('ZOHO_CLIENT_SECRET'),
                "grant_type": "refresh_token"
            }
            
            logger.debug(f"Requesting token from: {token_url}")
            response = requests.post(token_url, data=data, timeout=10)
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
            # FIX: Add better error handling and logging
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
        
    def _load_tax_configuration(self):
        """Load tax configuration from Zoho."""
        try:
            response = self._make_api_request('GET', 'settings/taxes')
            taxes = response.get('taxes', [])
            
            with self._cache_lock:
                self._cache['taxes'] = {
                    tax['tax_id']: tax for tax in taxes
                }
            
            logger.info(f"💰 Loaded {len(taxes)} tax configurations from Zoho")
            
        except Exception as e:
            logger.warning(f"⚠️ Could not load tax configuration: {e}")
            # Don't fail initialization for this
    
    def test_connection(self) -> bool:
        """Test Zoho API connection and return status."""
        try:
            # Try to fetch taxes as a simple connection test
            response = self._make_api_request('GET', 'settings/taxes')
            logger.info("✅ Zoho connection test successful")
            return True
        except Exception as e:
            logger.error(f"❌ Zoho connection test failed: {e}")
            return False

    def process_complete_data(self, clean_data: Dict, transaction_type: str) -> Dict:
        """
        Process clean data from Airtable through Zoho.
        
        Args:
            clean_data: Clean, validated data from Airtable
            transaction_type: 'purchase' or 'sale'
            
        Returns:
            Dict with processing results
        """
        result = {
            'success': False,
            'items_processed': [],
            'stock_adjusted': False,
            'adjustment_id': None,
            'revenue': 0,
            'cogs': 0,
            'errors': []
        }
        
        if not self.is_available:
            result['errors'].append("Zoho API is not available")
            return result
            
        try:
            if transaction_type == 'purchase':
                return self._process_purchase_from_airtable(clean_data)
            elif transaction_type == 'sale':
                return self._process_sale_from_airtable(clean_data)
            else:
                result['errors'].append(f"Unknown transaction type: {transaction_type}")
                
        except Exception as e:
            result['errors'].append(f"Processing error: {e}")
            logger.error(f"💥 Failed to process {transaction_type}: {e}", exc_info=True)
            
        return result

    def _process_purchase_from_airtable(self, airtable_data: Dict) -> Dict:
        """Process purchase from clean Airtable data."""
        result = {
            'success': False,
            'items_processed': [],
            'stock_adjusted': False,
            'adjustment_id': None,
            'errors': []
        }
        
        logger.info("🔄 Processing clean purchase data from Airtable")
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

    def _process_sale_from_airtable(self, airtable_data: Dict) -> Dict:
        """Process sale from clean Airtable data."""
        result = {
            'success': False,
            'items_processed': [],
            'stock_adjusted': False,
            'adjustment_id': None,
            'revenue': 0,
            'cogs': 0,
            'errors': []
        }
        
        logger.info("🔄 Processing clean sale data from Airtable")
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
                        'reason': 'Sale - Stock Sold',
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
                            'rate': item['rate']  # WAC rate for COGS
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