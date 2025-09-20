"""Zoho Inventory client with physical stock tracking and SKU generation."""

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
    """Handle Zoho Inventory API with physical stock tracking approach."""
    
    def __init__(self, config):
        self.config = config
        self.organization_id = config.get('ZOHO_ORGANIZATION_ID')
        self.access_token = None
        self.base_url = "https://inventory.zohoapis.com/api/v1"
        self.api_region = config.get('ZOHO_API_REGION', 'com')
        
        # Adjust base URL for region
        if self.api_region != 'com':
            self.base_url = f"https://inventory.zohoapis.{self.api_region}/api/v1"
            
        # Cache for entities to avoid repeated lookups
        self._cache = {
            'items': {},
            'vendors': {},
            'customers': {},
            'taxes': {},
            'skus_by_name': {}  # Cache SKUs by product name
        }
        self._cache_lock = Lock()
        
        # Configuration flags - Using physical stock tracking instead of accounting
        self.use_physical_stock = config.get_bool('ZOHO_USE_PHYSICAL_STOCK', True)
        self.auto_generate_sku = config.get_bool('ZOHO_AUTO_GENERATE_SKU', True)
        self.sku_prefix = config.get('ZOHO_SKU_PREFIX', 'AUTO')
        
        # Tax configuration
        self.default_tax_id = config.get('ZOHO_DEFAULT_TAX_ID')
        self.tax_inclusive = config.get_bool('ZOHO_TAX_INCLUSIVE', False)
        
        logger.info(f"🔧 Initializing Zoho client...")
        logger.info(f"   - Organization ID: {self.organization_id}")
        logger.info(f"   - API Region: {self.api_region}")
        logger.info(f"   - Physical Stock Mode: {self.use_physical_stock}")
        logger.info(f"   - Auto Generate SKU: {self.auto_generate_sku}")
        
        self._refresh_access_token()
        self._load_tax_configuration()
        
        logger.info(f"✅ Zoho client initialization complete")
        
    def _refresh_access_token(self):
        """Refresh Zoho access token using refresh token."""
        try:
            response = self._make_api_request('GET', 'items')
            
            summary = {
                'total_items': 0,
                'total_stock_value': 0,
                'low_stock_items': [],
                'out_of_stock_items': []
            }
            
            for item in response.get('items', []):
                if item.get('item_type') == 'inventory':
                    summary['total_items'] += 1
                    
                    stock = item.get('stock_on_hand', 0)
                    rate = item.get('purchase_rate', 0)
                    reorder_level = item.get('reorder_level', 0)
                    
                    summary['total_stock_value'] += stock * rate
                    
                    if stock == 0:
                        summary['out_of_stock_items'].append({
                            'name': item.get('name'),
                            'sku': item.get('sku')
                        })
                    elif stock <= reorder_level:
                        summary['low_stock_items'].append({
                            'name': item.get('name'),
                            'sku': item.get('sku'),
                            'stock': stock,
                            'reorder_level': reorder_level
                        })
                        
            logger.info(f"✅ Inventory summary generated:")
            logger.info(f"   - Total items: {summary['total_items']}")
            logger.info(f"   - Total value: ${summary['total_stock_value']:.2f}")
            logger.info(f"   - Out of stock: {len(summary['out_of_stock_items'])}")
            logger.info(f"   - Low stock: {len(summary['low_stock_items'])}")
            
            return summary
            
        except Exception as e:
            logger.error(f"❌ Failed to get inventory summary: {e}")
            return {}
            
    def verify_stock_levels(self, sku: str) -> Dict:
        """Verify current stock levels for a specific SKU."""
        logger.info(f"🔍 Verifying stock levels for SKU: {sku}")
        
        try:
            item_id = self._find_item_by_sku(sku)
            if not item_id:
                return {'error': f"SKU {sku} not found"}
                
            item = self._get_item_details(item_id)
            
            result = {
                'sku': item.get('sku'),
                'name': item.get('name'),
                'stock_on_hand': item.get('stock_on_hand', 0),
                'available_stock': item.get('available_stock', 0),
                'purchase_rate': item.get('purchase_rate', 0),
                'selling_price': item.get('selling_price', 0),
                'reorder_level': item.get('reorder_level', 0),
                'stock_value': item.get('stock_on_hand', 0) * item.get('purchase_rate', 0)
            }
            
            logger.info(f"✅ Stock verification complete:")
            logger.info(f"   - Stock on hand: {result['stock_on_hand']}")
            logger.info(f"   - Stock value: ${result['stock_value']:.2f}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Failed to verify stock for {sku}: {e}")
            return {'error': str(e)}
            
    def clear_cache(self):
        """Clear the entity cache."""
        with self._cache_lock:
            self._cache = {
                'items': {},
                'vendors': {},
                'customers': {},
                'taxes': self._cache.get('taxes', {}),  # Keep tax config
                'skus_by_name': {}
            }
        logger.info("🧹 Cleared entity cache")
            logger.info("🔑 Refreshing Zoho access token...")
            
            url = f"https://accounts.zohoapis.{self.api_region}/oauth/v2/token"
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
            
            logger.info("✅ Zoho access token refreshed successfully")
            
        except Exception as e:
            logger.error(f"❌ Failed to refresh Zoho token: {str(e)}")
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
            
        except requests.exceptions.Timeout:
            logger.error(f"⏰ Zoho API timeout for {method} {endpoint}")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"🌐 Zoho API HTTP error for {method} {endpoint}: {e}")
            logger.error(f"Response: {response.text if 'response' in locals() else 'No response'}")
            raise
        except Exception as e:
            logger.error(f"💥 Zoho API error for {method} {endpoint}: {e}")
            raise
        
    def _load_tax_configuration(self):
        """Load tax configuration from Zoho with better error handling."""
        try:
            logger.info("🧾 Loading tax configuration from Zoho...")
            
            response = self._make_api_request('GET', 'settings/taxes')
            taxes = response.get('taxes', [])
            
            with self._cache_lock:
                self._cache['taxes'] = {tax['tax_id']: tax for tax in taxes}
                
            logger.info(f"✅ Loaded {len(taxes)} tax configurations")
            
            if taxes:
                # Log available taxes for debugging
                for tax in taxes[:3]:  # Show first 3
                    logger.info(f"   - {tax.get('tax_name', 'Unknown')}: {tax.get('tax_percentage', 0)}%")
                    
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400:
                logger.warning("⚠️ Could not load tax configuration (400 error)")
                logger.warning("   This might be due to API permissions or organization setup")
                logger.warning("   Tax operations will use default settings")
                # Don't raise - continue without tax config
                with self._cache_lock:
                    self._cache['taxes'] = {}
            else:
                logger.error(f"❌ HTTP error loading tax configuration: {e}")
                raise
        except Exception as e:
            logger.warning(f"⚠️ Could not load tax configuration: {e}")
            logger.warning("   Continuing without tax configuration")
            # Don't raise - continue without tax config
            with self._cache_lock:
                self._cache['taxes'] = {}
            
    def process_complete_data(self, data: Dict, transaction_type: str) -> Dict:
        """
        Process complete data using physical stock adjustments.
        Only called when data is verified complete.
        
        Args:
            data: Complete parsed data
            transaction_type: 'purchase' or 'sale'
            
        Returns:
            Dictionary with processing results
        """
        order_number = data.get('order_number', 'N/A')
        logger.info(f"🔄 Processing complete {transaction_type} data for order: {order_number}")
        
        result = {
            'success': False,
            'stock_adjusted': False,
            'items_processed': [],
            'items_failed': [],
            'errors': [],
            'warnings': []
        }
        
        try:
            if transaction_type == 'purchase':
                result = self._process_purchase_stock(data)
            elif transaction_type == 'sale':
                result = self._process_sale_stock(data)
            else:
                result['errors'].append(f"Unknown transaction type: {transaction_type}")
                logger.error(f"❌ Unknown transaction type: {transaction_type}")
                
        except Exception as e:
            result['errors'].append(str(e))
            logger.error(f"💥 Error processing complete data: {e}", exc_info=True)
            
        return result
        
    def _process_purchase_stock(self, data: Dict) -> Dict:
        """
        Process purchase using physical stock adjustment.
        
        Args:
            data: Complete purchase data
            
        Returns:
            Processing result dictionary
        """
        order_number = data.get('order_number', 'N/A')
        vendor = data.get('vendor_name', 'Unknown')
        
        logger.info(f"📦 Processing purchase stock adjustment:")
        logger.info(f"   - Order: {order_number}")
        logger.info(f"   - Vendor: {vendor}")
        
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
            # Process each item
            adjustment_items = []
            total_cost = 0
            
            items = data.get('items', [])
            logger.info(f"📋 Processing {len(items)} items...")
            
            for i, item in enumerate(items, 1):
                item_name = item.get('name', f'Item {i}')
                logger.info(f"   [{i}/{len(items)}] Processing: {item_name}")
                
                try:
                    # Get or create item with SKU
                    item_id, sku_used = self._get_or_create_item_with_sku(
                        item.get('sku'),
                        item.get('upc'),
                        item.get('product_id'),
                        item_name
                    )
                    
                    logger.info(f"      - Item ID: {item_id}")
                    logger.info(f"      - SKU: {sku_used}")
                    
                    # Calculate item cost including tax proportion
                    quantity = item.get('quantity', 0)
                    unit_price = item.get('unit_price', 0)
                    item_subtotal = quantity * unit_price
                    
                    tax_proportion = 0
                    if data.get('taxes') and data.get('subtotal'):
                        tax_rate = data.get('taxes') / data.get('subtotal')
                        tax_proportion = item_subtotal * tax_rate
                        
                    item_total_cost = item_subtotal + tax_proportion
                    unit_cost_with_tax = item_total_cost / max(1, quantity)
                    
                    logger.info(f"      - Quantity: {quantity}")
                    logger.info(f"      - Unit price: ${unit_price:.2f}")
                    logger.info(f"      - Unit cost (w/tax): ${unit_cost_with_tax:.2f}")
                    
                    # Add to adjustment
                    adjustment_items.append({
                        'item_id': item_id,
                        'quantity_adjusted': quantity,
                        'new_rate': unit_cost_with_tax,  # Cost per unit including tax
                        'notes': f"Purchase from {vendor}"
                    })
                    
                    total_cost += item_total_cost
                    
                    result['items_processed'].append({
                        'name': item_name,
                        'sku': sku_used,
                        'quantity': quantity,
                        'unit_cost': unit_cost_with_tax
                    })
                    
                    logger.info(f"      ✅ Item processed successfully")
                    
                except Exception as e:
                    logger.error(f"      ❌ Failed to process item {item_name}: {e}")
                    result['items_failed'].append({
                        'name': item_name,
                        'error': str(e)
                    })
                    result['warnings'].append(f"Item {item_name} failed: {e}")
                    
            # Create stock adjustment if we have items
            if adjustment_items:
                logger.info(f"📈 Creating stock adjustment with {len(adjustment_items)} items...")
                
                try:
                    adjustment_data = {
                        'date': data.get('date', datetime.now().strftime('%Y-%m-%d')),
                        'reason': 'Stock Received',
                        'description': f"Purchase Order: {order_number} from {vendor}",
                        'adjustment_type': 'quantity',
                        'line_items': adjustment_items,
                        'reference_number': order_number,
                        'notes': self._build_adjustment_notes(data)
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
            logger.error(f"💥 Failed to process purchase stock: {e}", exc_info=True)
            
        return result
        
    def _process_sale_stock(self, data: Dict) -> Dict:
        """
        Process sale using physical stock adjustment (reduction).
        
        Args:
            data: Complete sales data
            
        Returns:
            Processing result dictionary
        """
        order_number = data.get('order_number', 'N/A')
        channel = data.get('channel', 'Unknown')
        
        logger.info(f"💰 Processing sale stock adjustment:")
        logger.info(f"   - Order: {order_number}")
        logger.info(f"   - Channel: {channel}")
        
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
            # Process each item
            adjustment_items = []
            total_revenue = 0
            total_cogs = 0
            
            items = data.get('items', [])
            logger.info(f"📋 Processing {len(items)} items...")
            
            for i, item in enumerate(items, 1):
                item_name = item.get('name', f'Item {i}')
                logger.info(f"   [{i}/{len(items)}] Processing: {item_name}")
                
                try:
                    # Get or create item with SKU
                    item_id, sku_used = self._get_or_create_item_with_sku(
                        item.get('sku'),
                        item.get('upc'),
                        item.get('product_id'),
                        item_name
                    )
                    
                    # Get current stock and cost
                    item_details = self._get_item_details(item_id)
                    current_stock = item_details.get('stock_on_hand', 0)
                    current_cost = item_details.get('purchase_rate', 0)
                    
                    quantity = item.get('quantity', 0)
                    sale_price = item.get('sale_price', 0)
                    
                    logger.info(f"      - Item ID: {item_id}")
                    logger.info(f"      - SKU: {sku_used}")
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
                        'notes': f"Sale on {channel}"
                    })
                    
                    # Calculate revenue and COGS
                    item_revenue = quantity * sale_price
                    item_cogs = quantity * current_cost
                    
                    total_revenue += item_revenue
                    total_cogs += item_cogs
                    
                    result['items_processed'].append({
                        'name': item_name,
                        'sku': sku_used,
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
                    logger.error(f"      ❌ Failed to process item {item_name}: {e}")
                    result['items_failed'].append({
                        'name': item_name,
                        'error': str(e)
                    })
                    result['warnings'].append(f"Item {item_name} failed: {e}")
                    
            # Create stock adjustment if we have items
            if adjustment_items:
                logger.info(f"📉 Creating stock adjustment with {len(adjustment_items)} items...")
                
                try:
                    adjustment_data = {
                        'date': data.get('date', datetime.now().strftime('%Y-%m-%d')),
                        'reason': 'Goods Sold',
                        'description': f"Sale Order: {order_number} on {channel}",
                        'adjustment_type': 'quantity',
                        'line_items': adjustment_items,
                        'reference_number': order_number,
                        'notes': self._build_adjustment_notes(data)
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
            logger.error(f"💥 Failed to process sale stock: {e}", exc_info=True)
            
        return result
        
    def _get_or_create_item_with_sku(self, sku: str, upc: str, product_id: str, name: str) -> Tuple[str, str]:
        """
        Get or create item with intelligent SKU handling.
        
        Args:
            sku: Provided SKU (may be None)
            upc: UPC if available
            product_id: Other product identifier
            name: Product name
            
        Returns:
            Tuple of (item_id, sku_used)
        """
        logger.info(f"🔍 Looking up/creating item: {name}")
        
        # Try existing identifiers first
        if sku:
            logger.info(f"   - Checking existing SKU: {sku}")
            item_id = self._find_item_by_sku(sku)
            if item_id:
                logger.info(f"   ✅ Found existing item by SKU: {item_id}")
                return item_id, sku
                
        if upc:
            logger.info(f"   - Checking existing UPC: {upc}")
            item_id = self._find_item_by_upc(upc)
            if item_id:
                logger.info(f"   ✅ Found existing item by UPC: {item_id}")
                return item_id, upc
                
        if product_id:
            logger.info(f"   - Checking existing Product ID: {product_id}")
            item_id = self._find_item_by_field('product_id', product_id)
            if item_id:
                logger.info(f"   ✅ Found existing item by Product ID: {item_id}")
                return item_id, product_id
                
        # Try to find by name
        with self._cache_lock:
            if name in self._cache['skus_by_name']:
                cached_sku = self._cache['skus_by_name'][name]
                logger.info(f"   - Checking cached SKU by name: {cached_sku}")
                item_id = self._find_item_by_sku(cached_sku)
                if item_id:
                    logger.info(f"   ✅ Found existing item by cached name: {item_id}")
                    return item_id, cached_sku
                    
        # Search for existing item by name
        try:
            logger.info(f"   - Searching Zoho for existing item by name...")
            params = {'name': name}
            response = self._make_api_request('GET', 'items', params=params)
            
            items = response.get('items', [])
            if items:
                # Use exact name match
                for item in items:
                    if item.get('name', '').lower() == name.lower():
                        item_id = item['item_id']
                        item_sku = item.get('sku', '')
                        
                        logger.info(f"   ✅ Found existing item by name search: {item_id}")
                        
                        # Cache the mapping
                        with self._cache_lock:
                            self._cache['skus_by_name'][name] = item_sku
                            self._cache['items'][item_sku] = item_id
                            
                        return item_id, item_sku
                        
        except Exception as e:
            logger.debug(f"   ⚠️ Error searching for item by name: {e}")
            
        # Generate SKU if needed
        if self.auto_generate_sku:
            generated_sku = self._generate_sku(name, sku, upc, product_id)
        else:
            generated_sku = sku or upc or product_id or f"TEMP-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
        logger.info(f"   🆕 Creating new item with SKU: {generated_sku}")
        
        # Create new item
        item_data = {
            'name': name,
            'sku': generated_sku.upper(),
            'item_type': 'inventory',
            'product_type': 'goods',
            'purchase_rate': 0,
            'selling_price': 0,
            'inventory_account_name': 'Inventory Asset',
            'purchase_account_name': 'Cost of Goods Sold',
            'initial_stock': 0,
            'reorder_level': 5,
            'track_inventory': True
        }
        
        # Add UPC if available
        if upc:
            item_data['upc'] = upc
            
        try:
            response = self._make_api_request('POST', 'items', item_data)
            item_id = response['item']['item_id']
            
            # Cache the new item
            with self._cache_lock:
                self._cache['items'][generated_sku] = item_id
                self._cache['skus_by_name'][name] = generated_sku
                
            logger.info(f"   ✅ Created new item successfully: {item_id}")
            return item_id, generated_sku
            
        except Exception as e:
            logger.error(f"   ❌ Failed to create item {name}: {e}")
            raise
            
    def _generate_sku(self, name: str, sku: str = None, upc: str = None, product_id: str = None) -> str:
        """
        Generate a SKU for an item.
        
        Args:
            name: Product name
            sku: Existing SKU if any
            upc: UPC if available
            product_id: Other identifier
            
        Returns:
            Generated SKU
        """
        if sku:
            return sku
            
        if upc:
            return f"UPC-{upc}"
            
        if product_id:
            return f"ID-{product_id}"
            
        # Generate from name
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
        
        return f"{self.sku_prefix}-{prefix}-{name_hash}"
        
    def _find_item_by_sku(self, sku: str) -> Optional[str]:
        """Find item by SKU."""
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
        
    def _find_item_by_upc(self, upc: str) -> Optional[str]:
        """Find item by UPC."""
        try:
            params = {'upc': upc}
            response = self._make_api_request('GET', 'items', params=params)
            
            items = response.get('items', [])
            if items:
                return items[0]['item_id']
                
        except Exception as e:
            logger.debug(f"Error finding item by UPC {upc}: {e}")
            
        return None
        
    def _find_item_by_field(self, field: str, value: str) -> Optional[str]:
        """Find item by custom field."""
        try:
            # Search with custom field
            params = {field: value}
            response = self._make_api_request('GET', 'items', params=params)
            
            items = response.get('items', [])
            if items:
                return items[0]['item_id']
                
        except Exception as e:
            logger.debug(f"Error finding item by {field} = {value}: {e}")
            
        return None
        
    def _get_item_details(self, item_id: str) -> Dict:
        """Get detailed item information."""
        try:
            response = self._make_api_request('GET', f'items/{item_id}')
            return response.get('item', {})
            
        except Exception as e:
            logger.error(f"Failed to get item details for {item_id}: {e}")
            return {}
            
    def _build_adjustment_notes(self, data: Dict) -> str:
        """Build notes for stock adjustment."""
        notes = []
        
        if data.get('order_number'):
            notes.append(f"Order #: {data.get('order_number')}")
            
        if data.get('vendor_name'):
            notes.append(f"Vendor: {data.get('vendor_name')}")
        elif data.get('channel'):
            notes.append(f"Channel: {data.get('channel')}")
            
        if data.get('email_seq_num'):
            notes.append(f"Email Seq: {data.get('email_seq_num')}")
            
        if data.get('confidence_score'):
            notes.append(f"Confidence: {data.get('confidence_score', 0):.2f}")
            
        return ' | '.join(notes)
        
    def get_inventory_summary(self) -> Dict:
        """Get summary of current inventory levels."""
        logger.info("📊 Generating inventory summary...")
        
        try: