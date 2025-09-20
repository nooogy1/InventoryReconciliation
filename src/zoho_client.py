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
        
        self._refresh_access_token()
        self._load_tax_configuration()
        
    def _refresh_access_token(self):
        """Refresh Zoho access token using refresh token."""
        try:
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
                
        except Exception as e:
            result['errors'].append(str(e))
            logger.error(f"Error processing complete data: {e}")
            
        return result
        
    def _process_purchase_stock(self, data: Dict) -> Dict:
        """
        Process purchase using physical stock adjustment.
        
        Args:
            data: Complete purchase data
            
        Returns:
            Processing result dictionary
        """
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
            
            for item in data.get('items', []):
                try:
                    # Get or create item with SKU
                    item_id, sku_used = self._get_or_create_item_with_sku(
                        item.get('sku'),
                        item.get('upc'),
                        item.get('product_id'),
                        item.get('name')
                    )
                    
                    # Calculate item cost including tax proportion
                    item_subtotal = item.get('quantity', 0) * item.get('unit_price', 0)
                    tax_proportion = 0
                    
                    if data.get('taxes') and data.get('subtotal'):
                        tax_rate = data.get('taxes') / data.get('subtotal')
                        tax_proportion = item_subtotal * tax_rate
                        
                    item_total_cost = item_subtotal + tax_proportion
                    unit_cost_with_tax = item_total_cost / max(1, item.get('quantity', 1))
                    
                    # Add to adjustment
                    adjustment_items.append({
                        'item_id': item_id,
                        'quantity_adjusted': item.get('quantity', 0),
                        'new_rate': unit_cost_with_tax,  # Cost per unit including tax
                        'notes': f"Purchase from {data.get('vendor_name', 'Unknown')}"
                    })
                    
                    total_cost += item_total_cost
                    
                    result['items_processed'].append({
                        'name': item.get('name'),
                        'sku': sku_used,
                        'quantity': item.get('quantity'),
                        'unit_cost': unit_cost_with_tax
                    })
                    
                except Exception as e:
                    logger.error(f"Failed to process item {item.get('name')}: {e}")
                    result['items_failed'].append({
                        'name': item.get('name'),
                        'error': str(e)
                    })
                    result['warnings'].append(f"Item {item.get('name')} failed: {e}")
                    
            # Create stock adjustment if we have items
            if adjustment_items:
                try:
                    adjustment_data = {
                        'date': data.get('date', datetime.now().strftime('%Y-%m-%d')),
                        'reason': 'Stock Received',
                        'description': f"Purchase Order: {data.get('order_number', 'N/A')} from {data.get('vendor_name', 'Unknown')}",
                        'adjustment_type': 'quantity',
                        'line_items': adjustment_items,
                        'reference_number': data.get('order_number'),
                        'notes': self._build_adjustment_notes(data)
                    }
                    
                    # Create the adjustment
                    adjustment_response = self._make_api_request(
                        'POST', 
                        'inventoryadjustments',
                        adjustment_data
                    )
                    
                    result['adjustment_id'] = adjustment_response.get('inventory_adjustment', {}).get('inventory_adjustment_id')
                    result['stock_adjusted'] = True
                    result['success'] = True
                    
                    logger.info(f"Created stock adjustment for purchase: {result['adjustment_id']}")
                    
                except Exception as e:
                    result['errors'].append(f"Failed to create stock adjustment: {e}")
                    logger.error(f"Stock adjustment failed: {e}")
                    
            else:
                result['errors'].append("No items could be processed")
                
        except Exception as e:
            result['errors'].append(f"Purchase processing error: {e}")
            logger.error(f"Failed to process purchase stock: {e}")
            
        return result
        
    def _process_sale_stock(self, data: Dict) -> Dict:
        """
        Process sale using physical stock adjustment (reduction).
        
        Args:
            data: Complete sales data
            
        Returns:
            Processing result dictionary
        """
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
            
            for item in data.get('items', []):
                try:
                    # Get or create item with SKU
                    item_id, sku_used = self._get_or_create_item_with_sku(
                        item.get('sku'),
                        item.get('upc'),
                        item.get('product_id'),
                        item.get('name')
                    )
                    
                    # Get current stock and cost
                    item_details = self._get_item_details(item_id)
                    current_stock = item_details.get('stock_on_hand', 0)
                    current_cost = item_details.get('purchase_rate', 0)
                    
                    # Check if we have sufficient stock
                    if current_stock < item.get('quantity', 0):
                        result['warnings'].append(
                            f"Insufficient stock for {item.get('name')}: "
                            f"have {current_stock}, need {item.get('quantity')}"
                        )
                        
                    # Add to adjustment (negative for sales)
                    adjustment_items.append({
                        'item_id': item_id,
                        'quantity_adjusted': -item.get('quantity', 0),  # Negative for reduction
                        'notes': f"Sale on {data.get('channel', 'Unknown')}"
                    })
                    
                    # Calculate revenue and COGS
                    item_revenue = item.get('quantity', 0) * item.get('sale_price', 0)
                    item_cogs = item.get('quantity', 0) * current_cost
                    
                    total_revenue += item_revenue
                    total_cogs += item_cogs
                    
                    result['items_processed'].append({
                        'name': item.get('name'),
                        'sku': sku_used,
                        'quantity': item.get('quantity'),
                        'sale_price': item.get('sale_price'),
                        'cost': current_cost,
                        'profit': item_revenue - item_cogs
                    })
                    
                except Exception as e:
                    logger.error(f"Failed to process item {item.get('name')}: {e}")
                    result['items_failed'].append({
                        'name': item.get('name'),
                        'error': str(e)
                    })
                    result['warnings'].append(f"Item {item.get('name')} failed: {e}")
                    
            # Create stock adjustment if we have items
            if adjustment_items:
                try:
                    adjustment_data = {
                        'date': data.get('date', datetime.now().strftime('%Y-%m-%d')),
                        'reason': 'Goods Sold',
                        'description': f"Sale Order: {data.get('order_number', 'N/A')} on {data.get('channel', 'Unknown')}",
                        'adjustment_type': 'quantity',
                        'line_items': adjustment_items,
                        'reference_number': data.get('order_number'),
                        'notes': self._build_adjustment_notes(data)
                    }
                    
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
                    
                    logger.info(f"Created stock adjustment for sale: {result['adjustment_id']}")
                    
                except Exception as e:
                    result['errors'].append(f"Failed to create stock adjustment: {e}")
                    logger.error(f"Stock adjustment failed: {e}")
                    
            else:
                result['errors'].append("No items could be processed")
                
        except Exception as e:
            result['errors'].append(f"Sale processing error: {e}")
            logger.error(f"Failed to process sale stock: {e}")
            
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
        # Try existing identifiers first
        if sku:
            item_id = self._find_item_by_sku(sku)
            if item_id:
                return item_id, sku
                
        if upc:
            item_id = self._find_item_by_upc(upc)
            if item_id:
                return item_id, upc
                
        if product_id:
            item_id = self._find_item_by_field('product_id', product_id)
            if item_id:
                return item_id, product_id
                
        # Try to find by name
        with self._cache_lock:
            if name in self._cache['skus_by_name']:
                cached_sku = self._cache['skus_by_name'][name]
                item_id = self._find_item_by_sku(cached_sku)
                if item_id:
                    return item_id, cached_sku
                    
        # Search for existing item by name
        try:
            params = {'name': name}
            response = self._make_api_request('GET', 'items', params=params)
            
            items = response.get('items', [])
            if items:
                # Use exact name match
                for item in items:
                    if item.get('name', '').lower() == name.lower():
                        item_id = item['item_id']
                        item_sku = item.get('sku', '')
                        
                        # Cache the mapping
                        with self._cache_lock:
                            self._cache['skus_by_name'][name] = item_sku
                            self._cache['items'][item_sku] = item_id
                            
                        return item_id, item_sku
                        
        except Exception as e:
            logger.debug(f"Error searching for item by name: {e}")
            
        # Generate SKU if needed
        if self.auto_generate_sku:
            generated_sku = self._generate_sku(name, sku, upc, product_id)
        else:
            generated_sku = sku or upc or product_id or f"TEMP-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
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
                
            logger.info(f"Created new item: {name} with SKU: {generated_sku}")
            return item_id, generated_sku
            
        except Exception as e:
            logger.error(f"Failed to create item {name}: {e}")
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
            
        if data.get('email_uid'):
            notes.append(f"Email UID: {data.get('email_uid')}")
            
        if data.get('confidence_score'):
            notes.append(f"Confidence: {data.get('confidence_score', 0):.2f}")
            
        return ' | '.join(notes)
        
    def get_inventory_summary(self) -> Dict:
        """Get summary of current inventory levels."""
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
                        
            return summary
            
        except Exception as e:
            logger.error(f"Failed to get inventory summary: {e}")
            return {}
            
    def verify_stock_levels(self, sku: str) -> Dict:
        """Verify current stock levels for a specific SKU."""
        try:
            item_id = self._find_item_by_sku(sku)
            if not item_id:
                return {'error': f"SKU {sku} not found"}
                
            item = self._get_item_details(item_id)
            
            return {
                'sku': item.get('sku'),
                'name': item.get('name'),
                'stock_on_hand': item.get('stock_on_hand', 0),
                'available_stock': item.get('available_stock', 0),
                'purchase_rate': item.get('purchase_rate', 0),
                'selling_price': item.get('selling_price', 0),
                'reorder_level': item.get('reorder_level', 0),
                'stock_value': item.get('stock_on_hand', 0) * item.get('purchase_rate', 0)
            }
            
        except Exception as e:
            logger.error(f"Failed to verify stock for {sku}: {e}")
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
        logger.info("Cleared entity cache")