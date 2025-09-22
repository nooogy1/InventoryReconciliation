"""Zoho entity management for vendors, customers, and items."""

import logging
from typing import Dict

logger = logging.getLogger(__name__)


class ZohoEntityManager:
    """Manages Zoho entities: vendors, customers, and items."""
    
    def __init__(self, base_client):
        self.base_client = base_client
        self.config = base_client.config
        
        # Account configuration
        self.default_inventory_account = self.config.get('ZOHO_DEFAULT_INVENTORY_ACCOUNT')
        self.default_cogs_account = self.config.get('ZOHO_DEFAULT_COGS_ACCOUNT')
        self.default_sales_account = self.config.get('ZOHO_DEFAULT_SALES_ACCOUNT')

    # ===========================================
    # VENDOR MANAGEMENT
    # ===========================================

    def find_or_create_vendor(self, vendor_name: str, vendor_data: Dict = None) -> str:
        """Find existing vendor or create new one."""
        standardized_name = self._standardize_vendor_name(vendor_name)
        
        # Check cache first
        with self.base_client._cache_lock:
            if standardized_name in self.base_client._cache['vendors']:
                return self.base_client._cache['vendors'][standardized_name]
        
        try:
            # Search for existing vendor
            search_response = self.base_client._make_api_request('GET', 'contacts', {
                'contact_type': 'vendor',
                'search_text': standardized_name
            })
            
            vendors = search_response.get('contacts', [])
            for vendor in vendors:
                if vendor.get('contact_name', '').lower() == standardized_name.lower():
                    vendor_id = vendor['contact_id']
                    
                    with self.base_client._cache_lock:
                        self.base_client._cache['vendors'][standardized_name] = vendor_id
                    
                    logger.info(f"✅ Found existing vendor: {standardized_name} (ID: {vendor_id})")
                    return vendor_id
            
            # Create new vendor
            vendor_create_data = {
                'contact_name': standardized_name,
                'contact_type': 'vendor',
                'company_name': standardized_name
            }
            
            create_response = self.base_client._make_api_request('POST', 'contacts', vendor_create_data)
            vendor_id = create_response.get('contact', {}).get('contact_id')
            
            with self.base_client._cache_lock:
                self.base_client._cache['vendors'][standardized_name] = vendor_id
            
            logger.info(f"✅ Created new vendor: {standardized_name} (ID: {vendor_id})")
            return vendor_id
            
        except Exception as e:
            logger.error(f"❌ Failed to find/create vendor {vendor_name}: {e}")
            raise

    def _standardize_vendor_name(self, vendor_name: str) -> str:
        """Clean and standardize vendor names."""
        if not vendor_name:
            return "Unknown Vendor"
        
        standardized = vendor_name.strip()
        
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

    def find_or_create_customer(self, channel_name: str, customer_email: str = None) -> str:
        """Find existing customer or create new one for sales channel."""
        standardized_name = self._standardize_channel_name(channel_name)
        
        # Check cache first
        with self.base_client._cache_lock:
            if standardized_name in self.base_client._cache['customers']:
                return self.base_client._cache['customers'][standardized_name]
        
        try:
            # Search for existing customer
            search_response = self.base_client._make_api_request('GET', 'contacts', {
                'contact_type': 'customer',
                'search_text': standardized_name
            })
            
            customers = search_response.get('contacts', [])
            for customer in customers:
                if customer.get('contact_name', '').lower() == standardized_name.lower():
                    customer_id = customer['contact_id']
                    
                    with self.base_client._cache_lock:
                        self.base_client._cache['customers'][standardized_name] = customer_id
                    
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
            
            create_response = self.base_client._make_api_request('POST', 'contacts', customer_create_data)
            customer_id = create_response.get('contact', {}).get('contact_id')
            
            with self.base_client._cache_lock:
                self.base_client._cache['customers'][standardized_name] = customer_id
            
            logger.info(f"✅ Created new customer: {standardized_name} (ID: {customer_id})")
            return customer_id
            
        except Exception as e:
            logger.error(f"❌ Failed to find/create customer {channel_name}: {e}")
            raise

    def _standardize_channel_name(self, channel_name: str) -> str:
        """Clean and standardize channel names for customer creation."""
        if not channel_name:
            return "Direct Sales"
        
        standardized = channel_name.strip()
        
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
    # ITEM MANAGEMENT
    # ===========================================

    def ensure_item_exists_in_zoho(self, sku: str, item_name: str) -> str:
        """Ensure item exists in Zoho, create if missing."""
        if not sku:
            raise ValueError("SKU is required for item creation")
        
        # Check cache first
        with self.base_client._cache_lock:
            if sku in self.base_client._cache['items']:
                return self.base_client._cache['items'][sku]
        
        try:
            # Search for existing item by SKU
            search_response = self.base_client._make_api_request('GET', 'items', {'sku': sku})
            items = search_response.get('items', [])
            
            if items:
                item_id = items[0]['item_id']
                
                with self.base_client._cache_lock:
                    self.base_client._cache['items'][sku] = item_id
                
                logger.debug(f"✅ Found existing item: {sku} (ID: {item_id})")
                return item_id
            
            # Create new item
            logger.info(f"📦 Creating new item: {sku} - {item_name}")
            item_data = self._build_item_creation_data(sku, item_name)
            
            create_response = self.base_client._make_api_request('POST', 'items', item_data)
            item_id = create_response.get('item', {}).get('item_id')
            
            with self.base_client._cache_lock:
                self.base_client._cache['items'][sku] = item_id
            
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

    def get_item_details(self, item_id: str) -> Dict:
        """Get detailed item information including current stock and rate."""
        try:
            response = self.base_client._make_api_request('GET', f'items/{item_id}')
            return response.get('item', {})
        except Exception as e:
            logger.warning(f"⚠️ Could not get item details for {item_id}: {e}")
            return {}