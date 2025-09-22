"""Base Zoho client with authentication and core API functionality."""

import logging
import requests
import json
from typing import Dict, Optional
from datetime import datetime
from threading import Lock

from ..github_token_manager import GitHubGistTokenManager

logger = logging.getLogger(__name__)


class ZohoBaseClient:
    """Base Zoho client handling authentication and core API operations."""
    
    def __init__(self, config):
        self.config = config
        self.organization_id = config.get('ZOHO_ORGANIZATION_ID')
        self.access_token = None
        
        # Initialize GitHub Gist token manager
        try:
            self.token_manager = GitHubGistTokenManager(config)
            self.use_token_caching = True
            logger.info("GitHub Gist token caching enabled")
        except ValueError as e:
            logger.warning(f"GitHub Gist token caching disabled: {e}")
            logger.warning("Falling back to refresh-on-every-call method")
            self.token_manager = None
            self.use_token_caching = False
        
        # API configuration
        self.base_url = "https://www.zohoapis.com/inventory/v1"
        self.api_region = config.get('ZOHO_API_REGION', 'com')
        self.is_available = None
        
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
        
        # Cache and thread safety
        self._cache = {
            'items': {},
            'vendors': {},
            'customers': {},
            'taxes': {}
        }
        self._cache_lock = Lock()
        
        logger.info(f"🔧 Zoho base client initialized:")
        logger.info(f"   - Base URL: {self.base_url}")
        logger.info(f"   - Token Caching: {self.use_token_caching}")

    def _ensure_connection(self) -> bool:
        """Ensure Zoho connection is available - only connects when first needed."""
        if self.is_available is not None:
            return self.is_available
            
        logger.info("🔗 First Zoho API access - establishing connection...")
        
        # Initialize connection - FIX: Use _ensure_access_token
        if self._ensure_access_token():
            self.is_available = self.test_connection()
            if self.is_available:
                self._load_cache()
                logger.info("✅ Zoho connection established successfully")
            else:
                logger.warning("⚠️ Zoho connection failed - will retry on next API call")
        else:
            self.is_available = False
            logger.error("❌ Failed to get Zoho access token")
            
        return self.is_available

    def _ensure_access_token(self) -> bool:
        """Ensure we have a valid access token using GitHub Gist caching."""
        if self.use_token_caching and self.token_manager:
            return self._get_cached_or_refresh_token()
        else:
            return self._refresh_access_token_legacy()

    def _get_cached_or_refresh_token(self) -> bool:
        """Get token from cache or refresh if needed."""
        try:
            logger.debug("Checking for cached access token...")
            cached_token = self.token_manager.get_cached_token()
            
            if cached_token:
                test_token = cached_token['access_token']
                if self._validate_token(test_token):
                    logger.info("Using valid cached access token")
                    self.access_token = test_token
                    return True
                else:
                    logger.info("Cached token is invalid, refreshing...")
            else:
                logger.info("No cached token found, refreshing...")
            
            if self._refresh_access_token_and_cache():
                return True
            
            logger.error("Failed to obtain valid access token")
            return False
            
        except Exception as e:
            logger.error(f"Error in token management: {e}")
            return self._refresh_access_token_legacy()

    def _validate_token(self, token: str) -> bool:
        """Validate an access token with a lightweight API call."""
        try:
            headers = {
                "Authorization": f"Zoho-oauthtoken {token}",
                "Content-Type": "application/json"
            }
            
            response = requests.get(
                f"{self.base_url}/organizations",
                headers=headers,
                timeout=10
            )
            
            return response.status_code == 200
            
        except Exception as e:
            logger.debug(f"Token validation failed: {e}")
            return False

    def _refresh_access_token_and_cache(self) -> bool:
        """Refresh access token and cache it to GitHub Gist."""
        try:
            logger.info("Refreshing Zoho access token...")
            
            auth_url = f"https://accounts.zoho.{self.api_region}/oauth/v2/token"
            
            data = {
                'refresh_token': self.config.get('ZOHO_REFRESH_TOKEN'),
                'client_id': self.config.get('ZOHO_CLIENT_ID'),
                'client_secret': self.config.get('ZOHO_CLIENT_SECRET'),
                'grant_type': 'refresh_token'
            }
            
            response = requests.post(auth_url, data=data, timeout=30)
            response.raise_for_status()
            
            token_data = response.json()
            access_token = token_data.get('access_token')
            expires_in = token_data.get('expires_in', 3600)
            
            if not access_token:
                logger.error("No access token in refresh response")
                return False
            
            if self.token_manager.cache_token(access_token, expires_in):
                logger.info("New access token cached successfully")
            else:
                logger.warning("Failed to cache new access token, but will continue")
            
            self.access_token = access_token
            logger.info("Zoho access token refreshed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to refresh access token: {e}")
            return False

    def _refresh_access_token_legacy(self) -> bool:
        """Legacy token refresh method (refresh on every call)."""
        try:
            logger.debug("Using legacy token refresh (no caching)")
            
            auth_url = f"https://accounts.zoho.{self.api_region}/oauth/v2/token"
            
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
                logger.debug("Legacy token refresh successful")
                return True
            else:
                logger.error("No access token in legacy refresh response")
                return False
                
        except Exception as e:
            logger.error(f"Legacy token refresh failed: {e}")
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
            
            # Handle token expiration - FIX: Use _ensure_access_token
            if response.status_code == 401 and retry:
                logger.info("🔑 Token expired, refreshing...")
                self._ensure_access_token()
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