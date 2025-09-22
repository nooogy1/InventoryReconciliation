"""
GitHub Gist-based token manager for Zoho OAuth access tokens.
Implements caching pattern to reduce unnecessary refresh requests.
Stores tokens in plain text in secret GitHub Gist.
"""

import json
import logging
import requests
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class GitHubGistTokenManager:
    """Manages Zoho access tokens using GitHub secret Gist for persistence."""
    
    def __init__(self, config):
        self.config = config
        self.github_token = config.get('GITHUB_TOKEN')
        self.gist_id = config.get('ZOHO_ACCESS_GIST_ID')
        
        # Validate required configuration
        if not self.github_token:
            raise ValueError("GITHUB_TOKEN environment variable is required")
        if not self.gist_id:
            raise ValueError("ZOHO_ACCESS_GIST_ID environment variable is required")
        
        logger.info("GitHub Gist token manager initialized")
        logger.info(f"   - Gist ID: {self.gist_id}")
        logger.info(f"   - Storage: Plain text (no encryption)")

    def get_cached_token(self) -> Optional[Dict[str, Any]]:
        """
        Retrieve cached access token from GitHub Gist.
        
        Returns:
            Dict with token data or None if not found/invalid
        """
        try:
            logger.debug("Fetching cached token from GitHub Gist...")
            
            # Fetch gist content
            headers = {
                'Authorization': f'token {self.github_token}',
                'Accept': 'application/vnd.github.v3+json'
            }
            
            response = requests.get(
                f'https://api.github.com/gists/{self.gist_id}',
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 404:
                logger.info("Gist not found - will create new one when caching token")
                return None
            
            response.raise_for_status()
            gist_data = response.json()
            
            # Extract token from gist files
            files = gist_data.get('files', {})
            token_file = files.get('zoho_access_token.json')
            
            if not token_file:
                logger.warning("Token file not found in gist")
                return None
            
            # Parse token data
            token_content = token_file.get('content', '')
            if not token_content:
                logger.warning("Empty token content in gist")
                return None
            
            token_data = json.loads(token_content)
            
            # Validate token structure
            if not all(key in token_data for key in ['access_token', 'expires_at']):
                logger.warning("Invalid token structure in gist")
                return None
            
            # Check if token is expired
            expires_at = datetime.fromisoformat(token_data['expires_at'])
            if datetime.utcnow() >= expires_at:
                logger.info("Cached token has expired")
                return None
            
            logger.info("Successfully retrieved valid cached token")
            return token_data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"GitHub API error while fetching token: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse token JSON: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching cached token: {e}")
            return None

    def cache_token(self, access_token: str, expires_in: int = 3600) -> bool:
        """
        Cache access token to GitHub Gist.
        
        Args:
            access_token: The access token to cache
            expires_in: Token lifetime in seconds (default 1 hour)
            
        Returns:
            True if successfully cached, False otherwise
        """
        try:
            logger.info("Caching new access token to GitHub Gist...")
            
            # Calculate expiry time (with 5 minute buffer for safety)
            expires_at = datetime.utcnow() + timedelta(seconds=expires_in - 300)
            
            # Prepare token data
            token_data = {
                'access_token': access_token,
                'expires_at': expires_at.isoformat(),
                'cached_at': datetime.utcnow().isoformat(),
                'expires_in': expires_in
            }
            
            # Convert to JSON
            token_content = json.dumps(token_data, indent=2)
            
            # Prepare gist data
            gist_data = {
                'description': 'Zoho OAuth Access Token Cache',
                'public': False,  # Secret gist
                'files': {
                    'zoho_access_token.json': {
                        'content': token_content
                    }
                }
            }
            
            headers = {
                'Authorization': f'token {self.github_token}',
                'Accept': 'application/vnd.github.v3+json'
            }
            
            # Update or create gist
            if self._gist_exists():
                # Update existing gist
                response = requests.patch(
                    f'https://api.github.com/gists/{self.gist_id}',
                    headers=headers,
                    json=gist_data,
                    timeout=10
                )
            else:
                # Create new gist
                response = requests.post(
                    'https://api.github.com/gists',
                    headers=headers,
                    json=gist_data,
                    timeout=10
                )
                
                # Update gist_id if created new one
                if response.status_code == 201:
                    new_gist_id = response.json().get('id')
                    logger.info(f"Created new gist with ID: {new_gist_id}")
                    logger.warning(f"Update ZOHO_ACCESS_GIST_ID to: {new_gist_id}")
            
            response.raise_for_status()
            
            logger.info("Successfully cached access token to GitHub Gist")
            logger.debug(f"Token expires at: {expires_at.isoformat()}")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"GitHub API error while caching token: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error caching token: {e}")
            return False

    def clear_cached_token(self) -> bool:
        """
        Clear cached token from GitHub Gist.
        
        Returns:
            True if successfully cleared, False otherwise
        """
        try:
            logger.info("Clearing cached token from GitHub Gist...")
            
            # Update gist with empty content
            gist_data = {
                'files': {
                    'zoho_access_token.json': {
                        'content': json.dumps({
                            'cleared_at': datetime.utcnow().isoformat(),
                            'status': 'cleared'
                        }, indent=2)
                    }
                }
            }
            
            headers = {
                'Authorization': f'token {self.github_token}',
                'Accept': 'application/vnd.github.v3+json'
            }
            
            response = requests.patch(
                f'https://api.github.com/gists/{self.gist_id}',
                headers=headers,
                json=gist_data,
                timeout=10
            )
            
            response.raise_for_status()
            
            logger.info("Successfully cleared cached token")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"GitHub API error while clearing token: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error clearing token: {e}")
            return False

    def _gist_exists(self) -> bool:
        """Check if the gist exists."""
        try:
            headers = {
                'Authorization': f'token {self.github_token}',
                'Accept': 'application/vnd.github.v3+json'
            }
            
            response = requests.get(
                f'https://api.github.com/gists/{self.gist_id}',
                headers=headers,
                timeout=10
            )
            
            return response.status_code == 200
            
        except:
            return False

    def get_token_info(self) -> Dict[str, Any]:
        """Get information about the current cached token."""
        cached_token = self.get_cached_token()
        
        if not cached_token:
            return {
                'cached': False,
                'status': 'No cached token available'
            }
        
        expires_at = datetime.fromisoformat(cached_token['expires_at'])
        time_until_expiry = expires_at - datetime.utcnow()
        
        return {
            'cached': True,
            'expires_at': cached_token['expires_at'],
            'cached_at': cached_token.get('cached_at'),
            'expires_in_seconds': int(time_until_expiry.total_seconds()),
            'expires_in_minutes': round(time_until_expiry.total_seconds() / 60),
            'is_valid': time_until_expiry.total_seconds() > 0
        }