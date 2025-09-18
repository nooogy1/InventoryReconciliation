"""Configuration management for the application with enhanced security and type safety."""

import os
import json
import logging
from typing import Any, Dict, Optional, Union
from dotenv import load_dotenv, find_dotenv
from pathlib import Path

logger = logging.getLogger(__name__)


class Config:
    """Application configuration handler with type safety and security features."""
    
    # Default values for optional configuration
    DEFAULTS = {
        'POLL_INTERVAL': 300,  # 5 minutes
        'LOG_LEVEL': 'INFO',
        'MAX_RETRIES': 3,
        'RETRY_DELAY': 5,
        'GMAIL_IMAP_SERVER': 'imap.gmail.com',
        'GMAIL_IMAP_PORT': 993,
        'AIRTABLE_PURCHASES_TABLE': 'Purchases',
        'AIRTABLE_SALES_TABLE': 'Sales',
        'OPENAI_MODEL': 'gpt-4',
        'OPENAI_TEMPERATURE': 0.1,
        'ZOHO_API_REGION': 'com',  # com, eu, in, au, jp
        'DISCORD_RETRY_ON_FAIL': True,
        'EMAIL_BATCH_SIZE': 10,
        'ENABLE_DRY_RUN': False,  # For testing without making actual API calls
    }
    
    # Required configuration keys
    REQUIRED_VARS = [
        'GMAIL_USER',
        'GMAIL_APP_PASSWORD',  # Renamed for clarity - must be app-specific password
        'OPENAI_API_KEY',
        'AIRTABLE_API_KEY',
        'AIRTABLE_BASE_ID',
        'ZOHO_CLIENT_ID',
        'ZOHO_CLIENT_SECRET',
        'ZOHO_REFRESH_TOKEN',
        'ZOHO_ORGANIZATION_ID',
        'DISCORD_WEBHOOK_URL'
    ]
    
    # Sensitive keys that should be masked in logs
    SENSITIVE_KEYS = [
        'GMAIL_APP_PASSWORD',
        'OPENAI_API_KEY',
        'AIRTABLE_API_KEY',
        'ZOHO_CLIENT_SECRET',
        'ZOHO_REFRESH_TOKEN',
        'DISCORD_WEBHOOK_URL'
    ]
    
    def __init__(self, env_file: Optional[str] = None, reload: bool = False):
        """
        Initialize configuration.
        
        Args:
            env_file: Path to .env file (optional)
            reload: Force reload of environment variables
        """
        self.env_file = env_file or find_dotenv()
        self._cache: Dict[str, Any] = {}
        self._secrets_loaded = False
        
        # Load environment variables
        self.load_env(reload)
        
        # Attempt to load from secret manager if available
        self._load_from_secret_manager()
        
        # Validate configuration
        self.validate()
        
        # Log configuration status (with sensitive data masked)
        self._log_config_status()
        
    def load_env(self, reload: bool = False) -> None:
        """
        Load or reload environment variables.
        
        Args:
            reload: Force reload even if already loaded
        """
        if reload:
            # Clear environment variable cache
            self._cache.clear()
            
        # Load from .env file
        load_dotenv(self.env_file, override=reload)
        logger.debug(f"Loaded environment from: {self.env_file}")
        
    def _load_from_secret_manager(self) -> None:
        """
        Attempt to load secrets from a secret manager.
        Supports AWS Secrets Manager, Azure Key Vault, or Google Secret Manager.
        """
        # Check if using a secret manager
        secret_backend = os.getenv('SECRET_BACKEND', 'env').lower()
        
        if secret_backend == 'aws':
            self._load_aws_secrets()
        elif secret_backend == 'azure':
            self._load_azure_secrets()
        elif secret_backend == 'google':
            self._load_google_secrets()
        elif secret_backend == 'env':
            # Using environment variables (default)
            pass
        else:
            logger.warning(f"Unknown secret backend: {secret_backend}")
            
    def _load_aws_secrets(self) -> None:
        """Load secrets from AWS Secrets Manager."""
        try:
            import boto3
            from botocore.exceptions import ClientError
            
            secret_name = os.getenv('AWS_SECRET_NAME')
            region = os.getenv('AWS_REGION', 'us-east-1')
            
            if not secret_name:
                return
                
            session = boto3.session.Session()
            client = session.client(
                service_name='secretsmanager',
                region_name=region
            )
            
            response = client.get_secret_value(SecretId=secret_name)
            secrets = json.loads(response['SecretString'])
            
            # Update environment with secrets
            for key, value in secrets.items():
                os.environ[key] = str(value)
                
            self._secrets_loaded = True
            logger.info("Loaded secrets from AWS Secrets Manager")
            
        except ImportError:
            logger.debug("boto3 not installed, skipping AWS Secrets Manager")
        except ClientError as e:
            logger.error(f"Error loading AWS secrets: {e}")
        except Exception as e:
            logger.error(f"Unexpected error loading AWS secrets: {e}")
            
    def _load_azure_secrets(self) -> None:
        """Load secrets from Azure Key Vault."""
        try:
            from azure.keyvault.secrets import SecretClient
            from azure.identity import DefaultAzureCredential
            
            vault_url = os.getenv('AZURE_VAULT_URL')
            if not vault_url:
                return
                
            credential = DefaultAzureCredential()
            client = SecretClient(vault_url=vault_url, credential=credential)
            
            # Load each required secret
            for key in self.REQUIRED_VARS:
                try:
                    secret = client.get_secret(key.replace('_', '-'))
                    os.environ[key] = secret.value
                except Exception:
                    pass  # Secret might not exist or be in env already
                    
            self._secrets_loaded = True
            logger.info("Loaded secrets from Azure Key Vault")
            
        except ImportError:
            logger.debug("azure-keyvault not installed, skipping Azure Key Vault")
        except Exception as e:
            logger.error(f"Error loading Azure secrets: {e}")
            
    def _load_google_secrets(self) -> None:
        """Load secrets from Google Secret Manager."""
        try:
            from google.cloud import secretmanager
            
            project_id = os.getenv('GCP_PROJECT_ID')
            if not project_id:
                return
                
            client = secretmanager.SecretManagerServiceClient()
            
            # Load each required secret
            for key in self.REQUIRED_VARS:
                try:
                    name = f"projects/{project_id}/secrets/{key}/versions/latest"
                    response = client.access_secret_version(request={"name": name})
                    os.environ[key] = response.payload.data.decode('UTF-8')
                except Exception:
                    pass  # Secret might not exist or be in env already
                    
            self._secrets_loaded = True
            logger.info("Loaded secrets from Google Secret Manager")
            
        except ImportError:
            logger.debug("google-cloud-secret-manager not installed, skipping Google Secret Manager")
        except Exception as e:
            logger.error(f"Error loading Google secrets: {e}")
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value with caching.
        
        Args:
            key: Configuration key
            default: Default value if not found
            
        Returns:
            Configuration value or default
        """
        if key in self._cache:
            return self._cache[key]
            
        value = os.getenv(key, self.DEFAULTS.get(key, default))
        self._cache[key] = value
        return value
        
    def get_int(self, key: str, default: Optional[int] = None) -> int:
        """
        Get configuration value as integer.
        
        Args:
            key: Configuration key
            default: Default value if not found or invalid
            
        Returns:
            Integer value
        """
        if default is None:
            default = self.DEFAULTS.get(key, 0)
            
        value = self.get(key, default)
        
        try:
            return int(value)
        except (ValueError, TypeError):
            logger.warning(f"Invalid integer value for {key}: {value}, using default: {default}")
            return default
            
    def get_float(self, key: str, default: Optional[float] = None) -> float:
        """
        Get configuration value as float.
        
        Args:
            key: Configuration key
            default: Default value if not found or invalid
            
        Returns:
            Float value
        """
        if default is None:
            default = float(self.DEFAULTS.get(key, 0.0))
            
        value = self.get(key, default)
        
        try:
            return float(value)
        except (ValueError, TypeError):
            logger.warning(f"Invalid float value for {key}: {value}, using default: {default}")
            return default
            
    def get_bool(self, key: str, default: Optional[bool] = None) -> bool:
        """
        Get configuration value as boolean.
        
        Args:
            key: Configuration key
            default: Default value if not found
            
        Returns:
            Boolean value
        """
        if default is None:
            default = bool(self.DEFAULTS.get(key, False))
            
        value = self.get(key, default)
        
        if isinstance(value, bool):
            return value
            
        if isinstance(value, str):
            return value.lower() in ('true', '1', 'yes', 'on', 'enabled')
            
        return bool(value)
        
    def get_list(self, key: str, separator: str = ',', default: Optional[list] = None) -> list:
        """
        Get configuration value as list.
        
        Args:
            key: Configuration key
            separator: String separator for list items
            default: Default value if not found
            
        Returns:
            List value
        """
        if default is None:
            default = self.DEFAULTS.get(key, [])
            
        value = self.get(key)
        
        if value is None:
            return default
            
        if isinstance(value, list):
            return value
            
        if isinstance(value, str):
            return [item.strip() for item in value.split(separator) if item.strip()]
            
        return default
        
    def get_json(self, key: str, default: Optional[Dict] = None) -> Dict:
        """
        Get configuration value as JSON/dict.
        
        Args:
            key: Configuration key
            default: Default value if not found or invalid JSON
            
        Returns:
            Dictionary value
        """
        if default is None:
            default = self.DEFAULTS.get(key, {})
            
        value = self.get(key)
        
        if value is None:
            return default
            
        if isinstance(value, dict):
            return value
            
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON value for {key}, using default")
                return default
                
        return default
        
    def set(self, key: str, value: Any) -> None:
        """
        Set configuration value (runtime only, doesn't persist).
        
        Args:
            key: Configuration key
            value: Value to set
        """
        os.environ[key] = str(value)
        self._cache[key] = value
        
    def validate(self) -> None:
        """Validate all required environment variables are set."""
        missing = []
        
        for var in self.REQUIRED_VARS:
            if not self.get(var):
                missing.append(var)
                
        if missing:
            error_msg = f"Missing required environment variables: {', '.join(missing)}"
            
            # Provide helpful setup instructions
            if 'GMAIL_APP_PASSWORD' in missing:
                error_msg += "\n\nNote: GMAIL_APP_PASSWORD must be an app-specific password, not your regular Gmail password."
                error_msg += "\nGenerate one at: https://myaccount.google.com/apppasswords"
                
            if self._secrets_loaded:
                error_msg += f"\n\nSecrets were loaded from external manager but some are still missing."
                
            raise ValueError(error_msg)
            
    def validate_connections(self) -> Dict[str, bool]:
        """
        Test connections to external services (optional diagnostic).
        
        Returns:
            Dictionary of service names and connection status
        """
        results = {}
        
        # Test Gmail IMAP
        try:
            import imaplib
            imap = imaplib.IMAP4_SSL(self.get('GMAIL_IMAP_SERVER', 'imap.gmail.com'))
            imap.login(self.get('GMAIL_USER'), self.get('GMAIL_APP_PASSWORD'))
            imap.logout()
            results['gmail'] = True
        except Exception as e:
            logger.debug(f"Gmail connection test failed: {e}")
            results['gmail'] = False
            
        # Test OpenAI
        try:
            from openai import OpenAI
            client = OpenAI(api_key=self.get('OPENAI_API_KEY'))
            # Just validate the key format, don't make an actual request
            results['openai'] = len(self.get('OPENAI_API_KEY', '')) > 20
        except Exception as e:
            logger.debug(f"OpenAI validation failed: {e}")
            results['openai'] = False
            
        # Add more service tests as needed
        
        return results
        
    def _log_config_status(self) -> None:
        """Log configuration status with sensitive data masked."""
        logger.info("Configuration loaded successfully")
        
        if logger.isEnabledFor(logging.DEBUG):
            config_status = {}
            
            for key in self.REQUIRED_VARS:
                value = self.get(key)
                if key in self.SENSITIVE_KEYS and value:
                    # Mask sensitive values
                    if len(value) > 8:
                        config_status[key] = f"{value[:4]}...{value[-4:]}"
                    else:
                        config_status[key] = "***"
                else:
                    config_status[key] = value if value else "NOT SET"
                    
            logger.debug(f"Configuration status: {json.dumps(config_status, indent=2)}")
            
    def export_safe_config(self) -> Dict[str, Any]:
        """
        Export non-sensitive configuration for debugging.
        
        Returns:
            Dictionary of safe configuration values
        """
        safe_config = {}
        
        for key, default in self.DEFAULTS.items():
            if key not in self.SENSITIVE_KEYS:
                safe_config[key] = self.get(key, default)
                
        return safe_config
        
    def reload(self) -> None:
        """Reload configuration from environment."""
        logger.info("Reloading configuration")
        self.load_env(reload=True)
        self.validate()
        self._log_config_status()