"""Discord notification client with robust error handling and formatting."""

import logging
import requests
import json
import asyncio
import httpx
from typing import Dict, Optional, List, Any, Union
from datetime import datetime, timezone
from collections.abc import Mapping, Iterable
from enum import Enum

logger = logging.getLogger(__name__)


class NotificationLevel(Enum):
    """Notification severity levels."""
    SUCCESS = ("✅", 0x00ff00)  # Green
    WARNING = ("⚠️", 0xffaa00)  # Orange
    ERROR = ("❌", 0xff0000)    # Red
    INFO = ("ℹ️", 0x0099ff)     # Blue
    DEBUG = ("🔍", 0x666666)    # Gray


class DiscordNotifier:
    """Send notifications to Discord webhook with robust formatting."""
    
    # Discord limits
    MAX_DESCRIPTION_LENGTH = 2048
    MAX_FIELD_VALUE_LENGTH = 1024
    MAX_FIELD_NAME_LENGTH = 256
    MAX_FIELDS = 25
    MAX_EMBED_TOTAL = 6000
    MAX_EMBEDS_PER_MESSAGE = 10
    
    def __init__(self, config):
        """
        Initialize Discord notifier with validation.
        
        Args:
            config: Configuration object
            
        Raises:
            ValueError: If webhook URL is missing or invalid
        """
        self.webhook_url = config.get('DISCORD_WEBHOOK_URL')
        
        # Validate webhook URL
        if not self.webhook_url:
            raise ValueError("Discord webhook URL is missing from configuration")
            
        if not self.webhook_url.startswith('https://discord.com/api/webhooks/'):
            raise ValueError(f"Invalid Discord webhook URL format: {self.webhook_url}")
            
        # Configuration
        self.retry_on_fail = config.get_bool('DISCORD_RETRY_ON_FAIL', True)
        self.max_retries = config.get_int('DISCORD_MAX_RETRIES', 3)
        self.retry_delay = config.get_int('DISCORD_RETRY_DELAY', 1)
        self.include_timestamp = config.get_bool('DISCORD_INCLUDE_TIMESTAMP', True)
        self.footer_text = config.get('DISCORD_FOOTER_TEXT', 'Inventory Reconciliation System')
        self.mention_on_error = config.get('DISCORD_MENTION_ON_ERROR')  # User/role ID to mention
        
        # Rate limiting
        self._last_sent = None
        self._min_interval = config.get_float('DISCORD_MIN_INTERVAL', 0.5)  # Min seconds between messages
        
        logger.info("Discord notifier initialized successfully")
        
    def send_success(self, message: str, data: Optional[Dict] = None, title: Optional[str] = None):
        """Send success notification to Discord."""
        self._send_notification(
            level=NotificationLevel.SUCCESS,
            message=message,
            data=data,
            title=title or "Success"
        )
        
    def send_error(self, message: str, data: Optional[Dict] = None, title: Optional[str] = None):
        """Send error notification to Discord with optional mention."""
        # Add mention for errors if configured
        content = None
        if self.mention_on_error:
            content = f"<@{self.mention_on_error}> Error detected!"
            
        self._send_notification(
            level=NotificationLevel.ERROR,
            message=message,
            data=data,
            title=title or "Error",
            content=content
        )
        
    def send_warning(self, message: str, data: Optional[Dict] = None, title: Optional[str] = None):
        """Send warning notification to Discord."""
        self._send_notification(
            level=NotificationLevel.WARNING,
            message=message,
            data=data,
            title=title or "Warning"
        )
        
    def send_info(self, message: str, data: Optional[Dict] = None, title: Optional[str] = None):
        """Send info notification to Discord."""
        self._send_notification(
            level=NotificationLevel.INFO,
            message=message,
            data=data,
            title=title or "Information"
        )
        
    def send_debug(self, message: str, data: Optional[Dict] = None):
        """Send debug notification (only if debug logging is enabled)."""
        if logger.isEnabledFor(logging.DEBUG):
            self._send_notification(
                level=NotificationLevel.DEBUG,
                message=message,
                data=data,
                title="Debug"
            )
            
    def _send_notification(self, level: NotificationLevel, message: str, 
                          data: Optional[Dict] = None, title: Optional[str] = None,
                          content: Optional[str] = None):
        """
        Internal method to send notification with proper formatting.
        
        Args:
            level: Notification severity level
            message: Main message text
            data: Optional data dictionary for fields
            title: Optional embed title override
            content: Optional message content (outside embed)
        """
        emoji, color = level.value
        
        # Build embed
        embed = {
            "title": f"{emoji} {title}",
            "description": self._truncate_text(message, self.MAX_DESCRIPTION_LENGTH),
            "color": color
        }
        
        # Add timestamp if enabled
        if self.include_timestamp:
            # Proper ISO8601 format with Z suffix for UTC
            embed["timestamp"] = datetime.now(timezone.utc).isoformat()
            
        # Add footer
        if self.footer_text:
            embed["footer"] = {
                "text": self.footer_text
            }
            
        # Add fields from data
        if data:
            fields = self._create_fields(data)
            if fields:
                embed["fields"] = fields[:self.MAX_FIELDS]  # Discord limit
                
        # Validate total embed size
        embed = self._validate_embed_size(embed)
        
        # Build payload
        payload = {"embeds": [embed]}
        if content:
            payload["content"] = content[:2000]  # Discord content limit
            
        # Apply rate limiting
        self._apply_rate_limit()
        
        # Send webhook
        self._send_webhook(payload, level)
        
    def _create_fields(self, data: Dict) -> List[Dict]:
        """
        Create embed fields from data dictionary.
        
        Args:
            data: Data dictionary
            
        Returns:
            List of field dictionaries
        """
        fields = []
        
        for key, value in data.items():
            # Skip certain keys
            if key in ['items', 'body', 'parse_result', 'parse_metadata']:
                continue
                
            # Format the field name
            field_name = self._format_field_name(key)
            
            # Format the value based on type
            field_value = self._format_field_value(value)
            
            # Skip empty values
            if not field_value or field_value == "N/A":
                continue
                
            fields.append({
                "name": self._truncate_text(field_name, self.MAX_FIELD_NAME_LENGTH),
                "value": self._truncate_text(field_value, self.MAX_FIELD_VALUE_LENGTH),
                "inline": True
            })
            
        return fields
        
    def _format_field_name(self, key: str) -> str:
        """Format field name for display."""
        # Convert snake_case to Title Case
        formatted = key.replace('_', ' ').title()
        
        # Handle special cases
        replacements = {
            'Uid': 'UID',
            'Id': 'ID',
            'Url': 'URL',
            'Api': 'API',
            'Sku': 'SKU',
            'Po': 'PO',
            'So': 'SO'
        }
        
        for old, new in replacements.items():
            formatted = formatted.replace(old, new)
            
        return formatted
        
    def _format_field_value(self, value: Any) -> str:
        """
        Format field value for display.
        
        Args:
            value: Value to format
            
        Returns:
            Formatted string
        """
        if value is None:
            return "N/A"
            
        # Handle different types
        if isinstance(value, bool):
            return "✅ Yes" if value else "❌ No"
            
        elif isinstance(value, (int, float)):
            # Format numbers nicely
            if isinstance(value, float):
                # Check if it's a percentage (0-1 range confidence scores)
                if 0 <= value <= 1 and 'confidence' in str(value):
                    return f"{value:.1%}"
                # Currency formatting for large numbers
                elif value >= 1000:
                    return f"${value:,.2f}"
                else:
                    return f"{value:.2f}"
            else:
                return f"{value:,}"
                
        elif isinstance(value, str):
            return value
            
        elif isinstance(value, datetime):
            # Format datetime nicely
            return value.strftime("%Y-%m-%d %H:%M:%S UTC")
            
        elif isinstance(value, list):
            # Format lists nicely
            if len(value) == 0:
                return "Empty"
            elif len(value) <= 3:
                return ", ".join(str(item) for item in value)
            else:
                return f"{', '.join(str(item) for item in value[:3])}, ... ({len(value)} total)"
                
        elif isinstance(value, dict):
            # Format nested dicts as JSON
            try:
                formatted = json.dumps(value, indent=2, default=str)
                if len(formatted) > 100:
                    # Truncate long JSON
                    return formatted[:97] + "..."
                return f"```json\n{formatted}\n```"
            except:
                return str(value)
                
        else:
            # Fallback to string representation
            return str(value)
            
    def _truncate_text(self, text: str, max_length: int) -> str:
        """
        Truncate text to maximum length with ellipsis.
        
        Args:
            text: Text to truncate
            max_length: Maximum allowed length
            
        Returns:
            Truncated text
        """
        if len(text) <= max_length:
            return text
            
        # Leave room for ellipsis
        return text[:max_length - 3] + "..."
        
    def _validate_embed_size(self, embed: Dict) -> Dict:
        """
        Validate and trim embed to Discord's size limits.
        
        Args:
            embed: Embed dictionary
            
        Returns:
            Validated embed
        """
        # Calculate total size
        total_size = 0
        
        if 'title' in embed:
            total_size += len(embed['title'])
        if 'description' in embed:
            total_size += len(embed['description'])
        if 'footer' in embed and 'text' in embed['footer']:
            total_size += len(embed['footer']['text'])
        if 'author' in embed and 'name' in embed['author']:
            total_size += len(embed['author']['name'])
            
        # Check fields
        if 'fields' in embed:
            for field in embed['fields']:
                total_size += len(field.get('name', ''))
                total_size += len(field.get('value', ''))
                
                # If we're over the limit, start trimming fields
                if total_size > self.MAX_EMBED_TOTAL:
                    # Remove this and remaining fields
                    idx = embed['fields'].index(field)
                    embed['fields'] = embed['fields'][:idx]
                    embed['fields'].append({
                        'name': 'Note',
                        'value': f'... and {len(embed["fields"]) - idx} more fields (truncated)',
                        'inline': False
                    })
                    break
                    
        return embed
        
    def _apply_rate_limit(self):
        """Apply rate limiting to avoid Discord rate limits."""
        if self._last_sent is not None:
            elapsed = (datetime.now() - self._last_sent).total_seconds()
            if elapsed < self._min_interval:
                import time
                time.sleep(self._min_interval - elapsed)
                
        self._last_sent = datetime.now()
        
    def _send_webhook(self, payload: Dict, level: NotificationLevel = NotificationLevel.INFO):
        """
        Send payload to Discord webhook with retry logic.
        
        Args:
            payload: Webhook payload
            level: Notification level for logging
        """
        attempt = 0
        last_error = None
        
        while attempt < self.max_retries:
            try:
                response = requests.post(
                    self.webhook_url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=10
                )
                
                if response.status_code == 204:
                    # Success with no content
                    logger.debug(f"Discord notification sent successfully ({level.name})")
                    return
                    
                elif response.status_code == 200:
                    # Success with content (shouldn't happen for webhooks but handle it)
                    logger.debug(f"Discord notification sent successfully ({level.name})")
                    return
                    
                elif response.status_code == 429:
                    # Rate limited
                    retry_after = response.json().get('retry_after', 1)
                    logger.warning(f"Discord rate limit hit, retrying after {retry_after}s")
                    import time
                    time.sleep(retry_after)
                    attempt += 1
                    continue
                    
                elif response.status_code >= 400:
                    # Client error
                    error_msg = f"Discord webhook failed: {response.status_code} - {response.text}"
                    logger.error(error_msg)
                    last_error = error_msg
                    
                    if not self.retry_on_fail:
                        break
                        
                    attempt += 1
                    if attempt < self.max_retries:
                        import time
                        time.sleep(self.retry_delay * attempt)
                        
            except requests.exceptions.Timeout:
                last_error = "Discord webhook timeout"
                logger.warning(last_error)
                attempt += 1
                
            except requests.exceptions.ConnectionError as e:
                last_error = f"Discord connection error: {e}"
                logger.warning(last_error)
                attempt += 1
                
            except Exception as e:
                last_error = f"Unexpected error sending Discord notification: {e}"
                logger.error(last_error, exc_info=True)
                break
                
        if last_error:
            logger.error(f"Failed to send Discord notification after {attempt} attempts: {last_error}")
            
    async def send_async(self, level: NotificationLevel, message: str, 
                        data: Optional[Dict] = None, title: Optional[str] = None):
        """
        Async version of send notification.
        
        Args:
            level: Notification severity level
            message: Main message text
            data: Optional data dictionary
            title: Optional title override
        """
        emoji, color = level.value
        
        # Build embed (same as sync version)
        embed = {
            "title": f"{emoji} {title or level.name.title()}",
            "description": self._truncate_text(message, self.MAX_DESCRIPTION_LENGTH),
            "color": color,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        if self.footer_text:
            embed["footer"] = {"text": self.footer_text}
            
        if data:
            fields = self._create_fields(data)
            if fields:
                embed["fields"] = fields[:self.MAX_FIELDS]
                
        embed = self._validate_embed_size(embed)
        payload = {"embeds": [embed]}
        
        # Send using async client
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    self.webhook_url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=10
                )
                
                if response.status_code not in [200, 204]:
                    logger.error(f"Discord async webhook failed: {response.status_code}")
                    
            except Exception as e:
                logger.error(f"Async Discord notification failed: {e}")
                
    def send_batch_summary(self, successes: int, warnings: int, errors: int, 
                          details: Optional[Dict] = None):
        """
        Send a batch processing summary.
        
        Args:
            successes: Number of successful operations
            warnings: Number of warnings
            errors: Number of errors
            details: Optional additional details
        """
        # Determine overall status
        if errors > 0:
            level = NotificationLevel.ERROR
            title = "Batch Processing Failed"
        elif warnings > 0:
            level = NotificationLevel.WARNING
            title = "Batch Processing Complete with Warnings"
        else:
            level = NotificationLevel.SUCCESS
            title = "Batch Processing Complete"
            
        # Build summary message
        message = f"Processed batch with:\n"
        message += f"✅ {successes} successful\n"
        if warnings > 0:
            message += f"⚠️ {warnings} warnings\n"
        if errors > 0:
            message += f"❌ {errors} errors\n"
            
        # Add details if provided
        summary_data = {
            "Total Processed": successes + warnings + errors,
            "Success Rate": f"{(successes / (successes + warnings + errors) * 100):.1f}%" if (successes + warnings + errors) > 0 else "N/A"
        }
        
        if details:
            summary_data.update(details)
            
        self._send_notification(
            level=level,
            message=message,
            data=summary_data,
            title=title
        )