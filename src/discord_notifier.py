"""Enhanced Discord notifications for proper Purchase/Sales Order workflows."""

import logging
import requests
import json
from typing import Dict, Optional, Any, List
from datetime import datetime

logger = logging.getLogger(__name__)


class DiscordNotifier:
    """Enhanced Discord notifications with workflow-specific messaging."""
    
    def __init__(self, config):
        self.config = config
        self.webhook_url = config.get('DISCORD_WEBHOOK_URL')
        self.mention_on_error = config.get('DISCORD_MENTION_ON_ERROR')
        self.retry_on_fail = config.get_bool('DISCORD_RETRY_ON_FAIL', True)
        
        # Color codes for different message types
        self.colors = {
            'success': 0x28a745,      # Green
            'warning': 0xffc107,      # Yellow  
            'error': 0xdc3545,        # Red
            'info': 0x17a2b8,         # Blue
            'purchase': 0x6f42c1,     # Purple
            'sale': 0x20c997          # Teal
        }
        
        logger.info(f"📢 Discord notifier initialized")
        
    def test_webhook(self) -> bool:
        """Test Discord webhook connectivity."""
        if not self.webhook_url:
            logger.warning("⚠️ No Discord webhook URL configured")
            return False
            
        try:
            test_embed = {
                "title": "🔧 Connection Test",
                "description": "Discord webhook is working correctly",
                "color": self.colors['info'],
                "timestamp": datetime.utcnow().isoformat()
            }
            
            response = requests.post(
                self.webhook_url,
                json={"embeds": [test_embed]},
                timeout=10
            )
            
            if response.status_code == 204:
                logger.info("✅ Discord webhook test successful")
                return True
            else:
                logger.error(f"❌ Discord webhook test failed: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Discord webhook test failed: {e}")
            return False

    def send_success_notification(self, title: str, description: str, details: Dict[str, Any], 
                                extra_info: Optional[Dict] = None):
        """Send enhanced success notification with workflow details."""
        
        # Build fields from details
        fields = []
        for key, value in details.items():
            fields.append({
                "name": key,
                "value": str(value),
                "inline": True
            })
        
        # Add workflow steps if provided
        if extra_info and 'workflow_steps' in extra_info:
            fields.append({
                "name": "Workflow Steps",
                "value": extra_info['workflow_steps'],
                "inline": False
            })
        
        embed = {
            "title": title,
            "description": description,
            "color": self.colors['success'],
            "fields": fields,
            "timestamp": datetime.utcnow().isoformat(),
            "footer": {
                "text": "Inventory System - Proper Workflows"
            }
        }
        
        self._send_embed(embed)

    def send_purchase_order_success(self, po_data: Dict, zoho_result: Dict):
        """Send Purchase Order creation success notification."""
        
        order_number = po_data.get('order_number', 'Unknown')
        vendor = po_data.get('vendor_name', 'Unknown')
        
        fields = [
            {"name": "📋 PO Number", "value": zoho_result.get('purchase_order_id', 'Unknown'), "inline": True},
            {"name": "👥 Vendor", "value": vendor, "inline": True},
            {"name": "📦 Items", "value": len(zoho_result.get('items_processed', [])), "inline": True},
            {"name": "🧾 Bill Generated", "value": "✅ Yes" if zoho_result.get('bill_id') else "❌ No", "inline": True},
            {"name": "📈 Inventory Updated", "value": "✅ Yes", "inline": True},
            {"name": "💰 COGS Method", "value": "FIFO (Zoho Native)", "inline": True}
        ]
        
        # Add workflow steps
        workflow_steps = "\n".join([f"• {step}" for step in zoho_result.get('workflow_steps', [])])
        if workflow_steps:
            fields.append({
                "name": "🔄 Workflow Steps",
                "value": workflow_steps,
                "inline": False
            })
        
        embed = {
            "title": "✅ Purchase Order Created Successfully",
            "description": f"**Order:** {order_number}",
            "color": self.colors['purchase'],
            "fields": fields,
            "timestamp": datetime.utcnow().isoformat(),
            "footer": {
                "text": "Purchase Workflow Complete"
            }
        }
        
        self._send_embed(embed)

    def send_sales_order_success(self, so_data: Dict, zoho_result: Dict):
        """Send Sales Order creation success notification."""
        
        order_number = so_data.get('order_number', 'Unknown')
        channel = so_data.get('channel', 'Unknown')
        
        fields = [
            {"name": "📋 SO Number", "value": zoho_result.get('sales_order_id', 'Unknown'), "inline": True},
            {"name": "🏪 Channel", "value": channel, "inline": True},
            {"name": "📦 Items", "value": len(zoho_result.get('items_processed', [])), "inline": True},
            {"name": "🧾 Invoice Generated", "value": "✅ Yes" if zoho_result.get('invoice_id') else "❌ No", "inline": True},
            {"name": "📦 Shipment Created", "value": "✅ Yes" if zoho_result.get('shipment_id') else "❌ No", "inline": True},
            {"name": "💵 Revenue", "value": f"${zoho_result.get('revenue', 0):.2f}", "inline": True},
            {"name": "💰 COGS", "value": f"${zoho_result.get('cogs', 0):.2f}", "inline": True},
            {"name": "📈 Profit", "value": f"${zoho_result.get('revenue', 0) - zoho_result.get('cogs', 0):.2f}", "inline": True}
        ]
        
        # Add workflow steps
        workflow_steps = "\n".join([f"• {step}" for step in zoho_result.get('workflow_steps', [])])
        if workflow_steps:
            fields.append({
                "name": "🔄 Workflow Steps",
                "value": workflow_steps,
                "inline": False
            })
        
        embed = {
            "title": "✅ Sales Order Created Successfully",
            "description": f"**Order:** {order_number}",
            "color": self.colors['sale'],
            "fields": fields,
            "timestamp": datetime.utcnow().isoformat(),
            "footer": {
                "text": "Sales Workflow Complete"
            }
        }
        
        self._send_embed(embed)

    def send_error_notification(self, title: str, description: str, details: Dict[str, Any]):
        """Send error notification with action items."""
        
        # Add mention if configured
        content = ""
        if self.mention_on_error:
            content = f"<@{self.mention_on_error}>"
        
        fields = []
        for key, value in details.items():
            # Handle lists and complex objects
            if isinstance(value, list):
                value = "\n".join([f"• {item}" for item in value[:5]])  # Limit to 5 items
                if len(details.get(key, [])) > 5:
                    value += f"\n... and {len(details[key]) - 5} more"
            elif isinstance(value, dict):
                value = json.dumps(value, indent=2)[:1000]  # Limit length
            
            fields.append({
                "name": key,
                "value": str(value)[:1024],  # Discord field value limit
                "inline": True if len(str(value)) < 50 else False
            })
        
        embed = {
            "title": title,
            "description": description,
            "color": self.colors['error'],
            "fields": fields,
            "timestamp": datetime.utcnow().isoformat(),
            "footer": {
                "text": "Error - Action Required"
            }
        }
        
        self._send_embed(embed, content)

    def send_workflow_error(self, transaction_type: str, order_number: str, workflow_stage: str, 
                          error_details: Dict, context: Dict):
        """Send workflow-specific error notification."""
        
        content = f"<@{self.mention_on_error}>" if self.mention_on_error else ""
        
        if transaction_type == 'purchase':
            title = "❌ Purchase Workflow Failed"
            color = self.colors['purchase']
            emoji = "🛒"
        else:
            title = "❌ Sales Workflow Failed"
            color = self.colors['sale']
            emoji = "🛍️"
        
        fields = [
            {"name": f"{emoji} Order", "value": order_number, "inline": True},
            {"name": "🚫 Failed Stage", "value": workflow_stage, "inline": True},
            {"name": "📊 Data Status", "value": "✅ Saved to Airtable", "inline": True}
        ]
        
        # Add context fields
        for key, value in context.items():
            fields.append({
                "name": key,
                "value": str(value),
                "inline": True
            })
        
        # Add error details
        error_text = "\n".join([f"• {error}" for error in error_details.get('errors', ['Unknown error'])[:3]])
        fields.append({
            "name": "🔍 Error Details",
            "value": error_text,
            "inline": False
        })
        
        # Add action items
        action_text = """
**Action Required:**
1. Check Zoho API connectivity
2. Verify vendor/customer exists
3. Review item configurations
4. Retry from Airtable if needed
        """
        
        fields.append({
            "name": "⚠️ Next Steps",
            "value": action_text,
            "inline": False
        })
        
        embed = {
            "title": title,
            "description": f"Failed to create {transaction_type} workflow in Zoho",
            "color": color,
            "fields": fields,
            "timestamp": datetime.utcnow().isoformat(),
            "footer": {
                "text": "Workflow Error - Manual Review Required"
            }
        }
        
        self._send_embed(embed, content)

    def send_warning_notification(self, title: str, description: str, details: Dict[str, Any], 
                                extra_info: Optional[Dict] = None):
        """Send warning notification."""
        
        fields = []
        for key, value in details.items():
            if isinstance(value, list):
                value = "\n".join([f"• {item}" for item in value[:5]])
            
            fields.append({
                "name": key,
                "value": str(value),
                "inline": True if len(str(value)) < 50 else False
            })
        
        # Add extra info if provided
        if extra_info:
            for key, value in extra_info.items():
                fields.append({
                    "name": key,
                    "value": str(value),
                    "inline": False
                })
        
        embed = {
            "title": title,
            "description": description,
            "color": self.colors['warning'],
            "fields": fields,
            "timestamp": datetime.utcnow().isoformat(),
            "footer": {
                "text": "Warning - Review Recommended"
            }
        }
        
        self._send_embed(embed)

    def send_human_review_notification(self, transaction_type: str, order_number: str, 
                                     missing_fields: List[str], record_id: str, confidence: float):
        """Send human review required notification."""
        
        fields = [
            {"name": "📋 Order", "value": order_number, "inline": True},
            {"name": "📊 Type", "value": transaction_type.title(), "inline": True},
            {"name": "🎯 Confidence", "value": f"{confidence:.1%}", "inline": True},
            {"name": "❌ Missing Fields", "value": "\n".join([f"• {field}" for field in missing_fields]), "inline": False},
            {"name": "🗃️ Airtable Record", "value": record_id, "inline": True}
        ]
        
        action_text = f"""
**Action Required:**
1. Open Airtable record: `{record_id}`
2. Fill missing fields: {', '.join(missing_fields)}
3. Uncheck 'Requires Review' when complete
4. System will auto-process within 5 minutes
        """
        
        fields.append({
            "name": "⚡ Action Steps",
            "value": action_text,
            "inline": False
        })
        
        embed = {
            "title": "⚠️ Human Review Required - Incomplete Data",
            "description": f"Missing required fields for {transaction_type}",
            "color": self.colors['warning'],
            "fields": fields,
            "timestamp": datetime.utcnow().isoformat(),
            "footer": {
                "text": "Human Review Required"
            }
        }
        
        self._send_embed(embed)

    def send_info_notification(self, title: str, description: str, details: Dict[str, Any]):
        """Send informational notification."""
        
        fields = []
        for key, value in details.items():
            fields.append({
                "name": key,
                "value": str(value),
                "inline": True
            })
        
        embed = {
            "title": title,
            "description": description,
            "color": self.colors['info'],
            "fields": fields,
            "timestamp": datetime.utcnow().isoformat(),
            "footer": {
                "text": "System Information"
            }
        }
        
        self._send_embed(embed)

    def send_validation_alert(self, validation_type: str, findings: Dict):
        """Send validation check alerts."""
        
        if validation_type == "inventory_adjustments":
            auto_adjustments = findings.get('auto_adjustments', 0)
            
            if auto_adjustments > 0:
                fields = [
                    {"name": "🔍 Total Adjustments", "value": findings.get('total_adjustments', 0), "inline": True},
                    {"name": "⚠️ Auto-Generated", "value": auto_adjustments, "inline": True},
                    {"name": "✅ Expected", "value": "0 (with proper workflows)", "inline": True}
                ]
                
                if findings.get('auto_adjustment_ids'):
                    adj_list = "\n".join([f"• {adj_id}" for adj_id in findings['auto_adjustment_ids'][:5]])
                    fields.append({
                        "name": "📋 Adjustment IDs",
                        "value": adj_list,
                        "inline": False
                    })
                
                embed = {
                    "title": "⚠️ Inventory Adjustments Found",
                    "description": "Found auto-generated adjustments. With proper workflows, this should be zero.",
                    "color": self.colors['warning'],
                    "fields": fields,
                    "timestamp": datetime.utcnow().isoformat(),
                    "footer": {
                        "text": "Validation Alert - Review Required"
                    }
                }
                
                self._send_embed(embed)
        
        elif validation_type == "inventory_sync":
            discrepancies = findings.get('discrepancies', [])
            
            if discrepancies:
                fields = [
                    {"name": "🔍 Items Compared", "value": findings.get('items_compared', 0), "inline": True},
                    {"name": "⚠️ Discrepancies", "value": len(discrepancies), "inline": True},
                    {"name": "💰 Value Difference", "value": f"${findings.get('total_value_difference', 0):.2f}", "inline": True}
                ]
                
                # Show first few discrepancies
                if discrepancies:
                    disc_list = "\n".join([f"• {disc['sku']}: AT={disc['airtable_qty']} ZO={disc['zoho_qty']}" 
                                         for disc in discrepancies[:5]])
                    fields.append({
                        "name": "📋 Sample Discrepancies",
                        "value": disc_list,
                        "inline": False
                    })
                
                embed = {
                    "title": "⚠️ Inventory Sync Discrepancies",
                    "description": "Found differences between Airtable and Zoho inventory levels",
                    "color": self.colors['warning'],
                    "fields": fields,
                    "timestamp": datetime.utcnow().isoformat(),
                    "footer": {
                        "text": "Sync Validation - Review Required"
                    }
                }
                
                self._send_embed(embed)

    def _send_embed(self, embed: Dict, content: str = ""):
        """Send Discord embed message."""
        if not self.webhook_url:
            logger.warning("⚠️ No Discord webhook URL - skipping notification")
            return
        
        payload = {"embeds": [embed]}
        if content:
            payload["content"] = content
        
        try:
            response = requests.post(
                self.webhook_url,
                json=payload,
                timeout=10
            )
            
            if response.status_code == 204:
                logger.debug("✅ Discord notification sent successfully")
            else:
                logger.error(f"❌ Discord notification failed: {response.status_code} - {response.text}")
                
                if self.retry_on_fail and response.status_code != 429:  # Don't retry rate limits
                    logger.info("🔄 Retrying Discord notification...")
                    time.sleep(2)
                    requests.post(self.webhook_url, json=payload, timeout=10)
                    
        except Exception as e:
            logger.error(f"💥 Failed to send Discord notification: {e}")
            if self.retry_on_fail:
                try:
                    logger.info("🔄 Retrying Discord notification after error...")
                    time.sleep(5)
                    requests.post(self.webhook_url, json=payload, timeout=10)
                except:
                    logger.error("💥 Discord retry also failed")

    # ===========================================
    # LEGACY METHODS (For Backward Compatibility)
    # ===========================================

    def send_purchase_success(self, data: Dict, result: Dict):
        """Legacy method - redirects to new enhanced method."""
        self.send_purchase_order_success(data, result)

    def send_sale_success(self, data: Dict, result: Dict):
        """Legacy method - redirects to new enhanced method."""  
        self.send_sales_order_success(data, result)

    def send_processing_error(self, error_type: str, message: str, details: Dict):
        """Legacy method - redirects to new enhanced method."""
        self.send_error_notification(f"❌ {error_type}", message, details)