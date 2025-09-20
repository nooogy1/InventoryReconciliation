#!/usr/bin/env python3
"""
Main application with complete data validation and human review workflow.
"""

import os
import time
import logging
import sys
from datetime import datetime
from typing import Dict, List, Optional, Any
from enum import Enum

from src.config import Config
from src.gmail_client import GmailClient
from src.openai_parser import EmailParser, ParseStatus, ParseResult, DataCompleteness
from src.airtable_client import AirtableClient
from src.zoho_client import ZohoClient
from src.discord_notifier import DiscordNotifier

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('inventory_reconciliation.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class ProcessingStatus(Enum):
    """Status of record processing."""
    COMPLETE = "complete"
    INCOMPLETE = "incomplete"
    PENDING_REVIEW = "pending_review"
    REVIEW_COMPLETE = "review_complete"
    SYNCED = "synced"
    FAILED = "failed"


class InventoryReconciliationApp:
    """Main application orchestrator with complete data validation."""
    
    def __init__(self):
        """Initialize all service clients."""
        self.config = Config()
        self.gmail = GmailClient(self.config)
        self.parser = EmailParser(self.config)
        self.airtable = AirtableClient(self.config)
        self.zoho = ZohoClient(self.config)
        self.discord = DiscordNotifier(self.config)
        
        # Track records pending review
        self.pending_reviews = {}  # airtable_id: data
        self.processed_uids = set()
        
        # Statistics tracking
        self.stats = {
            'processed': 0,
            'complete_data': 0,
            'incomplete_data': 0,
            'synced_to_zoho': 0,
            'pending_review': 0,
            'errors': 0,
            'session_start': datetime.now()
        }
        
    def process_email(self, email_data: Dict) -> None:
        """
        Process email with complete data validation workflow.
        
        Workflow:
        1. Parse email
        2. Validate completeness
        3. Save to Airtable (always)
        4. If complete -> Sync to Zoho
        5. If incomplete -> Flag for human review
        """
        try:
            self.stats['processed'] += 1
            
            # Step 1: Parse email with OpenAI
            logger.info(f"Parsing email: {email_data['subject']}")
            parse_result = self.parser.parse_email(
                email_data['body'],
                email_data['subject']
            )
            
            # Check parse status
            if parse_result.status == ParseStatus.FAILED:
                logger.error(f"Failed to parse email: {', '.join(parse_result.errors)}")
                self.discord.send_error(
                    f"Failed to parse email: {email_data['subject'][:100]}",
                    {'errors': parse_result.errors}
                )
                self.stats['errors'] += 1
                return
                
            if parse_result.status == ParseStatus.UNKNOWN_TYPE:
                logger.info(f"Unknown email type, skipping: {email_data['subject']}")
                return
                
            # Extract parsed data
            parsed_data = parse_result.data
            if not parsed_data:
                logger.warning(f"No data extracted from email: {email_data['subject']}")
                self.stats['errors'] += 1
                return
                
            # Add email metadata (using sequence number instead of UID)
            parsed_data['email_seq_num'] = email_data.get('seq_num')
            parsed_data['email_date'] = email_data['date']
            parsed_data['parse_result'] = parse_result.to_dict()
            
            # Step 2: Check completeness
            transaction_type = parsed_data.get('type')
            
            if parse_result.completeness == DataCompleteness.COMPLETE:
                # Complete data workflow
                self._process_complete_data(parsed_data, parse_result, transaction_type)
                self.stats['complete_data'] += 1
                
            elif parse_result.completeness == DataCompleteness.INCOMPLETE:
                # Incomplete data workflow
                self._process_incomplete_data(parsed_data, parse_result, transaction_type)
                self.stats['incomplete_data'] += 1
                
            else:
                # Invalid data
                logger.error(f"Invalid data completeness: {parse_result.completeness}")
                self.stats['errors'] += 1
                
        except Exception as e:
            error_msg = f"Error processing email {email_data.get('uid')}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.discord.send_error(error_msg, email_data)
            self.stats['errors'] += 1
            
    def _process_complete_data(self, data: Dict, parse_result: ParseResult, transaction_type: str):
        """
        Process complete data:
        1. Save to Airtable
        2. Sync to Zoho
        3. Send success notification
        """
        try:
            # Step 1: Save to Airtable
            logger.info(f"Saving complete {transaction_type} to Airtable")
            data['processing_status'] = ProcessingStatus.COMPLETE.value
            data['requires_review'] = False
            data['completeness'] = parse_result.completeness.value
            
            if transaction_type == 'purchase':
                airtable_record = self.airtable.create_purchase(data)
            else:
                airtable_record = self.airtable.create_sale(data)
                
            airtable_id = airtable_record.get('id')
            
            # Step 2: Sync to Zoho
            logger.info(f"Syncing complete {transaction_type} to Zoho")
            zoho_result = self.zoho.process_complete_data(data, transaction_type)
            
            if zoho_result.get('success'):
                data['processing_status'] = ProcessingStatus.SYNCED.value
                self.stats['synced_to_zoho'] += 1
                
                # Update Airtable with sync status
                if airtable_id:
                    # self.airtable.update_record(airtable_id, {'zoho_synced': True})
                    pass
                    
                # Send success notification
                self._send_complete_success_notification(data, parse_result, zoho_result, transaction_type)
                
            else:
                # Zoho sync failed
                logger.error(f"Zoho sync failed: {zoho_result.get('errors')}")
                self._send_zoho_failure_notification(data, zoho_result, transaction_type)
                
        except Exception as e:
            logger.error(f"Error processing complete data: {e}")
            self.discord.send_error(f"Failed to process complete {transaction_type}: {str(e)}")
            
    def _process_incomplete_data(self, data: Dict, parse_result: ParseResult, transaction_type: str):
        """
        Process incomplete data:
        1. Save to Airtable with incomplete flag
        2. Do NOT sync to Zoho
        3. Send review required notification
        4. Track for review completion
        """
        try:
            # Step 1: Save to Airtable with incomplete status
            logger.info(f"Saving incomplete {transaction_type} to Airtable for review")
            data['processing_status'] = ProcessingStatus.INCOMPLETE.value
            data['requires_review'] = True
            data['completeness'] = parse_result.completeness.value
            data['missing_fields'] = parse_result.missing_fields
            
            if transaction_type == 'purchase':
                airtable_record = self.airtable.create_purchase(data)
            else:
                airtable_record = self.airtable.create_sale(data)
                
            airtable_id = airtable_record.get('id')
            
            # Track for review
            if airtable_id:
                self.pending_reviews[airtable_id] = {
                    'data': data,
                    'type': transaction_type,
                    'missing_fields': parse_result.missing_fields,
                    'created_at': datetime.now()
                }
                self.stats['pending_review'] += 1
                
            # Step 2: Send human review notification
            self._send_review_required_notification(
                data, 
                parse_result, 
                transaction_type,
                airtable_id
            )
            
        except Exception as e:
            logger.error(f"Error processing incomplete data: {e}")
            self.discord.send_error(f"Failed to process incomplete {transaction_type}: {str(e)}")
            
    def handle_review_complete(self, airtable_id: str):
        """
        Handle when human review is complete.
        Re-validate data and sync to Zoho if now complete.
        """
        try:
            if airtable_id not in self.pending_reviews:
                logger.warning(f"No pending review found for {airtable_id}")
                self.discord.send_warning(f"No pending review found for Airtable ID: {airtable_id}")
                return
                
            review_info = self.pending_reviews[airtable_id]
            transaction_type = review_info['type']
            
            # Fetch updated data from Airtable
            logger.info(f"Fetching updated data from Airtable for {airtable_id}")
            # updated_data = self.airtable.get_record(airtable_id, transaction_type)
            # For now, using the stored data (would need to implement get_record)
            updated_data = review_info['data']
            
            # Re-validate completeness
            validation_result = self._validate_data_completeness(updated_data, transaction_type)
            
            if validation_result['is_complete']:
                # Data is now complete, sync to Zoho
                logger.info(f"Review complete, syncing {transaction_type} to Zoho")
                
                zoho_result = self.zoho.process_complete_data(updated_data, transaction_type)
                
                if zoho_result.get('success'):
                    # Update status
                    updated_data['processing_status'] = ProcessingStatus.SYNCED.value
                    self.stats['synced_to_zoho'] += 1
                    
                    # Remove from pending
                    del self.pending_reviews[airtable_id]
                    self.stats['pending_review'] -= 1
                    
                    # Send success notification
                    self.discord.send_success(
                        f"✅ Review Complete & Synced to Zoho\n"
                        f"Type: {transaction_type}\n"
                        f"Order #: {updated_data.get('order_number', 'N/A')}\n"
                        f"Items processed: {len(zoho_result.get('items_processed', []))}\n"
                        f"Stock adjusted: {'Yes' if zoho_result.get('stock_adjusted') else 'No'}"
                    )
                else:
                    # Zoho sync failed after review
                    self.discord.send_error(
                        f"❌ Zoho sync failed after review\n"
                        f"Errors: {', '.join(zoho_result.get('errors', []))}"
                    )
            else:
                # Still incomplete after review
                self.discord.send_warning(
                    f"⚠️ Data still incomplete after review\n"
                    f"Missing fields: {', '.join(validation_result.get('missing_fields', []))}\n"
                    f"Please complete all required fields in Airtable"
                )
                
        except Exception as e:
            logger.error(f"Error handling review completion: {e}")
            self.discord.send_error(f"Failed to process review completion: {str(e)}")
            
    def _validate_data_completeness(self, data: Dict, transaction_type: str) -> Dict:
        """
        Validate if data has all required fields.
        
        Returns:
            Dict with is_complete flag and missing_fields list
        """
        missing_fields = []
        
        # Check required fields based on transaction type
        if transaction_type == 'purchase':
            # Required: date, vendor_name, items with (name, quantity, unit_price), taxes
            if not data.get('date'):
                missing_fields.append('date')
            if not data.get('vendor_name'):
                missing_fields.append('vendor_name')
            if data.get('taxes') is None:
                missing_fields.append('taxes')
                
            # Check items
            items = data.get('items', [])
            if not items:
                missing_fields.append('items')
            else:
                for i, item in enumerate(items):
                    if not item.get('name'):
                        missing_fields.append(f'item_{i+1}_name')
                    if item.get('quantity') is None:
                        missing_fields.append(f'item_{i+1}_quantity')
                    if item.get('unit_price') is None:
                        missing_fields.append(f'item_{i+1}_unit_price')
                        
        elif transaction_type == 'sale':
            # Required: date, channel, items with (name, quantity, sale_price), taxes
            if not data.get('date'):
                missing_fields.append('date')
            if not data.get('channel'):
                missing_fields.append('channel')
            if data.get('taxes') is None:
                missing_fields.append('taxes')
                
            # Check items
            items = data.get('items', [])
            if not items:
                missing_fields.append('items')
            else:
                for i, item in enumerate(items):
                    if not item.get('name'):
                        missing_fields.append(f'item_{i+1}_name')
                    if item.get('quantity') is None:
                        missing_fields.append(f'item_{i+1}_quantity')
                    if item.get('sale_price') is None:
                        missing_fields.append(f'item_{i+1}_sale_price')
                        
        return {
            'is_complete': len(missing_fields) == 0,
            'missing_fields': missing_fields
        }
        
    def _send_complete_success_notification(self, data: Dict, parse_result: ParseResult, 
                                           zoho_result: Dict, transaction_type: str):
        """Send success notification for complete data processed."""
        if transaction_type == 'purchase':
            message = (
                f"✅ **Purchase Successfully Processed**\n"
                f"Order #: {data.get('order_number', 'N/A')}\n"
                f"Vendor: {data.get('vendor_name', 'Unknown')}\n"
                f"Date: {data.get('date', 'N/A')}\n"
                f"Items: {len(data.get('items', []))}\n"
                f"Subtotal: ${data.get('subtotal', 0):.2f}\n"
                f"Tax: ${data.get('taxes', 0):.2f}\n"
                f"Total: ${data.get('total', 0):.2f}\n\n"
                f"**Zoho Update:**\n"
                f"• Stock Adjusted: {'✅' if zoho_result.get('stock_adjusted') else '❌'}\n"
                f"• Items Processed: {len(zoho_result.get('items_processed', []))}\n"
                f"• Adjustment ID: {zoho_result.get('adjustment_id', 'N/A')}"
            )
        else:
            message = (
                f"✅ **Sale Successfully Processed**\n"
                f"Order #: {data.get('order_number', 'N/A')}\n"
                f"Channel: {data.get('channel', 'Unknown')}\n"
                f"Date: {data.get('date', 'N/A')}\n"
                f"Items: {len(data.get('items', []))}\n"
                f"Subtotal: ${data.get('subtotal', 0):.2f}\n"
                f"Tax: ${data.get('taxes', 0):.2f}\n"
                f"Total: ${data.get('total', 0):.2f}\n\n"
                f"**Zoho Update:**\n"
                f"• Stock Adjusted: {'✅' if zoho_result.get('stock_adjusted') else '❌'}\n"
                f"• Items Processed: {len(zoho_result.get('items_processed', []))}\n"
                f"• Revenue: ${zoho_result.get('revenue', 0):.2f}\n"
                f"• COGS: ${zoho_result.get('cogs', 0):.2f}\n"
                f"• Adjustment ID: {zoho_result.get('adjustment_id', 'N/A')}"
            )
            
        # Add any warnings
        if zoho_result.get('warnings'):
            message += f"\n\n⚠️ **Warnings:**\n"
            for warning in zoho_result['warnings'][:3]:
                message += f"• {warning}\n"
                
        self.discord.send_success(message, title="Complete Data Processed")
        
    def _send_review_required_notification(self, data: Dict, parse_result: ParseResult, 
                                          transaction_type: str, airtable_id: str):
        """Send notification that human review is required."""
        # Build list of missing fields
        missing_fields_text = "\n".join([f"• {field}" for field in parse_result.missing_fields[:10]])
        
        # Build incomplete items summary
        incomplete_items_text = ""
        if parse_result.incomplete_items:
            for item_info in parse_result.incomplete_items[:5]:
                item = item_info['item']
                missing = item_info['missing_fields']
                incomplete_items_text += f"• {item.get('name', 'Unknown')}: Missing {', '.join(missing)}\n"
                
        message = (
            f"⚠️ **Human Review Required - Incomplete Data**\n\n"
            f"**Transaction Details:**\n"
            f"Type: {transaction_type.title()}\n"
            f"Order #: {data.get('order_number', 'N/A')}\n"
        )
        
        if transaction_type == 'purchase':
            message += f"Vendor: {data.get('vendor_name', 'MISSING')}\n"
        else:
            message += f"Channel: {data.get('channel', 'MISSING')}\n"
            
        message += (
            f"Date: {data.get('date', 'MISSING')}\n"
            f"Tax: ${data.get('taxes', 'MISSING')}\n\n"
            f"**Missing Required Fields:**\n{missing_fields_text}\n"
        )
        
        if incomplete_items_text:
            message += f"\n**Incomplete Items:**\n{incomplete_items_text}"
            
        message += (
            f"\n**Action Required:**\n"
            f"1. Open Airtable and locate record ID: {airtable_id}\n"
            f"2. Fill in all missing fields\n"
            f"3. Reply 'resolved {airtable_id}' when complete\n\n"
            f"⚠️ **Note:** Data will NOT be synced to Zoho until all required fields are complete."
        )
        
        self.discord.send_warning(message, title="Review Required")
        
    def _send_zoho_failure_notification(self, data: Dict, zoho_result: Dict, transaction_type: str):
        """Send notification when Zoho sync fails."""
        message = (
            f"❌ **Zoho Sync Failed**\n\n"
            f"Transaction Type: {transaction_type.title()}\n"
            f"Order #: {data.get('order_number', 'N/A')}\n"
            f"\n**Errors:**\n"
        )
        
        for error in zoho_result.get('errors', [])[:5]:
            message += f"• {error}\n"
            
        if zoho_result.get('items_failed'):
            message += f"\n**Failed Items:**\n"
            for item in zoho_result['items_failed'][:5]:
                message += f"• {item.get('name')}: {item.get('error')}\n"
                
        message += f"\n**Note:** Data has been saved to Airtable but NOT synced to Zoho inventory."
        
        self.discord.send_error(message, title="Zoho Sync Failed")
        
    def process_discord_command(self, command: str, args: List[str]):
        """
        Process Discord commands (would be called by Discord bot).
        
        Commands:
        - resolved <airtable_id>: Mark review as complete
        - status: Show current statistics
        - pending: Show pending reviews
        """
        try:
            if command.lower() == 'resolved' and args:
                airtable_id = args[0]
                self.handle_review_complete(airtable_id)
                
            elif command.lower() == 'status':
                self._send_status_report()
                
            elif command.lower() == 'pending':
                self._send_pending_reviews_report()
                
            else:
                self.discord.send_info(f"Unknown command: {command}")
                
        except Exception as e:
            logger.error(f"Error processing Discord command: {e}")
            self.discord.send_error(f"Command failed: {str(e)}")
            
    def _send_status_report(self):
        """Send current status report to Discord."""
        runtime = (datetime.now() - self.stats['session_start']).total_seconds()
        
        message = (
            f"📊 **System Status Report**\n\n"
            f"**Session Statistics:**\n"
            f"• Runtime: {runtime/3600:.2f} hours\n"
            f"• Emails Processed: {self.stats['processed']}\n"
            f"• Complete Data: {self.stats['complete_data']}\n"
            f"• Incomplete Data: {self.stats['incomplete_data']}\n"
            f"• Synced to Zoho: {self.stats['synced_to_zoho']}\n"
            f"• Pending Review: {self.stats['pending_review']}\n"
            f"• Errors: {self.stats['errors']}\n"
        )
        
        if self.stats['processed'] > 0:
            complete_rate = (self.stats['complete_data'] / self.stats['processed']) * 100
            sync_rate = (self.stats['synced_to_zoho'] / self.stats['processed']) * 100
            message += (
                f"\n**Rates:**\n"
                f"• Data Completeness: {complete_rate:.1f}%\n"
                f"• Sync Success: {sync_rate:.1f}%"
            )
            
        self.discord.send_info(message, title="Status Report")
        
    def _send_pending_reviews_report(self):
        """Send list of pending reviews to Discord."""
        if not self.pending_reviews:
            self.discord.send_info("No pending reviews", title="Pending Reviews")
            return
            
        message = f"📋 **Pending Reviews ({len(self.pending_reviews)} total)**\n\n"
        
        for airtable_id, info in list(self.pending_reviews.items())[:10]:
            age = (datetime.now() - info['created_at']).total_seconds() / 3600
            message += (
                f"**ID:** {airtable_id}\n"
                f"• Type: {info['type']}\n"
                f"• Age: {age:.1f} hours\n"
                f"• Missing: {', '.join(info['missing_fields'][:5])}\n\n"
            )
            
        if len(self.pending_reviews) > 10:
            message += f"... and {len(self.pending_reviews) - 10} more"
            
        self.discord.send_info(message, title="Pending Reviews")
        
    def run_once(self) -> None:
        """Run a single iteration of email processing."""
        try:
            logger.info("Checking for new emails...")
            
            # Example: Fetch emails with optional date filter
            # You can uncomment and modify these as needed:
            
            # Fetch all unread emails (default)
            new_emails = self.gmail.fetch_unread_emails()
            
            # Or fetch unread emails from the last 7 days
            # from datetime import datetime, timedelta
            # seven_days_ago = datetime.now() - timedelta(days=7)
            # new_emails = self.gmail.fetch_unread_emails(since_date=seven_days_ago)
            
            # Or fetch unread emails from a specific sender
            # new_emails = self.gmail.fetch_unread_emails(from_sender="noreply@zoho.com")
            
            # Or combine filters
            # new_emails = self.gmail.fetch_unread_emails(
            #     since_date=seven_days_ago,
            #     from_sender="zoho.com"
            # )
            
            if not new_emails:
                logger.debug("No new emails found")
                return
                
            logger.info(f"Found {len(new_emails)} new emails")
            
            for email in new_emails:
                seq_num = email.get('seq_num')
                if seq_num and seq_num not in self.processed_seq_nums:
                    self.process_email(email)
                    self.processed_seq_nums.add(seq_num)
                    self.gmail.mark_as_processed(seq_num)
                    
        except Exception as e:
            error_msg = f"Error in run cycle: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.discord.send_error(error_msg)
            
    def run(self) -> None:
        """Main run loop."""
        logger.info("Starting Inventory Reconciliation App with Complete Data Validation")
        
        # Send startup notification
        self.discord.send_info(
            "🚀 Inventory Reconciliation System Started\n"
            "Mode: Complete Data Validation\n"
            "Incomplete data will be flagged for human review",
            title="System Started"
        )
        
        poll_interval = self.config.get_int('POLL_INTERVAL')
        
        try:
            while True:
                try:
                    self.run_once()
                    
                    # Periodic status report
                    if self.stats['processed'] > 0 and self.stats['processed'] % 50 == 0:
                        self._send_status_report()
                        
                    logger.debug(f"Sleeping for {poll_interval} seconds")
                    time.sleep(poll_interval)
                    
                except KeyboardInterrupt:
                    logger.info("Shutdown requested")
                    break
                    
                except Exception as e:
                    logger.error(f"Unexpected error: {str(e)}", exc_info=True)
                    self.stats['errors'] += 1
                    time.sleep(poll_interval)
                    
        finally:
            # Cleanup
            logger.info("Cleaning up resources...")
            self.gmail.close()
            
            # Send final report
            self._send_status_report()
            
            # List any remaining pending reviews
            if self.pending_reviews:
                self._send_pending_reviews_report()
                
            self.discord.send_info("⏹️ System stopped", title="Shutdown")
            logger.info("Shutdown complete")


if __name__ == "__main__":
    app = InventoryReconciliationApp()
    app.run()