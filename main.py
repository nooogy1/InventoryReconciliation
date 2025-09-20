#!/usr/bin/env python3
"""
Main application with complete data validation and human review workflow.
Enhanced with detailed pipeline logging.
"""

import os
import time
import logging
import sys
from datetime import datetime
from typing import Dict, List, Optional, Any, Set
from enum import Enum

from src.config import Config
from src.gmail_client import GmailClient
from src.openai_parser import EmailParser, ParseStatus, ParseResult, DataCompleteness
from src.airtable_client import AirtableClient
from src.zoho_client import ZohoClient
from src.discord_notifier import DiscordNotifier

# Configure logging with more detailed formatting
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s',
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
        logger.info("🚀 Initializing Inventory Reconciliation System...")
        
        self.config = Config()
        logger.info("✅ Configuration loaded successfully")
        
        # Initialize clients with detailed logging
        try:
            self.gmail = GmailClient(self.config)
            logger.info("✅ Gmail client initialized")
        except Exception as e:
            logger.error(f"❌ Gmail client initialization failed: {e}")
            raise
            
        try:
            self.parser = EmailParser(self.config)
            logger.info(f"✅ OpenAI parser initialized (model: {self.parser.model})")
        except Exception as e:
            logger.error(f"❌ OpenAI parser initialization failed: {e}")
            raise
            
        try:
            self.airtable = AirtableClient(self.config)
            logger.info("✅ Airtable client initialized")
        except Exception as e:
            logger.error(f"❌ Airtable client initialization failed: {e}")
            raise
            
        try:
            self.zoho = ZohoClient(self.config)
            logger.info("✅ Zoho client initialized")
        except Exception as e:
            logger.error(f"❌ Zoho client initialization failed: {e}")
            raise
            
        try:
            self.discord = DiscordNotifier(self.config)
            logger.info("✅ Discord notifier initialized")
        except Exception as e:
            logger.error(f"❌ Discord notifier initialization failed: {e}")
            raise
        
        # Track records pending review
        self.pending_reviews = {}  # airtable_id: data
        
        # Track processed emails by sequence number
        self.processed_seq_nums: Set[str] = set()
        
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
        
        logger.info("🎉 All components initialized successfully!")
        
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
        seq_num = email_data.get('seq_num', 'unknown')
        subject = email_data.get('subject', 'No Subject')[:100]
        
        logger.info(f"📧 Processing email [seq={seq_num}]: {subject}")
        
        try:
            self.stats['processed'] += 1
            
            # Step 1: Parse email with OpenAI
            logger.info(f"🤖 Parsing email with OpenAI...")
            parse_start = time.time()
            
            parse_result = self.parser.parse_email(
                email_data['body'],
                email_data['subject']
            )
            
            parse_duration = time.time() - parse_start
            logger.info(f"⏱️ OpenAI parsing completed in {parse_duration:.2f}s")
            
            # Check parse status
            if parse_result.status == ParseStatus.FAILED:
                logger.error(f"❌ OpenAI parsing failed: {', '.join(parse_result.errors)}")
                self.discord.send_error(
                    f"Failed to parse email: {subject}",
                    {'errors': parse_result.errors, 'seq_num': seq_num}
                )
                self.stats['errors'] += 1
                return
                
            if parse_result.status == ParseStatus.UNKNOWN_TYPE:
                logger.info(f"❓ Unknown email type, skipping: {subject}")
                return
                
            # Extract parsed data
            parsed_data = parse_result.data
            if not parsed_data:
                logger.warning(f"⚠️ No data extracted from email: {subject}")
                self.stats['errors'] += 1
                return
                
            transaction_type = parsed_data.get('type', 'unknown')
            order_number = parsed_data.get('order_number', 'N/A')
            confidence = parse_result.confidence_score
            
            logger.info(f"📊 Parse Results:")
            logger.info(f"  - Type: {transaction_type}")
            logger.info(f"  - Order: {order_number}")
            logger.info(f"  - Status: {parse_result.status.value}")
            logger.info(f"  - Completeness: {parse_result.completeness.value}")
            logger.info(f"  - Confidence: {confidence:.2f}")
            
            if parse_result.missing_fields:
                logger.info(f"  - Missing fields: {', '.join(parse_result.missing_fields)}")
            
            # Add email metadata (using sequence number instead of UID)
            parsed_data['email_seq_num'] = seq_num
            parsed_data['email_date'] = email_data['date']
            parsed_data['parse_result'] = parse_result.to_dict()
            
            # Step 2: Check completeness and route accordingly
            if parse_result.completeness == DataCompleteness.COMPLETE:
                logger.info(f"✅ Data is COMPLETE - proceeding with full pipeline")
                self._process_complete_data(parsed_data, parse_result, transaction_type)
                self.stats['complete_data'] += 1
                
            elif parse_result.completeness == DataCompleteness.INCOMPLETE:
                logger.info(f"⚠️ Data is INCOMPLETE - saving to Airtable for review")
                self._process_incomplete_data(parsed_data, parse_result, transaction_type)
                self.stats['incomplete_data'] += 1
                
            else:
                logger.error(f"❌ Invalid data completeness: {parse_result.completeness}")
                self.stats['errors'] += 1
                
        except Exception as e:
            error_msg = f"💥 Error processing email [seq={seq_num}]: {str(e)}"
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
        order_number = data.get('order_number', 'N/A')
        
        try:
            # Step 1: Save to Airtable
            logger.info(f"💾 Saving complete {transaction_type} to Airtable...")
            airtable_start = time.time()
            
            data['processing_status'] = ProcessingStatus.COMPLETE.value
            data['requires_review'] = False
            data['completeness'] = parse_result.completeness.value
            
            if transaction_type == 'purchase':
                airtable_record = self.airtable.create_purchase(data)
            else:
                airtable_record = self.airtable.create_sale(data)
                
            airtable_duration = time.time() - airtable_start
            airtable_id = airtable_record.get('id')
            
            logger.info(f"✅ Airtable save completed in {airtable_duration:.2f}s")
            logger.info(f"   - Record ID: {airtable_id}")
            
            # Step 2: Sync to Zoho
            logger.info(f"🔄 Syncing complete {transaction_type} to Zoho Inventory...")
            zoho_start = time.time()
            
            zoho_result = self.zoho.process_complete_data(data, transaction_type)
            zoho_duration = time.time() - zoho_start
            
            logger.info(f"⏱️ Zoho sync completed in {zoho_duration:.2f}s")
            
            if zoho_result.get('success'):
                data['processing_status'] = ProcessingStatus.SYNCED.value
                self.stats['synced_to_zoho'] += 1
                
                logger.info(f"✅ Zoho sync SUCCESS:")
                logger.info(f"   - Stock adjusted: {zoho_result.get('stock_adjusted', False)}")
                logger.info(f"   - Items processed: {len(zoho_result.get('items_processed', []))}")
                logger.info(f"   - Adjustment ID: {zoho_result.get('adjustment_id', 'N/A')}")
                
                if zoho_result.get('warnings'):
                    logger.warning(f"⚠️ Zoho warnings: {'; '.join(zoho_result['warnings'][:3])}")
                
                # Send success notification
                self._send_complete_success_notification(data, parse_result, zoho_result, transaction_type)
                
            else:
                logger.error(f"❌ Zoho sync FAILED:")
                for error in zoho_result.get('errors', [])[:3]:
                    logger.error(f"   - {error}")
                    
                if zoho_result.get('items_failed'):
                    logger.error(f"   - Failed items: {len(zoho_result['items_failed'])}")
                    
                self._send_zoho_failure_notification(data, zoho_result, transaction_type)
                
        except Exception as e:
            error_msg = f"💥 Error processing complete data for {order_number}: {e}"
            logger.error(error_msg, exc_info=True)
            self.discord.send_error(f"Failed to process complete {transaction_type}: {str(e)}")
            
    def _process_incomplete_data(self, data: Dict, parse_result: ParseResult, transaction_type: str):
        """
        Process incomplete data:
        1. Save to Airtable with incomplete flag
        2. Do NOT sync to Zoho
        3. Send review required notification
        4. Track for review completion
        """
        order_number = data.get('order_number', 'N/A')
        
        try:
            # Step 1: Save to Airtable with incomplete status
            logger.info(f"💾 Saving incomplete {transaction_type} to Airtable for review...")
            airtable_start = time.time()
            
            data['processing_status'] = ProcessingStatus.INCOMPLETE.value
            data['requires_review'] = True
            data['completeness'] = parse_result.completeness.value
            data['missing_fields'] = parse_result.missing_fields
            
            if transaction_type == 'purchase':
                airtable_record = self.airtable.create_purchase(data)
            else:
                airtable_record = self.airtable.create_sale(data)
                
            airtable_duration = time.time() - airtable_start
            airtable_id = airtable_record.get('id')
            
            logger.info(f"✅ Airtable save completed in {airtable_duration:.2f}s")
            logger.info(f"   - Record ID: {airtable_id}")
            logger.info(f"   - Status: REQUIRES_REVIEW")
            
            # Track for review
            if airtable_id:
                self.pending_reviews[airtable_id] = {
                    'data': data,
                    'type': transaction_type,
                    'missing_fields': parse_result.missing_fields,
                    'created_at': datetime.now()
                }
                self.stats['pending_review'] += 1
                
                logger.info(f"📋 Added to review queue:")
                logger.info(f"   - Missing: {', '.join(parse_result.missing_fields[:5])}")
                logger.info(f"   - Total pending: {self.stats['pending_review']}")
                
            # Step 2: Send human review notification
            logger.info(f"📢 Sending review notification to Discord...")
            self._send_review_required_notification(
                data, 
                parse_result, 
                transaction_type,
                airtable_id
            )
            
            logger.info(f"🚫 SKIPPING Zoho sync - data incomplete")
            
        except Exception as e:
            error_msg = f"💥 Error processing incomplete data for {order_number}: {e}"
            logger.error(error_msg, exc_info=True)
            self.discord.send_error(f"Failed to process incomplete {transaction_type}: {str(e)}")
            
    def handle_review_complete(self, airtable_id: str):
        """
        Handle when human review is complete.
        Re-validate data and sync to Zoho if now complete.
        """
        logger.info(f"🔄 Processing review completion for record: {airtable_id}")
        
        try:
            if airtable_id not in self.pending_reviews:
                logger.warning(f"❓ No pending review found for {airtable_id}")
                self.discord.send_warning(f"No pending review found for Airtable ID: {airtable_id}")
                return
                
            review_info = self.pending_reviews[airtable_id]
            transaction_type = review_info['type']
            
            logger.info(f"📝 Re-validating {transaction_type} data after review...")
            
            # Fetch updated data from Airtable
            # updated_data = self.airtable.get_record(airtable_id, transaction_type)
            # For now, using the stored data (would need to implement get_record)
            updated_data = review_info['data']
            
            # Re-validate completeness
            validation_result = self._validate_data_completeness(updated_data, transaction_type)
            
            if validation_result['is_complete']:
                logger.info(f"✅ Data is now complete! Syncing to Zoho...")
                
                zoho_result = self.zoho.process_complete_data(updated_data, transaction_type)
                
                if zoho_result.get('success'):
                    # Update status
                    updated_data['processing_status'] = ProcessingStatus.SYNCED.value
                    self.stats['synced_to_zoho'] += 1
                    
                    # Remove from pending
                    del self.pending_reviews[airtable_id]
                    self.stats['pending_review'] -= 1
                    
                    logger.info(f"🎉 Review complete & synced successfully!")
                    
                    # Send success notification
                    self.discord.send_success(
                        f"✅ Review Complete & Synced to Zoho\n"
                        f"Type: {transaction_type}\n"
                        f"Order #: {updated_data.get('order_number', 'N/A')}\n"
                        f"Items processed: {len(zoho_result.get('items_processed', []))}\n"
                        f"Stock adjusted: {'Yes' if zoho_result.get('stock_adjusted') else 'No'}"
                    )
                else:
                    logger.error(f"❌ Zoho sync failed after review completion")
                    self.discord.send_error(
                        f"❌ Zoho sync failed after review\n"
                        f"Errors: {', '.join(zoho_result.get('errors', []))}"
                    )
            else:
                logger.warning(f"⚠️ Data still incomplete after review")
                logger.warning(f"   - Still missing: {', '.join(validation_result.get('missing_fields', []))}")
                self.discord.send_warning(
                    f"⚠️ Data still incomplete after review\n"
                    f"Missing fields: {', '.join(validation_result.get('missing_fields', []))}\n"
                    f"Please complete all required fields in Airtable"
                )
                
        except Exception as e:
            error_msg = f"💥 Error handling review completion: {e}"
            logger.error(error_msg, exc_info=True)
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
            logger.info("🔍 Checking for new emails...")
            
            # Fetch all unread emails
            new_emails = self.gmail.fetch_unread_emails()
            
            if not new_emails:
                logger.info("📭 No new emails found")
                return
                
            logger.info(f"📬 Found {len(new_emails)} new emails to process")
            
            for i, email in enumerate(new_emails, 1):
                seq_num = email.get('seq_num')
                subject = email.get('subject', 'No Subject')[:100]
                
                logger.info(f"📧 [{i}/{len(new_emails)}] Processing email [seq={seq_num}]: {subject}")
                
                if seq_num and seq_num not in self.processed_seq_nums:
                    # Process the email
                    self.process_email(email)
                    
                    # Mark as processed in Gmail
                    logger.info(f"🏷️ Marking email [seq={seq_num}] as processed in Gmail...")
                    mark_success = self.gmail.mark_as_processed(seq_num)
                    
                    if mark_success:
                        logger.info(f"✅ Email [seq={seq_num}] successfully marked as processed")
                        self.processed_seq_nums.add(seq_num)
                    else:
                        logger.warning(f"⚠️ Failed to mark email [seq={seq_num}] as processed in Gmail")
                        # Still add to processed set to avoid reprocessing
                        self.processed_seq_nums.add(seq_num)
                        
                else:
                    logger.info(f"⏭️ Email [seq={seq_num}] already processed, skipping")
                    
            logger.info(f"✅ Completed processing {len(new_emails)} emails")
                    
        except Exception as e:
            error_msg = f"💥 Error in run cycle: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.discord.send_error(error_msg)
            
    def run(self) -> None:
        """Main run loop."""
        logger.info("🚀 Starting Inventory Reconciliation App with Complete Data Validation")
        
        # Send startup notification
        self.discord.send_info(
            "🚀 Inventory Reconciliation System Started\n"
            "Mode: Complete Data Validation\n"
            "✅ Enhanced logging enabled\n"
            "⚠️ Incomplete data will be flagged for human review",
            title="System Started"
        )
        
        poll_interval = self.config.get_int('POLL_INTERVAL')
        logger.info(f"⏰ Email polling interval: {poll_interval} seconds")
        
        try:
            cycle_count = 0
            while True:
                try:
                    cycle_count += 1
                    logger.info(f"🔄 Starting email check cycle #{cycle_count}")
                    
                    cycle_start = time.time()
                    self.run_once()
                    cycle_duration = time.time() - cycle_start
                    
                    logger.info(f"⏱️ Cycle #{cycle_count} completed in {cycle_duration:.2f}s")
                    
                    # Periodic status report
                    if self.stats['processed'] > 0 and self.stats['processed'] % 50 == 0:
                        logger.info(f"📊 Milestone reached: {self.stats['processed']} emails processed")
                        self._send_status_report()
                        
                    logger.info(f"😴 Sleeping for {poll_interval} seconds until next cycle...")
                    time.sleep(poll_interval)
                    
                except KeyboardInterrupt:
                    logger.info("🛑 Shutdown requested by user")
                    break
                    
                except Exception as e:
                    logger.error(f"💥 Unexpected error in cycle #{cycle_count}: {str(e)}", exc_info=True)
                    self.stats['errors'] += 1
                    
                    # Send error notification but continue running
                    self.discord.send_error(f"Cycle #{cycle_count} error: {str(e)}")
                    
                    logger.info(f"⏰ Waiting {poll_interval}s before retry...")
                    time.sleep(poll_interval)
                    
        finally:
            # Cleanup
            logger.info("🧹 Cleaning up resources...")
            
            try:
                self.gmail.close()
                logger.info("✅ Gmail connection closed")
            except Exception as e:
                logger.error(f"❌ Error closing Gmail connection: {e}")
            
            # Send final report
            logger.info("📊 Generating final session report...")
            self._send_status_report()
            
            # List any remaining pending reviews
            if self.pending_reviews:
                logger.info(f"📋 {len(self.pending_reviews)} reviews still pending")
                self._send_pending_reviews_report()
            else:
                logger.info("✅ No pending reviews")
                
            # Send shutdown notification
            runtime = (datetime.now() - self.stats['session_start']).total_seconds()
            shutdown_message = (
                f"🛑 **System Shutdown**\n\n"
                f"**Final Statistics:**\n"
                f"• Total Runtime: {runtime/3600:.2f} hours\n"
                f"• Emails Processed: {self.stats['processed']}\n"
                f"• Successfully Synced: {self.stats['synced_to_zoho']}\n"
                f"• Pending Review: {self.stats['pending_review']}\n"
                f"• Total Errors: {self.stats['errors']}"
            )
            
            self.discord.send_info(shutdown_message, title="System Stopped")
            logger.info("🏁 Shutdown complete")


if __name__ == "__main__":
    app = InventoryReconciliationApp()
    app.run()