#!/usr/bin/env python3
"""
Main application with sequential Airtable → Zoho workflow.
Enhanced with three-table inventory management.
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
    PARSED = "parsed"
    AIRTABLE_COMPLETE = "airtable_complete"
    AIRTABLE_INCOMPLETE = "airtable_incomplete"
    PENDING_REVIEW = "pending_review"
    READY_FOR_ZOHO = "ready_for_zoho"
    ZOHO_SYNCED = "zoho_synced"
    ZOHO_FAILED = "zoho_failed"
    FAILED = "failed"


class InventoryReconciliationApp:
    """Main application orchestrator with sequential Airtable → Zoho workflow."""
    
    def __init__(self):
        """Initialize all service clients."""
        logger.info("🚀 Initializing Inventory Reconciliation System...")
        logger.info("📊 New Architecture: Sequential Airtable → Zoho with Live Inventory")
        
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
            logger.info("✅ Airtable client initialized (3-table architecture)")
        except Exception as e:
            logger.error(f"❌ Airtable client initialization failed: {e}")
            raise
            
        try:
            self.zoho = ZohoClient(self.config)
            if self.zoho.is_available:
                logger.info("✅ Zoho client initialized")
            else:
                logger.warning("⚠️ Zoho client initialized but API is unavailable")
                logger.warning("   System will continue with Airtable-only mode")
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
            'airtable_saved': 0,
            'inventory_updated': 0,
            'synced_to_zoho': 0,
            'pending_review': 0,
            'errors': 0,
            'session_start': datetime.now()
        }
        
        logger.info("🎉 All components initialized successfully!")
        
    def process_email(self, email_data: Dict) -> None:
        """
        Process email with sequential Airtable → Zoho workflow.
        
        New Workflow:
        1. Parse email with OpenAI
        2. Process through Airtable (3-table architecture)
        3. If complete and Zoho available → Sync to Zoho
        4. If incomplete → Flag for human review
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
            
            # Add email metadata
            parsed_data['email_seq_num'] = seq_num
            parsed_data['email_date'] = email_data['date']
            parsed_data['parse_result'] = parse_result.to_dict()
            parsed_data['confidence_score'] = confidence
            
            # Step 2: Process through Airtable (regardless of completeness)
            if parse_result.completeness == DataCompleteness.COMPLETE:
                logger.info(f"✅ Data is COMPLETE - processing through full Airtable workflow")
                self._process_complete_data_sequential(parsed_data, parse_result, transaction_type)
                self.stats['complete_data'] += 1
                
            elif parse_result.completeness == DataCompleteness.INCOMPLETE:
                logger.info(f"⚠️ Data is INCOMPLETE - saving to Airtable for review")
                self._process_incomplete_data_sequential(parsed_data, parse_result, transaction_type)
                self.stats['incomplete_data'] += 1
                
            else:
                logger.error(f"❌ Invalid data completeness: {parse_result.completeness}")
                self.stats['errors'] += 1
                
        except Exception as e:
            error_msg = f"💥 Error processing email [seq={seq_num}]: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.discord.send_error(error_msg, email_data)
            self.stats['errors'] += 1
            
    def _process_complete_data_sequential(self, data: Dict, parse_result: ParseResult, transaction_type: str):
        """
        Process complete data through sequential Airtable → Zoho workflow.
        
        Steps:
        1. Process through Airtable (creates transaction + manages inventory)
        2. If successful → Sync clean data to Zoho
        3. Send appropriate notifications
        """
        order_number = data.get('order_number', 'N/A')
        
        try:
            # Step 1: Process through Airtable (3-table workflow)
            logger.info(f"📊 Processing complete {transaction_type} through Airtable workflow...")
            airtable_start = time.time()
            
            data['requires_review'] = False
            data['completeness'] = parse_result.completeness.value
            
            airtable_result = self.airtable.process_transaction(data, transaction_type)
            airtable_duration = time.time() - airtable_start
            
            logger.info(f"⏱️ Airtable processing completed in {airtable_duration:.2f}s")
            
            if airtable_result.get('success'):
                self.stats['airtable_saved'] += 1
                self.stats['inventory_updated'] += len(airtable_result.get('inventory_updates', []))
                
                transaction_record_id = airtable_result.get('transaction_record_id')
                items_processed = len(airtable_result.get('items_processed', []))
                
                logger.info(f"✅ Airtable processing SUCCESS:")
                logger.info(f"   - Transaction record: {transaction_record_id}")
                logger.info(f"   - Items processed: {items_processed}")
                logger.info(f"   - Inventory updates: {len(airtable_result.get('inventory_updates', []))}")
                
                if airtable_result.get('warnings'):
                    logger.warning(f"⚠️ Airtable warnings: {'; '.join(airtable_result['warnings'][:3])}")
                
                # Step 2: Sync to Zoho if available
                if self.zoho.is_available:
                    logger.info(f"🔄 Syncing clean data to Zoho...")
                    self._sync_to_zoho_from_airtable(airtable_result, transaction_type, transaction_record_id)
                else:
                    logger.warning(f"⚠️ Zoho unavailable - data saved to Airtable only")
                    self._send_airtable_success_notification(data, airtable_result, transaction_type)
                    
            else:
                logger.error(f"❌ Airtable processing FAILED:")
                for error in airtable_result.get('errors', [])[:3]:
                    logger.error(f"   - {error}")
                    
                self._send_airtable_failure_notification(data, airtable_result, transaction_type)
                
        except Exception as e:
            error_msg = f"💥 Error processing complete data for {order_number}: {e}"
            logger.error(error_msg, exc_info=True)
            self.discord.send_error(f"Failed to process complete {transaction_type}: {str(e)}")
            
    def _process_incomplete_data_sequential(self, data: Dict, parse_result: ParseResult, transaction_type: str):
        """
        Process incomplete data through Airtable-only workflow.
        
        Steps:
        1. Save to appropriate transaction table with incomplete flag
        2. Do NOT process through inventory (no SKUs generated)
        3. Flag for human review
        4. Track for completion
        """
        order_number = data.get('order_number', 'N/A')
        
        try:
            logger.info(f"📋 Saving incomplete {transaction_type} to Airtable for review...")
            airtable_start = time.time()
            
            data['requires_review'] = True
            data['completeness'] = parse_result.completeness.value
            data['processing_status'] = ProcessingStatus.AIRTABLE_INCOMPLETE.value
            data['missing_fields'] = parse_result.missing_fields
            
            # Save directly to transaction table (skip inventory processing)
            if transaction_type == 'purchase':
                airtable_record = self.airtable.create_purchase(data)
            else:
                airtable_record = self.airtable.create_sale(data)
                
            airtable_duration = time.time() - airtable_start
            airtable_id = airtable_record.get('id')
            
            logger.info(f"✅ Incomplete data saved in {airtable_duration:.2f}s")
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
                
            # Send review notification
            logger.info(f"📢 Sending review notification to Discord...")
            self._send_review_required_notification(
                data, 
                parse_result, 
                transaction_type,
                airtable_id
            )
            
            logger.info(f"🚫 SKIPPING inventory and Zoho processing - data incomplete")
            
        except Exception as e:
            error_msg = f"💥 Error processing incomplete data for {order_number}: {e}"
            logger.error(error_msg, exc_info=True)
            self.discord.send_error(f"Failed to process incomplete {transaction_type}: {str(e)}")
            
    def _sync_to_zoho_from_airtable(self, airtable_result: Dict, transaction_type: str, transaction_record_id: str):
        """
        Sync clean Airtable data to Zoho.
        
        Args:
            airtable_result: Result from Airtable processing with clean data
            transaction_type: 'purchase' or 'sale'
            transaction_record_id: Airtable transaction record ID
        """
        try:
            zoho_start = time.time()
            
            # Extract clean data from Airtable result
            clean_data = {
                'type': transaction_type,
                'items': []
            }
            
            # Build clean item list with guaranteed SKUs
            for item_processed in airtable_result.get('items_processed', []):
                clean_item = {
                    'name': item_processed.get('name'),
                    'sku': item_processed.get('sku'),  # Guaranteed to exist from Airtable
                    'quantity': item_processed.get('quantity')
                }
                
                if transaction_type == 'purchase':
                    clean_item['unit_price'] = item_processed.get('unit_price', 0)
                else:
                    clean_item['sale_price'] = item_processed.get('sale_price', 0)
                    
                clean_data['items'].append(clean_item)
            
            # Process through Zoho
            zoho_result = self.zoho.process_complete_data(clean_data, transaction_type)
            zoho_duration = time.time() - zoho_start
            
            logger.info(f"⏱️ Zoho sync completed in {zoho_duration:.2f}s")
            
            if zoho_result.get('success'):
                self.stats['synced_to_zoho'] += 1
                
                logger.info(f"✅ Zoho sync SUCCESS:")
                logger.info(f"   - Stock adjusted: {zoho_result.get('stock_adjusted', False)}")
                logger.info(f"   - Items processed: {len(zoho_result.get('items_processed', []))}")
                logger.info(f"   - Adjustment ID: {zoho_result.get('adjustment_id', 'N/A')}")
                
                # Mark Airtable record as synced
                self.airtable.mark_record_synced_to_zoho(
                    transaction_record_id,
                    transaction_type,
                    zoho_result.get('adjustment_id')
                )
                
                # Send full success notification
                self._send_full_success_notification(airtable_result, zoho_result, transaction_type)
                
            else:
                logger.error(f"❌ Zoho sync FAILED:")
                for error in zoho_result.get('errors', [])[:3]:
                    logger.error(f"   - {error}")
                    
                # Mark Airtable record as sync failed
                self.airtable.mark_record_synced_to_zoho(
                    transaction_record_id,
                    transaction_type,
                    errors=zoho_result.get('errors', [])
                )
                
                self._send_zoho_failure_notification(airtable_result, zoho_result, transaction_type)
                
        except Exception as e:
            logger.error(f"💥 Error syncing to Zoho: {e}")
            
            # Mark as failed in Airtable
            self.airtable.mark_record_synced_to_zoho(
                transaction_record_id,
                transaction_type,
                errors=[str(e)]
            )
            
    def _send_full_success_notification(self, airtable_result: Dict, zoho_result: Dict, transaction_type: str):
        """Send notification for complete Airtable + Zoho success."""
        items_count = len(airtable_result.get('items_processed', []))
        inventory_updates = len(airtable_result.get('inventory_updates', []))
        
        if transaction_type == 'purchase':
            message = (
                f"🎉 **Purchase Fully Processed**\n"
                f"✅ **Airtable Processing:**\n"
                f"• Transaction record: Created\n"
                f"• Items processed: {items_count}\n"
                f"• Inventory updates: {inventory_updates}\n"
                f"• SKUs assigned: {items_count}\n\n"
                f"✅ **Zoho Sync:**\n"
                f"• Stock adjusted: {'Yes' if zoho_result.get('stock_adjusted') else 'No'}\n"
                f"• Adjustment ID: {zoho_result.get('adjustment_id', 'N/A')}\n\n"
                f"🏆 **Result:** Complete end-to-end processing successful!"
            )
        else:
            message = (
                f"🎉 **Sale Fully Processed**\n"
                f"✅ **Airtable Processing:**\n"
                f"• Transaction record: Created\n"
                f"• Items processed: {items_count}\n"
                f"• Inventory updates: {inventory_updates}\n"
                f"• SKUs referenced: {items_count}\n\n"
                f"✅ **Zoho Sync:**\n"
                f"• Stock adjusted: {'Yes' if zoho_result.get('stock_adjusted') else 'No'}\n"
                f"• Revenue: ${zoho_result.get('revenue', 0):.2f}\n"
                f"• COGS: ${zoho_result.get('cogs', 0):.2f}\n"
                f"• Adjustment ID: {zoho_result.get('adjustment_id', 'N/A')}\n\n"
                f"🏆 **Result:** Complete end-to-end processing successful!"
            )
            
        self.discord.send_success(message, title="Full Pipeline Success")
        
    def _send_airtable_success_notification(self, data: Dict, airtable_result: Dict, transaction_type: str):
        """Send notification for Airtable-only success (Zoho unavailable)."""
        items_count = len(airtable_result.get('items_processed', []))
        inventory_updates = len(airtable_result.get('inventory_updates', []))
        
        message = (
            f"💾 **{transaction_type.title()} Processed (Airtable Only)**\n"
            f"Order #: {data.get('order_number', 'N/A')}\n"
            f"Items: {items_count}\n\n"
            f"✅ **Airtable Processing:**\n"
            f"• Transaction record: Created\n"
            f"• Inventory updates: {inventory_updates}\n"
            f"• SKUs managed: {items_count}\n\n"
            f"⚠️ **Zoho Status:**\n"
            f"• API: Unavailable\n"
            f"• Data ready for sync when available\n\n"
            f"**Note:** Complete data saved to Airtable with live inventory tracking."
        )
        
        self.discord.send_warning(message, title="Airtable Processing Complete")
        
    def _send_airtable_failure_notification(self, data: Dict, airtable_result: Dict, transaction_type: str):
        """Send notification when Airtable processing fails."""
        message = (
            f"❌ **Airtable Processing Failed**\n"
            f"Transaction: {transaction_type.title()}\n"
            f"Order #: {data.get('order_number', 'N/A')}\n\n"
            f"**Errors:**\n"
        )
        
        for error in airtable_result.get('errors', [])[:5]:
            message += f"• {error}\n"
            
        if airtable_result.get('items_failed'):
            message += f"\n**Failed Items:**\n"
            for item in airtable_result['items_failed'][:5]:
                message += f"• {item.get('name', 'Unknown')}\n"
                
        self.discord.send_error(message, title="Airtable Processing Failed")
        
    def _send_zoho_failure_notification(self, airtable_result: Dict, zoho_result: Dict, transaction_type: str):
        """Send notification when Zoho sync fails (but Airtable succeeded)."""
        items_count = len(airtable_result.get('items_processed', []))
        
        message = (
            f"⚠️ **Zoho Sync Failed (Airtable OK)**\n"
            f"Transaction: {transaction_type.title()}\n"
            f"Items processed: {items_count}\n\n"
            f"✅ **Airtable:** Successfully processed\n"
            f"❌ **Zoho:** Sync failed\n\n"
            f"**Zoho Errors:**\n"
        )
        
        for error in zoho_result.get('errors', [])[:5]:
            message += f"• {error}\n"
            
        message += f"\n**Note:** Data is safe in Airtable and can be re-synced to Zoho later."
        
        self.discord.send_warning(message, title="Partial Success")
        
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
            f"⚠️ **Note:** Data will NOT process through inventory or Zoho until complete."
        )
        
        self.discord.send_warning(message, title="Review Required")
        
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
        logger.info("🚀 Starting Inventory Reconciliation App")
        logger.info("📊 Architecture: Sequential Airtable → Zoho with Live Inventory")
        
        # Send startup notification
        zoho_status = "Available" if self.zoho.is_available else "Unavailable (Airtable-only mode)"
        
        self.discord.send_info(
            f"🚀 **Inventory Reconciliation System Started**\n\n"
            f"**New Architecture:**\n"
            f"• Sequential processing: Email → Airtable → Zoho\n"
            f"• Live inventory tracking in InventoryStock table\n"
            f"• Automatic SKU generation and management\n"
            f"• Human review workflow for incomplete data\n\n"
            f"**System Status:**\n"
            f"• Gmail: Connected\n"
            f"• OpenAI: Ready ({self.parser.model})\n"
            f"• Airtable: 3-table architecture ready\n"
            f"• Zoho: {zoho_status}\n"
            f"• Discord: Notifications active",
            title="System Started - New Architecture"
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
                    if self.stats['processed'] > 0 and self.stats['processed'] % 25 == 0:
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
            # Cleanup and final reporting
            self._shutdown_cleanup()
            
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
            f"• Airtable Records: {self.stats['airtable_saved']}\n"
            f"• Inventory Updates: {self.stats['inventory_updated']}\n"
            f"• Synced to Zoho: {self.stats['synced_to_zoho']}\n"
            f"• Pending Review: {self.stats['pending_review']}\n"
            f"• Errors: {self.stats['errors']}\n"
        )
        
        if self.stats['processed'] > 0:
            complete_rate = (self.stats['complete_data'] / self.stats['processed']) * 100
            airtable_rate = (self.stats['airtable_saved'] / self.stats['processed']) * 100
            
            message += (
                f"\n**Success Rates:**\n"
                f"• Data Completeness: {complete_rate:.1f}%\n"
                f"• Airtable Success: {airtable_rate:.1f}%\n"
            )
            
            if self.zoho.is_available:
                sync_rate = (self.stats['synced_to_zoho'] / max(1, self.stats['airtable_saved'])) * 100
                message += f"• Zoho Sync Rate: {sync_rate:.1f}%\n"
            else:
                message += f"• Zoho Status: Unavailable\n"
            
        self.discord.send_info(message, title="Status Report")
        
    def _shutdown_cleanup(self):
        """Handle shutdown cleanup and final reporting."""
        logger.info("🧹 Cleaning up resources...")
        
        try:
            self.gmail.close()
            logger.info("✅ Gmail connection closed")
        except Exception as e:
            logger.error(f"❌ Error closing Gmail connection: {e}")
        
        # Send final report
        logger.info("📊 Generating final session report...")
        self._send_status_report()
        
        # Final shutdown notification
        runtime = (datetime.now() - self.stats['session_start']).total_seconds()
        shutdown_message = (
            f"🛑 **System Shutdown**\n\n"
            f"**Final Statistics:**\n"
            f"• Total Runtime: {runtime/3600:.2f} hours\n"
            f"• Emails Processed: {self.stats['processed']}\n"
            f"• Airtable Records: {self.stats['airtable_saved']}\n"
            f"• Inventory Updates: {self.stats['inventory_updated']}\n"
            f"• Zoho Synced: {self.stats['synced_to_zoho']}\n"
            f"• Pending Review: {self.stats['pending_review']}\n"
            f"• Total Errors: {self.stats['errors']}\n\n"
            f"**Architecture:** Sequential Airtable → Zoho with Live Inventory"
        )
        
        self.discord.send_info(shutdown_message, title="System Stopped")
        logger.info("🏁 Shutdown complete")


if __name__ == "__main__":
    app = InventoryReconciliationApp()
    app.run()