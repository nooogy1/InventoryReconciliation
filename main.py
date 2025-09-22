#!/usr/bin/env python3
"""
Main application with sequential Airtable → Zoho workflow using proper Purchase/Sales Orders.
Enhanced with proper accounting workflows and FIFO COGS tracking.
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
    """Main application orchestrator with proper Purchase/Sales Order workflows."""
    
    def __init__(self):
        """Initialize all service clients."""
        logger.info("Starting Inventory Reconciliation System (Proper Workflows)")
        logger.info("Architecture: Sequential Airtable → Zoho with Purchase Orders & Sales Orders")
        
        # Load configuration
        self.config = Config()
        logger.info("Configuration loaded successfully")
        
        # Initialize clients with detailed logging
        try:
            self.gmail = GmailClient(self.config)
            logger.info("Gmail client initialized")
        except Exception as e:
            logger.error(f"Gmail client initialization failed: {e}")
            raise
            
        try:
            self.parser = EmailParser(self.config)
            logger.info(f"OpenAI parser initialized (model: {self.parser.model})")
        except Exception as e:
            logger.error(f"OpenAI parser initialization failed: {e}")
            raise
            
        try:
            self.airtable = AirtableClient(self.config)
            logger.info("Airtable client initialized (3-table architecture)")
        except Exception as e:
            logger.error(f"Airtable client initialization failed: {e}")
            raise
            
        try:
            self.zoho = ZohoClient(self.config)
            if self.zoho.is_available:
                logger.info("Zoho client initialized with proper workflows")
                logger.info(f"   - Proper Workflows: {self.zoho.use_proper_workflows}")
                logger.info(f"   - Auto Create Bills: {self.zoho.auto_create_bills}")
                logger.info(f"   - Auto Create Invoices: {self.zoho.auto_create_invoices}")
                logger.info(f"   - Auto Create Shipments: {self.zoho.auto_create_shipments}")
                logger.info(f"   - Allow Direct Adjustments: {self.zoho.allow_direct_adjustments}")
            else:
                logger.warning("Zoho client initialized but API is unavailable")
                logger.warning("   System will continue with Airtable-only mode")
        except Exception as e:
            logger.error(f"Zoho client initialization failed: {e}")
            raise
            
        try:
            self.discord = DiscordNotifier(self.config)
            logger.info("Discord notifier initialized")
        except Exception as e:
            logger.error(f"Discord notifier initialization failed: {e}")
            raise
        
        # Application state
        self.stats = {
            'emails_processed': 0,
            'parse_successful': 0,
            'parse_failed': 0,
            'incomplete_data': 0,
            'airtable_saved': 0,
            'synced_to_zoho': 0,
            'purchase_orders_created': 0,
            'sales_orders_created': 0,
            'bills_created': 0,
            'invoices_created': 0,
            'shipments_created': 0,
            'inventory_updated': 0,
            'human_reviews_required': 0,
            'errors': 0,
            'session_start': datetime.now()
        }
        
        # Track records pending review
        self.pending_reviews = {}  # airtable_id: data
        
        # Track processed emails by sequence number
        self.processed_seq_nums: Set[str] = set()
        
        logger.info("All components initialized successfully!")
        
        # Log system configuration
        self._log_system_status()

    def _log_system_status(self):
        """Log current system configuration and status."""
        logger.info("System Configuration:")
        logger.info(f"   - Proper Workflows: {self.zoho.use_proper_workflows}")
        logger.info(f"   - Auto Create Bills: {self.zoho.auto_create_bills}")
        logger.info(f"   - Auto Create Invoices: {self.zoho.auto_create_invoices}")
        logger.info(f"   - Auto Create Shipments: {self.zoho.auto_create_shipments}")
        logger.info(f"   - Allow Direct Adjustments: {self.zoho.allow_direct_adjustments}")
        
        logger.info("Service Status:")
        try:
            gmail_status = self.gmail.test_connection() if hasattr(self.gmail, 'test_connection') else True
            logger.info(f"   - Gmail: {'Connected' if gmail_status else 'Failed'}")
        except:
            logger.info(f"   - Gmail: Initialized")
            
        try:
            openai_status = self.parser.test_connection() if hasattr(self.parser, 'test_connection') else True
            logger.info(f"   - OpenAI: {'Available' if openai_status else 'Failed'}")
        except:
            logger.info(f"   - OpenAI: Initialized")
            
        try:
            airtable_status = self.airtable.test_connection() if hasattr(self.airtable, 'test_connection') else True
            logger.info(f"   - Airtable: {'Connected' if airtable_status else 'Failed'}")
        except:
            logger.info(f"   - Airtable: Initialized")
            
        logger.info(f"   - Zoho: {'Connected' if self.zoho.is_available else 'Failed'}")
        
        try:
            discord_status = self.discord.test_webhook() if hasattr(self.discord, 'test_webhook') else True
            logger.info(f"   - Discord: {'Ready' if discord_status else 'Failed'}")
        except:
            logger.info(f"   - Discord: Initialized")

    def process_email(self, email_data: Dict) -> None:
        """
        Process email with sequential Airtable → Zoho workflow using proper Purchase/Sales Orders.
        """
        seq_num = email_data.get('seq_num', 'unknown')
        subject = email_data.get('subject', 'No Subject')[:100]
        
        logger.info(f"Processing email [seq={seq_num}]: {subject}")
        
        try:
            self.stats['emails_processed'] += 1
            
            # Step 1: Parse email with OpenAI
            logger.info(f"Parsing email with OpenAI...")
            parse_start = time.time()
            
            parse_result = self.parser.parse_email(
                email_data['body'],
                email_data['subject']
            )
            
            parse_duration = time.time() - parse_start
            logger.info(f"OpenAI parsing completed in {parse_duration:.2f}s")
            
            # Check parse status
            if parse_result.status == ParseStatus.FAILED:
                logger.error(f"OpenAI parsing failed: {', '.join(parse_result.errors)}")
                if hasattr(self.discord, 'send_error_notification'):
                    self.discord.send_error_notification(
                        "Email Parsing Failed",
                        f"Failed to parse email: {subject}",
                        {'errors': parse_result.errors, 'seq_num': seq_num}
                    )
                self.stats['parse_failed'] += 1
                return
                
            if parse_result.status == ParseStatus.NOT_INVENTORY:
                logger.info(f"Email not related to inventory - skipping: {subject}")
                return
                
            # Extract parsed data
            parsed_data = parse_result.data
            if not parsed_data:
                logger.warning(f"No data extracted from email: {subject}")
                self.stats['errors'] += 1
                return
                
            self.stats['parse_successful'] += 1
            
            transaction_type = parsed_data.get('type', 'unknown')
            order_number = parsed_data.get('order_number', 'N/A')
            confidence = parse_result.confidence
            
            logger.info(f"Parse Results:")
            logger.info(f"  - Type: {transaction_type}")
            logger.info(f"  - Order: {order_number}")
            logger.info(f"  - Status: {parse_result.status.value}")
            logger.info(f"  - Completeness: {parse_result.completeness.value}")
            logger.info(f"  - Confidence: {confidence:.2%}")
            
            if parse_result.missing_fields:
                logger.info(f"  - Missing fields: {', '.join(parse_result.missing_fields)}")
            
            # Add email metadata
            parsed_data['email_seq_num'] = seq_num
            parsed_data['email_date'] = email_data['date']
            parsed_data['parse_result'] = parse_result.to_dict()
            parsed_data['confidence_score'] = confidence
            
            # Step 2: Process based on data completeness
            if parse_result.completeness == DataCompleteness.COMPLETE:
                logger.info(f"Data is COMPLETE - processing through full workflow")
                self._process_complete_transaction(parsed_data, transaction_type, parse_result)
                
            elif parse_result.completeness == DataCompleteness.INCOMPLETE:
                logger.info(f"Data is INCOMPLETE - saving to Airtable for review")
                self._process_incomplete_transaction(parsed_data, transaction_type, parse_result)
                self.stats['incomplete_data'] += 1
                
            else:
                logger.error(f"Invalid data completeness: {parse_result.completeness}")
                self.stats['errors'] += 1
                
        except Exception as e:
            error_msg = f"Error processing email [seq={seq_num}]: {str(e)}"
            logger.error(error_msg, exc_info=True)
            if hasattr(self.discord, 'send_error_notification'):
                self.discord.send_error_notification(
                    "Email Processing Error",
                    error_msg,
                    {'seq_num': seq_num, 'subject': subject}
                )
            self.stats['errors'] += 1

    def _process_complete_transaction(self, data: Dict, transaction_type: str, parse_result: ParseResult):
        """Process complete data through the full sequential workflow."""
        order_number = data.get('order_number', 'N/A')
        
        try:
            # Step 1: Process through Airtable (3-table workflow)
            logger.info(f"Processing complete {transaction_type} through Airtable workflow...")
            airtable_start = time.time()
            
            data['requires_review'] = False
            data['completeness'] = parse_result.completeness.value
            
            airtable_result = self.airtable.process_transaction(data, transaction_type)
            airtable_duration = time.time() - airtable_start
            
            logger.info(f"Airtable processing completed in {airtable_duration:.2f}s")
            
            if airtable_result.get('success'):
                self.stats['airtable_saved'] += 1
                self.stats['inventory_updated'] += len(airtable_result.get('inventory_updates', []))
                
                transaction_record_id = airtable_result.get('transaction_record_id')
                items_processed = len(airtable_result.get('items_processed', []))
                
                logger.info(f"Airtable processing SUCCESS:")
                logger.info(f"   - Transaction record: {transaction_record_id}")
                logger.info(f"   - Items processed: {items_processed}")
                logger.info(f"   - Inventory updates: {len(airtable_result.get('inventory_updates', []))}")
                
                if airtable_result.get('warnings'):
                    logger.warning(f"Airtable warnings: {'; '.join(airtable_result['warnings'][:3])}")
                
                # Step 2: Execute proper Zoho workflow
                if self.zoho.is_available:
                    logger.info(f"Executing proper Zoho {transaction_type} workflow...")
                    self._execute_zoho_workflow(airtable_result, transaction_type, transaction_record_id)
                else:
                    logger.warning("Zoho unavailable - skipping sync")
                    
                    # Send notification about Zoho being unavailable
                    if hasattr(self.discord, 'send_warning_notification'):
                        self.discord.send_warning_notification(
                            "Zoho Unavailable",
                            f"Transaction saved to Airtable but could not sync to Zoho: {order_number}",
                            {
                                "transaction_type": transaction_type,
                                "order_number": order_number,
                                "airtable_record": transaction_record_id
                            }
                        )
                
            else:
                logger.error(f"Airtable processing FAILED: {'; '.join(airtable_result.get('errors', []))}")
                self.stats['errors'] += 1
                
                # Send error notification
                if hasattr(self.discord, 'send_error_notification'):
                    self.discord.send_error_notification(
                        "Airtable Processing Failed",
                        f"Failed to save {transaction_type} to Airtable: {order_number}",
                        {
                            "errors": airtable_result.get('errors', []),
                            "transaction_type": transaction_type
                        }
                    )
                
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"Failed to process complete transaction: {e}", exc_info=True)
            
            if hasattr(self.discord, 'send_error_notification'):
                self.discord.send_error_notification(
                    "Transaction Processing Failed",
                    f"Unexpected error processing {transaction_type}: {order_number}",
                    {"error": str(e), "transaction_type": transaction_type}
                )

    def _execute_zoho_workflow(self, airtable_result: Dict, transaction_type: str, transaction_record_id: str):
        """Execute proper Zoho workflow using clean data from Airtable."""
        try:
            zoho_start = time.time()
            
            # Extract clean data from Airtable result - SKUs are guaranteed to exist
            clean_data = self._build_clean_data_from_airtable(airtable_result, transaction_type)
            
            # Execute proper workflow through ZohoClient
            zoho_result = self.zoho.process_complete_data(clean_data, transaction_type)
            zoho_duration = time.time() - zoho_start
            
            logger.info(f"Zoho workflow completed in {zoho_duration:.2f}s")
            
            if zoho_result.get('success'):
                self.stats['synced_to_zoho'] += 1
                
                # Update transaction-specific stats
                if transaction_type == 'purchase':
                    self.stats['purchase_orders_created'] += 1
                    if zoho_result.get('bill_id'):
                        self.stats['bills_created'] += 1
                else:  # sale
                    self.stats['sales_orders_created'] += 1
                    if zoho_result.get('invoice_id'):
                        self.stats['invoices_created'] += 1
                    if zoho_result.get('shipment_id'):
                        self.stats['shipments_created'] += 1
                
                logger.info(f"Zoho workflow SUCCESS:")
                
                # Log workflow steps
                for step in zoho_result.get('workflow_steps', []):
                    logger.info(f"   - {step}")
                
                # Mark Airtable record as synced
                if hasattr(self.airtable, 'mark_record_synced_to_zoho'):
                    self.airtable.mark_record_synced_to_zoho(
                        transaction_record_id,
                        transaction_type,
                        zoho_result
                    )
                
                # Send enhanced success notification
                self._send_enhanced_success_notification(airtable_result, zoho_result, transaction_type)
                
            else:
                logger.error(f"Zoho workflow FAILED: {'; '.join(zoho_result.get('errors', []))}")
                self.stats['errors'] += 1
                
                # Mark as failed in Airtable
                if hasattr(self.airtable, 'mark_record_zoho_failed'):
                    self.airtable.mark_record_zoho_failed(
                        transaction_record_id,
                        transaction_type,
                        zoho_result.get('errors', [])
                    )
                
                # Send error notification
                self._send_zoho_error_notification(airtable_result, zoho_result, transaction_type)
                
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"Zoho workflow execution failed: {e}", exc_info=True)
            
            # Mark as failed in Airtable
            if hasattr(self.airtable, 'mark_record_zoho_failed'):
                self.airtable.mark_record_zoho_failed(
                    transaction_record_id,
                    transaction_type,
                    [f"Workflow execution error: {e}"]
                )
            
            if hasattr(self.discord, 'send_error_notification'):
                self.discord.send_error_notification(
                    "Zoho Workflow Failed",
                    f"Failed to execute Zoho workflow for {transaction_type}",
                    {"error": str(e), "airtable_record": transaction_record_id}
                )

    def _build_clean_data_from_airtable(self, airtable_result: Dict, transaction_type: str) -> Dict:
        """Build clean data structure for Zoho using Airtable as single source of truth."""
        clean_data = {
            'type': transaction_type,
            'order_number': airtable_result.get('order_number'),
            'date': airtable_result.get('date'),
            'items': []
        }
        
        # Add transaction-specific fields
        if transaction_type == 'purchase':
            clean_data['vendor_name'] = airtable_result.get('vendor_name')
            clean_data['taxes'] = airtable_result.get('taxes', 0)
            clean_data['shipping'] = airtable_result.get('shipping', 0)
        else:  # sale
            clean_data['channel'] = airtable_result.get('channel')
            clean_data['customer_email'] = airtable_result.get('customer_email')
            clean_data['taxes'] = airtable_result.get('taxes', 0)
            clean_data['fees'] = airtable_result.get('fees', 0)
        
        # Build clean item list with guaranteed SKUs from Airtable
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
        
        return clean_data

    def _process_incomplete_transaction(self, data: Dict, transaction_type: str, parse_result: ParseResult):
        """Process incomplete data through Airtable-only workflow."""
        order_number = data.get('order_number', 'N/A')
        
        try:
            logger.info(f"Saving incomplete {transaction_type} to Airtable for review...")
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
            
            logger.info(f"Incomplete data saved in {airtable_duration:.2f}s")
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
                self.stats['human_reviews_required'] += 1
                
                logger.info(f"Added to review queue:")
                logger.info(f"   - Missing: {', '.join(parse_result.missing_fields[:5])}")
                logger.info(f"   - Total pending: {self.stats['human_reviews_required']}")
                
            # Send human review notification using enhanced Discord notifier
            if hasattr(self.discord, 'send_human_review_notification'):
                self.discord.send_human_review_notification(
                    transaction_type,
                    order_number,
                    parse_result.missing_fields,
                    airtable_id,
                    parse_result.confidence
                )
            
            logger.info(f"SKIPPING inventory and Zoho processing - data incomplete")
            
        except Exception as e:
            error_msg = f"Error processing incomplete data for {order_number}: {e}"
            logger.error(error_msg, exc_info=True)
            if hasattr(self.discord, 'send_error_notification'):
                self.discord.send_error_notification(
                    "Incomplete Data Processing Failed",
                    error_msg,
                    {'transaction_type': transaction_type, 'order_number': order_number}
                )

    def _send_enhanced_success_notification(self, airtable_result: Dict, zoho_result: Dict, transaction_type: str):
        """Send enhanced success notification with workflow details."""
        if transaction_type == 'purchase':
            if hasattr(self.discord, 'send_purchase_order_success'):
                self.discord.send_purchase_order_success(airtable_result, zoho_result)
        else:  # sale
            if hasattr(self.discord, 'send_sales_order_success'):
                self.discord.send_sales_order_success(airtable_result, zoho_result)

    def _send_zoho_error_notification(self, airtable_result: Dict, zoho_result: Dict, transaction_type: str):
        """Send enhanced error notification for Zoho workflow failures."""
        order_number = airtable_result.get('order_number', 'Unknown')
        
        if transaction_type == 'purchase':
            vendor = airtable_result.get('vendor_name', 'Unknown')
            context = {"Vendor": vendor}
            workflow_stage = "Purchase Order Creation"
        else:
            channel = airtable_result.get('channel', 'Unknown')
            context = {"Channel": channel}
            workflow_stage = "Sales Order Creation"
        
        if hasattr(self.discord, 'send_workflow_error'):
            self.discord.send_workflow_error(
                transaction_type,
                order_number,
                workflow_stage,
                zoho_result,
                context
            )

    def run_once(self) -> None:
        """Run a single iteration of email processing."""
        try:
            logger.info("Checking for new emails...")
            
            # Fetch all unread emails
            new_emails = self.gmail.fetch_unread_emails()
            
            if not new_emails:
                logger.debug("No new emails found")
                return
                
            logger.info(f"Found {len(new_emails)} new emails to process")
            
            for i, email in enumerate(new_emails, 1):
                seq_num = email.get('seq_num')
                subject = email.get('subject', 'No Subject')[:100]
                
                logger.info(f"[{i}/{len(new_emails)}] Processing email [seq={seq_num}]: {subject}")
                
                if seq_num and seq_num not in self.processed_seq_nums:
                    # Process the email
                    self.process_email(email)
                    
                    # Mark as processed in Gmail
                    logger.info(f"Marking email [seq={seq_num}] as processed in Gmail...")
                    if hasattr(self.gmail, 'mark_as_processed'):
                        mark_success = self.gmail.mark_as_processed(seq_num)
                        
                        if mark_success:
                            logger.info(f"Email [seq={seq_num}] successfully marked as processed")
                            self.processed_seq_nums.add(seq_num)
                        else:
                            logger.warning(f"Failed to mark email [seq={seq_num}] as processed in Gmail")
                            # Still add to processed set to avoid reprocessing
                            self.processed_seq_nums.add(seq_num)
                    else:
                        # If mark_as_processed doesn't exist, just track locally
                        self.processed_seq_nums.add(seq_num)
                        
                else:
                    logger.info(f"Email [seq={seq_num}] already processed, skipping")
                    
            logger.info(f"Completed processing {len(new_emails)} emails")
            
            # Check for resolved human reviews
            self._process_pending_reviews()
                    
        except Exception as e:
            error_msg = f"Error in run cycle: {str(e)}"
            logger.error(error_msg, exc_info=True)
            if hasattr(self.discord, 'send_error_notification'):
                self.discord.send_error_notification(
                    "Email Processing Cycle Failed",
                    error_msg,
                    {}
                )

    def _process_pending_reviews(self):
        """Check for resolved human reviews and process them."""
        if not self.pending_reviews:
            return
        
        logger.debug(f"Checking {len(self.pending_reviews)} pending reviews...")
        
        resolved_reviews = []
        
        for record_id, review_data in self.pending_reviews.items():
            try:
                # Check if record has been marked as resolved
                if hasattr(self.airtable, 'get_record'):
                    record = self.airtable.get_record(record_id, review_data['type'])
                    
                    if record and not record.get('requires_review', True):
                        logger.info(f"Human review resolved: {record_id}")
                        
                        # Process as complete transaction
                        transaction_type = review_data['type']
                        
                        # Update the data with resolved information
                        updated_data = {**review_data['data'], **record}
                        
                        # Create a new parse result for complete data
                        parse_result = ParseResult(
                            status=ParseStatus.SUCCESS,
                            completeness=DataCompleteness.COMPLETE,
                            data=updated_data,
                            confidence=1.0,
                            missing_fields=[],
                            errors=[]
                        )
                        
                        self._process_complete_transaction(updated_data, transaction_type, parse_result)
                        resolved_reviews.append(record_id)
                    
            except Exception as e:
                logger.error(f"Error checking review {record_id}: {e}")
        
        # Remove resolved reviews
        for record_id in resolved_reviews:
            del self.pending_reviews[record_id]
        
        if resolved_reviews:
            logger.info(f"Processed {len(resolved_reviews)} resolved reviews")

    def run(self) -> None:
        """Main run loop with proper workflow support."""
        logger.info("Starting Inventory Reconciliation App")
        logger.info("Architecture: Sequential Airtable → Zoho with Proper Purchase/Sales Orders")
        
        # Send startup notification using enhanced Discord notifier
        if hasattr(self.discord, 'send_info_notification'):
            self.discord.send_info_notification(
                "Inventory System Started",
                "System initialized with proper Purchase/Sales Order workflows",
                {
                    "Proper Workflows": "Enabled" if self.zoho.use_proper_workflows else "Disabled",
                    "Auto Bills": "Yes" if self.zoho.auto_create_bills else "No",
                    "Auto Invoices": "Yes" if self.zoho.auto_create_invoices else "No",
                    "Auto Shipments": "Yes" if self.zoho.auto_create_shipments else "No",
                    "Direct Adjustments": "Disabled" if not self.zoho.allow_direct_adjustments else "Enabled",
                    "Gmail": "Connected",
                    "Airtable": "3-table architecture",
                    "Zoho": "Connected" if self.zoho.is_available else "Unavailable"
                }
            )
        
        poll_interval = self.config.get_int('POLL_INTERVAL')
        logger.info(f"Email polling interval: {poll_interval} seconds")
        
        try:
            cycle_count = 0
            while True:
                try:
                    cycle_count += 1
                    logger.info(f"Starting email check cycle #{cycle_count}")
                    
                    cycle_start = time.time()
                    self.run_once()
                    cycle_duration = time.time() - cycle_start
                    
                    logger.info(f"Cycle #{cycle_count} completed in {cycle_duration:.2f}s")
                    
                    # Periodic status report and validation
                    if self.stats['emails_processed'] > 0 and self.stats['emails_processed'] % 25 == 0:
                        logger.info(f"Milestone reached: {self.stats['emails_processed']} emails processed")
                        self._send_status_report()
                        
                    # Periodic validation (every hour - 12 cycles if 5min intervals)
                    if cycle_count % 12 == 0 and cycle_count > 0:
                        self._run_periodic_validation()
                        
                    logger.info(f"Sleeping for {poll_interval} seconds until next cycle...")
                    time.sleep(poll_interval)
                    
                except KeyboardInterrupt:
                    logger.info("Shutdown requested by user")
                    break
                    
                except Exception as e:
                    logger.error(f"Unexpected error in cycle #{cycle_count}: {str(e)}", exc_info=True)
                    self.stats['errors'] += 1
                    
                    # Send error notification but continue running
                    if hasattr(self.discord, 'send_error_notification'):
                        self.discord.send_error_notification(
                            "Processing Cycle Error",
                            f"Cycle #{cycle_count} failed but system continues",
                            {"error": str(e), "cycle": cycle_count}
                        )
                    
                    logger.info(f"Waiting {poll_interval}s before retry...")
                    time.sleep(poll_interval)
                    
        except Exception as e:
            logger.critical(f"Critical error - application stopping: {e}", exc_info=True)
            if hasattr(self.discord, 'send_error_notification'):
                self.discord.send_error_notification(
                    "Critical Application Error",
                    f"Application crashed: {e}",
                    {"stats": self.stats}
                )
            raise
        
        finally:
            # Cleanup and final reporting
            self._shutdown_cleanup()

    def _run_periodic_validation(self):
        """Run periodic system validation checks."""
        logger.info("Running periodic system validation...")
        
        try:
            # Check that inventory adjustments tab is clean (only if using proper workflows)
            if self.zoho.use_proper_workflows and hasattr(self.zoho, 'validate_inventory_adjustments_empty'):
                adjustment_check = self.zoho.validate_inventory_adjustments_empty()
                
                if adjustment_check.get('is_clean', True):
                    logger.info("Inventory adjustments tab is clean")
                else:
                    auto_adjustments = adjustment_check.get('auto_adjustments', 0)
                    logger.warning(f"Found {auto_adjustments} auto-generated adjustments - should be zero with proper workflows")
                    
                    # Send validation alert using enhanced Discord notifier
                    if hasattr(self.discord, 'send_validation_alert'):
                        self.discord.send_validation_alert("inventory_adjustments", adjustment_check)
            
            # Generate inventory sync report
            if hasattr(self.zoho, 'generate_inventory_sync_report'):
                sync_report = self.zoho.generate_inventory_sync_report()
                
                if sync_report.get('discrepancies'):
                    logger.warning(f"Found {len(sync_report['discrepancies'])} inventory discrepancies")
                    
                    # Send discrepancy notification if significant
                    if len(sync_report['discrepancies']) > 5 and hasattr(self.discord, 'send_validation_alert'):
                        self.discord.send_validation_alert("inventory_sync", sync_report)
            
        except Exception as e:
            logger.error(f"Validation check failed: {e}")

    def _send_status_report(self):
        """Send current status report to Discord using enhanced notifier."""
        runtime = (datetime.now() - self.stats['session_start']).total_seconds()
        
        details = {
            "Runtime": f"{runtime/3600:.2f} hours",
            "Emails Processed": self.stats['emails_processed'],
            "Parse Success": self.stats['parse_successful'],
            "Parse Failed": self.stats['parse_failed'],
            "Complete Data": self.stats['complete_data'],
            "Incomplete Data": self.stats['incomplete_data'],
            "Airtable Records": self.stats['airtable_saved'],
            "Purchase Orders": self.stats['purchase_orders_created'],
            "Sales Orders": self.stats['sales_orders_created'],
            "Bills Created": self.stats['bills_created'],
            "Invoices Created": self.stats['invoices_created'],
            "Shipments Created": self.stats['shipments_created'],
            "Zoho Synced": self.stats['synced_to_zoho'],
            "Pending Review": self.stats['human_reviews_required'],
            "Errors": self.stats['errors']
        }
        
        if self.stats['emails_processed'] > 0:
            complete_rate = (self.stats['complete_data'] / self.stats['emails_processed']) * 100
            details["Data Completeness Rate"] = f"{complete_rate:.1f}%"
            
            if self.zoho.is_available and self.stats['airtable_saved'] > 0:
                sync_rate = (self.stats['synced_to_zoho'] / self.stats['airtable_saved']) * 100
                details["Zoho Sync Rate"] = f"{sync_rate:.1f}%"
        
        if hasattr(self.discord, 'send_info_notification'):
            self.discord.send_info_notification(
                "System Status Report",
                "Periodic status update from inventory system",
                details
            )

    def _shutdown_cleanup(self):
        """Handle shutdown cleanup and final reporting."""
        logger.info("Cleaning up resources...")
        
        try:
            if hasattr(self.gmail, 'close'):
                self.gmail.close()
                logger.info("Gmail connection closed")
        except Exception as e:
            logger.error(f"Error closing Gmail connection: {e}")
        
        # Send final report
        logger.info("Generating final session report...")
        
        # Final shutdown notification using enhanced Discord notifier
        runtime = (datetime.now() - self.stats['session_start']).total_seconds()
        
        final_stats = {
            "Total Runtime": f"{runtime/3600:.2f} hours",
            "Emails Processed": self.stats['emails_processed'],
            "Purchase Orders": self.stats['purchase_orders_created'],
            "Sales Orders": self.stats['sales_orders_created'],
            "Bills Created": self.stats['bills_created'],
            "Invoices Created": self.stats['invoices_created'],
            "Shipments Created": self.stats['shipments_created'],
            "Airtable Records": self.stats['airtable_saved'],
            "Zoho Synced": self.stats['synced_to_zoho'],
            "Human Reviews": self.stats['human_reviews_required'],
            "Total Errors": self.stats['errors'],
            "System Mode": "Proper Workflows" if self.zoho.use_proper_workflows else "Legacy Adjustments"
        }
        
        if hasattr(self.discord, 'send_info_notification'):
            self.discord.send_info_notification(
                "System Shutdown",
                "Inventory reconciliation system stopped gracefully",
                final_stats
            )
        
        logger.info("Shutdown complete")


def main():
    """Main entry point."""
    try:
        app = InventoryReconciliationApp()
        app.run()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.critical(f"Application crashed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()