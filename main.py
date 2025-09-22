#!/usr/bin/env python3
"""
Main application with sequential Airtable → Zoho workflow using proper Purchase/Sales Orders.
Enhanced with proper accounting workflows and FIFO COGS tracking.
Updated to integrate with new proper workflow ZohoClient.
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
        logger.info("🚀 Starting Inventory Reconciliation System (Proper Workflows)")
        logger.info("📊 Architecture: Sequential Airtable → Zoho with Purchase Orders & Sales Orders")
        
        # Load configuration
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
                logger.info("✅ Zoho client initialized with proper workflows")
                logger.info(f"   - Proper Workflows: {self.zoho.use_proper_workflows}")
                logger.info(f"   - Auto Create Bills: {self.zoho.auto_create_bills}")
                logger.info(f"   - Auto Create Invoices: {self.zoho.auto_create_invoices}")
                logger.info(f"   - Auto Create Shipments: {self.zoho.auto_create_shipments}")
                logger.info(f"   - Allow Direct Adjustments: {self.zoho.allow_direct_adjustments}")
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
        
        logger.info("🎉 All components initialized successfully!")
        
        # Log system configuration
        self._log_system_status()

    def _log_system_status(self):
        """Log current system configuration and status."""
        logger.info("📊 System Configuration:")
        logger.info(f"   - Proper Workflows: {self.zoho.use_proper_workflows}")
        logger.info(f"   - Auto Create Bills: {self.zoho.auto_create_bills}")
        logger.info(f"   - Auto Create Invoices: {self.zoho.auto_create_invoices}")
        logger.info(f"   - Auto Create Shipments: {self.zoho.auto_create_shipments}")
        logger.info(f"   - Allow Direct Adjustments: {self.zoho.allow_direct_adjustments}")
        
        logger.info("🔗 Service Status:")
        logger.info(f"   - Gmail: {'✅ Connected' if self.gmail.test_connection() else '❌ Failed'}")
        logger.info(f"   - OpenAI: {'✅ Available' if self.parser.test_connection() else '❌ Failed'}")
        logger.info(f"   - Airtable: {'✅ Connected' if self.airtable.test_connection() else '❌ Failed'}")
        logger.info(f"   - Zoho: {'✅ Connected' if self.zoho.is_available else '❌ Failed'}")
        logger.info(f"   - Discord: {'✅ Ready' if self.discord.test_webhook() else '❌ Failed'}")

    def process_email(self, email_data: Dict) -> None:
        """
        Process email with sequential Airtable → Zoho workflow using proper Purchase/Sales Orders.
        
        New Workflow:
        1. Parse email with OpenAI
        2. Process through Airtable (3-table architecture)
        3. If complete and Zoho available → Execute proper workflow (PO/SO/Bills/Invoices)
        4. If incomplete → Flag for human review
        """
        seq_num = email_data.get('seq_num', 'unknown')
        subject = email_data.get('subject', 'No Subject')[:100]
        
        logger.info(f"📧 Processing email [seq={seq_num}]: {subject}")
        
        try:
            self.stats['emails_processed'] += 1
            
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
                self.discord.send_error_notification(
                    "Email Parsing Failed",
                    f"Failed to parse email: {subject}",
                    {'errors': parse_result.errors, 'seq_num': seq_num}
                )
                self.stats['parse_failed'] += 1
                return
                
            if parse_result.status == ParseStatus.NOT_INVENTORY:
                logger.info(f"ℹ️ Email not related to inventory - skipping: {subject}")
                return
                
            # Extract parsed data
            parsed_data = parse_result.data
            if not parsed_data:
                logger.warning(f"⚠️ No data extracted from email: {subject}")
                self.stats['errors'] += 1
                return
                
            self.stats['parse_successful'] += 1
            
            transaction_type = parsed_data.get('type', 'unknown')
            order_number = parsed_data.get('order_number', 'N/A')
            confidence = parse_result.confidence
            
            logger.info(f"📊 Parse Results:")
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
                logger.info(f"✅ Data is COMPLETE - processing through full workflow")
                self._process_complete_transaction(parsed_data, transaction_type, parse_result)
                
            elif parse_result.completeness == DataCompleteness.INCOMPLETE:
                logger.info(f"⚠️ Data is INCOMPLETE - saving to Airtable for review")
                self._process_incomplete_transaction(parsed_data, transaction_type, parse_result)
                self.stats['incomplete_data'] += 1
                
            else:
                logger.error(f"❌ Invalid data completeness: {parse_result.completeness}")
                self.stats['errors'] += 1
                
        except Exception as e:
            error_msg = f"💥 Error processing email [seq={seq_num}]: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.discord.send_error_notification(
                "Email Processing Error",
                error_msg,
                {'seq_num': seq_num, 'subject': subject}
            )
            self.stats['errors'] += 1

    def _log_system_status(self):
        """Log current system configuration and status."""
        logger.info("📊 System Configuration:")
        logger.info(f"   - Proper Workflows: {self.zoho.use_proper_workflows}")
        logger.info(f"   - Auto Create Bills: {self.zoho.auto_create_bills}")
        logger.info(f"   - Auto Create Invoices: {self.zoho.auto_create_invoices}")
        logger.info(f"   - Auto Create Shipments: {self.zoho.auto_create_shipments}")
        logger.info(f"   - Allow Direct Adjustments: {self.zoho.allow_direct_adjustments}")
        
        logger.info("🔗 Service Status:")
        logger.info(f"   - Gmail: {'✅ Connected' if self.gmail.test_connection() else '❌ Failed'}")
        logger.info(f"   - OpenAI: {'✅ Available' if self.parser.test_connection() else '❌ Failed'}")
        logger.info(f"   - Airtable: {'✅ Connected' if self.airtable.test_connection() else '❌ Failed'}")
        logger.info(f"   - Zoho: {'✅ Connected' if self.zoho.is_available else '❌ Failed'}")
        logger.info(f"   - Discord: {'✅ Ready' if self.discord.test_webhook() else '❌ Failed'}")

    def run(self):
        """Main application loop."""
        logger.info("🔄 Starting main processing loop...")
        
        # Send startup notification
        self._send_startup_notification()
        
        poll_interval = self.config.get_int('POLL_INTERVAL', 300)
        
        try:
            while True:
                try:
                    self._process_emails()
                    self._process_pending_reviews()
                    
                    # Periodic validation (every hour)
                    if self.stats['emails_processed'] % 12 == 0 and self.stats['emails_processed'] > 0:
                        self._run_periodic_validation()
                    
                    logger.info(f"😴 Sleeping for {poll_interval} seconds...")
                    time.sleep(poll_interval)
                    
                except KeyboardInterrupt:
                    logger.info("👋 Received shutdown signal")
                    break
                except Exception as e:
                    self.stats['errors'] += 1
                    logger.error(f"💥 Unexpected error in main loop: {e}", exc_info=True)
                    
                    # Send error notification
                    self.discord.send_error_notification(
                        "Main Loop Error",
                        str(e),
                        {"stats": self.stats}
                    )
                    
                    # Wait before retrying
                    time.sleep(min(poll_interval, 60))
                    
        except Exception as e:
            logger.critical(f"💀 Critical error