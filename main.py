#!/usr/bin/env python3
"""
Main application entry point for inventory reconciliation system.
Continuously monitors Gmail for new purchase/sales emails and syncs to Zoho/Airtable.
"""

import os
import time
import logging
import sys
from datetime import datetime
from typing import Dict, List, Optional, Any

from src.config import Config
from src.gmail_client import GmailClient
from src.openai_parser import EmailParser, ParseStatus, ParseResult
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


class InventoryReconciliationApp:
    """Main application orchestrator."""
    
    def __init__(self):
        """Initialize all service clients."""
        self.config = Config()
        self.gmail = GmailClient(self.config)
        self.parser = EmailParser(self.config)
        self.airtable = AirtableClient(self.config)
        self.zoho = ZohoClient(self.config)
        self.discord = DiscordNotifier(self.config)
        self.processed_uids = set()
        
        # Statistics tracking
        self.stats = {
            'processed': 0,
            'successes': 0,
            'warnings': 0,
            'errors': 0,
            'session_start': datetime.now()
        }
        
    def process_email(self, email_data: Dict) -> None:
        """
        Process a single email through the entire pipeline.
        
        Args:
            email_data: Dictionary containing email metadata and content
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
                self.stats['warnings'] += 1
                return
                
            # Add email metadata
            parsed_data['email_uid'] = email_data['uid']
            parsed_data['email_date'] = email_data['date']
            parsed_data['parse_result'] = parse_result.to_dict()
            
            # Step 2: Determine transaction type
            transaction_type = parsed_data.get('type')
            
            # Check if review is required
            if parse_result.requires_review:
                logger.warning(f"Email requires manual review: {email_data['subject']}")
                self.discord.send_warning(
                    f"Email requires manual review\n"
                    f"Subject: {email_data['subject'][:100]}\n"
                    f"Missing fields: {', '.join(parse_result.missing_fields)}\n"
                    f"Confidence: {parse_result.confidence_score:.2f}"
                )
                self.stats['warnings'] += 1
                
            # Process based on type
            if transaction_type == 'purchase':
                success = self._process_purchase(parsed_data, parse_result)
            elif transaction_type == 'sale':
                success = self._process_sale(parsed_data, parse_result)
            else:
                logger.warning(f"Unknown transaction type: {transaction_type}")
                self.stats['warnings'] += 1
                success = False
                
            # Update statistics
            if success:
                self.stats['successes'] += 1
            elif parse_result.status == ParseStatus.PARTIAL:
                self.stats['warnings'] += 1
                
        except Exception as e:
            error_msg = f"Error processing email {email_data.get('uid')}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.discord.send_error(error_msg, email_data)
            self.stats['errors'] += 1
            
    def _process_purchase(self, data: Dict, parse_result: ParseResult) -> bool:
        """
        Process a purchase transaction with complete workflow.
        
        Returns:
            True if successful, False otherwise
        """
        airtable_record_id = None
        zoho_result = {'errors': [], 'inventory_updated': False}
        overall_success = True
        
        try:
            # Step 1: Save to Airtable with review flag
            logger.info("Saving purchase to Airtable")
            data['requires_review'] = parse_result.requires_review
            data['confidence_score'] = parse_result.confidence_score
            data['zoho_status'] = 'pending'
            
            airtable_record = self.airtable.create_purchase(data)
            airtable_record_id = airtable_record.get('id')
            
            # Step 2: Process in Zoho if confidence is sufficient
            if not parse_result.requires_review or parse_result.confidence_score > 0.8:
                logger.info("Processing complete purchase workflow in Zoho")
                
                # Add parse metadata for Zoho
                parse_metadata = {
                    'confidence_score': parse_result.confidence_score,
                    'missing_fields': parse_result.missing_fields,
                    'requires_review': parse_result.requires_review
                }
                
                try:
                    zoho_result = self.zoho.process_purchase_complete(data, parse_metadata)
                    
                    # Update Airtable with Zoho status
                    if airtable_record_id:
                        update_data = {
                            'zoho_status': 'success' if zoho_result.get('inventory_updated') else 'partial',
                            'zoho_po_id': zoho_result.get('purchase_order_id'),
                            'zoho_bill_id': zoho_result.get('bill_id')
                        }
                        # self.airtable.update_record(airtable_record_id, update_data)
                        
                except Exception as e:
                    logger.error(f"Zoho processing failed: {e}")
                    zoho_result['errors'].append(str(e))
                    overall_success = False
                    
                    # Update Airtable with failure status
                    if airtable_record_id:
                        update_data = {'zoho_status': 'failed', 'zoho_error': str(e)}
                        # self.airtable.update_record(airtable_record_id, update_data)
                        
            else:
                logger.info("Skipping Zoho update due to low confidence/missing data")
                
            # Step 3: Build and send notification
            status_parts = []
            if zoho_result.get('purchase_order_id'):
                status_parts.append("PO created")
            if zoho_result.get('bill_id'):
                status_parts.append("Bill created")
            if zoho_result.get('inventory_updated'):
                status_parts.append("Inventory updated")
                
            status_text = " → ".join(status_parts) if status_parts else "Saved for review"
            
            # Determine status emoji based on actual results
            if zoho_result.get('errors'):
                status_emoji = "❌"
                overall_success = False
            elif zoho_result.get('inventory_updated'):
                status_emoji = "✅"
            elif parse_result.requires_review:
                status_emoji = "⚠️"
            else:
                status_emoji = "✅"
                
            # Build notification message
            success_msg = (
                f"{status_emoji} Purchase: {status_text}\n"
                f"Order #: {data.get('order_number', 'N/A')}\n"
                f"Vendor: {data.get('vendor_name', 'Unknown')}\n"
                f"Items: {len(data.get('items', []))}\n"
                f"Subtotal: ${data.get('subtotal', 0):.2f}\n"
                f"Tax: ${data.get('taxes', 0):.2f}\n"
                f"Total: ${data.get('total', 0):.2f}\n"
                f"Confidence: {parse_result.confidence_score:.2f}"
            )
            
            if parse_result.missing_fields:
                success_msg += f"\nMissing: {', '.join(parse_result.missing_fields[:3])}"
                
            if zoho_result.get('errors'):
                success_msg += f"\n⚠️ Issues: {', '.join(zoho_result['errors'][:2])}"
                
            # Send appropriate notification type
            if zoho_result.get('errors'):
                self.discord.send_error(success_msg)
            elif parse_result.requires_review:
                self.discord.send_warning(success_msg)
            else:
                self.discord.send_success(success_msg)
                
        except Exception as e:
            error_msg = f"Failed to process purchase: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.discord.send_error(error_msg, data)
            overall_success = False
            
        return overall_success
            
    def _process_sale(self, data: Dict, parse_result: ParseResult) -> bool:
        """
        Process a sales transaction with complete workflow.
        
        Returns:
            True if successful, False otherwise
        """
        airtable_record_id = None
        zoho_result = {'errors': [], 'inventory_updated': False}
        overall_success = True
        
        try:
            # Step 1: Save to Airtable with review flag
            logger.info("Saving sale to Airtable")
            data['requires_review'] = parse_result.requires_review
            data['confidence_score'] = parse_result.confidence_score
            data['zoho_status'] = 'pending'
            
            airtable_record = self.airtable.create_sale(data)
            airtable_record_id = airtable_record.get('id')
            
            # Step 2: Process in Zoho if confidence is sufficient
            if not parse_result.requires_review or parse_result.confidence_score > 0.8:
                logger.info("Processing complete sales workflow in Zoho")
                
                # Add parse metadata for Zoho
                parse_metadata = {
                    'confidence_score': parse_result.confidence_score,
                    'missing_fields': parse_result.missing_fields,
                    'requires_review': parse_result.requires_review
                }
                
                try:
                    zoho_result = self.zoho.process_sale_complete(data, parse_metadata)
                    
                    # Update Airtable with Zoho status
                    if airtable_record_id:
                        update_data = {
                            'zoho_status': 'success' if zoho_result.get('inventory_updated') else 'partial',
                            'zoho_so_id': zoho_result.get('sales_order_id'),
                            'zoho_invoice_id': zoho_result.get('invoice_id'),
                            'zoho_shipment_id': zoho_result.get('shipment_id')
                        }
                        # self.airtable.update_record(airtable_record_id, update_data)
                        
                except Exception as e:
                    logger.error(f"Zoho processing failed: {e}")
                    zoho_result['errors'].append(str(e))
                    overall_success = False
                    
                    # Update Airtable with failure status
                    if airtable_record_id:
                        update_data = {'zoho_status': 'failed', 'zoho_error': str(e)}
                        # self.airtable.update_record(airtable_record_id, update_data)
                        
            else:
                logger.info("Skipping Zoho update due to low confidence/missing data")
                
            # Step 3: Build and send notification
            status_parts = []
            if zoho_result.get('sales_order_id'):
                status_parts.append("SO created")
            if zoho_result.get('invoice_id'):
                status_parts.append("Invoice created")
            if zoho_result.get('shipment_id'):
                status_parts.append("Shipped")
            if zoho_result.get('inventory_updated'):
                status_parts.append("Inventory updated")
                
            status_text = " → ".join(status_parts) if status_parts else "Saved for review"
            
            # Determine status emoji based on actual results
            if zoho_result.get('errors'):
                status_emoji = "❌"
                overall_success = False
            elif zoho_result.get('inventory_updated'):
                status_emoji = "✅"
            elif parse_result.requires_review:
                status_emoji = "⚠️"
            else:
                status_emoji = "✅"
                
            # Build notification message
            success_msg = (
                f"{status_emoji} Sale: {status_text}\n"
                f"Order #: {data.get('order_number', 'N/A')}\n"
                f"Channel: {data.get('channel', 'Unknown')}\n"
                f"Items: {len(data.get('items', []))}\n"
                f"Subtotal: ${data.get('subtotal', 0):.2f}\n"
                f"Tax: ${data.get('taxes', 0):.2f}\n"
                f"Fees: ${data.get('fees', 0):.2f}\n"
                f"Total: ${data.get('total', 0):.2f}\n"
                f"Confidence: {parse_result.confidence_score:.2f}"
            )
            
            if parse_result.missing_fields:
                success_msg += f"\nMissing: {', '.join(parse_result.missing_fields[:3])}"
                
            if zoho_result.get('errors'):
                success_msg += f"\n⚠️ Issues: {', '.join(zoho_result['errors'][:2])}"
                
            # Send appropriate notification type
            if zoho_result.get('errors'):
                self.discord.send_error(success_msg)
            elif parse_result.requires_review:
                self.discord.send_warning(success_msg)
            else:
                self.discord.send_success(success_msg)
                
        except Exception as e:
            error_msg = f"Failed to process sale: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.discord.send_error(error_msg, data)
            overall_success = False
            
        return overall_success
            
    def run_once(self) -> None:
        """Run a single iteration of email processing."""
        try:
            logger.info("Checking for new emails...")
            new_emails = self.gmail.fetch_unread_emails()
            
            if not new_emails:
                logger.debug("No new emails found")
                return
                
            logger.info(f"Found {len(new_emails)} new emails")
            
            batch_start = time.time()
            batch_successes = 0
            batch_warnings = 0
            batch_errors = 0
            
            for email in new_emails:
                if email['uid'] not in self.processed_uids:
                    initial_stats = dict(self.stats)
                    
                    self.process_email(email)
                    self.processed_uids.add(email['uid'])
                    self.gmail.mark_as_processed(email['uid'])
                    
                    # Track batch statistics
                    if self.stats['successes'] > initial_stats['successes']:
                        batch_successes += 1
                    if self.stats['warnings'] > initial_stats['warnings']:
                        batch_warnings += 1
                    if self.stats['errors'] > initial_stats['errors']:
                        batch_errors += 1
                        
            # Send batch summary if multiple emails processed
            if len(new_emails) > 1:
                batch_time = time.time() - batch_start
                self.discord.send_batch_summary(
                    successes=batch_successes,
                    warnings=batch_warnings,
                    errors=batch_errors,
                    details={
                        'Processing Time': f"{batch_time:.2f}s",
                        'Emails/sec': f"{len(new_emails) / batch_time:.2f}"
                    }
                )
                    
        except Exception as e:
            error_msg = f"Error in run cycle: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.discord.send_error(error_msg)
            
    def run(self) -> None:
        """Main run loop."""
        logger.info("Starting Inventory Reconciliation App")
        
        # Test connections if in debug mode
        if self.config.get_bool('ENABLE_CONNECTION_TEST'):
            logger.info("Testing connections to external services...")
            connection_status = self.config.validate_connections()
            for service, status in connection_status.items():
                if status:
                    logger.info(f"✅ {service}: Connected")
                else:
                    logger.warning(f"❌ {service}: Connection failed")
        
        self.discord.send_info("🚀 Inventory Reconciliation App started")
        
        poll_interval = self.config.get_int('POLL_INTERVAL')  # Properly typed as int
        
        try:
            while True:
                try:
                    self.run_once()
                    
                    # Log statistics periodically
                    if self.stats['processed'] > 0 and self.stats['processed'] % 100 == 0:
                        self._log_statistics()
                        
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
            # Ensure proper cleanup
            logger.info("Cleaning up resources...")
            self.gmail.close()
            
            # Send final statistics
            self._send_final_statistics()
            
            self.discord.send_info("⏹️ Inventory Reconciliation App stopped")
            logger.info("Shutdown complete")
            
    def _log_statistics(self):
        """Log current processing statistics."""
        runtime = (datetime.now() - self.stats['session_start']).total_seconds()
        
        logger.info(
            f"Session Statistics: "
            f"Processed: {self.stats['processed']}, "
            f"Success: {self.stats['successes']}, "
            f"Warnings: {self.stats['warnings']}, "
            f"Errors: {self.stats['errors']}, "
            f"Runtime: {runtime/3600:.2f} hours"
        )
        
    def _send_final_statistics(self):
        """Send final session statistics to Discord."""
        runtime = (datetime.now() - self.stats['session_start']).total_seconds()
        
        stats_message = (
            f"Session ended after {runtime/3600:.2f} hours\n"
            f"Total processed: {self.stats['processed']}\n"
            f"Successes: {self.stats['successes']}\n"
            f"Warnings: {self.stats['warnings']}\n"
            f"Errors: {self.stats['errors']}"
        )
        
        if self.stats['processed'] > 0:
            success_rate = (self.stats['successes'] / self.stats['processed']) * 100
            stats_message += f"\nSuccess rate: {success_rate:.1f}%"
            
        self.discord.send_info(stats_message, title="Session Statistics")


if __name__ == "__main__":
    app = InventoryReconciliationApp()
    app.run()