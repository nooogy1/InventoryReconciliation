"""Gmail client using consistent IMAP sequence numbers for stability."""

import imaplib
import email
import logging
import time
import chardet
import html2text
from typing import List, Dict, Optional, Set, Tuple
from datetime import datetime, timedelta
from email.header import decode_header
from contextlib import contextmanager
from threading import Lock

logger = logging.getLogger(__name__)


class GmailClient:
    """Handle Gmail IMAP operations using sequence numbers consistently."""
    
    def __init__(self, config):
        self.config = config
        self.imap = None
        self.processed_seq_nums: Set[str] = set()  # Track by sequence number
        self.connection_lock = Lock()
        self.last_reconnect = None
        self.reconnect_delay = 5  # seconds
        self.max_fetch_batch = config.get_int('EMAIL_BATCH_SIZE', 10)
        self.html_converter = html2text.HTML2Text()
        self.html_converter.ignore_links = False
        self.html_converter.ignore_images = True
        self.processed_label_name = config.get('GMAIL_PROCESSED_LABEL', 'PROCESSED')
        
        # Connect on initialization
        self.connect()
        
    def __enter__(self):
        """Context manager entry."""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensure cleanup."""
        self.close()
        
    def connect(self, retry: bool = True) -> bool:
        """
        Connect to Gmail via IMAP with retry logic.
        
        Args:
            retry: Whether to retry on failure
            
        Returns:
            Success status
        """
        max_retries = self.config.get_int('MAX_RETRIES', 3) if retry else 1
        retry_delay = self.config.get_int('RETRY_DELAY', 5)
        
        for attempt in range(max_retries):
            try:
                with self.connection_lock:
                    # Close existing connection if any
                    if self.imap:
                        try:
                            self.imap.close()
                            self.imap.logout()
                        except:
                            pass
                    
                    # Create new connection
                    server = self.config.get('GMAIL_IMAP_SERVER', 'imap.gmail.com')
                    port = self.config.get_int('GMAIL_IMAP_PORT', 993)
                    
                    logger.info(f"Connecting to Gmail IMAP server {server}:{port} (attempt {attempt + 1}/{max_retries})")
                    
                    self.imap = imaplib.IMAP4_SSL(server, port)
                    self.imap.login(
                        self.config.get('GMAIL_USER'),
                        self.config.get('GMAIL_APP_PASSWORD')
                    )
                    
                    # Select inbox
                    status, data = self.imap.select('INBOX')
                    if status != 'OK':
                        raise Exception(f"Failed to select INBOX: {data}")
                    
                    # Clear processed sequence numbers on new connection
                    # (sequence numbers are only valid within a session)
                    self.processed_seq_nums.clear()
                    
                    # Enable Gmail extensions if available
                    if self._check_capability('X-GM-EXT-1'):
                        logger.debug("Gmail extensions enabled")
                    
                    self.last_reconnect = datetime.now()
                    logger.info("Connected to Gmail successfully")
                    return True
                    
            except imaplib.IMAP4.abort as e:
                logger.warning(f"IMAP connection aborted: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                    
            except Exception as e:
                logger.error(f"Failed to connect to Gmail (attempt {attempt + 1}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise
                    
        return False
        
    def _check_capability(self, capability: str) -> bool:
        """Check if IMAP server supports a capability."""
        try:
            capabilities = self.imap.capability()[1][0].decode()
            return capability in capabilities
        except:
            return False
            
    def ensure_connection(self) -> bool:
        """Ensure IMAP connection is active, reconnect if needed."""
        try:
            # Check if connection is alive
            if self.imap:
                self.imap.noop()
                return True
        except:
            logger.info("Connection lost, reconnecting...")
            
        # Reconnect with rate limiting
        if self.last_reconnect:
            elapsed = (datetime.now() - self.last_reconnect).total_seconds()
            if elapsed < self.reconnect_delay:
                time.sleep(self.reconnect_delay - elapsed)
                
        return self.connect()
        
    def fetch_unread_emails(self, max_emails: Optional[int] = None) -> List[Dict]:
        """
        Fetch unread emails using sequence numbers.
        
        Args:
            max_emails: Maximum number of emails to fetch (None for config default)
            
        Returns:
            List of parsed email dictionaries
        """
        emails = []
        
        if not self.ensure_connection():
            logger.error("Could not establish connection to Gmail")
            return emails
            
        max_emails = max_emails or self.max_fetch_batch
        
        try:
            # Search using standard IMAP (returns sequence numbers)
            # First try with Gmail extensions if available
            if self._check_capability('X-GM-EXT-1'):
                search_criteria = f'(UNSEEN -X-GM-LABELS "{self.processed_label_name}")'
            else:
                # Fallback to standard IMAP
                search_criteria = '(UNSEEN UNFLAGGED)'
                
            logger.debug(f"Searching with criteria: {search_criteria}")
            
            # Use standard search (returns sequence numbers)
            status, data = self.imap.search(None, search_criteria)
            
            if status != 'OK':
                logger.error(f"Search failed: {data}")
                return emails
                
            # Get sequence numbers
            seq_num_list = data[0].split()
            
            if not seq_num_list:
                logger.debug("No unread emails found")
                return emails
                
            # Limit number of emails to process
            seq_num_list = seq_num_list[:max_emails]
            logger.info(f"Found {len(seq_num_list)} unread emails to process")
            
            # Fetch emails by sequence number in batches
            batch_size = 5  # Fetch 5 at a time to avoid timeouts
            
            for i in range(0, len(seq_num_list), batch_size):
                batch = seq_num_list[i:i + batch_size]
                
                for seq_num in batch:
                    # Skip if already processed in this session
                    seq_num_str = seq_num.decode() if isinstance(seq_num, bytes) else str(seq_num)
                    if seq_num_str in self.processed_seq_nums:
                        logger.debug(f"Skipping already processed sequence number: {seq_num_str}")
                        continue
                        
                    try:
                        email_dict = self._fetch_single_email(seq_num)
                        if email_dict:
                            emails.append(email_dict)
                            
                    except Exception as e:
                        logger.error(f"Error fetching email sequence {seq_num}: {str(e)}")
                        continue
                        
                # Small delay between batches to avoid rate limiting
                if i + batch_size < len(seq_num_list):
                    time.sleep(0.5)
                    
        except imaplib.IMAP4.abort:
            logger.error("IMAP connection aborted during fetch")
            # Try to reconnect for next operation
            self.connect()
            
        except Exception as e:
            logger.error(f"Error fetching emails: {str(e)}")
            
        return emails
        
    def _fetch_single_email(self, seq_num: bytes) -> Optional[Dict]:
        """
        Fetch and parse a single email by sequence number.
        
        Args:
            seq_num: Email sequence number (as bytes)
            
        Returns:
            Parsed email dictionary or None if failed
        """
        try:
            # Convert sequence number to string for consistent handling
            if isinstance(seq_num, bytes):
                seq_num_str = seq_num.decode()
            else:
                seq_num_str = str(seq_num)
            
            # Fetch email by sequence number (not UID)
            status, msg_data = self.imap.fetch(seq_num_str, '(RFC822 FLAGS INTERNALDATE)')
            
            if status != 'OK' or not msg_data or not msg_data[0]:
                logger.error(f"Failed to fetch email sequence {seq_num_str}")
                return None
                
            # Parse response
            email_body = msg_data[0][1]
            message = email.message_from_bytes(email_body)
            
            # Parse email with enhanced encoding detection
            email_dict = self._parse_email_enhanced(message)
            email_dict['seq_num'] = seq_num_str  # Store sequence number, not UID
            
            # Add IMAP metadata if available
            if len(msg_data[0]) > 2:
                try:
                    metadata = msg_data[0][0].decode() if msg_data[0][0] else ""
                    if 'INTERNALDATE' in metadata:
                        # Extract internal date if available
                        import re
                        date_match = re.search(r'INTERNALDATE "([^"]+)"', metadata)
                        if date_match:
                            email_dict['internal_date'] = date_match.group(1)
                except:
                    pass
                    
            return email_dict
            
        except Exception as e:
            logger.error(f"Error parsing email sequence {seq_num}: {str(e)}")
            return None
            
    def _parse_email_enhanced(self, message) -> Dict:
        """
        Parse email with enhanced encoding detection and content extraction.
        
        Args:
            message: Email message object
            
        Returns:
            Dictionary with parsed email data
        """
        email_dict = {}
        
        # Extract headers with proper decoding
        email_dict['subject'] = self._decode_header_enhanced(message.get('Subject', ''))
        email_dict['from'] = self._decode_header_enhanced(message.get('From', ''))
        email_dict['to'] = self._decode_header_enhanced(message.get('To', ''))
        email_dict['date'] = message.get('Date', '')
        email_dict['message_id'] = message.get('Message-ID', '')
        
        # Extract body with enhanced parsing
        body_plain = ""
        body_html = ""
        attachments = []
        
        if message.is_multipart():
            for part in message.walk():
                content_type = part.get_content_type()
                content_disposition = str(part.get('Content-Disposition', ''))
                
                # Skip attachments for now, but track them
                if 'attachment' in content_disposition:
                    filename = part.get_filename()
                    if filename:
                        attachments.append({
                            'filename': self._decode_header_enhanced(filename),
                            'content_type': content_type,
                            'size': len(part.get_payload())
                        })
                    continue
                    
                # Extract text content
                if content_type == 'text/plain':
                    body_plain += self._decode_part_content(part)
                elif content_type == 'text/html':
                    body_html += self._decode_part_content(part)
                    
        else:
            # Single part message
            content_type = message.get_content_type()
            if content_type == 'text/plain':
                body_plain = self._decode_part_content(message)
            elif content_type == 'text/html':
                body_html = self._decode_part_content(message)
                
        # Prefer plain text, but use HTML if plain is empty
        if body_plain.strip():
            email_dict['body'] = body_plain
            email_dict['body_type'] = 'plain'
        elif body_html.strip():
            # Convert HTML to plain text
            try:
                email_dict['body'] = self.html_converter.handle(body_html)
                email_dict['body_type'] = 'html_converted'
            except:
                # Fallback to basic HTML stripping
                email_dict['body'] = self._strip_html_basic(body_html)
                email_dict['body_type'] = 'html_stripped'
        else:
            email_dict['body'] = ""
            email_dict['body_type'] = 'empty'
            
        # Store original HTML if available (for reference)
        if body_html:
            email_dict['body_html'] = body_html
            
        # Add attachment info
        if attachments:
            email_dict['attachments'] = attachments
            
        return email_dict
        
    def _decode_part_content(self, part) -> str:
        """
        Decode email part content with automatic encoding detection.
        
        Args:
            part: Email message part
            
        Returns:
            Decoded string content
        """
        payload = part.get_payload(decode=True)
        
        if not payload:
            return ""
            
        # Try charset from content type
        charset = part.get_content_charset()
        
        if charset:
            try:
                return payload.decode(charset, errors='ignore')
            except (UnicodeDecodeError, LookupError):
                pass
                
        # Use chardet to detect encoding
        try:
            detected = chardet.detect(payload)
            if detected['encoding'] and detected['confidence'] > 0.7:
                return payload.decode(detected['encoding'], errors='ignore')
        except:
            pass
            
        # Try common encodings
        for encoding in ['utf-8', 'latin-1', 'windows-1252', 'ascii']:
            try:
                return payload.decode(encoding, errors='ignore')
            except (UnicodeDecodeError, LookupError):
                continue
                
        # Last resort: decode as latin-1 (accepts all byte values)
        return payload.decode('latin-1', errors='ignore')
        
    def _decode_header_enhanced(self, header_value) -> str:
        """
        Decode email header with enhanced error handling.
        
        Args:
            header_value: Raw header value
            
        Returns:
            Decoded string
        """
        if not header_value:
            return ""
            
        try:
            decoded_parts = decode_header(header_value)
            decoded_string = ""
            
            for part, encoding in decoded_parts:
                if isinstance(part, bytes):
                    if encoding:
                        try:
                            decoded_string += part.decode(encoding, errors='ignore')
                        except (UnicodeDecodeError, LookupError):
                            # Try with chardet
                            detected = chardet.detect(part)
                            if detected['encoding']:
                                decoded_string += part.decode(detected['encoding'], errors='ignore')
                            else:
                                decoded_string += part.decode('utf-8', errors='ignore')
                    else:
                        decoded_string += part.decode('utf-8', errors='ignore')
                else:
                    decoded_string += str(part)
                    
            return decoded_string.strip()
            
        except Exception as e:
            logger.debug(f"Header decode error: {e}")
            return str(header_value)
            
    def _strip_html_basic(self, html_content: str) -> str:
        """
        Basic HTML stripping as fallback.
        
        Args:
            html_content: HTML content
            
        Returns:
            Plain text
        """
        import re
        # Remove script and style elements
        html_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL)
        html_content = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL)
        # Remove HTML tags
        html_content = re.sub(r'<[^>]+>', ' ', html_content)
        # Clean up whitespace
        html_content = re.sub(r'\s+', ' ', html_content)
        return html_content.strip()
        
    def mark_as_processed(self, seq_num: str, use_flag: bool = True) -> bool:
        """
        Mark email as processed using sequence number.
        
        Args:
            seq_num: Email sequence number as string
            use_flag: Whether to use FLAG in addition to label
            
        Returns:
            Success status
        """
        if not self.ensure_connection():
            return False
            
        try:
            success = True
            
            # Mark as read using sequence number
            status, data = self.imap.store(seq_num, '+FLAGS', '\\Seen')
            if status != 'OK':
                logger.warning(f"Failed to mark email {seq_num} as read: {data}")
                success = False
                
            # Try to add Gmail label if available
            if self._check_capability('X-GM-EXT-1'):
                try:
                    status, data = self.imap.store(seq_num, '+X-GM-LABELS', f'({self.processed_label_name})')
                    if status != 'OK':
                        logger.warning(f"Failed to add label to email {seq_num}: {data}")
                except:
                    # Label might not be supported, fall back to flag
                    use_flag = True
                    
            # Use FLAG as fallback or additional marker
            if use_flag:
                status, data = self.imap.store(seq_num, '+FLAGS', '\\Flagged')
                if status != 'OK':
                    logger.warning(f"Failed to flag email {seq_num}: {data}")
                    success = False
                    
            # Track in session
            self.processed_seq_nums.add(seq_num)
            
            if success:
                logger.debug(f"Marked email sequence {seq_num} as processed")
            else:
                logger.warning(f"Partial success marking email sequence {seq_num} as processed")
                
            return success
            
        except Exception as e:
            logger.error(f"Error marking email as processed: {str(e)}")
            return False
            
    def search_by_criteria(self, criteria: str, max_results: int = 10) -> List[str]:
        """
        Search emails by custom criteria using sequence numbers.
        
        Args:
            criteria: IMAP search criteria
            max_results: Maximum results to return
            
        Returns:
            List of sequence numbers as strings
        """
        if not self.ensure_connection():
            return []
            
        try:
            # Use standard search (returns sequence numbers)
            status, data = self.imap.search(None, criteria)
            if status == 'OK':
                seq_nums = data[0].split()[:max_results]
                return [seq.decode() if isinstance(seq, bytes) else str(seq) for seq in seq_nums]
        except Exception as e:
            logger.error(f"Search failed: {e}")
            
        return []
        
    def get_folder_list(self) -> List[str]:
        """Get list of available folders/labels."""
        if not self.ensure_connection():
            return []
            
        try:
            status, folders = self.imap.list()
            if status == 'OK':
                folder_list = []
                for folder in folders:
                    if folder:
                        # Parse folder name from response
                        parts = folder.decode().split(' "/" ')
                        if len(parts) > 1:
                            folder_name = parts[1].strip('"')
                            folder_list.append(folder_name)
                return folder_list
        except Exception as e:
            logger.error(f"Failed to get folder list: {e}")
            
        return []
        
    def close(self):
        """Close IMAP connection and cleanup resources."""
        try:
            with self.connection_lock:
                if self.imap:
                    try:
                        self.imap.close()
                    except:
                        pass
                    try:
                        self.imap.logout()
                    except:
                        pass
                    self.imap = None
                    logger.info("Gmail connection closed")
        except Exception as e:
            logger.error(f"Error closing connection: {e}")
            
    def __del__(self):
        """Destructor to ensure cleanup."""
        self.close()