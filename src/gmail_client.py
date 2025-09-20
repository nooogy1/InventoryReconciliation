"""Gmail client using consistent IMAP sequence numbers for stability."""

import imaplib
import email
import logging
import time
import chardet
import html2text
from typing import List, Dict, Optional, Set
from datetime import datetime
from email.header import decode_header
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
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """Close IMAP connection gracefully."""
        with self.connection_lock:
            if self.imap:
                try:
                    self.imap.close()
                    self.imap.logout()
                except Exception:
                    pass
                self.imap = None

    def connect(self, retry: bool = True) -> bool:
        """Connect to Gmail via IMAP with retry logic."""
        max_retries = self.config.get_int('MAX_RETRIES', 3) if retry else 1
        retry_delay = self.config.get_int('RETRY_DELAY', 5)

        for attempt in range(max_retries):
            try:
                with self.connection_lock:
                    if self.imap:
                        try:
                            self.imap.close()
                            self.imap.logout()
                        except Exception:
                            pass

                    server = self.config.get('GMAIL_IMAP_SERVER', 'imap.gmail.com')
                    port = self.config.get_int('GMAIL_IMAP_PORT', 993)

                    logger.info(
                        f"Connecting to Gmail IMAP server {server}:{port} "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )

                    self.imap = imaplib.IMAP4_SSL(server, port)
                    self.imap.login(
                        self.config.get('GMAIL_USER'),
                        self.config.get('GMAIL_APP_PASSWORD')
                    )

                    status, data = self.imap.select('INBOX')
                    if status != 'OK':
                        raise Exception(f"Failed to select INBOX: {data}")

                    self.processed_seq_nums.clear()

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
        try:
            capabilities = self.imap.capability()[1][0].decode()
            return capability in capabilities
        except Exception:
            return False

    def ensure_connection(self) -> bool:
        """Ensure IMAP connection is alive, reconnect if needed."""
        try:
            if self.imap:
                self.imap.noop()
                return True
        except Exception:
            logger.info("Connection lost, reconnecting...")

        if self.last_reconnect:
            elapsed = (datetime.now() - self.last_reconnect).total_seconds()
            if elapsed < self.reconnect_delay:
                time.sleep(self.reconnect_delay - elapsed)

        return self.connect()

    def _fetch_single_email(self, seq_num) -> Optional[Dict]:
        """Fetch and parse a single email by sequence number."""
        try:
            seq_num_str = seq_num.decode() if isinstance(seq_num, bytes) else str(seq_num)
            status, msg_data = self.imap.fetch(seq_num_str, '(RFC822 FLAGS INTERNALDATE)')

            if status != 'OK' or not msg_data or not msg_data[0]:
                logger.error(f"Failed to fetch email sequence {seq_num_str}")
                return None

            email_body = msg_data[0][1]
            message = email.message_from_bytes(email_body)

            email_dict = self._parse_email_enhanced(message)
            email_dict['seq_num'] = seq_num_str

            # Extract INTERNALDATE if available
            if msg_data[0][0]:
                import re
                metadata = msg_data[0][0].decode()
                match = re.search(r'INTERNALDATE "([^"]+)"', metadata)
                if match:
                    email_dict['internal_date'] = match.group(1)

            return email_dict

        except Exception as e:
            logger.error(f"Error parsing email sequence {seq_num}: {str(e)}")
            return None

    # -------------------------------------------------------------
    # Parsing helpers (_parse_email_enhanced, _decode_part_content, etc.)
    # -------------------------------------------------------------

    def _parse_email_enhanced(self, message) -> Dict:
        """Parse email with encoding detection and content extraction."""
        email_dict = {
            'subject': self._decode_header_enhanced(message.get('Subject', '')),
            'from': self._decode_header_enhanced(message.get('From', '')),
            'to': self._decode_header_enhanced(message.get('To', '')),
            'date': message.get('Date', ''),
            'message_id': message.get('Message-ID', ''),
        }

        body_plain, body_html, attachments = "", "", []

        if message.is_multipart():
            for part in message.walk():
                content_type = part.get_content_type()
                disposition = str(part.get('Content-Disposition', ''))
                if 'attachment' in disposition:
                    filename = part.get_filename()
                    if filename:
                        attachments.append({
                            'filename': self._decode_header_enhanced(filename),
                            'content_type': content_type,
                            'size': len(part.get_payload())
                        })
                    continue
                if content_type == 'text/plain':
                    body_plain += self._decode_part_content(part)
                elif content_type == 'text/html':
                    body_html += self._decode_part_content(part)
        else:
            content_type = message.get_content_type()
            if content_type == 'text/plain':
                body_plain = self._decode_part_content(message)
            elif content_type == 'text/html':
                body_html = self._decode_part_content(message)

        if body_plain.strip():
            email_dict['body'], email_dict['body_type'] = body_plain, 'plain'
        elif body_html.strip():
            try:
                email_dict['body'] = self.html_converter.handle(body_html)
                email_dict['body_type'] = 'html_converted'
            except Exception:
                email_dict['body'] = self._strip_html_basic(body_html)
                email_dict['body_type'] = 'html_stripped'
        else:
            email_dict['body'], email_dict['body_type'] = "", "empty"

        if body_html:
            email_dict['body_html'] = body_html
        if attachments:
            email_dict['attachments'] = attachments

        return email_dict

    def _decode_part_content(self, part) -> str:
        payload = part.get_payload(decode=True)
        if not payload:
            return ""
        charset = part.get_content_charset()
        if charset:
            try:
                return payload.decode(charset, errors='ignore')
            except Exception:
                pass
        try:
            detected = chardet.detect(payload)
            if detected['encoding']:
                return payload.decode(detected['encoding'], errors='ignore')
        except Exception:
            pass
        for enc in ['utf-8', 'latin-1', 'windows-1252', 'ascii']:
            try:
                return payload.decode(enc, errors='ignore')
            except Exception:
                continue
        return payload.decode('latin-1', errors='ignore')

    def _decode_header_enhanced(self, header_value) -> str:
        if not header_value:
            return ""
        try:
            parts = decode_header(header_value)
            decoded = ""
            for part, enc in parts:
                if isinstance(part, bytes):
                    try:
                        decoded += part.decode(enc or 'utf-8', errors='ignore')
                    except Exception:
                        detected = chardet.detect(part)
                        decoded += part.decode(detected['encoding'] or 'utf-8', errors='ignore')
                else:
                    decoded += str(part)
            return decoded.strip()
        except Exception:
            return str(header_value)

    def _strip_html_basic(self, html_content: str) -> str:
        import re
        html_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL)
        html_content = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL)
        html_content = re.sub(r'<[^>]+>', ' ', html_content)
        return re.sub(r'\s+', ' ', html_content).strip()

    # -------------------------------------------------------------
    # Other utilities: mark_as_processed, search_by_criteria, etc.
    # -------------------------------------------------------------

    def mark_as_processed(self, seq_num: str, use_flag: bool = True) -> bool:
        if not self.ensure_connection():
            return False
        try:
            success = True
            status, data = self.imap.store(seq_num, '+FLAGS', '\\Seen')
            if status != 'OK':
                logger.warning(f"Failed to mark {seq_num} as read: {data}")
                success = False
            if self._check_capability('X-GM-EXT-1'):
                status, data = self.imap.store(seq_num, '+X-GM-LABELS', f'({self.processed_label_name})')
                if status != 'OK':
                    logger.warning(f"Failed to add label to {seq_num}: {data}")
            if use_flag:
                self.imap.store(seq_num, '+FLAGS', '\\Flagged')
            self.processed_seq_nums.add(seq_num)
            return success
        except Exception as e:
            logger.error(f"Error marking email as processed: {e}")
            return False

    def search_by_criteria(self, criteria: str, max_results: int = 10) -> List[str]:
        if not self.ensure_connection():
            return []
        try:
            logger.info(f"Custom search query: {criteria}")
            status, data = self.imap.search(None, criteria)
            if status != 'OK':
                logger.error(f"Search failed for {criteria}: {data}")
                return []
            seqs = data[0].split()[:max_results]
            return [s.decode() if isinstance(s, bytes) else str(s) for s in seqs]
        except Exception as e:
            logger.error(f"Custom search error: {e}")
            return []
