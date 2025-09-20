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
        self.processed_seq_nums: Set[str] = set()
        self.connection_lock = Lock()
        self.last_reconnect = None
        self.reconnect_delay = 5
        self.max_fetch_batch = config.get_int('EMAIL_BATCH_SIZE', 10)
        self.html_converter = html2text.HTML2Text()
        self.html_converter.ignore_links = False
        self.html_converter.ignore_images = True
        self.processed_label_name = config.get('GMAIL_PROCESSED_LABEL', 'PROCESSED')

        self.connect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self, retry: bool = True) -> bool:
        max_retries = self.config.get_int('MAX_RETRIES', 3) if retry else 1
        retry_delay = self.config.get_int('RETRY_DELAY', 5)

        for attempt in range(max_retries):
            try:
                with self.connection_lock:
                    if self.imap:
                        try:
                            self.imap.close()
                            self.imap.logout()
                        except:
                            pass

                    server = self.config.get('GMAIL_IMAP_SERVER', 'imap.gmail.com')
                    port = self.config.get_int('GMAIL_IMAP_PORT', 993)
                    logger.info(f"Connecting to Gmail IMAP server {server}:{port} (attempt {attempt + 1}/{max_retries})")
                    self.imap = imaplib.IMAP4_SSL(server, port)
                    self.imap.login(
                        self.config.get('GMAIL_USER'),
                        self.config.get('GMAIL_APP_PASSWORD')
                    )
                    status, data = self.imap.select('INBOX')
                    if status != 'OK':
                        raise Exception(f"Failed to select INBOX: {data}")
                    self.processed_seq_nums.clear()
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

    def ensure_connection(self) -> bool:
        try:
            if self.imap:
                self.imap.noop()
                return True
        except:
            logger.info("Connection lost, reconnecting...")

        if self.last_reconnect:
            elapsed = (datetime.now() - self.last_reconnect).total_seconds()
            if elapsed < self.reconnect_delay:
                time.sleep(self.reconnect_delay - elapsed)

        return self.connect()

    def fetch_unread_emails(self, max_emails: Optional[int] = None,
                            since_date: Optional[datetime] = None,
                            from_sender: Optional[str] = None) -> List[Dict]:
        emails = []
        if not self.ensure_connection():
            logger.error("Could not establish connection to Gmail")
            return emails

        max_emails = max_emails or self.max_fetch_batch

        try:
            search_criteria = self._build_search_criteria(
                unread=True,
                since_date=since_date,
                from_sender=from_sender
            )

            logger.info(f"IMAP search query: {search_criteria}")
            status, data = self.imap.search(None, search_criteria)

            if status != 'OK':
                logger.error(f"Search failed: {data}")
                search_criteria = 'UNSEEN'
                status, data = self.imap.search(None, search_criteria)
                if status != 'OK':
                    return emails

            seq_num_list = data[0].split()
            if not seq_num_list:
                return emails

            seq_num_list = seq_num_list[:max_emails]

            batch_size = 5
            for i in range(0, len(seq_num_list), batch_size):
                batch = seq_num_list[i:i + batch_size]

                for seq_num in batch:
                    seq_num_str = seq_num.decode() if isinstance(seq_num, bytes) else str(seq_num)
                    if seq_num_str in self.processed_seq_nums:
                        continue
                    try:
                        email_dict = self._fetch_single_email(seq_num)
                        if email_dict:
                            emails.append(email_dict)
                    except Exception as e:
                        logger.error(f"Error fetching email sequence {seq_num}: {str(e)}")
                        continue
                if i + batch_size < len(seq_num_list):
                    time.sleep(0.5)

        except Exception as e:
            logger.error(f"Error fetching emails: {str(e)}")

        return emails

    def _build_search_criteria(self, unread=True, since_date=None, from_sender=None, subject_contains=None) -> str:
        criteria_parts = []
        if unread:
            criteria_parts.append('UNSEEN')

        if since_date:
            date_str = since_date.strftime('%d-%b-%Y')
            criteria_parts.append(f'SINCE {date_str}')

        if from_sender:
            sender_escaped = from_sender.replace('"', '\\"')
            criteria_parts.append(f'FROM "{sender_escaped}"')

        if subject_contains:
            subject_escaped = subject_contains.replace('"', '\\"')
            criteria_parts.append(f'SUBJECT "{subject_escaped}"')

        if not criteria_parts:
            return 'ALL'
        elif len(criteria_parts) == 1:
            return criteria_parts[0]
        else:
            return f'({" ".join(criteria_parts)})'

    def _fetch_single_email(self, seq_num) -> Optional[Dict]:
        try:
            seq_num_str = seq_num.decode() if isinstance(seq_num, bytes) else str(seq_num)
            status, msg_data = self.imap.fetch(seq_num_str, '(RFC822 FLAGS INTERNALDATE)')
            if status != 'OK' or not msg_data or not msg_data[0]:
                return None
            email_body = msg_data[0][1]
            message = email.message_from_bytes(email_body)
            email_dict = self._parse_email_enhanced(message)
            email_dict['seq_num'] = seq_num_str
            self.processed_seq_nums.add(seq_num_str)
            return email_dict
        except Exception as e:
            logger.error(f"Error fetching single email {seq_num}: {str(e)}")
            return None

    def _parse_email_enhanced(self, message) -> Dict:
        email_dict = {}
        email_dict['subject'] = self._decode_header_enhanced(message.get('Subject', ''))
        email_dict['from'] = self._decode_header_enhanced(message.get('From', ''))
        email_dict['to'] = self._decode_header_enhanced(message.get('To', ''))
        email_dict['date'] = message.get('Date', '')
        email_dict['message_id'] = message.get('Message-ID', '')

        body_plain = ""
        body_html = ""
        attachments = []

        if message.is_multipart():
            for part in message.walk():
                content_type = part.get_content_type()
                content_disposition = str(part.get('Content-Disposition', ''))
                if 'attachment' in content_disposition:
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
            email_dict['body'] = body_plain
            email_dict['body_type'] = 'plain'
        elif body_html.strip():
            try:
                email_dict['body'] = self.html_converter.handle(body_html)
                email_dict['body_type'] = 'html_converted'
            except:
                email_dict['body'] = self._strip_html_basic(body_html)
                email_dict['body_type'] = 'html_stripped'
        else:
            email_dict['body'] = ""
            email_dict['body_type'] = 'empty'

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
            except:
                pass
        try:
            detected = chardet.detect(payload)
            if detected['encoding'] and detected['confidence'] > 0.7:
                return payload.decode(detected['encoding'], errors='ignore')
        except:
            pass
        for encoding in ['utf-8', 'latin-1', 'windows-1252', 'ascii']:
            try:
                return payload.decode(encoding, errors='ignore')
            except:
                continue
        return payload.decode('latin-1', errors='ignore')

    def _decode_header_enhanced(self, header_value) -> str:
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
                        except:
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
        except:
            return str(header_value)

    def _strip_html_basic(self, html_content: str) -> str:
        import re
        html_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL)
        html_content = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL)
        html_content = re.sub(r'<[^>]+>', ' ', html_content)
        html_content = re.sub(r'\s+', ' ', html_content)
        return html_content.strip()

    def mark_as_processed(self, seq_num: str, use_flag: bool = True) -> bool:
        if not self.ensure_connection():
            return False
        try:
            success = True
            status, data = self.imap.store(seq_num, '+FLAGS', '\\Seen')
            if status != 'OK':
                success = False
            if self._check_capability('X-GM-EXT-1'):
                try:
                    status, data = self.imap.store(seq_num, '+X-GM-LABELS', f'({self.processed_label_name})')
                    if status != 'OK':
                        success = False
                except:
                    use_flag = True
            if use_flag:
                status, data = self.imap.store(seq_num, '+FLAGS', '\\Flagged')
                if status != 'OK':
                    success = False
            self.processed_seq_nums.add(seq_num)
            return success
        except Exception as e:
            logger.error(f"Error marking email as processed: {str(e)}")
            return False

    def get_folder_list(self) -> List[str]:
        if not self.ensure_connection():
            return []
        try:
            status, folders = self.imap.list()
            if status != 'OK':
                return []
            folder_list = []
            for folder in folders:
                if folder:
                    parts = folder.decode().split(' "/" ')
                    if len(parts) > 1:
                        folder_name = parts[1].strip('"')
                        folder_list.append(folder_name)
            return folder_list
        except:
            return []

    def close(self):
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
        except:
            pass

    def __del__(self):
        self.close()
