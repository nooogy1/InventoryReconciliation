"""OpenAI-based email parser with robust validation and error handling."""

import json
import logging
import time
import re
from typing import Dict, Optional, List, Any, Union
from datetime import datetime
from dataclasses import dataclass, asdict
from enum import Enum
from openai import OpenAI, RateLimitError, APIError, APIConnectionError

logger = logging.getLogger(__name__)


class ParseStatus(Enum):
    """Email parsing status codes."""
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"
    UNKNOWN_TYPE = "unknown_type"
    VALIDATION_ERROR = "validation_error"
    API_ERROR = "api_error"


@dataclass
class ParseResult:
    """Container for parse results with metadata."""
    status: ParseStatus
    data: Optional[Dict] = None
    errors: List[str] = None
    warnings: List[str] = None
    missing_fields: List[str] = None
    confidence_score: float = 0.0
    parse_time: float = 0.0
    requires_review: bool = False
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.warnings is None:
            self.warnings = []
        if self.missing_fields is None:
            self.missing_fields = []
            
    def to_dict(self) -> Dict:
        """Convert to dictionary for storage."""
        result = asdict(self)
        result['status'] = self.status.value
        return result


class EmailParser:
    """Parse emails using OpenAI API with robust validation."""
    
    # Required fields for each transaction type
    REQUIRED_FIELDS = {
        'purchase': {
            'critical': ['date', 'vendor_name', 'items'],
            'important': ['order_number', 'total'],
            'optional': ['taxes', 'shipping', 'subtotal']
        },
        'sale': {
            'critical': ['date', 'channel', 'items'],
            'important': ['order_number', 'total'],
            'optional': ['customer_email', 'taxes', 'fees', 'subtotal']
        }
    }
    
    # Field validation patterns
    VALIDATION_PATTERNS = {
        'date': r'^\d{4}-\d{2}-\d{2}$',
        'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
        'order_number': r'^[A-Za-z0-9\-_#]+$'
    }
    
    def __init__(self, config):
        self.config = config
        self.client = OpenAI(api_key=config.get('OPENAI_API_KEY'))
        self.model = config.get('OPENAI_MODEL', 'gpt-4')
        self.temperature = config.get_float('OPENAI_TEMPERATURE', 0.1)
        self.max_retries = config.get_int('OPENAI_MAX_RETRIES', 3)
        self.retry_delay = config.get_int('OPENAI_RETRY_DELAY', 2)
        self.enable_sanitization = config.get_bool('ENABLE_DATA_SANITIZATION', True)
        self.confidence_threshold = config.get_float('CONFIDENCE_THRESHOLD', 0.7)
        
    def parse_email(self, body: str, subject: str) -> ParseResult:
        """
        Parse email content with comprehensive validation.
        
        Args:
            body: Email body content
            subject: Email subject
            
        Returns:
            ParseResult with status, data, and metadata
        """
        start_time = time.time()
        
        # Sanitize input if needed
        if self.enable_sanitization:
            body = self._sanitize_input(body)
            subject = self._sanitize_input(subject)
        
        # Try parsing with retries
        for attempt in range(self.max_retries):
            try:
                # Call OpenAI API
                raw_result = self._call_openai(body, subject)
                
                if not raw_result:
                    continue
                    
                # Parse and validate the response
                parsed_data = self._parse_response(raw_result)
                
                if not parsed_data:
                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay * (2 ** attempt))
                        continue
                    return ParseResult(
                        status=ParseStatus.API_ERROR,
                        errors=["Failed to parse OpenAI response"],
                        parse_time=time.time() - start_time
                    )
                
                # Validate and enrich the data
                validation_result = self._validate_and_enrich(parsed_data)
                validation_result.parse_time = time.time() - start_time
                
                # Log sanitized version
                self._log_result(validation_result)
                
                return validation_result
                
            except RateLimitError as e:
                logger.warning(f"Rate limit hit (attempt {attempt + 1}): {e}")
                if attempt < self.max_retries - 1:
                    wait_time = self._get_retry_after(e) or (self.retry_delay * (2 ** attempt))
                    logger.info(f"Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                    continue
                return ParseResult(
                    status=ParseStatus.API_ERROR,
                    errors=[f"Rate limit exceeded: {str(e)}"],
                    parse_time=time.time() - start_time
                )
                
            except (APIError, APIConnectionError) as e:
                logger.error(f"OpenAI API error (attempt {attempt + 1}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (2 ** attempt))
                    continue
                return ParseResult(
                    status=ParseStatus.API_ERROR,
                    errors=[f"API error: {str(e)}"],
                    parse_time=time.time() - start_time
                )
                
            except Exception as e:
                logger.error(f"Unexpected error parsing email: {e}", exc_info=True)
                return ParseResult(
                    status=ParseStatus.FAILED,
                    errors=[f"Unexpected error: {str(e)}"],
                    parse_time=time.time() - start_time
                )
        
        return ParseResult(
            status=ParseStatus.FAILED,
            errors=["Max retries exceeded"],
            parse_time=time.time() - start_time
        )
        
    def _call_openai(self, body: str, subject: str) -> Optional[str]:
        """Call OpenAI API with the email content."""
        try:
            prompt = self._create_enhanced_prompt(body, subject)
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": self._get_enhanced_system_prompt()},
                    {"role": "user", "content": prompt}
                ],
                temperature=self.temperature,
                response_format={"type": "json_object"},
                max_tokens=2000  # Ensure we don't truncate
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"Error calling OpenAI: {e}")
            raise
            
    def _parse_response(self, response: str) -> Optional[Dict]:
        """Parse and clean the OpenAI response."""
        try:
            # Try to parse as JSON
            data = json.loads(response)
            
            # Clean up the data
            return self._clean_parsed_data(data)
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON from OpenAI: {e}")
            
            # Try to extract JSON from the response
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                try:
                    data = json.loads(json_match.group())
                    return self._clean_parsed_data(data)
                except:
                    pass
                    
            return None
            
    def _clean_parsed_data(self, data: Dict) -> Dict:
        """Clean and normalize parsed data."""
        # Remove None values and empty strings
        cleaned = {}
        
        for key, value in data.items():
            if value is not None and value != "":
                if isinstance(value, str):
                    # Clean whitespace
                    value = value.strip()
                    # Convert "null" string to None
                    if value.lower() in ['null', 'none', 'n/a', 'unknown']:
                        value = None
                    else:
                        cleaned[key] = value
                elif isinstance(value, list):
                    # Clean list items
                    cleaned_list = []
                    for item in value:
                        if isinstance(item, dict):
                            cleaned_item = self._clean_parsed_data(item)
                            if cleaned_item:
                                cleaned_list.append(cleaned_item)
                        elif item is not None and item != "":
                            cleaned_list.append(item)
                    if cleaned_list:
                        cleaned[key] = cleaned_list
                elif isinstance(value, (int, float)):
                    # Validate numeric values
                    if value >= 0:  # Assuming no negative values
                        cleaned[key] = value
                else:
                    cleaned[key] = value
                    
        return cleaned
        
    def _validate_and_enrich(self, data: Dict) -> ParseResult:
        """Validate parsed data and identify missing fields."""
        result = ParseResult(status=ParseStatus.SUCCESS, data=data)
        
        # Check transaction type
        transaction_type = data.get('type')
        
        if not transaction_type:
            result.status = ParseStatus.UNKNOWN_TYPE
            result.errors.append("Transaction type not identified")
            result.requires_review = True
            return result
            
        if transaction_type not in ['purchase', 'sale']:
            if transaction_type == 'unknown':
                result.status = ParseStatus.UNKNOWN_TYPE
                return result
            else:
                result.warnings.append(f"Unexpected transaction type: {transaction_type}")
                
        # Validate required fields
        if transaction_type in self.REQUIRED_FIELDS:
            requirements = self.REQUIRED_FIELDS[transaction_type]
            
            # Check critical fields
            for field in requirements['critical']:
                if field not in data or data[field] is None:
                    result.missing_fields.append(field)
                    result.errors.append(f"Missing critical field: {field}")
                    result.status = ParseStatus.PARTIAL
                    result.requires_review = True
                    
            # Check important fields
            for field in requirements['important']:
                if field not in data or data[field] is None:
                    result.missing_fields.append(field)
                    result.warnings.append(f"Missing important field: {field}")
                    if result.status == ParseStatus.SUCCESS:
                        result.status = ParseStatus.PARTIAL
                        
        # Validate specific fields
        self._validate_date(data, result)
        self._validate_email_field(data, result)
        self._validate_items(data, result)
        self._validate_amounts(data, result)
        
        # Calculate confidence score
        result.confidence_score = self._calculate_confidence(data, result)
        
        # Determine if review is needed
        if result.confidence_score < self.confidence_threshold:
            result.requires_review = True
            
        # Add metadata
        data['parse_metadata'] = {
            'status': result.status.value,
            'confidence': result.confidence_score,
            'missing_fields': result.missing_fields,
            'requires_review': result.requires_review,
            'parsed_at': datetime.utcnow().isoformat()
        }
        
        return result
        
    def _validate_date(self, data: Dict, result: ParseResult):
        """Validate and normalize date field."""
        date_field = data.get('date')
        
        if date_field:
            # Check if it matches expected format
            if not re.match(self.VALIDATION_PATTERNS['date'], date_field):
                # Try to parse and reformat
                try:
                    from dateutil import parser
                    parsed_date = parser.parse(date_field)
                    data['date'] = parsed_date.strftime('%Y-%m-%d')
                    result.warnings.append(f"Date reformatted from '{date_field}'")
                except:
                    result.warnings.append(f"Invalid date format: {date_field}")
                    data['date_original'] = date_field
                    data['date'] = None
                    result.missing_fields.append('date')
                    
    def _validate_email_field(self, data: Dict, result: ParseResult):
        """Validate email fields."""
        email_field = data.get('customer_email')
        
        if email_field and not re.match(self.VALIDATION_PATTERNS['email'], email_field):
            result.warnings.append(f"Invalid email format: {email_field}")
            data['customer_email_original'] = email_field
            data['customer_email'] = None
            
    def _validate_items(self, data: Dict, result: ParseResult):
        """Validate items array."""
        items = data.get('items', [])
        
        if not items:
            result.errors.append("No items found")
            result.status = ParseStatus.PARTIAL
            result.requires_review = True
            return
            
        validated_items = []
        for i, item in enumerate(items):
            validated_item = {}
            
            # Check for required item fields
            if not item.get('name'):
                result.warnings.append(f"Item {i+1}: Missing name")
                result.requires_review = True
                
            # SKU is critical for inventory tracking
            if not item.get('sku'):
                result.warnings.append(f"Item {i+1}: Missing SKU")
                result.missing_fields.append(f'item_{i+1}_sku')
                result.requires_review = True
                validated_item['needs_sku'] = True
                
            # Validate quantity
            quantity = item.get('quantity')
            if quantity is None or quantity <= 0:
                result.warnings.append(f"Item {i+1}: Invalid quantity")
                result.missing_fields.append(f'item_{i+1}_quantity')
                result.requires_review = True
                
            # Validate price
            price_field = 'unit_price' if data.get('type') == 'purchase' else 'sale_price'
            price = item.get(price_field)
            if price is None or price < 0:
                result.warnings.append(f"Item {i+1}: Invalid {price_field}")
                result.missing_fields.append(f'item_{i+1}_{price_field}')
                result.requires_review = True
                
            # Copy validated fields
            for key, value in item.items():
                if value is not None:
                    validated_item[key] = value
                    
            validated_items.append(validated_item)
            
        data['items'] = validated_items
        
    def _validate_amounts(self, data: Dict, result: ParseResult):
        """Validate and reconcile monetary amounts."""
        # Get amounts
        subtotal = data.get('subtotal', 0) or 0
        taxes = data.get('taxes', 0) or 0
        shipping = data.get('shipping', 0) or 0
        fees = data.get('fees', 0) or 0
        total = data.get('total', 0) or 0
        
        # Calculate expected total
        if data.get('type') == 'purchase':
            calculated_total = subtotal + taxes + shipping
        else:  # sale
            calculated_total = subtotal + taxes - fees
            
        # Check if totals match (with small tolerance for rounding)
        if total > 0 and abs(calculated_total - total) > 0.10:
            result.warnings.append(
                f"Total mismatch: calculated ${calculated_total:.2f} vs stated ${total:.2f}"
            )
            data['total_calculated'] = calculated_total
            data['total_mismatch'] = True
            result.requires_review = True
            
        # Ensure all amounts are floats
        for field in ['subtotal', 'taxes', 'shipping', 'fees', 'total']:
            if field in data:
                try:
                    data[field] = float(data[field])
                except (TypeError, ValueError):
                    result.warnings.append(f"Invalid amount for {field}")
                    data[field] = 0.0
                    
    def _calculate_confidence(self, data: Dict, result: ParseResult) -> float:
        """Calculate confidence score for the parse result."""
        score = 1.0
        
        # Deduct for missing fields
        critical_missing = len([f for f in result.missing_fields if 'sku' in f or 'date' in f])
        score -= critical_missing * 0.2
        
        # Deduct for other missing fields
        other_missing = len(result.missing_fields) - critical_missing
        score -= other_missing * 0.05
        
        # Deduct for warnings
        score -= len(result.warnings) * 0.03
        
        # Deduct for errors
        score -= len(result.errors) * 0.1
        
        # Bonus for complete data
        if not result.missing_fields and not result.errors:
            score = min(score + 0.1, 1.0)
            
        return max(0.0, min(1.0, score))
        
    def _get_retry_after(self, error: RateLimitError) -> Optional[int]:
        """Extract retry-after value from rate limit error."""
        try:
            # Try to extract from error message
            if hasattr(error, 'response') and error.response:
                retry_after = error.response.headers.get('Retry-After')
                if retry_after:
                    return int(retry_after)
        except:
            pass
        return None
        
    def _sanitize_input(self, text: str) -> str:
        """Sanitize input text to remove sensitive information."""
        if not text:
            return text
            
        # Remove credit card numbers (basic pattern)
        text = re.sub(r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b', '[CARD_NUMBER]', text)
        
        # Remove SSN patterns
        text = re.sub(r'\b\d{3}-\d{2}-\d{4}\b', '[SSN]', text)
        
        # Limit length to prevent token overflow
        max_length = 10000
        if len(text) > max_length:
            text = text[:max_length] + "... [truncated]"
            
        return text
        
    def _log_result(self, result: ParseResult):
        """Log parse result with sensitive data removed."""
        if not logger.isEnabledFor(logging.DEBUG):
            return
            
        # Create sanitized copy for logging
        log_data = {
            'status': result.status.value,
            'confidence': result.confidence_score,
            'missing_fields': result.missing_fields,
            'warnings_count': len(result.warnings),
            'errors_count': len(result.errors),
            'requires_review': result.requires_review
        }
        
        if result.data:
            log_data['type'] = result.data.get('type')
            log_data['has_items'] = bool(result.data.get('items'))
            log_data['item_count'] = len(result.data.get('items', []))
            
        logger.debug(f"Parse result: {json.dumps(log_data, indent=2)}")
        
    def _get_enhanced_system_prompt(self) -> str:
        """Get enhanced system prompt with strict instructions."""
        return """You are an expert email parser for inventory management. Parse purchase and sales emails with these STRICT rules:

CRITICAL INSTRUCTIONS:
1. NEVER guess or invent data. If a field is missing, set it to null.
2. Only extract information explicitly stated in the email.
3. If you cannot determine the transaction type with confidence, return {"type": "unknown"}.
4. Preserve original values even if they seem incorrect - flag them for review instead.

DATA EXTRACTION RULES:
- Date: Extract exactly as shown, or null if missing
- SKU: Must be explicitly stated (e.g., "SKU:", "Item #:", "Product Code:"), never guess
- Prices: Only use explicitly stated amounts, never calculate or assume
- Order numbers: Extract exactly as shown, including any prefixes
- Quantities: Must be clearly stated numbers, default to null if ambiguous

For PURCHASE emails return:
{
    "type": "purchase",
    "date": "YYYY-MM-DD or original format" or null,
    "vendor_name": "exact vendor name" or null,
    "order_number": "exact order number" or null,
    "items": [
        {
            "name": "exact product name" or null,
            "sku": "exact SKU if explicitly stated" or null,
            "quantity": number or null,
            "unit_price": number or null,
            "total": number or null
        }
    ],
    "subtotal": number or null,
    "taxes": number or null,
    "shipping": number or null,
    "total": number or null,
    "confidence_indicators": {
        "has_explicit_skus": boolean,
        "has_clear_vendor": boolean,
        "has_order_number": boolean
    }
}

For SALES emails return:
{
    "type": "sale",
    "date": "YYYY-MM-DD or original format" or null,
    "channel": "eBay/Shopify/Amazon/etc" or null,
    "order_number": "exact order number" or null,
    "customer_email": "email if present" or null,
    "items": [
        {
            "name": "exact product name" or null,
            "sku": "exact SKU if explicitly stated" or null,
            "quantity": number or null,
            "sale_price": number or null,
            "tax": number or null,
            "fees": number or null,
            "total": number or null
        }
    ],
    "subtotal": number or null,
    "taxes": number or null,
    "fees": number or null,
    "total": number or null,
    "confidence_indicators": {
        "has_explicit_skus": boolean,
        "has_clear_channel": boolean,
        "has_order_number": boolean
    }
}

If the email is neither clearly a purchase nor sale, or if critical information is too ambiguous:
{
    "type": "unknown",
    "reason": "brief explanation",
    "partial_data": {any fields you could extract}
}

IMPORTANT: Set any field to null if not explicitly found. Never interpolate or guess missing data."""
        
    def _create_enhanced_prompt(self, body: str, subject: str) -> str:
        """Create enhanced prompt with context."""
        return f"""Parse this email into structured JSON following the STRICT rules provided.

IMPORTANT REMINDERS:
- Set fields to null if not found
- Never guess SKUs or invent data
- Extract dates exactly as shown
- Flag as "unknown" if transaction type is unclear

Email Subject: {subject}

Email Body:
---
{body}
---

Extract all available data into the specified JSON format. Remember: null for missing fields, never guess."""