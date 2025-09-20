"""OpenAI-based email parser with robust validation and completeness checking."""

import json
import logging
import time
import re
from typing import Dict, Optional, List, Any, Union, Tuple
from datetime import datetime
from dataclasses import dataclass, asdict, field
from enum import Enum
from openai import OpenAI, RateLimitError, APIError, APIConnectionError

logger = logging.getLogger(__name__)


class ParseStatus(Enum):
    """Email parsing status codes."""
    SUCCESS = "success"
    INCOMPLETE = "incomplete"  # New status for incomplete but valid data
    PARTIAL = "partial"
    FAILED = "failed"
    UNKNOWN_TYPE = "unknown_type"
    VALIDATION_ERROR = "validation_error"
    API_ERROR = "api_error"


class DataCompleteness(Enum):
    """Data completeness levels."""
    COMPLETE = "complete"
    INCOMPLETE = "incomplete"
    INVALID = "invalid"


@dataclass
class ItemCompleteness:
    """Track completeness of individual items."""
    has_name: bool = False
    has_quantity: bool = False
    has_unit_price: bool = False
    has_sku_or_identifier: bool = False
    
    @property
    def is_complete(self) -> bool:
        """Check if item has all required fields."""
        return all([
            self.has_name,
            self.has_quantity,
            self.has_unit_price
        ])
        
    @property
    def missing_fields(self) -> List[str]:
        """Get list of missing fields."""
        missing = []
        if not self.has_name:
            missing.append("name")
        if not self.has_quantity:
            missing.append("quantity")
        if not self.has_unit_price:
            missing.append("unit_price")
        if not self.has_sku_or_identifier:
            missing.append("sku/identifier")
        return missing


@dataclass
class ParseResult:
    """Container for parse results with enhanced completeness tracking."""
    status: ParseStatus
    data: Optional[Dict] = None
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    missing_fields: List[str] = field(default_factory=list)
    incomplete_items: List[Dict] = field(default_factory=list)
    confidence_score: float = 0.0
    parse_time: float = 0.0
    requires_review: bool = False
    completeness: DataCompleteness = DataCompleteness.INVALID
    completeness_details: Dict = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for storage."""
        result = asdict(self)
        result['status'] = self.status.value
        result['completeness'] = self.completeness.value
        return result


class EmailParser:
    """Parse emails using OpenAI API with comprehensive completeness validation."""
    
    # Complete data requirements (per PRD)
    COMPLETE_DATA_REQUIREMENTS = {
        'purchase': {
            'required': ['date', 'vendor_name', 'items', 'taxes'],
            'optional': ['order_number', 'shipping', 'subtotal', 'total'],
            'item_required': ['name', 'quantity', 'unit_price']
        },
        'sale': {
            'required': ['date', 'channel', 'items', 'taxes'],
            'optional': ['order_number', 'customer_email', 'fees', 'shipping', 'subtotal', 'total'],
            'item_required': ['name', 'quantity', 'sale_price']
        }
    }
    
    # Field validation patterns
    VALIDATION_PATTERNS = {
        'date': r'^\d{4}-\d{2}-\d{2}$',
        'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
        'order_number': r'^[A-Za-z0-9\-_#]+$',
        'sku': r'^[A-Za-z0-9\-_]+$',
        'upc': r'^\d{12,13}$'
    }
    
    def __init__(self, config):
        self.config = config
        self.client = OpenAI(api_key=config.get('OPENAI_API_KEY'))
        self.model = config.get('OPENAI_MODEL', 'gpt-4o-mini')  # Default to gpt-4o-mini
        self.temperature = config.get_float('OPENAI_TEMPERATURE', 0.1)
        self.max_retries = config.get_int('OPENAI_MAX_RETRIES', 3)
        self.retry_delay = config.get_int('OPENAI_RETRY_DELAY', 2)
        self.enable_sanitization = config.get_bool('ENABLE_DATA_SANITIZATION', True)
        self.strict_completeness = config.get_bool('STRICT_COMPLETENESS_CHECK', True)
        
    def parse_email(self, body: str, subject: str) -> ParseResult:
        """
        Parse email content with comprehensive completeness validation.
        
        Args:
            body: Email body content
            subject: Email subject
            
        Returns:
            ParseResult with status, data, completeness info
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
                        parse_time=time.time() - start_time,
                        completeness=DataCompleteness.INVALID
                    )
                
                # Validate completeness and enrich the data
                validation_result = self._validate_completeness(parsed_data)
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
                    parse_time=time.time() - start_time,
                    completeness=DataCompleteness.INVALID
                )
                
            except (APIError, APIConnectionError) as e:
                logger.error(f"OpenAI API error (attempt {attempt + 1}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (2 ** attempt))
                    continue
                return ParseResult(
                    status=ParseStatus.API_ERROR,
                    errors=[f"API error: {str(e)}"],
                    parse_time=time.time() - start_time,
                    completeness=DataCompleteness.INVALID
                )
                
            except Exception as e:
                logger.error(f"Unexpected error parsing email: {e}", exc_info=True)
                return ParseResult(
                    status=ParseStatus.FAILED,
                    errors=[f"Unexpected error: {str(e)}"],
                    parse_time=time.time() - start_time,
                    completeness=DataCompleteness.INVALID
                )
        
        return ParseResult(
            status=ParseStatus.FAILED,
            errors=["Max retries exceeded"],
            parse_time=time.time() - start_time,
            completeness=DataCompleteness.INVALID
        )
        
    def _call_openai(self, body: str, subject: str) -> Optional[str]:
        """Call OpenAI API with the email content."""
        try:
            prompt = self._create_enhanced_prompt(body, subject)
            
            # Removed response_format parameter as it's not supported by all models
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": self._get_completeness_focused_prompt()},
                    {"role": "user", "content": prompt}
                ],
                temperature=self.temperature,
                max_tokens=2000
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"Error calling OpenAI: {e}")
            raise
            
    def _parse_response(self, response: str) -> Optional[Dict]:
        """Parse and clean the OpenAI response."""
        try:
            # First, try to extract JSON from the response
            json_text = self._extract_json_from_text(response)
            
            if not json_text:
                logger.error("No JSON found in OpenAI response")
                return None
            
            # Try to parse as JSON
            data = json.loads(json_text)
            
            # Clean up the data
            return self._clean_parsed_data(data)
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON from OpenAI: {e}")
            logger.debug(f"Raw response: {response}")
            return None
            
    def _extract_json_from_text(self, text: str) -> Optional[str]:
        """Extract JSON object from text response."""
        # Try to find JSON block markers first
        json_patterns = [
            r'```json\s*(\{.*?\})\s*```',  # ```json { ... } ```
            r'```\s*(\{.*?\})\s*```',     # ``` { ... } ```
            r'(\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\})',  # Direct JSON object
        ]
        
        for pattern in json_patterns:
            matches = re.findall(pattern, text, re.DOTALL | re.IGNORECASE)
            if matches:
                # Return the first match
                return matches[0].strip()
        
        # If no clear JSON block found, try to extract the largest JSON-like structure
        # Look for anything that starts with { and ends with }
        start_idx = text.find('{')
        if start_idx != -1:
            # Find the matching closing brace
            brace_count = 0
            for i, char in enumerate(text[start_idx:], start_idx):
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        return text[start_idx:i+1]
        
        return None
        
    def _clean_parsed_data(self, data: Dict) -> Dict:
        """Clean and normalize parsed data."""
        cleaned = {}
        
        for key, value in data.items():
            if value is not None and value != "":
                if isinstance(value, str):
                    value = value.strip()
                    if value.lower() in ['null', 'none', 'n/a', 'unknown']:
                        value = None
                    else:
                        cleaned[key] = value
                elif isinstance(value, list):
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
                    if value >= 0:
                        cleaned[key] = value
                else:
                    cleaned[key] = value
                    
        return cleaned
        
    def _validate_completeness(self, data: Dict) -> ParseResult:
        """
        Validate data completeness according to PRD requirements.
        
        Complete data must have:
        - Item names (all items)
        - Quantities for each item
        - Price per item (unit price)
        - Tax as separate field
        - Shipping is optional
        """
        result = ParseResult(status=ParseStatus.SUCCESS, data=data)
        
        # Check transaction type
        transaction_type = data.get('type')
        
        if not transaction_type:
            result.status = ParseStatus.UNKNOWN_TYPE
            result.errors.append("Transaction type not identified")
            result.completeness = DataCompleteness.INVALID
            return result
            
        if transaction_type not in ['purchase', 'sale']:
            if transaction_type == 'unknown':
                result.status = ParseStatus.UNKNOWN_TYPE
                result.completeness = DataCompleteness.INVALID
                return result
            else:
                result.warnings.append(f"Unexpected transaction type: {transaction_type}")
                
        # Get requirements for this transaction type
        requirements = self.COMPLETE_DATA_REQUIREMENTS.get(transaction_type, {})
        
        # Track completeness
        completeness_tracker = {
            'has_date': False,
            'has_vendor_or_channel': False,
            'has_all_items': False,
            'has_taxes': False,
            'items_complete': [],
            'items_incomplete': []
        }
        
        # Check date
        if data.get('date'):
            completeness_tracker['has_date'] = True
            self._validate_date(data, result)
        else:
            result.missing_fields.append('date')
            
        # Check vendor/channel
        if transaction_type == 'purchase':
            if data.get('vendor_name'):
                completeness_tracker['has_vendor_or_channel'] = True
            else:
                result.missing_fields.append('vendor_name')
        else:  # sale
            if data.get('channel'):
                completeness_tracker['has_vendor_or_channel'] = True
            else:
                result.missing_fields.append('channel')
                
        # Check tax (required as separate field per PRD)
        if data.get('taxes') is not None:
            completeness_tracker['has_taxes'] = True
        else:
            result.missing_fields.append('taxes')
            result.warnings.append("Tax must be captured as a separate field")
            
        # Validate items completeness
        items = data.get('items', [])
        if not items:
            result.errors.append("No items found")
            result.missing_fields.append('items')
            completeness_tracker['has_all_items'] = False
        else:
            all_items_complete = True
            price_field = 'unit_price' if transaction_type == 'purchase' else 'sale_price'
            
            for i, item in enumerate(items):
                item_check = ItemCompleteness()
                
                # Check item name
                if item.get('name'):
                    item_check.has_name = True
                else:
                    result.missing_fields.append(f'item_{i+1}_name')
                    
                # Check quantity
                if item.get('quantity') is not None and item.get('quantity') > 0:
                    item_check.has_quantity = True
                else:
                    result.missing_fields.append(f'item_{i+1}_quantity')
                    
                # Check unit price
                if item.get(price_field) is not None and item.get(price_field) >= 0:
                    item_check.has_unit_price = True
                else:
                    result.missing_fields.append(f'item_{i+1}_{price_field}')
                    
                # Check for SKU or other identifier
                if item.get('sku') or item.get('upc') or item.get('product_id'):
                    item_check.has_sku_or_identifier = True
                else:
                    result.warnings.append(f"Item {i+1}: Missing SKU/identifier")
                    
                # Track item completeness
                if item_check.is_complete:
                    completeness_tracker['items_complete'].append({
                        'index': i,
                        'name': item.get('name'),
                        'complete': True
                    })
                else:
                    all_items_complete = False
                    completeness_tracker['items_incomplete'].append({
                        'index': i,
                        'name': item.get('name', f'Item {i+1}'),
                        'missing': item_check.missing_fields
                    })
                    result.incomplete_items.append({
                        'item': item,
                        'missing_fields': item_check.missing_fields
                    })
                    
            completeness_tracker['has_all_items'] = all_items_complete
            
        # Validate amounts
        self._validate_amounts(data, result)
        
        # Determine overall completeness
        is_complete = all([
            completeness_tracker['has_date'],
            completeness_tracker['has_vendor_or_channel'],
            completeness_tracker['has_all_items'],
            completeness_tracker['has_taxes']
        ])
        
        if is_complete:
            result.completeness = DataCompleteness.COMPLETE
            result.status = ParseStatus.SUCCESS
            result.requires_review = False
        else:
            result.completeness = DataCompleteness.INCOMPLETE
            result.status = ParseStatus.INCOMPLETE
            result.requires_review = True
            
        # Calculate confidence score
        result.confidence_score = self._calculate_confidence(data, result, completeness_tracker)
        
        # Store completeness details
        result.completeness_details = completeness_tracker
        
        # Add metadata
        data['parse_metadata'] = {
            'status': result.status.value,
            'completeness': result.completeness.value,
            'confidence': result.confidence_score,
            'missing_fields': result.missing_fields,
            'incomplete_items': len(result.incomplete_items),
            'requires_review': result.requires_review,
            'parsed_at': datetime.utcnow().isoformat()
        }
        
        return result
        
    def _validate_date(self, data: Dict, result: ParseResult):
        """Validate and normalize date field."""
        date_field = data.get('date')
        
        if date_field:
            if not re.match(self.VALIDATION_PATTERNS['date'], date_field):
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
                    
    def _validate_amounts(self, data: Dict, result: ParseResult):
        """Validate and reconcile monetary amounts."""
        subtotal = data.get('subtotal', 0) or 0
        taxes = data.get('taxes', 0) or 0
        shipping = data.get('shipping', 0) or 0
        fees = data.get('fees', 0) or 0
        total = data.get('total', 0) or 0
        
        # Calculate expected total
        if data.get('type') == 'purchase':
            calculated_total = subtotal + taxes + shipping
        else:  # sale
            calculated_total = subtotal + taxes - fees + shipping
            
        # Check if totals match (with small tolerance for rounding)
        if total > 0 and abs(calculated_total - total) > 0.10:
            result.warnings.append(
                f"Total mismatch: calculated ${calculated_total:.2f} vs stated ${total:.2f}"
            )
            data['total_calculated'] = calculated_total
            data['total_mismatch'] = True
            
        # Ensure all amounts are floats
        for field in ['subtotal', 'taxes', 'shipping', 'fees', 'total']:
            if field in data:
                try:
                    data[field] = float(data[field])
                except (TypeError, ValueError):
                    result.warnings.append(f"Invalid amount for {field}")
                    data[field] = 0.0
                    
    def _calculate_confidence(self, data: Dict, result: ParseResult, 
                             completeness_tracker: Dict) -> float:
        """Calculate confidence score based on completeness."""
        score = 1.0
        
        # Major deductions for missing critical fields
        if not completeness_tracker['has_date']:
            score -= 0.2
        if not completeness_tracker['has_vendor_or_channel']:
            score -= 0.2
        if not completeness_tracker['has_taxes']:
            score -= 0.15
        if not completeness_tracker['has_all_items']:
            incomplete_ratio = len(completeness_tracker['items_incomplete']) / max(1, len(data.get('items', [])))
            score -= 0.3 * incomplete_ratio
            
        # Minor deductions for warnings
        score -= len(result.warnings) * 0.02
        
        # Bonus for complete data
        if result.completeness == DataCompleteness.COMPLETE:
            score = min(score + 0.1, 1.0)
            
        return max(0.0, min(1.0, score))
        
    def _get_retry_after(self, error: RateLimitError) -> Optional[int]:
        """Extract retry-after value from rate limit error."""
        try:
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
            
        # Remove credit card numbers
        text = re.sub(r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b', '[CARD_NUMBER]', text)
        
        # Remove SSN patterns
        text = re.sub(r'\b\d{3}-\d{2}-\d{4}\b', '[SSN]', text)
        
        # Limit length
        max_length = 10000
        if len(text) > max_length:
            text = text[:max_length] + "... [truncated]"
            
        return text
        
    def _log_result(self, result: ParseResult):
        """Log parse result with sensitive data removed."""
        if not logger.isEnabledFor(logging.DEBUG):
            return
            
        log_data = {
            'status': result.status.value,
            'completeness': result.completeness.value,
            'confidence': result.confidence_score,
            'missing_fields': result.missing_fields,
            'incomplete_items_count': len(result.incomplete_items),
            'warnings_count': len(result.warnings),
            'errors_count': len(result.errors),
            'requires_review': result.requires_review
        }
        
        if result.data:
            log_data['type'] = result.data.get('type')
            log_data['has_items'] = bool(result.data.get('items'))
            log_data['item_count'] = len(result.data.get('items', []))
            
        logger.debug(f"Parse result: {json.dumps(log_data, indent=2)}")
        
    def _get_completeness_focused_prompt(self) -> str:
        """Get system prompt focused on completeness requirements."""
        return """You are an expert email parser for inventory management. Parse purchase and sales emails with STRICT completeness requirements.

COMPLETENESS REQUIREMENTS (per PRD):
- Item names: REQUIRED for all items
- Quantities: REQUIRED for each item
- Unit prices: REQUIRED for each item
- Tax: REQUIRED as a separate field (not included in item prices)
- Shipping: OPTIONAL

CRITICAL INSTRUCTIONS:
1. NEVER guess or invent data. If a field is missing, set it to null.
2. Extract EXACTLY what is in the email. Do not interpolate missing values.
3. Tax must be captured separately from item prices.
4. Mark any item missing name, quantity, or unit price as incomplete.
5. ALWAYS respond with valid JSON wrapped in ```json ``` blocks.

For PURCHASE emails return:
```json
{
    "type": "purchase",
    "date": "YYYY-MM-DD" or null,
    "vendor_name": "exact vendor name" or null,
    "order_number": "exact order number" or null,
    "items": [
        {
            "name": "exact product name" or null,
            "sku": "SKU if present" or null,
            "upc": "UPC if present" or null,
            "product_id": "any other ID" or null,
            "quantity": number or null,
            "unit_price": number (excluding tax) or null,
            "item_tax": number (if item-specific) or null
        }
    ],
    "subtotal": number or null,
    "taxes": number (total tax as separate field) or null,
    "shipping": number or null,
    "total": number or null
}
```

For SALES emails return:
```json
{
    "type": "sale",
    "date": "YYYY-MM-DD" or null,
    "channel": "eBay/Shopify/Amazon/etc" or null,
    "order_number": "exact order number" or null,
    "customer_email": "email if present" or null,
    "items": [
        {
            "name": "exact product name" or null,
            "sku": "SKU if present" or null,
            "upc": "UPC if present" or null,
            "product_id": "any other ID" or null,
            "quantity": number or null,
            "sale_price": number (excluding tax) or null,
            "item_tax": number (if item-specific) or null
        }
    ],
    "subtotal": number or null,
    "taxes": number (total tax as separate field) or null,
    "fees": number or null,
    "shipping": number or null,
    "total": number or null
}
```

If the email is neither clearly a purchase nor sale:
```json
{
    "type": "unknown",
    "reason": "brief explanation",
    "partial_data": {any fields you could extract}
}
```

IMPORTANT: 
- Set any missing field to null
- Tax MUST be a separate field from item prices
- Do NOT include tax in unit_price or sale_price
- Extract all available product identifiers (SKU, UPC, product ID)
- ALWAYS wrap your JSON response in ```json ``` blocks"""
        
    def _create_enhanced_prompt(self, body: str, subject: str) -> str:
        """Create enhanced prompt with completeness focus."""
        return f"""Parse this email into structured JSON following the STRICT completeness rules.

REMEMBER:
- Extract item names, quantities, and unit prices for ALL items
- Tax must be captured as a SEPARATE field
- Set missing fields to null - do not guess
- Unit prices should EXCLUDE tax
- Always wrap JSON in ```json ``` blocks

Email Subject: {subject}

Email Body:
---
{body}
---

Extract all available data into the specified JSON format. Ensure tax is separated from item prices."""