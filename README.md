\# Inventory Reconciliation System



An automated system that reads purchase and sales confirmation emails from Gmail, validates data completeness, parses them using OpenAI, and syncs complete inventory data to both Airtable and Zoho Inventory using physical stock tracking.



\## Key Features



\- \*\*Complete Data Validation\*\*: Enforces strict completeness requirements before syncing to Zoho

\- \*\*Human Review Workflow\*\*: Incomplete data is flagged for manual review with Discord notifications

\- \*\*Intelligent SKU Management\*\*: Auto-generates SKUs when missing, searches by name/UPC/product ID

\- \*\*Physical Stock Tracking\*\*: Direct stock adjustments instead of accounting-based tracking

\- \*\*AI-Powered Parsing\*\*: Uses OpenAI GPT-4 to extract structured data from emails

\- \*\*Multi-Platform Integration\*\*: 

&nbsp; - Gmail for email monitoring

&nbsp; - Airtable for data storage and review

&nbsp; - Zoho Inventory for stock management

&nbsp; - Discord for real-time notifications and commands

\- \*\*Error Prevention\*\*: Only complete, validated data enters inventory system



\## System Architecture



```

Gmail Inbox → Email Parser (OpenAI) → Completeness Validation

&nbsp;                                          ↓

&nbsp;                                   ┌─────────────┐

&nbsp;                                   │             │

&nbsp;                               COMPLETE      INCOMPLETE

&nbsp;                                   ↓             ↓

&nbsp;                             Airtable ←──────→ Airtable

&nbsp;                                   ↓             ↓

&nbsp;                             Zoho Stock    Human Review

&nbsp;                             Adjustment    (Discord Alert)

&nbsp;                                   ↓             ↓

&nbsp;                             ✅ Success    Reply "resolved"

&nbsp;                                             ↓

&nbsp;                                        Re-validate

&nbsp;                                             ↓

&nbsp;                                   Sync if Complete

```



\## Data Completeness Requirements



Per PRD specifications, \*\*complete data\*\* must include:

\- ✅ \*\*Item names\*\* for all items

\- ✅ \*\*Quantities\*\* for each item

\- ✅ \*\*Unit prices\*\* per item (excluding tax)

\- ✅ \*\*Tax\*\* as a separate field

\- ℹ️ \*\*Shipping\*\* is optional



Data missing any required fields will:

1\. Be saved to Airtable with incomplete flag

2\. NOT sync to Zoho

3\. Trigger human review notification

4\. Wait for manual completion



\## Prerequisites



\- Python 3.11+

\- Gmail account with App Password enabled

\- OpenAI API key (GPT-4 access)

\- Airtable account with API access

\- Zoho Inventory account with OAuth2 setup

\- Discord server with webhook configured



\## Installation



1\. Clone the repository:

```bash

git clone https://github.com/yourusername/inventory-reconciliation.git

cd inventory-reconciliation

```



2\. Install dependencies:

```bash

pip install -r requirements.txt

```



3\. Configure environment variables:

```bash

cp .env.example .env

\# Edit .env with your actual credentials

```



\## Configuration



\### Gmail Setup



1\. Enable 2-factor authentication on your Gmail account

2\. Generate an App Password:

&nbsp;  - Go to \[Google Account Settings](https://myaccount.google.com/apppasswords)

&nbsp;  - Security → 2-Step Verification → App passwords

&nbsp;  - Generate password for "Mail"

3\. Add credentials to `.env`:

&nbsp;  - `GMAIL\_USER`: Your Gmail address

&nbsp;  - `GMAIL\_APP\_PASSWORD`: Generated app password



\### OpenAI Setup



1\. Get API key from \[OpenAI Platform](https://platform.openai.com)

2\. Ensure you have GPT-4 access

3\. Add to `.env` as `OPENAI\_API\_KEY`



\### Airtable Setup



1\. Create a new base with two tables:



\*\*Purchases Table Fields:\*\*

\- Order Number (text)

\- Date (date)

\- Vendor (text)

\- Items (long text/JSON)

\- Subtotal (currency)

\- Taxes (currency) - \*\*Required\*\*

\- Shipping (currency)

\- Total (currency)

\- Requires Review (checkbox)

\- Processing Status (single select)

\- Missing Fields (text)

\- Confidence Score (number)



\*\*Sales Table Fields:\*\*

\- Order Number (text)

\- Date (date)

\- Channel (text)

\- Customer Email (email)

\- Items (long text/JSON)

\- Subtotal (currency)

\- Taxes (currency) - \*\*Required\*\*

\- Fees (currency)

\- Total (currency)

\- Requires Review (checkbox)

\- Processing Status (single select)

\- Missing Fields (text)

\- Confidence Score (number)



2\. Get your API key from Airtable account settings

3\. Find your Base ID (in Airtable API documentation)

4\. Add to `.env`:

&nbsp;  - `AIRTABLE\_API\_KEY`

&nbsp;  - `AIRTABLE\_BASE\_ID`

&nbsp;  - `AIRTABLE\_PURCHASES\_TABLE` (default: "Purchases")

&nbsp;  - `AIRTABLE\_SALES\_TABLE` (default: "Sales")



\### Zoho Inventory Setup (Physical Stock Tracking)



1\. Register application in \[Zoho API Console](https://api-console.zoho.com):

&nbsp;  - Create new client (Server-based Application)

&nbsp;  - Note Client ID and Client Secret



2\. Generate refresh token:

&nbsp;  - Use Zoho OAuth2 playground

&nbsp;  - Required scope: `ZohoInventory.fullaccess.all`



3\. Configure Zoho for physical stock tracking:

&nbsp;  - Enable inventory tracking for all items

&nbsp;  - Set up tax configurations

&nbsp;  - Note your Organization ID



4\. Add to `.env`:

&nbsp;  - `ZOHO\_CLIENT\_ID`

&nbsp;  - `ZOHO\_CLIENT\_SECRET`

&nbsp;  - `ZOHO\_REFRESH\_TOKEN`

&nbsp;  - `ZOHO\_ORGANIZATION\_ID`

&nbsp;  - `ZOHO\_USE\_PHYSICAL\_STOCK=true`

&nbsp;  - `ZOHO\_AUTO\_GENERATE\_SKU=true`



\### Discord Setup



1\. Create webhook in Discord server:

&nbsp;  - Server Settings → Integrations → Webhooks

&nbsp;  - Create New Webhook

&nbsp;  - Copy webhook URL



2\. Add to `.env`:

&nbsp;  - `DISCORD\_WEBHOOK\_URL`

&nbsp;  - `DISCORD\_MENTION\_ON\_ERROR` (optional user/role ID)



\## Usage



\### Running the System



```bash

python main.py

```



The system will:

1\. Monitor Gmail inbox every 5 minutes (configurable)

2\. Parse new emails with OpenAI

3\. Validate data completeness

4\. Process based on validation:

&nbsp;  - \*\*Complete data\*\* → Airtable → Zoho → Success notification

&nbsp;  - \*\*Incomplete data\*\* → Airtable → Review notification → Wait for human



\### Human Review Workflow



When incomplete data is detected:



1\. \*\*Discord Alert\*\* is sent:

```

⚠️ Human Review Required - Incomplete Data



Missing Required Fields:

• vendor\_name

• item\_2\_quantity

• taxes



Action Required:

1\. Open Airtable record ID: rec\_xyz123

2\. Fill missing fields

3\. Reply 'resolved rec\_xyz123' when complete

```



2\. \*\*Human reviews\*\* Airtable record and fills missing fields



3\. \*\*Human replies\*\* in Discord:

```

resolved rec\_xyz123

```



4\. \*\*System re-validates\*\* and syncs if complete



\### Discord Commands



\- `resolved <airtable\_id>` - Mark review as complete and trigger re-validation

\- `status` - Show current system statistics

\- `pending` - List all pending reviews



\## Email Format Examples



\### Purchase Email Format

```

Subject: Purchase Order Confirmation #PO-2024-001



Order Details:

Date: 2024-01-15

Vendor: ABC Suppliers



Items:

\- Product A (SKU: PROD-A) - Qty: 10 @ $25.00 = $250.00

\- Product B (SKU: PROD-B) - Qty: 5 @ $50.00 = $250.00



Subtotal: $500.00

Tax: $40.00  ← Required as separate field

Shipping: $10.00

Total: $550.00

```



\### Sales Email Format

```

Subject: Order Confirmation - eBay Order #123456



Sale Date: 2024-01-16

Platform: eBay

Customer: customer@example.com



Items Sold:

\- Product A (SKU: PROD-A) - Qty: 2 @ $45.00

\- Product B - Qty: 1 @ $85.00  ← SKU will be auto-generated



Subtotal: $175.00

Tax: $14.00  ← Required as separate field

eBay Fees: $17.50

Total: $206.50

```



\## SKU Management



The system handles SKUs intelligently:



1\. \*\*Existing SKU\*\*: Uses provided SKU

2\. \*\*UPC Available\*\*: Searches/creates with UPC

3\. \*\*Product ID\*\*: Uses alternative identifiers

4\. \*\*Name Match\*\*: Searches existing items by name

5\. \*\*Auto-Generation\*\*: Creates unique SKU using pattern: `AUTO-{PREFIX}-{HASH}`



Example auto-generated SKU: `AUTO-WIDG-A3F2B1`



\## Physical Stock Tracking



Instead of using accounting-based tracking (bills/invoices), the system uses direct stock adjustments:



\*\*Purchase Processing:\*\*

```

1\. Parse email → Complete data validation

2\. Create/match items with SKUs

3\. Calculate unit costs including tax proportion

4\. Create positive stock adjustment

5\. Update inventory levels immediately

```



\*\*Sales Processing:\*\*

```

1\. Parse email → Complete data validation

2\. Match items to existing SKUs

3\. Calculate COGS based on current item cost

4\. Create negative stock adjustment

5\. Reduce inventory levels immediately

```



\## Monitoring \& Notifications



\### Success Notifications

```

✅ Purchase Successfully Processed

Order #: PO-12345

Vendor: ABC Suppliers

Items: 5

Tax: $45.00

Stock Adjusted: ✅

Items Processed: 5

```



\### Review Required Notifications

```

⚠️ Human Review Required

Missing: vendor\_name, taxes

Items incomplete: 2

Action: Review Airtable record rec\_xyz123

```



\### Error Notifications

```

❌ Processing Failed

Error: Zoho API timeout

Details: Check logs for full trace

```



\## Deployment



\### Railway Deployment



1\. Create new project on Railway

2\. Connect GitHub repository

3\. Add all environment variables from `.env`

4\. Railway will detect `Procfile` and deploy as worker



\### Docker Deployment



```dockerfile

FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

CMD \["python", "main.py"]

```



\## Troubleshooting



\### Common Issues



\*\*Incomplete Data Loops\*\*

\- Ensure all required fields are filled in Airtable

\- Check that tax is a separate field, not included in prices

\- Verify quantities are numbers, not text



\*\*SKU Generation Issues\*\*

\- Check `ZOHO\_AUTO\_GENERATE\_SKU=true`

\- Verify items don't have duplicate names

\- Ensure SKU prefix is configured



\*\*Stock Not Updating\*\*

\- Verify `ZOHO\_USE\_PHYSICAL\_STOCK=true`

\- Check Zoho item has inventory tracking enabled

\- Ensure complete data validation passed



\*\*Discord Commands Not Working\*\*

\- Verify webhook URL is correct

\- Check bot has proper permissions

\- Ensure command format is exact



\### Logging



Check `inventory\_reconciliation.log` for detailed debugging:

```bash

tail -f inventory\_reconciliation.log

```



Log levels (set in `.env`):

\- `DEBUG`: Detailed processing steps

\- `INFO`: Normal operations (default)

\- `WARNING`: Issues that don't stop processing

\- `ERROR`: Processing failures



\## Advanced Configuration



\### Environment Variables



```env

\# Completeness Validation

STRICT\_COMPLETENESS\_CHECK=true

CONFIDENCE\_THRESHOLD=0.7



\# Physical Stock Tracking

ZOHO\_USE\_PHYSICAL\_STOCK=true

ZOHO\_AUTO\_GENERATE\_SKU=true

ZOHO\_SKU\_PREFIX=AUTO



\# Processing

POLL\_INTERVAL=300  # seconds

EMAIL\_BATCH\_SIZE=10

MAX\_RETRIES=3



\# Features

ENABLE\_DRY\_RUN=false

ENABLE\_CONNECTION\_TEST=true

ENABLE\_HUMAN\_REVIEW=true

```



\## Security Best Practices



1\. \*\*Never commit `.env` files\*\* to version control

2\. \*\*Use App Passwords\*\* for Gmail, not regular passwords

3\. \*\*Rotate API keys\*\* regularly

4\. \*\*Use secret managers\*\* in production (AWS/Azure/Google)

5\. \*\*Enable 2FA\*\* on all service accounts

6\. \*\*Monitor Discord\*\* for suspicious activity

7\. \*\*Validate webhook URLs\*\* before deployment



\## Contributing



1\. Fork the repository

2\. Create feature branch (`git checkout -b feature/amazing-feature`)

3\. Commit changes (`git commit -m 'Add amazing feature'`)

4\. Push to branch (`git push origin feature/amazing-feature`)

5\. Open Pull Request



\## License



MIT License - see LICENSE file for details



\## Support



For issues or questions:

\- Create a GitHub issue

\- Check existing issues for solutions

\- Review logs for detailed error messages



\## Version History



\### v2.0.0 (Current)

\- Complete data validation workflow

\- Physical stock tracking

\- Human review system

\- Auto SKU generation

\- Discord command integration



\### v1.0.0

\- Initial release

\- Basic email parsing

\- Accounting-based tracking

