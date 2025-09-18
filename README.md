\# Inventory Reconciliation System



An automated system that reads purchase and sales confirmation emails from Gmail, parses them using OpenAI, and syncs inventory data to both Airtable and Zoho Inventory with proper COGS tracking.



\## Features



\- \*\*Automated Email Processing\*\*: Monitors Gmail inbox for new purchase/sales emails

\- \*\*AI-Powered Parsing\*\*: Uses OpenAI GPT-4 to extract structured data from emails

\- \*\*Multi-Platform Integration\*\*: 

&nbsp; - Airtable for data storage and reporting

&nbsp; - Zoho Inventory for inventory management

&nbsp; - Discord for real-time notifications

\- \*\*Weighted Average Costing\*\*: Automatically calculates and applies WAC for accurate COGS

\- \*\*Error Handling\*\*: Comprehensive logging and retry mechanisms

\- \*\*Real-time Monitoring\*\*: Discord notifications for all operations



\## Prerequisites



\- Python 3.11+

\- Gmail account with App Password enabled

\- OpenAI API key

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

&nbsp;  - Go to Google Account settings

&nbsp;  - Security → 2-Step Verification → App passwords

&nbsp;  - Generate password for "Mail"

3\. Add credentials to `.env`



\### OpenAI Setup



1\. Get API key from https://platform.openai.com

2\. Add to `.env` as `OPENAI\_API\_KEY`



\### Airtable Setup



1\. Create a new base with two tables:

&nbsp;  - `Purchases`: For purchase orders

&nbsp;  - `Sales`: For sales transactions

2\. Get your API key from Airtable account settings

3\. Find your Base ID (in Airtable API documentation)

4\. Add credentials to `.env`



\### Zoho OAuth2 Setup



1\. Register your application in Zoho API Console:

&nbsp;  - Visit https://api-console.zoho.com

&nbsp;  - Create a new client (Server-based Application)

&nbsp;  - Note the Client ID and Client Secret

2\. Generate refresh token:

&nbsp;  - Use Zoho OAuth2 playground or Postman

&nbsp;  - Scope needed: `ZohoInventory.fullaccess.all`

3\. Get your Organization ID from Zoho Inventory settings

4\. Add all credentials to `.env`



\### Discord Webhook Setup



1\. In your Discord server, go to Server Settings → Integrations

2\. Create a new webhook

3\. Copy the webhook URL

4\. Add to `.env` as `DISCORD\_WEBHOOK\_URL`



\## Deployment on Railway



1\. Create a new project on Railway

2\. Connect your GitHub repository

3\. Add all environment variables from `.env`

4\. Railway will automatically detect the `Procfile` and deploy



\## Local Development



Run the application locally:

```bash

python main.py

```



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

Tax: $40.00

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

\- Product B (SKU: PROD-B) - Qty: 1 @ $85.00



Subtotal: $175.00

Tax: $14.00

eBay Fees: $17.50

Total: $206.50

```



\## System Architecture



```

Gmail Inbox → Email Parser (OpenAI) → Data Processing

&nbsp;                                          ↓

&nbsp;                                   ┌─────────────┐

&nbsp;                                   │             │

&nbsp;                                   ↓             ↓

&nbsp;                             Airtable      Zoho Inventory

&nbsp;                            (Storage)      (Complete Workflow)

&nbsp;                                   │             │

&nbsp;                                   └──────┬──────┘

&nbsp;                                          ↓

&nbsp;                                   Discord Notifications

```



\### Zoho Inventory Workflow



\*\*Purchase Processing:\*\*

1\. Create Purchase Order (PO)

2\. Auto-create Bill from PO → \*\*Updates inventory levels\*\*

3\. Mark as paid (if applicable)

4\. Native WAC calculation by Zoho



\*\*Sales Processing:\*\*

1\. Create Sales Order (SO)

2\. Auto-create Invoice from SO → \*\*Commits the sale\*\*

3\. Auto-create Shipment → \*\*Reduces inventory levels\*\*

4\. Mark as paid (if applicable)

5\. COGS automatically applied using WAC



\## Project Structure



```

inventory-reconciliation/

├── src/

│   ├── \_\_init\_\_.py

│   ├── gmail\_client.py      # Gmail IMAP operations

│   ├── openai\_parser.py     # Email parsing with AI

│   ├── airtable\_client.py   # Airtable data storage

│   ├── zoho\_client.py       # Zoho inventory management

│   ├── discord\_notifier.py  # Discord notifications

│   └── config.py            # Configuration management

├── main.py                  # Main application entry point

├── requirements.txt         # Python dependencies

├── Procfile                # Deployment configuration

├── .env.example            # Environment variables template

├── README.md               # This file

└── .gitignore             # Git ignore rules

```



\## Monitoring



The system sends Discord notifications for:

\- ✅ Successful order processing

\- ⚠️ Parsing warnings

\- ❌ API errors or failures

\- ℹ️ System status updates



Check the `inventory\_reconciliation.log` file for detailed debugging information.



\## Troubleshooting



\### Gmail Connection Issues

\- Verify App Password is correct

\- Check that IMAP is enabled in Gmail settings

\- Ensure no IP restrictions on your Google account



\### OpenAI Parsing Errors

\- Check API key validity and credits

\- Review email format for consistency

\- Increase temperature in parser for more flexibility



\### Zoho API Errors

\- Refresh token may be expired (regenerate if needed)

\- Check API rate limits

\- Verify organization ID is correct



\### Airtable Sync Issues

\- Verify table names match configuration

\- Check field types in Airtable match data types

\- Ensure API key has write permissions



\## Advanced Configuration



\### Custom Polling Interval

Adjust the `POLL\_INTERVAL` environment variable (in seconds):

\- Default: 300 (5 minutes)

\- Minimum recommended: 60 (1 minute)

\- Maximum recommended: 3600 (1 hour)



\### Logging Levels

Set `LOG\_LEVEL` in environment variables:

\- `DEBUG`: Detailed information for debugging

\- `INFO`: General operational messages (default)

\- `WARNING`: Warning messages only

\- `ERROR`: Error messages only



\## Contributing



1\. Fork the repository

2\. Create a feature branch (`git checkout -b feature/amazing-feature`)

3\. Commit your changes (`git commit -m 'Add amazing feature'`)

4\. Push to the branch (`git push origin feature/amazing-feature`)

5\. Create a Pull Request



\## License



MIT License - see LICENSE file for details



\## Support



For issues or questions, please create a GitHub issue or contact support.



\## Security Notes



\- Never commit `.env` files to version control

\- Rotate API keys regularly

\- Use strong App Passwords for Gmail

\- Monitor Discord notifications for suspicious activity

\- Enable 2FA on all service accounts

