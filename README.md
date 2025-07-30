# BizniWeb Order Export Tool

Python scripts to export orders from BizniWeb GraphQL API to CSV format and automatically generate invoices.

## Features

### Order Export
- Export all order items with detailed information
- Filter out cancelled (Storno) orders automatically
- Generate aggregated reports by date and product
- Generate daily summary reports with expense and ROI calculations
- Handle pagination for large datasets
- Robust error handling

### Invoice Generation
- Automatically generate invoices for eligible orders
- Filter orders by status, payment method, and invoice status
- Dry-run mode for testing
- Cross-platform execution scripts
- Comprehensive logging

## Quick Start

### Using the Shell Script (Recommended)

The easiest way to run the export is using the provided shell script:

```bash
# Export last 30 days
./export_orders.sh

# Export specific date range
./export_orders.sh -f 2024-01-01 -t 2024-01-31

# Show help
./export_orders.sh --help
```

The script will:
- Check Python installation
- Create and activate virtual environment
- Install all dependencies
- Check for API token configuration
- Run the export with your specified parameters

### Manual Setup

1. **Create virtual environment:**
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. **Install dependencies:**
```bash
pip install -r requirements.txt
```

3. **Configure API credentials:**
```bash
cp .env.example .env
```

Edit `.env` and add your BizniWeb API token:
```
BIZNISWEB_API_TOKEN=your_actual_token_here
BIZNISWEB_API_URL=https://vevo.flox.sk/api/graphql
```
Token you can generate in your BizniWeb account Settings -> BiznisWeb API -> Novy API Token button
## Usage

### Command Line Options

```bash
python export_orders.py [OPTIONS]

Options:
  --from-date DATE    Start date in YYYY-MM-DD format (default: 30 days ago)
  --to-date DATE      End date in YYYY-MM-DD format (default: today)
```

### Examples

```bash
# Export last 30 days (default)
python export_orders.py

# Export specific month
python export_orders.py --from-date 2024-01-01 --to-date 2024-01-31

# Export year to date
python export_orders.py --from-date 2024-01-01

# Export single day
python export_orders.py --from-date 2024-01-15 --to-date 2024-01-15
```

## Output Files

The script generates four CSV files in the `data/` directory:

### 1. Main Export File
**Filename:** `export_YYYYMMDD-YYYYMMDD.csv`

Contains one row per order item with:
- Order information (number, ID, date, status)
- Total items in order and item position
- Customer details (name, email, company ID, VAT ID)
- Item details (name, EAN, quantity, price, tax rate)
- Addresses (invoice and delivery)
- Order totals and currency

### 2. Date-Product Aggregation
**Filename:** `aggregate_by_date_product_YYYYMMDD-YYYYMMDD.csv`

Groups sales by date and product:
- Date
- Product name
- Total quantity sold
- Total revenue
- Number of orders

### 3. Daily Summary
**Filename:** `aggregate_by_date_YYYYMMDD-YYYYMMDD.csv`

Daily totals:
- Date
- Total quantity of items
- Total revenue (without tax)
- Number of unique orders
- Total item count

### 4. Items Summary
**Filename:** `aggregate_by_items_YYYYMMDD-YYYYMMDD.csv`

Product totals across all dates:
- Product name
- Total quantity sold
- Total price without tax
- Number of unique orders containing the product

## Filtering

The export automatically excludes:
- Orders with status "Storno" (cancelled orders)

## Error Handling

- The script handles API pagination limits (30 items per request)
- Continues processing if server errors occur during pagination
- Validates API token presence before running
- Creates data directory automatically if missing

## Requirements

- Python 3.7 or higher
- See `requirements.txt` for Python package dependencies

## API Configuration

- **API URL:** `https://vevo.flox.sk/api/graphql`
- **Authentication:** `BW-API-Key: Token {your_token}`
- **GraphQL Schema:** https://www.biznisweb.sk/api/docs/schema.graphql

## Troubleshooting

### "BIZNISWEB_API_TOKEN not found"
Make sure you've created the `.env` file and added your API token.

### "No orders found"
Check that:
- Your date range contains orders
- Your API token has proper permissions
- The date format is YYYY-MM-DD

### Server errors during export
The script will continue with partial data if server errors occur. Check the console output for the number of orders successfully fetched.

### Permission denied on shell script
Make the script executable:
```bash
chmod +x export_orders.sh
chmod +x generate_invoices*.sh
```

## Invoice Generation

### Overview
The `generate_invoices.py` script automatically creates invoices for orders based on specific criteria:
- Status: "Odoslaná" (sent)
- Payment method: "Dobierkou" (cash on delivery)
- No existing invoice

### Requirements

**Web login credentials are required** for invoice creation. The script will not proceed without valid credentials.

Add your BizniWeb login credentials to the `.env` file:
```
BIZNISWEB_USERNAME=your_username@example.com
BIZNISWEB_PASSWORD=your_password
```

### Usage

```bash
# Create invoices for last 7 days
python generate_invoices.py

# Create invoices for specific date range
python generate_invoices.py --from-date 2024-01-01 --to-date 2024-01-31

# Dry run (preview without creating invoices)
python generate_invoices.py --dry-run
```

### How it Works

1. **Login** - Authenticates with BizniWeb web interface using provided credentials
2. **Session Validation** - Verifies the session is active and obtains ARF token
3. **Fetch Orders** - Retrieves orders from GraphQL API for the specified date range
4. **Filter Orders** - Identifies orders matching the criteria (cash on delivery, no invoice)
5. **Create Invoices** - Creates invoices for each matching order via web API
6. **Send Emails** - Automatically sends invoice emails to customers

### Output

The script displays:
- Login status and session validation
- Number of orders fetched
- Details of each order being processed
- Success/failure status for each invoice
- Summary with total processed and amounts

### Cross-Platform Scripts

For automated daily execution, use the appropriate script for your platform:

**Unix-like systems (Linux/macOS):**
```bash
./generate_invoices_cross_platform.sh
```

**Windows Command Prompt:**
```cmd
generate_invoices.bat
```

**Windows PowerShell:**
```powershell
.\generate_invoices.ps1
```

All scripts support the same command-line arguments as the Python script.

### Scheduling Daily Execution

**Linux/macOS (cron):**
```bash
# Add to crontab (runs daily at 8 AM)
0 8 * * * /path/to/generate_invoices_cross_platform.sh
```

**Windows (Task Scheduler):**
- Use `generate_invoices.bat` or `generate_invoices.ps1`
- Set trigger to daily at desired time

See `CROSS_PLATFORM_SCRIPTS.md` for detailed platform-specific instructions.