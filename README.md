# BizniWeb Order Export Tool

Python script to export orders from BizniWeb GraphQL API to CSV format with aggregated reports.

## Features

- Export all order items with detailed information
- Filter out cancelled (Storno) orders automatically
- Generate aggregated reports by date and product
- Generate daily summary reports
- Handle pagination for large datasets
- Robust error handling

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

The script generates three CSV files in the `data/` directory:

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
- Total revenue
- Number of unique orders
- Total item count

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
```