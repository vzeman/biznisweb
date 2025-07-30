# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

BizniWeb Order Export and Invoice Management Tools - Python scripts for exporting orders from BizniWeb GraphQL API to CSV format and identifying orders that need invoices.

## Commands

### Setup
```bash
pip install -r requirements.txt
cp .env.example .env
# Edit .env to add API token
```

### Run export
```bash
# Export last 30 days
python export_orders.py

# Export specific date range
python export_orders.py --from-date 2024-01-01 --to-date 2024-01-31
```

### Identify orders needing invoices
```bash
# Identify orders needing invoices for last 7 days
python generate_invoices.py

# Identify orders for specific date range
python generate_invoices.py --from-date 2024-01-01 --to-date 2024-01-31

# Dry run (preview what would be identified)
python generate_invoices.py --dry-run
```

### Running invoice generation with scripts

#### Cross-platform shell script (Linux/macOS/Windows Git Bash):
```bash
./generate_invoices_cross_platform.sh
./generate_invoices_cross_platform.sh --dry-run
```

#### Windows batch file:
```cmd
generate_invoices.bat
generate_invoices.bat --dry-run
```

#### Windows PowerShell:
```powershell
.\generate_invoices.ps1
.\generate_invoices.ps1 -dry-run
```

## Architecture

### Key Components
- `export_orders.py`: Main script with GraphQL client and CSV export logic
- `generate_invoices.py`: Script to identify orders needing invoices and export them for manual processing
- `.env`: Configuration file for API credentials (not tracked in git)
- `data/`: Output directory for CSV exports

### API Configuration
- API URL: https://vevo.flox.sk/api/graphql
- Authentication: BW-API-Key header with format "Token {api_token}"
- GraphQL schema: https://www.biznisweb.sk/api/docs/schema.graphql
- GraphQL fragments: https://www.biznisweb.sk/api/docs/fragments.graphql

### Export Logic
1. Fetches orders using GraphQL query with date filtering
2. Handles pagination to get all orders
3. Flattens nested order data (one row per item)
4. Exports to CSV with comprehensive order, customer, and item details

### Invoice Generation Logic
1. Fetches orders from specified date range (default: last 7 days)
2. Filters orders matching criteria:
   - Status: "Odoslaná" (sent) or "Čaká na vybavenie" (waiting for processing)
   - Payment method: "Dobierkou" (cash on delivery)
   - No existing invoices
3. Identifies orders needing invoices and logs them
4. Exports to CSV for manual processing
5. Provides dry-run mode for testing

Note: Actual invoice creation requires session-based authentication (arf parameter) not available through API token. Orders must be processed manually in BizniWeb admin.