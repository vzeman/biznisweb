# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

BizniWeb Order Export Tool - A Python script that exports orders from BizniWeb GraphQL API to CSV format.

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

## Architecture

### Key Components
- `export_orders.py`: Main script with GraphQL client and CSV export logic
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