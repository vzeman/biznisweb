#!/usr/bin/env python3
"""
Export orders from BizniWeb GraphQL API to CSV
"""

import os
import csv
import argparse
import time
import json
import traceback
import hashlib
import re
import unicodedata
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import calendar
import numpy as np
from logger_config import get_logger

try:
    from dotenv import load_dotenv
except ImportError:
    print("❌ Missing package: python-dotenv")
    print("Please run: pip install python-dotenv")
    exit(1)

try:
    from gql import gql, Client
    from gql.transport.requests import RequestsHTTPTransport
except ImportError:
    print("❌ Missing package: gql")
    print("Please run: pip install 'gql[all]>=3.5.0'")
    exit(1)

try:
    import pandas as pd
except ImportError:
    print("❌ Missing package: pandas")
    print("Please run: pip install pandas>=2.0.0")
    exit(1)

# Optional packages - don't fail if missing
try:
    from facebook_ads import FacebookAdsClient
except ImportError:
    print("⚠️  Facebook Ads integration not available (missing facebook-business package)")
    class FacebookAdsClient:
        def __init__(self):
            self.is_configured = False
        def get_daily_spend(self, *args, **kwargs):
            return {}

try:
    from google_ads import GoogleAdsClient
except ImportError:
    print("⚠️  Google Ads integration not available (missing google-ads package)")
    print("   To enable, run: pip install google-ads google-auth-oauthlib google-auth-httplib2")
    class GoogleAdsClient:
        def __init__(self):
            self.is_configured = False
        def get_daily_spend(self, *args, **kwargs):
            return {}

from html_report_generator import generate_html_report, generate_email_strategy_report

# Load environment variables
load_dotenv()

# Set up logging
logger = get_logger('export_orders')

# Configuration
API_URL = os.getenv('BIZNISWEB_API_URL', 'https://vevo.flox.sk/api/graphql')
API_TOKEN = os.getenv('BIZNISWEB_API_TOKEN')

if not API_TOKEN:
    logger.error("BIZNISWEB_API_TOKEN not found in environment variables. Please set it in .env file.")
    raise ValueError("BIZNISWEB_API_TOKEN not found in environment variables. Please set it in .env file.")

# Fixed costs
PACKAGING_COST_PER_ORDER = 0.3  # EUR per order
SHIPPING_SUBSIDY_PER_ORDER = 0.2  # EUR per order (shipping subsidy)
FIXED_MONTHLY_COST = 0  # EUR per month (Marek, Uctovnictvo)

# Currency conversion rates to EUR
# These should be updated regularly or fetched from an API
CURRENCY_RATES_TO_EUR = {
    'EUR': 1.0,
    'CZK': 0.04,  # 1 CZK = ~0.04 EUR (1 EUR = ~25 CZK)
    'HUF': 0.0025,  # 1 HUF = ~0.0025 EUR (1 EUR = ~400 HUF)
    'PLN': 0.23,  # 1 PLN = ~0.23 EUR (1 EUR = ~4.3 PLN)
    'USD': 0.92,  # 1 USD = ~0.92 EUR
}

# Product expense mapping (expense per item in EUR)
# Keys are SKU/EAN codes or hash-based SKUs (H-XXXXXXXX) for products without EAN
# Products not found in this mapping will default to 1.0 EUR expense
PRODUCT_EXPENSES = {
    # === PARFUMY 500ml ===
    '8586024430327': 7.02,    # No.01 Cotton Paradise (500ml)
    '8586024430358': 9.38,    # No.02 Sweet Paradise (500ml)
    '8586024430433': 4.79,    # No.06 Royal Cotton (500ml)
    '8586024430457': 5.64,    # No.07 Ylang Absolute (500ml)
    '8586024430471': 6.53,    # No.08 Cotton Dream (500ml)
    '8586024430495': 5.80,    # No.09 Pure Garden (500ml)

    # === PARFUMY 200ml ===
    '8586024430310': 3.35,    # No.01 Cotton Paradise (200ml)
    '8586024430341': 4.29,    # No.02 Sweet Paradise (200ml)
    '8586024430426': 2.46,    # No.06 Royal Cotton (200ml)
    '8586024430440': 2.79,    # No.07 Ylang Absolute (200ml)
    '8586024430464': 3.15,    # No.08 Cotton Dream (200ml)
    '8586024430488': 2.86,    # No.09 Pure Garden (200ml)

    # === SADY VZORIEK ===
    'H-7D043A91': 1.79,       # Sada vzoriek všetkých vôní Vevo 6 x 10ml
    'H-E77D4634': 0.86,       # Sada vzoriek najpredávanejších vôní Vevo 3 x 10ml
    'H-125E3A73': 1.79,       # Sada vzoriek všetkých vôní Vevo Natural 6 x 10ml
    'H-31566B7A': 0.85,       # Sada vzoriek najpredávanejších vôní Vevo Natural 3 x 10ml

    # === PREMIUM VZORKY ===
    'H-A2620358': 0.42,       # Premium No.07 Ylang Absolute (Vzorka 10ml)
    'H-D00F4D4A': 0.47,       # Premium No.08 Cotton Dream (Vzorka 10ml)
    'H-45E7507C': 0.43,       # Premium No.09 Pure Garden (Vzorka 10ml)
    'H-29C4BDE2': 1.32,       # Vzorky parfumov do prania Vevo Premium 3 x 10ml

    # === NATURAL VZORKY ===
    '8586024430334': 0.30,    # Natural No.01 Cotton Paradise (Vzorka 10ml)
    'H-34DA3CE0': 0.35,       # Natural No.02 Sweet Paradise (Vzorka 10ml)
    'H-B3DCA297': 0.26,       # Natural No.06 Royal Cotton (Vzorka 10ml)
    'H-5854129C': 0.28,       # Natural No.07 Ylang Absolute (Vzorka 10ml)
    'H-4F7230B9': 0.29,       # Natural No.08 Cotton Dream (Vzorka 10ml)
    'H-A1EA61E5': 0.28,       # Natural No.09 Pure Garden (Vzorka 10ml)

    # === DOPLNKY ===
    'H-8F8BF46E': 0.31,       # Odmerka Vevo 7ml drevená
    'H-3583EAEC': 0.65,       # Vevo Shot - koncentrát na čistenie práčky 100ml
    'H-F03DF99A': 2.43,       # Prací gél hypoalergénny z Marseillského mydla 1L
    '8594201618000': 2.43,    # Prací gél hypoalergénny (EAN variant)
    'H-C633B766': 2.43,       # Prací gél Levanduľa 1L
    'H-95B10CAD': 2.43,       # Prací gél Ruža 1L
    'H-231AAF25': 2.83,       # Perkarbonát sodný PLUS 1kg
    'H-A2C58C41': 2.43,       # Strong PINK čistiaca pasta 500g
    'H-5916EC93': 4.80,       # Čistič podláh do robotického mopu
    'H-65B41890': 1.00,       # Biely ocot v spreji 500 ml
    'H-29C4BDE2': 1.00,       # Interiérový sprej Vevo Premium Škorica & Ihličie 150ml

    # === NULOVÉ NÁKLADY ===
    'H-36CA74A7': 0,          # Tringelt
    'H-A5F3BBB3': 0,          # Poistenie proti rozbitiu
}

# nove ceny nakladov
# PRODUCT_EXPENSES = {
#     'Sada vzoriek najpredávanejších vôní Vevo 6 x 10ml': 1.38,
#     'Sada vzoriek všetkých vôní Vevo 6 x 10ml': 1.38,
#     'Sada najpredávanejších vzoriek Vevo 6 x 10ml': 1.38,
#     'Sada 6 najpredávanejších vzoriek po 1ks': 1.38,
#     'Sada najpredávanejších vzoriek 6 x 10ml': 1.38,
#     'Sada vzorků všech vůní Vevo (6 × 10 ml)': 1.38,
#     'Sada vzoriek najpredávanejších vôní Vevo 3 x 10ml': 0.69,
#     'Parfum do prania Vevo No.08 Cotton Dream (500ml)': 3.13,
#     'Vevo No.08 Cotton Dream mosóparfüm (500ml)': 3.13,
#     'Parfum do prania Vevo No.07 Ylang Absolute (200ml)': 1.79,
#     'Vevo No.07 Ylang Absolute mosóparfüm (200ml)': 1.79,
#     'Parfum do prania Vevo No.08 Cotton Dream (200ml)': 1.79,
#     'Parfum do prania Vevo No.09 Pure Garden (200ml)': 1.79,
#     'Parfum do prania Vevo No.01 Cotton Paradise (500ml)': 3.13,
#     'Parfum do prania Vevo No.01 Cotton Paradise (200ml)': 1.79,
#     'Parfum do prania Vevo No.09 Pure Garden (500ml)': 3.13,
#     'Parfém na praní Vevo No.09 Pure Garden (500ml)': 1.79,
#     'Parfum do prania Vevo No.06 Royal Cotton (200ml)': 1.79,
#     'Parfum do prania Vevo No.02 Sweet Paradise (200ml)': 1.79,
#     'Odmerka Vevo 7ml drevená na parfum do prania': 0.31,
#     'Parfum do prania Vevo No.02 Sweet Paradise (500ml)': 3.13,
#     'Parfum do prania Vevo No.07 Ylang Absolute (Vzorka 10ml)': 0.22,
#     'Parfum do prania Vevo No.07 Ylang Absolute (Vzorka)': 0.22,
#     'Parfum do prania Vevo No.06 Royal Cotton (500ml)': 3.13,
#     'Parfum do prania Vevo No.08 Cotton Dream (Vzorka 10ml)': 0.22,
#     'Parfum do prania Vevo No.08 Cotton Dream (Vzorka)': 0.22,
#     'Parfum do prania Vevo No.07 Ylang Absolute (500ml)': 3.13,
#     'Parfum do prania Vevo No.09 Pure Garden (Vzorka 10ml)': 0.22,
#     'Parfum do prania Vevo No.09 Pure Garden (Vzorka)': 0.22,
#     'Parfum do prania Vevo No.02 Sweet Paradise (Vzorka 10ml)': 0.22,
#     'Parfum do prania Vevo No.02 Sweet Paradise (Vzorka)': 0.22,
#     'Parfum do prania Vevo No.06 Royal Cotton (Vzorka 10ml)': 0.22,
#     'Parfum do prania Vevo No.06 Royal Cotton (Vzorka)': 0.22,
#     'Parfum do prania Vevo No.03 Lavender Kiss (Vzorka)': 0.22,
#     'Tringelt': 0,
#     'Parfum do prania Vevo No.01 Cotton Paradise (Vzorka 10ml)': 0.22,
#     'Poistenie proti rozbitiu': 0,
#     'Vevo Shot - koncentrát na čistenie práčky 100ml': 0.65,
#     'Vevo Shot – koncentrát na čištění pračky 100 ml': 0.65
# }

# GraphQL query with fragments
ORDER_QUERY = gql("""
query GetOrders($filter: OrderFilter, $params: OrderParams) {
  getOrderList(filter: $filter, params: $params) {
    data {
      id
      order_num
      external_ref
      pur_date
      var_symb
      last_change
      oss
      oss_country
      status {
        id
        name
        color
      }
      customer {
        ... on Company {
          company_name
          company_id
          vat_id
          vat_id2
          name
          surname
          phone
          email
        }
        ... on Person {
          name
          surname
          phone
          email
        }
        ... on UnauthenticatedEmail {
          name
          surname
          phone
          email
        }
      }
      invoice_address {
        street
        descriptive_number
        orientation_number
        city
        zip
        country
      }
      items {
        item_label
        ean
        import_code
        warehouse_number
        quantity
        tax_rate
        weight {
          value
          unit
        }
        recycle_fee {
          value
          formatted
          is_net_price
          currency {
            symbol
            code
          }
        }
        price {
          value
          formatted
          is_net_price
          currency {
            symbol
            code
          }
        }
      }
      sum {
        value
        formatted
        is_net_price
        currency {
          symbol
          code
        }
      }
    }
    pageInfo {
      hasNextPage
      hasPreviousPage
      nextCursor
      previousCursor
      pageIndex
      totalPages
    }
  }
}
""")


class BizniWebExporter:
    def __init__(self, api_url: str, api_token: str):
        """Initialize the exporter with API credentials"""
        transport = RequestsHTTPTransport(
            url=api_url,
            headers={'BW-API-Key': f'Token {api_token}'},
            verify=True,
            retries=3,
        )
        self.client = Client(transport=transport, fetch_schema_from_transport=False)
        self.fb_client = FacebookAdsClient()
        self.google_ads_client = GoogleAdsClient()
        self.cache_dir = Path('data/cache')
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_days_threshold = 7  # Days from today that should always be fetched fresh (changed from 3 to 7)
        self.customer_first_order_dates = {}  # Track first order date for each customer
        self.excluded_orders = []  # Track orders with failed/excluded statuses for segmentation

    @staticmethod
    def get_product_sku(ean: str, title: str) -> str:
        """
        Get a consistent product SKU/identifier.
        Uses EAN if available, otherwise creates a short hash from the title.
        """
        if pd.notna(ean) and str(ean).strip() and str(ean).strip() != '':
            return str(ean).strip()
        # Create a short hash from the title (8 characters)
        title_hash = hashlib.md5(str(title).encode()).hexdigest()[:8].upper()
        return f"H-{title_hash}"

    def add_product_sku_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add a consistent product_sku column to the dataframe."""
        df['product_sku'] = df.apply(
            lambda row: self.get_product_sku(row.get('item_ean'), row.get('item_label', 'Unknown')),
            axis=1
        )
        return df

    def add_order_revenue_net_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add canonical order-level net revenue used across analytics.
        Definition: sum(item_total_without_tax) per order_num (EUR, net of VAT).
        """
        if 'item_total_without_tax' not in df.columns:
            df['order_revenue_net'] = df.get('order_total', 0)
            return df

        order_net_map = df.groupby('order_num')['item_total_without_tax'].sum().to_dict()
        df['order_revenue_net'] = df['order_num'].map(order_net_map).fillna(0).round(2)
        return df

    def deduplicate_orders(self, orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Remove duplicated raw orders by stable key (order_num preferred, fallback id).
        Keeps the last occurrence.
        """
        dedup_map: Dict[str, Dict[str, Any]] = {}
        unknown_orders: List[Dict[str, Any]] = []
        duplicate_count = 0

        for order in orders:
            order_num = str(order.get('order_num') or '').strip()
            order_id = str(order.get('id') or '').strip()
            dedup_key = order_num or (f"id:{order_id}" if order_id else '')

            if not dedup_key:
                # Very rare: no order_num and no id. Keep as-is to avoid data loss.
                unknown_orders.append(order)
                continue

            if dedup_key in dedup_map:
                duplicate_count += 1
            dedup_map[dedup_key] = order

        deduped_orders = list(dedup_map.values()) + unknown_orders
        if duplicate_count > 0:
            logger.warning(f"Removed {duplicate_count} duplicated raw orders before flattening")
            print(f"Deduplication: removed {duplicate_count} duplicated raw orders")

        return deduped_orders
    
    def get_daily_fixed_cost(self, date: datetime) -> float:
        """Calculate daily fixed cost based on days in the month"""
        days_in_month = calendar.monthrange(date.year, date.month)[1]
        return FIXED_MONTHLY_COST / days_in_month
    
    def convert_to_eur(self, amount: float, currency: str) -> float:
        """Convert amount from given currency to EUR"""
        if not currency or not amount:
            return 0.0
        
        currency = currency.upper()
        if currency not in CURRENCY_RATES_TO_EUR:
            print(f"Warning: Unknown currency {currency}, treating as EUR")
            return amount
        
        return amount * CURRENCY_RATES_TO_EUR[currency]
    
    def fetch_orders_for_month(self, date_from: datetime, date_to: datetime) -> List[Dict[str, Any]]:
        """
        Fetch orders for a specific date range (typically one month) with retry logic

        Note: API filter requires partner token, so we fetch all orders and filter client-side
        """
        all_orders = []
        has_next_page = True
        cursor = None
        max_retries = 3
        retry_delay = 10
        page_delay = 0.5  # 500ms delay between pages
        consecutive_errors = 0

        logger.info(f"Fetching orders for month from API (will filter client-side for {date_from.strftime('%Y-%m-%d')} to {date_to.strftime('%Y-%m-%d')})")

        while has_next_page:
            # Remove filter parameter as it requires partner token
            variables = {
                'params': {
                    'limit': 30,
                    'order_by': 'pur_date',
                    'sort': 'ASC'
                }
            }
            
            if cursor is not None:
                variables['params']['cursor'] = cursor
            
            retry_count = 0
            success = False
            
            while retry_count < max_retries and not success:
                try:
                    result = self.client.execute(ORDER_QUERY, variable_values=variables)
                    orders_data = result.get('getOrderList', {})
                    orders = orders_data.get('data', [])
                    all_orders.extend(orders)
                    
                    page_info = orders_data.get('pageInfo', {})
                    has_next_page = page_info.get('hasNextPage', False)
                    cursor = page_info.get('nextCursor')
                    
                    print(f"Fetched {len(orders)} orders (total: {len(all_orders)})")
                    success = True
                    consecutive_errors = 0  # Reset error counter on success

                    # Delay between pages to avoid overwhelming the API
                    if has_next_page:
                        time.sleep(page_delay)

                except Exception as e:
                    retry_count += 1
                    consecutive_errors += 1

                    # Log the full error details
                    error_msg = str(e)
                    logger.debug(f"GraphQL error details: {error_msg}")

                    # Print full stack trace for debugging
                    if os.getenv('DEBUG'):
                        logger.debug("Full stack trace:")
                        logger.debug(traceback.format_exc())

                    if retry_count < max_retries:
                        logger.warning(f"Error fetching orders (attempt {retry_count}/{max_retries}): {error_msg[:200]}")
                        print(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                    else:
                        logger.error(f"Error fetching orders after {max_retries} attempts: {error_msg[:200]}")
                        logger.error(f"Full error: {error_msg}")
                        logger.error(f"Stack trace:\n{traceback.format_exc()}")

                        # If we've had too many consecutive errors and have some data, return what we have
                        if consecutive_errors >= 3 and all_orders:
                            logger.info(f"Returning {len(all_orders)} orders fetched so far due to persistent errors")
                            has_next_page = False
                        else:
                            # Otherwise just break this pagination loop
                            has_next_page = False
                        break

        logger.info(f"Fetched {len(all_orders)} total orders from API for month")

        # Filter by date range (client-side since API filter requires partner token)
        date_filtered_orders = []
        for order in all_orders:
            pur_date_str = order.get('pur_date', '')
            if pur_date_str:
                try:
                    # Parse date (format: YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
                    pur_date = datetime.strptime(pur_date_str.split()[0], '%Y-%m-%d')
                    # Check if order is within date range
                    if date_from <= pur_date <= date_to:
                        date_filtered_orders.append(order)
                except (ValueError, IndexError) as e:
                    logger.warning(f"Could not parse date '{pur_date_str}' for order {order.get('order_num', 'unknown')}: {e}")
                    continue

        logger.info(f"Filtered to {len(date_filtered_orders)} orders within date range for month")

        # Filter out orders with excluded statuses
        excluded_statuses = [
            'Storno',
            'Platba online - platnosť vypršala',
            'Platba online - platba zamietnutá',
            'Čaká na úhradu',
            'GoPay - platebni metoda potvrzena'
        ]

        filtered_orders = []
        excluded_counts = {}

        for order in date_filtered_orders:
            status = order.get('status', {}) or {}
            status_name = status.get('name', '')

            if status_name not in excluded_statuses:
                filtered_orders.append(order)
            else:
                excluded_counts[status_name] = excluded_counts.get(status_name, 0) + 1

        # Report excluded orders
        if excluded_counts:
            print("\nFiltered out orders:")
            for status, count in excluded_counts.items():
                print(f"  - {status}: {count} orders")

        logger.info(f"Final count after status filtering for month: {len(filtered_orders)} orders")

        return filtered_orders

    def get_cache_filename(self, date: datetime) -> Path:
        """Generate cache filename for a specific date"""
        date_str = date.strftime('%Y-%m-%d')
        return self.cache_dir / f"orders_{date_str}.json"
    
    def should_use_cache(self, date: datetime) -> bool:
        """Determine if cache should be used for a given date"""
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        date_normalized = date.replace(hour=0, minute=0, second=0, microsecond=0)
        days_ago = (today - date_normalized).days
        
        # Always fetch fresh data for recent days
        if days_ago <= self.cache_days_threshold:
            return False
        
        # Use cache for older data
        cache_file = self.get_cache_filename(date)
        return cache_file.exists()
    
    def load_from_cache(self, date: datetime) -> Optional[List[Dict[str, Any]]]:
        """Load orders from cache for a specific date"""
        cache_file = self.get_cache_filename(date)
        if not cache_file.exists():
            return None
        
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                print(f"  Loaded {len(data.get('orders', []))} orders from cache for {date.strftime('%Y-%m-%d')}")
                return data.get('orders', [])
        except Exception as e:
            print(f"  Error loading cache for {date.strftime('%Y-%m-%d')}: {e}")
            return None
    
    def save_to_cache(self, date: datetime, orders: List[Dict[str, Any]]):
        """Save orders to cache for a specific date"""
        cache_file = self.get_cache_filename(date)
        
        try:
            # Filter orders for this specific date
            date_str = date.strftime('%Y-%m-%d')
            day_orders = []
            
            for order in orders:
                # Handle different date formats
                purchase_date = order.get('purchase_date', '')
                if purchase_date:
                    # Extract just the date part if it includes time
                    if ' ' in purchase_date:
                        purchase_date = purchase_date.split(' ')[0]
                    
                    if purchase_date == date_str:
                        day_orders.append(order)
            
            cache_data = {
                'date': date_str,
                'cached_at': datetime.now().isoformat(),
                'order_count': len(day_orders),
                'orders': day_orders
            }
            
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            
            if day_orders:
                print(f"  Cached {len(day_orders)} orders for {date_str}")
        except Exception as e:
            print(f"  Error saving cache for {date.strftime('%Y-%m-%d')}: {e}")
    
    def _group_consecutive_dates(self, dates: List[datetime]) -> List[Tuple[datetime, datetime]]:
        """Group consecutive dates into ranges"""
        if not dates:
            return []
        
        dates = sorted(dates)
        ranges = []
        start = dates[0]
        end = dates[0]
        
        for date in dates[1:]:
            if (date - end).days == 1:
                end = date
            else:
                ranges.append((start, end))
                start = date
                end = date
        
        ranges.append((start, end))
        return ranges
    
    def fetch_all_orders_bulk(self, max_orders: int = 900, start_cursor: str = None, sort_order: str = 'DESC') -> tuple[List[Dict[str, Any]], str]:
        """
        Fetch orders from API in bulk, stopping before hitting API limits

        Args:
            max_orders: Maximum orders to fetch (default 900 to stay under ~960 API limit)
            start_cursor: Cursor to continue from (for pagination across batches)
            sort_order: Sort order for orders (DESC = newest first, ASC = oldest first)

        Returns:
            Tuple of (orders list, next cursor)
        """
        all_orders = []
        has_next_page = True
        cursor = start_cursor
        max_retries = 3
        retry_delay = 10
        page_delay = 0.5  # 500ms delay between pages

        logger.info(f"Fetching up to {max_orders} orders from API in bulk ({sort_order} order)" + (f", continuing from cursor" if cursor else ""))

        while has_next_page and len(all_orders) < max_orders:
            variables = {
                'params': {
                    'limit': 30,
                    'order_by': 'pur_date',
                    'sort': sort_order  # DESC = newest first (for recent orders), ASC = oldest first (for historical)
                }
            }

            if cursor is not None:
                variables['params']['cursor'] = cursor

            retry_count = 0
            success = False

            while retry_count < max_retries and not success:
                try:
                    result = self.client.execute(ORDER_QUERY, variable_values=variables)
                    orders_data = result.get('getOrderList', {})
                    orders = orders_data.get('data', [])
                    all_orders.extend(orders)

                    page_info = orders_data.get('pageInfo', {})
                    has_next_page = page_info.get('hasNextPage', False)
                    cursor = page_info.get('nextCursor')

                    print(f"Fetched {len(orders)} orders (total: {len(all_orders)})")
                    success = True

                    # Stop if we're approaching the limit
                    if len(all_orders) >= max_orders:
                        logger.info(f"Reached {len(all_orders)} orders, stopping to avoid API limits")
                        has_next_page = False

                    # Delay between pages to avoid overwhelming the API
                    if has_next_page:
                        time.sleep(page_delay)

                except Exception as e:
                    retry_count += 1
                    error_msg = str(e)

                    if retry_count < max_retries:
                        logger.warning(f"Error fetching orders (attempt {retry_count}/{max_retries}): {error_msg[:200]}")
                        print(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                    else:
                        logger.error(f"Error fetching orders after {max_retries} attempts: {error_msg[:200]}")
                        logger.info(f"Returning {len(all_orders)} orders fetched before error")
                        has_next_page = False
                        break

        logger.info(f"Bulk fetch complete: {len(all_orders)} orders" + (f", next cursor available" if cursor else ""))
        return all_orders, cursor

    def fetch_orders(self, date_from: datetime, date_to: datetime) -> List[Dict[str, Any]]:
        """Fetch all orders within the specified date range, using cache for older data"""
        all_orders = []
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

        # Clear excluded orders from previous runs
        self.excluded_orders = []

        print(f"\nProcessing date range: {date_from.strftime('%Y-%m-%d')} to {date_to.strftime('%Y-%m-%d')}")
        print(f"Cache policy: Using cache for data older than {self.cache_days_threshold} days")

        # Check which dates need fetching (not in cache)
        dates_to_fetch = []
        current_date = date_from

        while current_date <= date_to:
            days_ago = (today - current_date).days

            # Check if we should use cache for this date
            if self.should_use_cache(current_date):
                cached_orders = self.load_from_cache(current_date)
                if cached_orders:
                    all_orders.extend(cached_orders)
                    current_date += timedelta(days=1)
                    continue

            # This date needs fetching
            dates_to_fetch.append(current_date)
            current_date += timedelta(days=1)

        # If we have dates to fetch, do bulk fetches in batches
        if dates_to_fetch:
            print(f"\nFetching orders from API for {len(dates_to_fetch)} uncached dates...")
            dates_to_fetch_set = {d.strftime('%Y-%m-%d') for d in dates_to_fetch}

            # Determine sort order based on what dates we're fetching
            # If fetching recent dates (within last 7 days), use DESC to get newest first
            # If fetching older dates, use ASC to get oldest first (more efficient for historical data)
            recent_dates = [d for d in dates_to_fetch if (today - d).days <= self.cache_days_threshold]
            primary_sort_order = 'DESC' if recent_dates else 'ASC'

            logger.info(
                f"Using {primary_sort_order} order: {len(recent_dates)} recent dates, "
                f"{len(dates_to_fetch) - len(recent_dates)} older dates"
            )

            # Fetch in multiple batches until we truly cover the requested boundary.
            # NOTE: We cannot stop based on "all dates found" because some dates may
            # legitimately have zero orders.
            orders_by_date = {}
            max_batches = 200  # Safety limit for pathological pagination loops
            earliest_needed = min(dates_to_fetch)
            latest_needed = max(dates_to_fetch)

            def run_bulk_pass(
                sort_order: str,
                target_earliest: Optional[datetime] = None,
                target_latest: Optional[datetime] = None,
            ) -> Dict[str, Any]:
                """Run one directional bulk pass and collect orders by date."""
                next_cursor = None
                batch_num = 1
                reached_boundary = False
                oldest_seen = None
                latest_seen = None

                while batch_num <= max_batches:
                    print(f"Batch {batch_num} ({sort_order} order)...")
                    bulk_orders, next_cursor = self.fetch_all_orders_bulk(
                        max_orders=900,
                        start_cursor=next_cursor,
                        sort_order=sort_order
                    )

                    if not bulk_orders:
                        logger.warning(
                            f"Empty batch returned in {sort_order} mode (cursor_present={bool(next_cursor)}). "
                            "Stopping current pass."
                        )
                        break

                    parsed_batch_dates = []
                    for order in bulk_orders:
                        pur_date_str = order.get('pur_date', '')
                        if pur_date_str:
                            try:
                                pur_date = datetime.strptime(pur_date_str.split()[0], '%Y-%m-%d')
                                parsed_batch_dates.append(pur_date)
                                date_key = pur_date.strftime('%Y-%m-%d')

                                # Only keep orders within requested range
                                if date_key in dates_to_fetch_set:
                                    if date_key not in orders_by_date:
                                        orders_by_date[date_key] = []
                                    orders_by_date[date_key].append(order)
                            except (ValueError, IndexError):
                                continue

                    latest_in_batch = max(parsed_batch_dates) if parsed_batch_dates else None
                    oldest_in_batch = min(parsed_batch_dates) if parsed_batch_dates else None

                    if oldest_in_batch is not None:
                        oldest_seen = oldest_in_batch if oldest_seen is None else min(oldest_seen, oldest_in_batch)
                    if latest_in_batch is not None:
                        latest_seen = latest_in_batch if latest_seen is None else max(latest_seen, latest_in_batch)

                    # Stop once pagination reached pass target boundary.
                    if sort_order == 'DESC' and target_earliest is not None and oldest_in_batch is not None and oldest_in_batch <= target_earliest:
                        logger.info(
                            f"Reached start boundary in DESC mode (oldest_in_batch={oldest_in_batch.strftime('%Y-%m-%d')}, "
                            f"needed={target_earliest.strftime('%Y-%m-%d')})"
                        )
                        reached_boundary = True
                        break

                    if sort_order == 'ASC' and target_latest is not None and latest_in_batch is not None and latest_in_batch >= target_latest:
                        logger.info(
                            f"Reached end boundary in ASC mode (latest_in_batch={latest_in_batch.strftime('%Y-%m-%d')}, "
                            f"needed={target_latest.strftime('%Y-%m-%d')})"
                        )
                        reached_boundary = True
                        break

                    if not next_cursor:
                        logger.info(f"No next cursor available in {sort_order} mode, stopping current pass")
                        break

                    batch_num += 1
                    time.sleep(1)  # Small delay between batches

                if batch_num > max_batches:
                    logger.warning(
                        f"Stopped after max_batches={max_batches} in {sort_order} mode without fully confirming range boundary "
                        f"({date_from.strftime('%Y-%m-%d')} to {date_to.strftime('%Y-%m-%d')})"
                    )

                return {
                    'reached_boundary': reached_boundary,
                    'oldest_seen': oldest_seen,
                    'latest_seen': latest_seen,
                }

            # Primary pass
            primary_stats = run_bulk_pass(
                sort_order=primary_sort_order,
                target_earliest=earliest_needed if primary_sort_order == 'DESC' else None,
                target_latest=latest_needed if primary_sort_order == 'ASC' else None,
            )

            # Fallback pass from the opposite side when primary direction fails to reach boundary.
            # This protects full-history exports from intermittent cursor/page failures.
            if not primary_stats['reached_boundary']:
                if primary_sort_order == 'DESC':
                    # We already have newest side; fetch oldest side up to overlap with DESC data.
                    fallback_target_latest = primary_stats['oldest_seen'] or latest_needed
                    logger.warning(
                        "Primary DESC pass did not reach full historical boundary. "
                        f"Running ASC fallback up to {fallback_target_latest.strftime('%Y-%m-%d')}."
                    )
                    run_bulk_pass(
                        sort_order='ASC',
                        target_latest=fallback_target_latest,
                    )
                else:
                    # We already have oldest side; fetch newest side down to overlap with ASC data.
                    fallback_target_earliest = primary_stats['latest_seen'] or earliest_needed
                    logger.warning(
                        "Primary ASC pass did not reach latest boundary. "
                        f"Running DESC fallback down to {fallback_target_earliest.strftime('%Y-%m-%d')}."
                    )
                    run_bulk_pass(
                        sort_order='DESC',
                        target_earliest=fallback_target_earliest,
                    )

            # Cache and add orders for each date
            for date in dates_to_fetch:
                date_str = date.strftime('%Y-%m-%d')
                day_orders = orders_by_date.get(date_str, [])
                days_ago = (today - date).days

                # Filter by status
                filtered_orders = self._filter_by_status(day_orders)

                # Validate that all orders are actually from the requested date
                validated_orders = []
                seen_day_order_keys = set()
                for order in filtered_orders:
                    pur_date_str = order.get('pur_date', '')
                    if pur_date_str:
                        try:
                            order_date = datetime.strptime(pur_date_str.split()[0], '%Y-%m-%d')
                            if order_date.strftime('%Y-%m-%d') == date_str:
                                dedupe_key = order.get('order_num') or order.get('id')
                                if dedupe_key in seen_day_order_keys:
                                    continue
                                seen_day_order_keys.add(dedupe_key)
                                validated_orders.append(order)
                            else:
                                logger.warning(f"Order {order.get('order_num', 'unknown')} has date {order_date.strftime('%Y-%m-%d')} but was in {date_str} bucket")
                        except (ValueError, IndexError) as e:
                            logger.warning(f"Could not validate date for order {order.get('order_num', 'unknown')}: {e}")

                print(f"  {date_str}: {len(validated_orders)} orders")
                all_orders.extend(validated_orders)

                # Cache if appropriate
                if days_ago > self.cache_days_threshold:
                    self.save_to_cache_simple(date, validated_orders)

        # Final validation: ensure all orders are within the overall date range
        final_validated_orders = []
        seen_final_order_keys = set()
        out_of_range_count = 0
        for order in all_orders:
            pur_date_str = order.get('pur_date', '')
            if pur_date_str:
                try:
                    order_date = datetime.strptime(pur_date_str.split()[0], '%Y-%m-%d')
                    if date_from <= order_date <= date_to:
                        dedupe_key = order.get('order_num') or order.get('id')
                        if dedupe_key in seen_final_order_keys:
                            continue
                        seen_final_order_keys.add(dedupe_key)
                        final_validated_orders.append(order)
                    else:
                        out_of_range_count += 1
                        logger.warning(f"Order {order.get('order_num', 'unknown')} date {order_date.strftime('%Y-%m-%d')} is outside requested range {date_from.strftime('%Y-%m-%d')} to {date_to.strftime('%Y-%m-%d')}")
                except (ValueError, IndexError):
                    # If we can't parse the date, skip it
                    out_of_range_count += 1

        if out_of_range_count > 0:
            logger.warning(f"Filtered out {out_of_range_count} orders outside the requested date range")

        return final_validated_orders

    def _filter_by_status(self, orders: List[Dict[str, Any]], track_excluded: bool = True) -> List[Dict[str, Any]]:
        """Filter out orders with excluded statuses.

        Args:
            orders: List of orders to filter
            track_excluded: If True, store excluded orders for later segmentation analysis
        """
        excluded_statuses = [
            'Storno',
            'Platba online - platnosť vypršala',
            'Platba online - platba zamietnutá',
            'Čaká na úhradu',
            'GoPay - platebni metoda potvrzena'
        ]

        # Statuses for failed payment segmentation (subset of excluded)
        failed_payment_statuses = [
            'Platba online - platnosť vypršala',
            'Platba online - platba zamietnutá'
        ]

        filtered_orders = []
        for order in orders:
            status = order.get('status', {}) or {}
            status_name = status.get('name', '')

            if status_name not in excluded_statuses:
                filtered_orders.append(order)
            elif track_excluded and status_name in failed_payment_statuses:
                # Track failed payment orders for segmentation
                self.excluded_orders.append(order)

        return filtered_orders
    
    def save_to_cache_simple(self, date: datetime, orders: List[Dict[str, Any]]):
        """Save orders to cache for a specific date (simplified version for single-day fetches)"""
        cache_file = self.get_cache_filename(date)
        
        try:
            cache_data = {
                'date': date.strftime('%Y-%m-%d'),
                'cached_at': datetime.now().isoformat(),
                'order_count': len(orders),
                'orders': orders
            }
            
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            
            if orders:
                print(f"  Cached {len(orders)} orders for {date.strftime('%Y-%m-%d')}")
        except Exception as e:
            print(f"  Error saving cache for {date.strftime('%Y-%m-%d')}: {e}")
    
    def _fetch_orders_original(self, date_from: datetime, date_to: datetime) -> List[Dict[str, Any]]:
        """Original fetch orders method (renamed for use in new caching logic)"""
        all_orders = []
        
        # Generate weekly ranges
        current_date = date_from
        week_number = 1
        
        while current_date <= date_to:
            # Calculate week end (7 days from current date, but not beyond date_to)
            week_end = min(current_date + timedelta(days=6), date_to)
            
            print(f"  Week {week_number} ({current_date.strftime('%Y-%m-%d')} to {week_end.strftime('%Y-%m-%d')})...")
            
            try:
                week_orders = self.fetch_orders_for_period(current_date, week_end)
                if week_orders:
                    all_orders.extend(week_orders)
                    print(f"  Successfully fetched {len(week_orders)} orders for week {week_number}")
                else:
                    print(f"  No orders fetched for week {week_number}")
            except Exception as e:
                print(f"  Failed to fetch week {week_number}: {e}")
                # Try fetching in smaller chunks (3-day periods)
                print(f"  Trying to fetch week {week_number} in smaller chunks...")
                chunk_start = current_date
                while chunk_start <= week_end:
                    chunk_end = min(chunk_start + timedelta(days=2), week_end)
                    try:
                        print(f"    Fetching {chunk_start.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}...")
                        chunk_orders = self.fetch_orders_for_period(chunk_start, chunk_end)
                        if chunk_orders:
                            all_orders.extend(chunk_orders)
                            print(f"    Got {len(chunk_orders)} orders")
                    except Exception as e:
                        print(f"    Failed to fetch chunk: {e}")
                    chunk_start = chunk_end + timedelta(days=1)
            
            # Move to next week
            current_date = week_end + timedelta(days=1)
            week_number += 1
            
            # Wait 2 seconds between weekly requests to avoid overwhelming the API
            if current_date <= date_to:
                time.sleep(2)
        
        return all_orders
    
    def fetch_orders_for_period(self, date_from: datetime, date_to: datetime) -> List[Dict[str, Any]]:
        """
        Fetch orders for a specific date range (typically one week)

        Note: API filter requires partner token, so we fetch all orders and filter client-side
        """
        all_orders = []
        has_next_page = True
        cursor = None
        max_retries = 3
        retry_delay = 10
        page_delay = 0.5  # 500ms delay between pages
        consecutive_errors = 0

        logger.info(f"Fetching orders from API (will filter client-side for {date_from.strftime('%Y-%m-%d')} to {date_to.strftime('%Y-%m-%d')})")

        while has_next_page:
            # Remove filter parameter as it requires partner token
            variables = {
                'params': {
                    'limit': 30,
                    'order_by': 'pur_date',
                    'sort': 'ASC'
                }
            }

            if cursor is not None:
                variables['params']['cursor'] = cursor

            retry_count = 0
            success = False

            while retry_count < max_retries and not success:
                try:
                    result = self.client.execute(ORDER_QUERY, variable_values=variables)
                    orders_data = result.get('getOrderList', {})
                    orders = orders_data.get('data', [])
                    all_orders.extend(orders)

                    page_info = orders_data.get('pageInfo', {})
                    has_next_page = page_info.get('hasNextPage', False)
                    cursor = page_info.get('nextCursor')

                    print(f"Fetched {len(orders)} orders (total: {len(all_orders)})")
                    success = True
                    consecutive_errors = 0  # Reset error counter on success

                    # Delay between pages to avoid overwhelming the API
                    if has_next_page:
                        time.sleep(page_delay)

                except Exception as e:
                    retry_count += 1
                    consecutive_errors += 1

                    # Log the full error details
                    error_msg = str(e)
                    logger.debug(f"GraphQL error details: {error_msg}")

                    # Print full stack trace for debugging
                    if os.getenv('DEBUG'):
                        logger.debug("Full stack trace:")
                        logger.debug(traceback.format_exc())

                    if retry_count < max_retries:
                        logger.warning(f"Error fetching orders (attempt {retry_count}/{max_retries}): {error_msg[:200]}")
                        print(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                    else:
                        logger.error(f"Error fetching orders after {max_retries} attempts: {error_msg[:200]}")
                        logger.error(f"Full error: {error_msg}")
                        logger.error(f"Stack trace:\n{traceback.format_exc()}")

                        # If we've had too many consecutive errors and have some data, return what we have
                        if consecutive_errors >= 3 and all_orders:
                            logger.info(f"Returning {len(all_orders)} orders fetched so far due to persistent errors")
                            has_next_page = False
                        else:
                            # Otherwise just break this pagination loop
                            has_next_page = False
                        break

        logger.info(f"Fetched {len(all_orders)} total orders from API")

        # Filter by date range (client-side since API filter requires partner token)
        date_filtered_orders = []
        for order in all_orders:
            pur_date_str = order.get('pur_date', '')
            if pur_date_str:
                try:
                    # Parse date (format: YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
                    pur_date = datetime.strptime(pur_date_str.split()[0], '%Y-%m-%d')
                    # Check if order is within date range
                    if date_from <= pur_date <= date_to:
                        date_filtered_orders.append(order)
                except (ValueError, IndexError) as e:
                    logger.warning(f"Could not parse date '{pur_date_str}' for order {order.get('order_num', 'unknown')}: {e}")
                    continue

        logger.info(f"Filtered to {len(date_filtered_orders)} orders within date range")

        # Filter out orders with excluded statuses
        excluded_statuses = [
            'Storno',
            'Platba online - platnosť vypršala',
            'Platba online - platba zamietnutá',
            'Čaká na úhradu',
            'GoPay - platebni metoda potvrzena'
        ]

        filtered_orders = []
        excluded_counts = {}

        for order in date_filtered_orders:
            status = order.get('status', {}) or {}
            status_name = status.get('name', '')

            if status_name not in excluded_statuses:
                filtered_orders.append(order)
            else:
                excluded_counts[status_name] = excluded_counts.get(status_name, 0) + 1

        # Report excluded orders
        if excluded_counts:
            print("\nFiltered out orders:")
            for status, count in excluded_counts.items():
                print(f"  - {status}: {count} orders")

        logger.info(f"Final count after status filtering: {len(filtered_orders)} orders")

        return filtered_orders
    
    def flatten_order(self, order: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Flatten order data for CSV export - one row per order item"""
        flattened_rows = []
        
        # Extract common order data
        customer = order.get('customer', {}) or {}
        invoice_addr = order.get('invoice_address', {}) or {}
        delivery_addr = order.get('delivery_address', {}) or {}
        status = order.get('status', {}) or {}
        order_sum = order.get('sum', {}) or {}
        
        # Get order currency
        order_currency = order_sum.get('currency', {}).get('code') if order_sum.get('currency') else 'EUR'
        
        # Convert order total to EUR
        order_total_original = order_sum.get('value', 0) or 0
        order_total_eur = self.convert_to_eur(order_total_original, order_currency)
        
        # Customer info
        customer_name = customer.get('company_name', '')
        if not customer_name:
            customer_name = f"{customer.get('name', '')} {customer.get('surname', '')}".strip()
        
        # Base order data
        base_data = {
            'order_id': order.get('id'),
            'order_num': order.get('order_num'),
            'external_ref': order.get('external_ref'),
            'purchase_date': order.get('pur_date'),
            'var_symbol': order.get('var_symb'),
            'last_change': order.get('last_change'),
            'oss': order.get('oss'),
            'oss_country': order.get('oss_country'),
            
            # Status
            'status_id': status.get('id'),
            'status_name': status.get('name'),
            
            # Customer
            'customer_name': customer_name,
            'customer_company_id': customer.get('company_id'),
            'customer_vat_id': customer.get('vat_id'),
            'customer_email': customer.get('email'),
            'customer_phone': customer.get('phone'),
            
            # Invoice address
            'invoice_street': invoice_addr.get('street'),
            'invoice_descriptive_num': invoice_addr.get('descriptive_number'),
            'invoice_orientation_num': invoice_addr.get('orientation_number'),
            'invoice_city': invoice_addr.get('city'),
            'invoice_zip': invoice_addr.get('zip'),
            'invoice_country': invoice_addr.get('country'),
            
            # Delivery address
            'delivery_street': delivery_addr.get('street') if delivery_addr else None,
            'delivery_descriptive_num': delivery_addr.get('descriptive_number') if delivery_addr else None,
            'delivery_orientation_num': delivery_addr.get('orientation_number') if delivery_addr else None,
            'delivery_city': delivery_addr.get('city') if delivery_addr else None,
            'delivery_zip': delivery_addr.get('zip') if delivery_addr else None,
            'delivery_country': delivery_addr.get('country') if delivery_addr else None,
            
            # Order total
            'order_total_original': order_total_original,
            'order_total': order_total_eur,  # Converted to EUR
            'order_total_formatted': order_sum.get('formatted'),
            'order_currency': order_currency,
            
            # Packaging cost (fixed per order)
            'packaging_cost': PACKAGING_COST_PER_ORDER,
        }
        
        # Create a row for each item
        items = order.get('items', [])
        total_items = len(items)
        
        if items:
            for idx, item in enumerate(items, 1):
                item_price = item.get('price', {}) or {}
                weight = item.get('weight', {}) or {}
                recycle_fee = item.get('recycle_fee', {}) or {}
                
                # Get item currency (use order currency if not specified)
                item_currency = item_price.get('currency', {}).get('code') if item_price.get('currency') else order_currency
                
                # Calculate prices with and without tax
                item_price_value_original = item_price.get('value', 0) or 0
                # Convert to EUR
                item_price_value = self.convert_to_eur(item_price_value_original, item_currency)
                item_quantity = item.get('quantity', 1) or 1
                item_tax_rate = item.get('tax_rate', 0) or 0
                
                # Calculate total price for this item (price * quantity) in EUR
                item_total_with_tax = item_price_value * item_quantity
                
                # Calculate price without tax
                # Price without tax = Price with tax / (1 + tax_rate/100)
                if item_tax_rate > 0:
                    item_total_without_tax = item_total_with_tax / (1 + item_tax_rate / 100)
                    item_tax_amount = item_total_with_tax - item_total_without_tax
                else:
                    item_total_without_tax = item_total_with_tax
                    item_tax_amount = 0
                
                # Get expense per item from mapping (using product_sku - EAN or hash)
                item_label = item.get('item_label', '')
                item_ean = item.get('ean', '')
                product_sku = self.get_product_sku(item_ean, item_label)
                # First try SKU, then title for backward compatibility, default to 1.0 for unknown products
                expense_per_item = PRODUCT_EXPENSES.get(product_sku, PRODUCT_EXPENSES.get(item_label, 1.0))
                total_expense = expense_per_item * item_quantity
                
                # Calculate profit and ROI (Note: FB ads will be added at aggregation level)
                # At item level, we only have product expense
                item_profit_before_ads = item_total_without_tax - total_expense
                item_roi_before_ads = (item_profit_before_ads / total_expense * 100) if total_expense > 0 else 0
                
                row = base_data.copy()
                row.update({
                    'total_items_in_order': total_items,
                    'item_number': idx,
                    'item_label': item.get('item_label'),
                    'item_ean': item.get('ean'),
                    'item_import_code': item.get('import_code'),
                    'item_warehouse_number': item.get('warehouse_number'),
                    'item_quantity': item_quantity,
                    'item_tax_rate': item_tax_rate,
                    'item_weight': weight.get('value'),
                    'item_weight_unit': weight.get('unit'),
                    'item_currency': item_currency,
                    'item_unit_price_original': item_price_value_original,
                    'item_unit_price': item_price_value,  # In EUR
                    'item_total_with_tax': round(item_total_with_tax, 2),  # In EUR
                    'item_total_without_tax': round(item_total_without_tax, 2),  # In EUR
                    'item_tax_amount': round(item_tax_amount, 2),  # In EUR
                    'item_recycle_fee': recycle_fee.get('value'),
                    'expense_per_item': expense_per_item,
                    'total_expense': round(total_expense, 2),
                    'profit_before_ads': round(item_profit_before_ads, 2),
                    'roi_before_ads': round(item_roi_before_ads, 2),
                })
                flattened_rows.append(row)
        else:
            # If no items, create one row with order data only
            base_data['total_items_in_order'] = 0
            base_data['item_number'] = None
            flattened_rows.append(base_data)
        
        return flattened_rows
    
    def cleanup_data_folder(self):
        """Clean up old data files before starting new export"""
        data_dir = Path('data')
        if data_dir.exists():
            # Remove all CSV and HTML files
            for pattern in ['*.csv', '*.html']:
                for file in data_dir.glob(pattern):
                    try:
                        file.unlink()
                        print(f"Removed old file: {file.name}")
                    except Exception as e:
                        print(f"Warning: Could not remove {file.name}: {e}")
        else:
            # Create data directory if it doesn't exist
            data_dir.mkdir(exist_ok=True)
    
    def export_to_csv(self, orders: List[Dict[str, Any]], date_from: datetime, date_to: datetime) -> str:
        """Export orders to CSV file"""
        # Clean up old data files first
        print("Cleaning up old data files...")
        self.cleanup_data_folder()

        # Safety dedup for long historical runs / cursor overlap edge cases.
        orders = self.deduplicate_orders(orders)
        
        # Fetch Facebook Ads spend data
        fb_daily_spend = {}
        fb_detailed_metrics = {}
        fb_campaigns = []
        fb_hourly_stats = []
        fb_dow_stats = []
        if self.fb_client.is_configured:
            print("Fetching Facebook Ads spend data...")
            fb_daily_spend = self.fb_client.get_daily_spend(date_from, date_to)
            if fb_daily_spend:
                print(f"Retrieved Facebook Ads data for {len(fb_daily_spend)} days")

            # Fetch detailed metrics for Facebook Ads report
            print("Fetching detailed Facebook Ads metrics...")
            fb_detailed_metrics = self.fb_client.get_daily_metrics(date_from, date_to)
            if fb_detailed_metrics:
                print(f"Retrieved detailed FB metrics for {len(fb_detailed_metrics)} days")

            # Fetch campaign-level performance
            print("Fetching Facebook campaign performance...")
            fb_campaigns = self.fb_client.get_campaign_spend(date_from, date_to)
            if fb_campaigns:
                print(f"Retrieved data for {len(fb_campaigns)} campaigns")

            # Fetch hourly stats
            print("Fetching Facebook hourly stats...")
            fb_hourly_stats = self.fb_client.get_hourly_stats(date_from, date_to)
            if fb_hourly_stats:
                print(f"Retrieved hourly stats for {len(fb_hourly_stats)} hours")

            # Fetch day of week stats
            print("Fetching Facebook day-of-week stats...")
            fb_dow_stats = self.fb_client.get_day_of_week_stats(date_from, date_to)
            if fb_dow_stats:
                print(f"Retrieved day-of-week stats for {len(fb_dow_stats)} days")
        
        # Fetch Google Ads spend data
        google_ads_daily_spend = {}
        if self.google_ads_client.is_configured:
            print("Fetching Google Ads spend data...")
            google_ads_daily_spend = self.google_ads_client.get_daily_spend(date_from, date_to)
            if google_ads_daily_spend:
                print(f"Retrieved Google Ads data for {len(google_ads_daily_spend)} days")
        
        # Flatten all orders
        all_rows = []
        for order in orders:
            all_rows.extend(self.flatten_order(order))
        
        # Create filename
        filename = f"data/export_{date_from.strftime('%Y%m%d')}-{date_to.strftime('%Y%m%d')}.csv"
        
        # Convert to DataFrame for easier CSV export
        df = pd.DataFrame(all_rows)

        # Safety dedup on flattened rows to avoid duplicate item rows in revenue/cost analytics.
        # Uses order_num + item_* shape as requested.
        dedup_cols = [
            'order_num',
            'item_number',
            'item_label',
            'item_ean',
            'item_quantity',
            'item_tax_rate',
            'item_total_without_tax',
            'item_total_with_tax'
        ]
        dedup_cols = [col for col in dedup_cols if col in df.columns]
        if dedup_cols:
            before_rows = len(df)
            df = df.drop_duplicates(subset=dedup_cols).reset_index(drop=True)
            removed_rows = before_rows - len(df)
            if removed_rows > 0:
                logger.warning(f"Removed {removed_rows} duplicated flattened rows before analytics")
                print(f"Deduplication: removed {removed_rows} duplicated item rows")

        # Add consistent product SKU column (EAN if available, otherwise hash of title)
        df = self.add_product_sku_column(df)
        # Add canonical order-level net revenue (unified revenue definition for report analytics)
        df = self.add_order_revenue_net_column(df)

        # Add Facebook Ads spend column
        if fb_daily_spend:
            # Convert purchase_date to date format for matching
            df['purchase_date_only'] = pd.to_datetime(df['purchase_date']).dt.strftime('%Y-%m-%d')
            df['fb_ads_daily_spend'] = df['purchase_date_only'].map(fb_daily_spend).fillna(0)
        else:
            df['fb_ads_daily_spend'] = 0
        
        # Add Google Ads spend column
        if google_ads_daily_spend:
            # Ensure purchase_date_only exists
            if 'purchase_date_only' not in df.columns:
                df['purchase_date_only'] = pd.to_datetime(df['purchase_date']).dt.strftime('%Y-%m-%d')
            df['google_ads_daily_spend'] = df['purchase_date_only'].map(google_ads_daily_spend).fillna(0)
        else:
            df['google_ads_daily_spend'] = 0
        
        # Reorder columns for better readability
        column_order = [
            'order_num', 'order_id', 'external_ref', 'purchase_date', 'status_name',
            'total_items_in_order', 'item_number',
            'product_sku', 'item_label', 'item_ean', 'item_quantity', 
            'item_currency', 'item_unit_price_original', 'item_unit_price',
            'item_total_without_tax', 'item_tax_rate', 'item_tax_amount', 'item_total_with_tax',
            'expense_per_item', 'total_expense', 'fb_ads_daily_spend', 'google_ads_daily_spend', 'profit_before_ads', 'roi_before_ads',
            'customer_name', 'customer_email', 'customer_company_id', 'customer_vat_id',
            'order_currency', 'order_total_original', 'order_total', 'order_revenue_net',
            'invoice_street', 'invoice_city', 'invoice_zip', 'invoice_country',
            'delivery_street', 'delivery_city', 'delivery_zip', 'delivery_country'
        ]
        
        # Only include columns that exist
        column_order = [col for col in column_order if col in df.columns]
        # Add any remaining columns
        remaining_cols = [col for col in df.columns if col not in column_order]
        column_order.extend(remaining_cols)
        
        df = df[column_order]
        
        # Save to CSV
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        
        # Analyze returning customers
        returning_customers_analysis = self.analyze_returning_customers(df)
        
        # Calculate CLV and return time analysis
        clv_return_time_analysis = self.calculate_clv_and_return_time(df)

        # Analyze order size distribution
        order_size_distribution = self.analyze_order_size_distribution(df)

        # Analyze item combinations
        item_combinations = self.analyze_item_combinations(df, min_count=5)

        # New analytics
        day_of_week_analysis = self.analyze_day_of_week(df)
        day_hour_heatmap = self.analyze_day_hour_heatmap(df)
        country_analysis, city_analysis = self.analyze_geographic(df)
        geo_profitability = self.analyze_geo_profitability(df, fb_campaigns)
        b2b_analysis = self.analyze_b2b_vs_b2c(df)
        product_margins = self.analyze_product_margins(df)
        product_trends = self.analyze_product_trends(df)
        customer_concentration = self.analyze_customer_concentration(df)
        order_status = self.analyze_order_status(df)
        ads_effectiveness = self.analyze_ads_effectiveness(df)
        new_vs_returning_revenue = self.analyze_new_vs_returning_revenue(df)
        refunds_analysis = self.analyze_refunds(df)

        # Repeat purchase cohort analysis
        cohort_analysis = self.analyze_repeat_purchase_cohorts(df)

        # Item-based retention analyses
        first_item_retention = self.analyze_retention_by_first_order_item(df)
        same_item_repurchase = self.analyze_same_item_repurchase(df)
        time_to_nth_by_first_item = self.analyze_time_to_nth_by_first_item(df)

        # Customer email segmentation analysis
        # Combine filtered orders with excluded (failed payment) orders for complete customer view
        all_orders_for_segmentation = orders + self.excluded_orders
        customer_email_segments = self.analyze_customer_email_segments(df, all_orders_for_segmentation)

        # Create aggregated reports
        date_product_agg, date_agg, items_agg, month_agg, ltv_by_date = self.create_aggregated_reports(df, date_from, date_to, fb_daily_spend, google_ads_daily_spend)

        # Calculate financial metrics
        financial_metrics = self.calculate_financial_metrics(df, date_agg, clv_return_time_analysis)
        consistency_checks = self.validate_metric_consistency(date_agg, financial_metrics, clv_return_time_analysis)

        # Cost Per Order analysis with campaign attribution
        # Use the same revenue source as financial summary to keep ROAS definitions aligned.
        cost_per_order = self.analyze_cost_per_order(
            df,
            fb_campaigns,
            reference_total_revenue=financial_metrics.get('total_revenue')
        )

        # Display aggregated data
        self.display_aggregated_data(date_product_agg, date_agg, month_agg)

        # Display returning customer analysis
        self.display_returning_customers_analysis(returning_customers_analysis)

        # Display CLV and return time analysis
        self.display_clv_return_time_analysis(clv_return_time_analysis)

        # Generate HTML report
        print("Generating HTML report...")
        html_content = generate_html_report(
            date_agg, date_product_agg, items_agg,
            date_from, date_to, fb_daily_spend, google_ads_daily_spend,
            returning_customers_analysis, clv_return_time_analysis,
            order_size_distribution, item_combinations,
            day_of_week_analysis=day_of_week_analysis,
            day_hour_heatmap=day_hour_heatmap,
            country_analysis=country_analysis,
            city_analysis=city_analysis,
            geo_profitability=geo_profitability,
            b2b_analysis=b2b_analysis,
            product_margins=product_margins,
            product_trends=product_trends,
            customer_concentration=customer_concentration,
            financial_metrics=financial_metrics,
            order_status=order_status,
            ads_effectiveness=ads_effectiveness,
            new_vs_returning_revenue=new_vs_returning_revenue,
            refunds_analysis=refunds_analysis,
            customer_email_segments=customer_email_segments,
            cohort_analysis=cohort_analysis,
            first_item_retention=first_item_retention,
            same_item_repurchase=same_item_repurchase,
            time_to_nth_by_first_item=time_to_nth_by_first_item,
            fb_detailed_metrics=fb_detailed_metrics,
            fb_campaigns=fb_campaigns,
            cost_per_order=cost_per_order,
            fb_hourly_stats=fb_hourly_stats,
            fb_dow_stats=fb_dow_stats,
            ltv_by_date=ltv_by_date,
            consistency_checks=consistency_checks
        )
        html_filename = f"data/report_{date_from.strftime('%Y%m%d')}-{date_to.strftime('%Y%m%d')}.html"
        # Write with UTF-8 BOM to avoid mojibake when a server/browser mis-detects charset
        with open(html_filename, 'w', encoding='utf-8-sig') as f:
            f.write(html_content)
        print(f"HTML report saved: {html_filename}")

        # Generate Email Strategy Report
        if customer_email_segments and cohort_analysis:
            print("Generating Email Strategy Report...")
            email_strategy_html = generate_email_strategy_report(
                customer_email_segments, cohort_analysis, date_from, date_to
            )
            email_strategy_filename = f"data/email_strategy_{date_from.strftime('%Y%m%d')}-{date_to.strftime('%Y%m%d')}.html"
            # Keep the same robust encoding strategy for secondary HTML report
            with open(email_strategy_filename, 'w', encoding='utf-8-sig') as f:
                f.write(email_strategy_html)
            print(f"Email Strategy Report saved: {email_strategy_filename}")

        return filename
    
    def create_aggregated_reports(self, df: pd.DataFrame, date_from: datetime, date_to: datetime, fb_daily_spend: Dict[str, float] = None, google_ads_daily_spend: Dict[str, float] = None):
        """Create aggregated CSV reports"""
        # Convert purchase_date to datetime and extract date only
        if 'purchase_date_only' not in df.columns:
            df['purchase_date_only'] = pd.to_datetime(df['purchase_date']).dt.date
        else:
            df['purchase_date_only'] = pd.to_datetime(df['purchase_date_only']).dt.date
        
        # 1. Group by date and product (using product_sku for consistent grouping)
        print("Creating date-product aggregation...")
        date_product_agg = df.groupby(['purchase_date_only', 'product_sku']).agg({
            'item_label': 'first',  # Keep product name for display
            'item_quantity': 'sum',
            'item_total_without_tax': 'sum',
            'total_expense': 'sum',
            'profit_before_ads': 'sum',
            'order_num': 'count'
        }).reset_index()

        date_product_agg.columns = ['date', 'product_sku', 'product_name', 'total_quantity', 'total_revenue', 'product_expense', 'profit', 'order_count']
        
        # Calculate ROI based on product expense only (no FB ads)
        date_product_agg['roi_percent'] = date_product_agg.apply(
            lambda row: round((row['profit'] / row['product_expense'] * 100) if row['product_expense'] > 0 else 0, 2),
            axis=1
        )
        
        # Round financial values
        date_product_agg['total_revenue'] = date_product_agg['total_revenue'].round(2)
        date_product_agg['product_expense'] = date_product_agg['product_expense'].round(2)
        date_product_agg['profit'] = date_product_agg['profit'].round(2)
        
        # Sort by date and product SKU
        date_product_agg = date_product_agg.sort_values(['date', 'product_sku'])
        
        # Save date-product aggregation
        date_product_filename = f"data/aggregate_by_date_product_{date_from.strftime('%Y%m%d')}-{date_to.strftime('%Y%m%d')}.csv"
        date_product_agg.to_csv(date_product_filename, index=False, encoding='utf-8-sig')
        print(f"Date-product aggregation saved: {date_product_filename}")
        
        # 2. Group by date only
        print("Creating date-only aggregation...")
        date_agg = df.groupby('purchase_date_only').agg({
            'item_quantity': 'sum',
            'item_total_without_tax': 'sum',
            'total_expense': 'sum',
            'profit_before_ads': 'sum',
            'fb_ads_daily_spend': 'first' if 'fb_ads_daily_spend' in df.columns else lambda x: 0,
            'google_ads_daily_spend': 'first' if 'google_ads_daily_spend' in df.columns else lambda x: 0,
            'order_num': 'nunique',  # Count unique orders
            'item_label': 'count'     # Count total items
        }).reset_index()
        
        date_agg.columns = ['date', 'total_quantity', 'total_revenue', 'product_expense', 'profit_before_ads', 'fb_ads_spend', 'google_ads_spend', 'unique_orders', 'total_items']

        # Fill in missing dates with zero values for orders but preserve ad spend data
        # Create a complete date range
        complete_date_range = pd.date_range(start=date_from, end=date_to, freq='D').date
        complete_date_df = pd.DataFrame({'date': complete_date_range})

        # Merge with existing data to include all dates
        date_agg = complete_date_df.merge(date_agg, on='date', how='left')

        # Fill missing order-related values with 0
        date_agg['total_quantity'] = date_agg['total_quantity'].fillna(0).astype(int)
        date_agg['total_revenue'] = date_agg['total_revenue'].fillna(0)
        date_agg['product_expense'] = date_agg['product_expense'].fillna(0)
        date_agg['profit_before_ads'] = date_agg['profit_before_ads'].fillna(0)
        date_agg['unique_orders'] = date_agg['unique_orders'].fillna(0).astype(int)
        date_agg['total_items'] = date_agg['total_items'].fillna(0).astype(int)

        # Fill ad spend from dictionaries for dates with no orders
        # First fill NaN values with values from dictionaries, then fill any remaining with 0
        if fb_daily_spend:
            # Create a mapping function for date to string
            date_agg['date_str'] = date_agg['date'].apply(lambda d: d.strftime('%Y-%m-%d'))
            # Fill NaN fb_ads_spend values with values from dictionary
            date_agg['fb_ads_spend'] = date_agg.apply(
                lambda row: fb_daily_spend.get(row['date_str'], 0) if pd.isna(row['fb_ads_spend']) else row['fb_ads_spend'],
                axis=1
            )
            date_agg = date_agg.drop('date_str', axis=1)
        date_agg['fb_ads_spend'] = date_agg['fb_ads_spend'].fillna(0)

        if google_ads_daily_spend:
            # Create a mapping function for date to string
            date_agg['date_str'] = date_agg['date'].apply(lambda d: d.strftime('%Y-%m-%d'))
            # Fill NaN google_ads_spend values with values from dictionary
            date_agg['google_ads_spend'] = date_agg.apply(
                lambda row: google_ads_daily_spend.get(row['date_str'], 0) if pd.isna(row['google_ads_spend']) else row['google_ads_spend'],
                axis=1
            )
            date_agg = date_agg.drop('date_str', axis=1)
        date_agg['google_ads_spend'] = date_agg['google_ads_spend'].fillna(0)

        # Add variable per-order logistics costs
        # Packaging + shipping subsidy both scale with number of orders.
        date_agg['packaging_cost'] = date_agg['unique_orders'] * PACKAGING_COST_PER_ORDER
        date_agg['shipping_subsidy_cost'] = date_agg['unique_orders'] * SHIPPING_SUBSIDY_PER_ORDER

        # Add daily fixed cost based on the date
        date_agg['fixed_daily_cost'] = date_agg['date'].apply(lambda d: round(self.get_daily_fixed_cost(pd.Timestamp(d)), 2))

        # Company-level cost (includes fixed overhead)
        # Total cost = product expense + ads + packaging + shipping subsidy + fixed daily cost
        date_agg['total_cost'] = (
            date_agg['product_expense']
            + date_agg['fb_ads_spend']
            + date_agg['google_ads_spend']
            + date_agg['packaging_cost']
            + date_agg['shipping_subsidy_cost']
            + date_agg['fixed_daily_cost']
        )

        # Company net profit: Revenue - All costs (including fixed overhead)
        date_agg['net_profit'] = date_agg['total_revenue'] - date_agg['total_cost']

        # Pre-ad contribution view (CM1): excludes fixed overhead and ad spend
        date_agg['pre_ad_contribution_cost'] = (
            date_agg['product_expense']
            + date_agg['packaging_cost']
            + date_agg['shipping_subsidy_cost']
        )
        date_agg['pre_ad_contribution_profit'] = date_agg['total_revenue'] - date_agg['pre_ad_contribution_cost']
        date_agg['pre_ad_contribution_margin_pct'] = date_agg.apply(
            lambda row: round((row['pre_ad_contribution_profit'] / row['total_revenue'] * 100) if row['total_revenue'] > 0 else 0, 2),
            axis=1
        )
        date_agg['pre_ad_contribution_profit_per_order'] = date_agg.apply(
            lambda row: round((row['pre_ad_contribution_profit'] / row['unique_orders']) if row['unique_orders'] > 0 else 0, 2),
            axis=1
        )

        # Post-ad contribution view (CM2): excludes fixed overhead, includes ad spend
        date_agg['contribution_cost'] = (
            date_agg['product_expense']
            + date_agg['packaging_cost']
            + date_agg['shipping_subsidy_cost']
            + date_agg['fb_ads_spend']
            + date_agg['google_ads_spend']
        )
        date_agg['contribution_profit'] = date_agg['total_revenue'] - date_agg['contribution_cost']
        date_agg['contribution_margin_pct'] = date_agg.apply(
            lambda row: round((row['contribution_profit'] / row['total_revenue'] * 100) if row['total_revenue'] > 0 else 0, 2),
            axis=1
        )
        date_agg['contribution_profit_per_order'] = date_agg.apply(
            lambda row: round((row['contribution_profit'] / row['unique_orders']) if row['unique_orders'] > 0 else 0, 2),
            axis=1
        )
        # Explicit post-ad aliases (terminology clarity)
        date_agg['post_ad_contribution_cost'] = date_agg['contribution_cost']
        date_agg['post_ad_contribution_profit'] = date_agg['contribution_profit']
        date_agg['post_ad_contribution_margin_pct'] = date_agg['contribution_margin_pct']
        date_agg['post_ad_contribution_profit_per_order'] = date_agg['contribution_profit_per_order']

        # Calculate ROI: (Profit / Total Cost) * 100
        date_agg['roi_percent'] = date_agg.apply(
            lambda row: round((row['net_profit'] / row['total_cost'] * 100) if row['total_cost'] > 0 else 0, 2),
            axis=1
        )

        # Round financial values
        date_agg['total_revenue'] = date_agg['total_revenue'].round(2)
        date_agg['product_expense'] = date_agg['product_expense'].round(2)
        date_agg['fb_ads_spend'] = date_agg['fb_ads_spend'].round(2)
        date_agg['google_ads_spend'] = date_agg['google_ads_spend'].round(2)
        date_agg['packaging_cost'] = date_agg['packaging_cost'].round(2)
        date_agg['shipping_subsidy_cost'] = date_agg['shipping_subsidy_cost'].round(2)
        date_agg['total_cost'] = date_agg['total_cost'].round(2)
        date_agg['net_profit'] = date_agg['net_profit'].round(2)
        date_agg['pre_ad_contribution_cost'] = date_agg['pre_ad_contribution_cost'].round(2)
        date_agg['pre_ad_contribution_profit'] = date_agg['pre_ad_contribution_profit'].round(2)
        date_agg['pre_ad_contribution_margin_pct'] = date_agg['pre_ad_contribution_margin_pct'].round(2)
        date_agg['pre_ad_contribution_profit_per_order'] = date_agg['pre_ad_contribution_profit_per_order'].round(2)
        date_agg['contribution_cost'] = date_agg['contribution_cost'].round(2)
        date_agg['contribution_profit'] = date_agg['contribution_profit'].round(2)
        date_agg['contribution_margin_pct'] = date_agg['contribution_margin_pct'].round(2)
        date_agg['contribution_profit_per_order'] = date_agg['contribution_profit_per_order'].round(2)

        # Sort by date
        date_agg = date_agg.sort_values('date')
        
        # Save date aggregation
        date_filename = f"data/aggregate_by_date_{date_from.strftime('%Y%m%d')}-{date_to.strftime('%Y%m%d')}.csv"
        date_agg.to_csv(date_filename, index=False, encoding='utf-8-sig')
        print(f"Date aggregation saved: {date_filename}")
        
        # 2b. Create monthly aggregation
        print("Creating monthly aggregation...")
        
        # Add month column to date_agg for grouping
        date_agg_copy = date_agg.copy()
        date_agg_copy['month'] = pd.to_datetime(date_agg_copy['date']).dt.to_period('M')
        
        # Group by month
        month_agg = date_agg_copy.groupby('month').agg({
            'unique_orders': 'sum',
            'total_items': 'sum',
            'total_quantity': 'sum',
            'total_revenue': 'sum',
            'product_expense': 'sum',
            'packaging_cost': 'sum',
            'shipping_subsidy_cost': 'sum',
            'fixed_daily_cost': 'sum',
            'fb_ads_spend': 'sum',
            'google_ads_spend': 'sum',
            'total_cost': 'sum',
            'net_profit': 'sum',
            'pre_ad_contribution_cost': 'sum',
            'pre_ad_contribution_profit': 'sum',
            'contribution_cost': 'sum',
            'contribution_profit': 'sum'
        }).reset_index()
        
        # Calculate ROI for each month
        month_agg['roi_percent'] = month_agg.apply(
            lambda row: round((row['net_profit'] / row['total_cost'] * 100) if row['total_cost'] > 0 else 0, 2),
            axis=1
        )
        month_agg['contribution_margin_pct'] = month_agg.apply(
            lambda row: round((row['contribution_profit'] / row['total_revenue'] * 100) if row['total_revenue'] > 0 else 0, 2),
            axis=1
        )
        month_agg['pre_ad_contribution_margin_pct'] = month_agg.apply(
            lambda row: round((row['pre_ad_contribution_profit'] / row['total_revenue'] * 100) if row['total_revenue'] > 0 else 0, 2),
            axis=1
        )
        month_agg['contribution_profit_per_order'] = month_agg.apply(
            lambda row: round((row['contribution_profit'] / row['unique_orders']) if row['unique_orders'] > 0 else 0, 2),
            axis=1
        )
        month_agg['pre_ad_contribution_profit_per_order'] = month_agg.apply(
            lambda row: round((row['pre_ad_contribution_profit'] / row['unique_orders']) if row['unique_orders'] > 0 else 0, 2),
            axis=1
        )
        # Explicit post-ad aliases (terminology clarity)
        month_agg['post_ad_contribution_margin_pct'] = month_agg['contribution_margin_pct']
        month_agg['post_ad_contribution_profit_per_order'] = month_agg['contribution_profit_per_order']
        
        # Convert month period to string for display
        month_agg['month'] = month_agg['month'].astype(str)
        
        # Save monthly aggregation
        month_filename = f"data/aggregate_by_month_{date_from.strftime('%Y%m%d')}-{date_to.strftime('%Y%m%d')}.csv"
        month_agg.to_csv(month_filename, index=False, encoding='utf-8-sig')
        print(f"Monthly aggregation saved: {month_filename}")
        
        # 3. Group by items only (across all dates) - use product_sku for consistent grouping
        print("Creating items aggregation...")

        items_agg = df.groupby('product_sku').agg({
            'item_label': 'first',  # Keep product name for display
            'item_quantity': 'sum',
            'item_total_without_tax': 'sum',
            'total_expense': 'sum',
            'profit_before_ads': 'sum',
            'order_num': 'nunique'  # Count unique orders
        }).reset_index()

        items_agg.columns = ['product_sku', 'product_name', 'total_quantity', 'total_revenue', 'product_expense', 'profit', 'order_count']
        
        # Calculate ROI based on product expense only (no FB ads)
        items_agg['roi_percent'] = items_agg.apply(
            lambda row: round((row['profit'] / row['product_expense'] * 100) if row['product_expense'] > 0 else 0, 2),
            axis=1
        )
        
        # Round financial values
        items_agg['total_revenue'] = items_agg['total_revenue'].round(2)
        items_agg['product_expense'] = items_agg['product_expense'].round(2)
        items_agg['profit'] = items_agg['profit'].round(2)
        
        # Sort by total revenue descending
        items_agg = items_agg.sort_values('total_revenue', ascending=False)
        
        # Save items aggregation
        items_filename = f"data/aggregate_by_items_{date_from.strftime('%Y%m%d')}-{date_to.strftime('%Y%m%d')}.csv"
        items_agg.to_csv(items_filename, index=False, encoding='utf-8-sig')
        print(f"Items aggregation saved: {items_filename}")

        # 4. Calculate Customer Lifetime Revenue by Acquisition Date
        print("Calculating customer lifetime revenue by acquisition date...")

        # Group by customer to get their first purchase date and total lifetime revenue
        customer_lifetime = df.groupby('customer_email').agg({
            'purchase_date_only': 'min',  # First purchase date
            'item_total_without_tax': 'sum',  # Total lifetime revenue
            'order_num': 'nunique'  # Total number of orders
        }).reset_index()

        customer_lifetime.columns = ['customer_email', 'first_purchase_date', 'lifetime_revenue', 'total_orders']

        # Now aggregate by first purchase date to get total lifetime revenue attributed to each acquisition date
        ltv_by_date = customer_lifetime.groupby('first_purchase_date').agg({
            'lifetime_revenue': 'sum',
            'customer_email': 'count',  # Count of customers acquired
            'total_orders': 'sum'  # Total orders these customers made over their lifetime
        }).reset_index()

        ltv_by_date.columns = ['date', 'ltv_revenue', 'customers_acquired', 'total_lifetime_orders']

        # Fill in missing dates with zeros
        ltv_complete_date_df = pd.DataFrame({'date': complete_date_range})
        ltv_by_date = ltv_complete_date_df.merge(ltv_by_date, on='date', how='left')
        ltv_by_date['ltv_revenue'] = ltv_by_date['ltv_revenue'].fillna(0).round(2)
        ltv_by_date['customers_acquired'] = ltv_by_date['customers_acquired'].fillna(0).astype(int)
        ltv_by_date['total_lifetime_orders'] = ltv_by_date['total_lifetime_orders'].fillna(0).astype(int)

        # Sort by date
        ltv_by_date = ltv_by_date.sort_values('date')

        # Save LTV by acquisition date
        ltv_filename = f"data/ltv_by_acquisition_date_{date_from.strftime('%Y%m%d')}-{date_to.strftime('%Y%m%d')}.csv"
        ltv_by_date.to_csv(ltv_filename, index=False, encoding='utf-8-sig')
        print(f"LTV by acquisition date saved: {ltv_filename}")

        # Return aggregated data for display
        return date_product_agg, date_agg, items_agg, month_agg, ltv_by_date
    
    def display_aggregated_data(self, date_product_agg: pd.DataFrame, date_agg: pd.DataFrame, month_agg: pd.DataFrame = None):
        """Display aggregated data with nice formatting"""
        
        # If we have monthly data, display each month separately first
        if month_agg is not None and not month_agg.empty:
            # Convert date column to datetime for grouping
            date_agg_copy = date_agg.copy()
            date_agg_copy['month'] = pd.to_datetime(date_agg_copy['date']).dt.to_period('M')
            
            # Display each month's daily data separately
            for month_period in date_agg_copy['month'].unique():
                month_str = str(month_period)
                month_data = date_agg_copy[date_agg_copy['month'] == month_period]
                
                print("\n" + "="*220)
                print(f"DAILY SUMMARY FOR {month_str.upper()}")
                print("Fixed Costs = Packaging + Shipping Subsidy + Fixed Daily Cost | AOV = Avg Order Value | FB/Order = Avg FB Cost per Order")
                print("="*220)
                
                print(f"\n{'Date':<12} {'Orders':>8} {'Items':>8} {'Revenue (€)':>12} {'AOV (€)':>8} {'Product (€)':>12} {'Fixed Costs (€)':>14} {'FB Ads (€)':>12} {'Google Ads (€)':>14} {'Total Cost (€)':>14} {'Profit (€)':>12} {'ROI %':>8}")
                print("-"*240)
                
                month_orders = 0
                month_items = 0
                month_revenue = 0
                month_product_expense = 0
                month_packaging = 0
                month_shipping = 0
                month_fixed = 0
                month_fb_ads = 0
                month_google_ads = 0
                month_net_profit = 0
                
                for _, row in month_data.iterrows():
                    date_str = str(row['date'])
                    fixed_costs = row['packaging_cost'] + row.get('shipping_subsidy_cost', 0) + row['fixed_daily_cost']
                    aov = row['total_revenue'] / row['unique_orders'] if row['unique_orders'] > 0 else 0
                    fb_per_order = row['fb_ads_spend'] / row['unique_orders'] if row['unique_orders'] > 0 else 0
                    google_ads = row.get('google_ads_spend', 0)
                    print(f"{date_str:<12} {row['unique_orders']:>8} {row['total_items']:>8} "
                          f"{row['total_revenue']:>12.2f} {aov:>8.2f} {row['product_expense']:>12.2f} "
                          f"{fixed_costs:>14.2f} "
                          f"{row['fb_ads_spend']:>12.2f} {google_ads:>14.2f} {row['total_cost']:>14.2f} {row['net_profit']:>12.2f} {row['roi_percent']:>8.2f}")
                    month_orders += row['unique_orders']
                    month_items += row['total_items']
                    month_revenue += row['total_revenue']
                    month_product_expense += row['product_expense']
                    month_packaging += row['packaging_cost']
                    month_shipping += row.get('shipping_subsidy_cost', 0)
                    month_fixed += row['fixed_daily_cost']
                    month_fb_ads += row['fb_ads_spend']
                    month_google_ads += google_ads
                    month_net_profit += row['net_profit']

                # Monthly total
                month_fixed_costs = month_packaging + month_shipping + month_fixed
                month_cost = month_product_expense + month_packaging + month_shipping + month_fixed + month_fb_ads + month_google_ads
                month_roi = (month_net_profit / month_cost * 100) if month_cost > 0 else 0
                month_aov = month_revenue / month_orders if month_orders > 0 else 0
                month_fb_per_order = month_fb_ads / month_orders if month_orders > 0 else 0
                
                print("-"*240)
                print(f"{'MONTH TOTAL':<12} {month_orders:>8} {month_items:>8} "
                      f"{month_revenue:>12.2f} {month_aov:>8.2f} {month_product_expense:>12.2f} "
                      f"{month_fixed_costs:>14.2f} "
                      f"{month_fb_ads:>12.2f} {month_google_ads:>14.2f} {month_cost:>14.2f} {month_net_profit:>12.2f} {month_roi:>8.2f}")
        
        # Display monthly summary if available
        if month_agg is not None and not month_agg.empty:
            print("\n" + "="*220)
            print("MONTHLY SUMMARY")
            print("="*220)
            print(f"\n{'Month':<12} {'Orders':>8} {'Items':>8} {'Revenue (€)':>12} {'AOV (€)':>8} {'Product (€)':>12} {'Fixed Costs (€)':>14} {'FB Ads (€)':>12} {'Google Ads (€)':>14} {'Total Cost (€)':>14} {'Profit (€)':>12} {'ROI %':>8}")
            print("-"*240)
            
            month_total_orders = 0
            month_total_items = 0
            month_total_revenue = 0
            month_total_product_expense = 0
            month_total_packaging = 0
            month_total_shipping = 0
            month_total_fixed = 0
            month_total_fb_ads = 0
            month_total_google_ads = 0
            month_total_net_profit = 0
            
            for _, row in month_agg.iterrows():
                month_str = str(row['month'])
                fixed_costs = row['packaging_cost'] + row.get('shipping_subsidy_cost', 0) + row['fixed_daily_cost']
                aov = row['total_revenue'] / row['unique_orders'] if row['unique_orders'] > 0 else 0
                fb_per_order = row['fb_ads_spend'] / row['unique_orders'] if row['unique_orders'] > 0 else 0
                google_ads = row.get('google_ads_spend', 0)
                print(f"{month_str:<12} {row['unique_orders']:>8} {row['total_items']:>8} "
                      f"{row['total_revenue']:>12.2f} {aov:>8.2f} {row['product_expense']:>12.2f} "
                      f"{fixed_costs:>14.2f} "
                      f"{row['fb_ads_spend']:>12.2f} {google_ads:>14.2f} {row['total_cost']:>14.2f} "
                      f"{row['net_profit']:>12.2f} {row['roi_percent']:>8.2f}")
                month_total_orders += row['unique_orders']
                month_total_items += row['total_items']
                month_total_revenue += row['total_revenue']
                month_total_product_expense += row['product_expense']
                month_total_packaging += row['packaging_cost']
                month_total_shipping += row.get('shipping_subsidy_cost', 0)
                month_total_fixed += row['fixed_daily_cost']
                month_total_fb_ads += row['fb_ads_spend']
                month_total_google_ads += google_ads
                month_total_net_profit += row['net_profit']

            # Calculate total for monthly summary
            month_total_fixed_costs = month_total_packaging + month_total_shipping + month_total_fixed
            month_total_cost = month_total_product_expense + month_total_packaging + month_total_shipping + month_total_fixed + month_total_fb_ads + month_total_google_ads
            month_total_roi = (month_total_net_profit / month_total_cost * 100) if month_total_cost > 0 else 0
            month_total_aov = month_total_revenue / month_total_orders if month_total_orders > 0 else 0
            month_total_fb_per_order = month_total_fb_ads / month_total_orders if month_total_orders > 0 else 0
            
            print("-"*240)
            print(f"{'TOTAL':<12} {month_total_orders:>8} {month_total_items:>8} "
                  f"{month_total_revenue:>12.2f} {month_total_aov:>8.2f} {month_total_product_expense:>12.2f} "
                  f"{month_total_fixed_costs:>14.2f} "
                  f"{month_total_fb_ads:>12.2f} {month_total_google_ads:>14.2f} {month_total_cost:>14.2f} "
                  f"{month_total_net_profit:>12.2f} {month_total_roi:>8.2f}")
        
        # Display all products
        print("\n" + "="*80)
        print("ALL PRODUCTS BY REVENUE")
        print("="*80)
        
        # Aggregate products across all dates
        product_summary = date_product_agg.groupby('product_name').agg({
            'total_quantity': 'sum',
            'total_revenue': 'sum',
            'product_expense': 'sum',
            'profit': 'sum',
            'order_count': 'sum'
        }).reset_index()
        
        # Calculate aggregated ROI (without FB ads)
        product_summary['roi_percent'] = product_summary.apply(
            lambda row: round((row['profit'] / row['product_expense'] * 100) if row['product_expense'] > 0 else 0, 2),
            axis=1
        )
        
        product_summary = product_summary.sort_values('total_revenue', ascending=False)
        
        print(f"\n{'Product':<40} {'Qty':>6} {'Revenue':>10} {'Product Cost':>12} {'Profit':>10} {'ROI %':>8}")
        print("-"*100)
        
        for _, row in product_summary.iterrows():
            product_name = row['product_name'][:40]  # Truncate long names
            print(f"{product_name:<40} {row['total_quantity']:>6} "
                  f"{row['total_revenue']:>10.2f} {row['product_expense']:>12.2f} "
                  f"{row['profit']:>10.2f} {row['roi_percent']:>8.2f}")
        
        print("\n")
    
    def analyze_returning_customers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Analyze returning customers and calculate weekly percentages"""
        print("\nAnalyzing returning customers...")
        revenue_col = 'order_revenue_net' if 'order_revenue_net' in df.columns else 'order_total'
        
        # Convert purchase_date to datetime
        df['purchase_datetime'] = pd.to_datetime(df['purchase_date'])
        df['purchase_date_only'] = df['purchase_datetime'].dt.date
        
        # Extract week information
        df['year_week'] = df['purchase_datetime'].dt.to_period('W')
        
        # Get unique customers per order (one row per order)
        orders_df = df[['order_num', 'customer_email', 'purchase_datetime', 'year_week', revenue_col]].drop_duplicates(subset=['order_num'])
        
        # Track first purchase date for each customer
        customer_first_purchase = orders_df.groupby('customer_email')['purchase_datetime'].min().to_dict()
        
        # Determine if each order is from a returning customer
        orders_df['is_returning'] = orders_df.apply(
            lambda row: row['purchase_datetime'] > customer_first_purchase[row['customer_email']], axis=1
        )
        
        # Calculate weekly statistics
        weekly_stats = orders_df.groupby('year_week').agg({
            'order_num': 'count',  # Total orders
            'is_returning': 'sum',  # Returning customer orders
            'customer_email': 'nunique'  # Unique customers
        }).reset_index()
        
        weekly_stats.columns = ['week', 'total_orders', 'returning_orders', 'unique_customers']
        
        # Calculate new customer orders
        weekly_stats['new_orders'] = weekly_stats['total_orders'] - weekly_stats['returning_orders']
        
        # Calculate percentages
        weekly_stats['returning_percentage'] = (weekly_stats['returning_orders'] / weekly_stats['total_orders'] * 100).round(2)
        weekly_stats['new_percentage'] = (weekly_stats['new_orders'] / weekly_stats['total_orders'] * 100).round(2)
        
        # Add week start date for better visualization
        weekly_stats['week_start'] = weekly_stats['week'].apply(lambda x: x.start_time.date())
        
        # Sort by week
        weekly_stats = weekly_stats.sort_values('week')
        
        # Save to CSV
        filename = f"data/returning_customers_analysis_{df['purchase_datetime'].min().strftime('%Y%m%d')}-{df['purchase_datetime'].max().strftime('%Y%m%d')}.csv"
        weekly_stats.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"Returning customers analysis saved: {filename}")
        
        return weekly_stats

    def analyze_new_vs_returning_revenue(self, df: pd.DataFrame) -> dict:
        """Analyze revenue split between new and returning customers (order-level, net revenue)."""
        print("\nAnalyzing new vs returning revenue split...")
        revenue_col = 'order_revenue_net' if 'order_revenue_net' in df.columns else 'order_total'

        orders_df = df[['order_num', 'customer_email', 'purchase_date', revenue_col]].drop_duplicates(subset=['order_num']).copy()
        orders_df['purchase_datetime'] = pd.to_datetime(orders_df['purchase_date'])
        orders_df['purchase_date_only'] = orders_df['purchase_datetime'].dt.date

        first_purchase_map = orders_df.groupby('customer_email')['purchase_datetime'].min().to_dict()
        orders_df['is_returning'] = orders_df.apply(
            lambda row: row['purchase_datetime'] > first_purchase_map.get(row['customer_email'], row['purchase_datetime']),
            axis=1
        )

        daily = orders_df.groupby(['purchase_date_only', 'is_returning'])[revenue_col].sum().reset_index()
        daily_pivot = daily.pivot(index='purchase_date_only', columns='is_returning', values=revenue_col).fillna(0).reset_index()
        daily_pivot = daily_pivot.rename(
            columns={
                False: 'new_revenue',
                True: 'returning_revenue',
                'purchase_date_only': 'date'
            }
        )

        if 'new_revenue' not in daily_pivot.columns:
            daily_pivot['new_revenue'] = 0.0
        if 'returning_revenue' not in daily_pivot.columns:
            daily_pivot['returning_revenue'] = 0.0

        daily_pivot['total_revenue'] = daily_pivot['new_revenue'] + daily_pivot['returning_revenue']
        daily_pivot['new_revenue_share_pct'] = daily_pivot.apply(
            lambda row: round((row['new_revenue'] / row['total_revenue'] * 100) if row['total_revenue'] > 0 else 0, 2),
            axis=1
        )
        daily_pivot['returning_revenue_share_pct'] = daily_pivot.apply(
            lambda row: round((row['returning_revenue'] / row['total_revenue'] * 100) if row['total_revenue'] > 0 else 0, 2),
            axis=1
        )
        daily_pivot = daily_pivot.sort_values('date')

        total_new_revenue = float(daily_pivot['new_revenue'].sum())
        total_returning_revenue = float(daily_pivot['returning_revenue'].sum())
        total_revenue = total_new_revenue + total_returning_revenue

        result = {
            'summary': {
                'new_revenue': round(total_new_revenue, 2),
                'returning_revenue': round(total_returning_revenue, 2),
                'total_revenue': round(total_revenue, 2),
                'new_revenue_share_pct': round((total_new_revenue / total_revenue * 100) if total_revenue > 0 else 0, 1),
                'returning_revenue_share_pct': round((total_returning_revenue / total_revenue * 100) if total_revenue > 0 else 0, 1),
            },
            'daily': daily_pivot
        }

        print(
            f"New vs returning revenue: new=€{result['summary']['new_revenue']:.2f}, "
            f"returning=€{result['summary']['returning_revenue']:.2f}"
        )
        return result

    def analyze_repeat_purchase_cohorts(self, df: pd.DataFrame) -> dict:
        """
        Analyze repeat purchase cohorts with detailed metrics:
        - Cohort by first purchase month
        - Time to nth order (2nd, 3rd, 4th, 5th+)
        - Time between consecutive orders
        - Cohort retention rates
        - Order frequency distribution
        - Revenue progression by order number
        """
        print("\nAnalyzing repeat purchase cohorts...")
        revenue_col = 'order_revenue_net' if 'order_revenue_net' in df.columns else 'order_total'

        # Ensure datetime column exists
        df['purchase_datetime'] = pd.to_datetime(df['purchase_date'])

        # Get unique orders with customer info (include total_items_in_order for metrics)
        orders_df = df[['order_num', 'customer_email', 'purchase_datetime', revenue_col, 'total_items_in_order']].drop_duplicates(subset=['order_num'])
        orders_df = orders_df.sort_values(['customer_email', 'purchase_datetime'])

        # Add order number for each customer (1st, 2nd, 3rd, etc.)
        orders_df['customer_order_num'] = orders_df.groupby('customer_email').cumcount() + 1

        # Get first order date for each customer (defines their cohort)
        customer_first_order = orders_df.groupby('customer_email').agg({
            'purchase_datetime': 'min',
            revenue_col: 'first'
        }).reset_index()
        customer_first_order.columns = ['customer_email', 'first_order_date', 'first_order_value']
        customer_first_order['cohort_month'] = customer_first_order['first_order_date'].dt.to_period('M')

        # Merge cohort info back to orders
        orders_df = orders_df.merge(
            customer_first_order[['customer_email', 'first_order_date', 'cohort_month']],
            on='customer_email'
        )

        # Calculate time since first order for each order
        orders_df['days_since_first_order'] = (orders_df['purchase_datetime'] - orders_df['first_order_date']).dt.days

        # Calculate time between consecutive orders
        orders_df['prev_order_date'] = orders_df.groupby('customer_email')['purchase_datetime'].shift(1)
        orders_df['days_since_prev_order'] = (orders_df['purchase_datetime'] - orders_df['prev_order_date']).dt.days

        result = {}

        # === 1. TIME TO NTH ORDER ANALYSIS ===
        print("  Calculating time to nth order...")
        time_to_nth_order = []
        for order_num in range(2, 7):  # 2nd through 6th order
            nth_orders = orders_df[orders_df['customer_order_num'] == order_num]
            if len(nth_orders) > 0:
                time_to_nth_order.append({
                    'order_number': f'{order_num}{"st" if order_num == 1 else "nd" if order_num == 2 else "rd" if order_num == 3 else "th"} Order',
                    'order_num_value': order_num,
                    'customer_count': len(nth_orders),
                    'avg_days_from_first': round(nth_orders['days_since_first_order'].mean(), 1),
                    'median_days_from_first': round(nth_orders['days_since_first_order'].median(), 1),
                    'min_days_from_first': int(nth_orders['days_since_first_order'].min()),
                    'max_days_from_first': int(nth_orders['days_since_first_order'].max()),
                    'avg_days_from_prev': round(nth_orders['days_since_prev_order'].mean(), 1) if order_num > 1 else 0,
                    'median_days_from_prev': round(nth_orders['days_since_prev_order'].median(), 1) if order_num > 1 else 0,
                    'avg_order_value': round(nth_orders[revenue_col].mean(), 2)
                })

        result['time_to_nth_order'] = pd.DataFrame(time_to_nth_order)

        # === 2. TIME BETWEEN ORDERS DISTRIBUTION ===
        print("  Analyzing time between orders...")
        repeat_orders = orders_df[orders_df['customer_order_num'] > 1].copy()
        if len(repeat_orders) > 0:
            # Create buckets for time between orders
            def categorize_time_between(days):
                if pd.isna(days):
                    return None
                if days <= 7:
                    return '0-7 days'
                elif days <= 14:
                    return '8-14 days'
                elif days <= 30:
                    return '15-30 days'
                elif days <= 60:
                    return '31-60 days'
                elif days <= 90:
                    return '61-90 days'
                else:
                    return '90+ days'

            repeat_orders['time_bucket'] = repeat_orders['days_since_prev_order'].apply(categorize_time_between)
            time_distribution = repeat_orders.groupby('time_bucket').size().reset_index(name='count')

            # Ensure proper ordering
            bucket_order = ['0-7 days', '8-14 days', '15-30 days', '31-60 days', '61-90 days', '90+ days']
            time_distribution['bucket_order'] = time_distribution['time_bucket'].apply(
                lambda x: bucket_order.index(x) if x in bucket_order else 99
            )
            time_distribution = time_distribution.sort_values('bucket_order').drop('bucket_order', axis=1)
            time_distribution['percentage'] = round(time_distribution['count'] / time_distribution['count'].sum() * 100, 1)

            result['time_between_orders'] = time_distribution

            # === 2b. TIME BETWEEN ORDERS BY ORDER NUMBER (1st→2nd, 2nd→3rd, etc.) ===
            print("  Analyzing time between orders by order number...")
            time_by_order_num = []
            for order_num in range(2, 7):  # 2nd through 6th order
                order_transitions = repeat_orders[repeat_orders['customer_order_num'] == order_num]
                if len(order_transitions) >= 3:  # Min 3 data points
                    transition_label = f'{order_num-1}→{order_num}'
                    time_by_order_num.append({
                        'transition': transition_label,
                        'order_num': order_num,
                        'count': len(order_transitions),
                        'avg_days': round(order_transitions['days_since_prev_order'].mean(), 1),
                        'median_days': round(order_transitions['days_since_prev_order'].median(), 1),
                        'min_days': int(order_transitions['days_since_prev_order'].min()),
                        'max_days': int(order_transitions['days_since_prev_order'].max())
                    })

            result['time_between_by_order_num'] = pd.DataFrame(time_by_order_num)
        else:
            result['time_between_orders'] = pd.DataFrame()
            result['time_between_by_order_num'] = pd.DataFrame()

        # === 3. COHORT RETENTION ANALYSIS ===
        print("  Calculating cohort retention...")
        cohort_data = []

        for cohort in orders_df['cohort_month'].unique():
            cohort_customers = orders_df[orders_df['cohort_month'] == cohort]['customer_email'].unique()
            total_customers = len(cohort_customers)

            if total_customers < 3:  # Skip very small cohorts
                continue

            # Count how many made 2nd, 3rd, 4th, 5th+ order
            orders_per_customer = orders_df[orders_df['customer_email'].isin(cohort_customers)].groupby('customer_email').size()

            cohort_data.append({
                'cohort': str(cohort),
                'total_customers': total_customers,
                'made_2nd_order': len(orders_per_customer[orders_per_customer >= 2]),
                'made_3rd_order': len(orders_per_customer[orders_per_customer >= 3]),
                'made_4th_order': len(orders_per_customer[orders_per_customer >= 4]),
                'made_5th_order': len(orders_per_customer[orders_per_customer >= 5]),
                'retention_2nd_pct': round(len(orders_per_customer[orders_per_customer >= 2]) / total_customers * 100, 1),
                'retention_3rd_pct': round(len(orders_per_customer[orders_per_customer >= 3]) / total_customers * 100, 1),
                'retention_4th_pct': round(len(orders_per_customer[orders_per_customer >= 4]) / total_customers * 100, 1),
                'retention_5th_pct': round(len(orders_per_customer[orders_per_customer >= 5]) / total_customers * 100, 1),
                'avg_orders_per_customer': round(orders_per_customer.mean(), 2),
                'total_orders': orders_per_customer.sum()
            })

        result['cohort_retention'] = pd.DataFrame(cohort_data)

        # Sort cohort retention by cohort month
        if not result['cohort_retention'].empty:
            result['cohort_retention'] = result['cohort_retention'].sort_values('cohort').reset_index(drop=True)

        # === 3b. TIME-BIAS-FREE COHORT ANALYSIS (only mature cohorts 90+ days old) ===
        print("  Calculating time-bias-free cohort retention (90+ day cohorts only)...")
        today = pd.Timestamp.now()
        mature_cohort_data = []

        for cohort in orders_df['cohort_month'].unique():
            # Calculate cohort age (days since end of cohort month)
            cohort_end = pd.Period(cohort, 'M').end_time
            cohort_age_days = (today - cohort_end).days

            # Only include cohorts that are 90+ days old
            if cohort_age_days < 90:
                continue

            cohort_customers = orders_df[orders_df['cohort_month'] == cohort]['customer_email'].unique()
            total_customers = len(cohort_customers)

            if total_customers < 3:  # Skip very small cohorts
                continue

            # Count how many made 2nd, 3rd, 4th, 5th+ order
            orders_per_customer = orders_df[orders_df['customer_email'].isin(cohort_customers)].groupby('customer_email').size()

            mature_cohort_data.append({
                'cohort': str(cohort),
                'cohort_age_days': cohort_age_days,
                'total_customers': total_customers,
                'made_2nd_order': len(orders_per_customer[orders_per_customer >= 2]),
                'made_3rd_order': len(orders_per_customer[orders_per_customer >= 3]),
                'made_4th_order': len(orders_per_customer[orders_per_customer >= 4]),
                'made_5th_order': len(orders_per_customer[orders_per_customer >= 5]),
                'retention_2nd_pct': round(len(orders_per_customer[orders_per_customer >= 2]) / total_customers * 100, 1),
                'retention_3rd_pct': round(len(orders_per_customer[orders_per_customer >= 3]) / total_customers * 100, 1),
                'retention_4th_pct': round(len(orders_per_customer[orders_per_customer >= 4]) / total_customers * 100, 1),
                'retention_5th_pct': round(len(orders_per_customer[orders_per_customer >= 5]) / total_customers * 100, 1),
                'avg_orders_per_customer': round(orders_per_customer.mean(), 2),
                'total_orders': orders_per_customer.sum()
            })

        result['mature_cohort_retention'] = pd.DataFrame(mature_cohort_data)

        # Sort by cohort month
        if not result['mature_cohort_retention'].empty:
            result['mature_cohort_retention'] = result['mature_cohort_retention'].sort_values('cohort').reset_index(drop=True)

            # Calculate average retention across mature cohorts (weighted by customer count)
            total_mature_customers = result['mature_cohort_retention']['total_customers'].sum()
            if total_mature_customers > 0:
                weighted_2nd = (result['mature_cohort_retention']['retention_2nd_pct'] * result['mature_cohort_retention']['total_customers']).sum() / total_mature_customers
                weighted_3rd = (result['mature_cohort_retention']['retention_3rd_pct'] * result['mature_cohort_retention']['total_customers']).sum() / total_mature_customers
                # Store for later adding to summary
                result['_mature_cohort_stats'] = {
                    'true_retention_2nd_pct': round(weighted_2nd, 1),
                    'true_retention_3rd_pct': round(weighted_3rd, 1),
                    'mature_cohorts_count': len(result['mature_cohort_retention'])
                }
                print(f"    Mature cohorts (90+ days): {len(result['mature_cohort_retention'])}")
                print(f"    True 2nd order retention: {result['_mature_cohort_stats']['true_retention_2nd_pct']}%")
                print(f"    True 3rd order retention: {result['_mature_cohort_stats']['true_retention_3rd_pct']}%")

        # === 4. ORDER FREQUENCY DISTRIBUTION ===
        print("  Analyzing order frequency distribution...")
        orders_per_customer = orders_df.groupby('customer_email').size().reset_index(name='order_count')

        def categorize_order_count(count):
            if count == 1:
                return '1 order'
            elif count == 2:
                return '2 orders'
            elif count == 3:
                return '3 orders'
            elif count == 4:
                return '4 orders'
            elif count == 5:
                return '5 orders'
            else:
                return '6+ orders'

        orders_per_customer['frequency_bucket'] = orders_per_customer['order_count'].apply(categorize_order_count)
        frequency_dist = orders_per_customer.groupby('frequency_bucket').agg({
            'customer_email': 'count',
            'order_count': 'sum'
        }).reset_index()
        frequency_dist.columns = ['frequency', 'customer_count', 'total_orders']

        # Ensure proper ordering
        freq_order = ['1 order', '2 orders', '3 orders', '4 orders', '5 orders', '6+ orders']
        frequency_dist['freq_order'] = frequency_dist['frequency'].apply(
            lambda x: freq_order.index(x) if x in freq_order else 99
        )
        frequency_dist = frequency_dist.sort_values('freq_order').drop('freq_order', axis=1)
        frequency_dist['customer_pct'] = round(frequency_dist['customer_count'] / frequency_dist['customer_count'].sum() * 100, 1)
        frequency_dist['orders_pct'] = round(frequency_dist['total_orders'] / frequency_dist['total_orders'].sum() * 100, 1)

        result['order_frequency'] = frequency_dist

        # === 5. REVENUE BY ORDER NUMBER ===
        print("  Analyzing revenue by order number...")
        revenue_by_order_num = orders_df.groupby('customer_order_num').agg({
            revenue_col: ['mean', 'sum', 'count'],
            'total_items_in_order': 'mean'
        }).reset_index()
        revenue_by_order_num.columns = ['order_number', 'avg_order_value', 'total_revenue', 'order_count', 'avg_items_per_order']
        revenue_by_order_num = revenue_by_order_num[revenue_by_order_num['order_count'] >= 3]  # Min 3 orders to include
        revenue_by_order_num['avg_order_value'] = revenue_by_order_num['avg_order_value'].round(2)
        revenue_by_order_num['total_revenue'] = revenue_by_order_num['total_revenue'].round(2)
        revenue_by_order_num['avg_items_per_order'] = revenue_by_order_num['avg_items_per_order'].round(2)
        # Calculate avg price per item
        revenue_by_order_num['avg_price_per_item'] = (revenue_by_order_num['avg_order_value'] / revenue_by_order_num['avg_items_per_order']).round(2)

        # Limit to first 10 orders for display
        revenue_by_order_num = revenue_by_order_num[revenue_by_order_num['order_number'] <= 10]

        result['revenue_by_order_num'] = revenue_by_order_num

        # === 6. SUMMARY STATISTICS ===
        print("  Calculating summary statistics...")
        total_customers = orders_df['customer_email'].nunique()
        repeat_customers = len(orders_per_customer[orders_per_customer['order_count'] > 1])

        # Average time to repeat purchase (for customers who made 2nd order)
        second_orders = orders_df[orders_df['customer_order_num'] == 2]
        avg_days_to_2nd = second_orders['days_since_first_order'].mean() if len(second_orders) > 0 else None
        median_days_to_2nd = second_orders['days_since_first_order'].median() if len(second_orders) > 0 else None

        # Average time between all repeat orders
        avg_days_between = repeat_orders['days_since_prev_order'].mean() if len(repeat_orders) > 0 else None
        median_days_between = repeat_orders['days_since_prev_order'].median() if len(repeat_orders) > 0 else None

        result['summary'] = {
            'total_customers': total_customers,
            'repeat_customers': repeat_customers,
            'repeat_rate_pct': round(repeat_customers / total_customers * 100, 1) if total_customers > 0 else 0,
            'one_time_customers': total_customers - repeat_customers,
            'avg_orders_per_customer': round(orders_df['order_num'].nunique() / total_customers, 2) if total_customers > 0 else 0,
            'avg_days_to_2nd_order': round(avg_days_to_2nd, 1) if avg_days_to_2nd else None,
            'median_days_to_2nd_order': round(median_days_to_2nd, 1) if median_days_to_2nd else None,
            'avg_days_between_orders': round(avg_days_between, 1) if avg_days_between else None,
            'median_days_between_orders': round(median_days_between, 1) if median_days_between else None,
            'max_orders_by_customer': int(orders_per_customer['order_count'].max()),
            'customers_with_5plus_orders': len(orders_per_customer[orders_per_customer['order_count'] >= 5])
        }

        # Add mature cohort stats to summary if available
        if '_mature_cohort_stats' in result:
            result['summary'].update(result['_mature_cohort_stats'])
            del result['_mature_cohort_stats']

        # Save cohort analysis to CSV
        cohort_filename = f"data/cohort_analysis_{df['purchase_datetime'].min().strftime('%Y%m%d')}-{df['purchase_datetime'].max().strftime('%Y%m%d')}.csv"
        if not result['cohort_retention'].empty:
            result['cohort_retention'].to_csv(cohort_filename, index=False, encoding='utf-8-sig')
            print(f"Cohort analysis saved: {cohort_filename}")

        print(f"  Cohort analysis complete:")
        print(f"    - Total customers: {result['summary']['total_customers']}")
        print(f"    - Repeat customers: {result['summary']['repeat_customers']} ({result['summary']['repeat_rate_pct']}%)")
        print(f"    - Avg days to 2nd order: {result['summary']['avg_days_to_2nd_order']}")
        print(f"    - Avg days between orders: {result['summary']['avg_days_between_orders']}")

        return result

    def analyze_retention_by_first_order_item(self, df: pd.DataFrame, min_first_orders: int = 50, top_n: int = 20) -> dict:
        """
        Analyze customer retention based on items in their first order.
        For top N items that appear in first orders (min occurrences), calculate:
        - Total customers whose first order contained this item
        - How many made 2nd, 3rd, 4th, 5th order
        - Retention percentages

        Args:
            df: DataFrame with order data
            min_first_orders: Minimum number of first orders containing the item to include in analysis
            top_n: Number of top items to analyze (by first order occurrence count)

        Returns:
            dict with 'item_retention' DataFrame and 'summary' dict
        """
        print(f"\nAnalyzing retention by first order item (top {top_n} items, min {min_first_orders} first orders)...")

        # Ensure datetime column exists
        df['purchase_datetime'] = pd.to_datetime(df['purchase_date'])

        # Get unique orders with customer info
        orders_df = df[['order_num', 'customer_email', 'purchase_datetime', 'item_label', 'product_sku']].copy()
        orders_df = orders_df.sort_values(['customer_email', 'purchase_datetime'])

        # Get first order for each customer
        first_order_per_customer = orders_df.groupby('customer_email')['order_num'].first().reset_index()
        first_order_per_customer.columns = ['customer_email', 'first_order_num']

        # Get items in first orders
        first_order_items = orders_df.merge(
            first_order_per_customer,
            left_on=['customer_email', 'order_num'],
            right_on=['customer_email', 'first_order_num']
        )[['customer_email', 'item_label', 'product_sku']]

        # Count how many first orders contain each item
        item_first_order_counts = first_order_items.groupby(['item_label', 'product_sku'])['customer_email'].nunique().reset_index()
        item_first_order_counts.columns = ['item_name', 'item_sku', 'first_order_count']

        # Filter items with minimum first orders and get top N
        qualified_items = item_first_order_counts[item_first_order_counts['first_order_count'] >= min_first_orders]
        qualified_items = qualified_items.nlargest(top_n, 'first_order_count')

        if qualified_items.empty:
            print(f"  No items found with {min_first_orders}+ first order occurrences")
            return {'item_retention': pd.DataFrame(), 'summary': {}}

        # Calculate total orders per customer
        orders_per_customer = orders_df.groupby('customer_email')['order_num'].nunique().reset_index()
        orders_per_customer.columns = ['customer_email', 'total_orders']

        # Calculate retention for each qualified item
        item_retention_data = []

        for _, item_row in qualified_items.iterrows():
            item_name = item_row['item_name']
            item_sku = item_row['item_sku']

            # Get customers whose first order contained this item
            customers_with_item = first_order_items[
                (first_order_items['item_label'] == item_name) &
                (first_order_items['product_sku'] == item_sku)
            ]['customer_email'].unique()

            # Get order counts for these customers
            customer_orders = orders_per_customer[orders_per_customer['customer_email'].isin(customers_with_item)]
            total_customers = len(customer_orders)

            if total_customers == 0:
                continue

            # Calculate retention rates
            made_2nd = len(customer_orders[customer_orders['total_orders'] >= 2])
            made_3rd = len(customer_orders[customer_orders['total_orders'] >= 3])
            made_4th = len(customer_orders[customer_orders['total_orders'] >= 4])
            made_5th = len(customer_orders[customer_orders['total_orders'] >= 5])

            item_retention_data.append({
                'item_name': item_name,
                'item_sku': item_sku,
                'first_order_customers': total_customers,
                'made_2nd_order': made_2nd,
                'made_3rd_order': made_3rd,
                'made_4th_order': made_4th,
                'made_5th_order': made_5th,
                'retention_2nd_pct': round(made_2nd / total_customers * 100, 1),
                'retention_3rd_pct': round(made_3rd / total_customers * 100, 1),
                'retention_4th_pct': round(made_4th / total_customers * 100, 1),
                'retention_5th_pct': round(made_5th / total_customers * 100, 1),
                'avg_orders_per_customer': round(customer_orders['total_orders'].mean(), 2)
            })

        item_retention_df = pd.DataFrame(item_retention_data)

        # Sort by retention rate (2nd order) descending
        if not item_retention_df.empty:
            item_retention_df = item_retention_df.sort_values('retention_2nd_pct', ascending=False).reset_index(drop=True)

        # Calculate summary statistics
        summary = {}
        if not item_retention_df.empty:
            summary = {
                'total_items_analyzed': len(item_retention_df),
                'avg_retention_2nd_pct': round(item_retention_df['retention_2nd_pct'].mean(), 1),
                'avg_retention_3rd_pct': round(item_retention_df['retention_3rd_pct'].mean(), 1),
                'best_retention_item': item_retention_df.iloc[0]['item_name'] if len(item_retention_df) > 0 else None,
                'best_retention_2nd_pct': item_retention_df.iloc[0]['retention_2nd_pct'] if len(item_retention_df) > 0 else 0,
                'worst_retention_item': item_retention_df.iloc[-1]['item_name'] if len(item_retention_df) > 0 else None,
                'worst_retention_2nd_pct': item_retention_df.iloc[-1]['retention_2nd_pct'] if len(item_retention_df) > 0 else 0,
                'retention_spread': round(item_retention_df['retention_2nd_pct'].max() - item_retention_df['retention_2nd_pct'].min(), 1) if len(item_retention_df) > 0 else 0
            }

        print(f"  First order item retention analysis complete:")
        print(f"    - Items analyzed: {len(item_retention_df)}")
        if summary:
            print(f"    - Avg 2nd order retention: {summary['avg_retention_2nd_pct']}%")
            print(f"    - Best item: {summary['best_retention_item'][:40]}... ({summary['best_retention_2nd_pct']}%)" if summary['best_retention_item'] else "")
            print(f"    - Retention spread: {summary['retention_spread']}%")

        return {
            'item_retention': item_retention_df,
            'summary': summary
        }

    def analyze_same_item_repurchase(self, df: pd.DataFrame, min_orders: int = 50, top_n: int = 20) -> dict:
        """
        Analyze retention based on same item repurchase across multiple orders.
        For each item, calculate how often customers who bought it buy it again.

        Args:
            df: DataFrame with order data
            min_orders: Minimum total orders containing the item to include in analysis
            top_n: Number of top items to analyze (by total order count)

        Returns:
            dict with:
            - 'item_repurchase': DataFrame with repurchase rates per item
            - 'customer_item_frequency': DataFrame with customer-item purchase frequency distribution
            - 'summary': Summary statistics
        """
        print(f"\nAnalyzing same item repurchase retention (top {top_n} items, min {min_orders} orders)...")

        # Ensure datetime column exists
        df['purchase_datetime'] = pd.to_datetime(df['purchase_date'])

        # Get item purchases with customer info
        item_purchases = df[['order_num', 'customer_email', 'purchase_datetime', 'item_label', 'product_sku', 'item_quantity']].copy()
        item_purchases = item_purchases.rename(columns={'item_label': 'item_name', 'product_sku': 'item_sku', 'item_quantity': 'quantity'})

        # Count total orders per item (unique orders, not quantity)
        item_order_counts = item_purchases.groupby(['item_name', 'item_sku'])['order_num'].nunique().reset_index()
        item_order_counts.columns = ['item_name', 'item_sku', 'total_orders']

        # Filter items with minimum orders and get top N
        qualified_items = item_order_counts[item_order_counts['total_orders'] >= min_orders]
        qualified_items = qualified_items.nlargest(top_n, 'total_orders')

        if qualified_items.empty:
            print(f"  No items found with {min_orders}+ orders")
            return {'item_repurchase': pd.DataFrame(), 'customer_item_frequency': pd.DataFrame(), 'summary': {}}

        # Calculate repurchase metrics for each qualified item
        item_repurchase_data = []

        for _, item_row in qualified_items.iterrows():
            item_name = item_row['item_name']
            item_sku = item_row['item_sku']

            # Get all purchases of this item
            item_orders = item_purchases[
                (item_purchases['item_name'] == item_name) &
                (item_purchases['item_sku'] == item_sku)
            ]

            # Count unique orders per customer for this item
            customer_item_orders = item_orders.groupby('customer_email')['order_num'].nunique().reset_index()
            customer_item_orders.columns = ['customer_email', 'item_order_count']

            total_customers = len(customer_item_orders)
            if total_customers == 0:
                continue

            # Calculate repurchase rates
            bought_2_times = len(customer_item_orders[customer_item_orders['item_order_count'] >= 2])
            bought_3_times = len(customer_item_orders[customer_item_orders['item_order_count'] >= 3])
            bought_4_times = len(customer_item_orders[customer_item_orders['item_order_count'] >= 4])
            bought_5_times = len(customer_item_orders[customer_item_orders['item_order_count'] >= 5])

            # Calculate average time between repurchases for this item
            repeat_purchasers = customer_item_orders[customer_item_orders['item_order_count'] >= 2]['customer_email'].unique()
            avg_days_between = None

            if len(repeat_purchasers) > 0:
                days_between_list = []
                for customer in repeat_purchasers:
                    customer_item_dates = item_orders[item_orders['customer_email'] == customer].sort_values('purchase_datetime')['purchase_datetime'].values
                    for i in range(1, len(customer_item_dates)):
                        days = (customer_item_dates[i] - customer_item_dates[i-1]) / pd.Timedelta(days=1)
                        days_between_list.append(days)
                if days_between_list:
                    avg_days_between = round(np.mean(days_between_list), 1)

            item_repurchase_data.append({
                'item_name': item_name,
                'item_sku': item_sku,
                'total_orders': item_row['total_orders'],
                'unique_customers': total_customers,
                'bought_2_times': bought_2_times,
                'bought_3_times': bought_3_times,
                'bought_4_times': bought_4_times,
                'bought_5_times': bought_5_times,
                'repurchase_2x_pct': round(bought_2_times / total_customers * 100, 1),
                'repurchase_3x_pct': round(bought_3_times / total_customers * 100, 1),
                'repurchase_4x_pct': round(bought_4_times / total_customers * 100, 1),
                'repurchase_5x_pct': round(bought_5_times / total_customers * 100, 1),
                'avg_purchases_per_customer': round(customer_item_orders['item_order_count'].mean(), 2),
                'avg_days_between_repurchase': avg_days_between
            })

        item_repurchase_df = pd.DataFrame(item_repurchase_data)

        # Sort by repurchase rate (2x) descending
        if not item_repurchase_df.empty:
            item_repurchase_df = item_repurchase_df.sort_values('repurchase_2x_pct', ascending=False).reset_index(drop=True)

        # Create frequency distribution of customer-item purchases
        all_customer_item_orders = []
        for _, item_row in qualified_items.iterrows():
            item_orders = item_purchases[
                (item_purchases['item_name'] == item_row['item_name']) &
                (item_purchases['item_sku'] == item_row['item_sku'])
            ]
            customer_counts = item_orders.groupby('customer_email')['order_num'].nunique()
            all_customer_item_orders.extend(customer_counts.tolist())

        frequency_dist = []
        if all_customer_item_orders:
            from collections import Counter
            count_dist = Counter(all_customer_item_orders)
            for freq in sorted(count_dist.keys())[:10]:  # Limit to first 10 frequencies
                label = f'{freq}x' if freq < 10 else '10+x'
                frequency_dist.append({
                    'purchase_frequency': label,
                    'customer_count': count_dist[freq],
                    'percentage': round(count_dist[freq] / len(all_customer_item_orders) * 100, 1)
                })

        customer_item_freq_df = pd.DataFrame(frequency_dist)

        # Calculate summary statistics
        summary = {}
        if not item_repurchase_df.empty:
            summary = {
                'total_items_analyzed': len(item_repurchase_df),
                'avg_repurchase_2x_pct': round(item_repurchase_df['repurchase_2x_pct'].mean(), 1),
                'avg_repurchase_3x_pct': round(item_repurchase_df['repurchase_3x_pct'].mean(), 1),
                'best_repurchase_item': item_repurchase_df.iloc[0]['item_name'] if len(item_repurchase_df) > 0 else None,
                'best_repurchase_2x_pct': item_repurchase_df.iloc[0]['repurchase_2x_pct'] if len(item_repurchase_df) > 0 else 0,
                'worst_repurchase_item': item_repurchase_df.iloc[-1]['item_name'] if len(item_repurchase_df) > 0 else None,
                'worst_repurchase_2x_pct': item_repurchase_df.iloc[-1]['repurchase_2x_pct'] if len(item_repurchase_df) > 0 else 0,
                'avg_days_between_repurchase': round(
                    item_repurchase_df[item_repurchase_df['avg_days_between_repurchase'].notna()]['avg_days_between_repurchase'].mean(), 1
                ) if item_repurchase_df['avg_days_between_repurchase'].notna().any() else None,
                'repurchase_spread': round(item_repurchase_df['repurchase_2x_pct'].max() - item_repurchase_df['repurchase_2x_pct'].min(), 1) if len(item_repurchase_df) > 0 else 0
            }

        print(f"  Same item repurchase analysis complete:")
        print(f"    - Items analyzed: {len(item_repurchase_df)}")
        if summary:
            print(f"    - Avg 2x repurchase rate: {summary['avg_repurchase_2x_pct']}%")
            print(f"    - Best item: {summary['best_repurchase_item'][:40]}... ({summary['best_repurchase_2x_pct']}%)" if summary['best_repurchase_item'] else "")
            print(f"    - Avg days between repurchase: {summary['avg_days_between_repurchase']}")

        return {
            'item_repurchase': item_repurchase_df,
            'customer_item_frequency': customer_item_freq_df,
            'summary': summary
        }

    def analyze_time_to_nth_by_first_item(self, df: pd.DataFrame, min_first_orders: int = 50, top_n: int = 20) -> dict:
        """
        Analyze time to nth order based on items in customer's first order.
        For top N items that appear in first orders, calculate:
        - Average/median days to 2nd, 3rd, 4th, 5th order
        - Comparison of timing between different first-order items

        Args:
            df: DataFrame with order data
            min_first_orders: Minimum number of first orders containing the item
            top_n: Number of top items to analyze

        Returns:
            dict with 'time_to_nth_by_item' DataFrame and 'summary' dict
        """
        print(f"\nAnalyzing time to nth order by first order item (top {top_n} items, min {min_first_orders} first orders)...")

        # Ensure datetime column exists
        df['purchase_datetime'] = pd.to_datetime(df['purchase_date'])

        # Get unique orders with customer info
        orders_df = df[['order_num', 'customer_email', 'purchase_datetime', 'item_label', 'product_sku']].copy()
        orders_df = orders_df.sort_values(['customer_email', 'purchase_datetime'])

        # Get first order for each customer
        first_order_per_customer = orders_df.groupby('customer_email').agg({
            'order_num': 'first',
            'purchase_datetime': 'first'
        }).reset_index()
        first_order_per_customer.columns = ['customer_email', 'first_order_num', 'first_order_date']

        # Get items in first orders
        first_order_items = orders_df.merge(
            first_order_per_customer,
            left_on=['customer_email', 'order_num'],
            right_on=['customer_email', 'first_order_num']
        )[['customer_email', 'item_label', 'product_sku', 'first_order_date']]

        # Count how many first orders contain each item
        item_first_order_counts = first_order_items.groupby(['item_label', 'product_sku'])['customer_email'].nunique().reset_index()
        item_first_order_counts.columns = ['item_name', 'item_sku', 'first_order_count']

        # Filter items with minimum first orders and get top N
        qualified_items = item_first_order_counts[item_first_order_counts['first_order_count'] >= min_first_orders]
        qualified_items = qualified_items.nlargest(top_n, 'first_order_count')

        if qualified_items.empty:
            print(f"  No items found with {min_first_orders}+ first order occurrences")
            return {'time_to_nth_by_item': pd.DataFrame(), 'summary': {}}

        # Get all orders per customer with order numbers
        all_orders = orders_df.drop_duplicates(subset=['customer_email', 'order_num']).copy()
        all_orders = all_orders.sort_values(['customer_email', 'purchase_datetime'])
        all_orders['customer_order_num'] = all_orders.groupby('customer_email').cumcount() + 1

        # Merge with first order date
        all_orders = all_orders.merge(
            first_order_per_customer[['customer_email', 'first_order_date']],
            on='customer_email'
        )
        all_orders['days_since_first_order'] = (all_orders['purchase_datetime'] - all_orders['first_order_date']).dt.days

        # Calculate time to nth order for each qualified item
        time_to_nth_data = []

        for _, item_row in qualified_items.iterrows():
            item_name = item_row['item_name']
            item_sku = item_row['item_sku']

            # Get customers whose first order contained this item
            customers_with_item = first_order_items[
                (first_order_items['item_label'] == item_name) &
                (first_order_items['product_sku'] == item_sku)
            ]['customer_email'].unique()

            # Get orders for these customers
            customer_orders = all_orders[all_orders['customer_email'].isin(customers_with_item)]

            item_data = {
                'item_name': item_name,
                'item_sku': item_sku,
                'first_order_customers': len(customers_with_item)
            }

            # Calculate time to nth order for orders 2-5
            for order_num in range(2, 6):
                nth_orders = customer_orders[customer_orders['customer_order_num'] == order_num]
                if len(nth_orders) > 0:
                    item_data[f'customers_{order_num}nd'] = len(nth_orders)
                    item_data[f'avg_days_to_{order_num}nd'] = round(nth_orders['days_since_first_order'].mean(), 1)
                    item_data[f'median_days_to_{order_num}nd'] = round(nth_orders['days_since_first_order'].median(), 1)
                else:
                    item_data[f'customers_{order_num}nd'] = 0
                    item_data[f'avg_days_to_{order_num}nd'] = None
                    item_data[f'median_days_to_{order_num}nd'] = None

            time_to_nth_data.append(item_data)

        time_to_nth_df = pd.DataFrame(time_to_nth_data)

        # Sort by avg days to 2nd order (ascending - faster return is better)
        if not time_to_nth_df.empty:
            time_to_nth_df = time_to_nth_df.sort_values('avg_days_to_2nd', ascending=True, na_position='last').reset_index(drop=True)

        # Calculate summary statistics
        summary = {}
        if not time_to_nth_df.empty:
            valid_2nd = time_to_nth_df[time_to_nth_df['avg_days_to_2nd'].notna()]
            if len(valid_2nd) > 0:
                summary = {
                    'total_items_analyzed': len(time_to_nth_df),
                    'avg_days_to_2nd_overall': round(valid_2nd['avg_days_to_2nd'].mean(), 1),
                    'fastest_return_item': valid_2nd.iloc[0]['item_name'] if len(valid_2nd) > 0 else None,
                    'fastest_return_days': valid_2nd.iloc[0]['avg_days_to_2nd'] if len(valid_2nd) > 0 else None,
                    'slowest_return_item': valid_2nd.iloc[-1]['item_name'] if len(valid_2nd) > 0 else None,
                    'slowest_return_days': valid_2nd.iloc[-1]['avg_days_to_2nd'] if len(valid_2nd) > 0 else None,
                    'days_spread': round(valid_2nd['avg_days_to_2nd'].max() - valid_2nd['avg_days_to_2nd'].min(), 1)
                }

        print(f"  Time to nth order by first item analysis complete:")
        print(f"    - Items analyzed: {len(time_to_nth_df)}")
        if summary:
            print(f"    - Avg days to 2nd order: {summary.get('avg_days_to_2nd_overall', 'N/A')}")
            if summary.get('fastest_return_item'):
                print(f"    - Fastest return: {summary['fastest_return_item'][:40]}... ({summary['fastest_return_days']} days)")
            print(f"    - Days spread: {summary.get('days_spread', 'N/A')}")

        return {
            'time_to_nth_by_item': time_to_nth_df,
            'summary': summary
        }

    def calculate_clv_and_return_time(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate Customer Lifetime Value and average return time"""
        print("\nCalculating CLV and customer return time...")
        revenue_col = 'order_revenue_net' if 'order_revenue_net' in df.columns else 'order_total'
        
        # Convert purchase_date to datetime
        df['purchase_datetime'] = pd.to_datetime(df['purchase_date'])
        df['purchase_date_only'] = df['purchase_datetime'].dt.date
        df['year_week'] = df['purchase_datetime'].dt.to_period('W')

        # One row per day spend table to avoid multiplying daily ad spend by order count
        daily_spend_df = df.groupby('purchase_date_only').agg({
            'fb_ads_daily_spend': 'first'
        }).reset_index()
        daily_spend_df['year_week'] = pd.to_datetime(daily_spend_df['purchase_date_only']).dt.to_period('W')
        weekly_fb_spend_map = daily_spend_df.groupby('year_week')['fb_ads_daily_spend'].sum().to_dict()
        
        # Get unique orders with customer info and FB ads spend
        orders_df = df[['order_num', 'customer_email', 'purchase_datetime', 'year_week', revenue_col, 'fb_ads_daily_spend']].drop_duplicates(subset=['order_num'])
        orders_df = orders_df.sort_values(['customer_email', 'purchase_datetime'])
        
        # Calculate CLV per customer (total revenue from customer)
        customer_clv = orders_df.groupby('customer_email')[revenue_col].sum().to_dict()
        
        # Calculate return times (days between orders)
        customer_return_times = {}
        customer_orders_count = {}
        
        for customer_email in orders_df['customer_email'].unique():
            customer_orders = orders_df[orders_df['customer_email'] == customer_email].sort_values('purchase_datetime')
            customer_orders_count[customer_email] = len(customer_orders)
            
            if len(customer_orders) > 1:
                # Calculate days between consecutive orders
                purchase_dates = customer_orders['purchase_datetime'].values
                return_times = []
                for i in range(1, len(purchase_dates)):
                    days_between = (purchase_dates[i] - purchase_dates[i-1]) / pd.Timedelta(days=1)
                    return_times.append(days_between)
                customer_return_times[customer_email] = np.mean(return_times) if return_times else None
            else:
                customer_return_times[customer_email] = None
        
        # Calculate weekly aggregations
        weekly_clv_stats = []
        
        for week in orders_df['year_week'].unique():
            week_orders = orders_df[orders_df['year_week'] == week]
            week_customers = week_orders['customer_email'].unique()
            
            # Calculate average CLV for customers who ordered this week
            week_clvs = [customer_clv[c] for c in week_customers]
            avg_clv = np.mean(week_clvs) if week_clvs else 0
            
            # Calculate average return time for returning customers this week
            week_return_times = []
            new_customers = 0
            returning_customers = 0
            
            for customer in week_customers:
                # Check if this is the customer's first order
                customer_first_order = orders_df[orders_df['customer_email'] == customer].iloc[0]
                if customer_first_order['year_week'] == week:
                    new_customers += 1
                else:
                    returning_customers += 1
                    if customer_return_times[customer] is not None:
                        week_return_times.append(customer_return_times[customer])
            
            avg_return_time = np.mean(week_return_times) if week_return_times else None
            
            # Calculate CAC (Customer Acquisition Cost) for the week
            # CAC = Total Marketing Costs / Number of New Customers
            week_fb_spend = weekly_fb_spend_map.get(week, 0)
            cac = round(week_fb_spend / new_customers, 2) if new_customers > 0 else 0
            
            # Calculate LTV/CAC ratio
            ltv_cac_ratio = round(avg_clv / cac, 2) if cac > 0 else 0
            
            weekly_clv_stats.append({
                'week': week,
                'week_start': week.start_time.date(),
                'unique_customers': len(week_customers),
                'new_customers': new_customers,
                'returning_customers': returning_customers,
                'avg_clv': round(avg_clv, 2),
                'avg_return_time_days': round(avg_return_time, 1) if avg_return_time else None,
                'total_revenue': week_orders[revenue_col].sum(),
                'fb_ads_spend': round(week_fb_spend, 2),
                'cac': cac,
                'ltv_cac_ratio': ltv_cac_ratio
            })
        
        # Convert to DataFrame
        weekly_clv_df = pd.DataFrame(weekly_clv_stats)
        weekly_clv_df = weekly_clv_df.sort_values('week')
        
        # Calculate cumulative CLV (how CLV is growing over time)
        all_customers_by_week = {}
        cumulative_clv = []
        
        for week in weekly_clv_df['week']:
            week_orders = orders_df[orders_df['year_week'] <= week]
            week_customer_clv = week_orders.groupby('customer_email')[revenue_col].sum()
            cumulative_clv.append(week_customer_clv.mean() if len(week_customer_clv) > 0 else 0)
        
        weekly_clv_df['cumulative_avg_clv'] = cumulative_clv
        weekly_clv_df['cumulative_avg_clv'] = weekly_clv_df['cumulative_avg_clv'].round(2)

        # Calculate cumulative CAC (running average of acquisition cost across all time)
        cumulative_cac = []
        cumulative_fb_spend = 0
        cumulative_new_customers = 0

        for idx, row in weekly_clv_df.iterrows():
            cumulative_fb_spend += row['fb_ads_spend']
            cumulative_new_customers += row['new_customers']
            avg_cac = cumulative_fb_spend / cumulative_new_customers if cumulative_new_customers > 0 else 0
            cumulative_cac.append(round(avg_cac, 2))

        weekly_clv_df['cumulative_avg_cac'] = cumulative_cac

        # Save to CSV
        filename = f"data/clv_return_time_analysis_{df['purchase_datetime'].min().strftime('%Y%m%d')}-{df['purchase_datetime'].max().strftime('%Y%m%d')}.csv"
        weekly_clv_df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"CLV and return time analysis saved: {filename}")
        
        return weekly_clv_df
    
    def analyze_order_size_distribution(self, df: pd.DataFrame) -> pd.DataFrame:
        """Analyze distribution of order sizes (number of items per order) grouped by day"""
        print("\nAnalyzing order size distribution...")

        # Convert purchase_date to datetime
        df['purchase_datetime'] = pd.to_datetime(df['purchase_date'])
        df['purchase_date_only'] = df['purchase_datetime'].dt.date

        # Get unique orders with their total items count per order
        orders_df = df[['order_num', 'purchase_date_only', 'total_items_in_order']].drop_duplicates(subset=['order_num'])

        # Define order size categories
        def categorize_order_size(items_count):
            if items_count == 1:
                return '1 item'
            elif items_count == 2:
                return '2 items'
            elif items_count == 3:
                return '3 items'
            elif items_count == 4:
                return '4 items'
            else:
                return '5+ items'

        orders_df['order_size_category'] = orders_df['total_items_in_order'].apply(categorize_order_size)

        # Group by date and order size category
        distribution = orders_df.groupby(['purchase_date_only', 'order_size_category']).size().reset_index(name='order_count')

        # Pivot to get categories as columns
        distribution_pivot = distribution.pivot(index='purchase_date_only', columns='order_size_category', values='order_count').fillna(0)

        # Ensure all categories are present (even if 0)
        for category in ['1 item', '2 items', '3 items', '4 items', '5+ items']:
            if category not in distribution_pivot.columns:
                distribution_pivot[category] = 0

        # Sort columns in order
        distribution_pivot = distribution_pivot[['1 item', '2 items', '3 items', '4 items', '5+ items']]

        # Reset index to get date as a column
        distribution_pivot = distribution_pivot.reset_index()

        # Sort by date
        distribution_pivot = distribution_pivot.sort_values('purchase_date_only')

        # Save to CSV
        filename = f"data/order_size_distribution_{df['purchase_datetime'].min().strftime('%Y%m%d')}-{df['purchase_datetime'].max().strftime('%Y%m%d')}.csv"
        distribution_pivot.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"Order size distribution saved: {filename}")

        return distribution_pivot

    def analyze_item_combinations(self, df: pd.DataFrame, min_count: int = 5) -> pd.DataFrame:
        """
        Analyze frequently ordered item combinations (grouped by product_sku).

        Args:
            df: DataFrame with order data (one row per item)
            min_count: Minimum number of times a combination must appear to be included

        Returns:
            DataFrame with combination analysis
        """
        from itertools import combinations
        from collections import Counter

        print("\nAnalyzing item combinations...")

        # Exclude items that should not be counted in combinations
        excluded_items = ['Tringelt']
        df_filtered = df[~df['item_label'].isin(excluded_items)].copy()
        print(f"Excluded {len(df) - len(df_filtered)} items ({', '.join(excluded_items)}) from combination analysis")

        # Create a mapping from product_sku to item_label for display
        sku_to_label = df_filtered.groupby('product_sku')['item_label'].first().to_dict()

        # Create a mapping from product_sku to average unit price
        sku_to_price = df_filtered.groupby('product_sku')['item_unit_price'].mean().to_dict()

        # Group items by order using product_sku
        order_items = df_filtered.groupby('order_num')['product_sku'].apply(lambda x: frozenset(x.unique())).reset_index()
        order_items.columns = ['order_num', 'items']

        # Filter to orders with 2 or more unique items
        multi_item_orders = order_items[order_items['items'].apply(len) >= 2]

        if multi_item_orders.empty:
            print("No orders with 2+ unique items found")
            return pd.DataFrame()

        print(f"Found {len(multi_item_orders)} orders with 2+ unique items")

        # Count all combinations (2, 3, 4, 5, etc.)
        combination_counts = Counter()

        for _, row in multi_item_orders.iterrows():
            items = sorted(row['items'])  # Sort for consistent ordering
            # Generate combinations of different sizes (2, 3, 4, 5, etc.)
            for combo_size in range(2, min(len(items) + 1, 6)):  # Up to 5 items in combo
                for combo in combinations(items, combo_size):
                    combination_counts[combo] += 1

        # Filter by minimum count
        filtered_combinations = {k: v for k, v in combination_counts.items() if v >= min_count}

        if not filtered_combinations:
            print(f"No combinations found with count >= {min_count}")
            return pd.DataFrame()

        # Create DataFrame
        combo_data = []
        for combo, count in filtered_combinations.items():
            # Convert product SKUs to labels for display
            combo_labels = [sku_to_label.get(sku, sku) for sku in combo]
            # Calculate total price for the combination
            combo_price = sum(sku_to_price.get(sku, 0) for sku in combo)
            combo_data.append({
                'combination_size': len(combo),
                'combination': '\n'.join(combo_labels),
                'combination_skus': '\n'.join(combo),
                'count': count,
                'price': round(combo_price, 2)
            })

        combo_df = pd.DataFrame(combo_data)
        combo_df = combo_df.sort_values('count', ascending=False)

        # Save to CSV
        filename = f"data/item_combinations_{df['purchase_datetime'].min().strftime('%Y%m%d')}-{df['purchase_datetime'].max().strftime('%Y%m%d')}.csv"
        combo_df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"Item combinations saved: {filename}")
        print(f"Found {len(combo_df)} combinations with count >= {min_count}")

        return combo_df

    def analyze_day_of_week(self, df: pd.DataFrame) -> pd.DataFrame:
        """Analyze orders and revenue by day of week"""
        print("\nAnalyzing day of week patterns...")

        df['day_of_week'] = pd.to_datetime(df['purchase_date']).dt.dayofweek
        df['day_name'] = pd.to_datetime(df['purchase_date']).dt.day_name()

        # Aggregate by day of week (using unique orders)
        orders_per_day = df.groupby(['day_of_week', 'day_name']).agg({
            'order_num': 'nunique',
            'item_total_without_tax': 'sum',
            'profit_before_ads': 'sum',
            'fb_ads_daily_spend': lambda x: x.drop_duplicates().sum(),
            'google_ads_daily_spend': lambda x: x.drop_duplicates().sum()
        }).reset_index()

        orders_per_day.columns = ['day_of_week', 'day_name', 'orders', 'revenue', 'profit', 'fb_spend', 'google_spend']
        orders_per_day = orders_per_day.sort_values('day_of_week')

        # Calculate averages and percentages
        total_orders = orders_per_day['orders'].sum()
        total_revenue = orders_per_day['revenue'].sum()
        orders_per_day['orders_pct'] = (orders_per_day['orders'] / total_orders * 100).round(1)
        orders_per_day['revenue_pct'] = (orders_per_day['revenue'] / total_revenue * 100).round(1)
        orders_per_day['aov'] = (orders_per_day['revenue'] / orders_per_day['orders']).round(2)

        print(f"Day of week analysis complete")
        return orders_per_day

    def analyze_day_hour_heatmap(self, df: pd.DataFrame) -> pd.DataFrame:
        """Analyze orders by day of week and hour of day for heatmap visualization"""
        print("\nAnalyzing day/hour heatmap patterns...")

        # Parse purchase_date to extract day of week and hour
        df['purchase_datetime_full'] = pd.to_datetime(df['purchase_date'])
        df['day_of_week'] = df['purchase_datetime_full'].dt.dayofweek  # 0=Monday, 6=Sunday
        df['hour_of_day'] = df['purchase_datetime_full'].dt.hour

        # Day names for display
        day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

        # Aggregate by day of week and hour (using unique orders)
        heatmap_data = df.groupby(['day_of_week', 'hour_of_day']).agg({
            'order_num': 'nunique'
        }).reset_index()
        heatmap_data.columns = ['day_of_week', 'hour', 'orders']

        # Create complete matrix (all combinations of day 0-6 and hour 0-23)
        complete_matrix = []
        for day in range(7):
            for hour in range(24):
                row = heatmap_data[(heatmap_data['day_of_week'] == day) & (heatmap_data['hour'] == hour)]
                if len(row) > 0:
                    orders = row['orders'].values[0]
                else:
                    orders = 0
                complete_matrix.append({
                    'day_of_week': day,
                    'day_name': day_names[day],
                    'hour': hour,
                    'orders': orders
                })

        result = pd.DataFrame(complete_matrix)
        print(f"Day/hour heatmap analysis complete")
        return result

    def analyze_geographic(self, df: pd.DataFrame) -> tuple:
        """Analyze orders by geographic location"""
        print("\nAnalyzing geographic distribution...")

        # By country
        country_agg = df.groupby('delivery_country').agg({
            'order_num': 'nunique',
            'item_total_without_tax': 'sum',
            'profit_before_ads': 'sum'
        }).reset_index()
        country_agg.columns = ['country', 'orders', 'revenue', 'profit']
        country_agg = country_agg.sort_values('revenue', ascending=False)
        country_agg['revenue_pct'] = (country_agg['revenue'] / country_agg['revenue'].sum() * 100).round(1)

        # By city (top 20)
        city_agg = df.groupby(['delivery_city', 'delivery_country']).agg({
            'order_num': 'nunique',
            'item_total_without_tax': 'sum',
            'profit_before_ads': 'sum'
        }).reset_index()
        city_agg.columns = ['city', 'country', 'orders', 'revenue', 'profit']
        city_agg = city_agg.sort_values('revenue', ascending=False).head(20)
        city_agg['revenue_pct'] = (city_agg['revenue'] / df['item_total_without_tax'].sum() * 100).round(1)

        print(f"Geographic analysis complete: {len(country_agg)} countries, showing top 20 cities")
        return country_agg, city_agg

    def analyze_geo_profitability(self, df: pd.DataFrame, fb_campaigns: list = None) -> dict:
        """
        Analyze SK/CZ/HU profitability with estimated FB spend attribution by campaign name.
        Returns country-level contribution margin and FB CPO.
        """
        print("\nAnalyzing geo profitability (SK/CZ/HU)...")

        # Build one row per order with country + order economics.
        # Prefer delivery country, fallback to invoice country if missing.
        geo_df = df.copy()
        if 'delivery_country' in geo_df.columns:
            geo_df['geo_country'] = geo_df['delivery_country']
        else:
            geo_df['geo_country'] = None
        geo_df['geo_country'] = geo_df['geo_country'].replace('', np.nan)
        if 'invoice_country' in geo_df.columns:
            geo_df['geo_country'] = geo_df['geo_country'].fillna(geo_df['invoice_country'])

        order_level = geo_df.groupby('order_num').agg({
            'geo_country': 'first',
            'item_total_without_tax': 'sum',
            'total_expense': 'sum'
        }).reset_index()
        order_level.columns = ['order_num', 'country', 'revenue', 'product_cost']
        order_level['country'] = order_level['country'].fillna('unknown').astype(str).str.lower().str.strip()

        # Normalize common country aliases.
        alias_map = {
            'slovakia': 'sk',
            'slovensko': 'sk',
            'czech republic': 'cz',
            'cesko': 'cz',
            'česko': 'cz',
            'hungary': 'hu',
            'madarsko': 'hu',
            'maďarsko': 'hu',
        }
        order_level['country'] = order_level['country'].replace(alias_map)
        order_level = order_level[order_level['country'].isin(['sk', 'cz', 'hu'])]

        if order_level.empty:
            return {
                'table': pd.DataFrame(),
                'fb_spend_by_country': {'sk': 0.0, 'cz': 0.0, 'hu': 0.0},
                'fb_spend_unattributed': 0.0
            }

        geo = order_level.groupby('country').agg({
            'order_num': 'nunique',
            'revenue': 'sum',
            'product_cost': 'sum'
        }).reset_index()
        geo.columns = ['country', 'orders', 'revenue', 'product_cost']

        geo['packaging_cost'] = geo['orders'] * PACKAGING_COST_PER_ORDER
        geo['shipping_subsidy_cost'] = geo['orders'] * SHIPPING_SUBSIDY_PER_ORDER

        fb_spend_by_country = {'sk': 0.0, 'cz': 0.0, 'hu': 0.0}
        fb_spend_unattributed = 0.0

        def infer_campaign_country(campaign_name: str) -> Optional[str]:
            name = str(campaign_name or '').upper()
            if re.search(r'(^|[^A-Z])SK([^A-Z]|$)', name) or 'SLOVAK' in name:
                return 'sk'
            if re.search(r'(^|[^A-Z])CZ([^A-Z]|$)', name) or 'CZECH' in name:
                return 'cz'
            if re.search(r'(^|[^A-Z])HU([^A-Z]|$)', name) or 'HUNGAR' in name:
                return 'hu'
            return None

        for campaign in (fb_campaigns or []):
            spend = float(campaign.get('spend', 0) or 0)
            country = infer_campaign_country(campaign.get('campaign_name', ''))
            if country and country in fb_spend_by_country:
                fb_spend_by_country[country] += spend
            else:
                fb_spend_unattributed += spend

        geo['fb_ads_spend'] = geo['country'].map(fb_spend_by_country).fillna(0)
        geo['contribution_cost'] = geo['product_cost'] + geo['packaging_cost'] + geo['shipping_subsidy_cost'] + geo['fb_ads_spend']
        geo['contribution_profit'] = geo['revenue'] - geo['contribution_cost']
        geo['contribution_margin_pct'] = geo.apply(
            lambda row: round((row['contribution_profit'] / row['revenue'] * 100) if row['revenue'] > 0 else 0, 2),
            axis=1
        )
        geo['fb_cpo'] = geo.apply(
            lambda row: round((row['fb_ads_spend'] / row['orders']) if row['orders'] > 0 else 0, 2),
            axis=1
        )
        geo['avg_order_value'] = geo.apply(
            lambda row: round((row['revenue'] / row['orders']) if row['orders'] > 0 else 0, 2),
            axis=1
        )

        # Round financial values for display.
        for col in ['revenue', 'product_cost', 'packaging_cost', 'shipping_subsidy_cost', 'fb_ads_spend', 'contribution_cost', 'contribution_profit']:
            geo[col] = geo[col].round(2)

        geo = geo.sort_values('revenue', ascending=False).reset_index(drop=True)
        print(f"Geo profitability complete: {len(geo)} countries, unattributed FB spend=€{fb_spend_unattributed:.2f}")

        return {
            'table': geo,
            'fb_spend_by_country': {k: round(v, 2) for k, v in fb_spend_by_country.items()},
            'fb_spend_unattributed': round(fb_spend_unattributed, 2)
        }

    def analyze_b2b_vs_b2c(self, df: pd.DataFrame) -> pd.DataFrame:
        """Analyze B2B vs B2C orders"""
        print("\nAnalyzing B2B vs B2C split...")

        # B2B = has company VAT ID or company ID
        df['is_b2b'] = df.apply(
            lambda row: pd.notna(row.get('customer_vat_id')) and str(row.get('customer_vat_id', '')).strip() != ''
                        or pd.notna(row.get('customer_company_id')) and str(row.get('customer_company_id', '')).strip() != '',
            axis=1
        )

        b2b_agg = df.groupby('is_b2b').agg({
            'order_num': 'nunique',
            'item_total_without_tax': 'sum',
            'profit_before_ads': 'sum',
            'customer_email': 'nunique'
        }).reset_index()

        b2b_agg.columns = ['is_b2b', 'orders', 'revenue', 'profit', 'unique_customers']
        b2b_agg['customer_type'] = b2b_agg['is_b2b'].map({True: 'B2B (Companies)', False: 'B2C (Individuals)'})
        b2b_agg['aov'] = (b2b_agg['revenue'] / b2b_agg['orders']).round(2)
        b2b_agg['orders_pct'] = (b2b_agg['orders'] / b2b_agg['orders'].sum() * 100).round(1)
        b2b_agg['revenue_pct'] = (b2b_agg['revenue'] / b2b_agg['revenue'].sum() * 100).round(1)

        print(f"B2B vs B2C analysis complete")
        return b2b_agg

    def analyze_product_margins(self, df: pd.DataFrame) -> pd.DataFrame:
        """Analyze profit margins by product (grouped by product_sku)"""
        print("\nAnalyzing product margins...")

        product_margins = df.groupby('product_sku').agg({
            'item_label': 'first',  # Keep product name for display
            'item_quantity': 'sum',
            'item_total_without_tax': 'sum',
            'total_expense': 'sum',
            'profit_before_ads': 'sum',
            'order_num': 'nunique'
        }).reset_index()

        product_margins.columns = ['sku', 'product', 'quantity', 'revenue', 'cost', 'profit', 'orders']
        product_margins['margin_pct'] = ((product_margins['profit'] / product_margins['revenue']) * 100).round(1)
        product_margins['margin_pct'] = product_margins['margin_pct'].fillna(0)
        product_margins = product_margins.sort_values('margin_pct', ascending=False)

        print(f"Product margin analysis complete: {len(product_margins)} products")
        return product_margins

    def analyze_product_trends(self, df: pd.DataFrame) -> pd.DataFrame:
        """Analyze product sales trends (growing vs declining) - grouped by product_sku"""
        print("\nAnalyzing product trends...")

        df['week'] = pd.to_datetime(df['purchase_date']).dt.isocalendar().week
        df['year_week'] = pd.to_datetime(df['purchase_date']).dt.strftime('%Y-W%W')

        # Get first and last half of the period
        all_weeks = df['year_week'].unique()
        if len(all_weeks) < 4:
            print("Not enough weeks for trend analysis")
            return pd.DataFrame()

        mid_point = len(all_weeks) // 2
        first_half_weeks = all_weeks[:mid_point]
        second_half_weeks = all_weeks[mid_point:]

        # Aggregate by product_sku for each half
        first_half = df[df['year_week'].isin(first_half_weeks)].groupby('product_sku').agg({
            'item_label': 'first',  # Keep product name for display
            'item_quantity': 'sum',
            'item_total_without_tax': 'sum'
        }).reset_index()
        first_half.columns = ['sku', 'product', 'qty_first', 'revenue_first']

        second_half = df[df['year_week'].isin(second_half_weeks)].groupby('product_sku').agg({
            'item_label': 'first',  # Keep product name for display
            'item_quantity': 'sum',
            'item_total_without_tax': 'sum'
        }).reset_index()
        second_half.columns = ['sku', 'product', 'qty_second', 'revenue_second']

        # Merge and calculate growth - merge on sku
        trends = first_half.merge(second_half, on='sku', how='outer', suffixes=('', '_r'))
        # Use product name from whichever half has data
        trends['product'] = trends['product'].combine_first(trends['product_r'])
        trends = trends.drop(columns=['product_r'], errors='ignore')
        trends = trends.fillna(0)
        trends['qty_growth_pct'] = ((trends['qty_second'] - trends['qty_first']) / trends['qty_first'].replace(0, 1) * 100).round(1)
        trends['revenue_growth_pct'] = ((trends['revenue_second'] - trends['revenue_first']) / trends['revenue_first'].replace(0, 1) * 100).round(1)

        # Classify trend
        def classify_trend(row):
            if row['qty_first'] == 0 and row['qty_second'] > 0:
                return 'New'
            elif row['qty_second'] == 0 and row['qty_first'] > 0:
                return 'Discontinued'
            elif row['qty_growth_pct'] > 20:
                return 'Growing'
            elif row['qty_growth_pct'] < -20:
                return 'Declining'
            else:
                return 'Stable'

        trends['trend'] = trends.apply(classify_trend, axis=1)
        trends['total_qty'] = trends['qty_first'] + trends['qty_second']
        trends['total_revenue'] = trends['revenue_first'] + trends['revenue_second']
        trends = trends.sort_values('total_revenue', ascending=False)

        print(f"Product trends analysis complete: {len(trends)} products")
        return trends

    def analyze_customer_concentration(self, df: pd.DataFrame) -> dict:
        """Analyze customer concentration (top customers % of revenue)"""
        print("\nAnalyzing customer concentration...")

        customer_revenue = df.groupby('customer_email').agg({
            'order_num': 'nunique',
            'item_total_without_tax': 'sum',
            'profit_before_ads': 'sum'
        }).reset_index()
        customer_revenue.columns = ['customer', 'orders', 'revenue', 'profit']
        customer_revenue = customer_revenue.sort_values('revenue', ascending=False)

        total_revenue = customer_revenue['revenue'].sum()
        total_customers = len(customer_revenue)

        # Calculate concentration metrics for 10%, 20%, 30%, 40%, 50% of customers
        concentration_levels = [10, 20, 30, 40, 50]
        level_counts = {}
        level_revenue = {}
        level_revenue_share = {}

        for level in concentration_levels:
            count = max(1, int(total_customers * level / 100))
            level_counts[level] = count
            level_revenue[level] = customer_revenue.head(count)['revenue'].sum()
            level_revenue_share[level] = round(level_revenue[level] / total_revenue * 100, 1) if total_revenue > 0 else 0

        # Top 10 customers by revenue (absolute, not percentage)
        top_10_customers = customer_revenue.head(10).copy()
        top_10_customers['revenue_pct'] = (top_10_customers['revenue'] / total_revenue * 100).round(1)

        concentration = {
            'total_customers': total_customers,
            'level_counts': level_counts,
            'level_revenue': level_revenue,
            'level_revenue_share': level_revenue_share,
            # Keep backward compatibility
            'top_10_pct_revenue_share': level_revenue_share.get(10, 0),
            'top_20_pct_revenue_share': level_revenue_share.get(20, 0),
            'top_10_customers': top_10_customers,
            'avg_revenue_per_customer': round(total_revenue / total_customers, 2) if total_customers > 0 else 0,
            'median_revenue_per_customer': round(customer_revenue['revenue'].median(), 2)
        }

        # Repeat purchase rate
        repeat_customers = len(customer_revenue[customer_revenue['orders'] > 1])
        concentration['repeat_purchase_rate'] = round(repeat_customers / total_customers * 100, 1) if total_customers > 0 else 0
        concentration['repeat_customers'] = repeat_customers
        concentration['one_time_customers'] = total_customers - repeat_customers

        print(f"Customer concentration analysis complete: {total_customers} customers")
        return concentration

    def calculate_financial_metrics(self, df: pd.DataFrame, date_agg: pd.DataFrame, clv_return_time_analysis: pd.DataFrame = None) -> dict:
        """Calculate additional financial metrics"""
        print("\nCalculating financial metrics...")
        revenue_col = 'order_revenue_net' if 'order_revenue_net' in df.columns else 'order_total'

        # Use date_agg to keep one consistent source of truth for summary metrics
        # and avoid ad-spend duplication from item-level rows.
        total_revenue = date_agg['total_revenue'].sum()
        total_orders = date_agg['unique_orders'].sum() if 'unique_orders' in date_agg.columns else df['order_num'].nunique()
        total_customers = df['customer_email'].nunique()
        total_fb_spend = date_agg['fb_ads_spend'].sum() if 'fb_ads_spend' in date_agg.columns else 0
        total_google_spend = date_agg['google_ads_spend'].sum() if 'google_ads_spend' in date_agg.columns else 0
        total_ad_spend = total_fb_spend + total_google_spend
        total_product_cost = date_agg['product_expense'].sum() if 'product_expense' in date_agg.columns else df['total_expense'].sum()
        total_packaging_cost = date_agg['packaging_cost'].sum() if 'packaging_cost' in date_agg.columns else 0
        total_shipping_subsidy = date_agg['shipping_subsidy_cost'].sum() if 'shipping_subsidy_cost' in date_agg.columns else 0
        total_fixed_overhead = date_agg['fixed_daily_cost'].sum() if 'fixed_daily_cost' in date_agg.columns else 0
        total_company_cost = date_agg['total_cost'].sum() if 'total_cost' in date_agg.columns else (total_product_cost + total_ad_spend + total_packaging_cost + total_shipping_subsidy + total_fixed_overhead)
        total_company_profit = date_agg['net_profit'].sum() if 'net_profit' in date_agg.columns else (df['profit_before_ads'].sum() - total_fb_spend - total_google_spend)
        total_contribution_cost = date_agg['contribution_cost'].sum() if 'contribution_cost' in date_agg.columns else (total_product_cost + total_packaging_cost + total_shipping_subsidy + total_ad_spend)
        total_contribution_profit = date_agg['contribution_profit'].sum() if 'contribution_profit' in date_agg.columns else (total_revenue - total_contribution_cost)
        # Break-even CAC is based on contribution before ad spend:
        # Revenue - Product Cost - Packaging - Shipping subsidy (fixed overhead excluded by design).
        total_pre_ad_contribution = total_revenue - total_product_cost - total_packaging_cost - total_shipping_subsidy
        pre_ad_contribution_per_order = (total_pre_ad_contribution / total_orders) if total_orders > 0 else 0
        pre_ad_contribution_per_customer = (total_pre_ad_contribution / total_customers) if total_customers > 0 else 0

        total_new_customers = 0
        if clv_return_time_analysis is not None and not clv_return_time_analysis.empty and 'new_customers' in clv_return_time_analysis.columns:
            total_new_customers = clv_return_time_analysis['new_customers'].sum()
        current_fb_cac = (total_fb_spend / total_new_customers) if total_new_customers > 0 else 0
        blended_cac = (total_ad_spend / total_new_customers) if total_new_customers > 0 else 0
        # Keep units aligned: CAC is per acquired customer, so break-even must also be per customer.
        break_even_cac = pre_ad_contribution_per_customer
        break_even_cac_order_based = pre_ad_contribution_per_order
        cac_headroom = break_even_cac - current_fb_cac
        cac_headroom_pct = (cac_headroom / break_even_cac * 100) if break_even_cac != 0 else 0
        contribution_ltv_cac = (pre_ad_contribution_per_customer / current_fb_cac) if current_fb_cac > 0 else 0
        avg_return_cycle_days = None
        if clv_return_time_analysis is not None and not clv_return_time_analysis.empty and 'avg_return_time_days' in clv_return_time_analysis.columns:
            valid_return_days = clv_return_time_analysis['avg_return_time_days'].dropna()
            if not valid_return_days.empty:
                avg_return_cycle_days = float(valid_return_days.mean())

        # Estimated payback period:
        # orders = current FB CAC / pre-ad contribution per order
        # days = max(orders - 1, 0) * avg return cycle (if available)
        if pre_ad_contribution_per_order > 0:
            payback_orders = (current_fb_cac / pre_ad_contribution_per_order) if current_fb_cac > 0 else 0.0
        else:
            payback_orders = None

        if payback_orders is not None and avg_return_cycle_days is not None:
            payback_days_estimated = max(payback_orders - 1, 0) * avg_return_cycle_days
        else:
            payback_days_estimated = None

        # Optional investor-style lens: how many orders to recover CAC
        # after ad spend is already included in per-order contribution.
        post_ad_contribution_per_order = (total_contribution_profit / total_orders) if total_orders > 0 else 0
        if post_ad_contribution_per_order > 0:
            post_ad_payback_orders = (current_fb_cac / post_ad_contribution_per_order) if current_fb_cac > 0 else 0.0
        else:
            post_ad_payback_orders = None

        if post_ad_payback_orders is not None and avg_return_cycle_days is not None:
            post_ad_payback_days_estimated = max(post_ad_payback_orders - 1, 0) * avg_return_cycle_days
        else:
            post_ad_payback_days_estimated = None

        payback_weekly_orders = []
        payback_weekly_labels = []
        if clv_return_time_analysis is not None and not clv_return_time_analysis.empty and 'cac' in clv_return_time_analysis.columns:
            for _, row in clv_return_time_analysis.iterrows():
                label = str(row['week_start']) if 'week_start' in clv_return_time_analysis.columns else str(row.get('week', ''))
                cac_week = row.get('cac', 0) or 0
                weekly_payback = (cac_week / pre_ad_contribution_per_order) if pre_ad_contribution_per_order > 0 else 0
                payback_weekly_labels.append(label)
                payback_weekly_orders.append(round(float(weekly_payback), 2))

        # New vs Returning revenue split (order-level, deduplicated)
        orders_df = df[['order_num', 'customer_email', 'purchase_date', revenue_col]].drop_duplicates(subset=['order_num']).copy()
        orders_df['purchase_datetime'] = pd.to_datetime(orders_df['purchase_date'])
        first_purchase_map = orders_df.groupby('customer_email')['purchase_datetime'].min().to_dict()
        orders_df['is_returning'] = orders_df.apply(
            lambda row: row['purchase_datetime'] > first_purchase_map.get(row['customer_email'], row['purchase_datetime']),
            axis=1
        )
        new_revenue = float(orders_df.loc[~orders_df['is_returning'], revenue_col].sum())
        returning_revenue = float(orders_df.loc[orders_df['is_returning'], revenue_col].sum())
        total_split_revenue = new_revenue + returning_revenue
        new_revenue_share_pct = (new_revenue / total_split_revenue * 100) if total_split_revenue > 0 else 0
        returning_revenue_share_pct = (returning_revenue / total_split_revenue * 100) if total_split_revenue > 0 else 0

        metrics = {
            'roas': round(total_revenue / total_ad_spend, 2) if total_ad_spend > 0 else 0,
            'roas_fb': round(total_revenue / total_fb_spend, 2) if total_fb_spend > 0 else 0,
            'roas_google': round(total_revenue / total_google_spend, 2) if total_google_spend > 0 else 0,
            'mer': round(total_revenue / total_ad_spend, 2) if total_ad_spend > 0 else 0,
            'revenue_per_customer': round(total_revenue / total_customers, 2) if total_customers > 0 else 0,
            'orders_per_customer': round(total_orders / total_customers, 2) if total_customers > 0 else 0,
            'cost_per_order': round(total_company_cost / total_orders, 2) if total_orders > 0 else 0,
            'profit_margin_pct': round(total_company_profit / total_revenue * 100, 1) if total_revenue > 0 else 0,  # backward-compatible key (company margin)
            'company_profit_margin_pct': round(total_company_profit / total_revenue * 100, 1) if total_revenue > 0 else 0,
            'product_gross_margin_pct': round((total_revenue - total_product_cost) / total_revenue * 100, 1) if total_revenue > 0 else 0,
            'pre_ad_contribution_margin_pct': round(total_pre_ad_contribution / total_revenue * 100, 1) if total_revenue > 0 else 0,
            'contribution_margin_pct': round(total_contribution_profit / total_revenue * 100, 1) if total_revenue > 0 else 0,
            'post_ad_contribution_margin_pct': round(total_contribution_profit / total_revenue * 100, 1) if total_revenue > 0 else 0,
            'company_net_profit': round(total_company_profit, 2),
            'pre_ad_contribution_profit': round(total_pre_ad_contribution, 2),
            'contribution_profit': round(total_contribution_profit, 2),
            'post_ad_contribution_profit': round(total_contribution_profit, 2),
            'pre_ad_contribution_profit_per_order': round(total_pre_ad_contribution / total_orders, 2) if total_orders > 0 else 0,
            'contribution_profit_per_order': round(total_contribution_profit / total_orders, 2) if total_orders > 0 else 0,
            'post_ad_contribution_profit_per_order': round(total_contribution_profit / total_orders, 2) if total_orders > 0 else 0,
            'pre_ad_contribution_per_order': round(pre_ad_contribution_per_order, 2),
            'pre_ad_contribution_per_customer': round(pre_ad_contribution_per_customer, 2),
            'break_even_cac': round(break_even_cac, 2),
            'break_even_cac_order_based': round(break_even_cac_order_based, 2),
            'current_fb_cac': round(current_fb_cac, 2),
            'paid_cac': round(current_fb_cac, 2),
            'blended_cac': round(blended_cac, 2),
            'blended_cac_scope': 'tracked_ads_fb_google',
            'cac_headroom': round(cac_headroom, 2),
            'cac_headroom_pct': round(cac_headroom_pct, 1),
            'contribution_ltv_cac': round(contribution_ltv_cac, 2),
            'payback_orders': round(payback_orders, 2) if payback_orders is not None else None,
            'payback_days_estimated': round(payback_days_estimated, 1) if payback_days_estimated is not None else None,
            'post_ad_payback_orders': round(post_ad_payback_orders, 2) if post_ad_payback_orders is not None else None,
            'post_ad_payback_days_estimated': round(post_ad_payback_days_estimated, 1) if post_ad_payback_days_estimated is not None else None,
            'payback_days_note': 'estimate_based_on_avg_return_cycle',
            'avg_return_cycle_days': round(avg_return_cycle_days, 1) if avg_return_cycle_days is not None else None,
            'payback_weekly_orders': payback_weekly_orders,
            'payback_weekly_labels': payback_weekly_labels,
            'new_revenue': round(new_revenue, 2),
            'returning_revenue': round(returning_revenue, 2),
            'new_revenue_share_pct': round(new_revenue_share_pct, 1),
            'returning_revenue_share_pct': round(returning_revenue_share_pct, 1),
            'ad_spend_per_order': round(total_ad_spend / total_orders, 2),
            'total_ad_spend': round(total_ad_spend, 2),
            'total_revenue': round(total_revenue, 2),
            'total_packaging_cost': round(total_packaging_cost, 2),
            'total_shipping_subsidy': round(total_shipping_subsidy, 2),
            'total_fixed_overhead': round(total_fixed_overhead, 2),
            'total_new_customers': int(total_new_customers),
            'total_orders': total_orders,
            'total_customers': total_customers
        }

        # Weekly profit margin trend
        if 'net_profit' in date_agg.columns and 'total_revenue' in date_agg.columns:
            date_agg['profit_margin_pct'] = (date_agg['net_profit'] / date_agg['total_revenue'] * 100).round(1)

        print(f"Financial metrics calculated: ROAS={metrics['roas']}x")
        return metrics

    def validate_metric_consistency(self, date_agg: pd.DataFrame, financial_metrics: dict, clv_return_time_analysis: pd.DataFrame = None) -> dict:
        """Validate key metric equations to catch data consistency issues early."""
        checks = {}

        total_revenue = date_agg['total_revenue'].sum() if 'total_revenue' in date_agg.columns else 0
        total_ad_spend = 0
        if 'fb_ads_spend' in date_agg.columns:
            total_ad_spend += date_agg['fb_ads_spend'].sum()
        if 'google_ads_spend' in date_agg.columns:
            total_ad_spend += date_agg['google_ads_spend'].sum()
        total_company_profit = date_agg['net_profit'].sum() if 'net_profit' in date_agg.columns else 0

        roas_expected = (total_revenue / total_ad_spend) if total_ad_spend > 0 else 0
        roas_reported = financial_metrics.get('roas', 0)
        checks['roas_expected'] = round(roas_expected, 4)
        checks['roas_reported'] = round(roas_reported, 4)
        checks['roas_delta'] = round(roas_reported - roas_expected, 4)
        checks['roas_ok'] = abs(checks['roas_delta']) <= 0.01

        margin_expected = (total_company_profit / total_revenue * 100) if total_revenue > 0 else 0
        margin_reported = financial_metrics.get('company_profit_margin_pct', financial_metrics.get('profit_margin_pct', 0))
        checks['company_margin_expected_pct'] = round(margin_expected, 4)
        checks['company_margin_reported_pct'] = round(margin_reported, 4)
        checks['company_margin_delta_pct'] = round(margin_reported - margin_expected, 4)
        checks['company_margin_ok'] = abs(checks['company_margin_delta_pct']) <= 0.05

        if clv_return_time_analysis is not None and not clv_return_time_analysis.empty:
            total_new_customers = clv_return_time_analysis['new_customers'].sum() if 'new_customers' in clv_return_time_analysis.columns else 0
            total_fb_spend = clv_return_time_analysis['fb_ads_spend'].sum() if 'fb_ads_spend' in clv_return_time_analysis.columns else 0
            cac_expected = (total_fb_spend / total_new_customers) if total_new_customers > 0 else 0
            cac_reported = financial_metrics.get('current_fb_cac', cac_expected)
            cac_delta = cac_reported - cac_expected
            total_orders = date_agg['unique_orders'].sum() if 'unique_orders' in date_agg.columns else 0
            cac_if_orders = (total_fb_spend / total_orders) if total_orders > 0 else 0
            checks['cac_expected'] = round(cac_expected, 4)
            checks['cac_reported'] = round(cac_reported, 4)
            checks['cac_delta'] = round(cac_delta, 4)
            checks['cac_formula'] = 'fb_ads_spend / new_customers'
            checks['cac_new_customers'] = int(total_new_customers)
            checks['cac_if_orders_denominator'] = round(cac_if_orders, 4)
            checks['cac_spend_source'] = 'fb_ads_spend'
            checks['cac_ok'] = abs(checks['cac_delta']) <= 0.01
        else:
            checks['cac_expected'] = 0
            checks['cac_reported'] = 0
            checks['cac_delta'] = 0
            checks['cac_formula'] = 'n/a'
            checks['cac_new_customers'] = 0
            checks['cac_if_orders_denominator'] = 0
            checks['cac_spend_source'] = 'n/a'
            checks['cac_ok'] = False

        if not checks['roas_ok']:
            logger.warning(f"Consistency check failed: ROAS mismatch (expected={checks['roas_expected']}, reported={checks['roas_reported']})")
        if not checks['company_margin_ok']:
            logger.warning(
                "Consistency check failed: Company margin mismatch "
                f"(expected={checks['company_margin_expected_pct']}%, reported={checks['company_margin_reported_pct']}%)"
            )

        logger.info(
            "Consistency checks: "
            f"roas_ok={checks['roas_ok']}, "
            f"company_margin_ok={checks['company_margin_ok']}, "
            f"cac_ok={checks['cac_ok']}"
        )

        return checks

    def analyze_order_status(self, df: pd.DataFrame) -> pd.DataFrame:
        """Analyze order status distribution"""
        print("\nAnalyzing order status distribution...")

        status_agg = df.groupby('status_name').agg({
            'order_num': 'nunique',
            'item_total_without_tax': 'sum'
        }).reset_index()
        status_agg.columns = ['status', 'orders', 'revenue']
        status_agg = status_agg.sort_values('orders', ascending=False)
        status_agg['orders_pct'] = (status_agg['orders'] / status_agg['orders'].sum() * 100).round(1)

        print(f"Order status analysis complete: {len(status_agg)} statuses")
        return status_agg

    def analyze_refunds(self, df: pd.DataFrame) -> dict:
        """
        Analyze refund/return rate trend.

        Current source: order statuses (e.g. Vratene/Vraceno/Returned).
        Note: explicit credit-note document type is not exposed in current reporting dataset.
        """
        print("\nAnalyzing refunds/returns...")
        revenue_col = 'order_revenue_net' if 'order_revenue_net' in df.columns else 'order_total'

        orders_df = df[['order_num', 'purchase_date', 'status_name', revenue_col]].drop_duplicates(subset=['order_num']).copy()
        orders_df['purchase_datetime'] = pd.to_datetime(orders_df['purchase_date'])
        orders_df['date'] = orders_df['purchase_datetime'].dt.date
        orders_df['status_name'] = orders_df['status_name'].fillna('').astype(str)

        def normalize_status(status: str) -> str:
            # Normalize to lowercase ASCII to avoid diacritics/encoding mismatches.
            normalized = unicodedata.normalize('NFKD', status)
            return ''.join(ch for ch in normalized if not unicodedata.combining(ch)).lower()

        orders_df['status_name_norm'] = orders_df['status_name'].apply(normalize_status)

        explicit_refund_statuses = {
            'vratene',
            'vraceno',
            'returned',
            'refunded',
        }

        orders_df['is_refund'] = orders_df['status_name_norm'].apply(
            lambda s: (s in explicit_refund_statuses)
            or ('vraten' in s)
            or ('vracen' in s)
            or ('refund' in s)
            or ('dobropis' in s)
        )

        daily_total = orders_df.groupby('date').agg({
            'order_num': 'nunique'
        }).reset_index().rename(columns={'order_num': 'total_orders'})

        daily_refunds = orders_df[orders_df['is_refund']].groupby('date').agg({
            'order_num': 'nunique',
            revenue_col: 'sum'
        }).reset_index().rename(columns={
            'order_num': 'refund_orders',
            revenue_col: 'refund_amount'
        })

        daily = daily_total.merge(daily_refunds, on='date', how='left')
        daily['refund_orders'] = daily['refund_orders'].fillna(0).astype(int)
        daily['refund_amount'] = daily['refund_amount'].fillna(0).round(2)
        daily['refund_rate_pct'] = daily.apply(
            lambda row: round((row['refund_orders'] / row['total_orders'] * 100) if row['total_orders'] > 0 else 0, 2),
            axis=1
        )
        daily = daily.sort_values('date')

        total_orders = int(daily['total_orders'].sum())
        total_refund_orders = int(daily['refund_orders'].sum())
        total_refund_amount = float(daily['refund_amount'].sum())
        refund_rate_pct = round((total_refund_orders / total_orders * 100) if total_orders > 0 else 0, 2)

        summary = {
            'total_orders': total_orders,
            'refund_orders': total_refund_orders,
            'refund_rate_pct': refund_rate_pct,
            'refund_amount': round(total_refund_amount, 2),
            'source': 'order_status'
        }

        print(
            f"Refund analysis complete: {total_refund_orders}/{total_orders} orders "
            f"({refund_rate_pct:.2f}%), amount={total_refund_amount:.2f} EUR"
        )

        return {
            'summary': summary,
            'daily': daily
        }

    def analyze_ads_effectiveness(self, df: pd.DataFrame) -> dict:
        """Analyze relationship between ad spend and orders/revenue"""
        print("\nAnalyzing ads effectiveness...")

        # Convert to date only (remove time component)
        df['purchase_date_only'] = pd.to_datetime(df['purchase_date']).dt.date

        # Daily aggregation for correlation - use date only
        daily_data = df.groupby('purchase_date_only').agg({
            'order_num': 'nunique',
            'item_total_without_tax': 'sum',
            'fb_ads_daily_spend': 'first',
            'google_ads_daily_spend': 'first',
            'profit_before_ads': 'sum'
        }).reset_index()
        daily_data.columns = ['date', 'orders', 'revenue', 'fb_spend', 'google_spend', 'profit']
        daily_data['total_ad_spend'] = daily_data['fb_spend'] + daily_data['google_spend']

        # Calculate correlations
        correlations = {}
        if len(daily_data) > 5:
            correlations['fb_orders'] = round(daily_data['fb_spend'].corr(daily_data['orders']), 3)
            correlations['fb_revenue'] = round(daily_data['fb_spend'].corr(daily_data['revenue']), 3)
            correlations['google_orders'] = round(daily_data['google_spend'].corr(daily_data['orders']), 3)
            correlations['google_revenue'] = round(daily_data['google_spend'].corr(daily_data['revenue']), 3)
            correlations['total_ads_orders'] = round(daily_data['total_ad_spend'].corr(daily_data['orders']), 3)
            correlations['total_ads_revenue'] = round(daily_data['total_ad_spend'].corr(daily_data['revenue']), 3)

        # Calculate optimal spend ranges with 10€ increments
        # Group by spend ranges and calculate average orders/revenue
        max_spend = daily_data['fb_spend'].max()
        # Create bins in 10€ increments up to the max spend
        spend_bins = list(range(0, int(max_spend) + 20, 10))
        spend_labels = [f'{spend_bins[i]}-{spend_bins[i+1]}€' for i in range(len(spend_bins) - 1)]
        daily_data['fb_spend_range'] = pd.cut(daily_data['fb_spend'], bins=spend_bins, labels=spend_labels, include_lowest=True)
        spend_effectiveness = daily_data.groupby('fb_spend_range', observed=True).agg({
            'orders': 'mean',
            'revenue': 'mean',
            'fb_spend': 'mean',
            'profit': 'mean'
        }).reset_index()
        spend_effectiveness.columns = ['spend_range', 'avg_orders', 'avg_revenue', 'avg_spend', 'avg_profit']

        # Calculate ROAS per spend range
        spend_effectiveness['roas'] = (spend_effectiveness['avg_revenue'] / spend_effectiveness['avg_spend']).round(2)
        spend_effectiveness['roas'] = spend_effectiveness['roas'].replace([float('inf'), float('-inf')], 0).fillna(0)

        # Find best performing spend range
        best_roas_range = spend_effectiveness.loc[spend_effectiveness['roas'].idxmax(), 'spend_range'] if not spend_effectiveness.empty else 'N/A'
        best_profit_range = spend_effectiveness.loc[spend_effectiveness['avg_profit'].idxmax(), 'spend_range'] if not spend_effectiveness.empty else 'N/A'

        # Day of week ad effectiveness
        daily_data['day_of_week'] = pd.to_datetime(daily_data['date']).dt.day_name()
        dow_effectiveness = daily_data.groupby('day_of_week').agg({
            'fb_spend': 'mean',
            'orders': 'mean',
            'revenue': 'mean'
        }).reset_index()
        dow_effectiveness['roas'] = (dow_effectiveness['revenue'] / dow_effectiveness['fb_spend']).round(2)
        dow_effectiveness['roas'] = dow_effectiveness['roas'].replace([float('inf'), float('-inf')], 0).fillna(0)

        # Order days by weekday
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        dow_effectiveness['day_order'] = dow_effectiveness['day_of_week'].map({d: i for i, d in enumerate(day_order)})
        dow_effectiveness = dow_effectiveness.sort_values('day_order')

        result = {
            'correlations': correlations,
            'spend_effectiveness': spend_effectiveness,
            'dow_effectiveness': dow_effectiveness,
            'best_roas_range': best_roas_range,
            'best_profit_range': best_profit_range,
            'daily_data': daily_data[['date', 'orders', 'revenue', 'fb_spend', 'google_spend', 'profit']].copy()
        }

        # Recommendations
        recommendations = []
        if correlations.get('fb_orders', 0) > 0.3:
            recommendations.append("Strong positive correlation between FB spend and orders - increasing spend likely effective")
        elif correlations.get('fb_orders', 0) < 0:
            recommendations.append("Negative correlation between FB spend and orders - consider optimizing ad targeting")

        if correlations.get('fb_revenue', 0) > correlations.get('fb_orders', 0):
            recommendations.append("FB ads drive higher value orders - focus on revenue optimization")

        result['recommendations'] = recommendations

        print(f"Ads effectiveness analysis complete. FB-Orders correlation: {correlations.get('fb_orders', 'N/A')}")
        return result

    def analyze_cost_per_order(self, df: pd.DataFrame, fb_campaigns: list = None, reference_total_revenue: float = None) -> dict:
        """
        Analyze estimated Cost Per Order (CPO) using time-based correlation.
        Since we don't have direct attribution, we estimate which campaigns drive orders.

        Args:
            df: Item-level dataframe
            fb_campaigns: Campaign-level Facebook data
            reference_total_revenue: Optional normalized revenue total used across report sections
        """
        print("\nAnalyzing Cost Per Order estimation...")

        # Convert to date only
        df['purchase_date_only'] = pd.to_datetime(df['purchase_date']).dt.date

        # Daily aggregation
        daily_data = df.groupby('purchase_date_only').agg({
            'order_num': 'nunique',
            'item_total_without_tax': 'sum',
            'fb_ads_daily_spend': 'first',
            'google_ads_daily_spend': 'first'
        }).reset_index()
        daily_data.columns = ['date', 'orders', 'revenue', 'fb_spend', 'google_spend']
        daily_data['total_ad_spend'] = daily_data['fb_spend'] + daily_data['google_spend']
        daily_data['date'] = pd.to_datetime(daily_data['date'])

        result = {
            'daily_cpo': [],
            'overall_cpo': 0,
            'fb_cpo': 0,
            'time_lagged_analysis': {},
            'campaign_attribution': [],
            'cpo_trend': [],
            'best_cpo_days': [],
            'worst_cpo_days': []
        }

        total_orders = daily_data['orders'].sum()
        total_fb_spend = daily_data['fb_spend'].sum()
        total_google_spend = daily_data['google_spend'].sum()
        total_ad_spend = total_fb_spend + total_google_spend
        total_revenue_raw = daily_data['revenue'].sum()
        total_revenue = float(reference_total_revenue) if reference_total_revenue is not None else total_revenue_raw

        # Overall CPO
        result['overall_cpo'] = total_ad_spend / total_orders if total_orders > 0 else 0
        result['fb_cpo'] = total_fb_spend / total_orders if total_orders > 0 else 0
        result['google_cpo'] = total_google_spend / total_orders if total_orders > 0 else 0
        result['total_orders'] = total_orders
        result['total_fb_spend'] = total_fb_spend
        result['total_revenue'] = total_revenue
        result['total_revenue_raw'] = round(total_revenue_raw, 2)
        result['total_revenue_source'] = 'financial_metrics.total_revenue' if reference_total_revenue is not None else 'daily_data.revenue'
        result['fb_spend_reconciliation'] = {
            'daily_source_spend': round(total_fb_spend, 2),
            'campaign_source_spend': None,
            'difference': None,
            'difference_pct': None
        }

        # Daily CPO trend
        daily_cpo = []
        for _, row in daily_data.iterrows():
            if row['orders'] > 0:
                cpo = row['fb_spend'] / row['orders']
                daily_cpo.append({
                    'date': row['date'].strftime('%Y-%m-%d'),
                    'orders': int(row['orders']),
                    'fb_spend': row['fb_spend'],
                    'revenue': row['revenue'],
                    'cpo': cpo,
                    'roas': row['revenue'] / row['fb_spend'] if row['fb_spend'] > 0 else 0
                })
        result['daily_cpo'] = daily_cpo

        # Time-lagged correlation analysis (do orders follow spend with delay?)
        if len(daily_data) > 7:
            # Calculate correlation with different time lags
            time_lags = {}
            for lag in range(0, 4):  # 0 to 3 day lag
                if lag == 0:
                    orders_shifted = daily_data['orders']
                else:
                    orders_shifted = daily_data['orders'].shift(-lag)

                valid_mask = ~orders_shifted.isna()
                if valid_mask.sum() > 5:
                    corr = daily_data.loc[valid_mask, 'fb_spend'].corr(orders_shifted[valid_mask])
                    time_lags[f'{lag}_day'] = round(corr, 3) if not pd.isna(corr) else 0

            result['time_lagged_analysis'] = time_lags

            # Find best lag
            if time_lags:
                best_lag = max(time_lags, key=lambda k: time_lags[k])
                result['best_attribution_lag'] = best_lag
                result['best_lag_correlation'] = time_lags[best_lag]

        # Weekly CPO trend (smoother than daily)
        daily_data['week'] = daily_data['date'].dt.isocalendar().week
        daily_data['year'] = daily_data['date'].dt.year
        weekly_data = daily_data.groupby(['year', 'week']).agg({
            'orders': 'sum',
            'fb_spend': 'sum',
            'revenue': 'sum',
            'date': 'min'
        }).reset_index()
        weekly_data['cpo'] = weekly_data['fb_spend'] / weekly_data['orders']
        weekly_data['cpo'] = weekly_data['cpo'].replace([float('inf'), float('-inf')], 0).fillna(0)

        result['weekly_cpo'] = [
            {
                'week_start': row['date'].strftime('%Y-%m-%d'),
                'orders': int(row['orders']),
                'fb_spend': row['fb_spend'],
                'cpo': row['cpo']
            }
            for _, row in weekly_data.iterrows()
        ]

        # Campaign attribution estimation (proportional to clicks/spend)
        if fb_campaigns:
            total_campaign_spend = sum(c.get('spend', 0) for c in fb_campaigns)
            total_campaign_clicks = sum(c.get('clicks', 0) for c in fb_campaigns)
            diff = total_fb_spend - total_campaign_spend
            diff_pct = (diff / total_fb_spend * 100) if total_fb_spend > 0 else 0
            result['fb_spend_reconciliation'] = {
                'daily_source_spend': round(total_fb_spend, 2),
                'campaign_source_spend': round(total_campaign_spend, 2),
                'difference': round(diff, 2),
                'difference_pct': round(diff_pct, 2)
            }

            campaign_attribution = []
            for campaign in fb_campaigns:
                spend = campaign.get('spend', 0)
                clicks = campaign.get('clicks', 0)

                if spend > 0:
                    # Estimate orders proportionally to spend
                    spend_share = spend / total_campaign_spend if total_campaign_spend > 0 else 0
                    estimated_orders_by_spend = total_orders * spend_share

                    # Estimate orders proportionally to clicks
                    click_share = clicks / total_campaign_clicks if total_campaign_clicks > 0 else 0
                    estimated_orders_by_clicks = total_orders * click_share

                    # Weighted average (60% clicks, 40% spend as clicks are better signal)
                    estimated_orders = estimated_orders_by_clicks * 0.6 + estimated_orders_by_spend * 0.4

                    # Calculate estimated CPO for campaign
                    estimated_cpo = spend / estimated_orders if estimated_orders > 0 else 0

                    # Calculate estimated revenue share
                    revenue_share = estimated_orders / total_orders if total_orders > 0 else 0
                    estimated_revenue = total_revenue * revenue_share

                    # ROAS for campaign
                    estimated_roas = estimated_revenue / spend if spend > 0 else 0

                    campaign_attribution.append({
                        'campaign_name': campaign.get('campaign_name', 'Unknown'),
                        'campaign_id': campaign.get('campaign_id', ''),
                        'spend': spend,
                        'clicks': clicks,
                        'impressions': campaign.get('impressions', 0),
                        'ctr': campaign.get('ctr', 0),
                        'cpc': campaign.get('cpc', 0),
                        'estimated_orders': round(estimated_orders, 1),
                        'estimated_cpo': round(estimated_cpo, 2),
                        'estimated_revenue': round(estimated_revenue, 2),
                        'estimated_roas': round(estimated_roas, 2),
                        'spend_share_pct': round(spend_share * 100, 1),
                        'click_share_pct': round(click_share * 100, 1)
                    })

            # Sort by estimated CPO (best first)
            campaign_attribution.sort(key=lambda x: x['estimated_cpo'] if x['estimated_cpo'] > 0 else float('inf'))
            result['campaign_attribution'] = campaign_attribution

        # Find best and worst CPO days
        if daily_cpo:
            sorted_by_cpo = sorted([d for d in daily_cpo if d['cpo'] > 0], key=lambda x: x['cpo'])
            result['best_cpo_days'] = sorted_by_cpo[:5]  # 5 best days
            result['worst_cpo_days'] = sorted_by_cpo[-5:][::-1]  # 5 worst days

        # Hourly order analysis (aggregate orders by hour of day)
        df['purchase_hour'] = pd.to_datetime(df['purchase_date']).dt.hour
        hourly_orders = df.groupby('purchase_hour').agg({
            'order_num': 'nunique',
            'item_total_without_tax': 'sum'
        }).reset_index()
        hourly_orders.columns = ['hour', 'orders', 'revenue']

        result['hourly_orders'] = [
            {
                'hour': int(row['hour']),
                'orders': int(row['orders']),
                'revenue': float(row['revenue'])
            }
            for _, row in hourly_orders.iterrows()
        ]

        print(f"Cost Per Order analysis complete. Overall FB CPO: €{result['fb_cpo']:.2f}")
        return result

    def display_returning_customers_analysis(self, analysis: pd.DataFrame):
        """Display returning customers analysis"""
        print("\n" + "="*120)
        print("RETURNING CUSTOMERS ANALYSIS - WEEKLY AGGREGATION")
        print("="*120)
        
        print(f"\n{'Week':>10} {'Week Start':>12} {'Total Orders':>13} {'New':>8} {'New %':>8} {'Returning':>11} {'Return %':>10} {'Unique Customers':>17}")
        print("-"*120)
        
        total_orders = 0
        total_new = 0
        total_returning = 0
        total_unique = 0
        
        for _, row in analysis.iterrows():
            week_str = str(row['week'])
            week_start = row['week_start'].strftime('%Y-%m-%d')
            print(f"{week_str:>10} {week_start:>12} {row['total_orders']:>13} "
                  f"{row['new_orders']:>8} {row['new_percentage']:>7.1f}% "
                  f"{row['returning_orders']:>11} {row['returning_percentage']:>9.1f}% "
                  f"{row['unique_customers']:>17}")
            
            total_orders += row['total_orders']
            total_new += row['new_orders']
            total_returning += row['returning_orders']
            total_unique += row['unique_customers']
        
        # Calculate overall percentages
        overall_new_pct = (total_new / total_orders * 100) if total_orders > 0 else 0
        overall_returning_pct = (total_returning / total_orders * 100) if total_orders > 0 else 0
        
        print("-"*120)
        print(f"{'TOTAL':>10} {' ':>12} {total_orders:>13} "
              f"{total_new:>8} {overall_new_pct:>7.1f}% "
              f"{total_returning:>11} {overall_returning_pct:>9.1f}% "
              f"{total_unique:>17}")
        
        print("\n")
    
    def display_clv_return_time_analysis(self, analysis: pd.DataFrame):
        """Display CLV and return time analysis"""
        print("\n" + "="*160)
        print("CUSTOMER LIFETIME VALUE, CAC & RETURN TIME ANALYSIS - WEEKLY AGGREGATION")
        print("="*160)
        
        print(f"\n{'Week':>10} {'Week Start':>12} {'Customers':>10} {'New':>8} {'Returning':>10} {'Avg CLV (€)':>12} {'Cumulative CLV (€)':>18} {'CAC (€)':>10} {'Avg Return Days':>16} {'Revenue (€)':>12}")
        print("-"*160)
        
        total_customers = 0
        total_new = 0
        total_returning = 0
        total_revenue = 0
        
        for _, row in analysis.iterrows():
            week_str = str(row['week'])
            week_start = row['week_start'].strftime('%Y-%m-%d')
            return_time = f"{row['avg_return_time_days']:.1f}" if pd.notna(row['avg_return_time_days']) else "N/A"
            cac = row.get('cac', 0)
            
            print(f"{week_str:>10} {week_start:>12} {row['unique_customers']:>10} "
                  f"{row['new_customers']:>8} {row['returning_customers']:>10} "
                  f"{row['avg_clv']:>12.2f} {row['cumulative_avg_clv']:>18.2f} "
                  f"{cac:>10.2f} "
                  f"{return_time:>16} {row['total_revenue']:>12.2f}")
            
            total_customers += row['unique_customers']
            total_new += row['new_customers']
            total_returning += row['returning_customers']
            total_revenue += row['total_revenue']
        
        # Calculate overall averages
        overall_avg_clv = analysis['avg_clv'].mean()
        final_cumulative_clv = analysis['cumulative_avg_clv'].iloc[-1] if not analysis.empty else 0
        overall_avg_return = analysis['avg_return_time_days'].mean()
        return_time_str = f"{overall_avg_return:.1f}" if pd.notna(overall_avg_return) else "N/A"
        
        # Calculate overall CAC
        total_fb_spend = analysis['fb_ads_spend'].sum() if 'fb_ads_spend' in analysis.columns else 0
        overall_cac = total_fb_spend / total_new if total_new > 0 else 0
        
        print("-"*160)
        print(f"{'TOTAL':>10} {' ':>12} {total_customers:>10} "
              f"{total_new:>8} {total_returning:>10} "
              f"{overall_avg_clv:>12.2f} {final_cumulative_clv:>18.2f} "
              f"{overall_cac:>10.2f} "
              f"{return_time_str:>16} {total_revenue:>12.2f}")
        
        print("\n")

    def analyze_customer_email_segments(self, df: pd.DataFrame, all_orders_raw: list = None) -> dict:
        """
        Analyze customers and segment them for email marketing campaigns.

        Segments:
        1. One-time buyers (inactive 30+ days): Bought once with status "Odoslaná",
           order was at least 30 days ago
        2. Repeat buyers (inactive 90+ days): Bought 2+ times with status "Odoslaná",
           last order was 90+ days ago
        3. Failed payment customers: All orders have status "Platba online - platnosť vypršala"
           or "Platba online - platba zamietnutá"
        4. Additional segments discovered from data patterns

        Returns dict with DataFrames for each segment
        """
        print("\nAnalyzing customer segments for email marketing...")
        revenue_col = 'order_revenue_net' if 'order_revenue_net' in df.columns else 'order_total'

        today = datetime.now()

        # Convert purchase_date to datetime if not already
        df['purchase_datetime'] = pd.to_datetime(df['purchase_date'])

        # Get unique orders with customer info
        orders_df = df[['order_num', 'customer_email', 'customer_name', 'purchase_datetime',
                        'status_name', revenue_col, 'invoice_city', 'invoice_country']].drop_duplicates(subset=['order_num'])

        # Filter to only "Odoslaná" (shipped) orders for segments 1 and 2
        shipped_orders = orders_df[orders_df['status_name'] == 'Odoslaná'].copy()

        # Calculate per-customer stats from shipped orders
        customer_stats = shipped_orders.groupby('customer_email').agg({
            'order_num': 'count',
            'purchase_datetime': ['min', 'max'],
            revenue_col: 'sum',
            'customer_name': 'first',
            'invoice_city': 'first',
            'invoice_country': 'first'
        }).reset_index()
        customer_stats.columns = ['email', 'order_count', 'first_order_date', 'last_order_date',
                                   'total_revenue', 'name', 'city', 'country']

        # Calculate days since last order
        customer_stats['days_since_last_order'] = (today - customer_stats['last_order_date']).dt.days
        customer_stats['days_since_first_order'] = (today - customer_stats['first_order_date']).dt.days

        segments = {}

        # ==== SEGMENT 1: One-time buyers inactive 30+ days ====
        # Customers with exactly 1 order, first order was 30+ days ago
        one_time_inactive = customer_stats[
            (customer_stats['order_count'] == 1) &
            (customer_stats['days_since_first_order'] >= 30)
        ].copy()
        one_time_inactive = one_time_inactive.sort_values('days_since_first_order', ascending=False)
        segments['one_time_buyers_30_days'] = {
            'data': one_time_inactive,
            'description': 'Zákazníci, ktorí nakúpili raz (objednávka "Odoslaná") a od objednávky uplynulo viac ako 30 dní',
            'description_en': 'Customers who bought once (status "Shipped") and order was 30+ days ago',
            'count': len(one_time_inactive),
            'email_purpose': 'Re-engagement - motivácia k druhému nákupu',
            'send_timing': '30-45 dní po prvej objednávke',
            'send_timing_en': '30-45 days after first order',
            'priority': 3,
            'discount_suggestion': '15% na druhú objednávku',
            'email_template': 'Chýbate nám! Tu je 15% zľava na Vašu ďalšiu objednávku.'
        }
        print(f"  Segment 1 (One-time buyers, 30+ days inactive): {len(one_time_inactive)} customers")

        # ==== SEGMENT 2: Repeat buyers inactive 90+ days ====
        # Customers with 2+ orders, last order was 90+ days ago
        repeat_inactive = customer_stats[
            (customer_stats['order_count'] >= 2) &
            (customer_stats['days_since_last_order'] >= 90)
        ].copy()
        repeat_inactive = repeat_inactive.sort_values('days_since_last_order', ascending=False)
        segments['repeat_buyers_90_days'] = {
            'data': repeat_inactive,
            'description': 'Zákazníci, ktorí nakúpili 2x a viac (objednávky "Odoslaná") ale posledná objednávka bola pred 90+ dňami',
            'description_en': 'Customers who bought 2+ times (status "Shipped") but last order was 90+ days ago',
            'count': len(repeat_inactive),
            'email_purpose': 'Win-back - návrat verných zákazníkov',
            'send_timing': 'Ihneď - sú v riziku odchodu',
            'send_timing_en': 'Immediately - at risk of churning',
            'priority': 2,
            'discount_suggestion': '20% + doprava zadarmo',
            'email_template': 'Váš obľúbený parfum čaká! Špeciálna ponuka pre verných zákazníkov.'
        }
        print(f"  Segment 2 (Repeat buyers, 90+ days inactive): {len(repeat_inactive)} customers")

        # ==== SEGMENT 3: Failed payment customers ====
        # Process raw orders to find customers with ONLY failed payments
        failed_payment_customers = pd.DataFrame()

        if all_orders_raw:
            # Extract customer emails from failed payment orders
            failed_statuses = ['Platba online - platnosť vypršala', 'Platba online - platba zamietnutá']

            failed_orders = []
            all_customer_orders = {}  # Track all orders per customer email

            for order in all_orders_raw:
                customer = order.get('customer', {}) or {}
                email = customer.get('email', '')
                status = (order.get('status', {}) or {}).get('name', '')

                if email:
                    if email not in all_customer_orders:
                        all_customer_orders[email] = {'failed': 0, 'other': 0, 'orders': []}

                    if status in failed_statuses:
                        all_customer_orders[email]['failed'] += 1
                        all_customer_orders[email]['orders'].append(order)
                    else:
                        all_customer_orders[email]['other'] += 1

            # Find customers with ONLY failed orders (no successful ones)
            failed_only_customers = []
            for email, data in all_customer_orders.items():
                if data['failed'] > 0 and data['other'] == 0:
                    # Get the latest order for customer info
                    latest_order = max(data['orders'], key=lambda x: x.get('pur_date', ''))
                    customer = latest_order.get('customer', {}) or {}
                    name = customer.get('company_name', '')
                    if not name:
                        name = f"{customer.get('name', '')} {customer.get('surname', '')}".strip()

                    failed_only_customers.append({
                        'email': email,
                        'name': name,
                        'failed_order_count': data['failed'],
                        'last_attempt_date': latest_order.get('pur_date', ''),
                        'city': (latest_order.get('invoice_address', {}) or {}).get('city', ''),
                        'country': (latest_order.get('invoice_address', {}) or {}).get('country', '')
                    })

            if failed_only_customers:
                failed_payment_customers = pd.DataFrame(failed_only_customers)
                failed_payment_customers['last_attempt_date'] = pd.to_datetime(failed_payment_customers['last_attempt_date'])
                failed_payment_customers = failed_payment_customers.sort_values('last_attempt_date', ascending=False)

        segments['failed_payment_only'] = {
            'data': failed_payment_customers,
            'description': 'Zákazníci, ktorí nedokončili žiadnu objednávku - všetky ich objednávky majú stav "Platba online - platnosť vypršala" alebo "Platba online - platba zamietnutá"',
            'description_en': 'Customers who never completed any order - all their orders have failed payment status',
            'count': len(failed_payment_customers),
            'email_purpose': 'Recovery - pomoc s dokončením objednávky',
            'send_timing': '24-48 hodín po neúspešnej platbe',
            'send_timing_en': '24-48 hours after failed payment',
            'priority': 1,
            'discount_suggestion': '10% + pomoc s platbou',
            'email_template': 'Vaša objednávka čaká! Pomôžeme Vám dokončiť nákup.'
        }
        print(f"  Segment 3 (Failed payment only): {len(failed_payment_customers)} customers")

        # ==== ADDITIONAL SEGMENTS ====

        # Segment 4: High-value one-time buyers (spent above average, haven't returned)
        avg_order_value = customer_stats['total_revenue'].mean()
        high_value_one_time = customer_stats[
            (customer_stats['order_count'] == 1) &
            (customer_stats['total_revenue'] > avg_order_value) &
            (customer_stats['days_since_first_order'] >= 14)  # At least 2 weeks ago
        ].copy()
        high_value_one_time = high_value_one_time.sort_values('total_revenue', ascending=False)
        segments['high_value_one_time'] = {
            'data': high_value_one_time,
            'description': f'Zákazníci s jednou objednávkou nad priemernou hodnotu (€{avg_order_value:.2f}), ktorí sa nevrátili',
            'description_en': f'One-time buyers who spent above average (€{avg_order_value:.2f}) but never returned',
            'count': len(high_value_one_time),
            'email_purpose': 'VIP re-engagement - osobnejší prístup k hodnotným zákazníkom',
            'send_timing': '14-21 dní po prvej objednávke',
            'send_timing_en': '14-21 days after first order',
            'priority': 2,
            'discount_suggestion': '15% + osobná správa',
            'email_template': 'Ďakujeme za veľkú objednávku! Pripravili sme pre Vás exkluzívnu ponuku.'
        }
        print(f"  Segment 4 (High-value one-time): {len(high_value_one_time)} customers")

        # Segment 5: Recent buyers who might need refill (bought 14-60 days ago)
        recent_buyers = customer_stats[
            (customer_stats['days_since_last_order'] >= 14) &
            (customer_stats['days_since_last_order'] <= 60)
        ].copy()
        recent_buyers = recent_buyers.sort_values('days_since_last_order', ascending=True)
        segments['recent_buyers_14_60_days'] = {
            'data': recent_buyers,
            'description': 'Zákazníci, ktorí nakúpili pred 14-60 dňami - ideálny čas na pripomenutie',
            'description_en': 'Customers who bought 14-60 days ago - perfect time for a reminder',
            'count': len(recent_buyers),
            'email_purpose': 'Reminder - pripomenutie produktu, cross-sell',
            'send_timing': 'Segmentovať podľa dní a posielať priebežne',
            'send_timing_en': 'Segment by days and send continuously',
            'priority': 3,
            'discount_suggestion': 'Doprava zadarmo nad X€',
            'email_template': 'Nezabudnite na doplnenie zásob! Máme pre Vás novinky.'
        }
        print(f"  Segment 5 (Recent 14-60 days): {len(recent_buyers)} customers")

        # Segment 6: VIP customers (3+ orders) - for loyalty program
        vip_customers = customer_stats[
            customer_stats['order_count'] >= 3
        ].copy()
        vip_customers = vip_customers.sort_values('total_revenue', ascending=False)
        segments['vip_customers'] = {
            'data': vip_customers,
            'description': 'VIP zákazníci - nakúpili 3x a viac, najvernejší zákazníci',
            'description_en': 'VIP customers - bought 3+ times, most loyal customers',
            'count': len(vip_customers),
            'email_purpose': 'Loyalty - špeciálne ponuky, poďakovanie, program lojality',
            'send_timing': 'Pravidelne 1x mesačne',
            'send_timing_en': 'Regularly once a month',
            'priority': 4,
            'discount_suggestion': 'VIP zľava 15-20%, prednostný prístup k novinkám',
            'email_template': 'Exkluzívne pre VIP: Nová vôňa ešte pred ostatnými!'
        }
        print(f"  Segment 6 (VIP 3+ orders): {len(vip_customers)} customers")

        # Segment 7: Churning customers (2+ orders, last 60-90 days ago)
        churning_customers = customer_stats[
            (customer_stats['order_count'] >= 2) &
            (customer_stats['days_since_last_order'] >= 60) &
            (customer_stats['days_since_last_order'] < 90)
        ].copy()
        churning_customers = churning_customers.sort_values('days_since_last_order', ascending=False)
        segments['churning_customers'] = {
            'data': churning_customers,
            'description': 'Zákazníci v riziku odchodu - nakúpili 2x+, posledná objednávka pred 60-90 dňami',
            'description_en': 'At-risk customers - bought 2+ times, last order 60-90 days ago',
            'count': len(churning_customers),
            'email_purpose': 'Prevention - zabrániť strate zákazníka',
            'send_timing': 'Ihneď - posledná šanca pred stratou',
            'send_timing_en': 'Immediately - last chance before losing them',
            'priority': 1,
            'discount_suggestion': '20% + limitovaná ponuka',
            'email_template': 'Všimli sme si, že dlhšie nenakupujete. Máme pre Vás špeciálnu ponuku!'
        }
        print(f"  Segment 7 (Churning 60-90 days): {len(churning_customers)} customers")

        # Segment 8: Long-term dormant (180+ days since last order)
        long_dormant = customer_stats[
            customer_stats['days_since_last_order'] >= 180
        ].copy()
        long_dormant = long_dormant.sort_values('total_revenue', ascending=False)
        segments['long_dormant'] = {
            'data': long_dormant,
            'description': 'Dlhodobo neaktívni zákazníci - posledná objednávka pred 180+ dňami',
            'description_en': 'Long-term dormant customers - last order 180+ days ago',
            'count': len(long_dormant),
            'email_purpose': 'Re-activation - agresívna zľava alebo špeciálna ponuka',
            'send_timing': 'Ihneď',
            'send_timing_en': 'Immediately',
            'priority': 5,
            'discount_suggestion': '20-30%'
        }
        print(f"  Segment 8 (Long dormant 180+ days): {len(long_dormant)} customers")

        # ==== NEW SEGMENTS BASED ON COHORT ANALYSIS ====

        # Segment 9: Sample buyers who haven't converted (bought sample set, no full-size)
        # Get orders with product info
        orders_with_products = df[['order_num', 'customer_email', 'item_label', 'purchase_datetime']].copy()

        # Identify sample set purchases
        sample_keywords = ['vzor', 'sample', 'sada vzor', 'vzoriek', 'vzorky']
        orders_with_products['is_sample'] = orders_with_products['item_label'].str.lower().apply(
            lambda x: any(kw in str(x).lower() for kw in sample_keywords) if pd.notna(x) else False
        )

        # Full-size products (200ml, 500ml bottles)
        fullsize_keywords = ['200ml', '500ml', '200 ml', '500 ml']
        orders_with_products['is_fullsize'] = orders_with_products['item_label'].str.lower().apply(
            lambda x: any(kw in str(x).lower() for kw in fullsize_keywords) if pd.notna(x) else False
        )

        # Group by customer
        customer_products = orders_with_products.groupby('customer_email').agg({
            'is_sample': 'any',
            'is_fullsize': 'any',
            'purchase_datetime': 'max'
        }).reset_index()
        customer_products.columns = ['email', 'bought_sample', 'bought_fullsize', 'last_order_date']
        customer_products['days_since_last'] = (today - customer_products['last_order_date']).dt.days

        # Sample buyers who never bought full-size, 7-30 days ago
        sample_not_converted = customer_products[
            (customer_products['bought_sample'] == True) &
            (customer_products['bought_fullsize'] == False) &
            (customer_products['days_since_last'] >= 7) &
            (customer_products['days_since_last'] <= 60)
        ].copy()

        # Merge with customer stats for more info
        sample_not_converted = sample_not_converted.merge(
            customer_stats[['email', 'name', 'order_count', 'total_revenue', 'city', 'country']],
            on='email', how='left'
        )
        sample_not_converted = sample_not_converted.sort_values('days_since_last', ascending=True)

        segments['sample_not_converted'] = {
            'data': sample_not_converted,
            'description': 'Zákazníci, ktorí kúpili vzorky ale ešte nekúpili plnú veľkosť (7-60 dní)',
            'description_en': 'Customers who bought samples but never bought full-size products (7-60 days ago)',
            'count': len(sample_not_converted),
            'email_purpose': 'Conversion - konverzia zo vzoriek na plnú veľkosť',
            'send_timing': '7-14 dní po nákupe vzoriek',
            'send_timing_en': '7-14 days after sample purchase',
            'priority': 1,
            'discount_suggestion': '10-15% na prvú plnú veľkosť',
            'email_template': 'Ktorá vôňa sa Vám najviac páčila? Teraz so zľavou X%!'
        }
        print(f"  Segment 9 (Sample not converted): {len(sample_not_converted)} customers")

        # Segment 10: Optimal reorder timing (approaching 20-day avg reorder time)
        # Customers with last order 15-25 days ago (sweet spot for reorder reminder)
        optimal_reorder = customer_stats[
            (customer_stats['days_since_last_order'] >= 15) &
            (customer_stats['days_since_last_order'] <= 25)
        ].copy()
        optimal_reorder = optimal_reorder.sort_values('days_since_last_order', ascending=True)
        segments['optimal_reorder_timing'] = {
            'data': optimal_reorder,
            'description': 'Zákazníci v optimálnom čase na opätovný nákup (15-25 dní od poslednej objednávky)',
            'description_en': 'Customers at optimal reorder timing (15-25 days since last order)',
            'count': len(optimal_reorder),
            'email_purpose': 'Reorder - pripomenutie na doplnenie zásob',
            'send_timing': 'Ihneď (sú v optimálnom okne)',
            'send_timing_en': 'Immediately (within optimal window)',
            'priority': 2,
            'discount_suggestion': '5-10% alebo doprava zadarmo',
            'email_template': 'Dochádza Vám parfum do prania? Objednajte teraz!'
        }
        print(f"  Segment 10 (Optimal reorder 15-25 days): {len(optimal_reorder)} customers")

        # Segment 11: New customers welcome sequence (0-7 days)
        new_customers = customer_stats[
            (customer_stats['order_count'] == 1) &
            (customer_stats['days_since_first_order'] <= 7)
        ].copy()
        new_customers = new_customers.sort_values('first_order_date', ascending=False)
        segments['new_customers_welcome'] = {
            'data': new_customers,
            'description': 'Noví zákazníci - prvá objednávka v posledných 7 dňoch',
            'description_en': 'New customers - first order within last 7 days',
            'count': len(new_customers),
            'email_purpose': 'Welcome - privítanie, tipy na použitie produktu',
            'send_timing': '3 dni po doručení',
            'send_timing_en': '3 days after delivery',
            'priority': 3,
            'discount_suggestion': 'Žiadna zľava, len hodnota',
            'email_template': 'Ako sa Vám páči váš nový parfum? Tipy na použitie...'
        }
        print(f"  Segment 11 (New customers 0-7 days): {len(new_customers)} customers")

        # Segment 12: Second order encouragement (8-14 days, first-timers)
        second_order_timing = customer_stats[
            (customer_stats['order_count'] == 1) &
            (customer_stats['days_since_first_order'] >= 8) &
            (customer_stats['days_since_first_order'] <= 14)
        ].copy()
        second_order_timing = second_order_timing.sort_values('days_since_first_order', ascending=True)
        segments['second_order_encouragement'] = {
            'data': second_order_timing,
            'description': 'Zákazníci pripravení na druhý nákup (8-14 dní po prvej objednávke)',
            'description_en': 'Customers ready for second order (8-14 days after first purchase)',
            'count': len(second_order_timing),
            'email_purpose': 'Second order - motivácia k druhému nákupu',
            'send_timing': '10-12 dní po prvej objednávke',
            'send_timing_en': '10-12 days after first order',
            'priority': 2,
            'discount_suggestion': '10% na druhú objednávku',
            'email_template': 'Páčil sa Vám náš produkt? Získajte 10% na ďalší nákup!'
        }
        print(f"  Segment 12 (Second order timing 8-14 days): {len(second_order_timing)} customers")

        # Save segments to CSV files
        for segment_name, segment_data in segments.items():
            if not segment_data['data'].empty:
                filename = f"data/email_segment_{segment_name}.csv"
                segment_data['data'].to_csv(filename, index=False, encoding='utf-8-sig')
                print(f"    Saved: {filename}")

        print(f"\nCustomer segmentation complete: {len(segments)} segments created")

        return segments


def main():
    """Main function to handle command line arguments and run the export"""
    parser = argparse.ArgumentParser(description='Export orders from BizniWeb API')
    parser.add_argument(
        '--from-date',
        type=str,
        help='From date in YYYY-MM-DD format (default: 2025-05-06)'
    )
    parser.add_argument(
        '--to-date',
        type=str,
        help='To date in YYYY-MM-DD format (default: today)'
    )
    parser.add_argument(
        '--clear-cache',
        action='store_true',
        help='Clear all cached data before running'
    )
    parser.add_argument(
        '--no-cache',
        action='store_true',
        help='Disable cache and fetch all data fresh from API'
    )
    
    args = parser.parse_args()
    
    # Parse dates
    if args.to_date:
        date_to = datetime.strptime(args.to_date, '%Y-%m-%d')
    else:
        date_to = datetime.now()
    
    if args.from_date:
        date_from = datetime.strptime(args.from_date, '%Y-%m-%d')
    else:
        # Default to start from May 11, 2025
        date_from = datetime(2025, 5, 3)

    print(f"Exporting orders from {date_from.strftime('%Y-%m-%d')} to {date_to.strftime('%Y-%m-%d')}")
    
    # Initialize exporter
    exporter = BizniWebExporter(API_URL, API_TOKEN)
    
    # Handle cache options
    if args.clear_cache:
        print("Clearing cache...")
        import shutil
        if exporter.cache_dir.exists():
            shutil.rmtree(exporter.cache_dir)
            exporter.cache_dir.mkdir(parents=True, exist_ok=True)
            print("Cache cleared.")
    
    if args.no_cache:
        print("Cache disabled for this run.")
        exporter.cache_days_threshold = float('inf')  # Never use cache
    
    # Fetch orders
    print("Fetching orders...")
    orders = exporter.fetch_orders(date_from, date_to)
    
    if not orders:
        print("No orders found in the specified date range.")
        return
    
    print(f"Found {len(orders)} orders")
    
    # Export to CSV
    print("Exporting to CSV...")
    filename = exporter.export_to_csv(orders, date_from, date_to)
    
    print(f"Export completed: {filename}")


if __name__ == "__main__":
    main()

