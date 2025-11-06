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

from html_report_generator import generate_html_report

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
FIXED_MONTHLY_COST = 3000.0  # EUR per month (Marek, Uctovnictvo)

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
PRODUCT_EXPENSES = {
    'Sada vzoriek najpredávanejších vôní Vevo 6 x 10ml': 1.79,
    'Sada vzoriek všetkých vôní Vevo 6 x 10ml': 1.79,
    'Sada najpredávanejších vzoriek Vevo 6 x 10ml': 1.79,
    'Sada 6 najpredávanejších vzoriek po 1ks': 1.79,
    'Sada najpredávanejších vzoriek 6 x 10ml': 1.79,
    'Sada vzorků všech vůní Vevo (6 × 10 ml)': 1.79,
    'Sada vzoriek najpredávanejších vôní Vevo 3 x 10ml': 1.79,
    'Parfum do prania Vevo No.08 Cotton Dream (500ml)': 6.53,
    'Vevo No.08 Cotton Dream mosóparfüm (500ml)': 6.53,
    'Parfum do prania Vevo No.07 Ylang Absolute (200ml)': 2.79,
    'Vevo No.07 Ylang Absolute mosóparfüm (200ml)': 2.79,
    'Parfum do prania Vevo No.08 Cotton Dream (200ml)': 3.15,
    'Parfum do prania Vevo No.09 Pure Garden (200ml)': 2.86,
    'Parfum do prania Vevo No.01 Cotton Paradise (500ml)': 7.02,
    'Parfum do prania Vevo No.01 Cotton Paradise (200ml)': 3.35,
    'Parfum do prania Vevo No.09 Pure Garden (500ml)': 5.8,
    'Parfém na praní Vevo No.09 Pure Garden (500ml)': 5.8,
    'Parfum do prania Vevo No.06 Royal Cotton (200ml)': 2.46,
    'Parfum do prania Vevo No.02 Sweet Paradise (200ml)': 4.29,
    'Odmerka Vevo 7ml drevená na parfum do prania': 0.31,
    'Parfum do prania Vevo No.02 Sweet Paradise (500ml)': 9.38,
    'Parfum do prania Vevo No.07 Ylang Absolute (Vzorka 10ml)': 0.28,
    'Parfum do prania Vevo No.07 Ylang Absolute (Vzorka)': 0.28,
    'Parfum do prania Vevo No.06 Royal Cotton (500ml)': 4.79,
    'Parfum do prania Vevo No.08 Cotton Dream (Vzorka 10ml)': 0.29,
    'Parfum do prania Vevo No.08 Cotton Dream (Vzorka)': 0.29,
    'Parfum do prania Vevo No.07 Ylang Absolute (500ml)': 5.64,
    'Parfum do prania Vevo No.09 Pure Garden (Vzorka 10ml)': 0.28,
    'Parfum do prania Vevo No.09 Pure Garden (Vzorka)': 0.28,
    'Parfum do prania Vevo No.02 Sweet Paradise (Vzorka 10ml)': 0.35,
    'Parfum do prania Vevo No.02 Sweet Paradise (Vzorka)': 0.35,
    'Parfum do prania Vevo No.06 Royal Cotton (Vzorka 10ml)': 0.26,
    'Parfum do prania Vevo No.06 Royal Cotton (Vzorka)': 0.26,
    'Parfum do prania Vevo No.03 Lavender Kiss (Vzorka)': 0.26,
    'Tringelt': 0,
    'Parfum do prania Vevo No.01 Cotton Paradise (Vzorka 10ml)': 0.3,
    'Poistenie proti rozbitiu': 0,
    'Vevo Shot - koncentrát na čistenie práčky 100ml': 0.65,
    'Vevo Shot – koncentrát na čištění pračky 100 ml': 0.65,
    'Prací gél hypoalergénny z Marseillského mydla 1L': 2.43,
    'Perkarbonát sodný PLUS 1kg': 2.83
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
        retry_delay = 2
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
        retry_delay = 2

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

            # Determine sort order based on what dates we're fetching
            # If fetching recent dates (within last 7 days), use DESC to get newest first
            # If fetching older dates, use ASC to get oldest first (more efficient for historical data)
            recent_dates = [d for d in dates_to_fetch if (today - d).days <= self.cache_days_threshold]
            sort_order = 'DESC' if recent_dates else 'ASC'

            logger.info(f"Using {sort_order} order: {len(recent_dates)} recent dates, {len(dates_to_fetch) - len(recent_dates)} older dates")

            # Fetch in multiple batches if needed (to cover all requested dates)
            orders_by_date = {}
            batch_num = 1
            max_batches = 5  # Safety limit
            next_cursor = None

            while batch_num <= max_batches:
                print(f"Batch {batch_num} ({sort_order} order)...")
                bulk_orders, next_cursor = self.fetch_all_orders_bulk(
                    max_orders=900,
                    start_cursor=next_cursor,
                    sort_order=sort_order
                )

                if not bulk_orders:
                    break

                # Organize orders by date
                new_dates_found = 0
                for order in bulk_orders:
                    pur_date_str = order.get('pur_date', '')
                    if pur_date_str:
                        try:
                            pur_date = datetime.strptime(pur_date_str.split()[0], '%Y-%m-%d')
                            date_key = pur_date.strftime('%Y-%m-%d')

                            # Only count orders within our date range
                            if any(d.strftime('%Y-%m-%d') == date_key for d in dates_to_fetch):
                                if date_key not in orders_by_date:
                                    orders_by_date[date_key] = []
                                    new_dates_found += 1
                                orders_by_date[date_key].append(order)
                        except (ValueError, IndexError):
                            continue

                # Stop if we've found all dates or no new dates in this batch
                if len(orders_by_date) >= len(dates_to_fetch) or new_dates_found == 0:
                    logger.info(f"Found orders for {len(orders_by_date)}/{len(dates_to_fetch)} requested dates")
                    break

                batch_num += 1
                time.sleep(1)  # Small delay between batches

            # Cache and add orders for each date
            for date in dates_to_fetch:
                date_str = date.strftime('%Y-%m-%d')
                day_orders = orders_by_date.get(date_str, [])
                days_ago = (today - date).days

                # Filter by status
                filtered_orders = self._filter_by_status(day_orders)

                # Validate that all orders are actually from the requested date
                validated_orders = []
                for order in filtered_orders:
                    pur_date_str = order.get('pur_date', '')
                    if pur_date_str:
                        try:
                            order_date = datetime.strptime(pur_date_str.split()[0], '%Y-%m-%d')
                            if order_date.strftime('%Y-%m-%d') == date_str:
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
        out_of_range_count = 0
        for order in all_orders:
            pur_date_str = order.get('pur_date', '')
            if pur_date_str:
                try:
                    order_date = datetime.strptime(pur_date_str.split()[0], '%Y-%m-%d')
                    if date_from <= order_date <= date_to:
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

    def _filter_by_status(self, orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter out orders with excluded statuses"""
        excluded_statuses = [
            'Storno',
            'Platba online - platnosť vypršala',
            'Platba online - platba zamietnutá',
            'Čaká na úhradu',
            'GoPay - platebni metoda potvrzena'
        ]

        filtered_orders = []
        for order in orders:
            status = order.get('status', {}) or {}
            status_name = status.get('name', '')

            if status_name not in excluded_statuses:
                filtered_orders.append(order)

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
        retry_delay = 2
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
                
                # Get expense per item from mapping
                item_label = item.get('item_label', '')
                expense_per_item = PRODUCT_EXPENSES.get(item_label, 0)
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
        
        # Fetch Facebook Ads spend data
        fb_daily_spend = {}
        if self.fb_client.is_configured:
            print("Fetching Facebook Ads spend data...")
            fb_daily_spend = self.fb_client.get_daily_spend(date_from, date_to)
            if fb_daily_spend:
                print(f"Retrieved Facebook Ads data for {len(fb_daily_spend)} days")
        
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
            'item_label', 'item_ean', 'item_quantity', 
            'item_currency', 'item_unit_price_original', 'item_unit_price',
            'item_total_without_tax', 'item_tax_rate', 'item_tax_amount', 'item_total_with_tax',
            'expense_per_item', 'total_expense', 'fb_ads_daily_spend', 'google_ads_daily_spend', 'profit_before_ads', 'roi_before_ads',
            'customer_name', 'customer_email', 'customer_company_id', 'customer_vat_id',
            'order_currency', 'order_total_original', 'order_total',
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
        
        # Create aggregated reports
        date_product_agg, date_agg, items_agg, month_agg = self.create_aggregated_reports(df, date_from, date_to, fb_daily_spend, google_ads_daily_spend)
        
        # Display aggregated data
        self.display_aggregated_data(date_product_agg, date_agg, month_agg)
        
        # Display returning customer analysis
        self.display_returning_customers_analysis(returning_customers_analysis)
        
        # Display CLV and return time analysis
        self.display_clv_return_time_analysis(clv_return_time_analysis)
        
        # Generate HTML report
        print("Generating HTML report...")
        html_content = generate_html_report(date_agg, date_product_agg, items_agg, 
                                           date_from, date_to, fb_daily_spend, google_ads_daily_spend,
                                           returning_customers_analysis, clv_return_time_analysis)
        html_filename = f"data/report_{date_from.strftime('%Y%m%d')}-{date_to.strftime('%Y%m%d')}.html"
        with open(html_filename, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"HTML report saved: {html_filename}")
        
        return filename
    
    def create_aggregated_reports(self, df: pd.DataFrame, date_from: datetime, date_to: datetime, fb_daily_spend: Dict[str, float] = None, google_ads_daily_spend: Dict[str, float] = None):
        """Create aggregated CSV reports"""
        # Convert purchase_date to datetime and extract date only
        if 'purchase_date_only' not in df.columns:
            df['purchase_date_only'] = pd.to_datetime(df['purchase_date']).dt.date
        else:
            df['purchase_date_only'] = pd.to_datetime(df['purchase_date_only']).dt.date
        
        # 1. Group by date and product
        print("Creating date-product aggregation...")
        date_product_agg = df.groupby(['purchase_date_only', 'item_label']).agg({
            'item_quantity': 'sum',
            'item_total_without_tax': 'sum',
            'total_expense': 'sum',
            'profit_before_ads': 'sum',
            'order_num': 'count'
        }).reset_index()
        
        date_product_agg.columns = ['date', 'product_name', 'total_quantity', 'total_revenue', 'product_expense', 'profit', 'order_count']
        
        # Calculate ROI based on product expense only (no FB ads)
        date_product_agg['roi_percent'] = date_product_agg.apply(
            lambda row: round((row['profit'] / row['product_expense'] * 100) if row['product_expense'] > 0 else 0, 2),
            axis=1
        )
        
        # Round financial values
        date_product_agg['total_revenue'] = date_product_agg['total_revenue'].round(2)
        date_product_agg['product_expense'] = date_product_agg['product_expense'].round(2)
        date_product_agg['profit'] = date_product_agg['profit'].round(2)
        
        # Sort by date and product
        date_product_agg = date_product_agg.sort_values(['date', 'product_name'])
        
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
        
        # Add packaging cost (0.3 EUR per unique order)
        date_agg['packaging_cost'] = date_agg['unique_orders'] * PACKAGING_COST_PER_ORDER
        
        # Add daily fixed cost based on the date
        date_agg['fixed_daily_cost'] = date_agg['date'].apply(lambda d: round(self.get_daily_fixed_cost(pd.Timestamp(d)), 2))
        
        # Calculate total cost (product expense + FB ads + Google ads + packaging + fixed daily cost)
        date_agg['total_cost'] = date_agg['product_expense'] + date_agg['fb_ads_spend'] + date_agg['google_ads_spend'] + date_agg['packaging_cost'] + date_agg['fixed_daily_cost']
        
        # Calculate actual profit: Revenue - All Costs
        date_agg['net_profit'] = date_agg['total_revenue'] - date_agg['total_cost']
        
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
        date_agg['total_cost'] = date_agg['total_cost'].round(2)
        date_agg['net_profit'] = date_agg['net_profit'].round(2)
        
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
            'fixed_daily_cost': 'sum',
            'fb_ads_spend': 'sum',
            'google_ads_spend': 'sum',
            'total_cost': 'sum',
            'net_profit': 'sum'
        }).reset_index()
        
        # Calculate ROI for each month
        month_agg['roi_percent'] = month_agg.apply(
            lambda row: round((row['net_profit'] / row['total_cost'] * 100) if row['total_cost'] > 0 else 0, 2),
            axis=1
        )
        
        # Convert month period to string for display
        month_agg['month'] = month_agg['month'].astype(str)
        
        # Save monthly aggregation
        month_filename = f"data/aggregate_by_month_{date_from.strftime('%Y%m%d')}-{date_to.strftime('%Y%m%d')}.csv"
        month_agg.to_csv(month_filename, index=False, encoding='utf-8-sig')
        print(f"Monthly aggregation saved: {month_filename}")
        
        # 3. Group by items only (across all dates)
        print("Creating items aggregation...")
        
        items_agg = df.groupby('item_label').agg({
            'item_quantity': 'sum',
            'item_total_without_tax': 'sum',
            'total_expense': 'sum',
            'profit_before_ads': 'sum',
            'order_num': 'nunique'  # Count unique orders
        }).reset_index()
        
        items_agg.columns = ['product_name', 'total_quantity', 'total_revenue', 'product_expense', 'profit', 'order_count']
        
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
        
        # Return aggregated data for display
        return date_product_agg, date_agg, items_agg, month_agg
    
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
                print("Fixed Costs = Packaging + Fixed Daily Cost | AOV = Avg Order Value | FB/Order = Avg FB Cost per Order")
                print("="*220)
                
                print(f"\n{'Date':<12} {'Orders':>8} {'Items':>8} {'Revenue (€)':>12} {'AOV (€)':>8} {'Product (€)':>12} {'Fixed Costs (€)':>14} {'FB Ads (€)':>12} {'Google Ads (€)':>14} {'Total Cost (€)':>14} {'Profit (€)':>12} {'ROI %':>8}")
                print("-"*240)
                
                month_orders = 0
                month_items = 0
                month_revenue = 0
                month_product_expense = 0
                month_packaging = 0
                month_fixed = 0
                month_fb_ads = 0
                month_google_ads = 0
                month_net_profit = 0
                
                for _, row in month_data.iterrows():
                    date_str = str(row['date'])
                    fixed_costs = row['packaging_cost'] + row['fixed_daily_cost']
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
                    month_fixed += row['fixed_daily_cost']
                    month_fb_ads += row['fb_ads_spend']
                    month_google_ads += google_ads
                    month_net_profit += row['net_profit']
                
                # Monthly total
                month_fixed_costs = month_packaging + month_fixed
                month_cost = month_product_expense + month_packaging + month_fixed + month_fb_ads + month_google_ads
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
            month_total_fixed = 0
            month_total_fb_ads = 0
            month_total_google_ads = 0
            month_total_net_profit = 0
            
            for _, row in month_agg.iterrows():
                month_str = str(row['month'])
                fixed_costs = row['packaging_cost'] + row['fixed_daily_cost']
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
                month_total_fixed += row['fixed_daily_cost']
                month_total_fb_ads += row['fb_ads_spend']
                month_total_google_ads += google_ads
                month_total_net_profit += row['net_profit']
            
            # Calculate total for monthly summary
            month_total_fixed_costs = month_total_packaging + month_total_fixed
            month_total_cost = month_total_product_expense + month_total_packaging + month_total_fixed + month_total_fb_ads + month_total_google_ads
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
        
        # Convert purchase_date to datetime
        df['purchase_datetime'] = pd.to_datetime(df['purchase_date'])
        df['purchase_date_only'] = df['purchase_datetime'].dt.date
        
        # Extract week information
        df['year_week'] = df['purchase_datetime'].dt.to_period('W')
        
        # Get unique customers per order (one row per order)
        orders_df = df[['order_num', 'customer_email', 'purchase_datetime', 'year_week', 'order_total']].drop_duplicates(subset=['order_num'])
        
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
    
    def calculate_clv_and_return_time(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate Customer Lifetime Value and average return time"""
        print("\nCalculating CLV and customer return time...")
        
        # Convert purchase_date to datetime
        df['purchase_datetime'] = pd.to_datetime(df['purchase_date'])
        df['year_week'] = df['purchase_datetime'].dt.to_period('W')
        
        # Get unique orders with customer info and FB ads spend
        orders_df = df[['order_num', 'customer_email', 'purchase_datetime', 'year_week', 'order_total', 'fb_ads_daily_spend']].drop_duplicates(subset=['order_num'])
        orders_df = orders_df.sort_values(['customer_email', 'purchase_datetime'])
        
        # Calculate CLV per customer (total revenue from customer)
        customer_clv = orders_df.groupby('customer_email')['order_total'].sum().to_dict()
        
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
            week_fb_spend = week_orders['fb_ads_daily_spend'].sum()
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
                'total_revenue': week_orders['order_total'].sum(),
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
            week_customer_clv = week_orders.groupby('customer_email')['order_total'].sum()
            cumulative_clv.append(week_customer_clv.mean() if len(week_customer_clv) > 0 else 0)
        
        weekly_clv_df['cumulative_avg_clv'] = cumulative_clv
        weekly_clv_df['cumulative_avg_clv'] = weekly_clv_df['cumulative_avg_clv'].round(2)
        
        # Save to CSV
        filename = f"data/clv_return_time_analysis_{df['purchase_datetime'].min().strftime('%Y%m%d')}-{df['purchase_datetime'].max().strftime('%Y%m%d')}.csv"
        weekly_clv_df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"CLV and return time analysis saved: {filename}")
        
        return weekly_clv_df
    
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


def main():
    """Main function to handle command line arguments and run the export"""
    parser = argparse.ArgumentParser(description='Export orders from BizniWeb API')
    parser.add_argument(
        '--from-date',
        type=str,
        help='From date in YYYY-MM-DD format (default: 2025-05-01)'
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
        # Default to start from June 23, 2025
        date_from = datetime(2025, 7, 23)
    
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