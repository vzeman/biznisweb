#!/usr/bin/env python3
"""
Export orders from BizniWeb GraphQL API to CSV
"""

import os
import csv
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional

from dotenv import load_dotenv
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
import pandas as pd

# Load environment variables
load_dotenv()

# Configuration
API_URL = os.getenv('BIZNISWEB_API_URL', 'https://vevo.flox.sk/api/graphql')
API_TOKEN = os.getenv('BIZNISWEB_API_TOKEN')

if not API_TOKEN:
    raise ValueError("BIZNISWEB_API_TOKEN not found in environment variables. Please set it in .env file.")

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
      delivery_address {
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
      price_elements {
        id
        title
        type
        tax_rate
        value
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
        self.client = Client(transport=transport, fetch_schema_from_transport=True)
    
    def fetch_orders(self, date_from: datetime, date_to: datetime) -> List[Dict[str, Any]]:
        """Fetch all orders within the specified date range"""
        all_orders = []
        has_next_page = True
        cursor = None
        
        while has_next_page:
            variables = {
                'filter': {
                    'pur_date_from': date_from.strftime('%Y-%m-%d'),
                    'pur_date_to': date_to.strftime('%Y-%m-%d')
                },
                'params': {
                    'limit': 30,
                    'order_by': 'pur_date',
                    'sort': 'ASC'
                }
            }
            
            if cursor is not None:
                variables['params']['cursor'] = cursor
            
            try:
                result = self.client.execute(ORDER_QUERY, variable_values=variables)
                orders_data = result.get('getOrderList', {})
                orders = orders_data.get('data', [])
                all_orders.extend(orders)
                
                page_info = orders_data.get('pageInfo', {})
                has_next_page = page_info.get('hasNextPage', False)
                cursor = page_info.get('nextCursor')
                
                print(f"Fetched {len(orders)} orders (total: {len(all_orders)})")
                
            except Exception as e:
                print(f"Error fetching orders: {e}")
                break
        
        # Filter out orders with status "Storno"
        filtered_orders = []
        storno_count = 0
        for order in all_orders:
            status = order.get('status', {}) or {}
            if status.get('name') != 'Storno':
                filtered_orders.append(order)
            else:
                storno_count += 1
        
        if storno_count > 0:
            print(f"Filtered out {storno_count} orders with status 'Storno'")
        
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
            'order_total': order_sum.get('value'),
            'order_total_formatted': order_sum.get('formatted'),
            'order_currency': order_sum.get('currency', {}).get('code') if order_sum.get('currency') else None,
        }
        
        # Create a row for each item
        items = order.get('items', [])
        total_items = len(items)
        
        if items:
            for idx, item in enumerate(items, 1):
                item_price = item.get('price', {}) or {}
                weight = item.get('weight', {}) or {}
                recycle_fee = item.get('recycle_fee', {}) or {}
                
                # Calculate prices with and without tax
                item_price_value = item_price.get('value', 0) or 0
                item_quantity = item.get('quantity', 1) or 1
                item_tax_rate = item.get('tax_rate', 0) or 0
                
                # Calculate total price for this item (price * quantity)
                item_total_with_tax = item_price_value * item_quantity
                
                # Calculate price without tax
                # Price without tax = Price with tax / (1 + tax_rate/100)
                if item_tax_rate > 0:
                    item_total_without_tax = item_total_with_tax / (1 + item_tax_rate / 100)
                    item_tax_amount = item_total_with_tax - item_total_without_tax
                else:
                    item_total_without_tax = item_total_with_tax
                    item_tax_amount = 0
                
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
                    'item_unit_price': item_price_value,
                    'item_total_with_tax': round(item_total_with_tax, 2),
                    'item_total_without_tax': round(item_total_without_tax, 2),
                    'item_tax_amount': round(item_tax_amount, 2),
                    'item_recycle_fee': recycle_fee.get('value'),
                })
                flattened_rows.append(row)
        else:
            # If no items, create one row with order data only
            base_data['total_items_in_order'] = 0
            base_data['item_number'] = None
            flattened_rows.append(base_data)
        
        return flattened_rows
    
    def export_to_csv(self, orders: List[Dict[str, Any]], date_from: datetime, date_to: datetime) -> str:
        """Export orders to CSV file"""
        # Flatten all orders
        all_rows = []
        for order in orders:
            all_rows.extend(self.flatten_order(order))
        
        # Create filename
        filename = f"data/export_{date_from.strftime('%Y%m%d')}-{date_to.strftime('%Y%m%d')}.csv"
        
        # Convert to DataFrame for easier CSV export
        df = pd.DataFrame(all_rows)
        
        # Reorder columns for better readability
        column_order = [
            'order_num', 'order_id', 'external_ref', 'purchase_date', 'status_name',
            'total_items_in_order', 'item_number',
            'item_label', 'item_ean', 'item_quantity', 
            'item_unit_price', 'item_total_without_tax', 'item_tax_rate', 'item_tax_amount', 'item_total_with_tax',
            'customer_name', 'customer_email', 'customer_company_id', 'customer_vat_id',
            'order_total', 'order_currency',
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
        
        # Create aggregated reports
        self.create_aggregated_reports(df, date_from, date_to)
        
        return filename
    
    def create_aggregated_reports(self, df: pd.DataFrame, date_from: datetime, date_to: datetime):
        """Create aggregated CSV reports"""
        # Convert purchase_date to datetime and extract date only
        df['purchase_date_only'] = pd.to_datetime(df['purchase_date']).dt.date
        
        # 1. Group by date and product
        print("Creating date-product aggregation...")
        date_product_agg = df.groupby(['purchase_date_only', 'item_label']).agg({
            'item_quantity': 'sum',
            'item_total_without_tax': 'sum',
            'order_num': 'count'
        }).reset_index()
        
        date_product_agg.columns = ['date', 'product_name', 'total_quantity', 'total_price_without_tax', 'order_count']
        
        # Round financial values
        date_product_agg['total_price_without_tax'] = date_product_agg['total_price_without_tax'].round(2)
        
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
            'order_num': 'nunique',  # Count unique orders
            'item_label': 'count'     # Count total items
        }).reset_index()
        
        date_agg.columns = ['date', 'total_quantity', 'total_revenue_without_tax', 'unique_orders', 'total_items']
        
        # Round financial values
        date_agg['total_revenue_without_tax'] = date_agg['total_revenue_without_tax'].round(2)
        
        # Sort by date
        date_agg = date_agg.sort_values('date')
        
        # Save date aggregation
        date_filename = f"data/aggregate_by_date_{date_from.strftime('%Y%m%d')}-{date_to.strftime('%Y%m%d')}.csv"
        date_agg.to_csv(date_filename, index=False, encoding='utf-8-sig')
        print(f"Date aggregation saved: {date_filename}")


def main():
    """Main function to handle command line arguments and run the export"""
    parser = argparse.ArgumentParser(description='Export orders from BizniWeb API')
    parser.add_argument(
        '--from-date',
        type=str,
        help='From date in YYYY-MM-DD format (default: 30 days ago)'
    )
    parser.add_argument(
        '--to-date',
        type=str,
        help='To date in YYYY-MM-DD format (default: today)'
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
        date_from = date_to - timedelta(days=30)
    
    print(f"Exporting orders from {date_from.strftime('%Y-%m-%d')} to {date_to.strftime('%Y-%m-%d')}")
    
    # Initialize exporter
    exporter = BizniWebExporter(API_URL, API_TOKEN)
    
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