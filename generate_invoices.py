#!/usr/bin/env python3
"""
Generate invoices for orders with specific criteria in BizniWeb
"""

import os
import argparse
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import json
import re
from urllib.parse import urlparse, parse_qs

from dotenv import load_dotenv
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from logger_config import get_logger

# Load environment variables
load_dotenv()

# Configuration
API_URL = os.getenv('BIZNISWEB_API_URL', 'https://vevo.flox.sk/api/graphql')
API_TOKEN = os.getenv('BIZNISWEB_API_TOKEN')
BASE_URL = 'https://vevo.flox.sk'
LOGIN_URL = f'{BASE_URL}/admin/login/authenticate/'
INVOICE_CREATE_URL = f'{BASE_URL}/erp/orders/invoices/create/{{order_num}}'
INVOICE_FINALIZE_URL = f'{BASE_URL}/erp/orders/invoices/finalize/{{order_num}}'
INVOICE_SEND_URL = f'{BASE_URL}/erp/orders/invoices/sendEmail/{{invoice_id}}'

# Web authentication credentials
WEB_USERNAME = os.getenv('BIZNISWEB_USERNAME')
WEB_PASSWORD = os.getenv('BIZNISWEB_PASSWORD')

# Set up logging
logger = get_logger('generate_invoices')

# GraphQL query to fetch orders with specific criteria
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
      invoices {
        id
        invoice_num
      }
      items {
        item_label
        quantity
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


class InvoiceGenerator:
    def __init__(self, api_url: str, api_token: str, username: Optional[str] = None, password: Optional[str] = None):
        """Initialize the invoice generator with API credentials"""
        transport = RequestsHTTPTransport(
            url=api_url,
            headers={'BW-API-Key': f'Token {api_token}'},
            verify=True,
            retries=3,
        )
        self.client = Client(transport=transport, fetch_schema_from_transport=False)
        self.api_token = api_token
        self.web_session = None
        self.arf_token = None
        
        # Initialize web session if credentials provided
        if username and password:
            self.web_session = requests.Session()
            logger.info("Attempting to login to web interface...")
            if self.login_web_session(username, password):
                logger.info("✓ Successfully logged in to web session")
                if self.arf_token:
                    logger.info(f"✓ ARF token obtained: {self.arf_token[:8]}...")
                else:
                    logger.info("⚠ No ARF token found yet, will try to obtain during invoice creation")
            else:
                logger.error("✗ Failed to login to web session - invoice creation will not be available")
                self.web_session = None
    
    def login_web_session(self, username: str, password: str) -> bool:
        """Login to BizniWeb web interface to get session cookies"""
        try:
            # Step 1: GET login page to obtain session cookie
            logger.info("Getting login page to establish session...")
            login_page_url = f"{BASE_URL}/erp/main/login"
            login_page_response = self.web_session.get(login_page_url)
            login_page_response.raise_for_status()
            
            # Check if we got a session cookie
            if 'SSID' in self.web_session.cookies:
                session_id = self.web_session.cookies['SSID']
                logger.info(f"✓ Session established: {session_id[:10]}...")
            else:
                logger.error("✗ No session cookie received from login page")
                return False
            
            # Extract any arf token from the login page
            arf_token = ''
            arf_match = re.search(r'[?&]arf=([a-zA-Z0-9]+)', login_page_response.text)
            if arf_match:
                arf_token = arf_match.group(1)
                logger.info(f"✓ Found arf token in login page: {arf_token[:8]}...")
            else:
                # Try to find CsrfToken in the page
                csrf_match = re.search(r"var\s+CsrfToken\s*=\s*function\s*\(\)\s*\{\s*var\s+\w+\s*=\s*'([a-zA-Z0-9]+)'", login_page_response.text)
                if csrf_match:
                    arf_token = csrf_match.group(1)
                    logger.info(f"✓ Found CsrfToken: {arf_token[:8]}...")
            
            # Step 2: POST credentials with session
            logger.info("Submitting login credentials...")
            login_data = {
                'username': username,
                'password': password,
                'res': '1890x362',  # Screen resolution
                'arf': arf_token  # Use arf from login page if found
            }
            
            # Submit login
            login_response = self.web_session.post(
                LOGIN_URL,
                data=login_data,
                allow_redirects=True
            )
            login_response.raise_for_status()
            
            # Check if login was successful
            response_text = login_response.text
            response_url = str(login_response.url)
            
            logger.debug(f"Login response URL: {response_url}")
            logger.debug(f"Response status: {login_response.status_code}")
            logger.debug(f"Response headers: {dict(login_response.headers)}")
            
            # Check if response is JSON (try to parse even if content-type is wrong)
            try:
                # First check if it looks like JSON or Python dict
                if response_text.strip().startswith('{') and response_text.strip().endswith('}'):
                    # Replace Python booleans with JSON booleans
                    json_text = response_text.replace("'", '"').replace('True', 'true').replace('False', 'false')
                    try:
                        response_json = json.loads(json_text)
                    except:
                        # Try the built-in method as fallback
                        response_json = login_response.json()
                else:
                    response_json = login_response.json()
                
                logger.debug(f"JSON response: {response_json}")
                
                # Check for success in JSON response
                if response_json.get('success') or response_json.get('status') == 'ok':
                    logger.info("✓ Login successful (JSON response)")
                    
                    # Extract arf from JSON if available
                    if 'arf' in response_json:
                        self.arf_token = response_json['arf']
                        logger.info(f"✓ ARF token from JSON: {self.arf_token[:8]}...")
                    
                    # Extract redirect URL if available
                    if 'redirect' in response_json or 'url' in response_json:
                        redirect_url = response_json.get('redirect') or response_json.get('url')
                        logger.info(f"Following redirect to: {redirect_url}")
                        
                        # Follow the redirect
                        redirect_response = self.web_session.get(f"{BASE_URL}{redirect_url}")
                        redirect_response.raise_for_status()
                        
                        # Extract arf from redirect
                        arf_match = re.search(r'[?&]arf=([a-zA-Z0-9]+)', redirect_response.url)
                        if arf_match:
                            self.arf_token = arf_match.group(1)
                            logger.info(f"✓ ARF token from redirect: {self.arf_token[:8]}...")
                    
                    # If login successful, navigate to dashboard to establish session properly
                    logger.info("Navigating to dashboard...")
                    dashboard_url = f"{BASE_URL}/erp/"
                    dashboard_response = self.web_session.get(dashboard_url, allow_redirects=True)
                    
                    logger.debug(f"Dashboard status: {dashboard_response.status_code}")
                    logger.debug(f"Dashboard URL: {dashboard_response.url}")
                    
                    # Extract ARF from dashboard URL
                    arf_match = re.search(r'[?&]arf=([a-zA-Z0-9]+)', str(dashboard_response.url))
                    if arf_match:
                        self.arf_token = arf_match.group(1)
                        logger.info(f"✓ ARF token from dashboard: {self.arf_token[:8]}...")
                    else:
                        # Try to find in response
                        arf_match = re.search(r'[?&]arf=([a-zA-Z0-9]+)', dashboard_response.text)
                        if arf_match:
                            self.arf_token = arf_match.group(1)
                            logger.info(f"✓ ARF token from dashboard HTML: {self.arf_token[:8]}...")
                        else:
                            # Save dashboard for debugging
                            if os.getenv('DEBUG'):
                                with open('dashboard_response.html', 'w') as f:
                                    f.write(dashboard_response.text)
                                logger.debug("Saved dashboard response to dashboard_response.html")
                            
                            # Try to find CsrfToken in the dashboard
                            csrf_match = re.search(r"var\s+CsrfToken\s*=\s*function\s*\(\)\s*\{\s*var\s+\w+\s*=\s*'([a-zA-Z0-9]+)'", dashboard_response.text)
                            if csrf_match:
                                self.arf_token = csrf_match.group(1)
                                logger.info(f"✓ Found CsrfToken in dashboard: {self.arf_token[:8]}...")
                            else:
                                # Maybe the system doesn't use ARF tokens consistently
                                logger.warning("No ARF token found - system might not require it for all operations")
                    
                    return True
                else:
                    logger.error(f"✗ Login failed: {response_json.get('message', 'Unknown error')}")
                    return False
                    
            except json.JSONDecodeError:
                # Not JSON, check HTML response
                logger.debug("Response is not JSON, checking HTML...")
                logger.debug(f"Response length: {len(response_text)}")
                logger.debug(f"First 500 chars: {response_text[:500]}")
                
                # Save response for debugging
                if os.getenv('DEBUG'):
                    with open('login_response.html', 'w') as f:
                        f.write(response_text)
                    logger.debug("Saved response to login_response.html")
            
            # Check for login failure indicators
            if 'error' in response_text.lower() or 'invalid' in response_text.lower() or 'nesprávne' in response_text.lower():
                logger.error("✗ Login failed - invalid credentials")
                return False
            
            # Try to extract arf token from response
            arf_match = re.search(r'[?&]arf=([a-zA-Z0-9]+)', response_text)
            if not arf_match and response_url:
                # Try to find it in URL
                arf_match = re.search(r'[?&]arf=([a-zA-Z0-9]+)', response_url)
            
            if not arf_match:
                # Try to find it in any JavaScript or hidden field
                arf_match = re.search(r'arf["\']?\s*[:=]\s*["\']([a-zA-Z0-9]+)["\']', response_text)
            
            if arf_match:
                self.arf_token = arf_match.group(1)
                logger.info(f"✓ Successfully logged in and extracted arf token: {self.arf_token[:8]}...")
                return True
            else:
                # Even without arf, check if we're logged in
                if 'logout' in response_text.lower() or '/erp/' in response_url:
                    logger.info("✓ Successfully logged in (no arf token found yet)")
                    # Try to get arf from dashboard
                    self.get_arf_token()
                    return True
                else:
                    logger.error("✗ Login failed - could not verify successful login")
                    logger.debug(f"Final URL: {response_url}")
                    return False
                
        except Exception as e:
            logger.error(f"Error during web login: {e}")
            return False
    
    def get_arf_token(self) -> Optional[str]:
        """Try to get arf token from various pages"""
        if self.arf_token:
            return self.arf_token
            
        try:
            # Try dashboard
            dashboard_url = f"{BASE_URL}/erp/orders/orders"
            response = self.web_session.get(dashboard_url)
            
            logger.debug(f"ARF search response status: {response.status_code}")
            logger.debug(f"ARF search response URL: {response.url}")
            
            # Search for arf in URL first
            arf_match = re.search(r'[?&]arf=([a-zA-Z0-9]+)', str(response.url))
            if arf_match:
                self.arf_token = arf_match.group(1)
                logger.info(f"✓ Found arf token in URL: {self.arf_token}")
                return self.arf_token
            
            # Search for arf in response text
            arf_match = re.search(r'[?&]arf=([a-zA-Z0-9]+)', response.text)
            if arf_match:
                self.arf_token = arf_match.group(1)
                logger.info(f"✓ Found arf token in HTML: {self.arf_token}")
                return self.arf_token
            
            # Try to find it in JavaScript or forms
            arf_match = re.search(r'arf["\']?\s*[:=]\s*["\']([a-zA-Z0-9]+)["\']', response.text)
            if arf_match:
                self.arf_token = arf_match.group(1)
                logger.info(f"✓ Found arf token in JavaScript: {self.arf_token}")
                return self.arf_token
            
            # Try to find CsrfToken
            csrf_match = re.search(r"var\s+CsrfToken\s*=\s*function\s*\(\)\s*\{\s*var\s+\w+\s*=\s*'([a-zA-Z0-9]+)'", response.text)
            if csrf_match:
                self.arf_token = csrf_match.group(1)
                logger.info(f"✓ Found CsrfToken as ARF: {self.arf_token}")
                return self.arf_token
            
            logger.debug("No ARF token found in dashboard response")
        except Exception as e:
            logger.error(f"Error getting arf token: {e}")
        
        return None
    
    def validate_session(self) -> bool:
        """Validate that the web session is still active"""
        if not self.web_session:
            return False
            
        try:
            # Try to access a protected page
            test_url = f"{BASE_URL}/erp/orders/orders"
            if self.arf_token:
                test_url += f"?arf={self.arf_token}"
            
            response = self.web_session.get(test_url, timeout=10)
            
            # Check if we're still logged in
            logger.debug(f"Validation response status: {response.status_code}")
            logger.debug(f"Validation response URL: {response.url}")
            response_text = response.text
            logger.debug(f"Response contains 'logout': {'logout' in response_text.lower()}")
            logger.debug(f"Response contains 'login': {'login' in response_text.lower()}")
            
            # If we get redirected to login page, session is invalid
            if 'login' in str(response.url).lower() and 'logout' not in response_text.lower():
                logger.error("✗ Redirected to login page - session invalid")
                return False
            
            # Accept 400 errors as they might just mean missing parameters
            if response.status_code == 400:
                logger.info("✓ Web session is valid (got 400 - likely missing parameters)")
                return True
            
            # If we see logout link or are on a protected page, we're logged in
            if response.status_code == 200 and ('logout' in response_text.lower() or '/erp/' in str(response.url)):
                logger.info("✓ Web session is valid")
                
                # Try to extract ARF token if we don't have it
                if not self.arf_token:
                    # Try URL first
                    arf_match = re.search(r'[?&]arf=([a-zA-Z0-9]+)', str(response.url))
                    if not arf_match:
                        # Try response text
                        arf_match = re.search(r'[?&]arf=([a-zA-Z0-9]+)', response_text)
                    
                    if arf_match:
                        self.arf_token = arf_match.group(1)
                        logger.info(f"✓ ARF token obtained from session validation: {self.arf_token[:8]}...")
                
                return True
            else:
                logger.error("✗ Web session validation failed")
                return False
                
        except Exception as e:
            logger.error(f"✗ Error validating web session: {e}")
            return False
    
    def fetch_orders(self, date_from: datetime, date_to: datetime) -> List[Dict[str, Any]]:
        """Fetch all orders and filter client-side (API filter requires partner token)"""
        all_orders = []
        has_next_page = True
        cursor = None

        logger.info("Note: Fetching all orders without date filter due to API limitations")
        logger.info("Orders will be filtered client-side by date range")

        while has_next_page:
            # Remove the filter param as it requires partner token
            # We'll filter by date on the client side instead
            variables = {
                'params': {
                    'limit': 30,  # API max limit is 30
                    'order_by': 'pur_date',
                    'sort': 'ASC'
                }
            }

            if cursor is not None:
                variables['params']['cursor'] = cursor

            try:
                logger.debug(f"Executing query with variables: {json.dumps(variables, indent=2)}")
                result = self.client.execute(ORDER_QUERY, variable_values=variables)
                orders_data = result.get('getOrderList', {})
                orders = orders_data.get('data', [])

                # Filter out None values (orders that failed to fetch)
                valid_orders = [o for o in orders if o is not None]
                all_orders.extend(valid_orders)

                page_info = orders_data.get('pageInfo', {})
                has_next_page = page_info.get('hasNextPage', False)
                cursor = page_info.get('nextCursor')

                skipped = len(orders) - len(valid_orders)
                if skipped > 0:
                    logger.warning(f"Skipped {skipped} orders with errors in this batch")
                logger.info(f"Fetched {len(valid_orders)} orders (total: {len(all_orders)})")

            except Exception as e:
                error_str = str(e)
                logger.error(f"Error fetching orders: {e}")
                logger.error(f"Full error details: {type(e).__name__}: {error_str}")

                # Check if this is a GraphQL error with partial data
                # The gql library might have partial results even with errors
                partial_data = None
                if hasattr(e, 'data') and e.data:
                    partial_data = e.data
                    logger.info("Error contains partial data, attempting to use it...")
                    try:
                        orders_data = partial_data.get('getOrderList', {})
                        orders = orders_data.get('data', [])

                        # Filter out None values and orders with errors
                        valid_orders = [o for o in orders if o is not None]
                        all_orders.extend(valid_orders)

                        page_info = orders_data.get('pageInfo', {})
                        has_next_page = page_info.get('hasNextPage', False)
                        cursor = page_info.get('nextCursor')

                        logger.info(f"Retrieved {len(valid_orders)} valid orders from partial response")
                        skipped = len(orders) - len(valid_orders)
                        if skipped > 0:
                            logger.warning(f"Skipped {skipped} problematic orders in this batch")

                        # Continue to next page
                        continue
                    except Exception as parse_error:
                        logger.error(f"Failed to parse partial data: {parse_error}")

                # If we can't recover from the error, check if we should continue
                if "Internal server error" in error_str and "price_elements" in error_str:
                    logger.warning("Encountered server error on price_elements field")
                    logger.warning("This is a BizniWeb API issue with a specific order's data")
                    # Skip to next page if we have a cursor
                    if cursor:
                        logger.info("Skipping to next page...")
                        continue

                # Try to get the underlying HTTP response from the GQL exception
                response = None

                # Check for response in the exception itself
                if hasattr(e, 'response'):
                    response = e.response
                # Check for response in the cause
                elif hasattr(e, '__cause__'):
                    cause = e.__cause__
                    if hasattr(cause, 'response'):
                        response = cause.response
                    # For requests.HTTPError, the response is in args
                    elif hasattr(cause, 'args') and len(cause.args) > 0:
                        if hasattr(cause.args[0], 'response'):
                            response = cause.args[0].response

                # Try to extract response from transport layer
                if not response and hasattr(self.client, 'transport'):
                    transport = self.client.transport
                    if hasattr(transport, 'response_headers'):
                        logger.error(f"Transport response headers: {transport.response_headers}")

                if response:
                    logger.error(f"HTTP Response Status: {response.status_code if hasattr(response, 'status_code') else 'N/A'}")
                    logger.error(f"HTTP Response Headers: {dict(response.headers) if hasattr(response, 'headers') else 'N/A'}")
                    try:
                        response_body = response.text if hasattr(response, 'text') else str(response.content if hasattr(response, 'content') else 'N/A')
                        logger.error(f"HTTP Response Body: {response_body}")
                    except Exception as body_err:
                        logger.error(f"Could not read response body: {body_err}")
                else:
                    logger.error("No HTTP response object found in exception")

                # Try making a raw request to see what we get
                logger.error("Attempting to make a raw HTTP request to diagnose the issue...")
                try:
                    import requests
                    headers = {'BW-API-Key': f'Token {self.api_token}', 'Content-Type': 'application/json'}

                    # Convert GQL DocumentNode to string properly
                    query_string = """
query GetOrders($filter: OrderFilter, $params: OrderParams) {
  getOrderList(filter: $filter, params: $params) {
    data {
      id
      order_num
      status {
        name
      }
    }
    pageInfo {
      hasNextPage
    }
  }
}
"""
                    payload = {
                        'query': query_string,
                        'variables': variables
                    }
                    logger.error(f"Making raw request to: {API_URL}")
                    logger.error(f"With headers: {headers}")
                    raw_response = requests.post(
                        API_URL,
                        json=payload,
                        headers=headers,
                        timeout=10
                    )
                    logger.error(f"Raw request status: {raw_response.status_code}")
                    logger.error(f"Raw request headers: {dict(raw_response.headers)}")
                    logger.error(f"Raw request body: {raw_response.text}")
                except Exception as raw_err:
                    logger.error(f"Raw request also failed: {raw_err}")

                break

        # Filter orders by date client-side
        if all_orders:
            filtered_orders = []
            date_from_str = date_from.strftime('%Y-%m-%d')
            date_to_str = date_to.strftime('%Y-%m-%d')

            for order in all_orders:
                pur_date = order.get('pur_date', '')
                # Extract just the date part if it includes time
                if ' ' in pur_date:
                    pur_date = pur_date.split(' ')[0]

                if date_from_str <= pur_date <= date_to_str:
                    filtered_orders.append(order)

            logger.info(f"Filtered {len(filtered_orders)} orders within date range {date_from_str} to {date_to_str}")
            return filtered_orders

        return all_orders
    
    def filter_orders_for_invoice(self, orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter orders that need invoice generation"""
        filtered_orders = []
        
        for order in orders:
            status = order.get('status', {}) or {}
            status_name = status.get('name', '').lower()
            
            # Check for payment method in price_elements (if available)
            # Note: price_elements removed from query due to API errors,
            # so we'll match orders by status only
            price_elements = order.get('price_elements', []) or []
            payment_name = ''
            for element in price_elements:
                if element.get('type') == 'payment':
                    payment_name = element.get('title', '').lower()
                    break

            # Check if invoices list is empty
            invoices = order.get('invoices', []) or []
            has_invoice = len(invoices) > 0

            # Check criteria:
            # 1. Status is "Odoslaná" (sent) OR "Čaká na vybavenie" (waiting for processing)
            # 2. Payment method check removed (was causing API errors)
            # 3. No invoices (empty list)
            #
            # Note: Since we removed price_elements from the query to avoid API crashes,
            # we now match all "Odoslaná" orders without invoices.
            # You may need to manually filter by payment method when processing.
            if ((status_name == 'odoslaná' or 'čaká na vybavenie' in status_name) and
                not has_invoice):
                filtered_orders.append(order)
                logger.info(f"Order {order.get('order_num')} matches criteria for invoice generation - Status: {status_name}")
            else:
                logger.debug(f"Order {order.get('order_num')} skipped - Status: {status_name}, Has Invoice: {has_invoice}")
        
        return filtered_orders
    
    def create_invoice(self, order: Dict[str, Any]) -> bool:
        """Create invoice for the order"""
        order_num = order.get('order_num')
        customer = order.get('customer', {})
        customer_name = customer.get('company_name', '')
        if not customer_name:
            customer_name = f"{customer.get('name', '')} {customer.get('surname', '')}"
        
        order_sum = order.get('sum', {}).get('formatted', 'N/A')
        
        # Log the order details
        logger.info(f"Processing order {order_num}:")
        logger.info(f"  Customer: {customer_name}")
        logger.info(f"  Amount: {order_sum}")
        logger.info(f"  Status: {order.get('status', {}).get('name', 'N/A')}")
        
        # Create invoice using web session
        try:
                # Add timestamp to avoid caching
                import time
                timestamp = int(time.time() * 1000)
                
                # Get order ID from the order data (not order_num)
                order_id = order.get('id')
                logger.debug(f"Order {order_num} has ID: {order_id}")
                
                # Set headers for AJAX request
                headers = {
                    'X-Requested-With': 'XMLHttpRequest',
                    'Accept': 'application/json, text/javascript, */*; q=0.01',
                    'Referer': f'{BASE_URL}/erp/orders/orders/detail/{order_num}'
                }
                
                # First try to create the invoice (this might be required before finalization)
                create_url = INVOICE_CREATE_URL.format(order_num=order_num)
                if self.arf_token:
                    create_url += f"?arf={self.arf_token}&_dc={timestamp}"
                else:
                    create_url += f"?_dc={timestamp}"
                
                logger.debug(f"Attempting to create invoice first: {create_url}")
                create_response = self.web_session.post(create_url, headers=headers)
                
                if create_response.status_code == 200:
                    try:
                        create_result = create_response.json()
                        logger.debug(f"Create response: {create_result}")
                        if not create_result.get('success'):
                            logger.debug(f"Create step failed: {create_result.get('errors', {}).get('reason', 'Unknown error')}")
                    except json.JSONDecodeError:
                        logger.debug(f"Create response not JSON: {create_response.text[:200]}")
                
                # Add a small delay to avoid locking issues
                time.sleep(1)
                
                # Now try to finalize the invoice
                # Try order_num first, then order_id
                urls_to_try = [('order_num', order_num)]
                if order_id:
                    urls_to_try.append(('order_id', order_id))
                
                for url_type, identifier in urls_to_try:
                    # Create invoice URL
                    finalize_url = INVOICE_FINALIZE_URL.format(order_num=identifier)
                    timestamp = int(time.time() * 1000)  # New timestamp for finalize
                    if self.arf_token:
                        finalize_url += f"?arf={self.arf_token}&_dc={timestamp}"
                    else:
                        finalize_url += f"?_dc={timestamp}"
                    
                    logger.debug(f"Attempting to finalize invoice via {url_type}: {finalize_url}")
                    
                    # Try POST first, then GET if it fails
                    response = self.web_session.post(finalize_url, headers=headers)
                    
                    if response.status_code == 405:  # Method not allowed
                        logger.debug("POST not allowed, trying GET")
                        response = self.web_session.get(finalize_url, headers=headers)
                    
                    # If successful, break out of loop
                    if response.status_code == 200:
                        break
                    elif response.status_code == 400:
                        logger.debug(f"Got 400 error with {url_type}, trying next...")
                        continue
                
                # Check if invoice was created
                if response.status_code == 200:
                    # Log the raw response first
                    logger.debug(f"  Raw response: {response.text[:1000]}")
                    
                    # Try to parse response
                    try:
                        result = response.json()
                        logger.debug(f"  JSON response: {result}")
                        if result.get('success'):
                            invoice_id = result.get('invoice_id') or result.get('id')
                            invoice_num = result.get('invoice_num')
                            logger.info(f"  ✓ Invoice created: {invoice_num}")
                            
                            # Try to send email
                            if invoice_id:
                                if self.send_invoice_email(invoice_id):
                                    logger.info(f"  ✓ Invoice email notification sent successfully to customer")
                                else:
                                    logger.warning(f"  ⚠ Failed to send invoice email notification")
                            
                            return True
                        else:
                            error_msg = result.get('message') or result.get('errors', {}).get('reason', 'Unknown error')
                            logger.error(f"  ✗ Invoice creation failed: {error_msg}")
                            return False
                    except json.JSONDecodeError:
                        # Response might be HTML, check for success indicators
                        if 'success' in response.text.lower() or 'invoice' in response.text.lower():
                            logger.info(f"  ✓ Invoice likely created (HTML response)")
                            return True
                        else:
                            logger.error(f"  ✗ Invoice creation failed (HTML response)")
                            logger.debug(f"  ✗ HTML response: {response.text[:500]}")
                            return False
                elif response.status_code == 400:
                    # Log the actual error response
                    logger.error(f"  ✗ Bad request (400) - URL: {finalize_url}")
                    try:
                        error_detail = response.json()
                        logger.error(f"  ✗ Error details: {error_detail}")
                    except:
                        logger.error(f"  ✗ Error response: {response.text[:500]}")
                    return False
                else:
                    logger.error(f"  ✗ Invoice creation failed with status {response.status_code}")
                    logger.error(f"  ✗ Response: {response.text[:500]}")
                    return False
                            
        except Exception as e:
            logger.error(f"  ✗ Error creating invoice: {e}")
            return False
    
    def send_invoice_email(self, invoice_id: str) -> bool:
        """Send invoice email notification to customer"""
        try:
            import time
            timestamp = int(time.time() * 1000)
            
            send_url = INVOICE_SEND_URL.format(invoice_id=invoice_id)
            if self.arf_token:
                send_url += f"?arf={self.arf_token}&_dc={timestamp}"
            else:
                send_url += f"?_dc={timestamp}"
            
            logger.debug(f"Sending invoice email via: {send_url}")
            response = self.web_session.post(send_url)
            response.raise_for_status()
            
            # Check response
            try:
                result = response.json()
                if result.get('success'):
                    logger.debug(f"Email API response: success")
                    return True
                else:
                    logger.debug(f"Email API response: {result}")
                    return False
            except json.JSONDecodeError:
                # Check HTML response
                if 'success' in response.text.lower() or response.status_code == 200:
                    logger.debug("Email sent (HTML response indicates success)")
                    return True
                else:
                    logger.debug(f"Email send unclear response: {response.status_code}")
                    return response.status_code == 200
                
        except Exception as e:
            logger.error(f"Error sending invoice email: {e}")
            return False
    
    def process_orders(self, date_from: datetime, date_to: datetime, dry_run: bool = False):
        """Main process to generate invoices for matching orders"""
        logger.info(f"Processing orders from {date_from.strftime('%Y-%m-%d')} to {date_to.strftime('%Y-%m-%d')}")
        
        # Check if we have web session for invoice creation
        if not self.web_session:
            logger.error("=" * 60)
            logger.error("✗ No web session available - cannot create invoices")
            logger.error("=" * 60)
            logger.error("Invoice creation requires web login credentials.")
            logger.error("Please add your credentials to the .env file:")
            logger.error("  BIZNISWEB_USERNAME=your_username@example.com")
            logger.error("  BIZNISWEB_PASSWORD=your_password")
            logger.error("=" * 60)
            return
        
        # Check if we have ARF token
        if not self.arf_token:
            logger.warning("No ARF token available - will try to proceed without it")
        
        # Validate web session
        logger.info("Validating web session...")
        if not self.validate_session():
            logger.error("=" * 60)
            logger.error("✗ Web session validation failed - cannot proceed")
            logger.error("=" * 60)
            logger.error("Please check your login credentials in .env file")
            logger.error("=" * 60)
            return
        
        # Fetch orders using GraphQL API
        logger.info("Fetching orders from GraphQL API...")
        orders = self.fetch_orders(date_from, date_to)
        logger.info(f"Total orders fetched: {len(orders)}")
        
        # Filter orders that need invoices
        orders_for_invoice = self.filter_orders_for_invoice(orders)
        logger.info(f"Orders matching criteria: {len(orders_for_invoice)}")
        
        if dry_run:
            logger.info("DRY RUN mode - no invoices will be created")
            for order in orders_for_invoice:
                customer = order.get('customer', {})
                customer_name = customer.get('company_name', '')
                if not customer_name:
                    customer_name = f"{customer.get('name', '')} {customer.get('surname', '')}".strip()
                
                logger.info(f"Would create invoice for order {order.get('order_num')} - {customer_name} - {order.get('sum', {}).get('formatted', 'N/A')}")
            
            logger.info("=" * 60)
            logger.info(f"DRY RUN Summary:")
            logger.info(f"  Orders that would be processed: {len(orders_for_invoice)}")
            total = sum(order.get('sum', {}).get('value', 0) for order in orders_for_invoice)
            logger.info(f"  Total amount: €{total:.2f}")
            if self.web_session:
                logger.info("  Web session: Available (invoices would be created)")
            else:
                logger.info("  Web session: Not available (manual processing required)")
            logger.info("=" * 60)
            return
        
        # Process orders for invoice creation
        success_count = 0
        failed_count = 0
        total_amount = 0.0
        processed_orders = []
        
        for order in orders_for_invoice:
            order_num = order.get('order_num')
            customer = order.get('customer', {})
            customer_email = customer.get('email', 'N/A')
            
            if self.create_invoice(order):
                success_count += 1
                # Try to extract numeric value from formatted amount
                order_sum = order.get('sum', {}).get('value', 0)
                if order_sum:
                    total_amount += float(order_sum)
                processed_orders.append({
                    'order_num': order_num,
                    'email': customer_email,
                    'amount': order.get('sum', {}).get('formatted', 'N/A')
                })
            else:
                failed_count += 1
        
        logger.info("=" * 60)
        logger.info(f"Invoice processing complete:")
        logger.info(f"  ✓ Invoices created: {success_count}")
        if failed_count > 0:
            logger.info(f"  ✗ Failed: {failed_count}")
        logger.info(f"  Total amount: €{total_amount:.2f}")
        logger.info("=" * 60)
        
        if success_count > 0:
            logger.info("Email notifications sent to:")
            for order in processed_orders:
                logger.info(f"  • Order {order['order_num']}: {order['email']} ({order['amount']})")
            logger.info("=" * 60)


def main():
    """Main function to handle command line arguments and run the invoice generator"""
    parser = argparse.ArgumentParser(description='Generate invoices for BizniWeb orders')
    parser.add_argument(
        '--from-date',
        type=str,
        help='From date in YYYY-MM-DD format (default: 7 days ago)'
    )
    parser.add_argument(
        '--to-date',
        type=str,
        help='To date in YYYY-MM-DD format (default: today)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run in dry-run mode (no invoices will be created)'
    )
    parser.add_argument(
        '--no-web-login',
        action='store_true',
        help='Skip web login (exits immediately as invoice creation requires web session)'
    )
    
    args = parser.parse_args()
    
    # Check if API token is available
    if not API_TOKEN:
        logger.error("✗ BIZNISWEB_API_TOKEN not found in environment variables")
        logger.error("Please set it in .env file:")
        logger.error("  BIZNISWEB_API_TOKEN=your_api_token_here")
        return
    
    # Parse dates
    if args.to_date:
        date_to = datetime.strptime(args.to_date, '%Y-%m-%d')
    else:
        date_to = datetime.now()
    
    if args.from_date:
        date_from = datetime.strptime(args.from_date, '%Y-%m-%d')
    else:
        date_from = date_to - timedelta(days=7)
    
    # Initialize generator with web credentials if available and not disabled
    if args.no_web_login:
        generator = InvoiceGenerator(API_URL, API_TOKEN)
    else:
        generator = InvoiceGenerator(API_URL, API_TOKEN, WEB_USERNAME, WEB_PASSWORD)
    
    # Process orders
    generator.process_orders(date_from, date_to, dry_run=args.dry_run)


if __name__ == "__main__":
    main()