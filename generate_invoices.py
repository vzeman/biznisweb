#!/usr/bin/env python3
"""
Generate invoices for orders with specific criteria in BizniWeb
"""

import os
import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Any, Iterable, Optional, Tuple, Union
import json
import re
import time
import unicodedata

from dotenv import load_dotenv
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from http_client import build_retry_session, resolve_timeout
from logger_config import get_logger
from reporting_core import (
    BASE_DEFAULT_PROJECT,
    derive_biznisweb_base_url,
    load_project_env,
    load_project_settings,
    resolve_biznisweb_api_url,
)

# Load environment variables
load_dotenv(encoding="utf-8-sig")

GRAPHQL_TIMEOUT_SEC = int(os.getenv('BIZNISWEB_API_TIMEOUT_SEC', os.getenv('REPORT_HTTP_READ_TIMEOUT_SEC', '30')))
WEB_TIMEOUT = resolve_timeout(os.getenv('BIZNISWEB_WEB_TIMEOUT_SEC'))

# Set up logging
logger = get_logger('generate_invoices')

DEFAULT_INVOICE_LOOKBACK_DAYS = 7
DEFAULT_INVOICE_STATUS_CHANGE_LOOKBACK_DAYS = 7
DEFAULT_INVOICE_RECONCILIATION_LOOKBACK_DAYS = 120
DEFAULT_INVOICE_PAGE_RETRY_ATTEMPTS = 3
DEFAULT_INVOICE_MAX_PAGES = 1000
DEFAULT_INVOICE_ELIGIBLE_STATUSES = ("Odoslaná",)
SUPPORTED_INVOICE_SCAN_DATE_FIELDS = ("pur_date", "last_change")

# GraphQL query to fetch orders with specific criteria
ORDER_QUERY = gql("""
query GetOrders($filter: OrderFilter, $params: OrderParams) {
  getOrderList(filter: $filter, params: $params) {
    data {
      id
      order_num
      pur_date
      last_change
      status {
        id
        name
        color
      }
      invoices {
        id
        invoice_num
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

ORDER_INVOICE_QUERY = gql("""
query GetOrderInvoices($order_num: String!) {
  getOrder(order_num: $order_num) {
    order_num
    invoices {
      id
      invoice_num
    }
  }
}
""")

ORDER_INVOICE_GUARD_QUERY = gql("""
query GetOrderInvoiceGuard($order_num: String!) {
  getOrder(order_num: $order_num) {
    id
    order_num
    pur_date
    status {
      id
      name
    }
    price_elements {
      type
      title
      reference_id
    }
    invoices {
      id
      invoice_num
    }
    sum {
      value
      formatted
    }
  }
}
""")


@dataclass
class InvoiceRunSummary:
    project: str
    date_from: str
    date_to: str
    dry_run: bool = False
    total_orders_fetched: int = 0
    matched_orders: int = 0
    created_invoices: int = 0
    failed_invoices: int = 0
    emailed_invoices: int = 0
    failed_invoice_emails: int = 0
    missing_invoice_ids: int = 0
    already_present_invoices: int = 0
    skipped_stale_orders: int = 0
    skipped_zero_total_orders: int = 0
    skipped_non_cod_orders: int = 0
    skipped_before_automation_start: int = 0
    total_amount: float = 0.0
    scan_mode: str = "regular"
    scan_complete: bool = True
    purchase_date_orders_fetched: int = 0
    recent_change_orders_fetched: int = 0
    pages_fetched: int = 0
    page_retry_count: int = 0


@dataclass
class InvoiceCreationResult:
    created: bool = False
    invoice_id: Optional[str] = None
    invoice_num: Optional[str] = None
    email_required: bool = True
    email_sent: bool = False
    email_error: str = ""
    already_present: bool = False
    skipped_stale: bool = False
    skipped_before_automation_start: bool = False

    def __bool__(self) -> bool:
        return self.created and (not self.email_required or self.email_sent)


class IncompleteInvoiceScanError(RuntimeError):
    """Raised when an invoice run cannot prove that every requested order page was scanned."""


def _normalize_optional_invoice_date(value: Any, *, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return ""
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", normalized):
        raise ValueError(f"{field_name} must use YYYY-MM-DD format")
    try:
        parsed = datetime.strptime(normalized, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a valid calendar date") from exc
    return parsed.strftime("%Y-%m-%d")


def resolve_invoice_generation_settings(project_settings: Dict[str, Any]) -> Dict[str, Any]:
    raw_settings = project_settings.get("invoice_generation") or {}
    realized_revenue_settings = project_settings.get("realized_revenue") or {}
    enabled = bool(raw_settings.get("enabled", False))
    automation_start_date = _normalize_optional_invoice_date(
        raw_settings.get("automation_start_date"),
        field_name="invoice_generation.automation_start_date",
    )
    if enabled and not automation_start_date:
        raise ValueError(
            "invoice_generation.automation_start_date is required when "
            "invoice generation is enabled"
        )
    raw_lookback_days = raw_settings.get("lookback_days", DEFAULT_INVOICE_LOOKBACK_DAYS)
    try:
        lookback_days = int(raw_lookback_days)
    except (TypeError, ValueError):
        lookback_days = DEFAULT_INVOICE_LOOKBACK_DAYS
    lookback_days = max(1, lookback_days)

    raw_status_change_lookback_days = raw_settings.get(
        "status_change_lookback_days",
        DEFAULT_INVOICE_STATUS_CHANGE_LOOKBACK_DAYS,
    )
    try:
        status_change_lookback_days = int(raw_status_change_lookback_days)
    except (TypeError, ValueError):
        status_change_lookback_days = DEFAULT_INVOICE_STATUS_CHANGE_LOOKBACK_DAYS
    status_change_lookback_days = max(lookback_days, status_change_lookback_days)

    raw_reconciliation_lookback_days = raw_settings.get(
        "reconciliation_lookback_days",
        DEFAULT_INVOICE_RECONCILIATION_LOOKBACK_DAYS,
    )
    try:
        reconciliation_lookback_days = int(raw_reconciliation_lookback_days)
    except (TypeError, ValueError):
        reconciliation_lookback_days = DEFAULT_INVOICE_RECONCILIATION_LOOKBACK_DAYS
    reconciliation_lookback_days = max(lookback_days, reconciliation_lookback_days)

    raw_page_retry_attempts = raw_settings.get(
        "page_retry_attempts",
        DEFAULT_INVOICE_PAGE_RETRY_ATTEMPTS,
    )
    try:
        page_retry_attempts = int(raw_page_retry_attempts)
    except (TypeError, ValueError):
        page_retry_attempts = DEFAULT_INVOICE_PAGE_RETRY_ATTEMPTS

    try:
        rollover_grace_hours = int(raw_settings.get("rollover_grace_hours", 3))
    except (TypeError, ValueError):
        rollover_grace_hours = 3
    try:
        max_pages = int(
            raw_settings.get("max_pages", DEFAULT_INVOICE_MAX_PAGES)
        )
    except (TypeError, ValueError):
        max_pages = DEFAULT_INVOICE_MAX_PAGES

    raw_statuses = raw_settings.get("eligible_statuses", DEFAULT_INVOICE_ELIGIBLE_STATUSES)
    if isinstance(raw_statuses, str):
        raw_statuses = [raw_statuses]
    eligible_statuses = [
        str(status).strip()
        for status in raw_statuses
        if str(status or "").strip()
    ]
    if not eligible_statuses:
        eligible_statuses = list(DEFAULT_INVOICE_ELIGIBLE_STATUSES)

    raw_cod_payment_ids = raw_settings.get(
        "cod_payment_ids",
        realized_revenue_settings.get("cod_payment_ids", []),
    )
    raw_cod_payment_patterns = raw_settings.get(
        "cod_payment_patterns",
        realized_revenue_settings.get("cod_payment_patterns", []),
    )
    cod_payment_ids = [
        str(value).strip()
        for value in (raw_cod_payment_ids or [])
        if str(value or "").strip()
    ]
    cod_payment_patterns = [
        str(value).strip()
        for value in (raw_cod_payment_patterns or [])
        if str(value or "").strip()
    ]

    return {
        "enabled": enabled,
        "lookback_days": lookback_days,
        "status_change_lookback_days": status_change_lookback_days,
        "reconciliation_lookback_days": reconciliation_lookback_days,
        "include_recent_changes": bool(raw_settings.get("include_recent_changes", True)),
        "page_retry_attempts": max(1, min(page_retry_attempts, 10)),
        "rollover_grace_hours": max(0, min(rollover_grace_hours, 12)),
        "max_pages": max(1, min(max_pages, 10_000)),
        "exclude_zero_total_orders": bool(raw_settings.get("exclude_zero_total_orders", True)),
        "require_cod_payment": bool(raw_settings.get("require_cod_payment", False)),
        "cod_payment_ids": cod_payment_ids,
        "cod_payment_patterns": cod_payment_patterns,
        "eligible_statuses": eligible_statuses,
        "send_invoice_email": bool(raw_settings.get("send_invoice_email", True)),
        "automation_start_date": automation_start_date,
    }


def resolve_invoice_date_window(reference_date: Union[str, datetime], lookback_days: int) -> Tuple[str, str]:
    if isinstance(reference_date, datetime):
        to_date = reference_date
    else:
        to_date = datetime.strptime(str(reference_date), "%Y-%m-%d")

    safe_lookback_days = max(1, int(lookback_days))
    from_date = to_date - timedelta(days=safe_lookback_days - 1)
    return from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d")


def validate_invoice_creation_limit(
    matched_orders: int,
    max_creations: Optional[int],
    *,
    dry_run: bool,
) -> None:
    if max_creations is None:
        return
    safe_limit = int(max_creations)
    if safe_limit < 0:
        raise ValueError("max_creations cannot be negative")
    if not dry_run and int(matched_orders) > safe_limit:
        raise RuntimeError(
            f"Invoice creation safety limit exceeded: matched={matched_orders}, "
            f"max_creations={safe_limit}"
        )


def _validated_invoice_list(
    invoices: Any,
    *,
    context: str,
) -> List[Dict[str, Any]]:
    if invoices is None:
        return []
    if not isinstance(invoices, list):
        raise ValueError(f"{context} returned an incomplete invoices list")
    for invoice in invoices:
        if not isinstance(invoice, dict) or not any(
            invoice.get(field) not in (None, "")
            for field in ("id", "invoice_num")
        ):
            raise ValueError(f"{context} returned an incomplete invoice entry")
    return invoices


def _parse_order_total_value(order: Dict[str, Any]) -> Optional[float]:
    order_sum = order.get("sum", {}) or {}
    raw_value = order_sum.get("value")
    if raw_value not in (None, ""):
        try:
            return float(raw_value)
        except (TypeError, ValueError):
            pass

    formatted = str(order_sum.get("formatted") or "").strip()
    if not formatted:
        return None

    normalized = re.sub(r"[^0-9,.\-]", "", formatted)
    if not normalized:
        return None
    if "," in normalized and "." in normalized:
        if normalized.rfind(",") > normalized.rfind("."):
            normalized = normalized.replace(".", "").replace(",", ".")
        else:
            normalized = normalized.replace(",", "")
    else:
        normalized = normalized.replace(",", ".")

    try:
        return float(normalized)
    except ValueError:
        return None


def _coerce_order_total_value(order: Dict[str, Any]) -> float:
    parsed = _parse_order_total_value(order)
    return 0.0 if parsed is None else parsed


def _normalize_status_text(status_name: str) -> str:
    normalized = unicodedata.normalize("NFKD", status_name or "")
    without_marks = "".join(char for char in normalized if not unicodedata.combining(char))
    return without_marks.strip().lower()


def _normalized_invoice_statuses(status_names: Iterable[str]) -> set[str]:
    return {
        _normalize_status_text(status)
        for status in status_names
        if _normalize_status_text(status)
    }


def _status_matches_invoice_generation(status_name: str, eligible_statuses: Optional[Iterable[str]] = None) -> bool:
    normalized = _normalize_status_text(status_name)
    allowed_statuses = _normalized_invoice_statuses(eligible_statuses or DEFAULT_INVOICE_ELIGIBLE_STATUSES)
    return normalized in allowed_statuses


def _payment_elements(order: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw_elements = order.get("price_elements")
    if not isinstance(raw_elements, list):
        return []
    return [
        element
        for element in raw_elements
        if isinstance(element, dict)
        and _normalize_status_text(str(element.get("type") or "")) == "payment"
    ]


def _payment_matches_cod(
    order: Dict[str, Any],
    cod_payment_ids: Iterable[str],
    cod_payment_patterns: Iterable[str],
) -> bool:
    configured_ids = {
        str(value).strip()
        for value in cod_payment_ids
        if str(value or "").strip()
    }
    normalized_patterns = {
        _normalize_status_text(str(value))
        for value in cod_payment_patterns
        if _normalize_status_text(str(value))
    }
    for element in _payment_elements(order):
        reference_id = str(element.get("reference_id") or "").strip()
        if reference_id and reference_id in configured_ids:
            return True
        title = _normalize_status_text(str(element.get("title") or ""))
        if any(pattern in title for pattern in normalized_patterns):
            return True
    return False


def _order_scan_date(order: Dict[str, Any], date_field: str) -> str:
    if date_field not in SUPPORTED_INVOICE_SCAN_DATE_FIELDS:
        raise ValueError(f"Unsupported invoice scan date field: {date_field}")
    raw_date = str(order.get(date_field) or "").strip()
    date_part = re.split(r"[T\s]", raw_date, maxsplit=1)[0]
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", date_part):
        raise ValueError(
            f"Invalid {date_field} value {raw_date!r} for order "
            f"{order.get('order_num') or order.get('id') or 'unknown'}"
        )
    try:
        parsed = datetime.strptime(date_part, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(
            f"Invalid {date_field} value {raw_date!r} for order "
            f"{order.get('order_num') or order.get('id') or 'unknown'}"
        ) from exc
    return parsed.strftime("%Y-%m-%d")


def _extract_invoice_id_from_payload(payload: Any) -> Optional[str]:
    if not isinstance(payload, dict):
        return None

    for key in ("invoice_id", "invoiceId", "invoiceID"):
        value = payload.get(key)
        if value not in (None, ""):
            return str(value)

    invoice = payload.get("invoice")
    if isinstance(invoice, dict):
        value = invoice.get("id") or invoice.get("invoice_id") or invoice.get("invoiceId")
        if value not in (None, ""):
            return str(value)

    for key in ("data", "result", "record"):
        nested = payload.get(key)
        nested_invoice_id = _extract_invoice_id_from_payload(nested)
        if nested_invoice_id:
            return nested_invoice_id

    value = payload.get("id")
    if value not in (None, ""):
        return str(value)

    return None


def _extract_invoice_num_from_payload(payload: Any) -> Optional[str]:
    if not isinstance(payload, dict):
        return None

    for key in ("invoice_num", "invoiceNum", "number"):
        value = payload.get(key)
        if value not in (None, ""):
            return str(value)

    invoice = payload.get("invoice")
    if isinstance(invoice, dict):
        value = invoice.get("invoice_num") or invoice.get("invoiceNum") or invoice.get("number")
        if value not in (None, ""):
            return str(value)

    for key in ("data", "result", "record"):
        nested = payload.get(key)
        nested_invoice_num = _extract_invoice_num_from_payload(nested)
        if nested_invoice_num:
            return nested_invoice_num

    return None


def _extract_invoice_id_from_text(*values: Any) -> Optional[str]:
    patterns = (
        r"/erp/orders/invoices/(?:detail|edit|sendEmail)/(\d+)",
        r"[\"']invoice[_-]?id[\"']\s*[:=]\s*[\"']?(\d+)",
        r"[\"']invoiceId[\"']\s*[:=]\s*[\"']?(\d+)",
    )
    for value in values:
        text = str(value or "")
        if not text:
            continue
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                return match.group(1)
    return None


class InvoiceGenerator:
    def __init__(
        self,
        api_url: str,
        api_token: str,
        base_url: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        exclude_zero_total_orders: bool = True,
        eligible_statuses: Optional[Iterable[str]] = None,
        send_invoice_email: bool = True,
        page_retry_attempts: int = DEFAULT_INVOICE_PAGE_RETRY_ATTEMPTS,
        require_cod_payment: bool = False,
        cod_payment_ids: Optional[Iterable[str]] = None,
        cod_payment_patterns: Optional[Iterable[str]] = None,
        max_pages: int = DEFAULT_INVOICE_MAX_PAGES,
        automation_start_date: str = "",
    ):
        """Initialize the invoice generator with API credentials"""
        transport = RequestsHTTPTransport(
            url=api_url,
            headers={'BW-API-Key': f'Token {api_token}'},
            verify=True,
            retries=3,
            timeout=GRAPHQL_TIMEOUT_SEC,
        )
        self.client = Client(transport=transport, fetch_schema_from_transport=False)
        self.api_token = api_token
        self.base_url = base_url.rstrip("/")
        self.login_url = f"{self.base_url}/admin/login/authenticate/"
        self.invoice_create_url = f"{self.base_url}/erp/orders/invoices/create/{{order_num}}"
        self.invoice_finalize_url = f"{self.base_url}/erp/orders/invoices/finalize/{{order_num}}"
        self.invoice_send_url = f"{self.base_url}/erp/orders/invoices/sendEmail/{{invoice_id}}"
        self.web_session = None
        self.arf_token = None
        self.exclude_zero_total_orders = exclude_zero_total_orders
        self.eligible_statuses = tuple(eligible_statuses or DEFAULT_INVOICE_ELIGIBLE_STATUSES)
        self.send_invoice_email_enabled = bool(send_invoice_email)
        self.page_retry_attempts = max(1, min(int(page_retry_attempts), 10))
        self.require_cod_payment = bool(require_cod_payment)
        self.cod_payment_ids = tuple(
            str(value).strip()
            for value in (cod_payment_ids or [])
            if str(value or "").strip()
        )
        self.cod_payment_patterns = tuple(
            str(value).strip()
            for value in (cod_payment_patterns or [])
            if str(value or "").strip()
        )
        self.max_pages = max(1, min(int(max_pages), 10_000))
        self.automation_start_date = _normalize_optional_invoice_date(
            automation_start_date,
            field_name="automation_start_date",
        )
        self.last_fetch_stats: Dict[str, Any] = {}
        
        # Initialize web session if credentials provided
        if username and password:
            self.web_session = build_retry_session(timeout=WEB_TIMEOUT)
            logger.info("Attempting to login to web interface...")
            if self.login_web_session(username, password):
                logger.info("âś“ Successfully logged in to web session")
                if self.arf_token:
                    logger.info(f"âś“ ARF token obtained: {self.arf_token[:8]}...")
                else:
                    logger.info("âš  No ARF token found yet, will try to obtain during invoice creation")
            else:
                logger.error("âś— Failed to login to web session - invoice creation will not be available")
                self.web_session = None

    def login_web_session(self, username: str, password: str) -> bool:
        """Login to BizniWeb web interface to get session cookies"""
        try:
            # Step 1: GET login page to obtain session cookie
            logger.info("Getting login page to establish session...")
            login_page_url = f"{self.base_url}/erp/main/login"
            login_page_response = self.web_session.get(login_page_url)
            login_page_response.raise_for_status()
            
            # Check if we got a session cookie
            if 'SSID' in self.web_session.cookies:
                session_id = self.web_session.cookies['SSID']
                logger.info(f"âś“ Session established: {session_id[:10]}...")
            else:
                logger.error("âś— No session cookie received from login page")
                return False
            
            # Extract any arf token from the login page
            arf_token = ''
            arf_match = re.search(r'[?&]arf=([a-zA-Z0-9]+)', login_page_response.text)
            if arf_match:
                arf_token = arf_match.group(1)
                logger.info(f"âś“ Found arf token in login page: {arf_token[:8]}...")
            else:
                # Try to find CsrfToken in the page
                csrf_match = re.search(r"var\s+CsrfToken\s*=\s*function\s*\(\)\s*\{\s*var\s+\w+\s*=\s*'([a-zA-Z0-9]+)'", login_page_response.text)
                if csrf_match:
                    arf_token = csrf_match.group(1)
                    logger.info(f"âś“ Found CsrfToken: {arf_token[:8]}...")
            
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
                self.login_url,
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
                    logger.info("âś“ Login successful (JSON response)")
                    
                    # Extract arf from JSON if available
                    if 'arf' in response_json:
                        self.arf_token = response_json['arf']
                        logger.info(f"âś“ ARF token from JSON: {self.arf_token[:8]}...")
                    
                    # Extract redirect URL if available
                    if 'redirect' in response_json or 'url' in response_json:
                        redirect_url = response_json.get('redirect') or response_json.get('url')
                        logger.info(f"Following redirect to: {redirect_url}")
                        
                        # Follow the redirect
                        redirect_response = self.web_session.get(f"{self.base_url}{redirect_url}")
                        redirect_response.raise_for_status()
                        
                        # Extract arf from redirect
                        arf_match = re.search(r'[?&]arf=([a-zA-Z0-9]+)', redirect_response.url)
                        if arf_match:
                            self.arf_token = arf_match.group(1)
                            logger.info(f"âś“ ARF token from redirect: {self.arf_token[:8]}...")
                    
                    # If login successful, navigate to dashboard to establish session properly
                    logger.info("Navigating to dashboard...")
                    dashboard_url = f"{self.base_url}/erp/"
                    dashboard_response = self.web_session.get(dashboard_url, allow_redirects=True)
                    
                    logger.debug(f"Dashboard status: {dashboard_response.status_code}")
                    logger.debug(f"Dashboard URL: {dashboard_response.url}")
                    
                    # Extract ARF from dashboard URL
                    arf_match = re.search(r'[?&]arf=([a-zA-Z0-9]+)', str(dashboard_response.url))
                    if arf_match:
                        self.arf_token = arf_match.group(1)
                        logger.info(f"âś“ ARF token from dashboard: {self.arf_token[:8]}...")
                    else:
                        # Try to find in response
                        arf_match = re.search(r'[?&]arf=([a-zA-Z0-9]+)', dashboard_response.text)
                        if arf_match:
                            self.arf_token = arf_match.group(1)
                            logger.info(f"âś“ ARF token from dashboard HTML: {self.arf_token[:8]}...")
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
                                logger.info(f"âś“ Found CsrfToken in dashboard: {self.arf_token[:8]}...")
                            else:
                                # Maybe the system doesn't use ARF tokens consistently
                                logger.warning("No ARF token found - system might not require it for all operations")
                    
                    return True
                else:
                    logger.error(f"âś— Login failed: {response_json.get('message', 'Unknown error')}")
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
            if 'error' in response_text.lower() or 'invalid' in response_text.lower() or 'nesprĂˇvne' in response_text.lower():
                logger.error("âś— Login failed - invalid credentials")
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
                logger.info(f"âś“ Successfully logged in and extracted arf token: {self.arf_token[:8]}...")
                return True
            else:
                # Even without arf, check if we're logged in
                if 'logout' in response_text.lower() or '/erp/' in response_url:
                    logger.info("âś“ Successfully logged in (no arf token found yet)")
                    # Try to get arf from dashboard
                    self.get_arf_token()
                    return True
                else:
                    logger.error("âś— Login failed - could not verify successful login")
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
            dashboard_url = f"{self.base_url}/erp/orders/orders"
            response = self.web_session.get(dashboard_url)
            
            logger.debug(f"ARF search response status: {response.status_code}")
            logger.debug(f"ARF search response URL: {response.url}")
            
            # Search for arf in URL first
            arf_match = re.search(r'[?&]arf=([a-zA-Z0-9]+)', str(response.url))
            if arf_match:
                self.arf_token = arf_match.group(1)
                logger.info(f"âś“ Found arf token in URL: {self.arf_token}")
                return self.arf_token
            
            # Search for arf in response text
            arf_match = re.search(r'[?&]arf=([a-zA-Z0-9]+)', response.text)
            if arf_match:
                self.arf_token = arf_match.group(1)
                logger.info(f"âś“ Found arf token in HTML: {self.arf_token}")
                return self.arf_token
            
            # Try to find it in JavaScript or forms
            arf_match = re.search(r'arf["\']?\s*[:=]\s*["\']([a-zA-Z0-9]+)["\']', response.text)
            if arf_match:
                self.arf_token = arf_match.group(1)
                logger.info(f"âś“ Found arf token in JavaScript: {self.arf_token}")
                return self.arf_token
            
            # Try to find CsrfToken
            csrf_match = re.search(r"var\s+CsrfToken\s*=\s*function\s*\(\)\s*\{\s*var\s+\w+\s*=\s*'([a-zA-Z0-9]+)'", response.text)
            if csrf_match:
                self.arf_token = csrf_match.group(1)
                logger.info(f"âś“ Found CsrfToken as ARF: {self.arf_token}")
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
            test_url = f"{self.base_url}/erp/orders/orders"
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
                logger.error("âś— Redirected to login page - session invalid")
                return False
            
            # Accept 400 errors as they might just mean missing parameters
            if response.status_code == 400:
                logger.info("âś“ Web session is valid (got 400 - likely missing parameters)")
                return True
            
            # If we see logout link or are on a protected page, we're logged in
            if response.status_code == 200 and ('logout' in response_text.lower() or '/erp/' in str(response.url)):
                logger.info("âś“ Web session is valid")
                
                # Try to extract ARF token if we don't have it
                if not self.arf_token:
                    # Try URL first
                    arf_match = re.search(r'[?&]arf=([a-zA-Z0-9]+)', str(response.url))
                    if not arf_match:
                        # Try response text
                        arf_match = re.search(r'[?&]arf=([a-zA-Z0-9]+)', response_text)
                    
                    if arf_match:
                        self.arf_token = arf_match.group(1)
                        logger.info(f"âś“ ARF token obtained from session validation: {self.arf_token[:8]}...")
                
                return True
            else:
                logger.error("âś— Web session validation failed")
                return False
                
        except Exception as e:
            logger.error(f"âś— Error validating web session: {e}")
            return False
    
    def fetch_orders(
        self,
        date_from: datetime,
        date_to: datetime,
        *,
        date_field: str = "pur_date",
    ) -> List[Dict[str, Any]]:
        """
        Fetch a complete descending order scan and filter it client-side.

        BizniWeb's date filter requires a partner token, so this scan stops only
        after a fully validated page crosses the requested boundary. A page
        error, a partial GraphQL row, or a broken cursor makes the whole scan
        fail closed; returning a partial list would incorrectly mark the invoice
        run as successful.
        """
        if date_field not in SUPPORTED_INVOICE_SCAN_DATE_FIELDS:
            raise ValueError(f"Unsupported invoice scan date field: {date_field}")

        all_orders: List[Dict[str, Any]] = []
        cursor: Any = None
        seen_cursors: set[Tuple[str, str]] = set()
        date_from_str = date_from.strftime("%Y-%m-%d")
        date_to_str = date_to.strftime("%Y-%m-%d")
        pages_fetched = 0
        page_retry_count = 0
        exhausted_history = False
        crossed_start_boundary = False
        previous_page_last_date: Optional[str] = None

        logger.info(
            "Fetching orders by %s DESC; pagination stops after a complete page crosses %s",
            date_field,
            date_from_str,
        )

        while True:
            variables = {
                "params": {
                    "limit": 30,
                    "order_by": date_field,
                    "sort": "DESC",
                }
            }
            if cursor is not None:
                variables["params"]["cursor"] = cursor

            page_orders: Optional[List[Dict[str, Any]]] = None
            page_info: Optional[Dict[str, Any]] = None
            page_dates: Optional[List[str]] = None
            last_error: Optional[Exception] = None
            for attempt in range(1, self.page_retry_attempts + 1):
                try:
                    logger.debug("Executing invoice order query with variables: %s", variables)
                    result = self.client.execute(ORDER_QUERY, variable_values=variables)
                    orders_data = result.get("getOrderList")
                    if not isinstance(orders_data, dict):
                        raise ValueError("GraphQL response is missing getOrderList")

                    raw_orders = orders_data.get("data")
                    if not isinstance(raw_orders, list):
                        raise ValueError("GraphQL response is missing getOrderList.data")
                    if any(order is None or not isinstance(order, dict) for order in raw_orders):
                        raise ValueError("GraphQL returned one or more incomplete order rows")

                    raw_page_info = orders_data.get("pageInfo")
                    if not isinstance(raw_page_info, dict):
                        raise ValueError("GraphQL response is missing getOrderList.pageInfo")
                    if (
                        "hasNextPage" not in raw_page_info
                        or not isinstance(raw_page_info.get("hasNextPage"), bool)
                    ):
                        raise ValueError(
                            "GraphQL response is missing a boolean pageInfo.hasNextPage"
                        )

                    missing_dates = [
                        str(order.get("order_num") or order.get("id") or "unknown")
                        for order in raw_orders
                        if not _order_scan_date(order, date_field)
                    ]
                    if missing_dates:
                        raise ValueError(
                            f"GraphQL returned orders without {date_field}: {missing_dates[:5]}"
                        )
                    for order in raw_orders:
                        order_num = str(order.get("order_num") or "").strip()
                        status = order.get("status")
                        invoices = order.get("invoices")
                        if not order_num:
                            raise ValueError("GraphQL returned an order without order_num")
                        if not isinstance(status, dict) or not str(status.get("name") or "").strip():
                            raise ValueError(
                                f"GraphQL returned order {order_num} without a complete status"
                            )
                        order["invoices"] = _validated_invoice_list(
                            invoices,
                            context=f"GraphQL order {order_num}",
                        )
                    candidate_dates = [
                        _order_scan_date(order, date_field)
                        for order in raw_orders
                    ]
                    if any(
                        later > earlier
                        for earlier, later in zip(
                            candidate_dates,
                            candidate_dates[1:],
                        )
                    ):
                        raise ValueError(
                            f"GraphQL {date_field} page is not sorted DESC"
                        )
                    if (
                        previous_page_last_date is not None
                        and candidate_dates
                        and candidate_dates[0] > previous_page_last_date
                    ):
                        raise ValueError(
                            f"GraphQL {date_field} pagination moved forward "
                            "instead of DESC"
                        )

                    page_orders = raw_orders
                    page_info = raw_page_info
                    page_dates = candidate_dates
                    break
                except Exception as exc:
                    last_error = exc
                    if attempt >= self.page_retry_attempts:
                        break
                    page_retry_count += 1
                    delay_seconds = min(2 ** (attempt - 1), 5)
                    logger.warning(
                        "Invoice order page failed for %s (attempt %s/%s): %s; retrying in %ss",
                        date_field,
                        attempt,
                        self.page_retry_attempts,
                        exc,
                        delay_seconds,
                    )
                    time.sleep(delay_seconds)

            if page_orders is None or page_info is None or page_dates is None:
                self.last_fetch_stats = {
                    "date_field": date_field,
                    "pages_fetched": pages_fetched,
                    "page_retry_count": page_retry_count,
                    "scan_complete": False,
                }
                raise IncompleteInvoiceScanError(
                    f"Incomplete {date_field} invoice scan after {pages_fetched} complete pages"
                ) from last_error

            pages_fetched += 1
            all_orders.extend(page_orders)
            batch_dates = page_dates
            if batch_dates:
                previous_page_last_date = batch_dates[-1]
            if batch_dates:
                logger.info(
                    "Fetched %s orders by %s (total: %s) covering %s..%s",
                    len(page_orders),
                    date_field,
                    len(all_orders),
                    min(batch_dates),
                    max(batch_dates),
                )

            if batch_dates and min(batch_dates) < date_from_str:
                crossed_start_boundary = True
                logger.info(
                    "Reached orders older than requested %s from-date %s; stopping pagination",
                    date_field,
                    date_from_str,
                )
                break

            has_next_page = bool(page_info.get("hasNextPage", False))
            if not has_next_page:
                exhausted_history = True
                break
            if pages_fetched >= self.max_pages:
                raise IncompleteInvoiceScanError(
                    f"Incomplete {date_field} invoice scan: exceeded "
                    f"max_pages={self.max_pages}"
                )

            next_cursor = page_info.get("nextCursor")
            if next_cursor is None or (
                isinstance(next_cursor, str) and not next_cursor.strip()
            ):
                raise IncompleteInvoiceScanError(
                    f"Incomplete {date_field} invoice scan: hasNextPage=true without nextCursor"
                )
            cursor_key = (type(next_cursor).__name__, str(next_cursor))
            if cursor_key in seen_cursors:
                raise IncompleteInvoiceScanError(
                    f"Incomplete {date_field} invoice scan: repeated cursor {next_cursor!r}"
                )
            seen_cursors.add(cursor_key)
            cursor = next_cursor

        filtered_orders = [
            order
            for order in all_orders
            if date_from_str <= _order_scan_date(order, date_field) <= date_to_str
        ]
        self.last_fetch_stats = {
            "date_field": date_field,
            "pages_fetched": pages_fetched,
            "page_retry_count": page_retry_count,
            "scan_complete": bool(crossed_start_boundary or exhausted_history),
            "orders_fetched": len(filtered_orders),
        }
        if not self.last_fetch_stats["scan_complete"]:
            raise IncompleteInvoiceScanError(
                f"Incomplete {date_field} invoice scan: requested boundary was not proven"
            )

        logger.info(
            "Filtered %s orders by %s within %s..%s",
            len(filtered_orders),
            date_field,
            date_from_str,
            date_to_str,
        )
        return filtered_orders

    def fetch_orders_for_invoice_scan(
        self,
        date_from: datetime,
        date_to: datetime,
        *,
        include_purchase_dates: bool = True,
        include_recent_changes: bool,
        recent_change_date_from: Optional[datetime] = None,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Return a complete de-duplicated invoice scan.

        The purchase-date pass preserves the original rolling window. The
        last-change pass catches old orders when they are shipped after that
        window, which is the failure mode that left delayed VEVO/ROY orders
        permanently invisible.
        """
        if not include_purchase_dates and not include_recent_changes:
            raise ValueError("Invoice scan must include at least one date basis")

        purchase_orders: List[Dict[str, Any]] = []
        purchase_stats: Dict[str, Any] = {
            "pages_fetched": 0,
            "page_retry_count": 0,
            "scan_complete": True,
            "orders_fetched": 0,
        }
        if include_purchase_dates:
            purchase_orders = self.fetch_orders(
                date_from,
                date_to,
                date_field="pur_date",
            )
            purchase_stats = dict(self.last_fetch_stats)
        recent_change_orders: List[Dict[str, Any]] = []
        recent_change_stats: Dict[str, Any] = {
            "pages_fetched": 0,
            "page_retry_count": 0,
            "scan_complete": True,
            "orders_fetched": 0,
        }
        if include_recent_changes:
            recent_change_orders = self.fetch_orders(
                recent_change_date_from or date_from,
                date_to,
                date_field="last_change",
            )
            recent_change_stats = dict(self.last_fetch_stats)

        unique_orders: Dict[str, Dict[str, Any]] = {}
        for order in purchase_orders + recent_change_orders:
            identity = str(order.get("order_num") or order.get("id") or "").strip()
            if not identity:
                raise IncompleteInvoiceScanError(
                    "Invoice scan returned an order without order_num or id"
                )
            unique_orders[identity] = order

        stats = {
            "scan_complete": bool(
                purchase_stats.get("scan_complete")
                and recent_change_stats.get("scan_complete")
            ),
            "purchase_date_orders_fetched": len(purchase_orders),
            "recent_change_orders_fetched": len(recent_change_orders),
            "deduplicated_orders_fetched": len(unique_orders),
            "pages_fetched": int(purchase_stats.get("pages_fetched", 0))
            + int(recent_change_stats.get("pages_fetched", 0)),
            "page_retry_count": int(purchase_stats.get("page_retry_count", 0))
            + int(recent_change_stats.get("page_retry_count", 0)),
        }
        if not stats["scan_complete"]:
            raise IncompleteInvoiceScanError("Combined invoice scan did not complete")
        return list(unique_orders.values()), stats

    def fetch_order_invoice_guard(self, order_num: Any) -> Optional[Dict[str, Any]]:
        """Re-read one candidate immediately before mutation to prevent duplicate invoices."""
        normalized_order_num = str(order_num or "").strip()
        if not normalized_order_num:
            return None
        last_error: Optional[Exception] = None
        for attempt in range(1, self.page_retry_attempts + 1):
            try:
                result = self.client.execute(
                    ORDER_INVOICE_GUARD_QUERY,
                    variable_values={"order_num": normalized_order_num},
                )
                order = result.get("getOrder")
                if not isinstance(order, dict):
                    raise ValueError(
                        f"Pre-create guard did not return order {normalized_order_num}"
                    )
                returned_order_num = str(order.get("order_num") or "").strip()
                if returned_order_num != normalized_order_num:
                    raise ValueError(
                        "Pre-create guard returned mismatched order identity "
                        f"{returned_order_num or '<missing>'} for "
                        f"{normalized_order_num}"
                    )
                order["invoices"] = _validated_invoice_list(
                    order.get("invoices"),
                    context=f"Pre-create guard for order {normalized_order_num}",
                )
                status = order.get("status")
                if not isinstance(status, dict) or not str(
                    status.get("name") or ""
                ).strip():
                    raise ValueError(
                        "Pre-create guard returned an incomplete status "
                        f"for order {normalized_order_num}"
                    )
                if self.require_cod_payment and not isinstance(
                    order.get("price_elements"),
                    list,
                ):
                    raise ValueError(
                        "Pre-create guard returned incomplete payment metadata "
                        f"for order {normalized_order_num}"
                    )
                return order
            except Exception as exc:
                last_error = exc
                if attempt >= self.page_retry_attempts:
                    break
                delay_seconds = min(2 ** (attempt - 1), 5)
                logger.warning(
                    "Invoice detail guard failed for order %s (attempt %s/%s): "
                    "%s; retrying in %ss",
                    normalized_order_num,
                    attempt,
                    self.page_retry_attempts,
                    exc,
                    delay_seconds,
                )
                time.sleep(delay_seconds)
        logger.error(
            "Pre-create guard failed for order %s after %s attempts: %s",
            normalized_order_num,
            self.page_retry_attempts,
            last_error,
        )
        return None

    def fetch_latest_invoice_for_order(self, order_num: Any) -> Tuple[Optional[str], Optional[str]]:
        """Read the order again after invoice finalization and return its invoice id/number."""
        normalized_order_num = str(order_num or "").strip()
        if not normalized_order_num:
            return None, None

        try:
            result = self.client.execute(
                ORDER_INVOICE_QUERY,
                variable_values={"order_num": normalized_order_num},
            )
            order = result.get("getOrder")
            if not isinstance(order, dict):
                raise ValueError(
                    f"Invoice fallback did not return order {normalized_order_num}"
                )
            returned_order_num = str(order.get("order_num") or "").strip()
            if returned_order_num != normalized_order_num:
                raise ValueError(
                    "Invoice fallback returned mismatched order identity "
                    f"{returned_order_num or '<missing>'} for "
                    f"{normalized_order_num}"
                )
            invoices = _validated_invoice_list(
                order.get("invoices"),
                context=f"Invoice fallback for order {normalized_order_num}",
            )
            if not invoices:
                logger.warning("Order %s has no invoices after finalization fallback", normalized_order_num)
                return None, None

            invoice = invoices[-1] or {}
            invoice_id = invoice.get("id")
            invoice_num = invoice.get("invoice_num")
            if invoice_id:
                logger.info(
                    "Resolved invoice id %s for order %s via GraphQL fallback",
                    invoice_id,
                    normalized_order_num,
                )
            return (str(invoice_id) if invoice_id not in (None, "") else None), (
                str(invoice_num) if invoice_num not in (None, "") else None
            )
        except Exception as exc:
            logger.error("Failed to resolve invoice id for order %s after finalization: %s", normalized_order_num, exc)
            return None, None
    
    def filter_orders_for_invoice(self, orders: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
        """Filter orders that need invoice generation"""
        filtered_orders = []
        stats = {
            "skipped_zero_total_orders": 0,
            "skipped_non_cod_orders": 0,
            "skipped_before_automation_start": 0,
        }

        for order in orders:
            status = order.get("status", {}) or {}
            status_name = status.get("name", "").lower()
            invoices = order.get("invoices", []) or []
            has_invoice = len(invoices) > 0
            parsed_order_total = _parse_order_total_value(order)
            status_matches = _status_matches_invoice_generation(
                status_name,
                self.eligible_statuses,
            )
            if (
                self.automation_start_date
                and status_matches
                and not has_invoice
            ):
                purchase_date = _order_scan_date(order, "pur_date")
                if purchase_date < self.automation_start_date:
                    stats["skipped_before_automation_start"] += 1
                    logger.info(
                        "Order %s skipped - purchase date %s is before "
                        "automation start %s",
                        order.get("order_num"),
                        purchase_date,
                        self.automation_start_date,
                    )
                    continue
            if (
                parsed_order_total is None
                and status_matches
                and not has_invoice
            ):
                raise IncompleteInvoiceScanError(
                    f"Eligible order {order.get('order_num')} has an unknown total"
                )
            order_total_value = 0.0 if parsed_order_total is None else parsed_order_total

            if self.exclude_zero_total_orders and order_total_value <= 0:
                stats["skipped_zero_total_orders"] += 1
                logger.info(
                    "Order %s skipped - zero or negative total (%.2f)",
                    order.get("order_num"),
                    order_total_value,
                )
                continue

            if not status_matches or has_invoice:
                logger.debug(
                    "Order %s skipped - Status: %s, Has Invoice: %s",
                    order.get("order_num"),
                    status_name,
                    has_invoice,
                )
                continue

            if (
                self.require_cod_payment
            ):
                if "price_elements" not in order:
                    refreshed = self.fetch_order_invoice_guard(order.get("order_num"))
                    if refreshed is None:
                        raise IncompleteInvoiceScanError(
                            f"Eligible order {order.get('order_num')} payment "
                            "metadata could not be verified"
                        )
                    order.update(refreshed)
                    status_name = str(
                        (order.get("status") or {}).get("name") or ""
                    ).lower()
                    invoices = order.get("invoices") or []
                    parsed_order_total = _parse_order_total_value(order)
                    if parsed_order_total is None:
                        raise IncompleteInvoiceScanError(
                            f"Eligible order {order.get('order_num')} has an "
                            "unknown total after detail refresh"
                        )
                    if (
                        not _status_matches_invoice_generation(
                            status_name,
                            self.eligible_statuses,
                        )
                        or invoices
                    ):
                        logger.info(
                            "Order %s changed while verifying payment metadata; skipped",
                            order.get("order_num"),
                        )
                        continue
                    if self.exclude_zero_total_orders and parsed_order_total <= 0:
                        stats["skipped_zero_total_orders"] += 1
                        logger.info(
                            "Order %s skipped after detail refresh - zero or "
                            "negative total (%.2f)",
                            order.get("order_num"),
                            parsed_order_total,
                        )
                        continue
                if not _payment_elements(order):
                    raise IncompleteInvoiceScanError(
                        f"Eligible order {order.get('order_num')} has no payment metadata"
                    )
                if not _payment_matches_cod(
                    order,
                    self.cod_payment_ids,
                    self.cod_payment_patterns,
                ):
                    stats["skipped_non_cod_orders"] += 1
                    logger.info(
                        "Order %s skipped - payment is not cash on delivery",
                        order.get("order_num"),
                    )
                    continue

            order_total_value = (
                0.0
                if parsed_order_total is None
                else parsed_order_total
            )
            filtered_orders.append(order)
            logger.info(
                "Order %s matches criteria for invoice generation - Status: %s "
                "- Total: %.2f",
                order.get("order_num"),
                status_name,
                order_total_value,
            )

        return filtered_orders, stats
    
    def create_invoice(self, order: Dict[str, Any]) -> InvoiceCreationResult:
        """Create invoice for the order"""
        order_num = order.get('order_num')
        creation_result = InvoiceCreationResult(email_required=self.send_invoice_email_enabled)
        guard_order = self.fetch_order_invoice_guard(order_num)
        if guard_order is None:
            logger.error("  Invoice pre-create guard failed for order %s", order_num)
            return creation_result

        guard_invoices = guard_order.get("invoices") or []
        if guard_invoices:
            latest_invoice = guard_invoices[-1] or {}
            creation_result.already_present = True
            creation_result.email_required = False
            creation_result.invoice_id = (
                str(latest_invoice.get("id"))
                if latest_invoice.get("id") not in (None, "")
                else None
            )
            creation_result.invoice_num = (
                str(latest_invoice.get("invoice_num"))
                if latest_invoice.get("invoice_num") not in (None, "")
                else None
            )
            logger.info(
                "  Invoice already exists for order %s; mutation skipped",
                order_num,
            )
            return creation_result

        if self.automation_start_date:
            guard_purchase_date = _order_scan_date(guard_order, "pur_date")
            if guard_purchase_date < self.automation_start_date:
                creation_result.skipped_before_automation_start = True
                creation_result.email_required = False
                logger.info(
                    "  Order %s purchase date %s is before automation start %s; "
                    "mutation skipped",
                    order_num,
                    guard_purchase_date,
                    self.automation_start_date,
                )
                return creation_result

        guard_status_name = str((guard_order.get("status") or {}).get("name") or "")
        guard_total = _parse_order_total_value(guard_order)
        if guard_total is None:
            logger.error("  Invoice pre-create guard returned unknown total for order %s", order_num)
            return creation_result
        if self.require_cod_payment:
            if not _payment_elements(guard_order):
                logger.error(
                    "  Invoice pre-create guard returned no payment metadata for order %s",
                    order_num,
                )
                return creation_result
            if not _payment_matches_cod(
                guard_order,
                self.cod_payment_ids,
                self.cod_payment_patterns,
            ):
                creation_result.skipped_stale = True
                creation_result.email_required = False
                logger.info(
                    "  Order %s is no longer cash on delivery; skipped",
                    order_num,
                )
                return creation_result
        if (
            not _status_matches_invoice_generation(
                guard_status_name,
                self.eligible_statuses,
            )
            or (self.exclude_zero_total_orders and guard_total <= 0)
        ):
            creation_result.skipped_stale = True
            creation_result.email_required = False
            logger.info(
                "  Order %s is no longer invoice-eligible at mutation time; skipped",
                order_num,
            )
            return creation_result

        order = {**order, **guard_order}
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
        
        try:
            timestamp = int(time.time() * 1000)

            order_id = order.get('id')
            logger.debug(f"Order {order_num} has ID: {order_id}")

            headers = {
                'X-Requested-With': 'XMLHttpRequest',
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'Referer': f'{self.base_url}/erp/orders/orders/detail/{order_num}'
            }

            create_url = self.invoice_create_url.format(order_num=order_num)
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

            time.sleep(1)

            response = None
            finalize_url = ""
            urls_to_try = [('order_num', order_num)]
            if order_id:
                urls_to_try.append(('order_id', order_id))

            for url_type, identifier in urls_to_try:
                finalize_url = self.invoice_finalize_url.format(order_num=identifier)
                timestamp = int(time.time() * 1000)
                if self.arf_token:
                    finalize_url += f"?arf={self.arf_token}&_dc={timestamp}"
                else:
                    finalize_url += f"?_dc={timestamp}"

                logger.debug(f"Attempting to finalize invoice via {url_type}: {finalize_url}")
                response = self.web_session.post(finalize_url, headers=headers)

                if response.status_code == 405:
                    logger.debug("POST not allowed, trying GET")
                    response = self.web_session.get(finalize_url, headers=headers)

                if response.status_code == 200:
                    break
                if response.status_code == 400:
                    logger.debug(f"Got 400 error with {url_type}, trying next...")
                    continue

            if response is None:
                logger.error("  âś— Invoice creation failed before finalization response")
                return creation_result

            if response.status_code == 200:
                logger.debug(f"  Raw response: {response.text[:1000]}")
                response_payload = None
                try:
                    response_payload = response.json()
                    logger.debug(f"  JSON response: {response_payload}")
                except json.JSONDecodeError:
                    response_payload = None

                if isinstance(response_payload, dict):
                    if not response_payload.get('success'):
                        error_msg = response_payload.get('message') or response_payload.get('errors', {}).get('reason', 'Unknown error')
                        logger.error(f"  âś— Invoice creation failed: {error_msg}")
                        return creation_result
                    creation_result.created = True
                    creation_result.invoice_id = _extract_invoice_id_from_payload(response_payload)
                    creation_result.invoice_num = _extract_invoice_num_from_payload(response_payload)
                elif 'success' in response.text.lower() or 'invoice' in response.text.lower():
                    creation_result.created = True
                    creation_result.invoice_id = _extract_invoice_id_from_text(response.text, response.url)
                    logger.info("  âś“ Invoice likely created (HTML response)")
                else:
                    logger.error("  âś— Invoice creation failed (HTML response)")
                    logger.debug(f"  âś— HTML response: {response.text[:500]}")
                    return creation_result

                if not creation_result.invoice_id:
                    fallback_invoice_id, fallback_invoice_num = self.fetch_latest_invoice_for_order(order_num)
                    creation_result.invoice_id = fallback_invoice_id
                    creation_result.invoice_num = creation_result.invoice_num or fallback_invoice_num

                logger.info(f"  âś“ Invoice created: {creation_result.invoice_num or creation_result.invoice_id or 'unknown'}")

                if not self.send_invoice_email_enabled:
                    logger.info("  Invoice email notification skipped by configuration")
                    return creation_result

                if not creation_result.invoice_id:
                    creation_result.email_error = "missing_invoice_id"
                    logger.error("  âś— Invoice email notification not sent: missing invoice id")
                    return creation_result

                if self.send_invoice_email(creation_result.invoice_id):
                    creation_result.email_sent = True
                    logger.info("  âś“ Invoice email notification sent successfully to customer")
                else:
                    creation_result.email_error = "send_failed"
                    logger.error("  âś— Failed to send invoice email notification")
                return creation_result

            if response.status_code == 400:
                logger.error(f"  âś— Bad request (400) - URL: {finalize_url}")
                try:
                    error_detail = response.json()
                    logger.error(f"  âś— Error details: {error_detail}")
                except Exception:
                    logger.error(f"  âś— Error response: {response.text[:500]}")
                return creation_result

            logger.error(f"  âś— Invoice creation failed with status {response.status_code}")
            logger.error(f"  âś— Response: {response.text[:500]}")
            return creation_result
                            
        except Exception as e:
            logger.error(f"  âś— Error creating invoice: {e}")
            return creation_result
    
    def send_invoice_email(self, invoice_id: str) -> bool:
        """Send invoice email notification to customer"""
        try:
            import time
            timestamp = int(time.time() * 1000)
            
            send_url = self.invoice_send_url.format(invoice_id=invoice_id)
            if self.arf_token:
                send_url += f"?arf={self.arf_token}&_dc={timestamp}"
            else:
                send_url += f"?_dc={timestamp}"
            
            logger.debug(f"Sending invoice email via: {send_url}")
            headers = {
                'X-Requested-With': 'XMLHttpRequest',
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'Referer': f'{self.base_url}/erp/orders/invoices/detail/{invoice_id}',
            }
            response = self.web_session.post(send_url, headers=headers)
            if response.status_code == 405:
                logger.debug("Invoice email POST not allowed, trying GET")
                response = self.web_session.get(send_url, headers=headers)
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
            logger.error("âś— No web session available - cannot create invoices")
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
            logger.error("âś— Web session validation failed - cannot proceed")
            logger.error("=" * 60)
            logger.error("Please check your login credentials in .env file")
            logger.error("=" * 60)
            return
        
        # Fetch orders using GraphQL API
        logger.info("Fetching orders from GraphQL API...")
        orders, _scan_stats = self.fetch_orders_for_invoice_scan(
            date_from,
            date_to,
            include_recent_changes=True,
        )
        logger.info(f"Total orders fetched: {len(orders)}")
        
        # Filter orders that need invoices
        orders_for_invoice, filter_stats = self.filter_orders_for_invoice(orders)
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
            total = sum(_coerce_order_total_value(order) for order in orders_for_invoice)
            logger.info(f"  Total amount: â‚¬{total:.2f}")
            logger.info(f"  Skipped zero-total orders: {filter_stats.get('skipped_zero_total_orders', 0)}")
            logger.info(
                "  Skipped before automation start: %s",
                filter_stats.get("skipped_before_automation_start", 0),
            )
            if self.web_session:
                logger.info("  Web session: Available (invoices would be created)")
            else:
                logger.info("  Web session: Not available (manual processing required)")
            logger.info("=" * 60)
            return
        
        # Process orders for invoice creation
        success_count = 0
        failed_count = 0
        email_failed_count = 0
        total_amount = 0.0
        processed_orders = []
        
        for order in orders_for_invoice:
            order_num = order.get('order_num')
            customer = order.get('customer', {})
            customer_email = customer.get('email', 'N/A')
            
            result = self.create_invoice(order)
            if (
                result.already_present
                or result.skipped_stale
                or result.skipped_before_automation_start
            ):
                logger.info(
                    "Order %s completed as idempotent no-op "
                    "(already_present=%s skipped_stale=%s "
                    "skipped_before_automation_start=%s)",
                    order_num,
                    result.already_present,
                    result.skipped_stale,
                    result.skipped_before_automation_start,
                )
            elif result.created:
                success_count += 1
                # Try to extract numeric value from formatted amount
                order_sum = order.get('sum', {}).get('value', 0)
                if order_sum:
                    total_amount += float(order_sum)
                if result.email_required and not result.email_sent:
                    email_failed_count += 1
                else:
                    processed_orders.append({
                        'order_num': order_num,
                        'email': customer_email,
                        'amount': order.get('sum', {}).get('formatted', 'N/A')
                    })
            else:
                failed_count += 1
        
        logger.info("=" * 60)
        logger.info(f"Invoice processing complete:")
        logger.info(f"  âś“ Invoices created: {success_count}")
        if failed_count > 0:
            logger.info(f"  âś— Failed: {failed_count}")
        if email_failed_count > 0:
            logger.info(f"  âś— Invoice emails failed: {email_failed_count}")
        logger.info(f"  Skipped zero-total orders: {filter_stats.get('skipped_zero_total_orders', 0)}")
        logger.info(
            "  Skipped before automation start: %s",
            filter_stats.get("skipped_before_automation_start", 0),
        )
        logger.info(f"  Total amount: â‚¬{total_amount:.2f}")
        logger.info("=" * 60)
        
        if success_count > 0:
            logger.info("Email notifications sent to:")
            for order in processed_orders:
                logger.info(f"  â€˘ Order {order['order_num']}: {order['email']} ({order['amount']})")
            logger.info("=" * 60)


def run_invoice_generation(
    project_name: str,
    date_from: Union[str, datetime],
    date_to: Union[str, datetime],
    dry_run: bool = False,
    no_web_login: bool = False,
    reconcile: bool = False,
    max_creations: Optional[int] = None,
) -> InvoiceRunSummary:
    project_name = (project_name or BASE_DEFAULT_PROJECT).strip() or BASE_DEFAULT_PROJECT
    os.environ["REPORT_PROJECT"] = project_name
    load_project_env(project_name, logger=logger)

    project_settings = load_project_settings(project_name)
    invoice_settings = resolve_invoice_generation_settings(project_settings)
    api_url = resolve_biznisweb_api_url(project_name, project_settings)
    api_token = os.getenv("BIZNISWEB_API_TOKEN")
    base_url = derive_biznisweb_base_url(api_url)
    web_username = os.getenv("BIZNISWEB_USERNAME")
    web_password = os.getenv("BIZNISWEB_PASSWORD")

    if not api_token:
        raise RuntimeError(f"BIZNISWEB_API_TOKEN not found for project '{project_name}'")

    from_dt = date_from if isinstance(date_from, datetime) else datetime.strptime(str(date_from), "%Y-%m-%d")
    to_dt = date_to if isinstance(date_to, datetime) else datetime.strptime(str(date_to), "%Y-%m-%d")

    if from_dt > to_dt:
        raise ValueError(f"from_date ({from_dt:%Y-%m-%d}) cannot be after to_date ({to_dt:%Y-%m-%d})")

    generator = InvoiceGenerator(
        api_url,
        api_token,
        base_url,
        None if no_web_login else web_username,
        None if no_web_login else web_password,
        exclude_zero_total_orders=invoice_settings["exclude_zero_total_orders"],
        eligible_statuses=invoice_settings["eligible_statuses"],
        send_invoice_email=invoice_settings["send_invoice_email"],
        page_retry_attempts=invoice_settings["page_retry_attempts"],
        require_cod_payment=invoice_settings["require_cod_payment"],
        cod_payment_ids=invoice_settings["cod_payment_ids"],
        cod_payment_patterns=invoice_settings["cod_payment_patterns"],
        max_pages=invoice_settings["max_pages"],
        automation_start_date=invoice_settings["automation_start_date"],
    )

    summary = InvoiceRunSummary(
        project=project_name,
        date_from=from_dt.strftime("%Y-%m-%d"),
        date_to=to_dt.strftime("%Y-%m-%d"),
        dry_run=dry_run,
        scan_mode="reconciliation" if reconcile else "regular",
    )

    if not generator.web_session and not dry_run:
        raise RuntimeError(
            f"Invoice generation for project '{project_name}' requires BIZNISWEB_USERNAME and BIZNISWEB_PASSWORD"
        )

    if generator.web_session and not dry_run:
        logger.info("Validating web session...")
        if not generator.validate_session():
            raise RuntimeError(f"BiznisWeb web session validation failed for project '{project_name}'")

    logger.info("Fetching orders from GraphQL API...")
    scan_from_dt = from_dt
    if invoice_settings["automation_start_date"]:
        automation_start_dt = datetime.strptime(
            invoice_settings["automation_start_date"],
            "%Y-%m-%d",
        )
        if from_dt.tzinfo is not None:
            automation_start_dt = automation_start_dt.replace(
                tzinfo=from_dt.tzinfo,
            )
        if scan_from_dt < automation_start_dt:
            scan_from_dt = automation_start_dt
            logger.info(
                "Invoice scan lower bound clamped from %s to automation start %s",
                from_dt.strftime("%Y-%m-%d"),
                invoice_settings["automation_start_date"],
            )

    if scan_from_dt > to_dt:
        logger.info(
            "Invoice scan skipped because requested end %s is before "
            "automation start %s",
            to_dt.strftime("%Y-%m-%d"),
            invoice_settings["automation_start_date"],
        )
        orders = []
        scan_stats = {
            "scan_complete": True,
            "purchase_date_orders_fetched": 0,
            "recent_change_orders_fetched": 0,
            "deduplicated_orders_fetched": 0,
            "pages_fetched": 0,
            "page_retry_count": 0,
        }
    else:
        orders, scan_stats = generator.fetch_orders_for_invoice_scan(
            scan_from_dt,
            to_dt,
            include_purchase_dates=reconcile,
            include_recent_changes=(
                invoice_settings["include_recent_changes"] and not reconcile
            ),
            recent_change_date_from=scan_from_dt,
        )
    summary.total_orders_fetched = len(orders)
    summary.scan_complete = bool(scan_stats["scan_complete"])
    summary.purchase_date_orders_fetched = int(
        scan_stats["purchase_date_orders_fetched"]
    )
    summary.recent_change_orders_fetched = int(
        scan_stats["recent_change_orders_fetched"]
    )
    summary.pages_fetched = int(scan_stats["pages_fetched"])
    summary.page_retry_count = int(scan_stats["page_retry_count"])
    logger.info("Total orders fetched: %s", summary.total_orders_fetched)

    orders_for_invoice, filter_stats = generator.filter_orders_for_invoice(orders)
    summary.matched_orders = len(orders_for_invoice)
    summary.skipped_zero_total_orders = filter_stats.get("skipped_zero_total_orders", 0)
    summary.skipped_non_cod_orders = filter_stats.get("skipped_non_cod_orders", 0)
    summary.skipped_before_automation_start = filter_stats.get(
        "skipped_before_automation_start",
        0,
    )
    logger.info("Orders matching criteria: %s", summary.matched_orders)
    validate_invoice_creation_limit(
        summary.matched_orders,
        max_creations,
        dry_run=dry_run,
    )

    if dry_run:
        summary.total_amount = sum(_coerce_order_total_value(order) for order in orders_for_invoice)
        logger.info(
            "DRY RUN summary - matched=%s total_amount=%.2f skipped_zero_total=%s "
            "skipped_before_automation_start=%s",
            summary.matched_orders,
            summary.total_amount,
            summary.skipped_zero_total_orders,
            summary.skipped_before_automation_start,
        )
        return summary

    for order in orders_for_invoice:
        result = generator.create_invoice(order)
        if result.already_present:
            summary.already_present_invoices += 1
        elif result.skipped_before_automation_start:
            summary.skipped_before_automation_start += 1
        elif result.skipped_stale:
            summary.skipped_stale_orders += 1
        elif result.created:
            summary.created_invoices += 1
            summary.total_amount += _coerce_order_total_value(order)
            if result.email_required:
                if result.email_sent:
                    summary.emailed_invoices += 1
                else:
                    summary.failed_invoice_emails += 1
                    if result.email_error == "missing_invoice_id":
                        summary.missing_invoice_ids += 1
        else:
            summary.failed_invoices += 1

    logger.info(
        (
            "Invoice run summary - project=%s matched=%s created=%s failed=%s "
            "emailed=%s email_failed=%s missing_invoice_ids=%s skipped_zero_total=%s total_amount=%.2f"
            " already_present=%s skipped_stale=%s"
            " scan_mode=%s scan_complete=%s purchase_fetched=%s recent_change_fetched=%s"
            " skipped_non_cod=%s skipped_before_automation_start=%s "
            "pages_fetched=%s page_retries=%s"
        ),
        summary.project,
        summary.matched_orders,
        summary.created_invoices,
        summary.failed_invoices,
        summary.emailed_invoices,
        summary.failed_invoice_emails,
        summary.missing_invoice_ids,
        summary.skipped_zero_total_orders,
        summary.total_amount,
        summary.already_present_invoices,
        summary.skipped_stale_orders,
        summary.scan_mode,
        summary.scan_complete,
        summary.purchase_date_orders_fetched,
        summary.recent_change_orders_fetched,
        summary.skipped_non_cod_orders,
        summary.skipped_before_automation_start,
        summary.pages_fetched,
        summary.page_retry_count,
    )
    return summary


def main():
    """Main function to handle command line arguments and run the invoice generator"""
    parser = argparse.ArgumentParser(description='Generate invoices for BizniWeb orders')
    parser.add_argument(
        '--project',
        default=os.getenv('REPORT_PROJECT', BASE_DEFAULT_PROJECT),
        help='Project name (loads projects/<project>/.env and settings.json)'
    )
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
    project_name = (args.project or BASE_DEFAULT_PROJECT).strip() or BASE_DEFAULT_PROJECT
    project_settings = load_project_settings(project_name)
    invoice_settings = resolve_invoice_generation_settings(project_settings)

    if args.to_date:
        date_to = datetime.strptime(args.to_date, '%Y-%m-%d')
    else:
        date_to = datetime.now()

    if args.from_date:
        date_from = datetime.strptime(args.from_date, '%Y-%m-%d')
    else:
        default_from_str, _ = resolve_invoice_date_window(date_to, invoice_settings["lookback_days"])
        date_from = datetime.strptime(default_from_str, "%Y-%m-%d")

    summary = run_invoice_generation(
        project_name=project_name,
        date_from=date_from,
        date_to=date_to,
        dry_run=args.dry_run,
        no_web_login=args.no_web_login,
    )
    logger.info(
        (
            "Invoice run summary - project=%s matched=%s created=%s failed=%s "
            "emailed=%s email_failed=%s missing_invoice_ids=%s skipped_zero_total=%s "
            "skipped_before_automation_start=%s"
        ),
        summary.project,
        summary.matched_orders,
        summary.created_invoices,
        summary.failed_invoices,
        summary.emailed_invoices,
        summary.failed_invoice_emails,
        summary.missing_invoice_ids,
        summary.skipped_zero_total_orders,
        summary.skipped_before_automation_start,
    )
    if not args.dry_run and (summary.failed_invoices or summary.failed_invoice_emails):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
