#!/usr/bin/env python3
"""
Generate invoices for orders with specific criteria in BizniWeb
"""

import os
import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Any, Iterable, Optional, Tuple, Union
import json
import re
import unicodedata
import time
import math
from contextlib import nullcontext
from zoneinfo import ZoneInfo

from invoice_automation_state import (
    AutomationLeaseBusy,
    AutomationStateError,
    build_automation_state_store,
    iso_utc,
    parse_utc,
    utc_now,
)

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
DEFAULT_INVOICE_ELIGIBLE_STATUSES = ("Odoslaná",)
DEFAULT_EXISTING_INVOICE_TARGET_STATUS_NAME = "Platba online - zaplatené"
DEFAULT_EXISTING_INVOICE_SOURCE_STATUSES = (
    "Čaká na vybavenie",
    "Čaká na úhradu",
    "Platba online - platnosť vypršala",
    "Platba online - platba zamietnutá",
    "GP WebPay - platba selhala",
    "GoPay - čeká se",
    "GoPay - platnost vypršela",
    "GoPay - zrušeno",
    "GoPay - platba selhala",
    "GoPay - platba předautorizována",
    "GoPay - platba vytvořená",
    "GoPay - platebni metoda potvrzena",
    "Besteron - platba zlyhala",
    "Besteron - platba expirovala",
    "Besteron - vytvorená",
    "Besteron - čaká na potvrdenie",
    "Besteron - prebieha",
    "Besteron - potrebná manuálna pozornosť",
    "Besteron - neplatný",
    "Besteron - zrušené",
    "Besteron - vypršal časový limit",
    "Besteron - chyba",
    "24 pay - Nezrealizovaná",
    "24 pay - Platba nebola potvrdená",
    "24 pay - Platba autorizovaná",
    "Stripe - cancelled",
    "Stripe - expired",
    "Stripe - unpaid",
)

# GraphQL query to fetch orders with specific criteria
ORDER_QUERY = gql("""
query GetOrders($status: Int, $changed_from: DateTime, $params: OrderParams) {
  getOrderList(status: $status, changed_from: $changed_from, include_blocking: true, params: $params) {
    data {
      id
      order_num
      pur_date
      last_change
      blocked
      status {
        id
        name
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
            code
        }
      }
    }
    pageInfo {
      hasNextPage
      nextCursor
      totalPages
    }
  }
}
""")

ORDER_INVOICE_QUERY = gql("""
query GetOrderInvoices($order_num: String!) {
  getOrder(order_num: $order_num) {
    id
    order_num
    pur_date
    last_change
    blocked
    status {
      id
      name
    }
    invoices {
      id
      invoice_num
    }
    sum { value formatted is_net_price currency { code } }
  }
}
""")

LIST_ORDER_STATUSES_QUERY = gql("""
query ListOrderStatuses($lang_code: CountryCodeAlpha2!) {
  listOrderStatuses(lang_code: $lang_code, only_active: true) {
    id
    name
  }
}
""")

CHANGE_ORDER_STATUS_MUTATION = gql("""
mutation ChangeOrderStatus($order_num: String!, $status_id: Int!) {
  changeOrderStatus(order_num: $order_num, status_id: $status_id) {
    order_num
    status {
      id
      name
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
    skipped_zero_total_orders: int = 0
    invoice_status_reconciliation_enabled: bool = False
    invoice_status_reconciliation_candidates: int = 0
    reconciled_invoice_statuses: int = 0
    failed_invoice_status_reconciliations: int = 0
    skipped_invoice_status_reconciliations_after_recheck: int = 0
    invoice_status_reconciliation_target_name: str = ""
    invoice_status_reconciliation_target_id: Optional[int] = None
    total_amount: float = 0.0
    invoice_scan_complete: bool = False
    invoice_scan_pages: int = 0
    invoice_scan_all_ages: bool = False
    skipped_blocked_orders: int = 0
    skipped_after_recheck_orders: int = 0
    pending_invoice_emails: int = 0
    ambiguous_invoice_operations: int = 0
    skipped_locked: bool = False
    recovered_invoices: int = 0
    invoice_status_review_required: int = 0
    pending_invoice_operations: int = 0
    full_scan_age_hours: float = 0.0


@dataclass
class InvoiceCreationResult:
    created: bool = False
    invoice_id: Optional[str] = None
    invoice_num: Optional[str] = None
    email_required: bool = True
    email_sent: bool = False
    email_error: str = ""
    skipped: bool = False
    ambiguous: bool = False
    recovered: bool = False

    def __bool__(self) -> bool:
        return self.created and (not self.email_required or self.email_sent)


def resolve_invoice_generation_settings(project_settings: Dict[str, Any]) -> Dict[str, Any]:
    raw_settings = project_settings.get("invoice_generation") or {}
    raw_reconciliation = raw_settings.get("existing_invoice_status_reconciliation") or {}
    raw_lookback_days = raw_settings.get("lookback_days", DEFAULT_INVOICE_LOOKBACK_DAYS)
    try:
        lookback_days = int(raw_lookback_days)
    except (TypeError, ValueError):
        lookback_days = DEFAULT_INVOICE_LOOKBACK_DAYS

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

    raw_reconciliation_statuses = raw_reconciliation.get(
        "source_statuses",
        DEFAULT_EXISTING_INVOICE_SOURCE_STATUSES,
    )
    if isinstance(raw_reconciliation_statuses, str):
        raw_reconciliation_statuses = [raw_reconciliation_statuses]
    reconciliation_source_statuses = [
        str(status).strip()
        for status in raw_reconciliation_statuses
        if str(status or "").strip()
    ]
    if not reconciliation_source_statuses:
        reconciliation_source_statuses = list(DEFAULT_EXISTING_INVOICE_SOURCE_STATUSES)

    raw_reconciliation_target_id = raw_reconciliation.get("target_status_id")
    reconciliation_target_id = (
        None
        if raw_reconciliation_target_id in (None, "")
        else int(raw_reconciliation_target_id)
    )

    return {
        "enabled": bool(raw_settings.get("enabled", False)),
        "lookback_days": max(1, lookback_days),
        "exclude_zero_total_orders": bool(raw_settings.get("exclude_zero_total_orders", True)),
        "eligible_statuses": eligible_statuses,
        "send_invoice_email": bool(raw_settings.get("send_invoice_email", True)),
        "all_age_backlog_enabled": bool(raw_settings.get("all_age_backlog_enabled", False)),
        "safety_state_enabled": bool(raw_settings.get("safety_state_enabled", False)),
        "full_scan_interval_hours": max(1, int(raw_settings.get("full_scan_interval_hours", 24))),
        "scan_max_pages": max(1, int(raw_settings.get("scan_max_pages", 5000))),
        "page_delay_seconds": max(0.0, float(raw_settings.get("page_delay_seconds", 1))),
        "read_attempts": max(1, min(5, int(raw_settings.get("read_attempts", 4)))),
        "existing_invoice_status_reconciliation": {
            "enabled": bool(raw_reconciliation.get("enabled", False)),
            "source_statuses": reconciliation_source_statuses,
            "target_status_name": str(
                raw_reconciliation.get("target_status_name")
                or DEFAULT_EXISTING_INVOICE_TARGET_STATUS_NAME
            ),
            "target_status_id": reconciliation_target_id,
            "lang_code": str(raw_reconciliation.get("lang_code") or "SK"),
        },
    }


def resolve_invoice_date_window(reference_date: Union[str, datetime], lookback_days: int) -> Tuple[str, str]:
    if isinstance(reference_date, datetime):
        to_date = reference_date
    else:
        to_date = datetime.strptime(str(reference_date), "%Y-%m-%d")

    safe_lookback_days = max(1, int(lookback_days))
    from_date = to_date - timedelta(days=safe_lookback_days - 1)
    return from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d")


def _coerce_order_total_value(order: Dict[str, Any]) -> float:
    order_sum = order.get("sum", {}) or {}
    raw_value = order_sum.get("value")
    if raw_value not in (None, ""):
        try:
            return float(raw_value)
        except (TypeError, ValueError):
            pass

    formatted = str(order_sum.get("formatted") or "").strip()
    if not formatted:
        return 0.0

    normalized = re.sub(r"[^0-9,.\-]", "", formatted)
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
        return 0.0


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


def _has_final_invoice(order: Dict[str, Any]) -> bool:
    return any(invoice and invoice.get("id") for invoice in (order.get("invoices") or []))


def _is_existing_invoice_status_reconciliation_candidate(
    order: Dict[str, Any],
    reconciliation_settings: Dict[str, Any],
) -> bool:
    if not reconciliation_settings.get("enabled") or not _has_final_invoice(order):
        return False
    status_name = str((order.get("status") or {}).get("name") or "")
    source_statuses = _normalized_invoice_statuses(
        reconciliation_settings.get("source_statuses") or DEFAULT_EXISTING_INVOICE_SOURCE_STATUSES
    )
    return _normalize_status_text(status_name) in source_statuses


def _resolve_existing_invoice_target_status_id(
    client: Client,
    reconciliation_settings: Dict[str, Any],
) -> int:
    target_name = str(
        reconciliation_settings.get("target_status_name")
        or DEFAULT_EXISTING_INVOICE_TARGET_STATUS_NAME
    )
    configured_id = reconciliation_settings.get("target_status_id")
    result = client.execute(
        LIST_ORDER_STATUSES_QUERY,
        variable_values={"lang_code": str(reconciliation_settings.get("lang_code") or "SK")},
    )
    target_normalized = _normalize_status_text(target_name)
    for row in (result.get("listOrderStatuses") or []):
        if not row:
            continue
        row_id = int(row.get("id") or 0)
        row_name = str(row.get("name") or "")
        row_name_normalized = _normalize_status_text(row_name)
        if configured_id and row_id == int(configured_id):
            if row_name_normalized != target_normalized:
                raise RuntimeError(
                    f"Configured invoice reconciliation status_id={configured_id} resolves to "
                    f"'{row_name}', expected '{target_name}'."
                )
            return row_id
        if configured_id is None and row_name_normalized == target_normalized:
            return row_id
    raise RuntimeError(f"Invoice reconciliation target status '{target_name}' not found in BiznisWeb.")


def reconcile_existing_invoice_statuses(
    client: Client, orders: List[Dict[str, Any]], reconciliation_settings: Dict[str, Any], *,
    dry_run: bool, journal: Any = None, creditnote_order_numbers: Optional[set[str]] = None,
    read_order: Optional[Callable[[str], Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    from order_status_safety import (
        change_status_verified, decide_recovery, fetch_order_safety_context, status_write_block_reason,
    )

    enabled = bool(reconciliation_settings.get("enabled"))
    result = {"enabled": enabled, "candidates": 0, "reconciled": 0, "failed": 0,
              "skipped_after_recheck": 0, "review_required": 0,
              "target_status_name": "", "target_status_id": None}
    if not enabled:
        return result
    target_name = str(reconciliation_settings.get("target_status_name") or DEFAULT_EXISTING_INVOICE_TARGET_STATUS_NAME)
    target_id = _resolve_existing_invoice_target_status_id(client, reconciliation_settings)
    result.update(target_status_name=target_name, target_status_id=target_id)
    candidates = [order for order in orders
                  if _is_existing_invoice_status_reconciliation_candidate(order, reconciliation_settings)]
    result["candidates"] = len(candidates)
    if journal:
        records = journal.snapshot()["orders"]
        source_names = _normalized_invoice_statuses(reconciliation_settings.get("source_statuses") or DEFAULT_EXISTING_INVOICE_SOURCE_STATUSES)
        for order in orders:
            number = str(order.get("order_num") or "")
            if ((records.get(number, {}).get("status_review") or {}).get("state") == "open"
                    and _normalize_status_text((order.get("status") or {}).get("name", "")) not in source_names):
                journal.update_order(number, status_review={"state": "closed", "reason": "status_resolved_elsewhere"})
    for order in candidates:
        order_num = str(order.get("order_num") or "").strip()
        try:
            if journal:
                journal.assert_owned()
            refreshed = read_order(order_num) if read_order else fetch_order_safety_context(client, order_num)
            if not _is_existing_invoice_status_reconciliation_candidate(refreshed, reconciliation_settings):
                result["skipped_after_recheck"] += 1
                continue
            record = journal.get_order(order_num) if journal else {}
            creditnote_status = ("unknown" if creditnote_order_numbers is None
                                 else "present" if order_num in creditnote_order_numbers else "clear")
            decision = decide_recovery(
                refreshed, reconciliation_settings.get("source_statuses") or DEFAULT_EXISTING_INVOICE_SOURCE_STATUSES,
                paid_target=target_name, creditnote_status=creditnote_status,
                verified_previous_status=record.get("verified_fulfillment_status"),
            )
            if decision.action == "review":
                result["review_required"] += 1
                if journal:
                    journal.update_order(order_num, status_review={"state": "open", "reason": decision.reason})
                logger.warning("Invoice-backed status requires review: order=%s reason=%s", order_num, decision.reason)
                continue
            if decision.action == "skip":
                result["skipped_after_recheck"] += 1
                continue
            block_reason = status_write_block_reason(record)
            if block_reason:
                result["review_required"] += 1
                if journal:
                    journal.update_order(order_num, status_review={"state": "open", "reason": block_reason})
                logger.warning("Repeated or uncertain status correction requires review: order=%s", order_num)
                continue
            if dry_run:
                continue
            if journal is None:
                raise AutomationStateError("Status correction requires the shared durable lease")
            actual_target = decision.target_status_name
            actual_id = target_id if actual_target == target_name else _resolve_existing_invoice_target_status_id(
                client, {"target_status_name": actual_target, "lang_code": reconciliation_settings.get("lang_code", "SK")})
            journal.assert_owned()
            operation = {"state": "pending", "source_status_id": refreshed["status"]["id"],
                         "source_status_name": refreshed["status"]["name"],
                         "last_change": refreshed.get("last_change"), "target_status_id": actual_id,
                         "target_status_name": actual_target, "reason": decision.reason}
            journal.update_order(order_num, status_mutation=operation)
            try:
                change_status_verified(client, order_num, actual_id, actual_target, silent=True)
            except Exception:
                journal.update_order(order_num, status_mutation={**operation, "state": "uncertain"})
                raise
            journal.update_order(order_num, status_mutation={**operation, "state": "verified"},
                                 status_review={"state": "closed", "reason": "verified_correction"})
            result["reconciled"] += 1
        except Exception as exc:
            result["failed"] += 1
            logger.error("Invoice status reconciliation failed for order %s (%s)", order_num, type(exc).__name__)
    if journal:
        result["review_required"] = sum(
            (record.get("status_review") or {}).get("state") == "open"
            for record in journal.snapshot()["orders"].values()
        )
    return result


def _order_purchase_date(order: Dict[str, Any]) -> str:
    pur_date = str(order.get("pur_date") or "")
    if " " in pur_date:
        pur_date = pur_date.split(" ", 1)[0]
    return pur_date


def _redact_headers(headers: Dict[str, Any]) -> Dict[str, Any]:
    sanitized = dict(headers)
    for key in list(sanitized.keys()):
        if key.lower() in {"bw-api-key", "authorization", "x-api-key"}:
            sanitized[key] = "[redacted]"
    return sanitized


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
        scan_max_pages: int = 5000,
        page_delay_seconds: float = 1.0,
        read_attempts: int = 4,
    ):
        """Initialize the invoice generator with API credentials"""
        transport = RequestsHTTPTransport(
            url=api_url,
            headers={'BW-API-Key': f'Token {api_token}'},
            verify=True,
            retries=0,
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
        self.scan_max_pages = scan_max_pages
        self.page_delay_seconds = page_delay_seconds
        self.read_attempts = read_attempts
        self.scan_pages = 0
        self.eligible_status_ids: set[int] = set()
        self.operation_journal = None
        self.last_email_outcome = "failed"
        self._last_read_at = 0.0
        self._last_lease_renewal_at = 0.0
        
        # Initialize web session if credentials provided
        if username and password:
            # Web GET fallbacks can mutate too; never replay an invoice request.
            self.web_session = build_retry_session(timeout=WEB_TIMEOUT, total=0)
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
                    except Exception:
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
    
    def execute_read(self, query: Any, variables: Dict[str, Any]) -> Dict[str, Any]:
        """Retry only reads; the transport used for writes has retries disabled."""
        from graphql import OperationType

        document = getattr(query, "document", query)
        operations = [node for node in document.definitions if hasattr(node, "operation")]
        if not operations or any(node.operation != OperationType.QUERY for node in operations):
            raise ValueError("Invoice execute_read only accepts queries")
        for attempt in range(self.read_attempts):
            if self.operation_journal and time.monotonic() - self._last_lease_renewal_at >= 30:
                self.operation_journal.assert_owned()
                self._last_lease_renewal_at = time.monotonic()
            remaining = self.page_delay_seconds - (time.monotonic() - self._last_read_at)
            if remaining > 0:
                time.sleep(remaining)
            self._last_read_at = time.monotonic()
            try:
                result = self.client.execute(query, variable_values=variables)
                if not isinstance(result, dict):
                    raise RuntimeError("BiznisWeb returned an invalid read response")
                return result
            except Exception:
                if attempt + 1 == self.read_attempts:
                    raise
                # FLOX can return quota errors inside HTTP 200 GraphQL responses.
                # Short 1s retries extend that throttle instead of recovering.
                time.sleep(min(30, 10 * (attempt + 1)))
        raise RuntimeError("Invoice read retries exhausted")

    def resolve_eligible_status_ids(self) -> set[int]:
        result = self.execute_read(LIST_ORDER_STATUSES_QUERY, {"lang_code": "SK"})
        rows = result.get("listOrderStatuses")
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise RuntimeError("Incomplete invoice status catalog")
        found: set[int] = set()
        for requested in self.eligible_statuses:
            matches = [int(row["id"]) for row in rows
                       if _normalize_status_text(row.get("name", "")) == _normalize_status_text(requested)]
            if len(matches) != 1 or matches[0] <= 0:
                raise RuntimeError("Invoice eligible status is missing or ambiguous")
            found.add(matches[0])
        self.eligible_status_ids = found
        return found

    @staticmethod
    def _validate_order_for_invoice_read(order: Any) -> None:
        if (not isinstance(order, dict) or not str(order.get("order_num") or "").strip()
                or not isinstance(order.get("status"), dict)
                or not order["status"].get("id")
                or "invoices" not in order
                or (order["invoices"] is not None and not isinstance(order["invoices"], list))
                or any(not isinstance(inv, dict) or not inv.get("id") for inv in (order["invoices"] or []))
                or not isinstance(order.get("blocked"), bool)
                or not isinstance(order.get("sum"), dict)
                or order["sum"].get("value") is None):
            raise RuntimeError("Incomplete invoice order evidence; no mutations are allowed")
        # FLOX returns explicit null for a present, empty nullable collection.
        # A missing field or a GraphQL error is still an incomplete response.
        if order["invoices"] is None:
            order["invoices"] = []
        value = float(order["sum"]["value"])
        if not math.isfinite(value):
            raise RuntimeError("Invalid non-finite invoice order amount")

    def fetch_order_for_invoice(self, order_num: Any) -> Dict[str, Any]:
        requested = str(order_num or "").strip()
        order = self.execute_read(ORDER_INVOICE_QUERY, {"order_num": requested}).get("getOrder")
        self._validate_order_for_invoice_read(order)
        if str(order["order_num"]) != requested:
            raise RuntimeError("Invoice recheck returned another order")
        return order

    def fetch_order_safety_context(self, order_num: str) -> Dict[str, Any]:
        from order_status_safety import ORDER_SAFETY_QUERY

        requested = str(order_num)
        order = self.execute_read(ORDER_SAFETY_QUERY, {"order_num": requested}).get("getOrder")
        if not isinstance(order, dict) or str(order.get("order_num") or "") != requested:
            raise RuntimeError("Order safety recheck returned an invalid order identity")
        return order

    def _fetch_order_pages(self, *, status_id: Optional[int] = None,
                           changed_from: Optional[str] = None,
                           purchase_window: Optional[Tuple[str, str]] = None) -> List[Dict[str, Any]]:
        orders: Dict[str, Dict[str, Any]] = {}
        cursor = None
        seen_cursors = set()
        started = time.monotonic()
        while True:
            if self.scan_pages >= self.scan_max_pages or time.monotonic() - started > 1200:
                raise RuntimeError("Invoice scan limit reached before completion")
            if self.operation_journal and self.scan_pages % 20 == 0:
                self.operation_journal.assert_owned()
            params: Dict[str, Any] = {"limit": 30, "order_by": "pur_date", "sort": "DESC"}
            if cursor is not None:
                params["cursor"] = cursor
            variables: Dict[str, Any] = {"params": params}
            if status_id is not None:
                variables["status"] = status_id
            if changed_from is not None:
                variables["changed_from"] = changed_from
            result = self.execute_read(ORDER_QUERY, variables)
            payload = result.get("getOrderList")
            if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
                raise RuntimeError("Incomplete invoice scan response")
            page = payload["data"]
            info = payload.get("pageInfo")
            if not isinstance(info, dict) or not isinstance(info.get("hasNextPage"), bool):
                raise RuntimeError("Invoice scan lacks reliable pagination metadata")
            self.scan_pages += 1
            for order in page:
                self._validate_order_for_invoice_read(order)
                number = str(order["order_num"])
                # Repeated offset rows can occur during concurrent shop updates.
                # Fail this pass rather than asserting complete coverage.
                if number in orders:
                    raise RuntimeError("Invoice scan repeated an order; restart required")
                orders[number] = order
            if not info["hasNextPage"]:
                break
            if not page:
                raise RuntimeError("Invoice scan has an empty non-final page")
            next_cursor = info.get("nextCursor")
            if (not isinstance(next_cursor, int) or isinstance(next_cursor, bool)
                    or next_cursor <= (cursor or 0) or next_cursor in seen_cursors):
                raise RuntimeError("Invoice scan cursor did not advance")
            seen_cursors.add(next_cursor)
            if purchase_window and any(_order_purchase_date(order) < purchase_window[0] for order in page):
                break
            cursor = next_cursor
        result_orders = list(orders.values())
        if purchase_window:
            result_orders = [order for order in result_orders
                             if purchase_window[0] <= _order_purchase_date(order) <= purchase_window[1]]
        return result_orders

    def fetch_orders(self, date_from: datetime, date_to: datetime) -> List[Dict[str, Any]]:
        """Legacy explicit purchase-window scan, with mandatory complete pages."""
        return self._fetch_order_pages(purchase_window=(date_from.strftime("%Y-%m-%d"), date_to.strftime("%Y-%m-%d")))

    def fetch_all_eligible_orders(self) -> List[Dict[str, Any]]:
        """Scan the shipped status across all languages and all purchase dates."""
        orders: Dict[str, Dict[str, Any]] = {}
        for status_id in sorted(self.resolve_eligible_status_ids()):
            for order in self._fetch_order_pages(status_id=status_id):
                orders[str(order["order_num"])] = order
        return list(orders.values())

    def fetch_changed_orders(self, changed_from: str) -> List[Dict[str, Any]]:
        return self._fetch_order_pages(changed_from=changed_from)

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
            order = result.get("getOrder") or {}
            invoices = order.get("invoices") or []
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
            "skipped_blocked_orders": 0,
        }

        for order in orders:
            status = order.get("status", {}) or {}
            status_name = status.get("name", "").lower()
            invoices = order.get("invoices", []) or []
            has_invoice = len(invoices) > 0
            order_total_value = _coerce_order_total_value(order)

            if order.get("blocked"):
                stats["skipped_blocked_orders"] += 1
                continue

            if self.exclude_zero_total_orders and order_total_value <= 0:
                stats["skipped_zero_total_orders"] += 1
                logger.info(
                    "Order %s skipped - zero or negative total (%.2f)",
                    order.get("order_num"),
                    order_total_value,
                )
                continue

            if self._status_is_eligible(order) and not has_invoice:
                filtered_orders.append(order)
                logger.info(
                    "Order %s matches criteria for invoice generation - Status: %s - Total: %.2f",
                    order.get("order_num"),
                    status_name,
                    order_total_value,
                )
            else:
                logger.debug(
                    "Order %s skipped - Status: %s, Has Invoice: %s",
                    order.get("order_num"),
                    status_name,
                    has_invoice,
                )

        return filtered_orders, stats

    def _status_is_eligible(self, order: Dict[str, Any]) -> bool:
        status = order.get("status") or {}
        if self.eligible_status_ids:
            return str(status.get("id")) in {str(value) for value in self.eligible_status_ids}
        return _status_matches_invoice_generation(status.get("name", ""), self.eligible_statuses)
    
    def _journal_update(self, order_num: str, **fields: Any) -> None:
        if self.operation_journal:
            self.operation_journal.update_order(order_num, **fields)

    def _confirm_created_invoice(self, order_num: str, claimed_id: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
        for attempt in range(3):
            current = self.fetch_order_for_invoice(order_num)
            invoices = current["invoices"]
            if invoices:
                if len(invoices) != 1:
                    raise RuntimeError("Invoice readback has multiple final documents")
                invoice = invoices[0]
                actual_id = str(invoice["id"])
                if claimed_id and actual_id != str(claimed_id):
                    raise RuntimeError("Created invoice identity differs from order readback")
                return actual_id, str(invoice.get("invoice_num") or "") or None
            if attempt < 2:
                time.sleep(1)
        return None, None

    def _send_journaled_email(self, order_num: str, result: InvoiceCreationResult) -> None:
        held = bool(self.operation_journal and self.operation_journal.get_order(order_num).get("email_policy") == "hold")
        if held:
            result.email_required = False
        if not result.email_required:
            self._journal_update(order_num, phase="complete", email_state="held" if held else "disabled")
            return
        if not result.invoice_id:
            result.email_error = "missing_invoice_id"
            return
        current = self.fetch_order_for_invoice(order_num)
        if (current["blocked"] or not self._status_is_eligible(current)
                or not any(str(inv["id"]) == str(result.invoice_id) for inv in current["invoices"])):
            result.email_error = "eligibility_changed"
            result.ambiguous = True
            self._journal_update(order_num, phase="email", email_state="ambiguous")
            return
        if self.operation_journal:
            self.operation_journal.assert_owned()
        self._journal_update(order_num, phase="email", email_state="sending", invoice_id=result.invoice_id,
                             invoice_num=result.invoice_num)
        result.email_sent = self.send_invoice_email(result.invoice_id)
        result.email_error = "" if result.email_sent else self.last_email_outcome
        result.ambiguous = self.last_email_outcome == "ambiguous"
        self._journal_update(order_num, phase="complete" if result.email_sent else "email",
                             email_state=self.last_email_outcome)

    def create_invoice(self, order: Dict[str, Any]) -> InvoiceCreationResult:
        """Fresh eligibility, one finalization attempt, mandatory invoice readback."""
        order_num = str(order.get("order_num") or "").strip()
        result = InvoiceCreationResult(email_required=self.send_invoice_email_enabled)
        if not order_num:
            return result
        current = self.fetch_order_for_invoice(order_num)
        prior = self.operation_journal.get_order(order_num) if self.operation_journal else {}
        if prior.get("email_policy") == "hold":
            result.email_required = False
        if current["invoices"]:
            if prior.get("phase") not in {"creating", "create_ambiguous"}:
                result.skipped = True
                self._journal_update(order_num, phase="complete", reason="invoice_exists")
                return result
            result.invoice_id, result.invoice_num = self._confirm_created_invoice(order_num, None)
            result.created = True
            result.recovered = True
            self._journal_update(order_num, phase="email", invoice_id=result.invoice_id,
                                 invoice_num=result.invoice_num, email_state="pending")
            self._send_journaled_email(order_num, result)
            return result
        if prior.get("phase") in {"creating", "create_ambiguous"}:
            # No provider idempotency key exists on this web endpoint. A missing
            # readback is not permission to replay an uncertain mutation.
            result.ambiguous = True
            self._journal_update(order_num, phase="create_ambiguous")
            return result
        eligible, _ = self.filter_orders_for_invoice([current])
        if not eligible:
            result.skipped = True
            self._journal_update(order_num, phase="complete", reason="eligibility_changed")
            return result
        if not self.web_session:
            raise RuntimeError("Invoice web session is unavailable")
        if self.operation_journal:
            self.operation_journal.assert_owned()
        self._journal_update(order_num, phase="creating", attempted_at=iso_utc(utc_now()))
        headers = {
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Referer": f"{self.base_url}/erp/orders/orders/detail/{order_num}",
        }
        suffix = f"?arf={self.arf_token}" if self.arf_token else ""
        claimed_id = None
        definite_rejection = False
        try:
            prepared = self.web_session.post(self.invoice_create_url.format(order_num=order_num) + suffix,
                                             headers=headers, allow_redirects=False)
            if prepared.status_code != 200:
                definite_rejection = prepared.status_code in {400, 401, 403, 404, 405, 429}
                raise RuntimeError(f"Invoice preparation returned HTTP {prepared.status_code}")
            # Creation opens the provider's preparation form; it is not evidence
            # that an accounting document exists. Only finalization + readback is.
            if self.operation_journal:
                self.operation_journal.assert_owned()
            latest = self.fetch_order_for_invoice(order_num)
            if latest["invoices"] or not self.filter_orders_for_invoice([latest])[0]:
                result.skipped = True
                self._journal_update(order_num, phase="complete", reason="changed_before_finalization")
                return result
            response = self.web_session.post(self.invoice_finalize_url.format(order_num=order_num) + suffix,
                                            headers=headers, allow_redirects=False)
            if response.status_code != 200:
                definite_rejection = response.status_code in {400, 401, 403, 404, 405, 429}
                raise RuntimeError(f"Invoice finalization returned HTTP {response.status_code}")
            try:
                payload = response.json()
            except (ValueError, json.JSONDecodeError):
                payload = None
            if not isinstance(payload, dict) or payload.get("success") is not True:
                definite_rejection = isinstance(payload, dict) and payload.get("success") is False
                raise RuntimeError("Invoice finalization did not explicitly confirm success")
            claimed_id = _extract_invoice_id_from_payload(payload)
        except AutomationStateError:
            raise
        except Exception as exc:
            logger.warning("Invoice request for order %s needs readback (%s)", order_num, type(exc).__name__)
            result.recovered = True
        try:
            result.invoice_id, result.invoice_num = self._confirm_created_invoice(order_num, claimed_id)
        except Exception:
            self._journal_update(order_num, phase="create_ambiguous")
            result.ambiguous = True
            return result
        if not result.invoice_id:
            result.ambiguous = not definite_rejection
            self._journal_update(order_num, phase="create_ambiguous" if result.ambiguous else "create_failed")
            return result
        result.created = True
        self._journal_update(order_num, phase="email", invoice_id=result.invoice_id,
                             invoice_num=result.invoice_num, email_state="pending")
        self._send_journaled_email(order_num, result)
        return result

    def send_invoice_email(self, invoice_id: str) -> bool:
        """A single send attempt; HTTP 200 HTML and timeouts remain ambiguous."""
        self.last_email_outcome = "ambiguous"
        suffix = f"?arf={self.arf_token}" if self.arf_token else ""
        try:
            response = self.web_session.post(
                self.invoice_send_url.format(invoice_id=invoice_id) + suffix,
                headers={"X-Requested-With": "XMLHttpRequest", "Accept": "application/json",
                         "Referer": f"{self.base_url}/erp/orders/invoices/detail/{invoice_id}"},
                allow_redirects=False,
            )
            if response.status_code in {400, 401, 403, 404, 405, 429}:
                self.last_email_outcome = "failed"
                return False
            if response.status_code != 200:
                return False
            payload = response.json()
            if isinstance(payload, dict) and payload.get("success") is True:
                self.last_email_outcome = "sent"
                return True
            if isinstance(payload, dict) and payload.get("success") is False:
                self.last_email_outcome = "failed"
        except Exception as exc:
            logger.warning("Invoice email result is unconfirmed (%s)", type(exc).__name__)
        return False

    def retry_pending_invoice_emails(self) -> List[InvoiceCreationResult]:
        results = []
        if not self.operation_journal:
            return results
        for record in self.operation_journal.pending_orders():
            if record.get("phase") != "email":
                continue
            result = InvoiceCreationResult(email_required=True, invoice_id=record.get("invoice_id"),
                                           invoice_num=record.get("invoice_num"))
            order_num = str(record["order_num"])
            if record.get("email_state") in {"sending", "ambiguous"}:
                result.ambiguous = True
                result.email_error = "ambiguous"
            elif record.get("email_policy") == "hold":
                self._journal_update(order_num, phase="complete", email_state="held")
                continue
            else:
                current = self.fetch_order_for_invoice(order_num)
                valid_invoice = any(str(inv["id"]) == str(result.invoice_id) for inv in current["invoices"])
                if not valid_invoice or current["blocked"] or not self._status_is_eligible(current):
                    result.email_error = "eligibility_changed"
                    result.ambiguous = True
                else:
                    self._send_journaled_email(order_num, result)
            results.append(result)
        return results

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
        orders = self.fetch_orders(date_from, date_to)
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
            logger.info("DRY RUN Summary:")
            logger.info(f"  Orders that would be processed: {len(orders_for_invoice)}")
            total = sum(_coerce_order_total_value(order) for order in orders_for_invoice)
            logger.info(f"  Total amount: â‚¬{total:.2f}")
            logger.info(f"  Skipped zero-total orders: {filter_stats.get('skipped_zero_total_orders', 0)}")
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
            if result.created:
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
        logger.info("Invoice processing complete:")
        logger.info(f"  âś“ Invoices created: {success_count}")
        if failed_count > 0:
            logger.info(f"  âś— Failed: {failed_count}")
        if email_failed_count > 0:
            logger.info(f"  âś— Invoice emails failed: {email_failed_count}")
        logger.info(f"  Skipped zero-total orders: {filter_stats.get('skipped_zero_total_orders', 0)}")
        logger.info(f"  Total amount: â‚¬{total_amount:.2f}")
        logger.info("=" * 60)
        
        if success_count > 0:
            logger.info("Email notifications sent to:")
            for order in processed_orders:
                logger.info(f"  â€˘ Order {order['order_num']}: {order['email']} ({order['amount']})")
            logger.info("=" * 60)


def _record_invoice_result(summary: InvoiceRunSummary, result: InvoiceCreationResult, order: Dict[str, Any]) -> None:
    if result.skipped:
        summary.skipped_after_recheck_orders += 1
        return
    if result.ambiguous:
        summary.ambiguous_invoice_operations += 1
    if result.created:
        if result.recovered:
            summary.recovered_invoices += 1
        else:
            summary.created_invoices += 1
        summary.total_amount += _coerce_order_total_value(order)
        if result.email_required:
            if result.email_sent:
                summary.emailed_invoices += 1
            else:
                summary.failed_invoice_emails += 1
                summary.missing_invoice_ids += int(result.email_error == "missing_invoice_id")
    else:
        summary.failed_invoices += 1


def run_invoice_generation(
    project_name: str, date_from: Union[str, datetime], date_to: Union[str, datetime],
    dry_run: bool = False, no_web_login: bool = False, *, full_backlog: Optional[bool] = None,
    state_store: Any = None, creditnote_order_numbers: Optional[set[str]] = None,
) -> InvoiceRunSummary:
    project_name = (project_name or BASE_DEFAULT_PROJECT).strip() or BASE_DEFAULT_PROJECT
    os.environ["REPORT_PROJECT"] = project_name
    load_project_env(project_name, logger=logger)
    project_settings = load_project_settings(project_name)
    settings = resolve_invoice_generation_settings(project_settings)
    api_url = resolve_biznisweb_api_url(project_name, project_settings)
    token = os.getenv("BIZNISWEB_API_TOKEN")
    if not token:
        raise RuntimeError("BIZNISWEB_API_TOKEN is required for invoice automation")
    from_dt = date_from if isinstance(date_from, datetime) else datetime.strptime(str(date_from), "%Y-%m-%d")
    to_dt = date_to if isinstance(date_to, datetime) else datetime.strptime(str(date_to), "%Y-%m-%d")
    if from_dt > to_dt:
        raise ValueError("Invoice window start cannot follow its end")
    summary = InvoiceRunSummary(project_name, from_dt.strftime("%Y-%m-%d"), to_dt.strftime("%Y-%m-%d"), dry_run)
    if state_store is None and settings["safety_state_enabled"]:
        state_store = build_automation_state_store(project_name, project_settings)
    if settings["all_age_backlog_enabled"] and not dry_run and state_store is None:
        raise AutomationStateError("All-age invoice automation requires a durable safety journal")
    lease = state_store.lease(owner="invoice-generation") if state_store and not dry_run else nullcontext(None)
    try:
        with lease as journal:
            state = journal.snapshot() if journal else state_store.read()[0] if state_store else {"last_scan": {}, "orders": {}}
            generator = InvoiceGenerator(
                api_url, token, derive_biznisweb_base_url(api_url),
                None if no_web_login or dry_run else os.getenv("BIZNISWEB_USERNAME"),
                None if no_web_login or dry_run else os.getenv("BIZNISWEB_PASSWORD"),
                exclude_zero_total_orders=settings["exclude_zero_total_orders"],
                eligible_statuses=settings["eligible_statuses"], send_invoice_email=settings["send_invoice_email"],
                scan_max_pages=settings["scan_max_pages"], page_delay_seconds=settings["page_delay_seconds"],
                read_attempts=settings["read_attempts"],
            )
            generator.operation_journal = journal
            if not dry_run and (not generator.web_session or not generator.validate_session()):
                raise RuntimeError("Invoice automation requires a valid BiznisWeb web session")
            generator.resolve_eligible_status_ids()
            started = utc_now()
            last_scan = state["last_scan"]
            summary.full_scan_age_hours = (
                max(0.0, (started - parse_utc(last_scan["full_scan_completed_at"])).total_seconds() / 3600)
                if last_scan.get("full_scan_completed_at") else -1.0
            )
            due = not last_scan.get("full_scan_completed_at") or (
                started - parse_utc(last_scan["full_scan_completed_at"]) >= timedelta(hours=settings["full_scan_interval_hours"]))
            do_full = bool(full_backlog) or (full_backlog is None and settings["all_age_backlog_enabled"] and due)
            summary.invoice_scan_all_ages = do_full
            if settings["all_age_backlog_enabled"] or full_backlog:
                full_orders = generator.fetch_all_eligible_orders() if do_full else []
                watermark = last_scan.get("changed_watermark")
                changed_at = parse_utc(watermark) - timedelta(minutes=5) if watermark else started - timedelta(days=settings["lookback_days"])
                zone = ZoneInfo(str(project_settings.get("invoice_generation", {}).get("timezone") or "Europe/Bratislava"))
                changed_orders = generator.fetch_changed_orders(changed_at.astimezone(zone).strftime("%Y-%m-%d %H:%M:%S"))
                by_number = {str(row["order_num"]): row for row in full_orders + changed_orders}
            else:
                changed_orders = generator.fetch_orders(from_dt, to_dt)
                by_number = {str(row["order_num"]): row for row in changed_orders}
            # A failed old creation remains eligible for retry even after the
            # incremental watermark advances and after its purchase leaves any window.
            recovered_candidates: List[Dict[str, Any]] = []
            externally_completed: List[str] = []
            if journal:
                changed_by_number = {str(row["order_num"]): row for row in changed_orders}
                for record in state["orders"].values():
                    if (record.get("status_review") or {}).get("state") == "open":
                        row = generator.fetch_order_for_invoice(record["order_num"])
                        changed_by_number[str(row["order_num"])] = row
                        by_number[str(row["order_num"])] = row
                changed_orders = list(changed_by_number.values())
                for record in journal.pending_orders():
                    if record.get("phase") in {"pending", "creating", "create_failed", "create_ambiguous"}:
                        row = generator.fetch_order_for_invoice(record["order_num"])
                        by_number[str(row["order_num"])] = row
                        if row["invoices"]:
                            if record.get("phase") in {"creating", "create_ambiguous"}:
                                recovered_candidates.append(row)
                            else:
                                externally_completed.append(str(row["order_num"]))
                        else:
                            # Process persisted attempts even if the current
                            # status changed: retire safe pending work and keep
                            # uncertain effects visible instead of dropping them.
                            recovered_candidates.append(row)
            orders = list(by_number.values())
            candidates, stats = generator.filter_orders_for_invoice(orders)
            candidate_numbers = {str(row["order_num"]) for row in candidates}
            candidates.extend(row for row in recovered_candidates if str(row["order_num"]) not in candidate_numbers)
            summary.total_orders_fetched = len(orders)
            summary.matched_orders = len(candidates)
            summary.skipped_zero_total_orders = stats["skipped_zero_total_orders"]
            summary.skipped_blocked_orders = stats["skipped_blocked_orders"]
            summary.invoice_scan_pages = generator.scan_pages
            summary.invoice_scan_complete = True
            if journal:
                # Persist the entire candidate batch before the first mutation.
                journal.enqueue_orders(candidates)
                journal.enqueue_status_reviews([
                    row for row in changed_orders
                    if _is_existing_invoice_status_reconciliation_candidate(row, settings["existing_invoice_status_reconciliation"])
                ])
                for number in externally_completed:
                    journal.update_order(number, phase="complete", reason="invoice_created_elsewhere")
                journal.remember_shipped_orders(orders, generator.eligible_status_ids)
                scan_update = {"changed_watermark": iso_utc(started), "pages": generator.scan_pages,
                               "candidate_count": len(candidates), "completed_at": iso_utc(utc_now())}
                if do_full:
                    scan_update["full_scan_completed_at"] = iso_utc(utc_now())
                    summary.full_scan_age_hours = 0.0
                journal.record_scan(**scan_update)
            creditnote_context_failed = False
            if (creditnote_order_numbers is None and settings["existing_invoice_status_reconciliation"]["enabled"]
                    and any(_is_existing_invoice_status_reconciliation_candidate(row, settings["existing_invoice_status_reconciliation"])
                            for row in changed_orders)):
                from creditnote_export import fetch_creditnote_automation_context

                try:
                    creditnote_order_numbers = set(fetch_creditnote_automation_context(
                        project_name, progress_callback=journal.assert_owned if journal else None,
                    ))
                except AutomationStateError:
                    raise
                except Exception as exc:
                    # Unknown creditnotes prevent status recovery. They do not
                    # invalidate independently checked shipped invoice orders.
                    creditnote_context_failed = True
                    logger.error("Creditnote context unavailable; status recovery requires review (%s)", type(exc).__name__)
            reconciliation = reconcile_existing_invoice_statuses(
                generator.client, changed_orders, settings["existing_invoice_status_reconciliation"], dry_run=dry_run,
                journal=journal, creditnote_order_numbers=creditnote_order_numbers,
                read_order=generator.fetch_order_safety_context,
            )
            summary.invoice_status_reconciliation_enabled = reconciliation["enabled"]
            summary.invoice_status_reconciliation_candidates = reconciliation["candidates"]
            summary.reconciled_invoice_statuses = reconciliation["reconciled"]
            summary.failed_invoice_status_reconciliations = reconciliation["failed"] + int(creditnote_context_failed)
            summary.skipped_invoice_status_reconciliations_after_recheck = reconciliation["skipped_after_recheck"]
            summary.invoice_status_reconciliation_target_name = reconciliation["target_status_name"]
            summary.invoice_status_reconciliation_target_id = reconciliation["target_status_id"]
            summary.invoice_status_review_required = reconciliation["review_required"]
            if dry_run:
                summary.total_amount = sum(_coerce_order_total_value(row) for row in candidates)
                return summary
            for email_result in generator.retry_pending_invoice_emails():
                summary.emailed_invoices += int(email_result.email_sent)
                summary.failed_invoice_emails += int(not email_result.email_sent)
                summary.ambiguous_invoice_operations += int(email_result.ambiguous)
            for order in candidates:
                result = generator.create_invoice(order)
                _record_invoice_result(summary, result, order)
            if journal:
                pending = journal.pending_orders()
                summary.pending_invoice_emails = sum(record.get("phase") == "email" for record in pending)
                summary.pending_invoice_operations = sum(record.get("phase") in {
                    "pending", "creating", "create_failed", "create_ambiguous", "email"} for record in pending)
            logger.info("Invoice automation finished: scan_complete=%s full_scan=%s pages=%s matched=%s created=%s failed=%s email_failed=%s ambiguous=%s",
                        summary.invoice_scan_complete, summary.invoice_scan_all_ages, summary.invoice_scan_pages,
                        summary.matched_orders, summary.created_invoices, summary.failed_invoices,
                        summary.failed_invoice_emails, summary.ambiguous_invoice_operations)
            return summary
    except AutomationLeaseBusy:
        summary.skipped_locked = True
        logger.info("Invoice run skipped because another project automation holds the lease")
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
            "emailed=%s email_failed=%s missing_invoice_ids=%s skipped_zero_total=%s"
        ),
        summary.project,
        summary.matched_orders,
        summary.created_invoices,
        summary.failed_invoices,
        summary.emailed_invoices,
        summary.failed_invoice_emails,
        summary.missing_invoice_ids,
        summary.skipped_zero_total_orders,
    )
    if not args.dry_run and (summary.failed_invoices or summary.failed_invoice_emails):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
