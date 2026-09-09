#!/usr/bin/env python3
"""Automatically cancel stale unpaid BizniWeb orders."""

from __future__ import annotations

import os
import re
import unicodedata
from contextlib import nullcontext
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport

from logger_config import get_logger
from order_status_safety import (
    acquire_status_automation_lease,
    ORDER_SAFETY_QUERY,
    assess_fulfillment_evidence,
    assess_payment_evidence,
    change_status_verified,
    decide_recovery,
    execute_read,
    fetch_order_safety_context,
    status_write_block_reason,
)
from reporting_core import BASE_DEFAULT_PROJECT, load_project_env, load_project_settings, resolve_biznisweb_api_url


logger = get_logger("unpaid_order_cancellation")

DEFAULT_TARGET_STATUS_NAME = "Nezaplaten\u00e1 - zru\u0161en\u00e1 objedn\u00e1vka"
DEFAULT_RECOVERY_TARGET_STATUS_NAME = "Platba online - zaplaten\u00e9"
DEFAULT_AGE_DAYS = 14
DEFAULT_SCAN_MAX_PAGES = 200
DEFAULT_PAGE_LIMIT = 30
DEFAULT_LANG_CODE = "SK"
DEFAULT_PAYMENT_REFERENCE_IDS = ("6", "17", "18", "11", "20")
DEFAULT_PAYMENT_TITLE_PATTERNS = (
    "bankovym prevodom",
    "bankovni prevod",
    "bankovy prevod",
    "prevodom",
    "bank transfer",
    "platba online",
    "okamzita platba online",
    "kartou",
    "karta",
    "card",
    "kartyas fizetes",
)
DEFAULT_CANDIDATE_STATUSES = (
    "\u010cak\u00e1 na \u00fahradu",
    "Platba online - platnos\u0165 vypr\u0161ala",
    "Platba online - platba zamietnut\u00e1",
    "GP WebPay - platba selhala",
    "GoPay - \u010dek\u00e1 se",
    "GoPay - platnost vypr\u0161ela",
    "GoPay - zru\u0161eno",
    "GoPay - platba selhala",
    "GoPay - platba p\u0159edautorizov\u00e1na",
    "GoPay - platba vytvo\u0159en\u00e1",
    "GoPay - platebni metoda potvrzena",
    "Besteron - platba zlyhala",
    "Besteron - platba expirovala",
    "Besteron - vytvoren\u00e1",
    "Besteron - \u010dak\u00e1 na potvrdenie",
    "Besteron - prebieha",
    "Besteron - potrebn\u00e1 manu\u00e1lna pozornos\u0165",
    "Besteron - neplatn\u00fd",
    "Besteron - zru\u0161en\u00e9",
    "Besteron - vypr\u0161al \u010dasov\u00fd limit",
    "Besteron - chyba",
    "24 pay - Nezrealizovan\u00e1",
    "24 pay - Platba nebola potvrden\u00e1",
    "24 pay - Platba autorizovan\u00e1",
    "Stripe - cancelled",
    "Stripe - expired",
    "Stripe - unpaid",
)
DEFAULT_EXCLUDED_STATUSES = (
    DEFAULT_TARGET_STATUS_NAME,
    "Platba online - zaplaten\u00e9",
    "Prijat\u00e1 platba / uhraden\u00e9",
    "\u010cak\u00e1 na vybavenie",
    "Pripraven\u00e9 k odberu",
    "Odoslan\u00e1",
    "Storno",
    "Vr\u00e1ten\u00e9",
    "Dobropis",
)
DEFAULT_RECOVERY_SOURCE_STATUSES = ("Stripe - expired",)


UNPAID_ORDER_QUERY = gql(
    """
query GetOrdersForUnpaidCancellation($status: Int, $params: OrderParams) {
  getOrderList(status: $status, params: $params) {
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
      price_elements {
        type
        title
        value
        reference_id
        price {
          value
          formatted
        }
      }
      sum {
        value
        formatted
      }
    }
    pageInfo {
      hasNextPage
      nextCursor
      pageIndex
      totalPages
    }
  }
}
"""
)


ORDER_RECHECK_QUERY = ORDER_SAFETY_QUERY


LIST_ORDER_STATUSES_QUERY = gql(
    """
query ListOrderStatuses($lang_code: CountryCodeAlpha2!) {
  listOrderStatuses(lang_code: $lang_code, only_active: true) {
    id
    name
  }
}
"""
)


@dataclass(frozen=True)
class UnpaidCancellationSettings:
    enabled: bool = False
    age_days: int = DEFAULT_AGE_DAYS
    target_status_name: str = DEFAULT_TARGET_STATUS_NAME
    target_status_id: Optional[int] = None
    recovery_enabled: bool = False
    recovery_target_status_name: str = DEFAULT_RECOVERY_TARGET_STATUS_NAME
    recovery_target_status_id: Optional[int] = None
    recovery_shipped_status_name: str = "Odoslaná"
    recovery_source_statuses: Tuple[str, ...] = DEFAULT_RECOVERY_SOURCE_STATUSES
    lang_code: str = DEFAULT_LANG_CODE
    payment_reference_ids: Tuple[str, ...] = DEFAULT_PAYMENT_REFERENCE_IDS
    payment_title_patterns: Tuple[str, ...] = DEFAULT_PAYMENT_TITLE_PATTERNS
    candidate_statuses: Tuple[str, ...] = DEFAULT_CANDIDATE_STATUSES
    excluded_statuses: Tuple[str, ...] = DEFAULT_EXCLUDED_STATUSES
    scan_max_pages: int = DEFAULT_SCAN_MAX_PAGES
    page_limit: int = DEFAULT_PAGE_LIMIT
    schedule_name: str = ""
    schedule_expression: str = ""
    timezone: str = "Europe/Bratislava"
    task_family: str = ""
    normalized_target_status_name: str = field(init=False)
    normalized_recovery_target_status_name: str = field(init=False)
    normalized_recovery_source_statuses: Tuple[str, ...] = field(init=False)
    normalized_payment_title_patterns: Tuple[str, ...] = field(init=False)
    normalized_candidate_statuses: Tuple[str, ...] = field(init=False)
    normalized_excluded_statuses: Tuple[str, ...] = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "normalized_target_status_name", normalize_text(self.target_status_name))
        object.__setattr__(
            self,
            "normalized_recovery_target_status_name",
            normalize_text(self.recovery_target_status_name),
        )
        object.__setattr__(
            self,
            "normalized_recovery_source_statuses",
            tuple(normalize_text(value) for value in self.recovery_source_statuses if normalize_text(value)),
        )
        object.__setattr__(
            self,
            "normalized_payment_title_patterns",
            tuple(normalize_text(value) for value in self.payment_title_patterns if normalize_text(value)),
        )
        object.__setattr__(
            self,
            "normalized_candidate_statuses",
            tuple(normalize_text(value) for value in self.candidate_statuses if normalize_text(value)),
        )
        object.__setattr__(
            self,
            "normalized_excluded_statuses",
            tuple(normalize_text(value) for value in self.excluded_statuses if normalize_text(value)),
        )


@dataclass
class UnpaidCancellationSummary:
    project: str
    enabled: bool
    dry_run: bool
    reference_date: str
    cutoff_date: str
    target_status_name: str
    target_status_id: Optional[int] = None
    recovery_enabled: bool = False
    recovery_target_status_name: str = ""
    recovery_target_status_id: Optional[int] = None
    total_orders_scanned: int = 0
    pages_scanned: int = 0
    eligible_orders: int = 0
    updated_orders: int = 0
    recovery_candidates: int = 0
    recovered_orders: int = 0
    recovery_failed_orders: int = 0
    review_required_orders: int = 0
    rechecked_orders: int = 0
    failed_orders: int = 0
    scan_limit_reached: bool = False
    scan_stop_reason: str = ""
    oldest_order_date: str = ""
    skipped_by_reason: Dict[str, int] = field(default_factory=dict)
    recovery_skipped_by_reason: Dict[str, int] = field(default_factory=dict)
    recheck_skipped_by_reason: Dict[str, int] = field(default_factory=dict)
    eligible_order_nums: List[str] = field(default_factory=list)
    updated_order_nums: List[str] = field(default_factory=list)
    recovery_candidate_order_nums: List[str] = field(default_factory=list)
    recovered_order_nums: List[str] = field(default_factory=list)
    recovery_failed_order_nums: List[str] = field(default_factory=list)
    failed_order_nums: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


def normalize_text(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    decomposed = unicodedata.normalize("NFKD", raw)
    ascii_text = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", " ", ascii_text).strip()


def _tuple_from_settings(value: Any, default: Sequence[str]) -> Tuple[str, ...]:
    if value is None:
        return tuple(default)
    if isinstance(value, str):
        return (value,)
    if isinstance(value, Iterable):
        return tuple(str(item) for item in value if str(item or "").strip())
    return tuple(default)


def resolve_unpaid_cancellation_settings(project_settings: Dict[str, Any]) -> UnpaidCancellationSettings:
    raw = project_settings.get("unpaid_order_cancellation") or {}
    target_status_id = raw.get("target_status_id")
    if target_status_id in ("", None):
        parsed_target_status_id: Optional[int] = None
    else:
        parsed_target_status_id = int(target_status_id)

    recovery_target_status_id = raw.get("recovery_target_status_id")
    if recovery_target_status_id in ("", None):
        parsed_recovery_target_status_id: Optional[int] = None
    else:
        parsed_recovery_target_status_id = int(recovery_target_status_id)

    age_days = max(1, int(raw.get("age_days", DEFAULT_AGE_DAYS)))
    scan_max_pages = max(1, int(raw.get("scan_max_pages", DEFAULT_SCAN_MAX_PAGES)))
    page_limit = max(1, min(DEFAULT_PAGE_LIMIT, int(raw.get("page_limit", DEFAULT_PAGE_LIMIT))))
    return UnpaidCancellationSettings(
        enabled=bool(raw.get("enabled", False)),
        age_days=age_days,
        target_status_name=str(raw.get("target_status_name") or DEFAULT_TARGET_STATUS_NAME),
        target_status_id=parsed_target_status_id,
        recovery_enabled=bool(raw.get("recovery_enabled", False)),
        recovery_target_status_name=str(
            raw.get("recovery_target_status_name") or DEFAULT_RECOVERY_TARGET_STATUS_NAME
        ),
        recovery_target_status_id=parsed_recovery_target_status_id,
        recovery_shipped_status_name=str(raw.get("recovery_shipped_status_name") or "Odoslaná"),
        recovery_source_statuses=_tuple_from_settings(
            raw.get("recovery_source_statuses"),
            DEFAULT_RECOVERY_SOURCE_STATUSES,
        ),
        lang_code=str(raw.get("lang_code") or DEFAULT_LANG_CODE),
        payment_reference_ids=_tuple_from_settings(raw.get("payment_reference_ids"), DEFAULT_PAYMENT_REFERENCE_IDS),
        payment_title_patterns=_tuple_from_settings(raw.get("payment_title_patterns"), DEFAULT_PAYMENT_TITLE_PATTERNS),
        candidate_statuses=_tuple_from_settings(raw.get("candidate_statuses"), DEFAULT_CANDIDATE_STATUSES),
        excluded_statuses=_tuple_from_settings(raw.get("excluded_statuses"), DEFAULT_EXCLUDED_STATUSES),
        scan_max_pages=scan_max_pages,
        page_limit=page_limit,
        schedule_name=str(raw.get("schedule_name") or ""),
        schedule_expression=str(raw.get("schedule_expression") or ""),
        timezone=str(raw.get("timezone") or "Europe/Bratislava"),
        task_family=str(raw.get("task_family") or ""),
    )


def build_client(project: str, project_settings: Dict[str, Any]) -> Client:
    api_url = resolve_biznisweb_api_url(project, project_settings)
    api_token = os.getenv("BIZNISWEB_API_TOKEN")
    if not api_token:
        raise RuntimeError(f"BIZNISWEB_API_TOKEN not found for project '{project}'")
    timeout = int(os.getenv("BIZNISWEB_API_TIMEOUT_SEC", os.getenv("REPORT_HTTP_READ_TIMEOUT_SEC", "30")))
    transport = RequestsHTTPTransport(
        url=api_url,
        headers={"BW-API-Key": f"Token {api_token}"},
        verify=True,
        retries=0,
        timeout=timeout,
    )
    return Client(transport=transport, fetch_schema_from_transport=False)


def parse_reference_date(value: Union[str, date, datetime, None]) -> date:
    if value is None:
        return datetime.utcnow().date()
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def order_purchase_date(order: Dict[str, Any]) -> Optional[date]:
    raw = str(order.get("pur_date") or "").strip()
    if not raw:
        return None
    raw_date = raw.split(" ", 1)[0][:10]
    try:
        return datetime.strptime(raw_date, "%Y-%m-%d").date()
    except ValueError:
        return None


def _status_name(order: Dict[str, Any]) -> str:
    return str((order.get("status") or {}).get("name") or "").strip()


def _payment_elements(order: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [
        element
        for element in (order.get("price_elements") or [])
        if normalize_text(element.get("type")) == "payment"
    ]


def _payment_matches_values(
    order: Dict[str, Any],
    reference_ids: Sequence[str],
    normalized_title_patterns: Sequence[str],
) -> bool:
    configured_ids = {str(value).strip() for value in reference_ids if str(value).strip()}
    for element in _payment_elements(order):
        reference_id = str(element.get("reference_id") or "").strip()
        if reference_id and reference_id in configured_ids:
            return True
        normalized_title = normalize_text(element.get("title"))
        if any(pattern and pattern in normalized_title for pattern in normalized_title_patterns):
            return True
    return False


def payment_matches(order: Dict[str, Any], settings: UnpaidCancellationSettings) -> bool:
    return _payment_matches_values(
        order,
        settings.payment_reference_ids,
        settings.normalized_payment_title_patterns,
    )


def _final_invoices(order: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [invoice for invoice in (order.get("invoices") or []) if invoice and invoice.get("id")]


def has_final_invoice(order: Dict[str, Any]) -> bool:
    return bool(_final_invoices(order))


def recovery_eligibility_reason(
    order: Dict[str, Any], settings: UnpaidCancellationSettings, *,
    creditnote_status: str = "unknown", verified_previous_status: Optional[str] = None,
) -> str:
    if not settings.recovery_enabled:
        return "recovery_disabled"

    status = normalize_text(_status_name(order))
    if not status:
        return "missing_status"
    if status not in settings.normalized_recovery_source_statuses:
        return "not_recovery_status"
    if not has_final_invoice(order):
        return "missing_final_invoice"
    decision = decide_recovery(
        order, settings.recovery_source_statuses,
        paid_target=settings.recovery_target_status_name,
        shipped_target=settings.recovery_shipped_status_name,
        creditnote_status=creditnote_status,
        verified_previous_status=verified_previous_status,
    )
    return "eligible" if decision.action in {"paid", "shipped"} else decision.reason


def cancellation_eligibility_reason(
    order: Dict[str, Any],
    settings: UnpaidCancellationSettings,
    cutoff_date: date,
    *,
    require_payment_context: bool = True,
) -> str:
    purchased_at = order_purchase_date(order)
    if purchased_at is None:
        return "missing_purchase_date"
    if purchased_at > cutoff_date:
        return "not_old_enough"

    status = normalize_text(_status_name(order))
    if not status:
        return "missing_status"
    if status == settings.normalized_target_status_name:
        return "already_target_status"
    if status in settings.normalized_excluded_statuses:
        return "excluded_status"
    if has_final_invoice(order):
        return "final_invoice_present"
    if settings.normalized_candidate_statuses and status not in settings.normalized_candidate_statuses:
        return "not_candidate_status"
    if not payment_matches(order, settings):
        return "payment_not_matched"
    if order.get("blocked") is True:
        return "order_blocked"
    if order.get("blocked") is not False:
        return "order_block_flag_missing"
    if require_payment_context:
        payment = assess_payment_evidence(order)
        if payment.state == "unknown":
            return "payment_evidence_unknown"
        if payment.state != "unpaid":
            return "payment_present"
        fulfillment = assess_fulfillment_evidence(order)
        if fulfillment.state == "unknown":
            return "shipment_evidence_unknown"
        if fulfillment.state != "none":
            return "shipment_present"
    return "eligible"


def is_order_eligible_for_cancellation(
    order: Dict[str, Any],
    settings: UnpaidCancellationSettings,
    cutoff_date: date,
) -> bool:
    return cancellation_eligibility_reason(order, settings, cutoff_date) == "eligible"


def list_order_statuses(client: Client, settings: UnpaidCancellationSettings) -> List[Dict[str, Any]]:
    result = execute_read(client, LIST_ORDER_STATUSES_QUERY, variable_values={"lang_code": settings.lang_code})
    return [row for row in (result.get("listOrderStatuses") or []) if row]


def _resolve_status_id(
    statuses: Sequence[Dict[str, Any]],
    target_status_name: str,
    configured_status_id: Optional[int],
) -> int:
    target_norm = normalize_text(target_status_name)
    for row in statuses:
        row_id = int(row.get("id") or 0)
        row_name_norm = normalize_text(row.get("name"))
        if configured_status_id and row_id == configured_status_id:
            if row_name_norm != target_norm:
                raise RuntimeError(
                    f"Configured status_id={configured_status_id} resolves to "
                    f"'{row.get('name')}', expected '{target_status_name}'."
                )
            return row_id
        if not configured_status_id and row_name_norm == target_norm:
            return row_id
    raise RuntimeError(f"Target status '{target_status_name}' not found in BizniWeb.")


def resolve_target_status_id(client: Client, settings: UnpaidCancellationSettings) -> int:
    return _resolve_status_id(
        list_order_statuses(client, settings),
        settings.target_status_name,
        settings.target_status_id,
    )


def resolve_recovery_target_status_id(client: Client, settings: UnpaidCancellationSettings) -> int:
    return _resolve_status_id(
        list_order_statuses(client, settings),
        settings.recovery_target_status_name,
        settings.recovery_target_status_id,
    )


def resolve_candidate_status_ids(
    statuses: Sequence[Dict[str, Any]],
    settings: UnpaidCancellationSettings,
) -> List[int]:
    configured_names = set(settings.normalized_candidate_statuses)
    if settings.recovery_enabled:
        configured_names.update(settings.normalized_recovery_source_statuses)
    resolved = [
        int(row.get("id") or 0)
        for row in statuses
        if int(row.get("id") or 0) > 0 and normalize_text(row.get("name")) in configured_names
    ]
    if not resolved:
        raise RuntimeError("No active BizniWeb order statuses match the configured cancellation candidates.")
    return resolved


def fetch_order_for_recheck(client: Client, order_num: str) -> Dict[str, Any]:
    return fetch_order_safety_context(client, order_num)


def fetch_orders_for_cancellation(
    client: Client,
    settings: UnpaidCancellationSettings,
    status_ids: Sequence[int],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    orders: List[Dict[str, Any]] = []
    page_count = 0
    oldest_order_date = ""
    seen_orders: set[str] = set()
    for status_id in status_ids:
        cursor = 0
        has_next_page = True
        while has_next_page:
            if page_count >= settings.scan_max_pages:
                raise RuntimeError("Unpaid cancellation scan is incomplete: page budget exhausted")
            params: Dict[str, Any] = {
                "limit": settings.page_limit,
                "order_by": "pur_date",
                "sort": "DESC",
            }
            if cursor:
                params["cursor"] = cursor
            result = execute_read(
                client, UNPAID_ORDER_QUERY,
                variable_values={"status": int(status_id), "params": params},
            )
            payload = result.get("getOrderList")
            if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
                raise RuntimeError("Unpaid cancellation scan returned an invalid order page")
            page_orders = payload["data"]
            for order in page_orders:
                number = str(order.get("order_num") or "").strip() if isinstance(order, dict) else ""
                if not number or number in seen_orders:
                    raise RuntimeError("Unpaid cancellation scan contains a missing or duplicate order identity")
                seen_orders.add(number)
            orders.extend(page_orders)
            page_count += 1

            for order in page_orders:
                purchased_at = order_purchase_date(order)
                if purchased_at:
                    text = purchased_at.strftime("%Y-%m-%d")
                    if not oldest_order_date or text < oldest_order_date:
                        oldest_order_date = text

            page_info = payload.get("pageInfo")
            if not isinstance(page_info, dict) or not isinstance(page_info.get("hasNextPage"), bool):
                raise RuntimeError("Unpaid cancellation scan has no complete pagination metadata")
            has_next_page = page_info["hasNextPage"]
            if has_next_page:
                next_cursor = page_info.get("nextCursor")
                if isinstance(next_cursor, bool):
                    raise RuntimeError("Unpaid cancellation scan cursor is invalid")
                try:
                    next_cursor = int(next_cursor)
                except (TypeError, ValueError):
                    raise RuntimeError("Unpaid cancellation scan next cursor is missing or invalid") from None
                if not page_orders or next_cursor <= cursor:
                    raise RuntimeError("Unpaid cancellation scan cursor did not advance")
                cursor = next_cursor

    return orders, {
        "pages_scanned": page_count,
        "scan_limit_reached": False,
        "scan_stop_reason": "api_exhausted",
        "oldest_order_date": oldest_order_date,
    }


def change_order_status(
    client: Client, order_num: str, status_id: int, status_name: str, *, silent: bool = False,
) -> Dict[str, Any]:
    return change_status_verified(client, order_num, status_id, status_name, silent=silent)


def run_unpaid_order_cancellation(
    project_name: str,
    reference_date: Union[str, date, datetime, None] = None,
    dry_run: bool = False,
    client: Optional[Client] = None,
    project_settings: Optional[Dict[str, Any]] = None,
    automation_state_store: Optional[Any] = None,
    creditnote_context: Optional[Dict[str, List[Dict[str, Any]]]] = None,
) -> UnpaidCancellationSummary:
    project = (project_name or BASE_DEFAULT_PROJECT).strip() or BASE_DEFAULT_PROJECT
    os.environ["REPORT_PROJECT"] = project
    loaded_project_env = False
    if project_settings is None:
        load_project_env(project, logger=logger)
        loaded_project_env = True
        project_settings = load_project_settings(project)

    settings = resolve_unpaid_cancellation_settings(project_settings)
    ref_date = parse_reference_date(reference_date)
    cutoff_date = ref_date - timedelta(days=settings.age_days)
    summary = UnpaidCancellationSummary(
        project=project,
        enabled=settings.enabled,
        dry_run=dry_run,
        reference_date=ref_date.strftime("%Y-%m-%d"),
        cutoff_date=cutoff_date.strftime("%Y-%m-%d"),
        target_status_name=settings.target_status_name,
        recovery_enabled=settings.recovery_enabled,
        recovery_target_status_name=(settings.recovery_target_status_name if settings.recovery_enabled else ""),
    )
    if not settings.enabled:
        logger.info("Unpaid order cancellation disabled for project=%s", project)
        return summary

    if client is None:
        if not loaded_project_env:
            load_project_env(project, logger=logger)
        client = build_client(project, project_settings)

    statuses = list_order_statuses(client, settings)
    target_status_id = _resolve_status_id(
        statuses,
        settings.target_status_name,
        settings.target_status_id,
    )
    summary.target_status_id = target_status_id
    recovery_target_status_id: Optional[int] = None
    if settings.recovery_enabled:
        recovery_target_status_id = _resolve_status_id(
            statuses,
            settings.recovery_target_status_name,
            settings.recovery_target_status_id,
        )
        summary.recovery_target_status_id = recovery_target_status_id

    candidate_status_ids = resolve_candidate_status_ids(statuses, settings)
    orders, scan = fetch_orders_for_cancellation(client, settings, candidate_status_ids)
    summary.total_orders_scanned = len(orders)
    summary.pages_scanned = int(scan.get("pages_scanned") or 0)
    summary.scan_limit_reached = bool(scan.get("scan_limit_reached"))
    summary.scan_stop_reason = str(scan.get("scan_stop_reason") or "")
    summary.oldest_order_date = str(scan.get("oldest_order_date") or "")

    def increment(counts: Dict[str, int], reason: str) -> None:
        counts[reason] = counts.get(reason, 0) + 1

    def record_failure(order_num: str, recovery: bool, *, review: bool = False) -> None:
        if order_num not in summary.failed_order_nums:
            summary.failed_orders += 1
            summary.failed_order_nums.append(order_num)
            summary.review_required_orders += int(review)
        if recovery and order_num not in summary.recovery_failed_order_nums:
            summary.recovery_failed_orders += 1
            summary.recovery_failed_order_nums.append(order_num)

    injected_creditnote_context = creditnote_context is not None
    unknown_reasons = {"missing_purchase_date", "missing_status", "order_block_flag_missing", "payment_evidence_unknown", "shipment_evidence_unknown"}

    def refresh_creditnotes(heartbeat=None) -> None:
        nonlocal creditnote_context
        from creditnote_export import fetch_creditnote_automation_context

        creditnote_context = fetch_creditnote_automation_context(project, progress_callback=heartbeat)

    def needs_recovery_evidence(order: Dict[str, Any]) -> bool:
        return settings.recovery_enabled and normalize_text(_status_name(order)) in settings.normalized_recovery_source_statuses and has_final_invoice(order)

    def choose_action(order: Dict[str, Any], prior: Dict[str, Any], *, heartbeat=None) -> Tuple[str, str, str]:
        if needs_recovery_evidence(order):
            if creditnote_context is None:
                refresh_creditnotes(heartbeat)
            decision = decide_recovery(
                order, settings.recovery_source_statuses,
                paid_target=settings.recovery_target_status_name,
                shipped_target=settings.recovery_shipped_status_name,
                creditnote_status="present" if str(order["order_num"]) in creditnote_context else "clear",
                verified_previous_status=prior.get("verified_fulfillment_status"),
            )
            if decision.action in {"paid", "shipped"}:
                return "recover", decision.target_status_name, decision.reason
            return "review", "", decision.reason
        reason = cancellation_eligibility_reason(order, settings, cutoff_date)
        if reason in unknown_reasons:
            return "review", "", reason
        return ("cancel", settings.target_status_name, reason) if reason == "eligible" else ("skip", "", reason)

    provisional_orders = []
    for order in orders:
        if settings.recovery_enabled and normalize_text(_status_name(order)) in settings.normalized_recovery_source_statuses:
            provisional_orders.append(order)
        else:
            reason = cancellation_eligibility_reason(order, settings, cutoff_date, require_payment_context=False)
            if reason == "eligible":
                provisional_orders.append(order)
            else:
                increment(summary.skipped_by_reason, reason)
                if reason in unknown_reasons:
                    record_failure(str(order["order_num"]), False, review=True)
    if not provisional_orders:
        return summary
    if not dry_run and automation_state_store is None:
        from invoice_automation_state import build_automation_state_store

        automation_state_store = build_automation_state_store(project, project_settings)
    lease = nullcontext(None) if dry_run else acquire_status_automation_lease(automation_state_store, owner="unpaid-cancellation")
    with lease as journal:
        for listed in provisional_orders:
            if journal:
                journal.assert_owned()
            order_num = str(listed["order_num"])
            prior = journal.get_order(order_num) if journal else {}
            is_recovery = normalize_text(_status_name(listed)) in settings.normalized_recovery_source_statuses
            try:
                checked = fetch_order_for_recheck(client, order_num)
                summary.rechecked_orders += 1
                action, target_name, reason = choose_action(checked, prior, heartbeat=journal.assert_owned if journal else None)
                if journal:
                    journal.assert_owned()
            except Exception:
                record_failure(order_num, is_recovery)
                logger.error("Order safety inspection failed for order %s", order_num)
                continue
            if action == "review":
                record_failure(order_num, is_recovery, review=True)
                increment(summary.recovery_skipped_by_reason if is_recovery else summary.skipped_by_reason, reason)
                if journal:
                    journal.update_order(order_num, status_review_reason=reason)
                continue
            if action == "skip":
                increment(summary.skipped_by_reason, reason)
                continue
            if action == "recover":
                summary.recovery_candidates += 1
                summary.recovery_candidate_order_nums.append(order_num)
            else:
                summary.eligible_orders += 1
                summary.eligible_order_nums.append(order_num)
            if dry_run:
                continue
            blocked_reason = status_write_block_reason(prior)
            if blocked_reason:
                record_failure(order_num, action == "recover", review=True)
                increment(summary.recheck_skipped_by_reason, blocked_reason)
                journal.update_order(order_num, status_review_reason=blocked_reason)
                continue
            journal.assert_owned()
            try:
                if action == "recover" and not injected_creditnote_context:
                    refresh_creditnotes(journal.assert_owned)
                live_order = fetch_order_for_recheck(client, order_num)
                summary.rechecked_orders += 1
                if action != "recover" and needs_recovery_evidence(live_order) and not injected_creditnote_context:
                    refresh_creditnotes(journal.assert_owned)
                    # The complete creditnote scan may be slow. The actual
                    # mutation candidate must be read after that scan finishes.
                    live_order = fetch_order_for_recheck(client, order_num)
                    summary.rechecked_orders += 1
                live_action, target_name, reason = choose_action(live_order, prior, heartbeat=journal.assert_owned)
                journal.assert_owned()
            except Exception:
                record_failure(order_num, action == "recover")
                continue
            if live_action in {"skip", "review"}:
                increment(summary.recheck_skipped_by_reason, reason)
                if live_action == "review":
                    record_failure(order_num, action == "recover", review=True)
                    journal.update_order(order_num, status_review_reason=reason)
                continue
            # Resolve again from the same verified shop status catalogue. A
            # shipped recovery never reuses the configured paid target ID.
            action_status_id = _resolve_status_id(statuses, target_name, None)
            mutation_record = {
                "state": "pending", "source_status": live_order.get("status"),
                "source_last_change": live_order.get("last_change"),
                "target_status_id": action_status_id, "target_status_name": target_name,
                "reason": reason,
            }
            journal.update_order(order_num, status_mutation=mutation_record)
            journal.assert_owned()
            try:
                change_order_status(client, order_num, action_status_id, target_name, silent=live_action == "recover")
            except Exception:
                journal.update_order(order_num, status_mutation={**mutation_record, "state": "uncertain"})
                record_failure(order_num, live_action == "recover")
                continue
            journal.update_order(order_num, status_mutation={**mutation_record, "state": "verified"}, status_review_reason=None)
            if live_action == "recover":
                summary.recovered_orders += 1
                summary.recovered_order_nums.append(order_num)
                logger.info("Recovered order %s to status_id=%s", order_num, action_status_id)
            else:
                summary.updated_orders += 1
                summary.updated_order_nums.append(order_num)
                logger.info("Cancelled unpaid order %s with status_id=%s", order_num, action_status_id)
    return summary
