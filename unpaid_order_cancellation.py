#!/usr/bin/env python3
"""Automatically cancel stale unpaid BizniWeb orders."""

from __future__ import annotations

import os
import re
import unicodedata
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport

from logger_config import get_logger
from reporting_core import BASE_DEFAULT_PROJECT, load_project_env, load_project_settings, resolve_biznisweb_api_url


logger = get_logger("unpaid_order_cancellation")

DEFAULT_TARGET_STATUS_NAME = "Nezaplaten\u00e1 - zru\u0161en\u00e1 objedn\u00e1vka"
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


UNPAID_ORDER_QUERY = gql(
    """
query GetOrdersForUnpaidCancellation($params: OrderParams) {
  getOrderList(params: $params) {
    data {
      id
      order_num
      pur_date
      last_change
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


CHANGE_ORDER_STATUS_MUTATION = gql(
    """
mutation ChangeOrderStatus($order_num: String!, $status_id: Int!) {
  changeOrderStatus(order_num: $order_num, status_id: $status_id) {
    order_num
    last_change
    status {
      id
      name
    }
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
    normalized_payment_title_patterns: Tuple[str, ...] = field(init=False)
    normalized_candidate_statuses: Tuple[str, ...] = field(init=False)
    normalized_excluded_statuses: Tuple[str, ...] = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "normalized_target_status_name", normalize_text(self.target_status_name))
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
    total_orders_scanned: int = 0
    pages_scanned: int = 0
    eligible_orders: int = 0
    updated_orders: int = 0
    failed_orders: int = 0
    scan_limit_reached: bool = False
    scan_stop_reason: str = ""
    oldest_order_date: str = ""
    skipped_by_reason: Dict[str, int] = field(default_factory=dict)
    eligible_order_nums: List[str] = field(default_factory=list)
    updated_order_nums: List[str] = field(default_factory=list)
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

    age_days = max(1, int(raw.get("age_days", DEFAULT_AGE_DAYS)))
    scan_max_pages = max(1, int(raw.get("scan_max_pages", DEFAULT_SCAN_MAX_PAGES)))
    page_limit = max(1, min(DEFAULT_PAGE_LIMIT, int(raw.get("page_limit", DEFAULT_PAGE_LIMIT))))
    return UnpaidCancellationSettings(
        enabled=bool(raw.get("enabled", False)),
        age_days=age_days,
        target_status_name=str(raw.get("target_status_name") or DEFAULT_TARGET_STATUS_NAME),
        target_status_id=parsed_target_status_id,
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
        retries=3,
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


def payment_matches(order: Dict[str, Any], settings: UnpaidCancellationSettings) -> bool:
    configured_ids = {str(value).strip() for value in settings.payment_reference_ids if str(value).strip()}
    for element in _payment_elements(order):
        reference_id = str(element.get("reference_id") or "").strip()
        if reference_id and reference_id in configured_ids:
            return True
        normalized_title = normalize_text(element.get("title"))
        if any(pattern and pattern in normalized_title for pattern in settings.normalized_payment_title_patterns):
            return True
    return False


def cancellation_eligibility_reason(
    order: Dict[str, Any],
    settings: UnpaidCancellationSettings,
    cutoff_date: date,
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
    if settings.normalized_candidate_statuses and status not in settings.normalized_candidate_statuses:
        return "not_candidate_status"
    if not payment_matches(order, settings):
        return "payment_not_matched"
    return "eligible"


def is_order_eligible_for_cancellation(
    order: Dict[str, Any],
    settings: UnpaidCancellationSettings,
    cutoff_date: date,
) -> bool:
    return cancellation_eligibility_reason(order, settings, cutoff_date) == "eligible"


def list_order_statuses(client: Client, settings: UnpaidCancellationSettings) -> List[Dict[str, Any]]:
    result = client.execute(LIST_ORDER_STATUSES_QUERY, variable_values={"lang_code": settings.lang_code})
    return [row for row in (result.get("listOrderStatuses") or []) if row]


def resolve_target_status_id(client: Client, settings: UnpaidCancellationSettings) -> int:
    statuses = list_order_statuses(client, settings)
    target_norm = settings.normalized_target_status_name
    for row in statuses:
        row_id = int(row.get("id") or 0)
        row_name_norm = normalize_text(row.get("name"))
        if settings.target_status_id and row_id == settings.target_status_id:
            if row_name_norm != target_norm:
                raise RuntimeError(
                    f"Configured target_status_id={settings.target_status_id} resolves to "
                    f"'{row.get('name')}', expected '{settings.target_status_name}'."
                )
            return row_id
        if not settings.target_status_id and row_name_norm == target_norm:
            return row_id
    raise RuntimeError(f"Target status '{settings.target_status_name}' not found in BizniWeb.")


def fetch_orders_for_cancellation(
    client: Client,
    settings: UnpaidCancellationSettings,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    orders: List[Dict[str, Any]] = []
    cursor: Any = None
    has_next_page = True
    page_count = 0
    oldest_order_date = ""
    stop_reason = "api_exhausted"

    while has_next_page and page_count < settings.scan_max_pages:
        params: Dict[str, Any] = {
            "limit": settings.page_limit,
            "order_by": "pur_date",
            "sort": "DESC",
        }
        if cursor not in ("", None):
            params["cursor"] = cursor

        try:
            result = client.execute(UNPAID_ORDER_QUERY, variable_values={"params": params})
        except Exception as exc:
            partial_data = getattr(exc, "data", None)
            if not partial_data:
                if page_count == 0:
                    raise
                stop_reason = "api_error_after_partial_scan"
                logger.warning("Stopping order-list scan after BizniWeb API error on page %s: %s", page_count + 1, exc)
                has_next_page = False
                break
            logger.warning("Using partial order-list response after BizniWeb API error: %s", exc)
            result = partial_data
        payload = result.get("getOrderList") or {}
        page_orders = [order for order in (payload.get("data") or []) if order]
        orders.extend(page_orders)
        page_count += 1

        for order in page_orders:
            purchased_at = order_purchase_date(order)
            if purchased_at:
                text = purchased_at.strftime("%Y-%m-%d")
                if not oldest_order_date or text < oldest_order_date:
                    oldest_order_date = text

        page_info = payload.get("pageInfo") or {}
        has_next_page = bool(page_info.get("hasNextPage"))
        cursor = page_info.get("nextCursor")
        if cursor in ("", None):
            has_next_page = False

    scan_limit_reached = bool(has_next_page and page_count >= settings.scan_max_pages)
    if scan_limit_reached:
        stop_reason = "scan_max_pages"

    return orders, {
        "pages_scanned": page_count,
        "scan_limit_reached": scan_limit_reached,
        "scan_stop_reason": stop_reason,
        "oldest_order_date": oldest_order_date,
    }


def change_order_status(client: Client, order_num: str, status_id: int) -> Dict[str, Any]:
    result = client.execute(
        CHANGE_ORDER_STATUS_MUTATION,
        variable_values={"order_num": str(order_num), "status_id": int(status_id)},
    )
    return result.get("changeOrderStatus") or {}


def run_unpaid_order_cancellation(
    project_name: str,
    reference_date: Union[str, date, datetime, None] = None,
    dry_run: bool = False,
    client: Optional[Client] = None,
    project_settings: Optional[Dict[str, Any]] = None,
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
    )
    if not settings.enabled:
        logger.info("Unpaid order cancellation disabled for project=%s", project)
        return summary

    if client is None:
        if not loaded_project_env:
            load_project_env(project, logger=logger)
        client = build_client(project, project_settings)

    target_status_id = resolve_target_status_id(client, settings)
    summary.target_status_id = target_status_id

    orders, scan = fetch_orders_for_cancellation(client, settings)
    summary.total_orders_scanned = len(orders)
    summary.pages_scanned = int(scan.get("pages_scanned") or 0)
    summary.scan_limit_reached = bool(scan.get("scan_limit_reached"))
    summary.scan_stop_reason = str(scan.get("scan_stop_reason") or "")
    summary.oldest_order_date = str(scan.get("oldest_order_date") or "")

    eligible_orders: List[Dict[str, Any]] = []
    skipped: Dict[str, int] = {}
    for order in orders:
        reason = cancellation_eligibility_reason(order, settings, cutoff_date)
        if reason == "eligible":
            eligible_orders.append(order)
        else:
            skipped[reason] = skipped.get(reason, 0) + 1

    summary.eligible_orders = len(eligible_orders)
    summary.skipped_by_reason = dict(sorted(skipped.items()))
    summary.eligible_order_nums = [str(order.get("order_num") or "") for order in eligible_orders]

    logger.info(
        "Unpaid order cancellation scan project=%s cutoff=%s scanned=%s eligible=%s dry_run=%s",
        project,
        summary.cutoff_date,
        summary.total_orders_scanned,
        summary.eligible_orders,
        dry_run,
    )

    if dry_run:
        return summary

    for order in eligible_orders:
        order_num = str(order.get("order_num") or "").strip()
        if not order_num:
            summary.failed_orders += 1
            summary.failed_order_nums.append("")
            continue
        try:
            change_order_status(client, order_num, target_status_id)
            summary.updated_orders += 1
            summary.updated_order_nums.append(order_num)
            logger.info("Changed order %s to status_id=%s", order_num, target_status_id)
        except Exception as exc:  # pragma: no cover - API failure path
            summary.failed_orders += 1
            summary.failed_order_nums.append(order_num)
            logger.error("Failed to change order %s: %s", order_num, exc)

    return summary
