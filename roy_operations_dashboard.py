#!/usr/bin/env python3
"""Live ROY operations dashboard data and actions."""

from __future__ import annotations

import copy
import json
import math
import os
import time
import unicodedata
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport

from reporting_core import BASE_DEFAULT_PROJECT, load_project_env, load_project_settings, resolve_biznisweb_api_url


DEFAULT_PAID_STATUSES = ("Platba online - zaplatené",)
DEFAULT_COD_STATUSES = ("Čaká na vybavenie",)
DEFAULT_COD_PAYMENT_PATTERNS = ("dobierk", "dobirk")
DEFAULT_PICKUP_SHIPPING_NAMES = ("Osobný odber na sklade",)
DEFAULT_PICKUP_SHIPPING_IDS = ("11",)
DEFAULT_PICKUP_ACTION_STATUSES = ("Čaká na vybavenie", "Platba online - zaplatené", "Pripravené k odberu")
DEFAULT_SHIPPED_STATUS_NAME = "Odoslaná"
DEFAULT_SHIPPED_STATUS_ID = 4
DEFAULT_SCAN_MAX_PAGES = 30
DEFAULT_SCAN_MIN_PAGES = 8
DEFAULT_STOP_AFTER_EMPTY_FULFILLABLE_PAGES = 3
DEFAULT_CACHE_TTL_SECONDS = 60
DEFAULT_AUTO_REFRESH_SECONDS = 90


ROY_OPERATIONS_ORDER_QUERY = gql(
    """
query GetRoyOperationsOrders($params: OrderParams) {
  getOrderList(params: $params) {
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
      items {
        item_label
        ean
        import_code
        warehouse_number
        quantity
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


_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
STATE_VERSION = 1


def _empty_operations_state() -> Dict[str, Any]:
    return {
        "version": STATE_VERSION,
        "loss_acknowledgements": {},
        "inbound_orders": {},
        "auto_cleared_inbound_orders": [],
    }


def _project_env_name(project: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in str(project or "").upper())


def _state_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _local_state_path(project: str) -> Path:
    configured = os.getenv("ROY_OPERATIONS_STATE_PATH", "").strip()
    if configured:
        return Path(configured)
    return Path(__file__).resolve().parent / "data" / project / "operations_state.json"


def _state_s3_location(project: str, project_settings: Dict[str, Any]) -> Optional[Tuple[str, str, str]]:
    s3_settings = project_settings.get("live_dashboard_artifacts") or {}
    env_project = _project_env_name(project)
    bucket = (
        os.getenv(f"LIVE_DASHBOARD_STATE_S3_BUCKET_{env_project}", "").strip()
        or os.getenv("LIVE_DASHBOARD_STATE_S3_BUCKET", "").strip()
        or os.getenv(f"LIVE_DASHBOARD_S3_BUCKET_{env_project}", "").strip()
        or os.getenv("LIVE_DASHBOARD_S3_BUCKET", "").strip()
        or os.getenv(f"REPORT_S3_BUCKET_{env_project}", "").strip()
        or os.getenv("REPORT_S3_BUCKET", "").strip()
        or str(s3_settings.get("s3_bucket") or "").strip()
    )
    prefix = (
        os.getenv(f"LIVE_DASHBOARD_STATE_S3_PREFIX_{env_project}", "").strip()
        or os.getenv("LIVE_DASHBOARD_STATE_S3_PREFIX", "").strip()
        or os.getenv(f"LIVE_DASHBOARD_S3_PREFIX_{env_project}", "").strip()
        or os.getenv("LIVE_DASHBOARD_S3_PREFIX", "").strip()
        or os.getenv(f"REPORT_S3_PREFIX_{env_project}", "").strip()
        or os.getenv("REPORT_S3_PREFIX", "").strip()
        or str(s3_settings.get("s3_prefix") or "").strip()
        or f"daily-reports/{project}"
    ).strip("/")
    region = (
        os.getenv(f"AWS_REGION_{env_project}", "").strip()
        or os.getenv("AWS_REGION", "eu-central-1").strip()
        or "eu-central-1"
    )
    if not bucket:
        return None
    return bucket, f"{prefix}/operations/state.json", region


def _normalize_operations_state(raw: Any) -> Dict[str, Any]:
    state = _empty_operations_state()
    if not isinstance(raw, dict):
        return state
    state["version"] = int(raw.get("version") or STATE_VERSION)
    for section in ("loss_acknowledgements", "inbound_orders"):
        values = raw.get(section) if isinstance(raw.get(section), dict) else {}
        state[section] = {
            str(key).strip(): value
            for key, value in values.items()
            if str(key).strip() and isinstance(value, dict)
        }
    cleared = raw.get("auto_cleared_inbound_orders")
    if isinstance(cleared, list):
        state["auto_cleared_inbound_orders"] = [row for row in cleared[-50:] if isinstance(row, dict)]
    return state


def load_roy_operations_state(project: str, project_settings: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    project = (project or BASE_DEFAULT_PROJECT).strip() or BASE_DEFAULT_PROJECT
    project_settings = project_settings or load_project_settings(project)
    location = _state_s3_location(project, project_settings)
    if location is not None:
        bucket, key, region = location
        try:
            import boto3  # type: ignore

            response = boto3.client("s3", region_name=region).get_object(Bucket=bucket, Key=key)
            return _normalize_operations_state(json.loads(response["Body"].read().decode("utf-8")))
        except Exception:
            pass

    path = _local_state_path(project)
    if not path.exists():
        return _empty_operations_state()
    try:
        return _normalize_operations_state(json.loads(path.read_text(encoding="utf-8")))
    except Exception:
        return _empty_operations_state()


def save_roy_operations_state(
    project: str,
    state: Dict[str, Any],
    project_settings: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    project = (project or BASE_DEFAULT_PROJECT).strip() or BASE_DEFAULT_PROJECT
    project_settings = project_settings or load_project_settings(project)
    normalized = _normalize_operations_state(state)
    body = json.dumps(normalized, ensure_ascii=False, indent=2).encode("utf-8")
    location = _state_s3_location(project, project_settings)
    if location is not None:
        bucket, key, region = location
        import boto3  # type: ignore

        boto3.client("s3", region_name=region).put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/json; charset=utf-8",
            ServerSideEncryption="AES256",
        )
        return {"storage": "s3", "bucket": bucket, "key": key}

    path = _local_state_path(project)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_bytes(body)
    tmp_path.replace(path)
    return {"storage": "local", "path": str(path)}


def _validate_eta_date(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("Expected arrival date is required.")
    try:
        datetime.strptime(raw, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("Expected arrival date must use YYYY-MM-DD format.") from exc
    return raw


def acknowledge_loss_product(project: str, sku: str, product: str = "") -> Dict[str, Any]:
    project = (project or BASE_DEFAULT_PROJECT).strip() or BASE_DEFAULT_PROJECT
    if project != "roy":
        raise ValueError("Loss product acknowledgement is only enabled for project 'roy'.")
    sku = str(sku or "").strip()
    if not sku:
        raise ValueError("Missing product SKU.")
    project_settings = load_project_settings(project)
    state = load_roy_operations_state(project, project_settings)
    state.setdefault("loss_acknowledgements", {})[sku] = {
        "sku": sku,
        "product": str(product or "").strip(),
        "acknowledged_at": _state_now_iso(),
    }
    storage = save_roy_operations_state(project, state, project_settings)
    _CACHE.pop(project, None)
    return {"ok": True, "project": project, "sku": sku, "storage": storage}


def set_inbound_stock_order(
    project: str,
    sku: str,
    *,
    product: str = "",
    ordered_units: Any,
    expected_arrival_date: Any,
    baseline_available_quantity: Any = 0.0,
) -> Dict[str, Any]:
    project = (project or BASE_DEFAULT_PROJECT).strip() or BASE_DEFAULT_PROJECT
    if project != "roy":
        raise ValueError("Inbound stock tracking is only enabled for project 'roy'.")
    sku = str(sku or "").strip()
    if not sku:
        raise ValueError("Missing product SKU.")
    units = _to_float(ordered_units)
    if units <= 0:
        raise ValueError("Ordered units must be greater than zero.")
    eta = _validate_eta_date(expected_arrival_date)
    project_settings = load_project_settings(project)
    state = load_roy_operations_state(project, project_settings)
    existing = (state.get("inbound_orders") or {}).get(sku) or {}
    now = _state_now_iso()
    state.setdefault("inbound_orders", {})[sku] = {
        "sku": sku,
        "product": str(product or existing.get("product") or "").strip(),
        "ordered_units": round(units, 2),
        "expected_arrival_date": eta,
        "baseline_available_quantity": round(_to_float(baseline_available_quantity), 2),
        "created_at": existing.get("created_at") or now,
        "updated_at": now,
    }
    storage = save_roy_operations_state(project, state, project_settings)
    _CACHE.pop(project, None)
    return {"ok": True, "project": project, "sku": sku, "storage": storage, "inbound_order": state["inbound_orders"][sku]}


def clear_inbound_stock_order(project: str, sku: str) -> Dict[str, Any]:
    project = (project or BASE_DEFAULT_PROJECT).strip() or BASE_DEFAULT_PROJECT
    if project != "roy":
        raise ValueError("Inbound stock tracking is only enabled for project 'roy'.")
    sku = str(sku or "").strip()
    if not sku:
        raise ValueError("Missing product SKU.")
    project_settings = load_project_settings(project)
    state = load_roy_operations_state(project, project_settings)
    removed = (state.get("inbound_orders") or {}).pop(sku, None)
    storage = save_roy_operations_state(project, state, project_settings)
    _CACHE.pop(project, None)
    return {"ok": True, "project": project, "sku": sku, "removed": bool(removed), "storage": storage}


def _normalize_text(value: Any) -> str:
    raw = str(value or "").strip().lower()
    decomposed = unicodedata.normalize("NFKD", raw)
    without_marks = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return " ".join(without_marks.split())


def _as_list(value: Any, default: Iterable[str]) -> List[str]:
    if value is None:
        return [str(item) for item in default]
    if isinstance(value, str):
        return [value] if value.strip() else []
    return [str(item) for item in value if str(item or "").strip()]


def _as_int(value: Any, default: int, *, minimum: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, parsed)


def _to_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _pct_change(current: Optional[float], previous: Optional[float]) -> Optional[float]:
    if current is None or previous in (None, 0):
        return None
    return (float(current) - float(previous)) / abs(float(previous)) * 100.0


def resolve_roy_operations_settings(project_settings: Dict[str, Any]) -> Dict[str, Any]:
    raw = project_settings.get("operations_dashboard") or {}
    paid_statuses = _as_list(raw.get("paid_statuses"), DEFAULT_PAID_STATUSES)
    cod_statuses = _as_list(raw.get("cod_statuses"), DEFAULT_COD_STATUSES)
    cod_payment_patterns = _as_list(raw.get("cod_payment_patterns"), DEFAULT_COD_PAYMENT_PATTERNS)
    cod_payment_ids = _as_list(raw.get("cod_payment_ids"), ())
    pickup_shipping_names = _as_list(raw.get("personal_pickup_shipping_names"), DEFAULT_PICKUP_SHIPPING_NAMES)
    pickup_shipping_ids = _as_list(raw.get("personal_pickup_shipping_ids"), DEFAULT_PICKUP_SHIPPING_IDS)
    pickup_action_statuses = _as_list(raw.get("pickup_action_statuses"), DEFAULT_PICKUP_ACTION_STATUSES)
    shipped_status_name = str(raw.get("shipped_status_name") or DEFAULT_SHIPPED_STATUS_NAME).strip()

    return {
        "enabled": bool(raw.get("enabled", False)),
        "paid_statuses": paid_statuses,
        "paid_statuses_normalized": {_normalize_text(status) for status in paid_statuses},
        "cod_statuses": cod_statuses,
        "cod_statuses_normalized": {_normalize_text(status) for status in cod_statuses},
        "cod_payment_patterns": cod_payment_patterns,
        "cod_payment_patterns_normalized": {_normalize_text(pattern) for pattern in cod_payment_patterns},
        "cod_payment_ids": {str(value).strip() for value in cod_payment_ids if str(value).strip()},
        "personal_pickup_shipping_names": pickup_shipping_names,
        "personal_pickup_shipping_names_normalized": {_normalize_text(name) for name in pickup_shipping_names},
        "personal_pickup_shipping_ids": {str(value).strip() for value in pickup_shipping_ids if str(value).strip()},
        "pickup_action_statuses": pickup_action_statuses,
        "pickup_action_statuses_normalized": {_normalize_text(status) for status in pickup_action_statuses},
        "shipped_status_name": shipped_status_name,
        "shipped_status_name_normalized": _normalize_text(shipped_status_name),
        "shipped_status_id": _as_int(raw.get("shipped_status_id"), DEFAULT_SHIPPED_STATUS_ID, minimum=1),
        "scan_max_pages": _as_int(raw.get("scan_max_pages"), DEFAULT_SCAN_MAX_PAGES, minimum=1),
        "scan_min_pages": _as_int(raw.get("scan_min_pages"), DEFAULT_SCAN_MIN_PAGES, minimum=1),
        "stop_after_empty_fulfillable_pages": _as_int(
            raw.get("stop_after_empty_fulfillable_pages"),
            DEFAULT_STOP_AFTER_EMPTY_FULFILLABLE_PAGES,
            minimum=0,
        ),
        "cache_ttl_seconds": _as_int(raw.get("cache_ttl_seconds"), DEFAULT_CACHE_TTL_SECONDS, minimum=1),
        "auto_refresh_seconds": _as_int(raw.get("auto_refresh_seconds"), DEFAULT_AUTO_REFRESH_SECONDS, minimum=30),
    }


def _price_elements(order: Dict[str, Any], element_type: str) -> List[Dict[str, Any]]:
    wanted = _normalize_text(element_type)
    return [
        element
        for element in order.get("price_elements") or []
        if _normalize_text(element.get("type")) == wanted
    ]


def _price_element_info(order: Dict[str, Any], element_type: str) -> Dict[str, Any]:
    elements = _price_elements(order, element_type)
    if not elements:
        return {"title": "", "reference_id": "", "value": "", "price": None}
    first = elements[0] or {}
    return {
        "title": str(first.get("title") or "").strip(),
        "reference_id": str(first.get("reference_id") or "").strip(),
        "value": str(first.get("value") or "").strip(),
        "price": first.get("price") or None,
    }


def _status_name(order: Dict[str, Any]) -> str:
    return str((order.get("status") or {}).get("name") or "").strip()


def _status_id(order: Dict[str, Any]) -> str:
    return str((order.get("status") or {}).get("id") or "").strip()


def _is_cod_payment(order: Dict[str, Any], settings: Dict[str, Any]) -> bool:
    payment = _price_element_info(order, "payment")
    reference_id = str(payment.get("reference_id") or "").strip()
    if reference_id and reference_id in settings["cod_payment_ids"]:
        return True
    payment_title = _normalize_text(payment.get("title"))
    return any(pattern and pattern in payment_title for pattern in settings["cod_payment_patterns_normalized"])


def _is_paid_online(order: Dict[str, Any], settings: Dict[str, Any]) -> bool:
    return _normalize_text(_status_name(order)) in settings["paid_statuses_normalized"]


def _is_cod_fulfillable(order: Dict[str, Any], settings: Dict[str, Any]) -> bool:
    return (
        _normalize_text(_status_name(order)) in settings["cod_statuses_normalized"]
        and _is_cod_payment(order, settings)
    )


def _is_fulfillable_order(order: Dict[str, Any], settings: Dict[str, Any]) -> Tuple[bool, str]:
    if _is_paid_online(order, settings):
        return True, "paid_online"
    if _is_cod_fulfillable(order, settings):
        return True, "cod_waiting"
    return False, "not_ready"


def _is_personal_pickup(order: Dict[str, Any], settings: Dict[str, Any]) -> bool:
    shipping = _price_element_info(order, "shipping")
    reference_id = str(shipping.get("reference_id") or "").strip()
    if reference_id and reference_id in settings["personal_pickup_shipping_ids"]:
        return True
    shipping_title = _normalize_text(shipping.get("title"))
    return any(name and name in shipping_title for name in settings["personal_pickup_shipping_names_normalized"])


def _order_items(order: Dict[str, Any]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for item in order.get("items") or []:
        quantity = _to_float(item.get("quantity"))
        if quantity <= 0:
            continue
        items.append(
            {
                "label": str(item.get("item_label") or "").strip(),
                "quantity": quantity,
                "ean": str(item.get("ean") or "").strip(),
                "import_code": str(item.get("import_code") or "").strip(),
                "warehouse_number": str(item.get("warehouse_number") or "").strip(),
            }
        )
    return items


def _public_order_row(order: Dict[str, Any], settings: Dict[str, Any]) -> Dict[str, Any]:
    payment = _price_element_info(order, "payment")
    shipping = _price_element_info(order, "shipping")
    fulfillable, reason = _is_fulfillable_order(order, settings)
    is_pickup = _is_personal_pickup(order, settings)
    status_name = _status_name(order)
    status_norm = _normalize_text(status_name)
    pickup_action_allowed = (
        is_pickup
        and status_norm != settings["shipped_status_name_normalized"]
        and status_norm in settings["pickup_action_statuses_normalized"]
    )
    return {
        "id": order.get("id"),
        "order_num": order.get("order_num"),
        "purchase_at": order.get("pur_date"),
        "last_change": order.get("last_change"),
        "status": status_name,
        "status_id": _status_id(order),
        "sum": (order.get("sum") or {}).get("formatted"),
        "sum_value": _to_float((order.get("sum") or {}).get("value")),
        "payment": payment,
        "shipping": shipping,
        "items": _order_items(order),
        "fulfillable": fulfillable,
        "fulfillment_reason": reason,
        "personal_pickup": is_pickup,
        "pickup_action_allowed": pickup_action_allowed,
    }


def build_roy_orders_snapshot(
    *,
    project: str,
    orders: List[Dict[str, Any]],
    settings: Dict[str, Any],
    scan: Optional[Dict[str, Any]] = None,
    generated_at: Optional[str] = None,
) -> Dict[str, Any]:
    if not settings["enabled"]:
        raise ValueError(f"ROY operations dashboard is not enabled for project '{project}'.")

    rows = [_public_order_row(order, settings) for order in orders]
    fulfillable_orders = [row for row in rows if row["fulfillable"]]
    pickup_orders = [row for row in rows if row["personal_pickup"] and row["status"] != settings["shipped_status_name"]]
    paid_orders = [row for row in fulfillable_orders if row["fulfillment_reason"] == "paid_online"]
    cod_orders = [row for row in fulfillable_orders if row["fulfillment_reason"] == "cod_waiting"]

    status_counts: Dict[str, int] = defaultdict(int)
    for row in fulfillable_orders:
        status_counts[str(row.get("status") or "")] += 1

    generated = generated_at or datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    fulfillable_orders.sort(key=lambda row: (str(row.get("purchase_at") or ""), str(row.get("order_num") or "")))
    pickup_orders.sort(key=lambda row: (str(row.get("purchase_at") or ""), str(row.get("order_num") or "")))

    return {
        "project": project,
        "generated_at": generated,
        "auto_refresh_seconds": settings["auto_refresh_seconds"],
        "summary": {
            "fulfillable_orders": len(fulfillable_orders),
            "paid_online_orders": len(paid_orders),
            "cod_waiting_orders": len(cod_orders),
            "personal_pickups": len(pickup_orders),
            "pickup_actions_available": len([row for row in pickup_orders if row["pickup_action_allowed"]]),
            "fulfillable_value": round(sum(float(row.get("sum_value") or 0.0) for row in fulfillable_orders), 2),
            "status_counts": dict(sorted(status_counts.items())),
        },
        "scan": scan or {},
        "orders": fulfillable_orders,
        "personal_pickups": pickup_orders,
    }


def _build_client(project: str, project_settings: Dict[str, Any]) -> Client:
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


def fetch_open_orders_for_roy_operations(project: str, settings: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    project_settings = load_project_settings(project)
    client = _build_client(project, project_settings)

    orders: List[Dict[str, Any]] = []
    cursor: Optional[int] = None
    page_count = 0
    has_next_page = True
    empty_fulfillable_pages = 0
    oldest_order_at: Optional[str] = None
    fulfillable_seen = 0
    pickup_seen = 0
    stop_reason = "api_exhausted"

    while has_next_page and page_count < settings["scan_max_pages"]:
        params: Dict[str, Any] = {
            "limit": 30,
            "order_by": "pur_date",
            "sort": "DESC",
        }
        if cursor is not None:
            params["cursor"] = cursor

        result = client.execute(ROY_OPERATIONS_ORDER_QUERY, variable_values={"params": params})
        payload = result.get("getOrderList") or {}
        page_orders = [order for order in (payload.get("data") or []) if order]
        orders.extend(page_orders)
        page_count += 1

        page_fulfillable = sum(1 for order in page_orders if _is_fulfillable_order(order, settings)[0])
        page_pickups = sum(1 for order in page_orders if _is_personal_pickup(order, settings))
        fulfillable_seen += page_fulfillable
        pickup_seen += page_pickups
        if page_fulfillable or page_pickups:
            empty_fulfillable_pages = 0
        else:
            empty_fulfillable_pages += 1

        for order in page_orders:
            pur_date = str(order.get("pur_date") or "")
            if pur_date and (oldest_order_at is None or pur_date < oldest_order_at):
                oldest_order_at = pur_date

        page_info = payload.get("pageInfo") or {}
        has_next_page = bool(page_info.get("hasNextPage"))
        cursor = page_info.get("nextCursor")
        if cursor in ("", None):
            has_next_page = False
        if (
            has_next_page
            and page_count >= settings["scan_min_pages"]
            and settings["stop_after_empty_fulfillable_pages"] > 0
            and empty_fulfillable_pages >= settings["stop_after_empty_fulfillable_pages"]
        ):
            has_next_page = False
            stop_reason = "empty_fulfillable_pages"

    limit_reached = bool(has_next_page and page_count >= settings["scan_max_pages"])
    if limit_reached:
        stop_reason = "scan_max_pages"

    scan = {
        "orders_scanned": len(orders),
        "pages_scanned": page_count,
        "scan_max_pages": settings["scan_max_pages"],
        "scan_min_pages": settings["scan_min_pages"],
        "stop_after_empty_fulfillable_pages": settings["stop_after_empty_fulfillable_pages"],
        "empty_fulfillable_pages_at_stop": empty_fulfillable_pages,
        "fulfillable_seen_during_scan": fulfillable_seen,
        "personal_pickups_seen_during_scan": pickup_seen,
        "oldest_order_at_scanned": oldest_order_at,
        "limit_reached": limit_reached,
        "stop_reason": stop_reason,
        "source": "biznisweb_api_desc_purchase_date_payment_shipping_filter",
    }
    return orders, scan


def _sum_series(series: Dict[str, Any], keys: Iterable[str], indexes: Iterable[int]) -> float:
    total = 0.0
    for key in keys:
        values = series.get(key) or []
        for index in indexes:
            if 0 <= index < len(values):
                total += _to_float(values[index])
    return total


def _sum_first_available_series(series: Dict[str, Any], keys: Iterable[str], indexes: Iterable[int]) -> float:
    for key in keys:
        values = series.get(key)
        if values:
            return _sum_series(series, (key,), indexes)
    return 0.0


def _month_label(month_key: str) -> str:
    try:
        dt = datetime.strptime(month_key, "%Y-%m")
    except ValueError:
        return month_key
    return dt.strftime("%B %Y")


def _compute_kpi_metrics_from_series(series: Dict[str, Any], indexes: List[int]) -> Dict[str, Any]:
    revenue = _sum_series(series, ("revenue",), indexes)
    orders = _sum_series(series, ("orders",), indexes)
    product_cost = _sum_series(series, ("product_cost",), indexes)
    packaging = _sum_series(series, ("packaging",), indexes)
    shipping = _sum_series(series, ("shipping",), indexes)
    total_ads = _sum_series(series, ("total_ads",), indexes)
    profit = _sum_first_available_series(series, ("profit_without_fixed", "profit"), indexes)
    profit_with_fixed = _sum_series(series, ("profit_with_fixed",), indexes)
    pre_ad_contribution = revenue - product_cost - packaging - shipping
    return {
        "revenue": round(revenue, 2),
        "profit": round(profit, 2),
        "orders": round(orders, 2),
        "aov": (revenue / orders) if orders > 0 else 0.0,
        "cac": (total_ads / orders) if orders > 0 else None,
        "roas": (revenue / total_ads) if total_ads > 0 else None,
        "pre_ad_contribution_margin": (pre_ad_contribution / revenue * 100.0) if revenue > 0 else 0.0,
        "post_ad_margin": (profit / revenue * 100.0) if revenue > 0 else 0.0,
        "company_margin_with_fixed": (profit_with_fixed / revenue * 100.0) if revenue > 0 else 0.0,
    }


def _build_monthly_kpis_from_series(series: Dict[str, Any]) -> List[Dict[str, Any]]:
    dates = [str(value or "") for value in series.get("dates") or []]
    month_indexes: Dict[str, List[int]] = defaultdict(list)
    for index, value in enumerate(dates):
        if len(value) >= 7:
            month_indexes[value[:7]].append(index)

    months: List[Dict[str, Any]] = []
    previous_metrics: Optional[Dict[str, Any]] = None
    for month_key in sorted(month_indexes):
        indexes = month_indexes[month_key]
        metrics = _compute_kpi_metrics_from_series(series, indexes)
        comparisons = {
            key: {"vs_previous_month": _pct_change(metrics.get(key), previous_metrics.get(key) if previous_metrics else None)}
            for key in metrics
        }
        months.append(
            {
                "key": month_key,
                "label_en": _month_label(month_key),
                "label_sk": month_key,
                "date_from": dates[indexes[0]],
                "date_to": dates[indexes[-1]],
                "metrics": metrics,
                "secondary_metrics": {"company_margin_with_fixed": round(_sum_series(series, ("profit_with_fixed",), indexes), 2)},
                "comparisons": comparisons,
            }
        )
        previous_metrics = metrics
    return months


def build_executive_kpi_snapshot(report_payload: Dict[str, Any]) -> Dict[str, Any]:
    dashboard = report_payload.get("dashboard") if isinstance(report_payload.get("dashboard"), dict) else {}
    kpis = dashboard.get("kpis") if isinstance(dashboard.get("kpis"), dict) else {}
    series = dashboard.get("series") if isinstance(dashboard.get("series"), dict) else {}
    return {
        "metric_defs": kpis.get("metric_defs") or [],
        "default_window": kpis.get("default_window") or "monthly",
        "windows": kpis.get("windows") or {},
        "comparisons": kpis.get("comparisons") or {},
        "comparison_labels": kpis.get("comparison_labels") or {},
        "months": _build_monthly_kpis_from_series(series),
        "source_generated_at": report_payload.get("generated_at"),
        "source_range": {
            "date_from": report_payload.get("date_from"),
            "date_to": report_payload.get("date_to"),
        },
    }


def build_commercial_snapshot(report_payload: Dict[str, Any], state: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    dashboard = report_payload.get("dashboard") if isinstance(report_payload.get("dashboard"), dict) else {}
    roy_demand = dashboard.get("roy_product_demand") if isinstance(dashboard.get("roy_product_demand"), dict) else {}
    acknowledged = set((state or {}).get("loss_acknowledgements") or {})

    product_revenue_rows = list(roy_demand.get("product_revenue_rows") or [])
    product_profit_rows = list(roy_demand.get("product_profit_rows") or [])
    if not product_profit_rows:
        product_profit_rows = list(dashboard.get("products") or [])

    loss_rows = []
    hidden_loss_count = 0
    for row in list(roy_demand.get("loss_product_rows") or []):
        sku = str(row.get("sku") or "").strip()
        if sku and sku in acknowledged:
            hidden_loss_count += 1
            continue
        loss_rows.append(row)

    return {
        "brand_revenue_rows": list(roy_demand.get("brand_revenue_rows") or [])[:3],
        "brand_profit_rows": list(roy_demand.get("brand_profit_rows") or [])[:3],
        "product_revenue_rows": product_revenue_rows[:10],
        "product_profit_rows": product_profit_rows[:10],
        "loss_product_rows": loss_rows[:80],
        "acknowledged_loss_product_count": hidden_loss_count,
    }


def _inventory_row_collections(inventory: Dict[str, Any]) -> Iterable[Tuple[str, List[Dict[str, Any]]]]:
    for key in (
        "alert_rows",
        "stock_risk_rows",
        "inventory_rows",
        "restock_priority_rows",
        "revenue_at_risk_rows",
        "forecast_rows",
    ):
        rows = inventory.get(key)
        if isinstance(rows, list):
            yield key, rows


def _current_availability_by_sku(inventory: Dict[str, Any]) -> Dict[str, float]:
    availability: Dict[str, float] = {}
    for _, rows in _inventory_row_collections(inventory):
        for row in rows:
            sku = str(row.get("sku") or "").strip()
            if not sku:
                continue
            value = _to_float(row.get("available_quantity"))
            if sku not in availability or value > availability[sku]:
                availability[sku] = value
    return availability


def _auto_clear_restocked_inbound_orders(state: Dict[str, Any], inventory: Dict[str, Any]) -> bool:
    inbound = state.get("inbound_orders") if isinstance(state.get("inbound_orders"), dict) else {}
    if not inbound:
        return False
    availability = _current_availability_by_sku(inventory)
    cleared: List[Dict[str, Any]] = []
    for sku, record in list(inbound.items()):
        current_available = availability.get(str(sku))
        if current_available is None:
            continue
        baseline = _to_float(record.get("baseline_available_quantity"))
        if current_available > baseline + 1e-9:
            removed = inbound.pop(sku)
            cleared.append(
                {
                    "sku": sku,
                    "product": removed.get("product"),
                    "ordered_units": removed.get("ordered_units"),
                    "expected_arrival_date": removed.get("expected_arrival_date"),
                    "baseline_available_quantity": baseline,
                    "restocked_available_quantity": round(current_available, 2),
                    "cleared_at": _state_now_iso(),
                    "reason": "stock_increased_after_inbound_marker",
                }
            )
    if not cleared:
        return False
    history = state.setdefault("auto_cleared_inbound_orders", [])
    history.extend(cleared)
    state["auto_cleared_inbound_orders"] = history[-50:]
    state["inbound_orders"] = inbound
    return True


def _apply_inbound_to_inventory(inventory: Dict[str, Any], state: Dict[str, Any], project_settings: Dict[str, Any]) -> None:
    inbound = state.get("inbound_orders") if isinstance(state.get("inbound_orders"), dict) else {}
    if not inbound:
        inventory.setdefault("summary", {})["inbound_order_count"] = 0
        inventory.setdefault("summary", {})["inbound_ordered_units"] = 0.0
        return

    model = project_settings.get("inventory_model") or {}
    critical_days = max(1, int(model.get("critical_days_of_cover", 14) or 14))
    warning_days = max(critical_days, int(model.get("warning_days_of_cover", 30) or 30))
    watch_days = max(warning_days, int(model.get("watch_days_of_cover", 45) or 45))
    reorder_cover_days = max(1, int(model.get("reorder_cover_days", warning_days) or warning_days))
    hero_cover_days = max(reorder_cover_days, int(model.get("hero_reorder_cover_days", watch_days) or watch_days))

    today = datetime.now(timezone.utc).date()
    risk_30d = {"Negative stock", "Out of stock", "Critical", "Low"}
    risk_45d = {"Negative stock", "Out of stock", "Critical", "Low", "Watch"}

    def apply_to_row(row: Dict[str, Any]) -> None:
        sku = str(row.get("sku") or "").strip()
        record = inbound.get(sku)
        if not record:
            return
        ordered_units = _to_float(record.get("ordered_units"))
        if ordered_units <= 0:
            return

        available = _to_float(row.get("available_quantity"))
        net_available = available + ordered_units
        alert_30d_units = _to_float(row.get("alert_30d_units"))
        if alert_30d_units <= 0:
            alert_30d_units = _to_float(row.get("recent_30d_units")) or _to_float(row.get("forecast_30d_units"))
        daily_units = alert_30d_units / 30.0 if alert_30d_units > 0 else 0.0
        days_of_cover = (net_available / daily_units) if daily_units > 0 else None
        lead_time_working = int(round(_to_float(row.get("lead_time_working_days"))))
        lead_time_calendar = int(math.ceil(max(0, lead_time_working) * (7.0 / 5.0)))
        target_cover = lead_time_calendar + (hero_cover_days if bool(row.get("strategic_stock_flag")) else reorder_cover_days)

        existing_suggested = _to_float(row.get("suggested_reorder_units"))
        if daily_units > 0:
            suggested = max(math.ceil((daily_units * target_cover) - net_available), 0.0)
        else:
            suggested = max(existing_suggested - ordered_units, 0.0)

        current_risk = str(row.get("stock_risk_level") or "")
        available_raw = _to_float(row.get("available_quantity_raw", available))
        if daily_units > 0:
            if available_raw < 0 and net_available < 0:
                current_risk = "Negative stock"
            elif net_available <= 0:
                current_risk = "Out of stock"
            elif days_of_cover is not None and days_of_cover <= critical_days:
                current_risk = "Critical"
            elif days_of_cover is not None and days_of_cover <= warning_days:
                current_risk = "Low"
            elif days_of_cover is not None and days_of_cover <= watch_days:
                current_risk = "Watch"
            else:
                current_risk = "Healthy"
        elif net_available > 0 and current_risk in risk_45d:
            current_risk = "Healthy"

        projected_stockout_date = row.get("projected_stockout_date")
        if days_of_cover is not None and days_of_cover > 0 and days_of_cover < 3650:
            projected_stockout_date = (today + timedelta(days=int(math.ceil(days_of_cover)))).isoformat()
        elif daily_units <= 0:
            projected_stockout_date = None

        expected_arrival_date = str(record.get("expected_arrival_date") or "").strip()
        inbound_covers = bool(ordered_units > 0 and (suggested <= 0 or current_risk not in risk_30d))
        row.update(
            {
                "inbound_ordered_units": round(ordered_units, 2),
                "inbound_expected_arrival_date": expected_arrival_date,
                "inbound_created_at": record.get("created_at"),
                "inbound_updated_at": record.get("updated_at"),
                "inbound_baseline_available_quantity": record.get("baseline_available_quantity"),
                "net_available_quantity": round(net_available, 2),
                "stock_risk_level": current_risk,
                "days_of_cover": round(days_of_cover, 1) if days_of_cover is not None else row.get("days_of_cover"),
                "projected_stockout_date": projected_stockout_date,
                "suggested_reorder_units": round(suggested, 1),
                "inbound_covers_reorder_flag": inbound_covers,
            }
        )
        if inbound_covers:
            row["reorder_action_label"] = "Inbound ordered"
            row["reorder_now_flag"] = False
            row["prepare_po_flag"] = False
        elif ordered_units > 0 and str(row.get("reorder_action_label") or "") in {"Order now", "Prepare PO", "30d alert"}:
            row["reorder_action_label"] = "Partially ordered"

    for _, rows in _inventory_row_collections(inventory):
        for row in rows:
            apply_to_row(row)

    def keep_30d(row: Dict[str, Any]) -> bool:
        if bool(row.get("inbound_covers_reorder_flag")):
            return False
        return str(row.get("stock_risk_level") or "") in risk_30d

    def keep_45d(row: Dict[str, Any]) -> bool:
        if bool(row.get("inbound_covers_reorder_flag")):
            return False
        return str(row.get("stock_risk_level") or "") in risk_45d

    inventory["alert_rows"] = [row for row in inventory.get("alert_rows", []) if keep_30d(row)]
    inventory["restock_priority_rows"] = [row for row in inventory.get("restock_priority_rows", []) if keep_45d(row)]
    inventory["revenue_at_risk_rows"] = [row for row in inventory.get("revenue_at_risk_rows", []) if keep_45d(row)]
    inventory["stock_risk_rows"] = [row for row in inventory.get("stock_risk_rows", []) if keep_45d(row)]

    summary = inventory.setdefault("summary", {})
    active_inbound = [record for record in inbound.values() if _to_float(record.get("ordered_units")) > 0]
    next_eta = min((str(record.get("expected_arrival_date")) for record in active_inbound if record.get("expected_arrival_date")), default=None)
    availability = _current_availability_by_sku(inventory)
    inventory["inbound_order_rows"] = sorted(
        [
            {
                "sku": str(record.get("sku") or sku),
                "product": str(record.get("product") or ""),
                "ordered_units": round(_to_float(record.get("ordered_units")), 2),
                "expected_arrival_date": record.get("expected_arrival_date"),
                "baseline_available_quantity": _to_float(record.get("baseline_available_quantity")),
                "current_available_quantity": round(availability.get(str(sku), _to_float(record.get("baseline_available_quantity"))), 2),
                "created_at": record.get("created_at"),
                "updated_at": record.get("updated_at"),
            }
            for sku, record in inbound.items()
            if _to_float(record.get("ordered_units")) > 0
        ],
        key=lambda row: (str(row.get("expected_arrival_date") or "9999-99-99"), str(row.get("sku") or "")),
    )
    alert_rows = inventory.get("alert_rows", [])
    restock_rows = inventory.get("restock_priority_rows", [])
    revenue_risk_rows = inventory.get("revenue_at_risk_rows", [])
    stock_risk_rows = inventory.get("stock_risk_rows", [])
    summary.update(
        {
            "inbound_order_count": len(active_inbound),
            "inbound_ordered_units": round(sum(_to_float(record.get("ordered_units")) for record in active_inbound), 1),
            "inbound_next_arrival_date": next_eta,
            "alert_delivery_count": len(alert_rows),
            "alert_reorder_now_count": sum(1 for row in alert_rows if str(row.get("reorder_action_label") or "") == "Order now"),
            "alert_prepare_po_count": sum(1 for row in alert_rows if str(row.get("reorder_action_label") or "") == "Prepare PO"),
            "stock_risk_critical_count": sum(1 for row in stock_risk_rows if str(row.get("stock_risk_level") or "") in {"Negative stock", "Out of stock", "Critical"}),
            "stock_risk_30d_count": sum(1 for row in stock_risk_rows if str(row.get("stock_risk_level") or "") in risk_30d),
            "stock_risk_45d_count": sum(1 for row in stock_risk_rows if str(row.get("stock_risk_level") or "") in risk_45d),
            "revenue_at_risk_30d": round(sum(_to_float(row.get("alert_30d_revenue")) for row in alert_rows), 2),
            "profit_at_risk_30d": round(sum(_to_float(row.get("alert_30d_profit_estimate")) for row in alert_rows), 2),
            "revenue_at_risk_45d": round(sum(_to_float(row.get("alert_30d_revenue")) for row in revenue_risk_rows), 2),
            "profit_at_risk_45d": round(sum(_to_float(row.get("alert_30d_profit_estimate")) for row in revenue_risk_rows), 2),
            "restock_priority_urgent_count": sum(1 for row in restock_rows if _to_float(row.get("restock_priority_score")) >= 80),
            "restock_priority_high_count": sum(
                1 for row in restock_rows if 60 <= _to_float(row.get("restock_priority_score")) < 80
            ),
        }
    )


def build_inventory_snapshot(
    report_payload: Dict[str, Any],
    *,
    state: Optional[Dict[str, Any]] = None,
    project_settings: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], bool]:
    dashboard = report_payload.get("dashboard") if isinstance(report_payload.get("dashboard"), dict) else {}
    roy_inventory = dashboard.get("roy_product_demand") if isinstance(dashboard.get("roy_product_demand"), dict) else {}
    summary = roy_inventory.get("summary") if isinstance(roy_inventory.get("summary"), dict) else {}
    inventory = {
        "summary": summary or {},
        "alert_rows": list(roy_inventory.get("alert_rows") or [])[:120],
        "stock_risk_rows": list(roy_inventory.get("stock_risk_rows") or [])[:120],
        "inventory_rows": list(roy_inventory.get("inventory_rows") or [])[:160],
        "restock_priority_rows": list(roy_inventory.get("restock_priority_rows") or [])[:120],
        "revenue_at_risk_rows": list(roy_inventory.get("revenue_at_risk_rows") or [])[:120],
        "forecast_rows": list(roy_inventory.get("forecast_rows") or [])[:80],
        "inbound_order_rows": [],
    }
    state_changed = False
    if state is not None:
        state_changed = _auto_clear_restocked_inbound_orders(state, inventory)
        if project_settings is not None:
            _apply_inbound_to_inventory(inventory, state, project_settings)
    return inventory, state_changed


def generate_roy_operations_snapshot(project: str, report_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    project = (project or BASE_DEFAULT_PROJECT).strip() or BASE_DEFAULT_PROJECT
    if project != "roy":
        raise ValueError("ROY operations dashboard is only enabled for project 'roy'.")

    load_project_env(project)
    project_settings = load_project_settings(project)
    settings = resolve_roy_operations_settings(project_settings)
    orders, scan = fetch_open_orders_for_roy_operations(project, settings)
    order_snapshot = build_roy_orders_snapshot(project=project, orders=orders, settings=settings, scan=scan)
    payload = report_payload or {}
    operations_state = load_roy_operations_state(project, project_settings)
    inventory_snapshot, state_changed = build_inventory_snapshot(
        payload,
        state=operations_state,
        project_settings=project_settings,
    )
    if state_changed:
        save_roy_operations_state(project, operations_state, project_settings)
    return {
        "marker": "roy-operations-dashboard",
        "project": project,
        "generated_at": order_snapshot["generated_at"],
        "auto_refresh_seconds": order_snapshot["auto_refresh_seconds"],
        "orders": order_snapshot,
        "executive_kpis": build_executive_kpi_snapshot(payload),
        "inventory": inventory_snapshot,
        "performance": build_commercial_snapshot(payload, operations_state),
        "operations_state": {
            "inbound_order_count": len(operations_state.get("inbound_orders") or {}),
            "acknowledged_loss_product_count": len(operations_state.get("loss_acknowledgements") or {}),
            "auto_cleared_inbound_order_count": len(operations_state.get("auto_cleared_inbound_orders") or []),
        },
    }


def get_cached_roy_operations_snapshot(
    project: str,
    *,
    report_payload: Optional[Dict[str, Any]] = None,
    force_refresh: bool = False,
) -> Dict[str, Any]:
    project = (project or BASE_DEFAULT_PROJECT).strip() or BASE_DEFAULT_PROJECT
    project_settings = load_project_settings(project)
    settings = resolve_roy_operations_settings(project_settings)
    if not settings["enabled"]:
        raise ValueError(f"ROY operations dashboard is not enabled for project '{project}'.")

    cache_key = project
    now = time.monotonic()
    cached = _CACHE.get(cache_key)
    if cached and not force_refresh:
        cached_at, payload = cached
        if now - cached_at <= settings["cache_ttl_seconds"]:
            result = copy.deepcopy(payload)
            result["cache"] = {
                "status": "fresh",
                "age_seconds": round(now - cached_at, 1),
                "ttl_seconds": settings["cache_ttl_seconds"],
            }
            return result

    try:
        payload = generate_roy_operations_snapshot(project, report_payload=report_payload)
    except Exception as exc:
        if cached:
            cached_at, payload = cached
            result = copy.deepcopy(payload)
            result["cache"] = {
                "status": "stale_after_error",
                "age_seconds": round(now - cached_at, 1),
                "ttl_seconds": settings["cache_ttl_seconds"],
                "error": str(exc),
            }
            return result
        raise

    _CACHE[cache_key] = (now, copy.deepcopy(payload))
    payload["cache"] = {
        "status": "refreshed",
        "age_seconds": 0,
        "ttl_seconds": settings["cache_ttl_seconds"],
    }
    return payload


def _resolve_shipped_status_id(client: Client, settings: Dict[str, Any]) -> int:
    configured = int(settings.get("shipped_status_id") or 0)
    if configured > 0:
        return configured
    result = client.execute(LIST_ORDER_STATUSES_QUERY, variable_values={"lang_code": "SK"})
    target = settings["shipped_status_name_normalized"]
    for row in result.get("listOrderStatuses") or []:
        if _normalize_text(row.get("name")) == target:
            return int(row["id"])
    raise RuntimeError(f"Target status '{settings['shipped_status_name']}' not found in BiznisWeb.")


def mark_personal_pickup_shipped(project: str, order_num: str) -> Dict[str, Any]:
    project = (project or BASE_DEFAULT_PROJECT).strip() or BASE_DEFAULT_PROJECT
    if project != "roy":
        raise ValueError("Pickup shipping action is only enabled for project 'roy'.")

    load_project_env(project)
    project_settings = load_project_settings(project)
    settings = resolve_roy_operations_settings(project_settings)
    if not settings["enabled"]:
        raise ValueError(f"ROY operations dashboard is not enabled for project '{project}'.")

    order_num = str(order_num or "").strip()
    if not order_num:
        raise ValueError("Missing order number.")

    client = _build_client(project, project_settings)
    result = client.execute(
        gql(
            """
query GetOrderForPickupAction($order_num: String!) {
  getOrder(order_num: $order_num) {
    order_num
    pur_date
    last_change
    status { id name }
    price_elements { type title value reference_id price { value formatted } }
    items { item_label ean import_code warehouse_number quantity }
    sum { value formatted }
  }
}
"""
        ),
        variable_values={"order_num": order_num},
    )
    order = result.get("getOrder")
    if not order:
        raise ValueError(f"Order '{order_num}' not found.")

    row = _public_order_row(order, settings)
    if not row["personal_pickup"]:
        raise ValueError(f"Order '{order_num}' is not configured as personal pickup.")
    if _normalize_text(row["status"]) == settings["shipped_status_name_normalized"]:
        raise ValueError(f"Order '{order_num}' is already in target status.")
    if not row["pickup_action_allowed"]:
        raise ValueError(f"Order '{order_num}' is not in an allowed pickup action status.")

    status_id = _resolve_shipped_status_id(client, settings)
    mutation_result = client.execute(
        CHANGE_ORDER_STATUS_MUTATION,
        variable_values={"order_num": order_num, "status_id": status_id},
    )
    _CACHE.pop(project, None)
    return {
        "ok": True,
        "project": project,
        "order_num": order_num,
        "target_status_id": status_id,
        "target_status_name": settings["shipped_status_name"],
        "result": mutation_result.get("changeOrderStatus"),
    }
