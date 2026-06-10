#!/usr/bin/env python3
"""Live ROY operations dashboard data and actions."""

from __future__ import annotations

import copy
import json
import math
import os
import re
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
DEFAULT_PICKUP_READY_STATUS_NAME = "Pripravené k odberu"
DEFAULT_PICKUP_READY_STATUS_ID = 0
DEFAULT_PICKUP_READY_ACTION_STATUSES = DEFAULT_PAID_STATUSES
DEFAULT_PICKUP_SHIP_ACTION_STATUSES = (DEFAULT_PICKUP_READY_STATUS_NAME,)
DEFAULT_SHIPPED_STATUS_NAME = "Odoslaná"
DEFAULT_SHIPPED_STATUS_ID = 4
DEFAULT_SCAN_MAX_PAGES = 30
DEFAULT_SCAN_MIN_PAGES = 8
DEFAULT_STOP_AFTER_EMPTY_FULFILLABLE_PAGES = 3
DEFAULT_CACHE_TTL_SECONDS = 60
DEFAULT_AUTO_REFRESH_SECONDS = 90
DEFAULT_WHOLESALE_DETECTION_DISCOUNT_THRESHOLD_PCT = 10.0
DEFAULT_WHOLESALE_DETECTION_REQUIRE_COMPANY = True
DEFAULT_WHOLESALE_RETAIL_TAX_RATE = 23.0
LATIN_FOLD_TRANSLATION = str.maketrans(
    {
        "Ł": "L",
        "ł": "l",
        "Đ": "D",
        "đ": "d",
        "Ð": "D",
        "ð": "d",
        "Þ": "Th",
        "þ": "th",
        "Æ": "Ae",
        "æ": "ae",
        "Œ": "Oe",
        "œ": "oe",
        "Ø": "O",
        "ø": "o",
        "ß": "ss",
        "ẞ": "SS",
        "Ħ": "H",
        "ħ": "h",
        "ı": "i",
    }
)


ROY_OPERATIONS_ORDER_QUERY = gql(
    """
query GetRoyOperationsOrders($params: OrderParams) {
  getOrderList(params: $params) {
    data {
      id
      order_num
      pur_date
      last_change
      note
      internal_note
      source
      status {
        id
        name
        color
      }
      customer {
        ... on Company {
          __typename
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
          __typename
          name
          surname
          phone
          email
        }
        ... on UnauthenticatedEmail {
          __typename
          name
          surname
          phone
          email
        }
      }
      invoice_address {
        company_name
        name
        surname
        street
        descriptive_number
        orientation_number
        city
        zip
        state
        country
        email
        phone
      }
      delivery_address {
        company_name
        name
        surname
        street
        descriptive_number
        orientation_number
        city
        zip
        state
        country
        email
        phone
      }
      price_elements {
        type
        title
        value
        reference_id
        price {
          value
          raw_value
          formatted
          is_net_price
        }
      }
      items {
        item_label
        ean
        import_code
        warehouse_number
        quantity
        tax_rate
        price {
          value
          raw_value
          formatted
          is_net_price
        }
        sum {
          value
          raw_value
          formatted
          is_net_price
        }
        sum_with_tax {
          value
          raw_value
          formatted
          is_net_price
        }
        product {
          title
          import_code
          final_price {
            value
            raw_value
            formatted
            is_net_price
          }
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


ROY_PRODUCT_STOCK_SEARCH_FIELDS = """
    data {
      id
      title
      active
      ean
      import_code
      warehouse_items {
        id
        warehouse_number
        quantity
        available_quantity
        status {
          id
          name
        }
      }
    }
"""


_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
STATE_VERSION = 1


def _empty_operations_state() -> Dict[str, Any]:
    return {
        "version": STATE_VERSION,
        "loss_acknowledgements": {},
        "inbound_orders": {},
        "auto_cleared_inbound_orders": [],
        "printed_picking_orders": {},
        "picking_print_batches": [],
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
    for section in ("loss_acknowledgements", "inbound_orders", "printed_picking_orders"):
        values = raw.get(section) if isinstance(raw.get(section), dict) else {}
        state[section] = {
            str(key).strip(): value
            for key, value in values.items()
            if str(key).strip() and isinstance(value, dict)
        }
    for section in ("auto_cleared_inbound_orders", "picking_print_batches"):
        rows = raw.get(section)
        if isinstance(rows, list):
            state[section] = [row for row in rows[-50:] if isinstance(row, dict)]
    return state


def _s3_error_code(exc: Exception) -> str:
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        error = response.get("Error")
        if isinstance(error, dict):
            return str(error.get("Code") or "").strip()
    return ""


def load_roy_operations_state(
    project: str,
    project_settings: Optional[Dict[str, Any]] = None,
    *,
    require_configured_remote: bool = False,
) -> Dict[str, Any]:
    project = (project or BASE_DEFAULT_PROJECT).strip() or BASE_DEFAULT_PROJECT
    project_settings = project_settings or load_project_settings(project)
    location = _state_s3_location(project, project_settings)
    if location is not None:
        bucket, key, region = location
        try:
            import boto3  # type: ignore

            response = boto3.client("s3", region_name=region).get_object(Bucket=bucket, Key=key)
            return _normalize_operations_state(json.loads(response["Body"].read().decode("utf-8")))
        except Exception as exc:
            if require_configured_remote and _s3_error_code(exc) not in {"NoSuchKey", "404"}:
                raise RuntimeError(f"Failed to load configured ROY operations state from s3://{bucket}/{key}: {exc}")
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


def _picking_order_key(order: Dict[str, Any]) -> str:
    return str(order.get("order_num") or order.get("id") or "").strip()


def filter_unprinted_picking_orders(
    orders: Iterable[Dict[str, Any]],
    state: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    printed = (state or {}).get("printed_picking_orders")
    printed_keys = {str(key).strip() for key in printed} if isinstance(printed, dict) else set()
    result: List[Dict[str, Any]] = []
    for order in orders:
        key = _picking_order_key(order)
        if key and key in printed_keys:
            continue
        result.append(order)
    return result


def mark_picking_orders_printed(
    state: Dict[str, Any],
    orders: Iterable[Dict[str, Any]],
    *,
    printed_at: Optional[str] = None,
) -> Dict[str, Any]:
    printed_at = printed_at or _state_now_iso()
    batch_id = "picking-" + "".join(ch for ch in printed_at if ch.isdigit())[:14]
    printed = state.setdefault("printed_picking_orders", {})
    if not isinstance(printed, dict):
        printed = {}
        state["printed_picking_orders"] = printed

    marked_order_nums: List[str] = []
    for order in orders:
        key = _picking_order_key(order)
        if not key or key in printed:
            continue
        record = {
            "order_num": key,
            "printed_at": printed_at,
            "batch_id": batch_id,
            "status": str(order.get("status") or "").strip(),
            "purchase_at": str(order.get("purchase_at") or "").strip(),
            "sum": str(order.get("sum") or "").strip(),
        }
        customer = order.get("customer") if isinstance(order.get("customer"), dict) else {}
        customer_name = str(customer.get("name") or customer.get("company_name") or "").strip()
        if customer_name:
            record["customer"] = customer_name
        printed[key] = record
        marked_order_nums.append(key)

    batch = {
        "batch_id": batch_id,
        "printed_at": printed_at,
        "order_count": len(marked_order_nums),
        "order_nums": marked_order_nums,
    }
    batches = state.setdefault("picking_print_batches", [])
    if not isinstance(batches, list):
        batches = []
    if marked_order_nums:
        batches.append(batch)
    state["picking_print_batches"] = [row for row in batches[-50:] if isinstance(row, dict)]
    return batch


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
    decomposed = unicodedata.normalize("NFKD", raw).translate(LATIN_FOLD_TRANSLATION)
    without_marks = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return " ".join(without_marks.split())


def _normalize_match_text(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or "")).translate(LATIN_FOLD_TRANSLATION)
    text = "".join(ch for ch in text if not unicodedata.combining(ch)).lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _matches_patterns(label: Any, patterns: Iterable[Any]) -> bool:
    normalized_label = _normalize_match_text(label)
    if not normalized_label:
        return False
    for pattern in patterns:
        normalized_pattern = _normalize_match_text(pattern)
        if normalized_pattern and normalized_pattern in normalized_label:
            return True
    return False


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


def _as_float(value: Any, default: float, *, minimum: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, parsed)


def _as_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "ano"}:
            return True
        if normalized in {"0", "false", "no", "n", "nie"}:
            return False
    return bool(value)


def _to_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _optional_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _pct_change(current: Optional[float], previous: Optional[float]) -> Optional[float]:
    if current is None or previous in (None, 0):
        return None
    return (float(current) - float(previous)) / abs(float(previous)) * 100.0


def resolve_roy_operations_settings(project_settings: Dict[str, Any]) -> Dict[str, Any]:
    raw = project_settings.get("operations_dashboard") or {}
    component_rules = project_settings.get("product_component_expansion_rules") or []
    component_rules = component_rules if isinstance(component_rules, list) else []
    wholesale_raw = raw.get("wholesale_detection") if isinstance(raw.get("wholesale_detection"), dict) else {}
    paid_statuses = _as_list(raw.get("paid_statuses"), DEFAULT_PAID_STATUSES)
    cod_statuses = _as_list(raw.get("cod_statuses"), DEFAULT_COD_STATUSES)
    cod_payment_patterns = _as_list(raw.get("cod_payment_patterns"), DEFAULT_COD_PAYMENT_PATTERNS)
    cod_payment_ids = _as_list(raw.get("cod_payment_ids"), ())
    pickup_shipping_names = _as_list(raw.get("personal_pickup_shipping_names"), DEFAULT_PICKUP_SHIPPING_NAMES)
    pickup_shipping_ids = _as_list(raw.get("personal_pickup_shipping_ids"), DEFAULT_PICKUP_SHIPPING_IDS)
    pickup_ready_status_name = str(raw.get("pickup_ready_status_name") or DEFAULT_PICKUP_READY_STATUS_NAME).strip()
    pickup_ready_action_statuses = _as_list(
        raw.get("pickup_ready_action_statuses"),
        DEFAULT_PICKUP_READY_ACTION_STATUSES,
    )
    pickup_ship_action_statuses = _as_list(
        raw.get("pickup_ship_action_statuses"),
        (pickup_ready_status_name,),
    )
    pickup_action_statuses = list(dict.fromkeys(pickup_ready_action_statuses + pickup_ship_action_statuses))
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
        "pickup_ready_status_name": pickup_ready_status_name,
        "pickup_ready_status_name_normalized": _normalize_text(pickup_ready_status_name),
        "pickup_ready_status_id": _as_int(
            raw.get("pickup_ready_status_id"),
            DEFAULT_PICKUP_READY_STATUS_ID,
            minimum=0,
        ),
        "pickup_ready_action_statuses": pickup_ready_action_statuses,
        "pickup_ready_action_statuses_normalized": {_normalize_text(status) for status in pickup_ready_action_statuses},
        "pickup_ship_action_statuses": pickup_ship_action_statuses,
        "pickup_ship_action_statuses_normalized": {_normalize_text(status) for status in pickup_ship_action_statuses},
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
        "product_component_expansion_rules": component_rules,
        "wholesale_detection": {
            "enabled": _as_bool(wholesale_raw.get("enabled"), True),
            "discount_threshold_pct": _as_float(
                wholesale_raw.get("discount_threshold_pct"),
                DEFAULT_WHOLESALE_DETECTION_DISCOUNT_THRESHOLD_PCT,
                minimum=0.0,
            ),
            "require_company_customer": _as_bool(
                wholesale_raw.get("require_company_customer"),
                DEFAULT_WHOLESALE_DETECTION_REQUIRE_COMPANY,
            ),
            "retail_tax_rate": _as_float(
                wholesale_raw.get("retail_tax_rate"),
                DEFAULT_WHOLESALE_RETAIL_TAX_RATE,
                minimum=0.0,
            ),
        },
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


def _is_pickup_ready_status(order: Dict[str, Any], settings: Dict[str, Any]) -> bool:
    return _normalize_text(_status_name(order)) == settings["pickup_ready_status_name_normalized"]


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


def _is_paid_personal_pickup(order: Dict[str, Any], settings: Dict[str, Any]) -> bool:
    return (
        _is_personal_pickup(order, settings)
        and _normalize_text(_status_name(order)) != settings["shipped_status_name_normalized"]
        and (_is_paid_online(order, settings) or _is_pickup_ready_status(order, settings))
    )


def _normalize_product_identifier(value: Any) -> str:
    return str(value or "").strip().upper()


def _component_rule_matches_order_item(item: Dict[str, Any], rule: Dict[str, Any]) -> bool:
    patterns = rule.get("bundle_patterns") or []
    if isinstance(patterns, list) and _matches_patterns(item.get("label"), patterns):
        return True

    item_identifiers = {
        _normalize_product_identifier(item.get("import_code")),
        _normalize_product_identifier(item.get("warehouse_number")),
        _normalize_product_identifier(item.get("ean")),
    }
    configured_identifiers: List[Any] = []
    for key in ("bundle_skus", "bundle_import_codes", "bundle_warehouse_numbers", "bundle_eans"):
        values = rule.get(key) or []
        if isinstance(values, list):
            configured_identifiers.extend(values)
    return any(_normalize_product_identifier(value) in item_identifiers for value in configured_identifiers)


def _expand_order_item_components(item: Dict[str, Any], settings: Dict[str, Any]) -> List[Dict[str, Any]]:
    for rule in settings.get("product_component_expansion_rules") or []:
        if not isinstance(rule, dict) or not _component_rule_matches_order_item(item, rule):
            continue
        components = rule.get("components") or []
        if not isinstance(components, list) or not components:
            return [item]
        expanded: List[Dict[str, Any]] = []
        parent_quantity = _to_float(item.get("quantity"))
        for component in components:
            if not isinstance(component, dict):
                continue
            component_quantity = _to_float(component.get("quantity"))
            component_label = str(component.get("item_label") or "").strip()
            if parent_quantity <= 0 or component_quantity <= 0 or not component_label:
                continue
            expanded.append(
                {
                    "label": component_label,
                    "quantity": round(parent_quantity * component_quantity, 2),
                    "ean": str(component.get("item_ean") or "").strip(),
                    "import_code": str(component.get("item_import_code") or "").strip(),
                    "warehouse_number": str(component.get("item_warehouse_number") or "").strip(),
                    "unit_price": None,
                    "unit_price_formatted": "",
                    "bundle_component": True,
                    "bundle_parent_label": item.get("label", ""),
                    "bundle_expansion_rule": str(rule.get("key") or "").strip(),
                }
            )
        return expanded or [item]
    return [item]


def _merge_order_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    for item in items:
        key = (
            _normalize_product_identifier(item.get("import_code")),
            _normalize_product_identifier(item.get("ean")),
            _normalize_product_identifier(item.get("warehouse_number")),
            _normalize_match_text(item.get("label")),
        )
        if key not in merged:
            merged[key] = dict(item)
            continue
        merged[key]["quantity"] = round(_to_float(merged[key].get("quantity")) + _to_float(item.get("quantity")), 2)
        existing_price = str(merged[key].get("unit_price_formatted") or "").strip()
        incoming_price = str(item.get("unit_price_formatted") or "").strip()
        if not existing_price and incoming_price:
            merged[key]["unit_price_formatted"] = incoming_price
            merged[key]["unit_price"] = item.get("unit_price")
        elif existing_price and incoming_price and existing_price != incoming_price:
            merged[key]["unit_price_formatted"] = ""
            merged[key]["unit_price"] = None
        if item.get("bundle_component"):
            merged[key]["bundle_component"] = True
            parent_labels = {
                label
                for label in str(merged[key].get("bundle_parent_label") or "").split(" | ")
                if label
            }
            if item.get("bundle_parent_label"):
                parent_labels.add(str(item["bundle_parent_label"]))
            if parent_labels:
                merged[key]["bundle_parent_label"] = " | ".join(sorted(parent_labels))
    return list(merged.values())


def _order_item_unit_price_info(item: Dict[str, Any]) -> Dict[str, Any]:
    price = item.get("price") if isinstance(item.get("price"), dict) else {}
    formatted = str(price.get("formatted") or "").strip()
    value = _optional_float(price.get("raw_value"))
    if value is None:
        value = _optional_float(price.get("value"))
    return {
        "unit_price": value,
        "unit_price_formatted": formatted,
    }


def _order_items(order: Dict[str, Any], settings: Dict[str, Any]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for item in order.get("items") or []:
        quantity = _to_float(item.get("quantity"))
        if quantity <= 0:
            continue
        price_info = _order_item_unit_price_info(item)
        base_item = {
            "label": str(item.get("item_label") or "").strip(),
            "quantity": quantity,
            "ean": str(item.get("ean") or "").strip(),
            "import_code": str(item.get("import_code") or "").strip(),
            "warehouse_number": str(item.get("warehouse_number") or "").strip(),
            "unit_price": price_info["unit_price"],
            "unit_price_formatted": price_info["unit_price_formatted"],
        }
        items.extend(_expand_order_item_components(base_item, settings))
    return _merge_order_items(items)


def _join_name(*values: Any) -> str:
    return " ".join(str(value or "").strip() for value in values if str(value or "").strip()).strip()


def _customer_info(order: Dict[str, Any]) -> Dict[str, Any]:
    customer = order.get("customer") if isinstance(order.get("customer"), dict) else {}
    customer_type = str(customer.get("__typename") or "").strip()
    company_name = str(customer.get("company_name") or "").strip()
    company_id = str(customer.get("company_id") or "").strip()
    person_name = _join_name(customer.get("name"), customer.get("surname"))
    display_name = company_name or person_name
    return {
        "type": customer_type,
        "company_name": company_name,
        "company_id": company_id,
        "vat_id": str(customer.get("vat_id") or customer.get("vat_id2") or "").strip(),
        "display_name": display_name,
        "phone": str(customer.get("phone") or "").strip(),
        "email": str(customer.get("email") or "").strip(),
        "is_company": bool(company_id),
    }


def _address_info(address: Any, fallback_customer: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    address = address if isinstance(address, dict) else {}
    fallback_customer = fallback_customer or {}
    company_name = str(address.get("company_name") or "").strip()
    person_name = _join_name(address.get("name"), address.get("surname"))
    display_name = company_name or person_name or str(fallback_customer.get("display_name") or "").strip()
    street_number = _join_name(address.get("descriptive_number"), address.get("orientation_number"))
    street = _join_name(address.get("street"), street_number)
    city = _join_name(address.get("zip"), address.get("city"))
    region = _join_name(city, address.get("state"))
    country = str(address.get("country") or "").strip()
    phone = str(address.get("phone") or fallback_customer.get("phone") or "").strip()
    email = str(address.get("email") or fallback_customer.get("email") or "").strip()
    lines = [line for line in (display_name, street, _join_name(region, country), phone, email) if line]
    return {
        "display_name": display_name,
        "street": street,
        "city": city,
        "state": str(address.get("state") or "").strip(),
        "country": country,
        "phone": phone,
        "email": email,
        "lines": lines,
    }


def _price_number(price: Any) -> Optional[float]:
    if not isinstance(price, dict):
        return None
    for key in ("raw_value", "value"):
        parsed = _optional_float(price.get(key))
        if parsed is not None:
            return parsed
    return None


def _item_tax_rate(item: Dict[str, Any]) -> float:
    return _to_float(item.get("tax_rate"))


def _price_as_net(price: Any, tax_rate: float) -> Optional[float]:
    value = _price_number(price)
    if value is None:
        return None
    if isinstance(price, dict) and bool(price.get("is_net_price")):
        return value
    return value / (1.0 + (tax_rate / 100.0)) if tax_rate > 0 else value


def _price_as_gross(price: Any, tax_rate: float) -> Optional[float]:
    value = _price_number(price)
    if value is None:
        return None
    if isinstance(price, dict) and bool(price.get("is_net_price")):
        return value * (1.0 + (tax_rate / 100.0)) if tax_rate > 0 else value
    return value


def _item_unit_gross_price(item: Dict[str, Any]) -> Optional[float]:
    quantity = _to_float(item.get("quantity"))
    if quantity <= 0:
        return None
    tax_rate = _item_tax_rate(item)
    gross_total = _price_number(item.get("sum_with_tax"))
    if gross_total is not None:
        return gross_total / quantity
    net_total = _price_number(item.get("sum"))
    if net_total is not None:
        return (net_total * (1.0 + (tax_rate / 100.0)) if tax_rate > 0 else net_total) / quantity
    unit_price = _price_number(item.get("price"))
    if unit_price is None:
        return None
    # BizniWeb ROY item unit prices behave as net values even when is_net_price is false.
    return unit_price * (1.0 + (tax_rate / 100.0)) if tax_rate > 0 else unit_price


def _item_unit_net_price(item: Dict[str, Any]) -> Optional[float]:
    quantity = _to_float(item.get("quantity"))
    if quantity <= 0:
        return None
    tax_rate = _item_tax_rate(item)
    net_total = _price_number(item.get("sum"))
    if net_total is not None:
        return net_total / quantity
    gross_total = _price_number(item.get("sum_with_tax"))
    if gross_total is not None:
        net_total = gross_total / (1.0 + (tax_rate / 100.0)) if tax_rate > 0 else gross_total
        return net_total / quantity
    unit_price = _price_number(item.get("price"))
    if unit_price is None:
        return None
    # BizniWeb ROY item unit prices behave as net values even when is_net_price is false.
    return unit_price


def _product_retail_gross_price(item: Dict[str, Any]) -> Optional[float]:
    product = item.get("product") if isinstance(item.get("product"), dict) else {}
    return _price_as_gross(product.get("final_price"), _item_tax_rate(item))


def _product_retail_net_price(item: Dict[str, Any], retail_tax_rate: float) -> Optional[float]:
    product = item.get("product") if isinstance(item.get("product"), dict) else {}
    final_price = product.get("final_price")
    value = _price_number(final_price)
    if value is None:
        return None
    if isinstance(final_price, dict) and bool(final_price.get("is_net_price")):
        return value
    tax_rate = _item_tax_rate(item) or retail_tax_rate
    return value / (1.0 + (tax_rate / 100.0)) if tax_rate > 0 else value


def _normalize_discount_signal(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or "").casefold())
    return "".join(ch for ch in text if not unicodedata.combining(ch)).strip()


def _has_discount_code_price_element(order: Dict[str, Any]) -> bool:
    discount_type_patterns = ("discount", "coupon", "voucher", "gift")
    discount_text_patterns = ("zlav", "zlava", "kod", "kupon", "coupon", "voucher")
    for element in order.get("price_elements") or []:
        if not isinstance(element, dict):
            continue
        element_type = _normalize_discount_signal(element.get("type"))
        title = _normalize_discount_signal(element.get("title"))
        value = _normalize_discount_signal(element.get("value"))
        if any(pattern in element_type for pattern in discount_type_patterns):
            return True
        if any(pattern in title or pattern in value for pattern in discount_text_patterns):
            return True
    return False


def _detect_wholesale_pricing(order: Dict[str, Any], customer: Dict[str, Any], settings: Dict[str, Any]) -> Dict[str, Any]:
    detection = settings.get("wholesale_detection") if isinstance(settings.get("wholesale_detection"), dict) else {}
    if not detection.get("enabled", True):
        return {"is_wholesale": False, "reason": "disabled"}

    require_company = bool(detection.get("require_company_customer", DEFAULT_WHOLESALE_DETECTION_REQUIRE_COMPANY))
    threshold_pct = _to_float(detection.get("discount_threshold_pct"))
    if threshold_pct <= 0:
        threshold_pct = DEFAULT_WHOLESALE_DETECTION_DISCOUNT_THRESHOLD_PCT
    threshold_ratio = max(0.0, 1.0 - (threshold_pct / 100.0))
    retail_tax_rate = _to_float(detection.get("retail_tax_rate"))

    priced_lines = 0
    discounted_lines = 0
    max_discount_pct = 0.0
    discount_code_used = _has_discount_code_price_element(order)
    examples: List[Dict[str, Any]] = []
    for item in order.get("items") or []:
        if not isinstance(item, dict):
            continue
        item_price = _item_unit_net_price(item)
        retail_price = _product_retail_net_price(item, retail_tax_rate)
        if item_price is None or retail_price is None or item_price <= 0 or retail_price <= 0:
            continue
        priced_lines += 1
        ratio = item_price / retail_price
        if ratio <= threshold_ratio:
            discounted_lines += 1
            discount_pct = (1.0 - ratio) * 100.0
            max_discount_pct = max(max_discount_pct, discount_pct)
            if len(examples) < 3:
                examples.append(
                    {
                        "import_code": str(item.get("import_code") or "").strip(),
                        "label": str(item.get("item_label") or "").strip(),
                        "order_unit_net": round(item_price, 2),
                        "retail_unit_net": round(retail_price, 2),
                        "comparison_basis": "net",
                        "discount_pct": round(discount_pct, 1),
                    }
                )

    is_company = bool(customer.get("is_company"))
    is_wholesale = discounted_lines > 0 and not discount_code_used and (is_company or not require_company)
    reason = ""
    if is_wholesale:
        if is_company:
            reason = f"company customer + {discounted_lines}/{priced_lines} discounted line(s) vs current retail final price"
        else:
            reason = f"{discounted_lines}/{priced_lines} discounted line(s) vs current retail final price"
    elif discount_code_used:
        reason = "discount code used"
    elif discounted_lines > 0:
        reason = f"{discounted_lines}/{priced_lines} discounted line(s), but customer is not Company"
    elif is_company:
        reason = "company customer, no wholesale price discount detected"
    else:
        reason = "no wholesale price signal"

    return {
        "is_wholesale": bool(is_wholesale),
        "customer_is_company": is_company,
        "discounted_lines": discounted_lines,
        "priced_lines": priced_lines,
        "discount_code_used": discount_code_used,
        "max_discount_pct": round(max_discount_pct, 1),
        "threshold_pct": round(threshold_pct, 1),
        "comparison_basis": "net",
        "reason": reason,
        "examples": examples,
    }


def _public_order_row(order: Dict[str, Any], settings: Dict[str, Any]) -> Dict[str, Any]:
    payment = _price_element_info(order, "payment")
    shipping = _price_element_info(order, "shipping")
    customer = _customer_info(order)
    wholesale_pricing = _detect_wholesale_pricing(order, customer, settings)
    fulfillable, reason = _is_fulfillable_order(order, settings)
    is_pickup = _is_personal_pickup(order, settings)
    status_name = _status_name(order)
    status_norm = _normalize_text(status_name)
    pickup_ready = status_norm == settings["pickup_ready_status_name_normalized"]
    paid_pickup_ready = _is_paid_personal_pickup(order, settings)
    pickup_ready_action_allowed = (
        paid_pickup_ready
        and not pickup_ready
        and status_norm in settings["pickup_ready_action_statuses_normalized"]
    )
    pickup_ship_action_allowed = (
        paid_pickup_ready
        and status_norm in settings["pickup_ship_action_statuses_normalized"]
    )
    return {
        "id": order.get("id"),
        "order_num": order.get("order_num"),
        "purchase_at": order.get("pur_date"),
        "last_change": order.get("last_change"),
        "source": str(order.get("source") or "").strip(),
        "customer": customer,
        "invoice_address": _address_info(order.get("invoice_address"), customer),
        "delivery_address": _address_info(order.get("delivery_address"), customer),
        "customer_note": str(order.get("note") or "").strip(),
        "internal_note": str(order.get("internal_note") or "").strip(),
        "wholesale_pricing": wholesale_pricing,
        "status": status_name,
        "status_id": _status_id(order),
        "sum": (order.get("sum") or {}).get("formatted"),
        "sum_value": _to_float((order.get("sum") or {}).get("value")),
        "payment": payment,
        "shipping": shipping,
        "items": _order_items(order, settings),
        "fulfillable": fulfillable,
        "fulfillment_reason": reason,
        "personal_pickup": is_pickup,
        "paid_personal_pickup": paid_pickup_ready,
        "pickup_ready": pickup_ready,
        "pickup_ready_action_allowed": pickup_ready_action_allowed,
        "pickup_ship_action_allowed": pickup_ship_action_allowed,
        "pickup_action_allowed": pickup_ship_action_allowed,
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
    pickup_orders = [row for row in rows if row["paid_personal_pickup"]]
    paid_orders = [row for row in fulfillable_orders if row["fulfillment_reason"] == "paid_online"]
    cod_orders = [row for row in fulfillable_orders if row["fulfillment_reason"] == "cod_waiting"]

    status_counts: Dict[str, int] = defaultdict(int)
    for row in fulfillable_orders:
        status_counts[str(row.get("status") or "")] += 1

    generated = generated_at or datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    fulfillable_orders.sort(key=lambda row: (str(row.get("purchase_at") or ""), str(row.get("order_num") or "")))
    pickup_orders.sort(key=lambda row: (str(row.get("purchase_at") or ""), str(row.get("order_num") or "")))
    pickup_ready_actions_available = len([row for row in pickup_orders if row["pickup_ready_action_allowed"]])
    pickup_ship_actions_available = len([row for row in pickup_orders if row["pickup_ship_action_allowed"]])

    return {
        "project": project,
        "generated_at": generated,
        "auto_refresh_seconds": settings["auto_refresh_seconds"],
        "summary": {
            "fulfillable_orders": len(fulfillable_orders),
            "paid_online_orders": len(paid_orders),
            "cod_waiting_orders": len(cod_orders),
            "personal_pickups": len(pickup_orders),
            "pickup_actions_available": pickup_ready_actions_available + pickup_ship_actions_available,
            "pickup_ready_actions_available": pickup_ready_actions_available,
            "pickup_ship_actions_available": pickup_ship_actions_available,
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
    max_page_retries = 3
    retry_delay_seconds = 2.0

    while has_next_page and page_count < settings["scan_max_pages"]:
        params: Dict[str, Any] = {
            "limit": 30,
            "order_by": "pur_date",
            "sort": "DESC",
        }
        if cursor is not None:
            params["cursor"] = cursor

        last_error: Optional[Exception] = None
        for attempt in range(max_page_retries):
            try:
                result = client.execute(ROY_OPERATIONS_ORDER_QUERY, variable_values={"params": params})
                break
            except Exception as exc:
                last_error = exc
                if attempt + 1 >= max_page_retries:
                    raise
                time.sleep(retry_delay_seconds * (attempt + 1))
        else:
            raise RuntimeError(f"Failed to fetch ROY operations orders page: {last_error}")
        payload = result.get("getOrderList") or {}
        page_orders = [order for order in (payload.get("data") or []) if order]
        orders.extend(page_orders)
        page_count += 1

        page_fulfillable = sum(1 for order in page_orders if _is_fulfillable_order(order, settings)[0])
        page_pickups = sum(1 for order in page_orders if _is_paid_personal_pickup(order, settings))
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
    product_revenue_by_sku = {
        str(row.get("sku") or "").strip(): row
        for row in product_revenue_rows
        if isinstance(row, dict) and str(row.get("sku") or "").strip()
    }
    if product_revenue_by_sku:
        enriched_product_profit_rows = []
        for raw_row in product_profit_rows:
            if not isinstance(raw_row, dict):
                enriched_product_profit_rows.append(raw_row)
                continue
            row = dict(raw_row)
            source_row = product_revenue_by_sku.get(str(row.get("sku") or "").strip())
            if isinstance(source_row, dict):
                for key in ("gross_profit", "cm1_profit", "gross_margin_pct"):
                    if row.get(key) in (None, "") and source_row.get(key) not in (None, ""):
                        row[key] = source_row.get(key)
            enriched_product_profit_rows.append(row)
        product_profit_rows = enriched_product_profit_rows

    def row_gross_profit(row: Dict[str, Any]) -> float:
        for key in ("gross_profit", "cm1_profit", "profit_without_fixed", "profit_with_fixed"):
            value = _optional_float(row.get(key))
            if value is not None:
                return value
        return 0.0

    def sort_by_revenue_then_gross(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return sorted(
            [row for row in rows if isinstance(row, dict)],
            key=lambda row: (_to_float(row.get("revenue")), row_gross_profit(row)),
            reverse=True,
        )

    def sort_by_gross_then_revenue(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return sorted(
            [row for row in rows if isinstance(row, dict)],
            key=lambda row: (row_gross_profit(row), _to_float(row.get("revenue"))),
            reverse=True,
        )

    product_revenue_rows = sort_by_revenue_then_gross(product_revenue_rows)
    product_profit_rows = sort_by_gross_then_revenue(product_profit_rows)
    brand_revenue_rows = sort_by_revenue_then_gross(list(roy_demand.get("brand_revenue_rows") or []))
    brand_profit_rows = sort_by_gross_then_revenue(list(roy_demand.get("brand_profit_rows") or []))
    country_rows = list(roy_demand.get("country_rows") or [])
    geo_rows = list(dashboard.get("geo_rows") or [])
    geo_by_country = {
        str(row.get("country") or "").strip().lower(): row
        for row in geo_rows
        if isinstance(row, dict) and str(row.get("country") or "").strip()
    }
    if country_rows and geo_by_country:
        merged_country_rows: List[Dict[str, Any]] = []
        for row in country_rows:
            merged = dict(row)
            geo = geo_by_country.get(str(row.get("country") or "").strip().lower())
            if geo:
                merged["fb_ads_spend"] = geo.get("fb_ads_spend", merged.get("fb_ads_spend"))
                merged["google_ads_spend"] = geo.get("google_ads_spend", merged.get("google_ads_spend"))
                merged["paid_ads_spend"] = geo.get("paid_ads_spend", merged.get("paid_ads_spend"))
                merged["spend"] = geo.get("paid_ads_spend", merged.get("spend"))
                merged["gross_profit"] = geo.get("gross_profit", merged.get("gross_profit"))
                merged["profit_without_fixed"] = geo.get(
                    "contribution_profit_without_fixed",
                    merged.get("profit_without_fixed"),
                )
                merged["profit_with_fixed"] = geo.get(
                    "contribution_profit_with_fixed",
                    merged.get("profit_with_fixed"),
                )
                merged["net_margin_pct"] = geo.get(
                    "contribution_margin_with_fixed_pct",
                    merged.get("net_margin_pct"),
                )
            merged_country_rows.append(merged)
        country_rows = merged_country_rows
    if not country_rows:
        country_rows = geo_rows

    loss_rows = []
    hidden_loss_count = 0
    for raw_row in list(roy_demand.get("loss_product_rows") or []):
        if not isinstance(raw_row, dict):
            continue
        row = dict(raw_row)
        gross_profit = _optional_float(row.get("gross_profit"))
        if gross_profit is None:
            gross_profit = _optional_float(row.get("cm1_profit"))
        if gross_profit is None or gross_profit >= 0:
            continue
        row["gross_profit"] = gross_profit
        if _optional_float(row.get("gross_margin_pct")) is None:
            revenue = _to_float(row.get("revenue"))
            row["gross_margin_pct"] = round((gross_profit / revenue * 100.0) if revenue > 0 else 0.0, 1)
        sku = str(row.get("sku") or "").strip()
        if sku and sku in acknowledged:
            hidden_loss_count += 1
            continue
        loss_rows.append(row)

    return {
        "brand_revenue_rows": brand_revenue_rows[:3],
        "brand_profit_rows": brand_profit_rows[:3],
        "product_revenue_rows": product_revenue_rows[:10],
        "product_profit_rows": product_profit_rows[:10],
        "country_rows": country_rows[:12],
        "loss_product_rows": loss_rows[:80],
        "acknowledged_loss_product_count": hidden_loss_count,
    }


def _select_roy_operations_inventory_payload(dashboard: Dict[str, Any]) -> Dict[str, Any]:
    operations_inventory = dashboard.get("roy_operations_inventory")
    if isinstance(operations_inventory, dict):
        return operations_inventory
    roy_inventory = dashboard.get("roy_product_demand")
    return roy_inventory if isinstance(roy_inventory, dict) else {}


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


def _stock_risk_sets() -> Tuple[set[str], set[str]]:
    risk_30d = {"Negative stock", "Out of stock", "Critical", "Low"}
    risk_45d = {"Negative stock", "Out of stock", "Critical", "Low", "Watch"}
    return risk_30d, risk_45d


def _stock_model_thresholds(project_settings: Dict[str, Any]) -> Dict[str, int]:
    model = project_settings.get("inventory_model") or {}
    critical_days = max(1, int(model.get("critical_days_of_cover", 14) or 14))
    warning_days = max(critical_days, int(model.get("warning_days_of_cover", 30) or 30))
    watch_days = max(warning_days, int(model.get("watch_days_of_cover", 45) or 45))
    reorder_cover_days = max(1, int(model.get("reorder_cover_days", warning_days) or warning_days))
    hero_cover_days = max(reorder_cover_days, int(model.get("hero_reorder_cover_days", watch_days) or watch_days))
    return {
        "critical_days": critical_days,
        "warning_days": warning_days,
        "watch_days": watch_days,
        "reorder_cover_days": reorder_cover_days,
        "hero_cover_days": hero_cover_days,
    }


def _clean_stock_search_term(value: Any) -> str:
    normalized = _normalize_match_text(value)
    return normalized if len(normalized) >= 3 else ""


def _normalize_identifier(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if re.fullmatch(r"\d+\.0+", text):
        text = text.split(".", 1)[0]
    return text.upper()


def _stock_lookup_targets(inventory: Dict[str, Any], state: Optional[Dict[str, Any]] = None) -> List[Dict[str, str]]:
    severe_risk_levels = {"Negative stock", "Out of stock", "Critical"}
    target_by_sku: Dict[str, Dict[str, str]] = {}
    source_keys = ("alert_rows", "restock_priority_rows", "revenue_at_risk_rows", "stock_risk_rows")

    for key, rows in _inventory_row_collections(inventory):
        if key not in source_keys:
            continue
        for row in rows:
            sku = str(row.get("sku") or "").strip()
            product = str(row.get("product") or "").strip()
            if not sku or not product:
                continue
            risk = str(row.get("stock_risk_level") or "")
            if risk not in severe_risk_levels and _to_float(row.get("available_quantity")) > 0:
                continue
            target = target_by_sku.setdefault(sku, {"sku": sku, "product": product, "sources": ""})
            if key not in target["sources"].split(","):
                target["sources"] = ",".join(part for part in (target["sources"], key) if part)

    inbound = (state or {}).get("inbound_orders") if isinstance((state or {}).get("inbound_orders"), dict) else {}
    if inbound:
        for sku, record in inbound.items():
            normalized_sku = str(record.get("sku") or sku or "").strip()
            if not normalized_sku:
                continue
            product = str(record.get("product") or "").strip()
            target_by_sku.setdefault(normalized_sku, {"sku": normalized_sku, "product": product, "sources": "inbound_orders"})

    return list(target_by_sku.values())


def _stock_search_terms_for_target(target: Dict[str, str]) -> List[str]:
    terms: List[str] = []
    product = str(target.get("product") or "").strip()
    cleaned_product = _clean_stock_search_term(product)
    if cleaned_product:
        terms.append(cleaned_product)
    elif product:
        terms.append(product)
    sku = _normalize_identifier(target.get("sku"))
    if sku and not sku.startswith("H-"):
        terms.append(sku)

    deduped: List[str] = []
    seen: set[str] = set()
    for term in terms:
        cleaned = str(term or "").strip()
        if not cleaned:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(cleaned[:120])
    return deduped[:3]


def _execute_product_stock_searches(
    client: Client,
    *,
    lang_code: str,
    search_terms: List[str],
    page_limit: int = 8,
    batch_size: int = 5,
) -> Tuple[Dict[str, List[Dict[str, Any]]], List[str]]:
    results: Dict[str, List[Dict[str, Any]]] = {}
    errors: List[str] = []
    terms = [term for term in search_terms if str(term or "").strip()]

    def execute_batch(batch: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        variable_defs = ["$lang_code: CountryCodeAlpha2!"]
        field_blocks: List[str] = []
        variables: Dict[str, Any] = {"lang_code": lang_code}
        for index, term in enumerate(batch):
            param_name = f"params{index}"
            alias = f"p{index}"
            variable_defs.append(f"${param_name}: ProductParams")
            variables[param_name] = {"limit": max(1, min(int(page_limit), 30)), "search": term}
            field_blocks.append(
                f"""
  {alias}: getProductList(lang_code: $lang_code, params: ${param_name}) {{
{ROY_PRODUCT_STOCK_SEARCH_FIELDS}
  }}
"""
            )
        query = gql(
            "query SearchRoyProductStock("
            + ", ".join(variable_defs)
            + ") {\n"
            + "\n".join(field_blocks)
            + "\n}"
        )
        payload = client.execute(query, variable_values=variables)
        batch_results: Dict[str, List[Dict[str, Any]]] = {}
        for index, term in enumerate(batch):
            block = (payload or {}).get(f"p{index}") or {}
            batch_results[term] = [row for row in (block.get("data") or []) if isinstance(row, dict)]
        return batch_results

    def execute_batch_with_retry(batch: List[str], attempts: int = 2) -> Dict[str, List[Dict[str, Any]]]:
        last_error: Optional[Exception] = None
        for attempt in range(max(1, attempts)):
            try:
                return execute_batch(batch)
            except Exception as exc:
                last_error = exc
                if attempt + 1 < max(1, attempts):
                    time.sleep(0.4 * (attempt + 1))
        if last_error is not None:
            raise last_error
        return {}

    for start in range(0, len(terms), max(1, batch_size)):
        batch = terms[start : start + max(1, batch_size)]
        try:
            results.update(execute_batch_with_retry(batch))
        except Exception as exc:
            if len(batch) <= 1:
                errors.append(str(exc)[:240])
                continue
            for term in batch:
                try:
                    results.update(execute_batch_with_retry([term]))
                except Exception as single_exc:
                    errors.append(str(single_exc)[:240])
    return results, errors


def _warehouse_stock_totals(product: Dict[str, Any]) -> Dict[str, Any]:
    quantity_raw = 0.0
    available_raw = 0.0
    warehouses: List[Dict[str, Any]] = []
    for warehouse_item in product.get("warehouse_items") or []:
        raw_quantity = _to_float((warehouse_item or {}).get("quantity"))
        raw_available = _to_float(
            (warehouse_item or {}).get("available_quantity")
            if (warehouse_item or {}).get("available_quantity") is not None
            else raw_quantity
        )
        quantity_raw += raw_quantity
        available_raw += raw_available
        warehouses.append(
            {
                "warehouse_number": str((warehouse_item or {}).get("warehouse_number") or "").strip(),
                "quantity": raw_quantity,
                "available_quantity": raw_available,
                "status": str(((warehouse_item or {}).get("status") or {}).get("name") or "").strip(),
            }
        )
    return {
        "quantity_raw": quantity_raw,
        "available_quantity_raw": available_raw,
        "quantity": max(quantity_raw, 0.0),
        "available_quantity": max(available_raw, 0.0),
        "warehouses": warehouses,
    }


def _score_stock_candidate(target: Dict[str, str], product: Dict[str, Any]) -> int:
    target_sku = _normalize_identifier(target.get("sku"))
    target_title = _normalize_match_text(target.get("product"))
    product_title = _normalize_match_text(product.get("title"))
    import_code = _normalize_identifier(product.get("import_code"))
    ean = _normalize_identifier(product.get("ean"))
    score = 0
    title_matches = False
    if target_title and product_title:
        if target_title == product_title:
            score += 100
            title_matches = True
        elif target_title in product_title or product_title in target_title:
            score += 80
            title_matches = True
    if target_sku and import_code and target_sku == import_code:
        score += 65
    if target_sku and ean and target_sku == ean:
        score += 45 if title_matches else 20
    return score


def _select_current_stock_for_target(
    target: Dict[str, str],
    search_results: Dict[str, List[Dict[str, Any]]],
) -> Optional[Dict[str, Any]]:
    best: Optional[Tuple[int, Dict[str, Any]]] = None
    for term in _stock_search_terms_for_target(target):
        for product in search_results.get(term) or []:
            score = _score_stock_candidate(target, product)
            if score < 60:
                continue
            if best is None or score > best[0]:
                best = (score, product)
    if best is None:
        return None
    product = best[1]
    stock = _warehouse_stock_totals(product)
    return {
        "sku": str(target.get("sku") or "").strip(),
        "product": str(target.get("product") or "").strip(),
        "matched_product_id": str(product.get("id") or "").strip(),
        "matched_product_title": str(product.get("title") or "").strip(),
        "matched_import_code": str(product.get("import_code") or "").strip(),
        "matched_ean": str(product.get("ean") or "").strip(),
        "active": bool(product.get("active", False)),
        **stock,
    }


def fetch_current_stock_for_inventory_alerts(
    project: str,
    project_settings: Dict[str, Any],
    inventory: Dict[str, Any],
    state: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    model = project_settings.get("inventory_model") or {}
    lang_code = str(model.get("lang_code", "SK") or "SK").strip().upper() or "SK"
    targets = _stock_lookup_targets(inventory, state=state)
    diagnostics: Dict[str, Any] = {
        "enabled": True,
        "source": "biznisweb_product_search",
        "target_count": len(targets),
        "matched_count": 0,
        "error_count": 0,
        "checked_at": _state_now_iso(),
    }
    if not targets:
        return {}, diagnostics

    client = _build_client(project, project_settings)
    terms: List[str] = []
    seen_terms: set[str] = set()
    for target in targets:
        target_terms = _stock_search_terms_for_target(target)
        for term in target_terms:
            key = term.lower()
            if key in seen_terms:
                continue
            seen_terms.add(key)
            terms.append(term)

    search_results, errors = _execute_product_stock_searches(client, lang_code=lang_code, search_terms=terms)
    current_stock: Dict[str, Dict[str, Any]] = {}
    for target in targets:
        stock = _select_current_stock_for_target(target, search_results)
        if not stock:
            continue
        current_stock[str(target.get("sku") or "").strip()] = stock

    diagnostics.update(
        {
            "search_term_count": len(terms),
            "matched_count": len(current_stock),
            "unmatched_count": max(len(targets) - len(current_stock), 0),
            "error_count": len(errors),
            "errors": errors[:3],
        }
    )
    return current_stock, diagnostics


def _recalculate_inventory_row_after_stock_update(row: Dict[str, Any], project_settings: Dict[str, Any]) -> None:
    thresholds = _stock_model_thresholds(project_settings)
    available = _to_float(row.get("available_quantity"))
    available_raw = _to_float(row.get("available_quantity_raw", available))
    alert_30d_units = _to_float(row.get("alert_30d_units"))
    if alert_30d_units <= 0:
        alert_30d_units = _to_float(row.get("recent_30d_units")) or _to_float(row.get("forecast_30d_units"))
    daily_units = alert_30d_units / 30.0 if alert_30d_units > 0 else 0.0
    days_of_cover: Optional[float] = None
    current_risk = str(row.get("stock_risk_level") or "")
    if daily_units > 0:
        days_of_cover = available / daily_units
        if available_raw < 0:
            current_risk = "Negative stock"
        elif available <= 0:
            current_risk = "Out of stock"
        elif days_of_cover <= thresholds["critical_days"]:
            current_risk = "Critical"
        elif days_of_cover <= thresholds["warning_days"]:
            current_risk = "Low"
        elif days_of_cover <= thresholds["watch_days"]:
            current_risk = "Watch"
        else:
            current_risk = "Healthy"
    elif available > 0:
        current_risk = "Healthy"

    lead_time_working = int(round(_to_float(row.get("lead_time_working_days"))))
    lead_time_calendar = int(math.ceil(max(0, lead_time_working) * (7.0 / 5.0)))
    target_cover = lead_time_calendar + (
        thresholds["hero_cover_days"] if bool(row.get("strategic_stock_flag")) else thresholds["reorder_cover_days"]
    )
    if daily_units > 0:
        suggested = max(math.ceil((daily_units * target_cover) - available), 0.0)
    else:
        suggested = 0.0 if available > 0 else _to_float(row.get("suggested_reorder_units"))

    projected_stockout_date = row.get("projected_stockout_date")
    if days_of_cover is not None and days_of_cover > 0 and days_of_cover < 3650:
        projected_stockout_date = (datetime.now(timezone.utc).date() + timedelta(days=int(math.ceil(days_of_cover)))).isoformat()
    elif available <= 0:
        projected_stockout_date = None

    row.update(
        {
            "stock_risk_level": current_risk,
            "days_of_cover": round(days_of_cover, 1) if days_of_cover is not None else row.get("days_of_cover"),
            "projected_stockout_date": projected_stockout_date,
            "suggested_reorder_units": round(suggested, 1),
            "reorder_now_flag": current_risk in {"Negative stock", "Out of stock", "Critical"},
            "prepare_po_flag": current_risk == "Watch",
        }
    )
    if current_risk in {"Negative stock", "Out of stock", "Critical"}:
        row["reorder_action_label"] = "Order now"
    elif current_risk == "Low":
        row["reorder_action_label"] = "30d alert"
    elif current_risk == "Watch":
        row["reorder_action_label"] = "Prepare PO"
    elif str(row.get("reorder_action_label") or "") in {"Order now", "30d alert", "Prepare PO"}:
        row["reorder_action_label"] = "OK"


def _apply_current_stock_to_inventory(
    inventory: Dict[str, Any],
    current_stock_by_sku: Dict[str, Dict[str, Any]],
    project_settings: Dict[str, Any],
) -> None:
    if not current_stock_by_sku:
        return
    checked_at = _state_now_iso()
    touched = 0
    for _, rows in _inventory_row_collections(inventory):
        for row in rows:
            sku = str(row.get("sku") or "").strip()
            stock = current_stock_by_sku.get(sku)
            if not stock:
                continue
            touched += 1
            row.update(
                {
                    "available_quantity": stock.get("available_quantity", row.get("available_quantity")),
                    "available_quantity_raw": stock.get("available_quantity_raw", row.get("available_quantity_raw")),
                    "quantity": stock.get("quantity", row.get("quantity")),
                    "quantity_raw": stock.get("quantity_raw", row.get("quantity_raw")),
                    "active": stock.get("active", row.get("active")),
                    "live_stock_checked_at": checked_at,
                    "live_stock_source": "biznisweb_product_search",
                    "live_stock_product_id": stock.get("matched_product_id"),
                    "live_stock_product_title": stock.get("matched_product_title"),
                    "live_stock_import_code": stock.get("matched_import_code"),
                    "live_stock_ean": stock.get("matched_ean"),
                }
            )
            _recalculate_inventory_row_after_stock_update(row, project_settings)
    summary = inventory.setdefault("summary", {})
    summary["live_stock_overlay_touched_rows"] = touched
    summary["live_stock_overlay_matched_products"] = len(current_stock_by_sku)
    summary["live_stock_overlay_checked_at"] = checked_at


def _finalize_inventory_alert_rows(inventory: Dict[str, Any]) -> None:
    risk_30d, risk_45d = _stock_risk_sets()

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
    alert_rows = inventory.get("alert_rows", [])
    restock_rows = inventory.get("restock_priority_rows", [])
    revenue_risk_rows = inventory.get("revenue_at_risk_rows", [])
    stock_risk_rows = inventory.get("stock_risk_rows", [])
    summary.update(
        {
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
        inventory["inbound_order_rows"] = []
        _finalize_inventory_alert_rows(inventory)
        return

    thresholds = _stock_model_thresholds(project_settings)
    critical_days = thresholds["critical_days"]
    warning_days = thresholds["warning_days"]
    watch_days = thresholds["watch_days"]
    reorder_cover_days = thresholds["reorder_cover_days"]
    hero_cover_days = thresholds["hero_cover_days"]

    today = datetime.now(timezone.utc).date()
    risk_30d, risk_45d = _stock_risk_sets()

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
    current_stock_by_sku: Optional[Dict[str, Dict[str, Any]]] = None,
    live_stock_diagnostics: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], bool]:
    dashboard = report_payload.get("dashboard") if isinstance(report_payload.get("dashboard"), dict) else {}
    roy_inventory = _select_roy_operations_inventory_payload(dashboard)
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
    if live_stock_diagnostics:
        inventory.setdefault("summary", {})["live_stock_overlay"] = live_stock_diagnostics
    if project_settings is not None and current_stock_by_sku:
        _apply_current_stock_to_inventory(inventory, current_stock_by_sku, project_settings)
    state_changed = False
    if state is not None:
        state_changed = _auto_clear_restocked_inbound_orders(state, inventory)
        if project_settings is not None:
            _apply_inbound_to_inventory(inventory, state, project_settings)
    elif project_settings is not None:
        _finalize_inventory_alert_rows(inventory)
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
    base_inventory, _ = build_inventory_snapshot(payload, project_settings=project_settings)
    try:
        current_stock_by_sku, live_stock_diagnostics = fetch_current_stock_for_inventory_alerts(
            project,
            project_settings,
            base_inventory,
            state=operations_state,
        )
    except Exception as exc:
        current_stock_by_sku = {}
        live_stock_diagnostics = {
            "enabled": True,
            "source": "biznisweb_product_search",
            "target_count": 0,
            "matched_count": 0,
            "error_count": 1,
            "errors": [str(exc)[:240]],
            "checked_at": _state_now_iso(),
        }
    inventory_snapshot, state_changed = build_inventory_snapshot(
        payload,
        state=operations_state,
        project_settings=project_settings,
        current_stock_by_sku=current_stock_by_sku,
        live_stock_diagnostics=live_stock_diagnostics,
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
            "printed_picking_order_count": len(operations_state.get("printed_picking_orders") or {}),
            "last_picking_print_batch": (operations_state.get("picking_print_batches") or [None])[-1],
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

    generate_attempts = 3 if cached is None else 1
    last_error: Optional[Exception] = None
    payload = None
    for attempt in range(generate_attempts):
        try:
            payload = generate_roy_operations_snapshot(project, report_payload=report_payload)
            break
        except Exception as exc:
            last_error = exc
            if attempt + 1 < generate_attempts:
                time.sleep(2.0 * (attempt + 1))
    if payload is None:
        if cached:
            cached_at, cached_payload = cached
            result = copy.deepcopy(cached_payload)
            result["cache"] = {
                "status": "stale_after_error",
                "age_seconds": round(now - cached_at, 1),
                "ttl_seconds": settings["cache_ttl_seconds"],
                "error": str(last_error),
            }
            return result
        raise last_error or RuntimeError("Failed to generate ROY operations snapshot")

    _CACHE[cache_key] = (now, copy.deepcopy(payload))
    payload["cache"] = {
        "status": "refreshed",
        "age_seconds": 0,
        "ttl_seconds": settings["cache_ttl_seconds"],
    }
    return payload


def _resolve_order_status_id(
    client: Client,
    *,
    configured_id: Any,
    target_status_name: str,
    target_status_name_normalized: str,
) -> int:
    configured = int(configured_id or 0)
    if configured > 0:
        return configured
    result = client.execute(LIST_ORDER_STATUSES_QUERY, variable_values={"lang_code": "SK"})
    for row in result.get("listOrderStatuses") or []:
        if _normalize_text(row.get("name")) == target_status_name_normalized:
            return int(row["id"])
    raise RuntimeError(f"Target status '{target_status_name}' not found in BiznisWeb.")


def _resolve_pickup_ready_status_id(client: Client, settings: Dict[str, Any]) -> int:
    return _resolve_order_status_id(
        client,
        configured_id=settings.get("pickup_ready_status_id"),
        target_status_name=settings["pickup_ready_status_name"],
        target_status_name_normalized=settings["pickup_ready_status_name_normalized"],
    )


def _resolve_shipped_status_id(client: Client, settings: Dict[str, Any]) -> int:
    return _resolve_order_status_id(
        client,
        configured_id=settings.get("shipped_status_id"),
        target_status_name=settings["shipped_status_name"],
        target_status_name_normalized=settings["shipped_status_name_normalized"],
    )


def _mark_personal_pickup_status(project: str, order_num: str, action: str) -> Dict[str, Any]:
    project = (project or BASE_DEFAULT_PROJECT).strip() or BASE_DEFAULT_PROJECT
    if project != "roy":
        raise ValueError("Pickup status action is only enabled for project 'roy'.")

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
    if action == "ready":
        if row["pickup_ready"]:
            raise ValueError(f"Order '{order_num}' is already in target status.")
        if not row["pickup_ready_action_allowed"]:
            raise ValueError(f"Order '{order_num}' is not in an allowed pickup ready status.")
        status_id = _resolve_pickup_ready_status_id(client, settings)
        target_status_name = settings["pickup_ready_status_name"]
    elif action == "ship":
        if _normalize_text(row["status"]) == settings["shipped_status_name_normalized"]:
            raise ValueError(f"Order '{order_num}' is already in target status.")
        if not row["pickup_ship_action_allowed"]:
            raise ValueError(f"Order '{order_num}' is not in an allowed pickup ship status.")
        status_id = _resolve_shipped_status_id(client, settings)
        target_status_name = settings["shipped_status_name"]
    else:
        raise ValueError(f"Unknown pickup action '{action}'.")

    mutation_result = client.execute(
        CHANGE_ORDER_STATUS_MUTATION,
        variable_values={"order_num": order_num, "status_id": status_id},
    )
    _CACHE.pop(project, None)
    return {
        "ok": True,
        "project": project,
        "order_num": order_num,
        "action": action,
        "target_status_id": status_id,
        "target_status_name": target_status_name,
        "result": mutation_result.get("changeOrderStatus"),
    }


def mark_personal_pickup_ready(project: str, order_num: str) -> Dict[str, Any]:
    return _mark_personal_pickup_status(project, order_num, "ready")


def mark_personal_pickup_shipped(project: str, order_num: str) -> Dict[str, Any]:
    return _mark_personal_pickup_status(project, order_num, "ship")
