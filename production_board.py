#!/usr/bin/env python3
"""Live production demand board for open BizniWeb orders."""

from __future__ import annotations

import copy
import os
import time
import unicodedata
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport

from reporting_core import (
    BASE_DEFAULT_PROJECT,
    load_project_env,
    load_project_settings,
    resolve_biznisweb_api_url,
)


DEFAULT_ACTIVE_ORDER_STATUSES = ("Čaká na vybavenie", "Platba online - zaplatené")
DEFAULT_MANUFACTURED_TERMS = ("vevo",)
DEFAULT_SCAN_MAX_PAGES = 30
DEFAULT_SCAN_MIN_PAGES = 10
DEFAULT_STOP_AFTER_EMPTY_ACTIVE_PAGES = 2
DEFAULT_CACHE_TTL_SECONDS = 60
DEFAULT_AUTO_REFRESH_SECONDS = 90


ORDER_QUERY = gql(
    """
query GetProductionOrders($params: OrderParams) {
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
      items {
        item_label
        ean
        import_code
        warehouse_number
        quantity
      }
      sum {
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


_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}


def _normalize_text(value: Any) -> str:
    raw = str(value or "").strip().lower()
    decomposed = unicodedata.normalize("NFKD", raw)
    without_marks = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return " ".join(without_marks.split())


def _as_list(value: Any, default: Iterable[str]) -> List[str]:
    if value is None:
        return [str(item) for item in default]
    if isinstance(value, str):
        return [value]
    return [str(item) for item in value if str(item or "").strip()]


def _as_int(value: Any, default: int, *, minimum: int = 1) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, parsed)


def resolve_production_board_settings(project_settings: Dict[str, Any]) -> Dict[str, Any]:
    raw = project_settings.get("production_board") or {}
    active_statuses = _as_list(raw.get("active_order_statuses"), DEFAULT_ACTIVE_ORDER_STATUSES)
    manufactured_terms = _as_list(raw.get("manufactured_product_terms"), DEFAULT_MANUFACTURED_TERMS)
    excluded_labels = _as_list(raw.get("excluded_product_labels"), ())
    excluded_patterns = _as_list(raw.get("excluded_product_label_patterns"), ())

    return {
        "enabled": bool(raw.get("enabled", False)),
        "active_order_statuses": active_statuses,
        "active_order_statuses_normalized": {_normalize_text(status) for status in active_statuses},
        "manufactured_product_terms": manufactured_terms,
        "manufactured_product_terms_normalized": {_normalize_text(term) for term in manufactured_terms},
        "excluded_product_labels": excluded_labels,
        "excluded_product_labels_normalized": {_normalize_text(label) for label in excluded_labels},
        "excluded_product_label_patterns": excluded_patterns,
        "excluded_product_label_patterns_normalized": {_normalize_text(pattern) for pattern in excluded_patterns},
        "scan_max_pages": _as_int(raw.get("scan_max_pages"), DEFAULT_SCAN_MAX_PAGES),
        "scan_min_pages": _as_int(raw.get("scan_min_pages"), DEFAULT_SCAN_MIN_PAGES),
        "stop_after_empty_active_pages": _as_int(
            raw.get("stop_after_empty_active_pages"),
            DEFAULT_STOP_AFTER_EMPTY_ACTIVE_PAGES,
            minimum=0,
        ),
        "cache_ttl_seconds": _as_int(raw.get("cache_ttl_seconds"), DEFAULT_CACHE_TTL_SECONDS),
        "auto_refresh_seconds": _as_int(raw.get("auto_refresh_seconds"), DEFAULT_AUTO_REFRESH_SECONDS),
    }


def _coerce_quantity(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _product_identifier(item: Dict[str, Any], label_norm: str) -> Tuple[str, str]:
    for field in ("ean", "import_code", "warehouse_number"):
        value = str(item.get(field) or "").strip()
        if value:
            return f"{field}:{value}", value
    return f"label:{label_norm}", ""


def _manufacturing_decision(label: str, settings: Dict[str, Any]) -> Tuple[bool, str]:
    label_norm = _normalize_text(label)
    if not label_norm:
        return False, "missing_label"

    if label_norm in settings["excluded_product_labels_normalized"]:
        return False, "excluded_product"

    for pattern in settings["excluded_product_label_patterns_normalized"]:
        if pattern and pattern in label_norm:
            return False, "excluded_product"

    manufactured_terms = settings["manufactured_product_terms_normalized"]
    if manufactured_terms and not any(term and term in label_norm for term in manufactured_terms):
        return False, "not_manufactured_brand"

    return True, "manufactured"


def _is_active_order(order: Dict[str, Any], settings: Dict[str, Any]) -> bool:
    status = order.get("status") or {}
    return _normalize_text(status.get("name")) in settings["active_order_statuses_normalized"]


def _order_sort_key(order: Dict[str, Any]) -> Tuple[str, str]:
    return (str(order.get("purchase_at") or ""), str(order.get("order_num") or ""))


def build_production_board_snapshot(
    *,
    project: str,
    orders: List[Dict[str, Any]],
    settings: Dict[str, Any],
    scan: Optional[Dict[str, Any]] = None,
    generated_at: Optional[str] = None,
) -> Dict[str, Any]:
    if not settings["enabled"]:
        raise ValueError(f"Production board is not enabled for project '{project}'.")

    active_orders: List[Dict[str, Any]] = []
    product_map: Dict[str, Dict[str, Any]] = {}
    ignored_items: Dict[str, Dict[str, Any]] = {}

    for order in orders:
        if not _is_active_order(order, settings):
            continue

        status = order.get("status") or {}
        status_name = str(status.get("name") or "").strip()
        order_items: List[Dict[str, Any]] = []
        manufacturing_units = 0.0
        ignored_units = 0.0

        for item in order.get("items") or []:
            label = str(item.get("item_label") or "").strip()
            quantity = _coerce_quantity(item.get("quantity"))
            if quantity <= 0:
                continue

            label_norm = _normalize_text(label)
            item_key, identifier = _product_identifier(item, label_norm)
            should_make, reason = _manufacturing_decision(label, settings)

            item_row = {
                "key": item_key,
                "label": label,
                "identifier": identifier,
                "quantity": quantity,
                "manufactured": should_make,
                "reason": reason,
            }
            order_items.append(item_row)

            if should_make:
                manufacturing_units += quantity
                product = product_map.setdefault(
                    item_key,
                    {
                        "key": item_key,
                        "label": label,
                        "identifier": identifier,
                        "quantity_required": 0.0,
                        "orders_count": 0,
                        "orders": [],
                        "statuses": {},
                        "labels": [],
                        "oldest_order_at": None,
                        "latest_order_at": None,
                    },
                )
                product["quantity_required"] += quantity
                product["statuses"][status_name] = product["statuses"].get(status_name, 0) + 1
                if label and label not in product["labels"]:
                    product["labels"].append(label)
                product_order = {
                    "order_num": order.get("order_num"),
                    "purchase_at": order.get("pur_date"),
                    "last_change": order.get("last_change"),
                    "status": status_name,
                    "quantity": quantity,
                    "sum": (order.get("sum") or {}).get("formatted"),
                }
                product["orders"].append(product_order)
            else:
                ignored_units += quantity
                ignored = ignored_items.setdefault(
                    item_key,
                    {
                        "key": item_key,
                        "label": label,
                        "identifier": identifier,
                        "quantity": 0.0,
                        "reason": reason,
                        "orders_count": 0,
                    },
                )
                ignored["quantity"] += quantity
                ignored["orders_count"] += 1

        active_orders.append(
            {
                "id": order.get("id"),
                "order_num": order.get("order_num"),
                "purchase_at": order.get("pur_date"),
                "last_change": order.get("last_change"),
                "status": status_name,
                "status_id": status.get("id"),
                "sum": (order.get("sum") or {}).get("formatted"),
                "manufacturing_units": manufacturing_units,
                "ignored_units": ignored_units,
                "items": order_items,
            }
        )

    for product in product_map.values():
        product["orders"].sort(key=_order_sort_key)
        product["orders_count"] = len({str(order.get("order_num")) for order in product["orders"]})
        product["oldest_order_at"] = product["orders"][0].get("purchase_at") if product["orders"] else None
        product["latest_order_at"] = product["orders"][-1].get("purchase_at") if product["orders"] else None
        product["quantity_required"] = round(float(product["quantity_required"]), 3)

    products = sorted(
        product_map.values(),
        key=lambda row: (-float(row["quantity_required"]), str(row.get("oldest_order_at") or ""), str(row.get("label") or "")),
    )
    active_orders.sort(key=_order_sort_key)

    generated = generated_at or datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    oldest_active = active_orders[0]["purchase_at"] if active_orders else None
    latest_active = active_orders[-1]["purchase_at"] if active_orders else None

    return {
        "project": project,
        "generated_at": generated,
        "active_order_statuses": settings["active_order_statuses"],
        "manufactured_product_terms": settings["manufactured_product_terms"],
        "excluded_product_labels": settings["excluded_product_labels"],
        "auto_refresh_seconds": settings["auto_refresh_seconds"],
        "summary": {
            "active_orders": len(active_orders),
            "manufacturing_orders": len([order for order in active_orders if order["manufacturing_units"] > 0]),
            "manufacturing_products": len(products),
            "units_to_make": round(sum(float(product["quantity_required"]) for product in products), 3),
            "ignored_units": round(sum(float(order["ignored_units"]) for order in active_orders), 3),
            "oldest_active_order_at": oldest_active,
            "latest_active_order_at": latest_active,
        },
        "scan": scan or {},
        "products": products,
        "orders": active_orders,
        "ignored_items": sorted(
            ignored_items.values(),
            key=lambda row: (-float(row["quantity"]), str(row.get("label") or "")),
        ),
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


def fetch_open_orders_for_production(project: str, settings: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    project_settings = load_project_settings(project)
    client = _build_client(project, project_settings)

    orders: List[Dict[str, Any]] = []
    cursor: Optional[int] = None
    page_count = 0
    has_next_page = True
    oldest_order_at: Optional[str] = None
    empty_active_pages = 0
    active_orders_seen = 0
    stop_reason = "api_exhausted"

    while has_next_page and page_count < settings["scan_max_pages"]:
        params: Dict[str, Any] = {
            "limit": 30,
            "order_by": "pur_date",
            "sort": "DESC",
        }
        if cursor is not None:
            params["cursor"] = cursor

        result = client.execute(ORDER_QUERY, variable_values={"params": params})
        payload = result.get("getOrderList") or {}
        page_orders = [order for order in (payload.get("data") or []) if order]
        orders.extend(page_orders)
        page_count += 1

        page_active_orders = sum(1 for order in page_orders if _is_active_order(order, settings))
        active_orders_seen += page_active_orders
        if page_active_orders:
            empty_active_pages = 0
        else:
            empty_active_pages += 1

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
            and settings["stop_after_empty_active_pages"] > 0
            and empty_active_pages >= settings["stop_after_empty_active_pages"]
        ):
            has_next_page = False
            stop_reason = "empty_active_pages"

    limit_reached = bool(has_next_page and page_count >= settings["scan_max_pages"])
    if limit_reached:
        stop_reason = "scan_max_pages"

    scan = {
        "orders_scanned": len(orders),
        "pages_scanned": page_count,
        "scan_max_pages": settings["scan_max_pages"],
        "scan_min_pages": settings["scan_min_pages"],
        "stop_after_empty_active_pages": settings["stop_after_empty_active_pages"],
        "empty_active_pages_at_stop": empty_active_pages,
        "active_orders_seen_during_scan": active_orders_seen,
        "oldest_order_at_scanned": oldest_order_at,
        "limit_reached": limit_reached,
        "stop_reason": stop_reason,
        "source": "biznisweb_api_desc_purchase_date_client_status_filter",
    }
    return orders, scan


def generate_production_board_snapshot(project: str = BASE_DEFAULT_PROJECT) -> Dict[str, Any]:
    project = (project or BASE_DEFAULT_PROJECT).strip() or BASE_DEFAULT_PROJECT
    load_project_env(project)
    project_settings = load_project_settings(project)
    settings = resolve_production_board_settings(project_settings)
    orders, scan = fetch_open_orders_for_production(project, settings)
    return build_production_board_snapshot(project=project, orders=orders, settings=settings, scan=scan)


def get_cached_production_board_snapshot(project: str, *, force_refresh: bool = False) -> Dict[str, Any]:
    project = (project or BASE_DEFAULT_PROJECT).strip() or BASE_DEFAULT_PROJECT
    project_settings = load_project_settings(project)
    settings = resolve_production_board_settings(project_settings)
    if not settings["enabled"]:
        raise ValueError(f"Production board is not enabled for project '{project}'.")

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
        payload = generate_production_board_snapshot(project)
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
