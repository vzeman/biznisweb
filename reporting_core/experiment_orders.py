#!/usr/bin/env python3
"""PII-dropping adapter from BiznisWeb orders to experiment order facts."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from .experiments import ExperimentDataError, ORDER_FIELDS


_ORDER_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_CENT = Decimal("0.01")


def _money(value: Any, field: str) -> Decimal:
    if isinstance(value, bool):
        raise ExperimentDataError(f"invalid {field}")
    try:
        parsed = Decimal(str(value or 0))
    except (InvalidOperation, ValueError) as exc:
        raise ExperimentDataError(f"invalid {field}") from exc
    if not parsed.is_finite() or abs(parsed) > Decimal("1000000"):
        raise ExperimentDataError(f"invalid {field}")
    return parsed.quantize(_CENT, rounding=ROUND_HALF_UP)


def _iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _lifecycle_state(exporter: Any, order: Mapping[str, Any], realized: bool) -> str:
    status = order.get("status") or {}
    status_name = status.get("name", "") if isinstance(status, Mapping) else ""
    bucket = exporter._classify_lifecycle_bucket(status_name)[0]
    if bucket == "refunded_returned":
        return "refunded"
    if bucket == "failed_cancelled":
        return "cancelled"
    return "realized" if realized else "pending"


def build_biznisweb_authoritative_orders(
    exporter: Any,
    raw_orders: Iterable[Mapping[str, Any]],
    *,
    completion_receipts: Mapping[str, datetime],
    generated_at: datetime,
    maturity_checkpoint_days: int,
    packaging_cost_eur: Any,
    shipping_net_cost_eur: Any,
    excluded_order_nums: Sequence[str] = (),
) -> List[Dict[str, Any]]:
    """Build the exact seven-field order boundary used by experiment facts.

    ``completion_receipts`` comes from validated, server-received
    ``order_completed`` events.  BiznisWeb remains authoritative for order
    existence, lifecycle and money.  The adapter may inspect PII-bearing raw
    orders in memory, but emits only ``ORDER_FIELDS``.
    """

    if generated_at.tzinfo is None or generated_at.utcoffset() is None:
        raise ExperimentDataError("generated_at must be timezone-aware")
    if (
        type(maturity_checkpoint_days) is not int
        or not 1 <= maturity_checkpoint_days <= 90
    ):
        raise ExperimentDataError("invalid maturity checkpoint")
    packaging_cost = _money(packaging_cost_eur, "packaging_cost_eur")
    shipping_net_cost = _money(shipping_net_cost_eur, "shipping_net_cost_eur")

    normalized_receipts: Dict[str, datetime] = {}
    for order_num, receipt in completion_receipts.items():
        normalized_num = str(order_num or "").strip()
        if not _ORDER_ID_RE.fullmatch(normalized_num):
            raise ExperimentDataError("invalid completion receipt order_num")
        if not isinstance(receipt, datetime) or receipt.tzinfo is None or receipt.utcoffset() is None:
            raise ExperimentDataError("completion receipt must be timezone-aware")
        normalized_receipts[normalized_num] = receipt.astimezone(timezone.utc)

    excluded = set()
    for order_num in excluded_order_nums:
        normalized_num = str(order_num or "").strip()
        if not _ORDER_ID_RE.fullmatch(normalized_num):
            raise ExperimentDataError("invalid excluded order_num")
        excluded.add(normalized_num)

    orders_by_num: Dict[str, Mapping[str, Any]] = {}
    for order in raw_orders:
        if not isinstance(order, Mapping):
            raise ExperimentDataError("BiznisWeb order must be an object")
        order_num = str(order.get("order_num") or "").strip()
        if not order_num or order_num not in normalized_receipts:
            continue
        if not _ORDER_ID_RE.fullmatch(order_num):
            raise ExperimentDataError("invalid BiznisWeb order_num")
        existing = orders_by_num.get(order_num)
        if existing is not None and existing != order:
            raise ExperimentDataError("one BiznisWeb order_num has conflicting source records")
        orders_by_num[order_num] = order

    facts: List[Dict[str, Any]] = []
    for order_num in sorted(orders_by_num):
        order = orders_by_num[order_num]
        realized, _reason = exporter._realized_revenue_decision(order)
        lifecycle_state = _lifecycle_state(exporter, order, bool(realized))
        rows = exporter.flatten_order(dict(order))
        if not isinstance(rows, list):
            raise ExperimentDataError("BiznisWeb flattening returned an invalid result")
        if lifecycle_state == "realized" and not rows:
            raise ExperimentDataError("realized BiznisWeb order has no reportable item rows")

        revenue = Decimal("0.00")
        product_cost = Decimal("0.00")
        for row in rows:
            if not isinstance(row, Mapping):
                raise ExperimentDataError("BiznisWeb flattened order row is invalid")
            revenue += _money(row.get("item_total_without_tax", 0), "item_total_without_tax")
            product_cost += _money(row.get("total_expense", 0), "total_expense")

        if lifecycle_state == "realized":
            net_revenue = revenue.quantize(_CENT, rounding=ROUND_HALF_UP)
            cm1 = (revenue - product_cost - packaging_cost - shipping_net_cost).quantize(
                _CENT,
                rounding=ROUND_HALF_UP,
            )
        else:
            # Non-realized orders stay in the join/lifecycle denominator but do
            # not introduce browser-derived or hypothetical business value.
            net_revenue = Decimal("0.00")
            cm1 = Decimal("0.00")

        completion_at = normalized_receipts[order_num]
        fact = {
            "order_num": order_num,
            "order_at": _iso_utc(completion_at),
            "net_revenue_eur": float(net_revenue),
            "cm1_eur": float(cm1),
            "lifecycle_state": lifecycle_state,
            "mature": generated_at.astimezone(timezone.utc)
            >= completion_at + timedelta(days=maturity_checkpoint_days),
            "excluded": order_num in excluded,
        }
        if set(fact) != ORDER_FIELDS:
            raise AssertionError("authoritative order schema drift")
        facts.append(fact)

    return facts
