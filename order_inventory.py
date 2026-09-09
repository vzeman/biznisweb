"""Complete, bounded BiznisWeb order inventories over mutable offset cursors.

The caller owns authentication, read retries/pacing and business validation.
This module has no mutation, storage or shop-specific dependencies.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class InventoryScanBudget:
    max_pages: int = 5000
    pages: int = 0
    started_at: float | None = None
    timeout_seconds: float = 1200

    def start(self) -> float:
        if self.started_at is None:
            self.started_at = time.monotonic()
        return self.started_at + self.timeout_seconds


def internal_order_id(order: dict[str, Any]) -> int:
    raw = order.get("id")
    if (isinstance(raw, bool) or not isinstance(raw, (int, str))
            or not str(raw).isascii() or not str(raw).isdigit() or int(raw) <= 0):
        raise RuntimeError("Order inventory has an invalid internal order ID")
    return int(raw)


def scan_order_inventory(
    read: Callable[[dict[str, Any], float], dict[str, Any]],
    validate_order: Callable[[Any], None],
    *,
    budget: InventoryScanBudget,
    changed_from: str | None = None,
    page_limit: int = 30,
    label: str = "Order inventory scan",
    logger: logging.Logger | None = None,
) -> list[dict[str, Any]]:
    """Use a maximum-ID anchor and consume pages proving ID continuity.

    Full inventories must not filter mutable status membership. Incremental
    changed-from scans may gain older IDs behind the cursor; their caller must
    persist a scan-START watermark with overlap, never the completion time.
    The read callback must enforce the supplied deadline during retry waits.
    Reuse one budget for a runner's full and incremental inventory scans.
    """
    if not 2 <= page_limit <= 30:
        raise ValueError("Order inventory page limit must be between 2 and 30 for overlap")
    deadline = budget.start()
    orders: dict[str, dict[str, Any]] = {}
    repairs = 0

    def read_page(offset: int, *, limit: int = page_limit, descending: bool = False,
                  unfiltered: bool = False) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        if budget.pages >= budget.max_pages or time.monotonic() > deadline:
            raise RuntimeError(f"{label} limit reached before completion")
        variables: dict[str, Any] = {"params": {
            "limit": limit, "order_by": "order_id", "sort": "DESC" if descending else "ASC",
            "cursor": offset,
        }}
        if changed_from is not None and not unfiltered:
            variables["changed_from"] = changed_from
        result = read(variables, deadline)
        budget.pages += 1
        if time.monotonic() > deadline:
            raise RuntimeError(f"{label} time limit reached before completion")
        payload = result.get("getOrderList") if isinstance(result, dict) else None
        if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
            raise RuntimeError(f"Incomplete {label.lower()} response")
        page = payload["data"]
        info = payload.get("pageInfo")
        if (not isinstance(info, dict) or not isinstance(info.get("hasNextPage"), bool)
                or not isinstance(info.get("totalRecords"), int)
                or isinstance(info["totalRecords"], bool) or info["totalRecords"] < 0):
            raise RuntimeError(f"{label} lacks reliable pagination metadata")
        if len(page) > limit:
            raise RuntimeError(f"{label} exceeded the requested page size")
        end = offset + len(page)
        has_remaining = end < info["totalRecords"]
        # FLOX can advertise a next page when this terminal page is exactly
        # full, including a nextCursor equal to totalRecords. Validate that
        # narrow convention before deriving logical remaining rows. A short,
        # missing or contradictory page must never be accepted as complete.
        full_terminal_hint = (
            info["hasNextPage"] and len(page) == limit and end == info["totalRecords"]
        )
        if ((info["hasNextPage"] != has_remaining and not full_terminal_hint)
                or (page and end > info["totalRecords"])
                or (not page and offset < info["totalRecords"])
                or (info["hasNextPage"] and len(page) != limit)):
            raise RuntimeError(f"{label} has inconsistent pagination metadata at offset {offset}")
        for order in page:
            validate_order(order)
            if not isinstance(order, dict) or not str(order.get("order_num") or "").strip():
                raise RuntimeError(f"{label} has a missing order identity")
        ids = [internal_order_id(order) for order in page]
        if any((a <= b if descending else a >= b) for a, b in zip(ids, ids[1:])):
            raise RuntimeError(f"{label} order IDs are not strictly sorted at offset {offset}")
        if not page and info["hasNextPage"]:
            raise RuntimeError(f"{label} has an empty non-final page")
        if info["hasNextPage"]:
            next_cursor = info.get("nextCursor")
            if (not isinstance(next_cursor, int) or isinstance(next_cursor, bool)
                    or next_cursor != offset + len(page)):
                raise RuntimeError(f"{label} cursor did not advance reliably at offset {offset}")
        # Preserve the response's raw metadata for callers and diagnostics.
        # Continuity repair and termination use only this validated logical view.
        return page, {**info, "hasNextPage": has_remaining}

    def boundary_is_proven(page: list[dict[str, Any]], info: dict[str, Any],
                           offset: int, previous_id: int) -> bool:
        if not page:
            return offset == 0 and not info["hasNextPage"]
        if internal_order_id(page[0]) > previous_id:
            return offset == 0
        return internal_order_id(page[-1]) > previous_id or not info["hasNextPage"]

    def recover_boundary(offset: int, page: list[dict[str, Any]], info: dict[str, Any],
                         previous_id: int) -> tuple[int, list[dict[str, Any]], dict[str, Any]]:
        nonlocal repairs
        while not boundary_is_proven(page, info, offset, previous_id):
            repairs += 1
            if repairs > 8:
                raise RuntimeError(f"{label} boundary repair limit reached at offset {offset}")
            if logger:
                logger.warning("%s repairing offset boundary: offset=%s previous_internal_id=%s attempt=%s",
                               label, offset, previous_id, repairs)
            # Seek an estimate, then prove its adjacent boundary in ONE response.
            # Churn can invalidate the estimate, never the continuity check.
            low, high = 0, max(offset, info["totalRecords"])
            for _ in range(32):
                if low >= high:
                    break
                middle = (low + high) // 2
                probe, _ = read_page(middle, limit=1)
                if not probe or internal_order_id(probe[0]) > previous_id:
                    high = middle
                else:
                    low = middle + 1
            else:
                raise RuntimeError(f"{label} boundary seek limit reached")
            offset = max(0, low - 1)
            page, info = read_page(offset)
        return offset, page, info

    anchor, _ = read_page(0, limit=1, descending=True, unfiltered=True)
    if not anchor:
        return []
    upper_id = internal_order_id(anchor[0])
    previous_id = 0
    offset = 0
    while True:
        page, info = read_page(offset)
        offset, page, info = recover_boundary(offset, page, info, previous_id)
        # Consume this exact verified page: another fetch could lose a successor
        # to a concurrent deletion after the continuity proof.
        for order in page:
            identity = internal_order_id(order)
            if identity <= previous_id:
                continue  # Verified overlap, never blind deduplication.
            if identity > upper_id:
                break
            number = str(order["order_num"])
            if number in orders:
                raise RuntimeError(f"{label} reused an order number at offset {offset}")
            orders[number] = order
            previous_id = identity
        if not page or internal_order_id(page[-1]) >= upper_id or not info["hasNextPage"]:
            break
        offset += len(page) - 1
    return list(orders.values())
