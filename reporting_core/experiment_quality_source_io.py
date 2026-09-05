"""Read-only input adapters for a future protected exact-window quality run.

Clients are injected: this module never finds credentials, creates a client,
writes files, publishes facts or logs source data. Returned rows are sensitive
runner-memory inputs, NOT artifacts. Only ``sanitized_proof`` may be exported.

An unchanged inventory proves a complete, stable read of the *retained* date
partitions. It cannot prove historical retention, the context floor, AWS/shop
identity, or successful-main provenance. The protected producer must establish
those separately before calling this adapter; no runtime gate is opened here.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from itertools import islice
from typing import Any, Callable, Mapping

from .experiments import order_completion_receipts


_BUCKET = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
_FILE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,179}[.]json$")
_ETAG = re.compile(r'^"[a-fA-F0-9]{32}(?:-[1-9][0-9]*)?"$')
_ORDER = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_MAX_EVENT_BYTES = 16_384
_MAX_EXTRACT_BYTES = 128 * 1024 * 1024
_MAX_RAW_READ_WORKERS = 8
_MAX_ORDER_BYTES = 128 * 1024
_MAX_ORDER_EXTRACT_BYTES = 32 * 1024 * 1024
RAW_SOURCE_PHASES = (
    "raw-inventory-before", "raw-conditional-reads",
    "raw-event-validation", "raw-inventory-after",
)

# The same money/item/status fields used by the existing reporting adapter;
# intentionally no customer, address, contact, payment credentials or list API.
# The fixed operation cannot be replaced by caller-supplied query text.
RECEIPTED_ORDER_QUERY = """query ExactReceiptedQualityOrder($order_num: String!) {
  getOrder(order_num: $order_num) {
    order_num
    status { id name }
    price_elements {
      type title value reference_id
      price { value raw_value formatted is_net_price }
    }
    items {
      item_label ean import_code warehouse_number quantity tax_rate
      price { value is_net_price currency { code } }
      sum { value is_net_price currency { code } }
      sum_with_tax { value is_net_price currency { code } }
    }
    sum { value is_net_price currency { code } }
  }
}
"""


class QualityInputError(ValueError):
    """An input read cannot establish the required source boundary."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise QualityInputError(message)


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        _require(key not in result, "duplicate source JSON field")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    raise QualityInputError("nonfinite source JSON value")


def _utc(value: Any) -> datetime:
    _require(isinstance(value, datetime) and value.tzinfo is not None
             and value.utcoffset() == timedelta(0), "source timestamp must be UTC")
    return value.astimezone(UTC)


def _stamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class RetainedRawSource:
    # repr=False prevents accidental diagnostic printing of anonymous identities.
    rows: tuple[dict[str, Any], ...] = field(repr=False)
    sanitized_proof: dict[str, Any]


def read_stable_retained_raw_source(
    s3: Any, *, bucket: str, context_from_utc: datetime, through_utc: datetime,
    max_objects: int = 100_000, progress: Callable[[str], None] | None = None,
    max_read_workers: int = 1,
) -> RetainedRawSource:
    """Enumerate exact partitions twice; bind every bounded GET with IfMatch.

    Includes the complete last UTC partition, even if ``through`` is a local
    midnight inside it. Receipt validation is performed on every row; the
    existing windowed calculator, not this I/O adapter, excludes the later edge.
    ``context_from`` must be a proven UTC midnight to avoid a partial first day.
    Optional progress receives four fixed substep names, never source values,
    per-object updates or counts. The default remains silent.
    Reads default to serial. Opting into 2..8 workers requires an already-created
    thread-safe client; only conditional GETs run concurrently. Submission stays
    bounded by that limit, reduction stays in sorted key order, and all workers
    finish/close their bodies before validation, the second inventory or return.
    """
    try:
        return _read_raw(s3, bucket, context_from_utc, through_utc, max_objects,
                         progress, max_read_workers)
    except Exception:
        # SDK exceptions can contain keys, request IDs and payloads. Never chain
        # or expose them through the future producer's standard error handler.
        raise QualityInputError("retained raw source coverage could not be verified") from None


def _read_raw(s3, bucket, context_from, through, max_objects, progress, max_read_workers):
    _require(isinstance(bucket, str) and _BUCKET.fullmatch(bucket) is not None,
             "invalid source bucket")
    start = _utc(context_from)
    end = _utc(through)
    _require(start.hour == start.minute == start.second == start.microsecond == 0,
             "source context must begin at UTC midnight")
    _require(end.microsecond == 0 and start < end, "invalid source bounds")
    last_date = (end - timedelta(microseconds=1)).date()
    days = (last_date - start.date()).days + 1
    _require(1 <= days <= 90, "source partition limit exceeded")
    _require(type(max_objects) is int and 1 <= max_objects <= 100_000,
             "invalid source object limit")
    _require(type(max_read_workers) is int and 1 <= max_read_workers <= _MAX_RAW_READ_WORKERS,
             "invalid source read worker limit")
    prefixes = [f"experiment-events/raw/event_date={(start + timedelta(days=i)).date()}/"
                for i in range(days)]

    def inventory():
        entries = {}
        total_bytes = 0
        for prefix in prefixes:
            token = None
            used_tokens = set()
            while True:
                request = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
                if token is not None:
                    request["ContinuationToken"] = token
                page = s3.list_objects_v2(**request)
                _require(isinstance(page, Mapping), "invalid source listing")
                _require(type(page.get("IsTruncated")) is bool, "missing pagination proof")
                objects = page.get("Contents", [])
                _require(isinstance(objects, list) and len(objects) <= 1000,
                         "invalid source page")
                for item in objects:
                    _require(isinstance(item, Mapping), "invalid source object")
                    key = item.get("Key")
                    _require(isinstance(key, str) and key.startswith(prefix)
                             and _FILE.fullmatch(key[len(prefix):]) is not None
                             and ".." not in key, "source escaped exact partition")
                    _require(key not in entries, "source listing repeated an object")
                    size, etag = item.get("Size"), item.get("ETag")
                    _require(type(size) is int and 0 < size <= _MAX_EVENT_BYTES,
                             "invalid source object size")
                    _require(isinstance(etag, str) and _ETAG.fullmatch(etag) is not None,
                             "source object is not conditionally readable")
                    modified = _utc(item.get("LastModified"))
                    _require(len(entries) < max_objects, "source object limit exceeded")
                    total_bytes += size
                    _require(total_bytes <= _MAX_EXTRACT_BYTES, "source byte limit exceeded")
                    entries[key] = {"key": key, "size": size, "etag": etag,
                                    "last_modified_utc": _stamp(modified)}
                if not page["IsTruncated"]:
                    _require(not page.get("NextContinuationToken"), "unexpected continuation")
                    break
                token = page.get("NextContinuationToken")
                _require(isinstance(token, str) and 0 < len(token) <= 4096
                         and token not in used_tokens and bool(objects),
                         "invalid source continuation")
                used_tokens.add(token)
        return entries

    def mark(phase):
        if progress is not None:
            progress(phase)

    mark(RAW_SOURCE_PHASES[0])
    before = inventory()
    rows = []
    mark(RAW_SOURCE_PHASES[1])

    def read_one(key):
        item = before[key]
        response = s3.get_object(Bucket=bucket, Key=key, IfMatch=item["etag"])
        _require(isinstance(response, Mapping), "invalid source response")
        body = response.get("Body")
        _require(hasattr(body, "read") and hasattr(body, "close"), "invalid source body")
        try:
            _require(response.get("ETag") == item["etag"]
                     and type(response.get("ContentLength")) is int
                     and response["ContentLength"] == item["size"]
                     and _stamp(_utc(response.get("LastModified"))) == item["last_modified_utc"],
                     "source changed while reading")
            raw = body.read(_MAX_EVENT_BYTES + 1)
            _require(isinstance(raw, bytes) and len(raw) == item["size"], "incomplete source body")
            row = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object,
                             parse_constant=_reject_json_constant)
            _require(isinstance(row, dict), "invalid source JSON object")
            receipt = datetime.fromisoformat(row["received_at"].replace("Z", "+00:00"))
            _utc(receipt)
            expected_prefix = f"experiment-events/raw/event_date={receipt.date()}/"
            _require(key.startswith(expected_prefix), "receipt partition mismatch")
            return row
        finally:
            body.close()

    if max_read_workers == 1 or not before:
        rows = [read_one(key) for key in sorted(before)]
    else:
        keys = iter(sorted(before))
        # Do not use Executor.map: Python 3.11 eagerly submits the whole input.
        # Only the coordinator submits/reduces; workers never await futures or
        # mutate shared rows/inventories. On any failure cancel pending work and
        # let the context manager drain running reads before the sanitized error.
        with ThreadPoolExecutor(max_workers=max_read_workers) as executor:
            pending = deque()
            try:
                for key in islice(keys, max_read_workers):
                    pending.append(executor.submit(read_one, key))
                while pending:
                    rows.append(pending.popleft().result())
                    key = next(keys, None)
                    if key is not None:
                        pending.append(executor.submit(read_one, key))
            finally:
                for future in pending:
                    future.cancel()
    # Shared strict event validation: reject unknown/PII fields, contradictory
    # duplicates and invalid receipt/order identities even on partition edges.
    mark(RAW_SOURCE_PHASES[2])
    order_completion_receipts(rows)
    mark(RAW_SOURCE_PHASES[3])
    after = inventory()
    _require(before == after, "source inventory changed during capture")
    digest = _digest([before[key] for key in sorted(before)])
    return RetainedRawSource(tuple(rows), {
        "schema_version": 1,
        "coverage": "stable_retained_utc_partitions_only",
        "context_from_utc": _stamp(start),
        "through_utc": _stamp(end),
        "last_partition_date": last_date.isoformat(),
        "inventory_before_sha256": digest,
        "inventory_after_sha256": digest,
        "conditional_reads_verified": True,
        "receipt_partition_parity_verified": True,
        "historical_retention_proven": False,
        "context_floor_proven": False,
        "contains_identities": False,
    })


@dataclass(frozen=True)
class ReceiptedOrderSource:
    orders: tuple[dict[str, Any], ...] = field(repr=False)
    sanitized_proof: dict[str, Any]


_MONEY = {"value": None, "is_net_price": None, "currency": {"code": None}}
_ORDER_SHAPE = {
    "order_num": None,
    "status": {"id": None, "name": None},
    "price_elements": [{"type": None, "title": None, "value": None,
                        "reference_id": None,
                        "price": {"value": None, "raw_value": None,
                                  "formatted": None, "is_net_price": None}}],
    "items": [{"item_label": None, "ean": None, "import_code": None,
               "warehouse_number": None, "quantity": None, "tax_rate": None,
               "price": _MONEY, "sum": _MONEY, "sum_with_tax": _MONEY}],
    "sum": _MONEY,
}


def _validate_shape(value: Any, shape: Any) -> None:
    if value is None:
        return
    if shape is None:
        _require(type(value) in (str, bool, int, float), "invalid order scalar")
    elif isinstance(shape, list):
        _require(isinstance(value, list) and len(value) <= 1000, "invalid order collection")
        for child in value:
            _require(child is not None, "missing order collection member")
            _validate_shape(child, shape[0])
    else:
        _require(isinstance(value, dict) and set(value) == set(shape), "order field set drift")
        for key, child_shape in shape.items():
            _validate_shape(value[key], child_shape)


def read_receipted_order_source(
    execute_query: Callable[..., Mapping[str, Any]],
    *, completion_receipts: Mapping[str, datetime], max_orders: int = 2000,
) -> ReceiptedOrderSource:
    """Read only exact receipt IDs twice, including explicit not-found answers.

    ``execute_query`` must use the independently verified VEVO endpoint and
    managed token, pace requests and reject HTTP/GraphQL errors. It returns the
    GraphQL *data* object, never a partial error response. No list, enrichment,
    retry-until-pass, synthetic order or mutation is performed by this adapter.
    Two equal passes are a drift check, not an atomic historical API snapshot.
    """
    try:
        return _read_orders(execute_query, completion_receipts, max_orders)
    except Exception:
        raise QualityInputError("receipted order coverage could not be verified") from None


def _read_orders(execute_query, receipts, max_orders):
    _require(isinstance(receipts, Mapping), "invalid completion receipts")
    _require(type(max_orders) is int and 1 <= max_orders <= 2000
             and len(receipts) <= max_orders, "order source limit exceeded")
    for number, receipt in receipts.items():
        _require(isinstance(number, str) and _ORDER.fullmatch(number) is not None,
                 "invalid receipt order identity")
        _utc(receipt)
    numbers = sorted(receipts)

    def capture():
        result = {}
        byte_count = 0
        for number in numbers:
            data = execute_query(RECEIPTED_ORDER_QUERY, variable_values={"order_num": number})
            _require(isinstance(data, dict) and set(data) == {"getOrder"},
                     "incomplete or errored order response")
            order = data["getOrder"]
            if order is not None:
                _validate_shape(order, _ORDER_SHAPE)
                _require(order["order_num"] == number, "order response identity mismatch")
            raw = _canonical(order)
            byte_count += len(raw)
            _require(len(raw) <= _MAX_ORDER_BYTES and byte_count <= _MAX_ORDER_EXTRACT_BYTES,
                     "order source byte limit exceeded")
            # Isolate from a client reusing/mutating response objects between calls.
            result[number] = json.loads(raw)
        return result

    before = capture()
    after = capture()
    _require(_canonical(before) == _canonical(after), "order source changed during capture")
    digest = _digest(before)
    return ReceiptedOrderSource(tuple(before[n] for n in numbers if before[n] is not None), {
        "schema_version": 1,
        "coverage": "every_supplied_receipt_id_explicitly_queried",
        "query_sha256": hashlib.sha256(RECEIPTED_ORDER_QUERY.encode()).hexdigest(),
        "receipt_set_sha256": _digest({n: _stamp(receipts[n]) for n in numbers}),
        "responses_before_sha256": digest,
        "responses_after_sha256": digest,
        "explicit_not_found_retained_in_digest": True,
        "atomic_historical_snapshot_proven": False,
        "contains_identities": False,
    })
