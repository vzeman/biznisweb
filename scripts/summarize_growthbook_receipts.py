#!/usr/bin/env python3
"""Reduce raw CloudWatch receipt events to a sanitized A/A count component.

The input is a temporary local AWS ``filter-log-events`` JSON response. Raw log
event identities, stream names, timestamps, and messages are validated but are
never copied into the canonical output.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

try:
    from growthbook_collector.handler import RECEIPT_MARKER
except ModuleNotFoundError:  # Direct execution from the scripts directory.
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from growthbook_collector.handler import RECEIPT_MARKER


UTC_RE = re.compile(r"^20[2-9][0-9]-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")
EXPECTED_RECEIPT_KEYS = {"accepted", "duplicate", "marker", "schema_version"}
RECEIPT_FAILURE_CODES = frozenset({
    "receipt-summary-invalid", "receipt-export-invalid", "receipt-export-incomplete",
    "receipt-event-invalid", "receipt-log-id-missing", "receipt-log-id-duplicate",
    "receipt-timestamp-invalid", "receipt-outside-window", "receipt-message-invalid",
    "receipt-json-invalid", "receipt-json-concatenated-markers",
    "receipt-object-invalid", "receipt-fields-drift",
    "receipt-schema-drift", "receipt-marker-drift", "receipt-accepted-drift",
    "receipt-duplicate-drift",
})


class ReceiptSummaryError(ValueError):
    """Raised when the CloudWatch receipt export cannot be trusted."""

    def __init__(self, message: str, *, safe_code: str = "receipt-summary-invalid") -> None:
        super().__init__(message)
        # The managed source may emit this fixed category, never the detailed
        # local exception message (which can contain an event position).
        self.safe_code = (
            safe_code if type(safe_code) is str and safe_code in RECEIPT_FAILURE_CODES
            else "receipt-summary-invalid"
        )


def _require(condition: bool, message: str, *, safe_code: str = "receipt-summary-invalid") -> None:
    if not condition:
        raise ReceiptSummaryError(message, safe_code=safe_code)


def _parse_utc(value: str, field: str) -> datetime:
    _require(UTC_RE.fullmatch(value) is not None, f"{field} must use whole-second UTC Z format")
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError as exc:
        raise ReceiptSummaryError(f"{field} is invalid") from exc


def _canonical_json(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def _invalid_json_code(message: str) -> str:
    """Recognize only a bounded exact marker-framing shape; still reject it.

    No recovery, splitting into accepted events, message export or count output.
    Each candidate is the collector's exact canonical four-field serialization.
    Any other prefix/suffix/field/format or an exceeded diagnostic bound stays
    generic. This cannot prove why multiple markers share one log event.
    """
    if len(message) > 8192:
        return "receipt-json-invalid"
    candidates = tuple(_canonical_json({
        "accepted": True, "duplicate": duplicate, "marker": RECEIPT_MARKER, "schema_version": 1,
    }).decode().strip() for duplicate in (False, True))
    remaining, matched = message.strip(), 0
    while remaining and matched < 64:
        for candidate in candidates:
            if remaining.startswith(candidate):
                remaining = remaining[len(candidate):].lstrip()
                matched += 1
                break
        else:
            return "receipt-json-invalid"
    return "receipt-json-concatenated-markers" if not remaining and matched >= 2 else "receipt-json-invalid"


def _write_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(_canonical_json(payload))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    except Exception:
        try:
            os.unlink(temporary_name)
        except OSError:
            pass
        raise


def summarize_receipts(
    payload: Mapping[str, Any], *, from_utc: str, through_utc: str
) -> dict[str, Any]:
    """Validate exact marker messages and return aggregate counts only."""

    start = _parse_utc(from_utc, "from_utc")
    through = _parse_utc(through_utc, "through_utc")
    _require(through > start, "receipt window must be non-empty")
    start_ms = int(start.timestamp() * 1000)
    through_ms = int(through.timestamp() * 1000)
    _require(isinstance(payload, dict), "CloudWatch export must be an object", safe_code="receipt-export-invalid")
    _require(not payload.get("nextToken"), "CloudWatch export is incomplete", safe_code="receipt-export-incomplete")
    events = payload.get("events")
    _require(isinstance(events, list), "CloudWatch export events must be a list", safe_code="receipt-export-invalid")

    event_ids: set[str] = set()
    duplicates = 0
    for index, event in enumerate(events):
        _require(isinstance(event, dict), f"CloudWatch event {index} must be an object", safe_code="receipt-event-invalid")
        event_id = event.get("eventId")
        timestamp = event.get("timestamp")
        message = event.get("message")
        _require(isinstance(event_id, str) and event_id, f"CloudWatch event {index} ID missing", safe_code="receipt-log-id-missing")
        _require(event_id not in event_ids, "CloudWatch receipt event ID is duplicated", safe_code="receipt-log-id-duplicate")
        event_ids.add(event_id)
        _require(type(timestamp) is int, f"CloudWatch event {index} timestamp is invalid", safe_code="receipt-timestamp-invalid")
        _require(
            start_ms <= timestamp < through_ms,
            f"CloudWatch event {index} is outside the requested window",
            safe_code="receipt-outside-window",
        )
        _require(isinstance(message, str), f"CloudWatch event {index} message is invalid", safe_code="receipt-message-invalid")
        try:
            receipt = json.loads(message.strip())
        except json.JSONDecodeError as exc:
            raise ReceiptSummaryError(
                f"CloudWatch event {index} receipt is not valid JSON", safe_code=_invalid_json_code(message)
            ) from exc
        _require(isinstance(receipt, dict), f"CloudWatch event {index} receipt must be an object", safe_code="receipt-object-invalid")
        _require(
            set(receipt) == EXPECTED_RECEIPT_KEYS,
            f"CloudWatch event {index} receipt field set drift",
            safe_code="receipt-fields-drift",
        )
        _require(receipt["schema_version"] == 1, "receipt schema drift", safe_code="receipt-schema-drift")
        _require(receipt["marker"] == RECEIPT_MARKER, "receipt marker drift", safe_code="receipt-marker-drift")
        _require(receipt["accepted"] is True, "receipt accepted state drift", safe_code="receipt-accepted-drift")
        _require(type(receipt["duplicate"]) is bool, "receipt duplicate state drift", safe_code="receipt-duplicate-drift")
        duplicates += int(receipt["duplicate"])

    received = len(events)
    return {
        "schema_version": 1,
        "evidence_type": "vevo_growthbook_collector_receipt_counts",
        "marker": RECEIPT_MARKER,
        "from_utc": from_utc,
        "through_utc": through_utc,
        "collector_received_event_count": received,
        "collector_unique_accepted_event_count": received - duplicates,
        "collector_duplicate_event_count": duplicates,
        "contains_raw_log_events": False,
        "contains_event_or_device_ids": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--from-utc", required=True)
    parser.add_argument("--through-utc", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--require-nonempty", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        payload = json.loads(args.input.read_text(encoding="utf-8"))
        summary = summarize_receipts(
            payload,
            from_utc=args.from_utc,
            through_utc=args.through_utc,
        )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ReceiptSummaryError) as exc:
        print(f"VEVO_GROWTHBOOK_RECEIPTS_INVALID:{exc}")
        return 2
    _write_atomic(args.output, summary)
    print(
        "VEVO_GROWTHBOOK_RECEIPTS_SUMMARIZED:"
        f"received={summary['collector_received_event_count']}:"
        f"unique={summary['collector_unique_accepted_event_count']}:"
        f"duplicates={summary['collector_duplicate_event_count']}:raw=false"
    )
    if args.require_nonempty and summary["collector_received_event_count"] == 0:
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
