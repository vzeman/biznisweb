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


class ReceiptSummaryError(ValueError):
    """Raised when the CloudWatch receipt export cannot be trusted."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ReceiptSummaryError(message)


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
    _require(isinstance(payload, dict), "CloudWatch export must be an object")
    _require(not payload.get("nextToken"), "CloudWatch export is incomplete")
    events = payload.get("events")
    _require(isinstance(events, list), "CloudWatch export events must be a list")

    event_ids: set[str] = set()
    duplicates = 0
    for index, event in enumerate(events):
        _require(isinstance(event, dict), f"CloudWatch event {index} must be an object")
        event_id = event.get("eventId")
        timestamp = event.get("timestamp")
        message = event.get("message")
        _require(isinstance(event_id, str) and event_id, f"CloudWatch event {index} ID missing")
        _require(event_id not in event_ids, "CloudWatch receipt event ID is duplicated")
        event_ids.add(event_id)
        _require(type(timestamp) is int, f"CloudWatch event {index} timestamp is invalid")
        _require(
            start_ms <= timestamp < through_ms,
            f"CloudWatch event {index} is outside the requested window",
        )
        _require(isinstance(message, str), f"CloudWatch event {index} message is invalid")
        try:
            receipt = json.loads(message.strip())
        except json.JSONDecodeError as exc:
            raise ReceiptSummaryError(
                f"CloudWatch event {index} receipt is not valid JSON"
            ) from exc
        _require(isinstance(receipt, dict), f"CloudWatch event {index} receipt must be an object")
        _require(
            set(receipt) == EXPECTED_RECEIPT_KEYS,
            f"CloudWatch event {index} receipt field set drift",
        )
        _require(receipt["schema_version"] == 1, "receipt schema drift")
        _require(receipt["marker"] == RECEIPT_MARKER, "receipt marker drift")
        _require(receipt["accepted"] is True, "receipt accepted state drift")
        _require(type(receipt["duplicate"]) is bool, "receipt duplicate state drift")
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
