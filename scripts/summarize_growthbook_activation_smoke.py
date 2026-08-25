#!/usr/bin/env python3
"""Reduce Production collector raw objects to identity-free A/A smoke evidence."""

from __future__ import annotations

import argparse
import json
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


EXPERIMENT_ID = "vevo-sk-aa-001"
VARIATIONS = ("control", "variant")
COLLECTOR_VERSION = "vevo-growthbook-collector-v1"
COMMON_FIELDS = {
    "schema_version",
    "event_id",
    "event_name",
    "occurred_at",
    "device_id",
    "page_path",
    "page_type",
    "consent_state",
    "experiment_id",
    "variation_id",
    "utm_source",
    "utm_medium",
    "meta_campaign_id",
    "meta_adset_id",
    "meta_ad_id",
    "meta_placement",
    "received_at",
    "event_date",
    "collector_version",
    "risk_result",
}
EVENT_FIELDS = {
    "experiment_exposure": set(),
    "add_to_cart": {"product_id"},
    "order_completed": {"transaction_id"},
    "performance_vital": {"page_load_id", "vital_name", "vital_value"},
    "client_error_observed": {"page_load_id", "error_kind"},
}


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def _parse_utc(value: Any, field: str) -> datetime:
    _require(isinstance(value, str) and value.endswith("Z"), f"{field} must be UTC Z")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError(f"{field} is invalid") from exc
    _require(parsed.tzinfo is not None and parsed.utcoffset() == timezone.utc.utcoffset(parsed), f"{field} must be UTC")
    return parsed


def _uuid4(value: Any, field: str) -> str:
    _require(isinstance(value, str), f"{field} must be a UUID string")
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError) as exc:
        raise ValueError(f"{field} is invalid") from exc
    _require(parsed.version == 4 and str(parsed) == value, f"{field} must be canonical UUIDv4")
    return value


def _canonical_bytes(payload: dict[str, Any]) -> bytes:
    return (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")


def summarize(directory: Path, from_utc: str, through_utc: str) -> dict[str, Any]:
    start = _parse_utc(from_utc, "from_utc")
    through = _parse_utc(through_utc, "through_utc")
    _require(through > start, "smoke window must be positive")
    _require(directory.is_dir(), "raw object directory is missing")

    event_ids: set[str] = set()
    exposure_counts_by_device: Counter[str] = Counter()
    exposure_variations_by_device: dict[str, set[str]] = defaultdict(set)
    variation_counts: Counter[str] = Counter()
    raw_event_count = 0
    target_exposure_count = 0
    product_exposure_count = 0

    paths = sorted(path for path in directory.iterdir() if path.is_file())
    _require(paths, "raw object directory is empty")
    for path in paths:
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("collector raw object is unreadable") from exc
        _require(isinstance(record, dict), "collector raw object must be an object")

        event_name = record.get("event_name")
        _require(event_name in EVENT_FIELDS, "collector raw event name drift")
        _require(set(record) == COMMON_FIELDS | EVENT_FIELDS[event_name], "collector raw field set drift")
        _require(record.get("schema_version") == 1, "collector raw schema drift")
        event_id = _uuid4(record.get("event_id"), "event_id")
        device_id = _uuid4(record.get("device_id"), "device_id")
        _require(event_id not in event_ids, "duplicate event object in smoke input")
        event_ids.add(event_id)
        received = _parse_utc(record.get("received_at"), "received_at")
        _parse_utc(record.get("occurred_at"), "occurred_at")
        _require(record.get("event_date") == received.date().isoformat(), "event date drift")
        _require(record.get("collector_version") == COLLECTOR_VERSION, "collector version drift")
        _require(record.get("risk_result") == "accepted", "collector risk result drift")
        _require(record.get("consent_state") == "analytics_granted", "collector consent drift")
        _require(record.get("experiment_id") == EXPERIMENT_ID, "collector experiment drift")
        variation = record.get("variation_id")
        _require(variation in VARIATIONS, "collector variation drift")

        if not start <= received < through:
            continue
        raw_event_count += 1
        if event_name != "experiment_exposure":
            continue
        target_exposure_count += 1
        variation_counts[variation] += 1
        exposure_counts_by_device[device_id] += 1
        exposure_variations_by_device[device_id].add(variation)
        if record.get("page_type") == "product":
            product_exposure_count += 1

    repeat_devices = {
        device_id for device_id, count in exposure_counts_by_device.items() if count >= 2
    }
    inconsistent_devices = {
        device_id
        for device_id in repeat_devices
        if len(exposure_variations_by_device[device_id]) != 1
    }
    return {
        "schema_version": 1,
        "component_type": "vevo_growthbook_production_aa_activation_smoke_reduction",
        "experiment_id": EXPERIMENT_ID,
        "from_utc": from_utc,
        "through_utc": through_utc,
        "raw_event_count": raw_event_count,
        "target_exposure_count": target_exposure_count,
        "unique_exposed_device_count": len(exposure_counts_by_device),
        "repeat_exposed_device_count": len(repeat_devices),
        "sticky_consistent_repeat_device_count": len(repeat_devices - inconsistent_devices),
        "sticky_inconsistent_device_count": len(inconsistent_devices),
        "variation_exposure_counts": {
            variation: variation_counts[variation] for variation in VARIATIONS
        },
        "observed_variations": [
            variation for variation in VARIATIONS if variation_counts[variation] > 0
        ],
        "product_exposure_count": product_exposure_count,
        "contains_raw_events": False,
        "contains_event_or_device_ids": False,
        "contains_customer_or_order_data": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-directory", required=True, type=Path)
    parser.add_argument("--from-utc", required=True)
    parser.add_argument("--through-utc", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = summarize(args.raw_directory, args.from_utc, args.through_utc)
    args.output.write_bytes(_canonical_bytes(result))
    print(
        "PRODUCTION_AA_SMOKE_REDUCTION_OK:"
        f"events={result['raw_event_count']}:"
        f"exposures={result['target_exposure_count']}:"
        f"repeat-devices={result['repeat_exposed_device_count']}:"
        f"sticky-conflicts={result['sticky_inconsistent_device_count']}:"
        "identities=false"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
