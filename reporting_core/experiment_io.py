#!/usr/bin/env python3
"""Bounded, fail-closed I/O for anonymous experiment events.

The loader deliberately lists one exact server-receipt date partition at a
time.  It never scans the bucket root and it never logs or returns a partially
decoded object after a validation failure.
"""

from __future__ import annotations

import json
import re
from datetime import date, timedelta
from typing import Any, Dict, List, Mapping

from .experiments import ExperimentDataError


_PREFIX_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{2,199}$")
_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")


def _validated_prefix(value: str) -> str:
    normalized = str(value or "").strip(" /")
    if not _PREFIX_RE.fullmatch(normalized) or ".." in normalized or "//" in normalized:
        raise ExperimentDataError("invalid raw event prefix")
    return normalized


def _validated_date(value: Any, field: str) -> date:
    if type(value) is not date:
        raise ExperimentDataError(f"{field} must be a date")
    return value


def _read_bounded_body(body: Any, max_object_bytes: int) -> bytes:
    if not hasattr(body, "read"):
        raise ExperimentDataError("raw event object body is unreadable")
    payload = body.read(max_object_bytes + 1)
    if not isinstance(payload, (bytes, bytearray)) or not payload:
        raise ExperimentDataError("raw event object body is empty")
    if len(payload) > max_object_bytes:
        raise ExperimentDataError("raw event object exceeds the size limit")
    return bytes(payload)


def load_raw_experiment_events(
    s3: Any,
    *,
    bucket: str,
    start_date: date,
    end_date: date,
    prefix: str = "experiment-events/raw",
    max_window_days: int = 90,
    max_objects: int = 100_000,
    max_object_bytes: int = 16_384,
) -> List[Dict[str, Any]]:
    """Load JSON objects from exact raw event date partitions.

    Schema and PII validation remains the responsibility of
    :func:`reporting_core.experiments.build_experiment_facts`.  This function
    enforces only the storage boundary and JSON-object shape so callers cannot
    accidentally perform an unbounded S3 scan.
    """

    if not isinstance(bucket, str) or not _BUCKET_RE.fullmatch(bucket.strip()):
        raise ExperimentDataError("invalid raw event bucket")
    start = _validated_date(start_date, "start_date")
    end = _validated_date(end_date, "end_date")
    if end < start:
        raise ExperimentDataError("end_date precedes start_date")
    window_days = (end - start).days + 1
    if (
        type(max_window_days) is not int
        or not 1 <= max_window_days <= 366
        or window_days > max_window_days
    ):
        raise ExperimentDataError("raw event window exceeds the configured limit")
    if type(max_objects) is not int or not 1 <= max_objects <= 1_000_000:
        raise ExperimentDataError("invalid raw event object limit")
    if type(max_object_bytes) is not int or not 1_024 <= max_object_bytes <= 1_048_576:
        raise ExperimentDataError("invalid raw event object size limit")

    normalized_prefix = _validated_prefix(prefix)
    rows: List[Dict[str, Any]] = []
    current = start
    while current <= end:
        partition_prefix = f"{normalized_prefix}/event_date={current.isoformat()}/"
        continuation_token = None
        while True:
            request: Dict[str, Any] = {
                "Bucket": bucket.strip(),
                "Prefix": partition_prefix,
                # Ask for one sentinel object beyond the remaining allowance
                # so an exact-limit window can still prove whether it overflows.
                "MaxKeys": min(1_000, max_objects - len(rows) + 1),
            }
            if continuation_token:
                request["ContinuationToken"] = continuation_token
            response = s3.list_objects_v2(**request)
            if not isinstance(response, Mapping):
                raise ExperimentDataError("raw event listing returned an invalid response")

            objects = response.get("Contents") or []
            if not isinstance(objects, list):
                raise ExperimentDataError("raw event listing contains invalid objects")
            for candidate in objects:
                if not isinstance(candidate, Mapping):
                    raise ExperimentDataError("raw event listing contains an invalid object")
                key = candidate.get("Key")
                size = candidate.get("Size")
                if (
                    not isinstance(key, str)
                    or not key.startswith(partition_prefix)
                    or not key.endswith(".json")
                    or "/" in key[len(partition_prefix) :]
                ):
                    raise ExperimentDataError("raw event listing escaped the exact partition")
                if type(size) is not int or not 1 <= size <= max_object_bytes:
                    raise ExperimentDataError("raw event object has an invalid size")
                if len(rows) >= max_objects:
                    raise ExperimentDataError("raw event object limit exceeded")

                object_response = s3.get_object(Bucket=bucket.strip(), Key=key)
                if not isinstance(object_response, Mapping):
                    raise ExperimentDataError("raw event read returned an invalid response")
                declared_length = object_response.get("ContentLength")
                if declared_length is not None and (
                    type(declared_length) is not int
                    or declared_length != size
                    or declared_length > max_object_bytes
                ):
                    raise ExperimentDataError("raw event object length does not reconcile")
                raw_body = _read_bounded_body(object_response.get("Body"), max_object_bytes)
                try:
                    decoded = json.loads(raw_body.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise ExperimentDataError("raw event object is not valid UTF-8 JSON") from exc
                if not isinstance(decoded, dict):
                    raise ExperimentDataError("raw event object must contain one JSON object")
                rows.append(decoded)

            truncated = response.get("IsTruncated", False)
            if type(truncated) is not bool:
                raise ExperimentDataError("raw event listing has an invalid pagination flag")
            if not truncated:
                break
            next_token = response.get("NextContinuationToken")
            if not isinstance(next_token, str) or not next_token or next_token == continuation_token:
                raise ExperimentDataError("raw event listing has an invalid continuation token")
            if len(rows) >= max_objects:
                raise ExperimentDataError("raw event object limit exceeded")
            continuation_token = next_token
        current += timedelta(days=1)

    return rows
