#!/usr/bin/env python3
"""Summarize a Production foundation bucket listing without exposing object keys."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping


class FoundationBucketSummaryError(ValueError):
    """Raised when an S3 listing is malformed or ambiguous."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise FoundationBucketSummaryError(message)


def summarize_bucket_listing(payload: Mapping[str, Any]) -> dict[str, int]:
    """Return only safe class counts; never return or print object keys."""

    contents = payload.get("Contents") or []
    key_count = payload.get("KeyCount")
    _require(type(key_count) is int and key_count >= 0, "bucket key count drift")
    _require(isinstance(contents, list), "bucket contents shape drift")
    _require(key_count == len(contents), "bucket listing count mismatch")
    _require(payload.get("IsTruncated") is False, "bucket listing is truncated")

    classes: Counter[str] = Counter()
    for row in contents:
        _require(isinstance(row, dict), "bucket object shape drift")
        key = row.get("Key")
        _require(isinstance(key, str) and key != "", "bucket object key drift")
        if key.startswith("experiment-events/raw/") and key != "experiment-events/raw/":
            classes["raw_events"] += 1
        elif key.startswith("athena-results/") and key != "athena-results/":
            classes["athena_results"] += 1
        else:
            classes["unexpected"] += 1

    result = {
        "total": key_count,
        "raw_events": classes["raw_events"],
        "athena_results": classes["athena_results"],
        "unexpected": classes["unexpected"],
    }
    _require(
        sum(value for key, value in result.items() if key != "total") == key_count,
        "bucket summary parity drift",
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--listing", required=True, type=Path)
    args = parser.parse_args()
    payload = json.loads(args.listing.read_text(encoding="utf-8"))
    result = summarize_bucket_listing(payload)
    print(
        "FOUNDATION_BUCKET_DIAGNOSTIC:"
        f"count={result['total']}:raw-events={result['raw_events']}:"
        f"athena-results={result['athena_results']}:"
        f"unexpected={result['unexpected']}:keys=false:content=false"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
