#!/usr/bin/env python3
"""Prepare and build one identity-free VEVO CTA safety checkpoint.

The module is deterministic and has no network or external mutation client.
GitHub Actions supplies only read-only Athena output and two GET response bodies.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import re
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

try:
    from scripts.evaluate_growthbook_cta_safety import (
        MONITORING,
        canonical_json_bytes,
        evaluate,
        validate_contract,
    )
except ModuleNotFoundError:  # pragma: no cover - direct execution from scripts/
    from evaluate_growthbook_cta_safety import (  # type: ignore
        MONITORING,
        canonical_json_bytes,
        evaluate,
        validate_contract,
    )


ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = ROOT / "projects" / "vevo" / "growthbook_cta_safety_monitoring.json"
SQL_PATH = ROOT / "projects" / "vevo" / "growthbook_sql" / "cta_safety_checkpoint_production.sql"
WORKFLOW = ".github/workflows/check-vevo-growthbook-production-cta-safety.yml"
ARTIFACT_NAME = "vevo-growthbook-cta-safety-checkpoint"
EVIDENCE_FILE = "vevo-growthbook-cta-safety-evidence.json"
DECISION_FILE = "vevo-growthbook-cta-safety-decision.json"
PROVENANCE_FILE = "vevo-growthbook-cta-safety-provenance.json"
RESULT_COLUMNS = [
    "variation_id",
    "eligible_devices",
    "measured_page_loads",
    "client_error_devices",
    "lcp_p75_ms",
    "inp_p75_ms",
    "cls_p75_milli",
    "duplicate_device_fact_rows",
    "conflicting_assignment_devices",
]
VARIATIONS = ("control", "brand_contrast")


class SafetyCheckpointBuildError(ValueError):
    """Raised when a safety checkpoint cannot be built safely."""


class SafetyCheckpointSkip(Exception):
    """Raised when a scheduled checkpoint is not due and AWS must stay closed."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise SafetyCheckpointBuildError(message)


def _load(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SafetyCheckpointBuildError(f"unreadable JSON: {path}") from exc
    _require(isinstance(value, dict), f"JSON root must be an object: {path}")
    return value


def _timestamp(value: str, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise SafetyCheckpointBuildError(f"{label} is invalid") from exc
    _require(parsed.tzinfo is not None, f"{label} must include a timezone")
    return parsed.astimezone(UTC)


def checkpoint_gate(manifest: Mapping[str, Any], now: datetime) -> dict[str, Any]:
    """Return the exact due checkpoint or skip without opening AWS."""

    validate_contract(manifest)
    if manifest["status"] != MONITORING:
        raise SafetyCheckpointSkip("monitoring-not-open")
    for field in (
        "safety_checkpoint_collection_allowed",
        "safety_checkpoint_recording_allowed",
        "protected_safety_collection_workflow_allowed",
    ):
        _require(
            manifest["release_boundaries"][field] is True,
            f"CTA safety gate closed: {field}",
        )
    _require(now.tzinfo is not None, "current time must include a timezone")
    now = now.astimezone(UTC)
    started = _timestamp(manifest["assignment_started_at_utc"], "assignment start")
    policy = manifest["checkpoint_policy"]
    first_due = started + timedelta(hours=policy["first_checkpoint_after_start_hours"])
    if now < first_due:
        raise SafetyCheckpointSkip("before-first-due")
    checkpoint_index = (
        int((now - first_due).total_seconds() // (policy["cadence_hours"] * 3600))
        + 1
    )
    due_at = first_due + timedelta(
        hours=(checkpoint_index - 1) * policy["cadence_hours"]
    )
    if now > due_at + timedelta(minutes=policy["maximum_checkpoint_lateness_minutes"]):
        raise SafetyCheckpointSkip("outside-exact-due-window")
    latest = manifest["latest_checkpoint"]
    if latest["status"] == "recorded" and latest["checkpoint_index"] >= checkpoint_index:
        raise SafetyCheckpointSkip("already-recorded")
    return {
        "checkpoint_index": checkpoint_index,
        "assignment_started_at_utc": started.isoformat(timespec="seconds").replace(
            "+00:00", "Z"
        ),
        "checkpoint_through_utc": due_at.isoformat(timespec="seconds").replace(
            "+00:00", "Z"
        ),
        "checkpoint_due_utc": due_at.isoformat(timespec="seconds").replace(
            "+00:00", "Z"
        ),
    }


def render_query(
    manifest: Mapping[str, Any], *, started_at_utc: str, through_utc: str
) -> str:
    binding = manifest["source_bindings"]["safety_query"]
    source = SQL_PATH.read_bytes()
    _require(
        binding["path"]
        == "projects/vevo/growthbook_sql/cta_safety_checkpoint_production.sql",
        "CTA safety SQL path drift",
    )
    _require(
        hashlib.sha256(source).hexdigest() == binding["sha256"],
        "CTA safety SQL hash mismatch",
    )
    query = source.decode("utf-8")
    _require(query.count("__CTA_STARTED_AT_UTC__") >= 1, "CTA start token drift")
    _require(
        query.count("__CHECKPOINT_THROUGH_UTC__") >= 1,
        "checkpoint through token drift",
    )
    for value, label in (
        (started_at_utc, "CTA start"),
        (through_utc, "checkpoint through"),
    ):
        _timestamp(value, label)
    query = query.replace("__CTA_STARTED_AT_UTC__", started_at_utc)
    query = query.replace("__CHECKPOINT_THROUGH_UTC__", through_utc)
    _require("__CTA_" not in query and "__CHECKPOINT_" not in query, "SQL token remains")
    return query.rstrip() + "\n"


def _cell(row: Mapping[str, Any], index: int) -> str | None:
    data = row.get("Data") or []
    _require(isinstance(data, list) and len(data) == len(RESULT_COLUMNS), "Athena row width drift")
    cell = data[index]
    _require(isinstance(cell, Mapping), "Athena cell drift")
    value = cell.get("VarCharValue")
    _require(value is None or isinstance(value, str), "Athena cell value drift")
    return value


def _integer(value: str | None, label: str) -> int:
    _require(value is not None and value.isdigit(), f"{label} must be an integer")
    return int(value)


def _metric(value: str | None, label: str) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except ValueError as exc:
        raise SafetyCheckpointBuildError(f"{label} must be numeric") from exc
    _require(number >= 0, f"{label} must be non-negative")
    return number


def parse_athena_result(payload: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, bool]]:
    """Reduce the bounded Athena response to two aggregate safety rows."""

    empty = {
        variation: {
            "eligible_devices": 0,
            "measured_page_loads": 0,
            "client_error_devices": 0,
            "lcp_p75_ms": None,
            "inp_p75_ms": None,
            "cls_p75_milli": None,
        }
        for variation in VARIATIONS
    }
    if payload.get("query_failed") is True:
        return empty, {
            "query_complete": False,
            "exact_two_variations": False,
            "assignment_source_match": True,
            "duplicate_or_conflicting_assignment_detected": False,
        }
    result_set = payload.get("ResultSet") or {}
    rows = result_set.get("Rows") or []
    _require(isinstance(rows, list) and rows, "Athena result rows missing")
    header = [_cell(rows[0], index) for index in range(len(RESULT_COLUMNS))]
    _require(header == RESULT_COLUMNS, "Athena result header drift")
    query_complete = payload.get("NextToken") is None and len(rows) <= 3
    parsed: dict[str, Any] = {}
    duplicate_counts: set[int] = set()
    conflict_counts: set[int] = set()
    unknown_variation = False
    for row in rows[1:]:
        variation = _cell(row, 0)
        if variation not in VARIATIONS or variation in parsed:
            unknown_variation = True
            continue
        parsed[variation] = {
            "eligible_devices": _integer(_cell(row, 1), f"{variation}.eligible_devices"),
            "measured_page_loads": _integer(_cell(row, 2), f"{variation}.measured_page_loads"),
            "client_error_devices": _integer(_cell(row, 3), f"{variation}.client_error_devices"),
            "lcp_p75_ms": _metric(_cell(row, 4), f"{variation}.lcp_p75_ms"),
            "inp_p75_ms": _metric(_cell(row, 5), f"{variation}.inp_p75_ms"),
            "cls_p75_milli": _metric(_cell(row, 6), f"{variation}.cls_p75_milli"),
        }
        duplicate_counts.add(_integer(_cell(row, 7), "duplicate rows"))
        conflict_counts.add(_integer(_cell(row, 8), "conflicting assignments"))
    health = {variation: parsed.get(variation, empty[variation]) for variation in VARIATIONS}
    exact_two = set(parsed) == set(VARIATIONS) and not unknown_variation
    assignment_source_match = len(duplicate_counts) <= 1 and len(conflict_counts) <= 1
    duplicate_detected = (
        not assignment_source_match
        or any(value > 0 for value in duplicate_counts | conflict_counts)
    )
    return health, {
        "query_complete": query_complete,
        "exact_two_variations": exact_two,
        "assignment_source_match": assignment_source_match,
        "duplicate_or_conflicting_assignment_detected": duplicate_detected,
    }


def _plain_text(source: str) -> str:
    return " ".join(
        html.unescape(re.sub(r"<[^>]+>", " ", source)).replace("\xa0", " ").split()
    )


def commerce_readback(
    manifest: Mapping[str, Any],
    *,
    product_html: str,
    cart_html: str,
    product_fetch_succeeded: bool,
    cart_fetch_succeeded: bool,
) -> dict[str, bool]:
    baseline = manifest["commerce_probe"]
    product_plain = _plain_text(product_html)
    cta_match = re.search(
        r"<button[^>]*class=[\"'][^\"']*s1-submitCart[^\"']*[\"'][^>]*>(.*?)</button>",
        product_html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    price_match = re.search(
        r"class=[\"'][^\"']*priceTaxValueNumber[^\"']*[\"'][^>]*>(.*?)</",
        product_html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    actual_cta = _plain_text(cta_match.group(1)) if cta_match else ""
    actual_price = _plain_text(price_match.group(1)) if price_match else ""
    cart_rendered = "košík" in _plain_text(cart_html).lower()
    product_code_present = re.search(
        rf"K[oó]d produktu:\s*{re.escape(baseline['product_code'])}(?:\D|$)",
        product_plain,
        flags=re.IGNORECASE,
    ) is not None
    reproducible_error = (
        not product_fetch_succeeded
        or not cart_fetch_succeeded
        or not cart_rendered
    )
    return {
        "add_to_cart_text_unchanged": (
            product_fetch_succeeded
            and product_code_present
            and actual_cta == baseline["cta_text"]
        ),
        "price_unchanged": (
            product_fetch_succeeded and actual_price == baseline["price_text"]
        ),
        "cart_checkout_order_mutated": False,
        "reproducible_cart_or_checkout_runtime_error": reproducible_error,
    }


def build_bundle(
    manifest: Mapping[str, Any],
    athena_payload: Mapping[str, Any],
    *,
    checkpoint_index: int,
    assignment_started_at_utc: str,
    observed_at_utc: str,
    product_html: str,
    cart_html: str,
    product_fetch_succeeded: bool,
    cart_fetch_succeeded: bool,
    repository: str,
    workflow_run_id: str,
    main_commit: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    health, quality = parse_athena_result(athena_payload)
    evidence = {
        "schema_version": 1,
        "evidence_type": "vevo_growthbook_cta_safety_checkpoint",
        "experiment_id": "vevo-sk-product-cta-color-001",
        "checkpoint_index": checkpoint_index,
        "assignment_started_at_utc": assignment_started_at_utc,
        "observed_at_utc": observed_at_utc,
        "variation_health": health,
        "commerce_readback": commerce_readback(
            manifest,
            product_html=product_html,
            cart_html=cart_html,
            product_fetch_succeeded=product_fetch_succeeded,
            cart_fetch_succeeded=cart_fetch_succeeded,
        ),
        "data_quality": quality,
        "safety": {
            "contains_event_or_device_ids": False,
            "contains_customer_or_order_data": False,
            "primary_metric_read": False,
            "business_outcome_read": False,
            "meta_dimensions_read": False,
            "winner_call_made": False,
            "external_or_automatic_mutation": False,
        },
    }
    decision = evaluate(evidence, manifest)
    evidence_sha256 = hashlib.sha256(canonical_json_bytes(evidence)).hexdigest()
    decision_sha256 = hashlib.sha256(canonical_json_bytes(decision)).hexdigest()
    provenance = {
        "schema_version": 1,
        "provenance_type": "vevo_growthbook_cta_safety_checkpoint",
        "repository": repository,
        "workflow": WORKFLOW,
        "workflow_run_id": workflow_run_id,
        "main_commit": main_commit,
        "artifact_name": ARTIFACT_NAME,
        "files": {
            "evidence_file": EVIDENCE_FILE,
            "evidence_sha256": evidence_sha256,
            "decision_file": DECISION_FILE,
            "decision_sha256": decision_sha256,
            "provenance_file": PROVENANCE_FILE,
        },
        "safety": {
            "contains_event_or_device_ids": False,
            "contains_customer_or_order_data": False,
            "contains_primary_or_business_outcomes": False,
            "contains_meta_dimensions": False,
            "winner_call_made": False,
            "external_or_automatic_mutation": False,
        },
    }
    return evidence, decision, provenance


def _write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--manifest", type=Path, default=MANIFEST_PATH)
    prepare.add_argument("--event-name", required=True)
    prepare.add_argument("--github-env", required=True, type=Path)
    prepare.add_argument("--runner-temp", required=True, type=Path)
    prepare.add_argument("--run-id", required=True)
    prepare.add_argument("--now-utc")
    render = commands.add_parser("render-query")
    render.add_argument("--manifest", type=Path, default=MANIFEST_PATH)
    render.add_argument("--started-at-utc", required=True)
    render.add_argument("--through-utc", required=True)
    render.add_argument("--output", required=True, type=Path)
    assemble = commands.add_parser("assemble")
    assemble.add_argument("--manifest", type=Path, default=MANIFEST_PATH)
    assemble.add_argument("--athena-result", required=True, type=Path)
    assemble.add_argument("--product-html", required=True, type=Path)
    assemble.add_argument("--cart-html", required=True, type=Path)
    assemble.add_argument("--product-fetch-succeeded", required=True)
    assemble.add_argument("--cart-fetch-succeeded", required=True)
    assemble.add_argument("--checkpoint-index", required=True, type=int)
    assemble.add_argument("--assignment-started-at-utc", required=True)
    assemble.add_argument("--observed-at-utc", required=True)
    assemble.add_argument("--repository", required=True)
    assemble.add_argument("--workflow-run-id", required=True)
    assemble.add_argument("--main-commit", required=True)
    assemble.add_argument("--output-directory", type=Path, default=Path("."))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        manifest = _load(args.manifest)
        if args.command == "prepare":
            now = (
                _timestamp(args.now_utc, "now")
                if args.now_utc
                else datetime.now(UTC)
            )
            try:
                gate = checkpoint_gate(manifest, now)
            except SafetyCheckpointSkip as exc:
                with args.github_env.open("a", encoding="utf-8") as handle:
                    handle.write("RUN_CHECKPOINT=false\n")
                if args.event_name == "schedule":
                    print(f"PRODUCTION_CTA_SAFETY_SCHEDULE_SKIP:reason={exc}:aws=false")
                    return 0
                raise SafetyCheckpointBuildError(f"CTA safety checkpoint skipped: {exc}") from exc
            temporary = args.runner_temp / f"vevo-cta-safety-{args.run_id}"
            temporary.mkdir(mode=0o700)
            with args.github_env.open("a", encoding="utf-8") as handle:
                handle.write("RUN_CHECKPOINT=true\n")
                handle.write(f"CHECKPOINT_INDEX={gate['checkpoint_index']}\n")
                handle.write(
                    f"CTA_STARTED_AT_UTC={gate['assignment_started_at_utc']}\n"
                )
                handle.write(
                    f"CHECKPOINT_THROUGH_UTC={gate['checkpoint_through_utc']}\n"
                )
                handle.write(f"CHECKPOINT_DUE_UTC={gate['checkpoint_due_utc']}\n")
                handle.write(f"TEMP_CHECKPOINT_DIR={temporary}\n")
            print(
                "PRODUCTION_CTA_SAFETY_LOCAL_GATE_OK:assignment=running:"
                "primary=false:business=false:meta=false:winner=false:mutation=none"
            )
        elif args.command == "render-query":
            _write(
                args.output,
                render_query(
                    manifest,
                    started_at_utc=args.started_at_utc,
                    through_utc=args.through_utc,
                ).encode("utf-8"),
            )
            print("PRODUCTION_CTA_SAFETY_QUERY_OK:aggregate=true:outcomes=false")
        else:
            observed = _timestamp(args.observed_at_utc, "observed at")
            evidence, decision, provenance = build_bundle(
                manifest,
                _load(args.athena_result),
                checkpoint_index=args.checkpoint_index,
                assignment_started_at_utc=args.assignment_started_at_utc,
                observed_at_utc=observed.isoformat(timespec="seconds").replace(
                    "+00:00", "Z"
                ),
                product_html=args.product_html.read_text(
                    encoding="utf-8", errors="replace"
                ),
                cart_html=args.cart_html.read_text(encoding="utf-8", errors="replace"),
                product_fetch_succeeded=args.product_fetch_succeeded == "true",
                cart_fetch_succeeded=args.cart_fetch_succeeded == "true",
                repository=args.repository,
                workflow_run_id=args.workflow_run_id,
                main_commit=args.main_commit,
            )
            _write(
                args.output_directory / EVIDENCE_FILE,
                canonical_json_bytes(evidence),
            )
            _write(
                args.output_directory / DECISION_FILE,
                canonical_json_bytes(decision),
            )
            _write(
                args.output_directory / PROVENANCE_FILE,
                canonical_json_bytes(provenance),
            )
            print(
                f"PRODUCTION_CTA_SAFETY_BUNDLE_OK:verdict={decision['verdict']}:"
                "canonical=true:identity=false:outcomes=false:winner=false:mutation=none"
            )
    except (OSError, SafetyCheckpointBuildError, ValueError) as exc:
        print(f"build_growthbook_cta_safety_checkpoint.py: FAIL: {exc}")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
