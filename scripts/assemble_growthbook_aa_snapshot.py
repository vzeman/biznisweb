#!/usr/bin/env python3
"""Assemble a VEVO Production A/A snapshot from two sanitized evidence files.

The automated component is produced from bounded Production infrastructure and
analytics reads. The manual component is produced by the separately protected
GrowthBook/Tag Assistant/commerce QA run. Both inputs must be canonical JSON,
bound to independently supplied GitHub run/commit identities and SHA-256
digests, and contain only aggregate fields. This script is deliberately
offline and cannot start traffic, query a service, or mutate any control plane.
"""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any, Mapping, Sequence

from scripts.evaluate_growthbook_aa import AaEvaluationError, evaluate, load_config


SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
RUN_ID_RE = re.compile(r"^[1-9][0-9]{5,19}$")
UTC_RE = re.compile(r"^20[2-9][0-9]-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")
TASK_ID_RE = re.compile(r"^[0-9a-f]{32}$")

AUTOMATED_KEYS = {
    "schema_version",
    "evidence_type",
    "experiment_id",
    "from_utc",
    "through_utc",
    "source_run_id",
    "source_main_commit",
    "production_runtime",
    "pipeline_counts",
    "reporting_quality",
    "meta_dimension_audit",
    "privacy_audit",
    "consent_audit",
    "source_read_only",
    "contains_raw_aws_payloads",
    "contains_cloudwatch_messages",
    "contains_event_or_device_ids",
    "contains_customer_or_order_data",
    "mutation_observed",
}
AUTOMATED_RUNTIME_KEYS = {
    "instance_id",
    "private_ip",
    "service",
    "path",
    "task_id",
    "image_digest",
    "stack_name",
    "database",
}
MANUAL_KEYS = {
    "schema_version",
    "evidence_type",
    "experiment_id",
    "from_utc",
    "through_utc",
    "source_run_id",
    "source_main_commit",
    "production_allocation_percent",
    "identical_variations_verified",
    "growthbook_srm_warning",
    "growthbook_variation_counts",
    "commerce_health",
    "qa_checklist",
    "tag_assistant_connected",
    "production_storefront_observed",
    "growthbook_read_only",
    "contains_event_or_device_ids",
    "contains_customer_or_order_data",
    "unplanned_mutation_observed",
}
PIPELINE_KEYS = {
    "collector_received_event_count",
    "collector_unique_accepted_event_count",
    "collector_duplicate_event_count",
    "athena_unique_event_count",
    "reporting_unique_event_count",
}
REPORTING_KEYS = {
    "raw_event_count",
    "unique_event_count",
    "duplicate_event_count",
    "orphan_event_count",
    "eligible_device_count",
    "contaminated_device_count",
    "srm_p_value",
    "unique_transaction_count",
    "exact_joined_transaction_count",
    "unmatched_transaction_count",
    "ambiguous_transaction_count",
    "variation_health",
}
VARIATION_HEALTH_KEYS = {
    "eligible_devices",
    "measured_page_loads",
    "lcp_p75_ms",
    "inp_p75_ms",
    "cls_p75_milli",
    "client_error_device_rate_pct",
}
META_KEYS = {
    "meta_exposure_count",
    "complete_stable_dimension_exposure_count",
    "invalid_dimension_row_count",
    "forbidden_click_identifier_count",
}
PRIVACY_KEYS = {
    "total_stored_row_count",
    "sampled_row_count",
    "pii_finding_count",
    "forbidden_field_finding_count",
    "raw_ip_address_stored_count",
    "full_url_stored_count",
    "click_identifier_stored_count",
    "customer_field_stored_count",
}
CONSENT_KEYS = {
    "pre_consent_request_count",
    "non_analytical_consent_exposure_count",
    "post_withdrawal_event_count",
}
COMMERCE_KEYS = {
    "checkout_runtime_error_count",
    "duplicate_ga4_purchase_event_count",
    "duplicate_meta_purchase_event_count",
    "price_cart_checkout_mutation_observed",
    "add_to_cart_behavior_regression_observed",
    "rollback_test_passed",
}
QA_KEYS = {
    "desktop_passed",
    "mobile_passed",
    "consent_accept_passed",
    "consent_reject_passed",
    "consent_withdrawal_passed",
}
VARIATIONS = {"control", "variant"}


class SnapshotAssemblyError(ValueError):
    """Raised when component evidence cannot support a trusted snapshot."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise SnapshotAssemblyError(message)


def _exact(value: Any, keys: set[str], field: str) -> Mapping[str, Any]:
    _require(isinstance(value, dict), f"{field} must be an object")
    _require(set(value) == keys, f"{field} field set drift")
    return value


def _integer(value: Any, field: str, *, maximum: int | None = None) -> int:
    _require(type(value) is int and value >= 0, f"{field} must be a non-negative integer")
    if maximum is not None:
        _require(value <= maximum, f"{field} exceeds its allowed maximum")
    return value


def _number(value: Any, field: str, *, nullable: bool = False) -> float | None:
    if value is None and nullable:
        return None
    _require(type(value) in (int, float), f"{field} must be numeric")
    result = float(value)
    _require(result == result and result not in (float("inf"), float("-inf")), f"{field} must be finite")
    _require(result >= 0, f"{field} must be non-negative")
    return result


def _boolean(value: Any, field: str) -> bool:
    _require(type(value) is bool, f"{field} must be boolean")
    return value


def _canonical_json(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def _load_canonical(path: Path, expected_sha256: str, field: str) -> dict[str, Any]:
    _require(SHA256_RE.fullmatch(expected_sha256) is not None, f"{field} SHA-256 is invalid")
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SnapshotAssemblyError(f"{field} evidence is unreadable") from exc
    _require(isinstance(payload, dict), f"{field} evidence must contain an object")
    _require(raw == _canonical_json(payload), f"{field} evidence must use canonical JSON bytes")
    _require(hashlib.sha256(raw).hexdigest() == expected_sha256, f"{field} SHA-256 mismatch")
    return payload


def _validate_provenance(
    payload: Mapping[str, Any], *, expected_run_id: str, expected_commit: str, field: str
) -> None:
    _require(RUN_ID_RE.fullmatch(expected_run_id) is not None, f"{field} expected run ID is invalid")
    _require(COMMIT_RE.fullmatch(expected_commit) is not None, f"{field} expected commit is invalid")
    _require(
        isinstance(payload["source_run_id"], str)
        and RUN_ID_RE.fullmatch(payload["source_run_id"]) is not None,
        f"{field} source run ID is invalid",
    )
    _require(
        isinstance(payload["source_main_commit"], str)
        and COMMIT_RE.fullmatch(payload["source_main_commit"]) is not None,
        f"{field} source main commit is invalid",
    )
    _require(payload["source_run_id"] == expected_run_id, f"{field} run ID mismatch")
    _require(payload["source_main_commit"] == expected_commit, f"{field} main commit mismatch")


def _validate_window(payload: Mapping[str, Any], field: str) -> None:
    _require(UTC_RE.fullmatch(str(payload["from_utc"])) is not None, f"{field} from_utc is invalid")
    _require(UTC_RE.fullmatch(str(payload["through_utc"])) is not None, f"{field} through_utc is invalid")
    _require(payload["through_utc"] > payload["from_utc"], f"{field} window is empty")


def _validate_nested_automated(payload: Mapping[str, Any]) -> None:
    runtime = _exact(payload["production_runtime"], AUTOMATED_RUNTIME_KEYS, "production_runtime")
    _require(runtime["instance_id"] == "N/A:Fargate", "Production runtime instance ID drift")
    try:
        private_ip = ipaddress.ip_address(str(runtime["private_ip"]))
    except ValueError as exc:
        raise SnapshotAssemblyError("Production runtime IP is invalid") from exc
    _require(
        private_ip.version == 4
        and private_ip.is_private
        and str(private_ip).startswith("172.31."),
        "Production runtime IP is outside the VEVO private VPC",
    )
    _require(runtime["service"] == "vevo-growthbook-collector-production", "Production service drift")
    _require(runtime["path"] == "/app", "Production runtime path drift")
    _require(TASK_ID_RE.fullmatch(str(runtime["task_id"])) is not None, "Production task ID drift")
    _require(
        re.fullmatch(r"sha256:[0-9a-f]{64}", str(runtime["image_digest"])) is not None,
        "Production image digest drift",
    )
    _require(runtime["stack_name"] == "vevo-growthbook-production", "Production stack drift")
    _require(runtime["database"] == "vevo_growthbook_production", "Production database drift")

    pipeline = _exact(payload["pipeline_counts"], PIPELINE_KEYS, "pipeline_counts")
    for key in PIPELINE_KEYS:
        _integer(pipeline[key], f"pipeline_counts.{key}")
    _require(
        pipeline["collector_received_event_count"]
        == pipeline["collector_unique_accepted_event_count"]
        + pipeline["collector_duplicate_event_count"],
        "collector receipt identity drift",
    )

    reporting = _exact(payload["reporting_quality"], REPORTING_KEYS, "reporting_quality")
    for key in REPORTING_KEYS - {"srm_p_value", "variation_health"}:
        _integer(reporting[key], f"reporting_quality.{key}")
    _number(reporting["srm_p_value"], "reporting_quality.srm_p_value")
    health = _exact(reporting["variation_health"], VARIATIONS, "variation_health")
    for variation in sorted(VARIATIONS):
        row = _exact(health[variation], VARIATION_HEALTH_KEYS, f"variation_health.{variation}")
        _integer(row["eligible_devices"], f"variation_health.{variation}.eligible_devices")
        _integer(row["measured_page_loads"], f"variation_health.{variation}.measured_page_loads")
        for key in ("lcp_p75_ms", "inp_p75_ms", "cls_p75_milli", "client_error_device_rate_pct"):
            _number(row[key], f"variation_health.{variation}.{key}", nullable=True)

    for group, keys in (
        ("meta_dimension_audit", META_KEYS),
        ("privacy_audit", PRIVACY_KEYS),
        ("consent_audit", CONSENT_KEYS),
    ):
        rows = _exact(payload[group], keys, group)
        for key in keys:
            _integer(rows[key], f"{group}.{key}")


def _validate_manual(payload: Mapping[str, Any]) -> None:
    _integer(payload["production_allocation_percent"], "production_allocation_percent", maximum=100)
    _boolean(payload["identical_variations_verified"], "identical_variations_verified")
    _boolean(payload["growthbook_srm_warning"], "growthbook_srm_warning")
    counts = _exact(payload["growthbook_variation_counts"], VARIATIONS, "growthbook_variation_counts")
    for variation in VARIATIONS:
        _integer(counts[variation], f"growthbook_variation_counts.{variation}")
    commerce = _exact(payload["commerce_health"], COMMERCE_KEYS, "commerce_health")
    for key in (
        "checkout_runtime_error_count",
        "duplicate_ga4_purchase_event_count",
        "duplicate_meta_purchase_event_count",
    ):
        _integer(commerce[key], f"commerce_health.{key}")
    for key in COMMERCE_KEYS - {
        "checkout_runtime_error_count",
        "duplicate_ga4_purchase_event_count",
        "duplicate_meta_purchase_event_count",
    }:
        _boolean(commerce[key], f"commerce_health.{key}")
    qa = _exact(payload["qa_checklist"], QA_KEYS, "qa_checklist")
    for key in QA_KEYS:
        _boolean(qa[key], f"qa_checklist.{key}")


def assemble_snapshot(
    automated: Mapping[str, Any],
    manual: Mapping[str, Any],
    *,
    expected_automated_run_id: str,
    expected_automated_commit: str,
    expected_manual_run_id: str,
    expected_manual_commit: str,
    config: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate sanitized components and return the evaluator's exact schema."""

    automated = _exact(automated, AUTOMATED_KEYS, "automated evidence")
    manual = _exact(manual, MANUAL_KEYS, "manual evidence")
    _require(automated["schema_version"] == 1, "automated evidence schema drift")
    _require(manual["schema_version"] == 1, "manual evidence schema drift")
    _require(
        automated["evidence_type"] == "vevo_growthbook_aa_automated_evidence",
        "automated evidence type drift",
    )
    _require(
        manual["evidence_type"] == "vevo_growthbook_aa_manual_qa_evidence",
        "manual evidence type drift",
    )
    _require(automated["experiment_id"] == "vevo-sk-aa-001", "automated experiment drift")
    _require(manual["experiment_id"] == automated["experiment_id"], "component experiment mismatch")
    _validate_provenance(
        automated,
        expected_run_id=expected_automated_run_id,
        expected_commit=expected_automated_commit,
        field="automated",
    )
    _validate_provenance(
        manual,
        expected_run_id=expected_manual_run_id,
        expected_commit=expected_manual_commit,
        field="manual",
    )
    _validate_window(automated, "automated")
    _validate_window(manual, "manual")
    _require(
        (manual["from_utc"], manual["through_utc"])
        == (automated["from_utc"], automated["through_utc"]),
        "component window mismatch",
    )
    _validate_nested_automated(automated)
    _validate_manual(manual)
    _require(
        manual["production_allocation_percent"]
        == config["required_production_allocation_percent"],
        "manual Production allocation drift",
    )

    for field in (
        "source_read_only",
    ):
        _require(automated[field] is True, f"automated {field} must be true")
    for field in (
        "contains_raw_aws_payloads",
        "contains_cloudwatch_messages",
        "contains_event_or_device_ids",
        "contains_customer_or_order_data",
        "mutation_observed",
    ):
        _require(automated[field] is False, f"automated {field} must be false")
    for field in (
        "tag_assistant_connected",
        "production_storefront_observed",
        "growthbook_read_only",
    ):
        _require(manual[field] is True, f"manual {field} must be true")
    for field in (
        "contains_event_or_device_ids",
        "contains_customer_or_order_data",
        "unplanned_mutation_observed",
    ):
        _require(manual[field] is False, f"manual {field} must be false")

    snapshot = {
        "schema_version": 1,
        "experiment_id": automated["experiment_id"],
        "full_allocation_started_at_utc": automated["from_utc"],
        "evaluated_at_utc": automated["through_utc"],
        "production_allocation_percent": manual["production_allocation_percent"],
        "identical_variations_verified": manual["identical_variations_verified"],
        "growthbook_srm_warning": manual["growthbook_srm_warning"],
        "pipeline_counts": dict(automated["pipeline_counts"]),
        "growthbook_variation_counts": dict(manual["growthbook_variation_counts"]),
        "reporting_quality": dict(automated["reporting_quality"]),
        "meta_dimension_audit": dict(automated["meta_dimension_audit"]),
        "privacy_audit": dict(automated["privacy_audit"]),
        "consent_audit": dict(automated["consent_audit"]),
        "commerce_health": dict(manual["commerce_health"]),
        "qa_checklist": dict(manual["qa_checklist"]),
    }
    try:
        evaluate(snapshot, config)
    except AaEvaluationError as exc:
        raise SnapshotAssemblyError(f"assembled snapshot is invalid: {exc}") from exc
    return snapshot


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


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--automated", required=True, type=Path)
    parser.add_argument("--automated-sha256", required=True)
    parser.add_argument("--automated-run-id", required=True)
    parser.add_argument("--automated-main-commit", required=True)
    parser.add_argument("--manual", required=True, type=Path)
    parser.add_argument("--manual-sha256", required=True)
    parser.add_argument("--manual-run-id", required=True)
    parser.add_argument("--manual-main-commit", required=True)
    parser.add_argument("--output", required=True, type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        automated = _load_canonical(args.automated, args.automated_sha256, "automated")
        manual = _load_canonical(args.manual, args.manual_sha256, "manual")
        snapshot = assemble_snapshot(
            automated,
            manual,
            expected_automated_run_id=args.automated_run_id,
            expected_automated_commit=args.automated_main_commit,
            expected_manual_run_id=args.manual_run_id,
            expected_manual_commit=args.manual_main_commit,
            config=load_config(),
        )
        _write_atomic(args.output, snapshot)
    except (OSError, SnapshotAssemblyError, AaEvaluationError) as exc:
        print(f"VEVO_GROWTHBOOK_AA_SNAPSHOT_INVALID:{exc}")
        return 2
    print(
        "VEVO_GROWTHBOOK_AA_SNAPSHOT_ASSEMBLED:"
        f"experiment={snapshot['experiment_id']}:"
        f"from={snapshot['full_allocation_started_at_utc']}:"
        f"through={snapshot['evaluated_at_utc']}:winner=false"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
