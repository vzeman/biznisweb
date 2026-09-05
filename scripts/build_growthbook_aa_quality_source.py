"""Pure exact-window A/A quality calculation and canonical source validation.

No network, credentials, filesystem writes or publishing client is provided.
The future protected producer must prove complete source coverage and bind the
independently verified resolved snapshot/checkpoint before invoking this module.
It may export only the returned aggregate envelope, never the internal bundle.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from typing import Any, Iterable, Mapping

from reporting_core.experiments import (
    ExperimentBuildConfig,
    ExperimentDataError,
    ExperimentReceiptWindow,
    build_experiment_facts,
    order_completion_receipts,
)


WORKFLOW = ".github/workflows/collect-vevo-growthbook-production-aa-quality-source.yml"
EXPERIMENT_ID = "vevo-sk-aa-001"
CONTRACT = "vevo_cm1_v1_2026-08-20"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
RUN_RE = re.compile(r"^[1-9][0-9]{5,19}$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
TOP_KEYS = {
    "schema_version", "evidence_type", "repository", "experiment_id",
    "window", "provenance", "quality", "safety",
}
PROVENANCE_KEYS = {
    "workflow", "workflow_run_id", "main_commit", "generated_at_utc",
    "snapshot_manifest_sha256", "checkpoint_evidence_sha256",
    "raw_extract_sha256", "authoritative_orders_sha256",
}
SAFETY = {
    "source_read_only": True,
    "contains_event_or_device_ids": False,
    "contains_customer_or_order_data": False,
    "contains_raw_aws_payloads": False,
    "ordinary_curated_publish_allowed": False,
    "winner_calls_allowed": False,
}


class QualitySourceError(ValueError):
    """A source cannot prove the exact resolved A/A quality contract."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise QualitySourceError(message)


def canonical_source_bytes(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True,
                   separators=(",", ":")) + "\n"
    ).encode("utf-8")


def _bounded_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for row in rows:
        _require(len(result) < 100_000, "source input exceeds the row limit")
        _require(isinstance(row, Mapping), "source input row is not an object")
        result.append(dict(row))
    return result


def _input_digest(rows: list[dict[str, Any]]) -> str:
    # Sort per-row digests so input order is irrelevant; retain duplicate rows.
    # Only this whole-extract digest may leave the approved source boundary.
    row_hashes = sorted(hashlib.sha256(canonical_source_bytes(row)).digest() for row in rows)
    return hashlib.sha256(b"vevo-aa-quality-input-v1\n" + b"".join(row_hashes)).hexdigest()


def validate_quality_source_bytes(
    raw: bytes, *, expected_sha256: str, **expected_provenance: Any
) -> dict[str, Any]:
    """Verify independently downloaded bytes without retaining any input file."""
    _require(isinstance(raw, bytes) and 0 < len(raw) <= 1_048_576,
             "quality source byte size is invalid")
    _require(isinstance(expected_sha256, str) and SHA256_RE.fullmatch(expected_sha256) is not None
             and hashlib.sha256(raw).hexdigest() == expected_sha256,
             "quality source SHA-256 mismatch")
    try:
        source = json.loads(raw.decode("utf-8"))
        _require(isinstance(source, dict) and canonical_source_bytes(source) == raw,
                 "quality source must use canonical JSON bytes")
    except (ValueError, UnicodeDecodeError, TypeError) as exc:
        raise QualitySourceError("quality source is not canonical JSON") from exc
    validate_quality_source(source, **expected_provenance)
    return source


def validate_quality_source(
    source: Mapping[str, Any],
    *,
    expected_window: ExperimentReceiptWindow,
    expected_eligible_devices: int,
    expected_snapshot_manifest_sha256: str,
    expected_checkpoint_evidence_sha256: str,
    expected_workflow_run_id: str,
    expected_main_commit: str,
) -> None:
    _require(isinstance(source, Mapping) and set(source) == TOP_KEYS, "quality source field set drift")
    _require(type(source["schema_version"]) is int and source["schema_version"] == 1,
             "quality source schema drift")
    _require(source["evidence_type"] == "vevo_growthbook_aa_quality_source",
             "quality source type drift")
    _require(source["repository"] == "vzeman/biznisweb" and source["experiment_id"] == EXPERIMENT_ID,
             "quality source identity drift")
    _require(isinstance(expected_window, ExperimentReceiptWindow), "expected window is invalid")
    _require(source["window"] == expected_window.as_dict(), "quality source window drift")
    _require(type(expected_eligible_devices) is int and expected_eligible_devices >= 0,
             "expected eligible-device count is invalid")
    provenance = source["provenance"]
    _require(isinstance(provenance, dict) and set(provenance) == PROVENANCE_KEYS,
             "quality source provenance field set drift")
    _require(provenance["workflow"] == WORKFLOW, "quality source workflow drift")
    for key, value, pattern in (
        ("workflow_run_id", expected_workflow_run_id, RUN_RE),
        ("main_commit", expected_main_commit, COMMIT_RE),
        ("snapshot_manifest_sha256", expected_snapshot_manifest_sha256, SHA256_RE),
        ("checkpoint_evidence_sha256", expected_checkpoint_evidence_sha256, SHA256_RE),
    ):
        _require(isinstance(value, str) and pattern.fullmatch(value) is not None,
                 "independently supplied source provenance is invalid")
        _require(provenance[key] == value, "quality source provenance mismatch")
    for key in ("raw_extract_sha256", "authoritative_orders_sha256"):
        _require(isinstance(provenance[key], str) and SHA256_RE.fullmatch(provenance[key]) is not None,
                 "quality source input digest is invalid")
    generated = provenance["generated_at_utc"]
    _require(isinstance(generated, str) and re.fullmatch(
        r"20[2-9][0-9]-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z", generated
    ) is not None, "quality source generation is invalid")
    try:
        generated_at = datetime.fromisoformat(generated.replace("Z", "+00:00"))
    except ValueError as exc:
        raise QualitySourceError("quality source generation is invalid") from exc
    _require(generated_at >= expected_window.through_utc, "quality source window is incomplete")
    _require(isinstance(source["safety"], dict) and set(source["safety"]) == set(SAFETY)
             and all(source["safety"][key] is value for key, value in SAFETY.items()),
             "quality source safety boundary drift")
    quality = source["quality"]
    _require(isinstance(quality, dict) and quality.get("facts_generated_at") == generated,
             "quality source generation mismatch")
    # Reuse the existing statistics/schema checks. The legacy key here is only
    # their timestamp-format adapter; it is not an S3 address of this source.
    from scripts.record_growthbook_aa_evidence_gates import (
        EvidenceGateRecordingError, validate_quality_report,
    )
    legacy_format_key = (
        f"experiment-events/curated/quality/experiment_id={EXPERIMENT_ID}/"
        f"facts_generated_at={generated_at.strftime('%Y%m%dT%H%M%SZ')}.json"
    )
    try:
        validate_quality_report(
            quality, quality_report_key=legacy_format_key,
            resolved_through_utc=expected_window.as_dict()["through_utc"],
            resolved_eligible_devices=expected_eligible_devices,
        )
    except EvidenceGateRecordingError as exc:
        raise QualitySourceError("quality statistics do not match the resolved source contract") from exc


def build_quality_source(
    raw_events: Iterable[Mapping[str, Any]],
    authoritative_orders: Iterable[Mapping[str, Any]],
    *,
    config: ExperimentBuildConfig,
    window: ExperimentReceiptWindow,
    generated_at: datetime,
    expected_eligible_devices: int,
    snapshot_manifest_sha256: str,
    checkpoint_evidence_sha256: str,
    workflow_run_id: str,
    main_commit: str,
) -> dict[str, Any]:
    _require(config.metric_contract_version == CONTRACT
             and config.expected_variation_weights == {EXPERIMENT_ID: {"control": 0.5, "variant": 0.5}}
             and (config.cart_window_hours, config.order_window_days,
                  config.health_window_hours, config.maturity_checkpoint_days) == (24, 7, 24, 14),
             "quality source metric contract drift")
    _require(isinstance(window, ExperimentReceiptWindow), "quality source window is invalid")
    _require(isinstance(generated_at, datetime) and generated_at.tzinfo is not None
             and generated_at.utcoffset() is not None and generated_at.microsecond == 0,
             "quality source generation requires whole-second aware time")
    generated_at = generated_at.astimezone(UTC)
    events = _bounded_rows(raw_events)
    orders = _bounded_rows(authoritative_orders)
    _require(all(row.get("experiment_id") == EXPERIMENT_ID for row in events),
             "quality source contains a different experiment")
    try:
        bundle = build_experiment_facts(
            events, orders, config=config, generated_at=generated_at, measurement_window=window
        )
        context_events = [row for row in events if
                          datetime.fromisoformat(row["received_at"].replace("Z", "+00:00")) < window.through_utc]
        receipts = order_completion_receipts(context_events)
        _require(all(row["order_num"] in receipts for row in orders),
                 "authoritative source includes an unrelated order")
        _require(len(bundle.quality_reports) == 1, "quality source experiment count drift")
        quality = dict(bundle.quality_reports[0])
        # The source contract uses whole seconds. No legacy stored report is
        # rewritten: this is the generation of the new exact-window calculation.
        generated_text = generated_at.isoformat(timespec="seconds").replace("+00:00", "Z")
        quality["facts_generated_at"] = generated_text
        source = {
            "schema_version": 1,
            "evidence_type": "vevo_growthbook_aa_quality_source",
            "repository": "vzeman/biznisweb",
            "experiment_id": EXPERIMENT_ID,
            "window": window.as_dict(),
            "provenance": {
                "workflow": WORKFLOW, "workflow_run_id": workflow_run_id,
                "main_commit": main_commit, "generated_at_utc": generated_text,
                "snapshot_manifest_sha256": snapshot_manifest_sha256,
                "checkpoint_evidence_sha256": checkpoint_evidence_sha256,
                "raw_extract_sha256": _input_digest(events),
                "authoritative_orders_sha256": _input_digest(orders),
            },
            "quality": quality,
            "safety": dict(SAFETY),
        }
    except (ExperimentDataError, ValueError, TypeError, KeyError) as exc:
        if isinstance(exc, QualitySourceError):
            raise
        raise QualitySourceError("quality source inputs cannot be validated") from exc
    validate_quality_source(
        source, expected_window=window, expected_eligible_devices=expected_eligible_devices,
        expected_snapshot_manifest_sha256=snapshot_manifest_sha256,
        expected_checkpoint_evidence_sha256=checkpoint_evidence_sha256,
        expected_workflow_run_id=workflow_run_id, expected_main_commit=main_commit,
    )
    return source
