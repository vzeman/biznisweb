"""Offline validation of a managed A/A source capture against independent inputs.

``plan`` must be derived from the snapshot/workspace at the verified successful
source run's exact main commit, with its independently verified health binding.
Never derive expected provenance solely from the capture being checked.
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta

from scripts.build_growthbook_aa_quality_source import canonical_source_bytes, validate_quality_source

SHA = re.compile(r"^[a-f0-9]{64}$")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def exact(value, keys):
    require(isinstance(value, dict) and set(value) == set(keys), "capture field set drift")


def timestamp(value):
    require(isinstance(value, str) and re.fullmatch(r"20[2-9][0-9]-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", value),
            "capture timestamp format drift")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def hash_field(value):
    require(isinstance(value, str) and SHA.fullmatch(value), "capture digest invalid")


def validate_capture(capture, plan):
    exact(capture, {"schema_version", "evidence_type", "source", "acquisition", "safety"})
    require(type(capture["schema_version"]) is int and capture["schema_version"] == 1
            and capture["evidence_type"] == "vevo_growthbook_aa_quality_capture", "capture type drift")
    validate_quality_source(capture["source"], expected_window=plan.window, expected_eligible_devices=plan.eligible,
        expected_snapshot_manifest_sha256=plan.snapshot_sha256, expected_checkpoint_evidence_sha256=plan.checkpoint_sha256,
        expected_workflow_run_id=plan.run_id, expected_main_commit=plan.main_commit)
    acquisition = capture["acquisition"]
    exact(acquisition, {"started_at_utc", "completed_at_utc", "foundation_evidence_sha256", "context_floor_source",
        "retention_policy_days", "raw_input", "order_input", "receipt_parity", "control_before_sha256",
        "control_after_sha256", "managed_token_reference_sha256", "health", "collector_live_identity_verified",
        "reconciler_immutable_localhost_gate_inherited", "source_schedule_unchanged"})
    start = timestamp(acquisition["started_at_utc"])
    end = timestamp(acquisition["completed_at_utc"])
    generated = timestamp(capture["source"]["provenance"]["generated_at_utc"])
    require(plan.window.through_utc <= start <= generated <= end and end - start <= timedelta(minutes=45),
            "capture chronology drift")
    require(acquisition["foundation_evidence_sha256"] == plan.foundation_sha256
            and acquisition["context_floor_source"] == "verified_empty_production_foundation_utc_day"
            and type(acquisition["retention_policy_days"]) is int and acquisition["retention_policy_days"] == 180,
            "capture source foundation/retention drift")
    require(acquisition["collector_live_identity_verified"] is True
            and acquisition["reconciler_immutable_localhost_gate_inherited"] is True
            and acquisition["source_schedule_unchanged"] is True, "source runtime verification missing")
    for field in ("foundation_evidence_sha256", "control_before_sha256", "control_after_sha256", "managed_token_reference_sha256"):
        hash_field(acquisition[field])
    require(acquisition["control_before_sha256"] == acquisition["control_after_sha256"], "capture control drift")
    exact(acquisition["health"], {"workflow_run_id", "main_commit", "sha256"})
    require(acquisition["health"] == {"workflow_run_id": plan.health_run_id, "main_commit": plan.main_commit,
                                      "sha256": plan.health_sha256}, "capture health binding drift")
    parity = acquisition["receipt_parity"]
    exact(parity, {"context_receipt_summary_sha256", "accepted_write_count_parity_verified"})
    hash_field(parity["context_receipt_summary_sha256"])
    require(parity["accepted_write_count_parity_verified"] is True, "accepted write parity missing")
    raw = acquisition["raw_input"]
    exact(raw, {"schema_version", "coverage", "context_from_utc", "through_utc", "last_partition_date",
                "inventory_before_sha256", "inventory_after_sha256", "conditional_reads_verified",
                "receipt_partition_parity_verified", "historical_retention_proven", "context_floor_proven", "contains_identities"})
    require(type(raw["schema_version"]) is int and raw["schema_version"] == 1
            and raw["coverage"] == "stable_retained_utc_partitions_only"
            and timestamp(raw["context_from_utc"]) == plan.window.context_from_utc
            and timestamp(raw["through_utc"]) == plan.window.through_utc
            and raw["last_partition_date"] == (plan.window.through_utc - timedelta(microseconds=1)).date().isoformat(),
            "raw coverage window drift")
    for key in ("conditional_reads_verified", "receipt_partition_parity_verified"):
        require(raw[key] is True, "raw coverage proof missing")
    for key in ("historical_retention_proven", "context_floor_proven", "contains_identities"):
        require(raw[key] is False, "raw adapter overclaimed proof")
    hash_field(raw["inventory_before_sha256"])
    require(raw["inventory_before_sha256"] == raw["inventory_after_sha256"], "raw inventory changed")
    orders = acquisition["order_input"]
    exact(orders, {"schema_version", "coverage", "query_sha256", "receipt_set_sha256", "responses_before_sha256",
                   "responses_after_sha256", "explicit_not_found_retained_in_digest", "atomic_historical_snapshot_proven",
                   "contains_identities"})
    from reporting_core.experiment_quality_source_io import RECEIPTED_ORDER_QUERY
    require(type(orders["schema_version"]) is int and orders["schema_version"] == 1
            and orders["coverage"] == "every_supplied_receipt_id_explicitly_queried"
            and orders["query_sha256"] == hashlib.sha256(RECEIPTED_ORDER_QUERY.encode()).hexdigest()
            and orders["explicit_not_found_retained_in_digest"] is True
            and orders["atomic_historical_snapshot_proven"] is False and orders["contains_identities"] is False,
            "order coverage boundary drift")
    for key in ("receipt_set_sha256", "responses_before_sha256", "responses_after_sha256"):
        hash_field(orders[key])
    require(orders["responses_before_sha256"] == orders["responses_after_sha256"], "order source changed")
    expected_safety = {"read_only": True, "contains_identities": False, "contains_credentials": False,
        "contains_raw_aws_payloads": False, "ordinary_publish_allowed": False, "preview_woken": False,
        "experiment_mutations": False, "winner_calls": False}
    exact(capture["safety"], expected_safety)
    require(all(capture["safety"][key] is value for key, value in expected_safety.items()), "capture safety drift")


def validate_capture_bytes(raw, plan, *, expected_sha256):
    hash_field(expected_sha256)
    require(isinstance(raw, bytes) and len(raw) <= 1024 * 1024, "capture byte bound exceeded")
    capture = json.loads(raw)
    require(raw == canonical_source_bytes(capture)
            and hashlib.sha256(raw).hexdigest() == expected_sha256, "capture canonical digest mismatch")
    validate_capture(capture, plan)
    return capture
