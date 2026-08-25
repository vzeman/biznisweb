#!/usr/bin/env python3
"""Build the VEVO CTA planning baseline from one aggregate-only Athena result.

The producer is fail-closed until the checked-in Production A/A completion
proves both an independently reproduced PASS and the reviewed zero-allocation
stop readback. It uses anonymous device identity only inside Athena and emits
exactly two counts; no event/device identity or commerce data enters output.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

try:
    from scripts.freeze_growthbook_cta_sample import (
        CtaSampleFreezeError,
        canonical_json_bytes,
        validate_observation,
        validate_plan,
    )
    from scripts.record_growthbook_aa_completion import (
        AaCompletionRecordingError,
        validate_manifest as validate_completion_manifest,
    )
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from freeze_growthbook_cta_sample import (
        CtaSampleFreezeError,
        canonical_json_bytes,
        validate_observation,
        validate_plan,
    )
    from record_growthbook_aa_completion import (
        AaCompletionRecordingError,
        validate_manifest as validate_completion_manifest,
    )


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST_PATH = ROOT / "projects" / "vevo" / "growthbook_cta_baseline.json"
DEFAULT_COMPLETION_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_production_aa_completion.json"
)
DEFAULT_SNAPSHOT_MANIFEST_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_aa_snapshot.json"
)
DEFAULT_ACTIVATION_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_production_aa_activation.json"
)
DEFAULT_STOP_OBSERVATION_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_aa_completion_observation.json"
)
DEFAULT_PLAN_PATH = ROOT / "projects" / "vevo" / "growthbook_cta_sample_plan.json"
DEFAULT_WORKSPACE_PATH = ROOT / "projects" / "vevo" / "growthbook_workspace.json"

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
UTC_RE = re.compile(r"^20[2-9][0-9]-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")
INTEGER_RE = re.compile(r"^(?:0|[1-9][0-9]*)$")
TOKEN_RE = re.compile(r"__[A-Z0-9_]+__")

ROOT_KEYS = {
    "schema_version",
    "producer_type",
    "experiment_id",
    "source_experiment_id",
    "source_completion_manifest",
    "source_snapshot_manifest",
    "source_sample_plan",
    "status",
    "workflow",
    "workflow_name",
    "collection_gate",
    "query_contract",
    "output",
    "release_boundaries",
    "next_gate",
}
COLLECTION_GATE_KEYS = {
    "required_completion_status",
    "required_aa_verdict",
    "required_stop_readback_status",
    "required_workspace_state",
    "minimum_followup_hours_after_aa_window",
    "dispatch_policy",
}
QUERY_CONTRACT_KEYS = {
    "template_path",
    "template_sha256",
    "database",
    "workgroup",
    "source_tables",
    "metric_contract_version",
    "event_time_field",
    "population_definition",
    "exposure_event",
    "exposure_page_type",
    "conversion_event",
    "conversion_page_type",
    "followup_hours",
    "consent_state",
    "risk_result",
    "eligible_only",
    "contaminated_excluded",
    "variation_breakdown_allowed",
    "result_columns",
}
OUTPUT_KEYS = {
    "artifact_name",
    "file_name",
    "retention_days",
    "canonical_json_required",
    "contains_raw_aws_payloads",
    "contains_event_or_device_ids",
    "contains_customer_or_order_data",
}
BOUNDARY_KEYS = {
    "main_only",
    "aws_aggregate_reads_only",
    "growthbook_mutation_allowed",
    "gtm_mutation_allowed",
    "meta_ads_mutation_allowed",
    "biznisweb_mutation_allowed",
    "collector_or_reporting_mutation_allowed",
    "price_cart_checkout_order_mutation_allowed",
    "winner_calls_allowed",
    "cta_activation_allowed",
}
QUERY_TOKENS = {
    "__AA_FROM_UTC__",
    "__AA_THROUGH_UTC__",
    "__FOLLOWUP_THROUGH_UTC__",
    "__AA_FROM_DATE__",
    "__AA_LAST_EXPOSURE_DATE__",
    "__FOLLOWUP_LAST_DATE__",
}


class CtaBaselineError(ValueError):
    """Raised when the CTA baseline producer fails closed."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise CtaBaselineError(message)


def _exact(value: Any, keys: set[str], field: str) -> Mapping[str, Any]:
    _require(isinstance(value, dict), f"{field} must be an object")
    _require(set(value) == keys, f"{field} field set drift")
    return value


def _load(path: Path, field: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CtaBaselineError(f"{field} is unreadable") from exc
    _require(isinstance(value, dict), f"{field} must contain an object")
    return value


def _parse_utc(value: Any, field: str) -> datetime:
    text = str(value or "")
    _require(UTC_RE.fullmatch(text) is not None, f"{field} must use whole-second UTC Z")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CtaBaselineError(f"{field} is invalid") from exc
    _require(parsed.tzinfo == UTC and not parsed.microsecond, f"{field} is invalid")
    return parsed


def _normalized_source_bytes(path: Path) -> bytes:
    try:
        return path.read_bytes().replace(b"\r\n", b"\n")
    except OSError as exc:
        raise CtaBaselineError("CTA baseline SQL template is unreadable") from exc


def _repo_file(relative_path: Any, field: str) -> Path:
    _require(isinstance(relative_path, str) and relative_path, f"{field} is invalid")
    candidate = (ROOT / relative_path).resolve()
    _require(
        ROOT.resolve() in candidate.parents and candidate.is_file(),
        f"{field} is unsafe or missing",
    )
    return candidate


def validate_manifest(manifest: Mapping[str, Any]) -> None:
    root = _exact(manifest, ROOT_KEYS, "CTA baseline manifest")
    _require(root["schema_version"] == 1, "CTA baseline schema drift")
    _require(
        root["producer_type"] == "vevo_growthbook_cta_product_baseline",
        "CTA baseline type drift",
    )
    _require(
        root["experiment_id"] == "vevo-sk-product-cta-color-001", "CTA experiment drift"
    )
    _require(
        root["source_experiment_id"] == "vevo-sk-aa-001", "CTA source experiment drift"
    )
    _require(
        root["source_completion_manifest"]
        == "projects/vevo/growthbook_production_aa_completion.json",
        "CTA completion source drift",
    )
    _require(
        root["source_snapshot_manifest"] == "projects/vevo/growthbook_aa_snapshot.json",
        "CTA snapshot source drift",
    )
    _require(
        root["source_sample_plan"] == "projects/vevo/growthbook_cta_sample_plan.json",
        "CTA sample-plan source drift",
    )
    _require(
        root["status"] == "prepared_runtime_gated_by_verified_aa_completion",
        "CTA baseline status drift",
    )
    _require(
        root["workflow"] == ".github/workflows/collect-vevo-growthbook-cta-baseline.yml"
        and root["workflow_name"] == "Collect VEVO GrowthBook CTA Baseline",
        "CTA baseline workflow drift",
    )

    gate = _exact(root["collection_gate"], COLLECTION_GATE_KEYS, "CTA collection gate")
    _require(
        gate
        == {
            "required_completion_status": "production_aa_stopped_verified_cta_activation_blocked",
            "required_aa_verdict": "PASS",
            "required_stop_readback_status": "verified_zero_allocation",
            "required_workspace_state": "production_aa_completed_cta_sample_freeze_pending_pro_quantiles_blocked",
            "minimum_followup_hours_after_aa_window": 24,
            "dispatch_policy": "main_manual_exact_confirmation_only",
        },
        "CTA collection gate drift",
    )

    query = _exact(root["query_contract"], QUERY_CONTRACT_KEYS, "CTA query contract")
    expected_query = {
        "template_path": "projects/vevo/growthbook_sql/cta_baseline_production.sql",
        "database": "vevo_growthbook_production",
        "workgroup": "vevo-growthbook-reporting-production",
        "source_tables": ["experiment_events_raw", "experiment_device_facts"],
        "metric_contract_version": "vevo_cm1_v1_2026-08-20",
        "event_time_field": "received_at",
        "population_definition": "first_valid_aa_product_page_exposure_proxy",
        "exposure_event": "experiment_exposure",
        "exposure_page_type": "product",
        "conversion_event": "add_to_cart",
        "conversion_page_type": "product",
        "followup_hours": 24,
        "consent_state": "analytics_granted",
        "risk_result": "accepted",
        "eligible_only": True,
        "contaminated_excluded": True,
        "variation_breakdown_allowed": False,
        "result_columns": ["exposed_devices", "converted_devices"],
    }
    for field, expected in expected_query.items():
        _require(query[field] == expected, f"CTA query contract drift: {field}")
    _require(
        SHA256_RE.fullmatch(str(query["template_sha256"] or "")) is not None,
        "CTA SQL SHA-256 is invalid",
    )
    template_path = _repo_file(query["template_path"], "CTA SQL template path")
    template_bytes = _normalized_source_bytes(template_path)
    _require(
        hashlib.sha256(template_bytes).hexdigest() == query["template_sha256"],
        "CTA SQL template SHA-256 drift",
    )
    sql = template_bytes.decode("utf-8")
    _require(set(TOKEN_RE.findall(sql)) == QUERY_TOKENS, "CTA SQL token set drift")
    lowered = sql.lower()
    for required in (
        "from experiment_device_facts",
        "from experiment_events_raw",
        "inner join eligible_aa_devices",
        "raw.variation_id = eligible.variation_id",
        "cart.variation_id = exposure.variation_id",
        "eligible = 1",
        "contaminated = 0",
        "raw.event_name = 'experiment_exposure'",
        "cart.event_name = 'add_to_cart'",
        "raw.page_type = 'product'",
        "cart.page_type = 'product'",
        "consent_state = 'analytics_granted'",
        "risk_result = 'accepted'",
        "count(*) as exposed_devices",
        "count(converted.device_id) as converted_devices",
    ):
        _require(required in lowered, f"CTA SQL safety marker missing: {required}")
    _require(
        "'control'" not in lowered and "'variant'" not in lowered,
        "CTA baseline must not emit or filter an arm breakdown",
    )
    _require("select *" not in lowered, "CTA baseline must not select raw rows")
    for forbidden in (
        "transaction_id",
        "customer",
        "email",
        "phone",
        "address",
        "fbclid",
        "_fbp",
        "_fbc",
    ):
        _require(
            forbidden not in lowered, f"CTA SQL forbidden field detected: {forbidden}"
        )
    _require(
        re.search(
            r"\b(insert|update|delete|drop|create|alter|merge|unload|call)\b", lowered
        )
        is None,
        "CTA SQL contains a mutation statement",
    )

    output = _exact(root["output"], OUTPUT_KEYS, "CTA baseline output")
    _require(
        output
        == {
            "artifact_name": "vevo-growthbook-cta-baseline",
            "file_name": "vevo-growthbook-cta-baseline.json",
            "retention_days": 14,
            "canonical_json_required": True,
            "contains_raw_aws_payloads": False,
            "contains_event_or_device_ids": False,
            "contains_customer_or_order_data": False,
        },
        "CTA baseline output drift",
    )
    boundaries = _exact(
        root["release_boundaries"], BOUNDARY_KEYS, "CTA release boundaries"
    )
    _require(boundaries["main_only"] is True, "CTA baseline is not main-only")
    _require(
        boundaries["aws_aggregate_reads_only"] is True,
        "CTA AWS reads are not aggregate-only",
    )
    for field in BOUNDARY_KEYS - {"main_only", "aws_aggregate_reads_only"}:
        _require(boundaries[field] is False, f"CTA release boundary opened: {field}")
    _require(
        root["next_gate"]
        == "after_verified_aa_pass_and_stop_collect_one_hash_bound_aggregate_then_freeze_sample_offline_activation_still_blocked",
        "CTA baseline next gate drift",
    )


def _validate_collection_gate(
    *,
    manifest: Mapping[str, Any],
    completion: Mapping[str, Any],
    snapshot_manifest: Mapping[str, Any],
    activation: Mapping[str, Any],
    stop_observation: Mapping[str, Any] | None,
    plan: Mapping[str, Any],
    workspace: Mapping[str, Any],
    now_utc: datetime,
) -> tuple[datetime, datetime, datetime]:
    validate_manifest(manifest)
    validate_plan(plan)
    try:
        validate_completion_manifest(
            completion,
            activation,
            snapshot_manifest,
            observation=stop_observation,
        )
    except AaCompletionRecordingError as exc:
        raise CtaBaselineError(f"A/A completion gate is invalid: {exc}") from exc
    gate = manifest["collection_gate"]
    _require(
        completion.get("status") == gate["required_completion_status"],
        "CTA baseline requires verified A/A PASS and stop readback",
    )
    _require(
        completion["aa_pass"].get("verdict") == gate["required_aa_verdict"],
        "CTA baseline requires A/A PASS",
    )
    _require(
        completion["aa_pass"].get("winner_calls_allowed") is False,
        "A/A winner call boundary drift",
    )
    _require(
        completion["stop_readback"].get("status")
        == gate["required_stop_readback_status"],
        "CTA baseline requires verified zero-allocation stop readback",
    )
    _require(
        plan.get("status") == "pending_aa_pass_and_final_sample_freeze",
        "CTA sample is already frozen",
    )
    _require(
        plan.get("activation_allowed") is False,
        "CTA sample plan activation gate is open",
    )
    _require(
        workspace.get("state") == gate["required_workspace_state"],
        "CTA workspace is not post-A/A",
    )
    _require(
        workspace.get("workspace", {}).get("production_allocation_percent") == 0,
        "workspace Production allocation is nonzero",
    )
    _require(
        workspace.get("decision_gates", {}).get("production_activation_allowed")
        is False,
        "workspace activation gate is open",
    )
    experiments = {
        row.get("tracking_key"): row
        for row in workspace.get("experiments") or []
        if isinstance(row, dict)
    }
    _require(
        set(experiments) == {"vevo-sk-aa-001", "vevo-sk-product-cta-color-001"},
        "workspace experiment set drift",
    )
    aa = experiments["vevo-sk-aa-001"]
    cta = experiments["vevo-sk-product-cta-color-001"]
    _require(
        aa.get("status") == "stopped_production_aa_pass_verified",
        "workspace A/A is not stopped",
    )
    _require(
        aa.get("production_allocation_percent") == 0,
        "workspace A/A allocation is nonzero",
    )
    _require(
        cta.get("status") == "draft" and cta.get("feature_rule_status") == "draft",
        "workspace CTA is not a draft",
    )
    _require(
        cta.get("production_allocation_percent") == 0,
        "workspace CTA allocation is nonzero",
    )

    window = snapshot_manifest.get("measurement_window") or {}
    start = _parse_utc(window.get("from_utc"), "A/A window start")
    through = _parse_utc(window.get("resolved_through_utc"), "A/A window end")
    _require(
        completion["aa_pass"].get("evaluated_at_utc")
        == window.get("resolved_through_utc"),
        "A/A completion end differs from the resolved window",
    )
    _require(
        SHA256_RE.fullmatch(str(completion["aa_pass"].get("snapshot_sha256") or ""))
        is not None,
        "A/A snapshot hash is missing",
    )
    followup_through = through + timedelta(
        hours=gate["minimum_followup_hours_after_aa_window"]
    )
    _require(now_utc.tzinfo is not None, "current time must be timezone-aware")
    current = now_utc.astimezone(UTC)
    _require(
        current >= followup_through, "CTA baseline 24-hour follow-up is incomplete"
    )
    return start, through, followup_through


def _load_collection_state(args: argparse.Namespace) -> tuple[dict[str, Any], ...]:
    manifest = _load(args.manifest, "CTA baseline manifest")
    completion = _load(args.completion, "A/A completion manifest")
    snapshot_manifest = _load(args.snapshot_manifest, "A/A snapshot manifest")
    activation = _load(args.activation, "A/A activation manifest")
    plan = _load(args.plan, "CTA sample plan")
    workspace = _load(args.workspace, "GrowthBook workspace")
    stop_observation = None
    if (
        completion.get("status")
        == "production_aa_stopped_verified_cta_activation_blocked"
    ):
        stop_observation = _load(args.stop_observation, "A/A stop observation")
    return (
        manifest,
        completion,
        snapshot_manifest,
        activation,
        stop_observation,
        plan,
        workspace,
    )


def render_query(
    *,
    manifest: Mapping[str, Any],
    completion: Mapping[str, Any],
    snapshot_manifest: Mapping[str, Any],
    activation: Mapping[str, Any],
    stop_observation: Mapping[str, Any] | None,
    plan: Mapping[str, Any],
    workspace: Mapping[str, Any],
    now_utc: datetime,
) -> str:
    start, through, followup_through = _validate_collection_gate(
        manifest=manifest,
        completion=completion,
        snapshot_manifest=snapshot_manifest,
        activation=activation,
        stop_observation=stop_observation,
        plan=plan,
        workspace=workspace,
        now_utc=now_utc,
    )
    query_path = _repo_file(
        manifest["query_contract"]["template_path"], "CTA SQL template path"
    )
    query = _normalized_source_bytes(query_path).decode("utf-8")
    replacements = {
        "__AA_FROM_UTC__": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "__AA_THROUGH_UTC__": through.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "__FOLLOWUP_THROUGH_UTC__": followup_through.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "__AA_FROM_DATE__": start.date().isoformat(),
        "__AA_LAST_EXPOSURE_DATE__": (through - timedelta(seconds=1))
        .date()
        .isoformat(),
        "__FOLLOWUP_LAST_DATE__": (followup_through - timedelta(seconds=1))
        .date()
        .isoformat(),
    }
    for token, value in replacements.items():
        query = query.replace(token, value)
    _require(TOKEN_RE.search(query) is None, "CTA SQL contains an unresolved token")
    return query.rstrip() + "\n"


def _athena_counts(
    payload: Mapping[str, Any], expected_columns: list[str]
) -> tuple[int, int]:
    _require("NextToken" not in payload, "CTA aggregate returned more than one row")
    result_set = payload.get("ResultSet")
    _require(isinstance(result_set, dict), "Athena ResultSet is missing")
    rows = result_set.get("Rows")
    _require(
        isinstance(rows, list) and len(rows) == 2,
        "CTA aggregate must contain one header and one data row",
    )
    metadata = result_set.get("ResultSetMetadata") or {}
    column_info = metadata.get("ColumnInfo") or []
    _require(
        [row.get("Name") for row in column_info] == expected_columns,
        "Athena CTA aggregate metadata drift",
    )
    header = [cell.get("VarCharValue") for cell in rows[0].get("Data") or []]
    values = [cell.get("VarCharValue") for cell in rows[1].get("Data") or []]
    _require(
        header == expected_columns and len(values) == len(expected_columns),
        "Athena CTA aggregate columns drift",
    )
    _require(
        all(isinstance(value, str) and INTEGER_RE.fullmatch(value) for value in values),
        "Athena CTA aggregate values are not nonnegative integers",
    )
    return int(values[0]), int(values[1])


def build_observation(
    *,
    manifest: Mapping[str, Any],
    completion: Mapping[str, Any],
    snapshot_manifest: Mapping[str, Any],
    activation: Mapping[str, Any],
    stop_observation: Mapping[str, Any] | None,
    plan: Mapping[str, Any],
    workspace: Mapping[str, Any],
    athena_result: Mapping[str, Any],
    now_utc: datetime,
) -> dict[str, Any]:
    start, through, _followup_through = _validate_collection_gate(
        manifest=manifest,
        completion=completion,
        snapshot_manifest=snapshot_manifest,
        activation=activation,
        stop_observation=stop_observation,
        plan=plan,
        workspace=workspace,
        now_utc=now_utc,
    )
    exposed, converted = _athena_counts(
        athena_result,
        manifest["query_contract"]["result_columns"],
    )
    observation = {
        "schema_version": 1,
        "source_experiment_id": manifest["source_experiment_id"],
        "aa_snapshot_sha256": completion["aa_pass"]["snapshot_sha256"],
        "aa_window_started_at_utc": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "aa_window_ended_at_utc": through.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "population_definition": manifest["query_contract"]["population_definition"],
        "exposed_devices": exposed,
        "converted_devices": converted,
        "contains_device_or_event_identity": False,
        "contains_customer_or_order_data": False,
    }
    try:
        validate_observation(observation, plan)
    except CtaSampleFreezeError as exc:
        raise CtaBaselineError(f"CTA baseline observation is invalid: {exc}") from exc
    return observation


def _write_bytes(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=path.parent, prefix=f".{path.name}.", delete=False
    ) as handle:
        temporary = Path(handle.name)
        handle.write(body)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _add_common_paths(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--completion", type=Path, default=DEFAULT_COMPLETION_PATH)
    parser.add_argument(
        "--snapshot-manifest", type=Path, default=DEFAULT_SNAPSHOT_MANIFEST_PATH
    )
    parser.add_argument("--activation", type=Path, default=DEFAULT_ACTIVATION_PATH)
    parser.add_argument(
        "--stop-observation", type=Path, default=DEFAULT_STOP_OBSERVATION_PATH
    )
    parser.add_argument("--plan", type=Path, default=DEFAULT_PLAN_PATH)
    parser.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE_PATH)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    validate_parser = subparsers.add_parser(
        "validate", help="Validate the static producer contract"
    )
    validate_parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST_PATH)
    render_parser = subparsers.add_parser(
        "render-query", help="Render the gated aggregate query"
    )
    _add_common_paths(render_parser)
    render_parser.add_argument("--output", type=Path, required=True)
    build_parser = subparsers.add_parser(
        "build-observation", help="Build canonical identity-free evidence"
    )
    _add_common_paths(build_parser)
    build_parser.add_argument("--athena-results", type=Path, required=True)
    build_parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "validate":
            validate_manifest(_load(args.manifest, "CTA baseline manifest"))
            print(
                "VEVO_CTA_BASELINE_CONTRACT_OK:completion-gated=true:aggregate-only=true"
            )
            return 0

        state = _load_collection_state(args)
        if args.command == "render-query":
            query = render_query(
                manifest=state[0],
                completion=state[1],
                snapshot_manifest=state[2],
                activation=state[3],
                stop_observation=state[4],
                plan=state[5],
                workspace=state[6],
                now_utc=datetime.now(UTC),
            )
            _write_bytes(args.output, query.encode("utf-8"))
            print(
                "VEVO_CTA_BASELINE_QUERY_READY:aggregate-only=true:variation-breakdown=false"
            )
            return 0

        athena_result = _load(args.athena_results, "Athena CTA aggregate")
        observation = build_observation(
            manifest=state[0],
            completion=state[1],
            snapshot_manifest=state[2],
            activation=state[3],
            stop_observation=state[4],
            plan=state[5],
            workspace=state[6],
            athena_result=athena_result,
            now_utc=datetime.now(UTC),
        )
        _write_bytes(args.output, canonical_json_bytes(observation))
        digest = hashlib.sha256(canonical_json_bytes(observation)).hexdigest()
        print(
            "VEVO_CTA_BASELINE_READY:"
            f"sha256={digest}:identities=false:commerce=false:activation=false"
        )
        return 0
    except (CtaBaselineError, CtaSampleFreezeError, OSError) as exc:
        print(
            f"build_growthbook_cta_baseline_observation.py: FAIL: {exc}",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
