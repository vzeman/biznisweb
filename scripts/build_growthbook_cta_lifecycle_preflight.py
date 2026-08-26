#!/usr/bin/env python3
"""Build one source-explicit, identity-free CTA lifecycle preflight artifact.

The preflight uses the completed Production A/A cohort, never CTA outcomes. It
compares the same frozen cohort through two independent read paths: canonical
curated S3 objects reduced locally and the read-only Athena reporting table.
Raw device facts remain temporary and no external system is mutated.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

try:
    from evaluate_growthbook_cta import (
        CtaEvaluationError,
        validate_lifecycle_manifest,
        validate_lifecycle_observation,
    )
except (
    ModuleNotFoundError
):  # Imported as scripts.build_growthbook_cta_lifecycle_preflight.
    from scripts.evaluate_growthbook_cta import (
        CtaEvaluationError,
        validate_lifecycle_manifest,
        validate_lifecycle_observation,
    )


ROOT = Path(__file__).resolve().parents[1]
VEVO = ROOT / "projects" / "vevo"
DEFAULT_MANIFEST = VEVO / "growthbook_cta_lifecycle_reconciliation.json"
DEFAULT_COMPLETION = VEVO / "growthbook_production_aa_completion.json"
DEFAULT_SNAPSHOT = VEVO / "growthbook_aa_snapshot.json"
DEFAULT_QUERY_TEMPLATE = (
    VEVO / "growthbook_sql" / "cta_lifecycle_preflight_production.sql"
)

TARGET_EXPERIMENT = "vevo-sk-product-cta-color-001"
SOURCE_EXPERIMENT = "vevo-sk-aa-001"
METRIC_CONTRACT = "vevo_cm1_v1_2026-08-20"
ORDER_WINDOW_DAYS = 7
LIFECYCLE_DAYS = 14
MINIMUM_FOLLOWUP_DAYS = ORDER_WINDOW_DAYS + LIFECYCLE_DAYS
QUALITY_KEY_RE = re.compile(
    r"^experiment-events/curated/quality/experiment_id=vevo-sk-aa-001/"
    r"facts_generated_at=(20[2-9][0-9]{5}T[0-9]{6}Z)[.]json$"
)
HASH_RE = re.compile(r"^[0-9a-f]{64}$")
RUN_RE = re.compile(r"^[1-9][0-9]{5,19}$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")

DEVICE_FACT_KEYS = {
    "metric_contract_version",
    "device_id",
    "first_exposure_at",
    "variation_id",
    "meta_campaign_id",
    "meta_adset_id",
    "meta_ad_id",
    "meta_placement",
    "add_to_cart_24h",
    "purchase_converted",
    "joined_order_count",
    "net_revenue_eur",
    "cm1_eur",
    "cancelled_order_count",
    "refunded_order_count",
    "immature_order_count",
    "client_error_observed",
    "contaminated",
    "eligible",
    "order_attribution_eligible",
    "order_attribution_issue",
    "unmatched_transaction_count",
    "ambiguous_transaction_count",
    "exclusion_reason",
    "facts_generated_at",
}
ATHENA_COLUMNS = (
    "eligible_device_count",
    "joined_order_count",
    "mature_joined_order_count",
    "immature_order_count",
    "cm1_sum_eur",
    "cancelled_order_count",
    "refunded_or_creditnoted_order_count",
    "facts_generation_count",
    "facts_generated_at",
)


class LifecyclePreflightError(ValueError):
    """Raised when a lifecycle preflight input is incomplete or unsafe."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise LifecyclePreflightError(message)


def canonical_json_bytes(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def _load(path: Path, field: str) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LifecyclePreflightError(f"{field} is unreadable") from exc
    _require(isinstance(value, dict), f"{field} must contain an object")
    return value


def _parse_utc(value: Any, field: str) -> datetime:
    _require(isinstance(value, str) and value.endswith("Z"), f"{field} must use UTC Z")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00").astimezone(timezone.utc)
    except ValueError as exc:
        raise LifecyclePreflightError(f"{field} is invalid") from exc
    _require(
        parsed.isoformat(timespec="milliseconds").replace("+00:00", "Z") == value
        or parsed.isoformat(timespec="seconds").replace("+00:00", "Z") == value,
        f"{field} is not canonical",
    )
    return parsed


def _integer(value: Any, field: str) -> int:
    _require(
        type(value) is int and value >= 0, f"{field} must be a non-negative integer"
    )
    return value


def _money(value: Any, field: str) -> Decimal:
    _require(not isinstance(value, bool), f"{field} must be numeric")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise LifecyclePreflightError(f"{field} must be numeric") from exc
    _require(parsed.is_finite(), f"{field} must be finite")
    _require(
        parsed == parsed.quantize(Decimal("0.01")), f"{field} must use cent precision"
    )
    return parsed


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def prepare_context(
    manifest: Mapping[str, Any],
    completion: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    *,
    manifest_sha256: str,
    completion_sha256: str,
    snapshot_sha256: str,
    query_template_sha256: str,
    now: datetime,
) -> dict[str, Any]:
    try:
        validate_lifecycle_manifest(manifest)
    except CtaEvaluationError as exc:
        raise LifecyclePreflightError(f"lifecycle manifest is invalid: {exc}") from exc
    _require(manifest["verified"] is False, "lifecycle preflight is already recorded")
    _require(
        manifest["source_experiment_id"] == SOURCE_EXPERIMENT
        and manifest["target_experiment_id"] == TARGET_EXPERIMENT,
        "lifecycle source/target experiment drift",
    )
    _require(
        manifest["query_template_sha256"] == query_template_sha256,
        "lifecycle query template SHA-256 drift",
    )
    _require(
        completion.get("status")
        == "production_aa_stopped_verified_cta_activation_blocked",
        "A/A completion and reviewed zero-allocation stop are not recorded",
    )
    _require(
        (completion.get("aa_pass") or {}).get("status") == "verified_pass"
        and (completion.get("aa_pass") or {}).get("verdict") == "PASS",
        "A/A PASS is not independently recorded",
    )
    _require(
        (completion.get("stop_readback") or {}).get("status")
        == "verified_zero_allocation",
        "A/A zero-allocation readback is not verified",
    )
    window = snapshot.get("measurement_window") or {}
    _require(window.get("resolution_status") == "resolved", "A/A window is unresolved")
    source_from = str(window.get("from_utc") or "")
    source_through = str(window.get("resolved_through_utc") or "")
    started = _parse_utc(source_from, "source_from_utc")
    through = _parse_utc(source_through, "source_through_utc")
    _require(through > started, "A/A source window is invalid")
    due = through + timedelta(days=MINIMUM_FOLLOWUP_DAYS)
    _require(
        now.tzinfo is not None and now.utcoffset() is not None,
        "current time must be aware",
    )
    _require(
        now.astimezone(timezone.utc) >= due,
        "A/A order plus lifecycle follow-up is incomplete",
    )
    return {
        "schema_version": 1,
        "target_experiment_id": TARGET_EXPERIMENT,
        "source_experiment_id": SOURCE_EXPERIMENT,
        "source_from_utc": source_from,
        "source_through_utc": source_through,
        "minimum_collection_due_utc": due.isoformat(timespec="seconds").replace(
            "+00:00", "Z"
        ),
        "order_window_days": ORDER_WINDOW_DAYS,
        "lifecycle_checkpoint_days": LIFECYCLE_DAYS,
        "minimum_followup_days_after_source_end": MINIMUM_FOLLOWUP_DAYS,
        "manifest_sha256": manifest_sha256,
        "source_completion_sha256": completion_sha256,
        "source_aa_snapshot_sha256": snapshot_sha256,
        "query_template_sha256": query_template_sha256,
    }


def render_query(template: str, context: Mapping[str, Any]) -> str:
    _require(
        template.count("__SOURCE_FROM_UTC__") == 1, "query start placeholder drift"
    )
    _require(
        template.count("__SOURCE_THROUGH_UTC__") == 1, "query end placeholder drift"
    )
    rendered = template.replace("__SOURCE_FROM_UTC__", context["source_from_utc"])
    rendered = rendered.replace("__SOURCE_THROUGH_UTC__", context["source_through_utc"])
    _require("__SOURCE_" not in rendered, "query placeholder remained unresolved")
    return rendered.rstrip() + "\n"


def validate_quality_object(
    quality_bytes: bytes,
    *,
    quality_key: str,
    minimum_due_utc: str,
) -> dict[str, Any]:
    try:
        value = json.loads(quality_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LifecyclePreflightError("reporting quality object is not JSON") from exc
    _require(isinstance(value, dict), "reporting quality object must contain an object")
    _require(
        quality_bytes == canonical_json_bytes(value),
        "reporting quality object is not canonical",
    )
    _require(
        value.get("metric_contract_version") == METRIC_CONTRACT, "quality metric drift"
    )
    _require(
        value.get("experiment_id") == SOURCE_EXPERIMENT, "quality experiment drift"
    )
    generated = _parse_utc(
        value.get("facts_generated_at"), "quality facts_generated_at"
    )
    due = _parse_utc(minimum_due_utc, "minimum_due_utc")
    _require(generated >= due, "reporting quality object predates complete follow-up")
    match = QUALITY_KEY_RE.fullmatch(quality_key)
    _require(match is not None, "reporting quality object key is invalid")
    expected_marker = generated.strftime("%Y%m%dT%H%M%SZ")
    _require(match.group(1) == expected_marker, "reporting quality key timestamp drift")
    return value


def _load_direct_facts(paths: Iterable[Path]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen_devices: set[str] = set()
    for path in sorted(paths):
        value = _load(path, "curated device fact")
        _require(set(value) == DEVICE_FACT_KEYS, "curated device fact field set drift")
        _require(
            path.read_bytes() == canonical_json_bytes(value),
            "curated device fact is not canonical",
        )
        device_id = str(value.get("device_id") or "")
        _require(
            device_id and device_id not in seen_devices,
            "duplicate curated device identity",
        )
        seen_devices.add(device_id)
        rows.append(value)
    _require(rows, "no curated device facts were downloaded")
    return rows


def select_quality_context(
    rows: Iterable[Mapping[str, Any]],
    *,
    source_from_utc: str,
    source_through_utc: str,
    minimum_due_utc: str,
) -> dict[str, str]:
    started = _parse_utc(source_from_utc, "source_from_utc")
    through = _parse_utc(source_through_utc, "source_through_utc")
    generations: set[str] = set()
    selected_count = 0
    for row in rows:
        _require(set(row) == DEVICE_FACT_KEYS, "curated device fact field set drift")
        _require(
            row["metric_contract_version"] == METRIC_CONTRACT, "curated metric drift"
        )
        exposed = _parse_utc(row["first_exposure_at"], "first_exposure_at")
        if not (started <= exposed < through):
            continue
        if row["eligible"] != 1 or row["contaminated"] != 0:
            continue
        selected_count += 1
        generations.add(str(row["facts_generated_at"]))
    _require(selected_count > 0, "frozen A/A cohort is empty")
    _require(
        len(generations) == 1,
        "frozen A/A cohort spans multiple curated facts generations",
    )
    generation = next(iter(generations))
    generated = _parse_utc(generation, "curated facts_generated_at")
    due = _parse_utc(minimum_due_utc, "minimum_due_utc")
    _require(generated >= due, "curated A/A facts predate complete follow-up")
    marker = generated.strftime("%Y%m%dT%H%M%SZ")
    return {
        "facts_generated_at": generation,
        "quality_key": (
            "experiment-events/curated/quality/experiment_id=vevo-sk-aa-001/"
            f"facts_generated_at={marker}.json"
        ),
    }


def aggregate_direct_facts(
    rows: Iterable[Mapping[str, Any]],
    *,
    source_from_utc: str,
    source_through_utc: str,
    expected_generation: str,
) -> dict[str, Any]:
    started = _parse_utc(source_from_utc, "source_from_utc")
    through = _parse_utc(source_through_utc, "source_through_utc")
    selected: list[Mapping[str, Any]] = []
    for row in rows:
        _require(
            row["metric_contract_version"] == METRIC_CONTRACT, "curated metric drift"
        )
        exposed = _parse_utc(row["first_exposure_at"], "first_exposure_at")
        if not (started <= exposed < through):
            continue
        if row["eligible"] != 1 or row["contaminated"] != 0:
            continue
        _require(
            row["facts_generated_at"] == expected_generation, "curated generation drift"
        )
        selected.append(row)
    _require(selected, "frozen A/A cohort is empty")
    joined = sum(
        _integer(row["joined_order_count"], "joined_order_count") for row in selected
    )
    immature = sum(
        _integer(row["immature_order_count"], "immature_order_count")
        for row in selected
    )
    cancelled = sum(
        _integer(row["cancelled_order_count"], "cancelled_order_count")
        for row in selected
    )
    refunded = sum(
        _integer(row["refunded_order_count"], "refunded_order_count")
        for row in selected
    )
    cm1 = sum((_money(row["cm1_eur"], "cm1_eur") for row in selected), Decimal("0.00"))
    _require(immature == 0, "frozen A/A cohort still contains immature orders")
    _require(
        joined >= cancelled + refunded, "curated lifecycle counts are inconsistent"
    )
    _require(
        cancelled + refunded >= 1,
        "no mature cancelled/refunded/creditnoted case exists",
    )
    return {
        "eligible_device_count": len(selected),
        "joined_order_count": joined,
        "mature_joined_order_count": joined - immature,
        "immature_order_count": immature,
        "cm1_sum_eur": cm1,
        "cancelled_order_count": cancelled,
        "refunded_or_creditnoted_order_count": refunded,
        "facts_generation_count": 1,
        "facts_generated_at": expected_generation,
    }


def parse_athena_result(value: Mapping[str, Any]) -> dict[str, Any]:
    rows = (value.get("ResultSet") or {}).get("Rows") or []
    _require(
        len(rows) == 2, "Athena lifecycle result must contain one header and one row"
    )

    def cells(row: Mapping[str, Any]) -> list[str]:
        return [
            str((cell.get("VarCharValue") if isinstance(cell, dict) else "") or "")
            for cell in row.get("Data") or []
        ]

    _require(tuple(cells(rows[0])) == ATHENA_COLUMNS, "Athena lifecycle header drift")
    data = cells(rows[1])
    _require(len(data) == len(ATHENA_COLUMNS), "Athena lifecycle row width drift")
    raw = dict(zip(ATHENA_COLUMNS, data, strict=True))
    integer_fields = ATHENA_COLUMNS[:4] + ATHENA_COLUMNS[5:8]
    parsed: dict[str, Any] = {}
    for field in integer_fields:
        _require(raw[field].isdigit(), f"Athena {field} is not an integer")
        parsed[field] = int(raw[field])
    parsed["cm1_sum_eur"] = _money(raw["cm1_sum_eur"], "Athena cm1_sum_eur")
    parsed["facts_generated_at"] = raw["facts_generated_at"]
    _parse_utc(parsed["facts_generated_at"], "Athena facts_generated_at")
    return parsed


def build_observation(
    *,
    context: Mapping[str, Any],
    quality_bytes: bytes,
    quality_key: str,
    direct_rows: Iterable[Mapping[str, Any]],
    athena_result: Mapping[str, Any],
    workflow_run_id: str,
    main_commit: str,
) -> dict[str, Any]:
    _require(
        RUN_RE.fullmatch(workflow_run_id) is not None, "workflow run ID is invalid"
    )
    _require(COMMIT_RE.fullmatch(main_commit) is not None, "main commit is invalid")
    quality = validate_quality_object(
        quality_bytes,
        quality_key=quality_key,
        minimum_due_utc=context["minimum_collection_due_utc"],
    )
    direct = aggregate_direct_facts(
        direct_rows,
        source_from_utc=context["source_from_utc"],
        source_through_utc=context["source_through_utc"],
        expected_generation=quality["facts_generated_at"],
    )
    athena = parse_athena_result(athena_result)
    _require(
        athena["facts_generation_count"] == 1, "Athena facts span multiple generations"
    )
    _require(
        athena["facts_generated_at"] == quality["facts_generated_at"],
        "Athena/quality generation drift",
    )
    for field in (
        "eligible_device_count",
        "joined_order_count",
        "mature_joined_order_count",
        "immature_order_count",
        "cancelled_order_count",
        "refunded_or_creditnoted_order_count",
    ):
        _require(
            direct[field] == athena[field], f"direct/Athena lifecycle mismatch: {field}"
        )
    difference = abs(direct["cm1_sum_eur"] - athena["cm1_sum_eur"])
    _require(difference == Decimal("0.00"), "direct/Athena CM1 parity failed")
    quality_marker = QUALITY_KEY_RE.fullmatch(quality_key)
    _require(quality_marker is not None, "reporting quality object key is invalid")
    observed_at_utc = (
        datetime.strptime(quality_marker.group(1), "%Y%m%dT%H%M%SZ")
        .replace(tzinfo=timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )
    observation = {
        "schema_version": 2,
        "evidence_type": "vevo_growthbook_cta_prelaunch_lifecycle_reconciliation",
        "target_experiment_id": TARGET_EXPERIMENT,
        "source_experiment_id": SOURCE_EXPERIMENT,
        "metric_contract_version": METRIC_CONTRACT,
        "workflow_run_id": workflow_run_id,
        "main_commit": main_commit,
        "observed_at_utc": observed_at_utc,
        "source_from_utc": context["source_from_utc"],
        "source_through_utc": context["source_through_utc"],
        "order_window_days": ORDER_WINDOW_DAYS,
        "lifecycle_checkpoint_days": LIFECYCLE_DAYS,
        "minimum_followup_days_after_source_end": MINIMUM_FOLLOWUP_DAYS,
        "source_completion_sha256": context["source_completion_sha256"],
        "source_aa_snapshot_sha256": context["source_aa_snapshot_sha256"],
        "query_template_sha256": context["query_template_sha256"],
        "reporting_quality_object_key": quality_key,
        "reporting_quality_object_sha256": hashlib.sha256(quality_bytes).hexdigest(),
        "eligible_devices_checked": direct["eligible_device_count"],
        "joined_orders_checked": direct["joined_order_count"],
        "mature_orders_checked": direct["mature_joined_order_count"],
        "immature_orders_checked": direct["immature_order_count"],
        "cancelled_orders_checked": direct["cancelled_order_count"],
        "refunded_or_creditnoted_orders_checked": direct[
            "refunded_or_creditnoted_order_count"
        ],
        "direct_curated_cm1_sum_eur": float(direct["cm1_sum_eur"]),
        "athena_reporting_cm1_sum_eur": float(athena["cm1_sum_eur"]),
        "cm1_absolute_difference_eur": float(difference),
        "lifecycle_counts_match": True,
        "refund_creditnote_value_parity_verified": True,
        "non_realized_value_policy": "zero_value_until_realized_with_explicit_lifecycle_counts",
        "non_realized_value_policy_verified": True,
        "cta_outcome_data_read": False,
        "contains_event_or_device_identity": False,
        "customer_or_order_identity_in_evidence": False,
        "source_read_only": True,
        "no_external_mutation": True,
    }
    try:
        validate_lifecycle_observation(observation)
    except CtaEvaluationError as exc:
        raise LifecyclePreflightError(
            f"built lifecycle observation is invalid: {exc}"
        ) from exc
    return observation


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    validate = commands.add_parser("validate-contract")
    validate.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)

    prepare = commands.add_parser("prepare")
    prepare.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    prepare.add_argument("--completion", type=Path, default=DEFAULT_COMPLETION)
    prepare.add_argument("--snapshot", type=Path, default=DEFAULT_SNAPSHOT)
    prepare.add_argument("--query-template", type=Path, default=DEFAULT_QUERY_TEMPLATE)
    prepare.add_argument("--context-output", required=True, type=Path)
    prepare.add_argument("--query-output", required=True, type=Path)

    select_quality = commands.add_parser("select-quality-context")
    select_quality.add_argument("--context", required=True, type=Path)
    select_quality.add_argument("--direct-facts", required=True, type=Path)
    select_quality.add_argument("--output", required=True, type=Path)

    build = commands.add_parser("build-observation")
    build.add_argument("--context", required=True, type=Path)
    build.add_argument("--quality", required=True, type=Path)
    build.add_argument("--quality-key", required=True)
    build.add_argument("--direct-facts", required=True, type=Path)
    build.add_argument("--athena-results", required=True, type=Path)
    build.add_argument("--workflow-run-id", required=True)
    build.add_argument("--main-commit", required=True)
    build.add_argument("--output", required=True, type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "validate-contract":
            validate_lifecycle_manifest(_load(args.manifest, "lifecycle manifest"))
            print(
                "VEVO_CTA_LIFECYCLE_PREFLIGHT_CONTRACT_OK:source=completed-aa:cta-outcomes=false"
            )
            return 0
        if args.command == "prepare":
            manifest = _load(args.manifest, "lifecycle manifest")
            completion = _load(args.completion, "A/A completion")
            snapshot = _load(args.snapshot, "A/A snapshot")
            template = args.query_template.read_text(encoding="utf-8")
            context = prepare_context(
                manifest,
                completion,
                snapshot,
                manifest_sha256=_sha256(args.manifest),
                completion_sha256=_sha256(args.completion),
                snapshot_sha256=_sha256(args.snapshot),
                query_template_sha256=_sha256(args.query_template),
                now=datetime.now(timezone.utc),
            )
            args.context_output.write_bytes(canonical_json_bytes(context))
            args.query_output.write_text(
                render_query(template, context), encoding="utf-8"
            )
            print("VEVO_CTA_LIFECYCLE_PREFLIGHT_READY:source=completed-aa:followup=21d")
            return 0
        context = _load(args.context, "lifecycle context")
        if args.command == "select-quality-context":
            direct_paths = list(args.direct_facts.rglob("*.json"))
            quality_context = select_quality_context(
                _load_direct_facts(direct_paths),
                source_from_utc=context["source_from_utc"],
                source_through_utc=context["source_through_utc"],
                minimum_due_utc=context["minimum_collection_due_utc"],
            )
            args.output.write_bytes(canonical_json_bytes(quality_context))
            print(
                "VEVO_CTA_LIFECYCLE_QUALITY_CONTEXT_OK:"
                "source=completed-aa:generation=direct-facts-bound"
            )
            return 0
        quality_bytes = args.quality.read_bytes()
        athena_result = _load(args.athena_results, "Athena lifecycle result")
        direct_paths = list(args.direct_facts.rglob("*.json"))
        observation = build_observation(
            context=context,
            quality_bytes=quality_bytes,
            quality_key=args.quality_key,
            direct_rows=_load_direct_facts(direct_paths),
            athena_result=athena_result,
            workflow_run_id=args.workflow_run_id,
            main_commit=args.main_commit,
        )
        args.output.write_bytes(canonical_json_bytes(observation))
        print(
            "VEVO_CTA_LIFECYCLE_PREFLIGHT_BUILT:source=completed-aa:identity=false:mutation=false"
        )
        return 0
    except (CtaEvaluationError, LifecyclePreflightError, OSError) as exc:
        print(f"VEVO_CTA_LIFECYCLE_PREFLIGHT_INVALID:{exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
