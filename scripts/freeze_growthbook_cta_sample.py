#!/usr/bin/env python3
"""Freeze VEVO's first CTA A/B sample from hash-bound A/A aggregate evidence.

The tool is deliberately offline and mutation-free outside its two explicit
output files. It never starts an experiment or opens the Production gate.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import NormalDist
from typing import Any, Mapping

try:
    from evaluate_growthbook_aa import (
        AaEvaluationError,
        DEFAULT_CONFIG_PATH as AA_CONFIG_PATH,
        evaluate as evaluate_aa,
        load_config as load_aa_config,
    )
except ModuleNotFoundError:  # Imported as scripts.freeze_growthbook_cta_sample.
    from scripts.evaluate_growthbook_aa import (
        AaEvaluationError,
        DEFAULT_CONFIG_PATH as AA_CONFIG_PATH,
        evaluate as evaluate_aa,
        load_config as load_aa_config,
    )


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PLAN_PATH = ROOT / "projects" / "vevo" / "growthbook_cta_sample_plan.json"
DEFAULT_WORKSPACE_PATH = ROOT / "projects" / "vevo" / "growthbook_workspace.json"
HASH_RE = re.compile(r"^[0-9a-f]{64}$")
UTC_RE = re.compile(r"^20[2-9][0-9]-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")

PLAN_KEYS = {
    "schema_version",
    "experiment_id",
    "source_experiment_id",
    "timezone",
    "status",
    "population_definition",
    "primary_metric",
    "planning_method",
    "expected_variation_weights",
    "relative_mde_percent",
    "power_percent",
    "two_sided_alpha_percent",
    "minimum_baseline_exposed_devices",
    "minimum_full_calendar_days",
    "maximum_full_calendar_days",
    "provisional",
    "final",
    "activation_allowed",
    "next_gate",
}
PROVISIONAL_KEYS = {
    "source",
    "exposed_devices",
    "converted_devices",
    "sample_per_arm",
    "total_sample",
}
FINAL_KEYS = {
    "observation_sha256",
    "aa_snapshot_sha256",
    "aa_window_started_at_utc",
    "aa_window_ended_at_utc",
    "exposed_devices",
    "converted_devices",
    "baseline_rate_percent",
    "target_rate_percent",
    "sample_per_arm",
    "total_sample",
    "frozen_at_utc",
}
OBSERVATION_KEYS = {
    "schema_version",
    "source_experiment_id",
    "aa_snapshot_sha256",
    "aa_window_started_at_utc",
    "aa_window_ended_at_utc",
    "population_definition",
    "exposed_devices",
    "converted_devices",
    "contains_device_or_event_identity",
    "contains_customer_or_order_data",
}


class CtaSampleFreezeError(ValueError):
    """Raised when a sample plan or observation fails closed."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise CtaSampleFreezeError(message)


def _exact_object(value: Any, keys: set[str], field: str) -> Mapping[str, Any]:
    _require(isinstance(value, dict), f"{field} must be an object")
    _require(set(value) == keys, f"{field} field set drift")
    return value


def _integer(value: Any, field: str, *, minimum: int = 0) -> int:
    _require(type(value) is int and value >= minimum, f"{field} must be an integer >= {minimum}")
    return value


def _number(value: Any, field: str, *, minimum: float, maximum: float) -> float:
    _require(type(value) in (int, float) and not isinstance(value, bool), f"{field} must be numeric")
    result = float(value)
    _require(math.isfinite(result) and minimum <= result <= maximum, f"{field} is out of range")
    return result


def _parse_utc(value: Any, field: str) -> datetime:
    text = str(value or "")
    _require(UTC_RE.fullmatch(text) is not None, f"{field} must use whole-second UTC Z format")
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError as exc:
        raise CtaSampleFreezeError(f"{field} is invalid") from exc


def canonical_json_bytes(value: Mapping[str, Any]) -> bytes:
    return (json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n").encode(
        "utf-8"
    )


def calculate_sample_per_arm(
    *,
    exposed_devices: int,
    converted_devices: int,
    relative_mde_percent: float,
    power_percent: float,
    alpha_percent: float,
) -> tuple[int, float, float]:
    exposed = _integer(exposed_devices, "exposed_devices", minimum=2)
    converted = _integer(converted_devices, "converted_devices", minimum=1)
    _require(converted < exposed, "converted_devices must be below exposed_devices")
    relative_mde = _number(relative_mde_percent, "relative_mde_percent", minimum=0.01, maximum=500) / 100
    power = _number(power_percent, "power_percent", minimum=50.000001, maximum=99.999999) / 100
    alpha = _number(alpha_percent, "two_sided_alpha_percent", minimum=0.000001, maximum=49.999999) / 100

    baseline = converted / exposed
    target = baseline * (1 + relative_mde)
    _require(target < 1, "relative MDE makes the target conversion rate invalid")
    pooled = (baseline + target) / 2
    z_alpha = NormalDist().inv_cdf(1 - alpha / 2)
    z_power = NormalDist().inv_cdf(power)
    numerator = (
        z_alpha * math.sqrt(2 * pooled * (1 - pooled))
        + z_power * math.sqrt(baseline * (1 - baseline) + target * (1 - target))
    ) ** 2
    denominator = (target - baseline) ** 2
    per_arm = math.ceil(numerator / denominator)
    _require(per_arm > 0, "calculated sample must be positive")
    return per_arm, baseline, target


def validate_plan(plan: Mapping[str, Any]) -> None:
    root = _exact_object(plan, PLAN_KEYS, "CTA sample plan")
    _require(root["schema_version"] == 1, "CTA sample plan schema drift")
    _require(root["experiment_id"] == "vevo-sk-product-cta-color-001", "CTA experiment drift")
    _require(root["source_experiment_id"] == "vevo-sk-aa-001", "CTA A/A source drift")
    _require(root["timezone"] == "Europe/Bratislava", "CTA timezone drift")
    _require(
        root["status"] in {"pending_aa_pass_and_final_sample_freeze", "sample_frozen_activation_still_blocked"},
        "CTA sample status drift",
    )
    _require(
        root["population_definition"] == "first_valid_aa_product_page_exposure_proxy",
        "CTA population drift",
    )
    _require(
        root["primary_metric"] == "add_to_cart_within_24h_per_first_product_page_exposed_device",
        "CTA primary metric drift",
    )
    _require(
        root["planning_method"] == "two_sample_proportions_normal_approximation_equal_allocation",
        "CTA planning method drift",
    )
    _require(root["expected_variation_weights"] == {"control": 0.5, "brand_contrast": 0.5}, "CTA weights drift")
    relative_mde = _number(root["relative_mde_percent"], "relative_mde_percent", minimum=0.01, maximum=500)
    power = _number(root["power_percent"], "power_percent", minimum=50.000001, maximum=99.999999)
    alpha = _number(root["two_sided_alpha_percent"], "two_sided_alpha_percent", minimum=0.000001, maximum=49.999999)
    minimum_baseline = _integer(root["minimum_baseline_exposed_devices"], "minimum_baseline_exposed_devices", minimum=2)
    minimum_days = _integer(root["minimum_full_calendar_days"], "minimum_full_calendar_days", minimum=1)
    maximum_days = _integer(root["maximum_full_calendar_days"], "maximum_full_calendar_days", minimum=minimum_days)
    _require((minimum_days, maximum_days) == (14, 42), "CTA duration contract drift")
    _require(root["activation_allowed"] is False, "sample freeze must never activate CTA")

    provisional = _exact_object(root["provisional"], PROVISIONAL_KEYS, "CTA provisional plan")
    _require(provisional["source"] == "ga4_2026-07-23_2026-08-19_diagnostic", "CTA provisional source drift")
    exposed = _integer(provisional["exposed_devices"], "provisional.exposed_devices", minimum=minimum_baseline)
    converted = _integer(provisional["converted_devices"], "provisional.converted_devices", minimum=1)
    per_arm, _baseline, _target = calculate_sample_per_arm(
        exposed_devices=exposed,
        converted_devices=converted,
        relative_mde_percent=relative_mde,
        power_percent=power,
        alpha_percent=alpha,
    )
    _require(provisional["sample_per_arm"] == per_arm, "CTA provisional per-arm sample drift")
    _require(provisional["total_sample"] == 2 * per_arm, "CTA provisional total sample drift")

    final = _exact_object(root["final"], FINAL_KEYS, "CTA final plan")
    if root["status"] == "pending_aa_pass_and_final_sample_freeze":
        _require(all(value is None for value in final.values()), "pending CTA final plan must be empty")
        _require(
            root["next_gate"]
            == "after_aa_pass_record_hash_bound_product_page_baseline_then_review_full_cta_decision_contract",
            "pending CTA next gate drift",
        )
        return

    for field in ("observation_sha256", "aa_snapshot_sha256"):
        _require(HASH_RE.fullmatch(str(final[field] or "")) is not None, f"final.{field} is invalid")
    started = _parse_utc(final["aa_window_started_at_utc"], "final.aa_window_started_at_utc")
    ended = _parse_utc(final["aa_window_ended_at_utc"], "final.aa_window_ended_at_utc")
    frozen = _parse_utc(final["frozen_at_utc"], "final.frozen_at_utc")
    _require(ended - started >= timedelta(days=7), "A/A baseline window is shorter than seven days")
    _require(frozen >= ended, "CTA sample was frozen before the A/A window ended")
    final_exposed = _integer(final["exposed_devices"], "final.exposed_devices", minimum=minimum_baseline)
    final_converted = _integer(final["converted_devices"], "final.converted_devices", minimum=1)
    expected_per_arm, baseline, target = calculate_sample_per_arm(
        exposed_devices=final_exposed,
        converted_devices=final_converted,
        relative_mde_percent=relative_mde,
        power_percent=power,
        alpha_percent=alpha,
    )
    _require(final["baseline_rate_percent"] == round(100 * baseline, 6), "CTA final baseline drift")
    _require(final["target_rate_percent"] == round(100 * target, 6), "CTA final target drift")
    _require(final["sample_per_arm"] == expected_per_arm, "CTA final per-arm sample drift")
    _require(final["total_sample"] == 2 * expected_per_arm, "CTA final total sample drift")
    _require(root["next_gate"] == "review_full_cta_decision_contract_before_activation", "frozen CTA next gate drift")


def validate_observation(observation: Mapping[str, Any], plan: Mapping[str, Any]) -> None:
    row = _exact_object(observation, OBSERVATION_KEYS, "CTA sample observation")
    _require(row["schema_version"] == 1, "CTA sample observation schema drift")
    _require(row["source_experiment_id"] == plan["source_experiment_id"], "CTA source experiment mismatch")
    _require(HASH_RE.fullmatch(str(row["aa_snapshot_sha256"] or "")) is not None, "A/A snapshot hash is invalid")
    _require(row["population_definition"] == plan["population_definition"], "CTA population mismatch")
    _require(row["contains_device_or_event_identity"] is False, "CTA observation contains identity")
    _require(row["contains_customer_or_order_data"] is False, "CTA observation contains commerce data")
    started = _parse_utc(row["aa_window_started_at_utc"], "aa_window_started_at_utc")
    ended = _parse_utc(row["aa_window_ended_at_utc"], "aa_window_ended_at_utc")
    _require(ended - started >= timedelta(days=7), "A/A baseline window is shorter than seven days")
    exposed = _integer(
        row["exposed_devices"],
        "exposed_devices",
        minimum=_integer(plan["minimum_baseline_exposed_devices"], "minimum_baseline_exposed_devices", minimum=2),
    )
    converted = _integer(row["converted_devices"], "converted_devices", minimum=1)
    _require(converted < exposed, "converted_devices must be below exposed_devices")


def _load_object(path: Path, field: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise CtaSampleFreezeError(f"{field} cannot be read") from exc
    _require(isinstance(value, dict), f"{field} must be an object")
    return value


def load_canonical_object(path: Path, field: str) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise CtaSampleFreezeError(f"{field} cannot be read") from exc
    value = _load_object(path, field)
    _require(raw == canonical_json_bytes(value), f"{field} is not canonical")
    return value


def load_canonical_observation(path: Path) -> dict[str, Any]:
    return load_canonical_object(path, "CTA sample observation")


def _updated_workspace(
    workspace: Mapping[str, Any], *, final: Mapping[str, Any]
) -> dict[str, Any]:
    updated = json.loads(json.dumps(workspace))
    experiments = updated.get("experiments")
    _require(isinstance(experiments, list), "GrowthBook workspace experiments are missing")
    matches = [row for row in experiments if row.get("tracking_key") == "vevo-sk-product-cta-color-001"]
    _require(len(matches) == 1, "GrowthBook CTA experiment identity drift")
    cta = matches[0]
    _require(cta.get("status") == "draft", "GrowthBook CTA experiment is already running")
    _require(cta.get("feature_rule_status") == "draft", "GrowthBook CTA feature rule is not a draft")
    _require(cta.get("production_allocation_percent") == 0, "GrowthBook CTA Production allocation is nonzero")
    _require(cta.get("provisional_total_sample") == 1084, "GrowthBook CTA provisional sample drift")
    _require(
        cta.get("final_sample_status") == "recompute_and_freeze_from_aa_before_launch",
        "GrowthBook CTA sample is not in the pending state",
    )
    cta["final_sample_status"] = "frozen_from_hash_bound_aa_activation_still_blocked"
    cta["final_sample_per_arm"] = final["sample_per_arm"]
    cta["final_total_sample"] = final["total_sample"]
    cta["sample_observation_sha256"] = final["observation_sha256"]
    cta["aa_snapshot_sha256"] = final["aa_snapshot_sha256"]
    gates = updated.get("decision_gates") or {}
    _require(gates.get("production_activation_allowed") is False, "Production activation gate is open")
    _require(gates.get("price_tests_allowed") is False, "price tests are enabled")
    return updated


def freeze_sample(
    plan: Mapping[str, Any],
    workspace: Mapping[str, Any],
    observation: Mapping[str, Any],
    aa_snapshot: Mapping[str, Any],
    *,
    observation_sha256: str,
    aa_snapshot_sha256: str,
    frozen_at_utc: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    validate_plan(plan)
    _require(plan["status"] == "pending_aa_pass_and_final_sample_freeze", "CTA sample is already frozen")
    validate_observation(observation, plan)
    _require(HASH_RE.fullmatch(observation_sha256) is not None, "observation SHA-256 is invalid")
    _require(
        hashlib.sha256(canonical_json_bytes(observation)).hexdigest() == observation_sha256,
        "observation SHA-256 mismatch",
    )
    _require(HASH_RE.fullmatch(aa_snapshot_sha256) is not None, "A/A snapshot SHA-256 is invalid")
    _require(
        hashlib.sha256(canonical_json_bytes(aa_snapshot)).hexdigest() == aa_snapshot_sha256,
        "A/A snapshot SHA-256 mismatch",
    )
    _require(
        observation["aa_snapshot_sha256"] == aa_snapshot_sha256,
        "CTA observation is not bound to the supplied A/A snapshot",
    )
    try:
        aa_decision = evaluate_aa(aa_snapshot, load_aa_config(AA_CONFIG_PATH))
    except AaEvaluationError as exc:
        raise CtaSampleFreezeError(f"A/A snapshot is invalid: {exc}") from exc
    _require(aa_decision["verdict"] == "PASS", "CTA sample cannot be frozen before A/A PASS")
    _require(aa_decision["winner_calls_allowed"] is False, "A/A decision attempted a winner call")
    _require(
        aa_snapshot.get("full_allocation_started_at_utc")
        == observation["aa_window_started_at_utc"],
        "CTA observation start differs from the A/A snapshot",
    )
    _require(
        aa_decision["evaluated_at_utc"] == observation["aa_window_ended_at_utc"],
        "CTA observation end differs from the A/A decision",
    )
    frozen = _parse_utc(frozen_at_utc, "frozen_at_utc")
    ended = _parse_utc(observation["aa_window_ended_at_utc"], "aa_window_ended_at_utc")
    _require(frozen >= ended, "CTA sample cannot be frozen before the A/A window ends")
    per_arm, baseline, target = calculate_sample_per_arm(
        exposed_devices=observation["exposed_devices"],
        converted_devices=observation["converted_devices"],
        relative_mde_percent=plan["relative_mde_percent"],
        power_percent=plan["power_percent"],
        alpha_percent=plan["two_sided_alpha_percent"],
    )
    updated_plan = json.loads(json.dumps(plan))
    updated_plan["status"] = "sample_frozen_activation_still_blocked"
    updated_plan["final"] = {
        "observation_sha256": observation_sha256,
        "aa_snapshot_sha256": observation["aa_snapshot_sha256"],
        "aa_window_started_at_utc": observation["aa_window_started_at_utc"],
        "aa_window_ended_at_utc": observation["aa_window_ended_at_utc"],
        "exposed_devices": observation["exposed_devices"],
        "converted_devices": observation["converted_devices"],
        "baseline_rate_percent": round(100 * baseline, 6),
        "target_rate_percent": round(100 * target, 6),
        "sample_per_arm": per_arm,
        "total_sample": 2 * per_arm,
        "frozen_at_utc": frozen_at_utc,
    }
    updated_plan["next_gate"] = "review_full_cta_decision_contract_before_activation"
    validate_plan(updated_plan)
    updated_workspace = _updated_workspace(workspace, final=updated_plan["final"])
    return updated_plan, updated_workspace


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = (json.dumps(value, indent=2, ensure_ascii=False) + "\n").encode("utf-8")
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=f".{path.name}.", delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(body)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, default=DEFAULT_PLAN_PATH)
    parser.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE_PATH)
    parser.add_argument("--observation", type=Path, required=True)
    parser.add_argument("--observation-sha256", required=True)
    parser.add_argument("--aa-snapshot", type=Path, required=True)
    parser.add_argument("--aa-snapshot-sha256", required=True)
    parser.add_argument("--frozen-at-utc", required=True)
    parser.add_argument("--plan-output", type=Path, required=True)
    parser.add_argument("--workspace-output", type=Path, required=True)
    args = parser.parse_args()
    try:
        plan = _load_object(args.plan, "CTA sample plan")
        workspace = _load_object(args.workspace, "GrowthBook workspace")
        observation = load_canonical_observation(args.observation)
        aa_snapshot = load_canonical_object(args.aa_snapshot, "A/A snapshot")
        updated_plan, updated_workspace = freeze_sample(
            plan,
            workspace,
            observation,
            aa_snapshot,
            observation_sha256=args.observation_sha256,
            aa_snapshot_sha256=args.aa_snapshot_sha256,
            frozen_at_utc=args.frozen_at_utc,
        )
        _require(args.plan_output.resolve() != args.workspace_output.resolve(), "output paths must differ")
        _write_json(args.plan_output, updated_plan)
        _write_json(args.workspace_output, updated_workspace)
        print(
            "CTA_SAMPLE_FROZEN:"
            f"per_arm={updated_plan['final']['sample_per_arm']}:"
            f"total={updated_plan['final']['total_sample']}:activation_allowed=false"
        )
        return 0
    except (CtaSampleFreezeError, OSError) as exc:
        print(f"freeze_growthbook_cta_sample.py: FAIL: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
