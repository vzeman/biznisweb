"""Offline, hash-bound GrowthBook Pro upgrade and quantile-metric recorder."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence


ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = ROOT / "projects" / "vevo" / "growthbook_pro_upgrade.json"
WORKSPACE_PATH = ROOT / "projects" / "vevo" / "growthbook_workspace.json"
COMPLETION_PATH = ROOT / "projects" / "vevo" / "growthbook_production_aa_completion.json"
OBSERVATION_PATH = ROOT / "projects" / "vevo" / "growthbook_pro_upgrade_observation.json"

WAITING = "waiting_for_verified_aa_completion"
REVIEW_OPEN = "manual_paid_upgrade_review_open"
VERIFIED = "pro_active_quantile_metrics_verified_cta_still_blocked"
POST_AA_BLOCKED = "production_aa_completed_cta_sample_freeze_pending_pro_quantiles_blocked"
POST_AA_VERIFIED = "production_aa_completed_cta_sample_freeze_pro_quantiles_verified"
METRIC_KEYS = [
    "vevo_lcp_p75_24h",
    "vevo_inp_p75_24h",
    "vevo_cls_p75_milli_24h",
]
SORTED_METRIC_KEYS = sorted(METRIC_KEYS)
CTA_GUARDRAILS = ["vevo_client_error_device_rate_24h", *METRIC_KEYS]
HEX_64 = re.compile(r"[0-9a-f]{64}")
METRIC_ID = re.compile(r"fact__[A-Za-z0-9]+")


class ProUpgradeError(ValueError):
    """Raised when the paid upgrade handoff fails closed."""


def canonical_json_bytes(value: Mapping[str, Any]) -> bytes:
    return (json.dumps(value, sort_keys=True, indent=2) + "\n").encode("utf-8")


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ProUpgradeError(message)


def _exact(value: Any, keys: set[str], label: str) -> Mapping[str, Any]:
    _require(isinstance(value, dict), f"{label} must be an object")
    _require(set(value) == keys, f"{label} field set drift")
    return value


def _load(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProUpgradeError(f"{label} is unreadable") from exc
    _require(isinstance(value, dict), f"{label} must be an object")
    return value


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _timestamp(value: Any, label: str) -> datetime:
    _require(isinstance(value, str) and value.endswith("Z"), f"{label} must be UTC Z")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ProUpgradeError(f"{label} is invalid") from exc
    _require(parsed.tzinfo is not None and parsed.utcoffset() == timezone.utc.utcoffset(parsed), f"{label} must be UTC")
    return parsed


def _positive_number(value: Any, label: str) -> float:
    _require(isinstance(value, (int, float)) and not isinstance(value, bool), f"{label} must be numeric")
    numeric = float(value)
    _require(math.isfinite(numeric) and numeric > 0, f"{label} must be positive and finite")
    return numeric


def metric_contract(metric: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: copy.deepcopy(metric[key])
        for key in (
            "key",
            "name",
            "fact_table",
            "type",
            "aggregation",
            "row_filters",
            "quantile",
            "group_by_experiment_user",
            "ignore_zeros",
            "goal",
            "growthbook_window",
            "growthbook_window_hours",
            "roles",
        )
    }


def metric_contract_hashes(workspace: Mapping[str, Any]) -> dict[str, str]:
    metrics = {row.get("key"): row for row in workspace.get("metrics", []) if isinstance(row, dict)}
    _require(set(METRIC_KEYS) <= set(metrics), "GrowthBook p75 metric contract is incomplete")
    return {
        key: hashlib.sha256(canonical_json_bytes(metric_contract(metrics[key]))).hexdigest()
        for key in SORTED_METRIC_KEYS
    }


def _validate_target(target: Mapping[str, Any], workspace: Mapping[str, Any]) -> None:
    _exact(
        target,
        {
            "organization_name", "project_name", "from_plan", "to_plan", "seat_count",
            "currency", "base_monthly_price", "billing_period", "recurring_subscription",
            "official_pricing_url", "quantile_metric_keys", "cta_guardrail_metrics",
            "metric_contract_sha256",
        },
        "target",
    )
    _require(target["organization_name"] == "Vevo" and target["project_name"] == "VEVO SK Web", "GrowthBook target identity drift")
    _require(target["from_plan"] == "starter" and target["to_plan"] == "pro", "GrowthBook target plan drift")
    _require(target["seat_count"] == 1 and target["currency"] == "USD", "GrowthBook seat/currency drift")
    _require(target["base_monthly_price"] == 40 and target["billing_period"] == "monthly", "GrowthBook price/period drift")
    _require(target["recurring_subscription"] is True, "GrowthBook recurring-subscription disclosure drift")
    _require(target["official_pricing_url"] == "https://www.growthbook.io/pricing", "GrowthBook pricing source drift")
    _require(target["quantile_metric_keys"] == METRIC_KEYS, "GrowthBook p75 metric order drift")
    _require(target["cta_guardrail_metrics"] == CTA_GUARDRAILS, "GrowthBook CTA guardrail contract drift")
    hashes = _exact(target["metric_contract_sha256"], set(SORTED_METRIC_KEYS), "metric_contract_sha256")
    expected = metric_contract_hashes(workspace)
    for key in SORTED_METRIC_KEYS:
        _require(hashes[key] == expected[key], f"GrowthBook p75 metric contract hash drift: {key}")


def validate_manifest(manifest: Mapping[str, Any], workspace: Mapping[str, Any]) -> None:
    _exact(
        manifest,
        {"schema_version", "transition_type", "status", "source_bindings", "target", "authorization", "verification", "release_boundaries", "next_gate"},
        "manifest",
    )
    _require(manifest["schema_version"] == 1 and manifest["transition_type"] == "vevo_growthbook_pro_upgrade_and_quantile_metrics", "GrowthBook Pro manifest identity drift")
    _require(manifest["status"] in {WAITING, REVIEW_OPEN, VERIFIED}, "GrowthBook Pro manifest status drift")
    sources = _exact(manifest["source_bindings"], {"aa_completion", "workspace_before_upgrade"}, "source_bindings")
    for name, expected_path in {
        "aa_completion": "projects/vevo/growthbook_production_aa_completion.json",
        "workspace_before_upgrade": "projects/vevo/growthbook_workspace.json",
    }.items():
        binding = _exact(sources[name], {"path", "sha256"}, f"source_bindings.{name}")
        _require(binding["path"] == expected_path, f"GrowthBook Pro source path drift: {name}")
        if manifest["status"] == WAITING:
            _require(binding["sha256"] is None, f"GrowthBook Pro source bound before review: {name}")
        else:
            _require(isinstance(binding["sha256"], str) and HEX_64.fullmatch(binding["sha256"]) is not None, f"GrowthBook Pro source hash missing: {name}")
    _validate_target(manifest["target"], workspace)
    authorization = _exact(manifest["authorization"], {"status", "authorized_at_utc", "confirmed_seat_count", "confirmed_base_monthly_price", "confirmed_recurring_subscription", "action_time_confirmation_required"}, "authorization")
    _require(authorization["action_time_confirmation_required"] is True, "GrowthBook paid action-time confirmation must remain required")
    verification = _exact(manifest["verification"], {"status", "observation_file", "observation_sha256", "observed_at_utc", "preview_metric_ids", "production_metric_ids"}, "verification")
    _require(verification["observation_file"] == "projects/vevo/growthbook_pro_upgrade_observation.json", "GrowthBook Pro observation path drift")
    preview_ids = _exact(verification["preview_metric_ids"], set(SORTED_METRIC_KEYS), "preview_metric_ids")
    production_ids = _exact(verification["production_metric_ids"], set(SORTED_METRIC_KEYS), "production_metric_ids")
    boundaries = _exact(manifest["release_boundaries"], {"manual_paid_upgrade_allowed", "automatic_paid_upgrade_allowed", "automatic_growthbook_mutation_allowed", "cta_activation_allowed", "gtm_mutation_allowed", "meta_ads_mutation_allowed", "biznisweb_mutation_allowed", "collector_or_reporting_mutation_allowed", "price_product_stock_cart_checkout_payment_or_order_mutation_allowed"}, "release_boundaries")
    _require(all(boundaries[key] is False for key in boundaries if key != "manual_paid_upgrade_allowed"), "GrowthBook Pro automatic/external release boundary opened")
    if manifest["status"] == WAITING:
        _require(authorization == {"status": "not_recorded", "authorized_at_utc": None, "confirmed_seat_count": None, "confirmed_base_monthly_price": None, "confirmed_recurring_subscription": None, "action_time_confirmation_required": True}, "GrowthBook Pro authorization opened early")
        _require(verification["status"] == "not_recorded" and verification["observation_sha256"] is None and verification["observed_at_utc"] is None, "GrowthBook Pro verification opened early")
        _require(all(value is None for value in [*preview_ids.values(), *production_ids.values()]), "GrowthBook Pro metric IDs recorded early")
        _require(boundaries["manual_paid_upgrade_allowed"] is False and manifest["next_gate"] == "wait_for_verified_aa_pass_and_zero_allocation_stop_readback", "GrowthBook Pro waiting gate drift")
    elif manifest["status"] == REVIEW_OPEN:
        _require(authorization["status"] == "explicit_action_time_confirmation_recorded", "GrowthBook Pro authorization missing")
        _timestamp(authorization["authorized_at_utc"], "authorization.authorized_at_utc")
        _require(authorization["confirmed_seat_count"] == 1 and authorization["confirmed_base_monthly_price"] == 40 and authorization["confirmed_recurring_subscription"] is True, "GrowthBook Pro authorized offer drift")
        _require(verification["status"] == "not_recorded" and verification["observation_sha256"] is None, "GrowthBook Pro verification recorded before UI readback")
        _require(all(value is None for value in [*preview_ids.values(), *production_ids.values()]), "GrowthBook Pro metric IDs recorded before UI readback")
        _require(boundaries["manual_paid_upgrade_allowed"] is True and manifest["next_gate"] == "manually_upgrade_then_create_and_query_test_six_quantile_metrics", "GrowthBook Pro manual review gate drift")
    else:
        _require(authorization["status"] == "consumed_by_verified_upgrade" and boundaries["manual_paid_upgrade_allowed"] is False, "GrowthBook Pro authorization not safely closed")
        _timestamp(authorization["authorized_at_utc"], "authorization.authorized_at_utc")
        _require(verification["status"] == "verified_pro_active_quantile_metrics", "GrowthBook Pro verification status drift")
        _require(isinstance(verification["observation_sha256"], str) and HEX_64.fullmatch(verification["observation_sha256"]) is not None, "GrowthBook Pro observation hash missing")
        _timestamp(verification["observed_at_utc"], "verification.observed_at_utc")
        all_ids = [*preview_ids.values(), *production_ids.values()]
        _require(all(isinstance(value, str) and METRIC_ID.fullmatch(value) is not None for value in all_ids), "GrowthBook Pro metric ID invalid")
        _require(len(set(all_ids)) == 6, "GrowthBook Pro metric IDs must be unique across Preview and Production")
        _require(manifest["next_gate"] == "collect_24h_cta_baseline_then_freeze_sample_with_cta_still_blocked", "GrowthBook Pro verified next gate drift")


def validate_observation(observation: Mapping[str, Any], manifest: Mapping[str, Any], workspace: Mapping[str, Any]) -> None:
    _require(
        workspace.get("state") in {POST_AA_BLOCKED, POST_AA_VERIFIED},
        "GrowthBook Pro observation workspace state drift",
    )
    _exact(observation, {"schema_version", "observation_type", "observed_at_utc", "organization", "billing", "quantile_metrics", "cta_draft", "control", "safety"}, "observation")
    _require(observation["schema_version"] == 1 and observation["observation_type"] == "vevo_growthbook_pro_upgrade_observation", "GrowthBook Pro observation identity drift")
    _timestamp(observation["observed_at_utc"], "observation.observed_at_utc")
    organization = _exact(observation["organization"], {"name", "id", "project_name", "project_id"}, "organization")
    _require(organization == {"name": "Vevo", "id": "org_19g6mmt1q79o1", "project_name": "VEVO SK Web", "project_id": "prj_2CeEJc6J9FwQFix9UhsnKr"}, "GrowthBook Pro organization/project drift")
    billing = _exact(observation["billing"], {"plan", "status", "seat_count", "currency", "base_monthly_price", "billing_period", "recurring_subscription"}, "billing")
    _require(billing == {"plan": "pro", "status": "pro_active_paid_monthly_one_seat", "seat_count": 1, "currency": "USD", "base_monthly_price": 40, "billing_period": "monthly", "recurring_subscription": True}, "GrowthBook Pro billing readback drift")
    metric_rows = _exact(observation["quantile_metrics"], set(SORTED_METRIC_KEYS), "quantile_metrics")
    expected_hashes = metric_contract_hashes(workspace)
    all_ids: list[str] = []
    for key in SORTED_METRIC_KEYS:
        row = _exact(metric_rows[key], {"preview_metric_id", "production_metric_id", "contract_sha256", "preview_configuration_readback_match", "production_configuration_readback_match", "preview_query_test_passed", "production_query_test_passed"}, f"quantile_metrics.{key}")
        _require(row["contract_sha256"] == expected_hashes[key] == manifest["target"]["metric_contract_sha256"][key], f"GrowthBook Pro metric contract drift: {key}")
        ids = [row["preview_metric_id"], row["production_metric_id"]]
        _require(all(isinstance(value, str) and METRIC_ID.fullmatch(value) is not None for value in ids), f"GrowthBook Pro metric ID invalid: {key}")
        _require(all(row[field] is True for field in ("preview_configuration_readback_match", "production_configuration_readback_match", "preview_query_test_passed", "production_query_test_passed")), f"GrowthBook Pro metric readback/query incomplete: {key}")
        all_ids.extend(ids)
    _require(len(set(all_ids)) == 6, "GrowthBook Pro Preview/Production metric IDs are reused")
    workspace_verified = workspace.get("state") == POST_AA_VERIFIED
    workspace_metrics = {
        row.get("key"): row
        for row in workspace.get("metrics", [])
        if isinstance(row, dict)
    }
    if workspace_verified:
        for key in SORTED_METRIC_KEYS:
            _require(
                workspace_metrics.get(key, {}).get("growthbook_id")
                == metric_rows[key]["preview_metric_id"]
                and workspace_metrics.get(key, {}).get("production_growthbook_id")
                == metric_rows[key]["production_metric_id"],
                f"GrowthBook Pro verified workspace metric ID drift: {key}",
            )
    old_ids = {
        row.get("growthbook_id")
        for row in workspace.get("metrics", [])
        if isinstance(row, dict)
        and row.get("growthbook_id")
        and (not workspace_verified or row.get("key") not in METRIC_KEYS)
    }
    clone = workspace["athena"]["production"]["growthbook_clone"]
    old_ids.update(
        value
        for key, value in clone.get("source_metric_ids", {}).items()
        if value and (not workspace_verified or key not in METRIC_KEYS)
    )
    old_ids.update(
        value
        for key, value in clone.get("target_metric_ids", {}).items()
        if value and (not workspace_verified or key not in METRIC_KEYS)
    )
    _require(not (set(all_ids) & old_ids), "GrowthBook Pro metric ID reuses an existing metric")
    cta = _exact(observation["cta_draft"], {"experiment_id", "status", "production_allocation_percent", "guardrail_metrics"}, "cta_draft")
    _require(cta == {"experiment_id": "exp_19g6mmt1qxzrp", "status": "draft", "production_allocation_percent": 0, "guardrail_metrics": CTA_GUARDRAILS}, "GrowthBook Pro CTA draft drift")
    control = _exact(observation["control"], {"aa_status", "aa_production_allocation_percent", "active_production_experiments", "gtm_container_version_id", "gtm_unprocessed_changes"}, "control")
    _require(control == {"aa_status": "stopped", "aa_production_allocation_percent": 0, "active_production_experiments": [], "gtm_container_version_id": "15", "gtm_unprocessed_changes": 0}, "GrowthBook Pro control-plane drift")
    safety = _exact(observation["safety"], {"contains_credentials", "contains_payment_method_or_card_data", "contains_invoice_address_or_tax_id", "contains_user_email", "contains_event_or_device_ids", "contains_customer_or_order_data", "gtm_mutated", "meta_ads_mutated", "biznisweb_mutated", "collector_or_reporting_mutated", "price_product_stock_cart_checkout_payment_or_order_mutated"}, "safety")
    _require(not any(safety.values()), "GrowthBook Pro observation contains unsafe data or unrelated mutation")


def open_review(manifest: Mapping[str, Any], workspace: Mapping[str, Any], completion: Mapping[str, Any], *, authorized_at_utc: str, confirm_paid_upgrade: str, confirmed_seat_count: int, confirmed_base_monthly_price: float, confirmed_recurring_subscription: str) -> dict[str, Any]:
    validate_manifest(manifest, workspace)
    _require(manifest["status"] == WAITING, "GrowthBook Pro review is already opened")
    _require(completion.get("status") == "production_aa_stopped_verified_cta_activation_blocked", "GrowthBook Pro requires verified A/A completion")
    _require(completion.get("aa_pass", {}).get("verdict") == "PASS" and completion.get("stop_readback", {}).get("status") == "verified_zero_allocation", "GrowthBook Pro requires A/A PASS and zero-allocation stop")
    _require(workspace.get("state") == POST_AA_BLOCKED and workspace.get("workspace", {}).get("production_allocation_percent") == 0, "GrowthBook Pro requires the post-A/A blocked workspace")
    _require(workspace["workspace"].get("plan_type") == "starter" and workspace["workspace"].get("subscription_or_trial_status") == "starter_active_no_paid_upgrade_accepted", "GrowthBook Pro source plan drift")
    _require(confirm_paid_upgrade == "true", "exact paid-upgrade confirmation is required")
    _require(confirmed_seat_count == 1 and _positive_number(confirmed_base_monthly_price, "confirmed_base_monthly_price") == 40 and confirmed_recurring_subscription == "true", "GrowthBook Pro confirmed offer differs from the reviewed monthly plan")
    _timestamp(authorized_at_utc, "authorized_at_utc")
    updated = copy.deepcopy(manifest)
    updated["source_bindings"]["aa_completion"]["sha256"] = hashlib.sha256(canonical_json_bytes(completion)).hexdigest()
    updated["source_bindings"]["workspace_before_upgrade"]["sha256"] = hashlib.sha256(canonical_json_bytes(workspace)).hexdigest()
    updated["authorization"].update({"status": "explicit_action_time_confirmation_recorded", "authorized_at_utc": authorized_at_utc, "confirmed_seat_count": 1, "confirmed_base_monthly_price": 40, "confirmed_recurring_subscription": True})
    updated["status"] = REVIEW_OPEN
    updated["release_boundaries"]["manual_paid_upgrade_allowed"] = True
    updated["next_gate"] = "manually_upgrade_then_create_and_query_test_six_quantile_metrics"
    validate_manifest(updated, workspace)
    return updated


def record_upgrade(manifest: Mapping[str, Any], workspace: Mapping[str, Any], completion: Mapping[str, Any], observation: Mapping[str, Any], *, expected_observation_sha256: str) -> tuple[dict[str, Any], dict[str, Any]]:
    validate_manifest(manifest, workspace)
    _require(manifest["status"] == REVIEW_OPEN and manifest["release_boundaries"]["manual_paid_upgrade_allowed"] is True, "GrowthBook Pro manual review is not open")
    _require(hashlib.sha256(canonical_json_bytes(completion)).hexdigest() == manifest["source_bindings"]["aa_completion"]["sha256"], "A/A completion changed after GrowthBook Pro review")
    _require(hashlib.sha256(canonical_json_bytes(workspace)).hexdigest() == manifest["source_bindings"]["workspace_before_upgrade"]["sha256"], "GrowthBook workspace changed after GrowthBook Pro review")
    validate_observation(observation, manifest, workspace)
    observation_bytes = canonical_json_bytes(observation)
    actual_hash = hashlib.sha256(observation_bytes).hexdigest()
    _require(HEX_64.fullmatch(expected_observation_sha256 or "") is not None and actual_hash == expected_observation_sha256, "GrowthBook Pro observation SHA-256 mismatch")
    updated_workspace = copy.deepcopy(workspace)
    updated_workspace["state"] = POST_AA_VERIFIED
    updated_workspace["workspace"]["plan_type"] = "pro"
    updated_workspace["workspace"]["subscription_or_trial_status"] = "pro_active_paid_monthly_one_seat"
    clone = updated_workspace["athena"]["production"]["growthbook_clone"]
    clone["paid_pro_upgrade_authorized"] = True
    metric_map = {row["key"]: row for row in updated_workspace["metrics"]}
    verified_date = observation["observed_at_utc"][:10]
    preview_ids: dict[str, str] = {}
    production_ids: dict[str, str] = {}
    for key in SORTED_METRIC_KEYS:
        row = observation["quantile_metrics"][key]
        preview_ids[key] = row["preview_metric_id"]
        production_ids[key] = row["production_metric_id"]
        metric_map[key].update({
            "growthbook_id": row["preview_metric_id"],
            "production_growthbook_id": row["production_metric_id"],
            "status": "growthbook_pro_preview_and_production_created_query_verified",
            "blocker": None,
            "blocker_resolved_date": verified_date,
            "created_verified_date": verified_date,
            "analysis_query_verified_date": verified_date,
        })
    clone["source_metric_ids"].update(preview_ids)
    clone["target_metric_ids"].update(production_ids)
    experiments = {row["tracking_key"]: row for row in updated_workspace["experiments"]}
    cta = experiments["vevo-sk-product-cta-color-001"]
    cta["pro_guardrail_metrics"] = CTA_GUARDRAILS
    cta["pro_quantile_metrics_verified_date"] = verified_date
    updated_manifest = copy.deepcopy(manifest)
    updated_manifest["status"] = VERIFIED
    updated_manifest["authorization"]["status"] = "consumed_by_verified_upgrade"
    updated_manifest["verification"].update({
        "status": "verified_pro_active_quantile_metrics",
        "observation_sha256": actual_hash,
        "observed_at_utc": observation["observed_at_utc"],
        "preview_metric_ids": preview_ids,
        "production_metric_ids": production_ids,
    })
    updated_manifest["release_boundaries"]["manual_paid_upgrade_allowed"] = False
    updated_manifest["next_gate"] = "collect_24h_cta_baseline_then_freeze_sample_with_cta_still_blocked"
    validate_manifest(updated_manifest, updated_workspace)
    validate_observation(observation, updated_manifest, updated_workspace)
    return updated_manifest, updated_workspace


def _write(path: Path, value: Mapping[str, Any]) -> None:
    path.write_bytes(canonical_json_bytes(value))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    open_parser = sub.add_parser("open-review")
    open_parser.add_argument("--authorized-at-utc", required=True)
    open_parser.add_argument("--confirm-paid-upgrade", required=True)
    open_parser.add_argument("--confirmed-seat-count", required=True, type=int)
    open_parser.add_argument("--confirmed-base-monthly-price", required=True, type=float)
    open_parser.add_argument("--confirmed-recurring-subscription", required=True)
    record_parser = sub.add_parser("record")
    record_parser.add_argument("--observation", required=True, type=Path)
    record_parser.add_argument("--expected-observation-sha256", required=True)
    args = parser.parse_args(argv)
    try:
        manifest = _load(MANIFEST_PATH, "GrowthBook Pro manifest")
        workspace = _load(WORKSPACE_PATH, "GrowthBook workspace")
        completion = _load(COMPLETION_PATH, "A/A completion")
        if args.command == "open-review":
            updated = open_review(manifest, workspace, completion, authorized_at_utc=args.authorized_at_utc, confirm_paid_upgrade=args.confirm_paid_upgrade, confirmed_seat_count=args.confirmed_seat_count, confirmed_base_monthly_price=args.confirmed_base_monthly_price, confirmed_recurring_subscription=args.confirmed_recurring_subscription)
            _write(MANIFEST_PATH, updated)
        else:
            observation_bytes = args.observation.read_bytes()
            observation = _load(args.observation, "GrowthBook Pro observation")
            _require(observation_bytes == canonical_json_bytes(observation), "GrowthBook Pro observation is not canonical JSON")
            updated_manifest, updated_workspace = record_upgrade(manifest, workspace, completion, observation, expected_observation_sha256=args.expected_observation_sha256)
            _write(MANIFEST_PATH, updated_manifest)
            _write(WORKSPACE_PATH, updated_workspace)
            _write(OBSERVATION_PATH, observation)
    except (OSError, ProUpgradeError) as exc:
        print(f"record_growthbook_pro_upgrade.py: FAIL: {exc}")
        return 1
    print("record_growthbook_pro_upgrade.py: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
