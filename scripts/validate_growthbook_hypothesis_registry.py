#!/usr/bin/env python3
"""Validate the durable, aggregate-only VEVO GrowthBook hypothesis registry."""

from __future__ import annotations

import copy
import hashlib
import json
import pathlib
import re
import sys
from typing import Any, Mapping, Sequence


ROOT = pathlib.Path(__file__).resolve().parents[1]
DEFAULT_REGISTRY_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_hypothesis_registry.json"
)
DEFAULT_DECISION_CONTRACT_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_cta_decision_contract.json"
)
EXPERIMENT_ID = "vevo-sk-product-cta-color-001"
GROWTHBOOK_EXPERIMENT_ID = "exp_19g6mmt1qxzrp"
PENDING = "preregistered_waiting_for_final_decision"
RECORDED = "final_decision_recorded_manual_action_pending"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
RUN_ID_RE = re.compile(r"^[1-9][0-9]{5,19}$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
UTC_RE = re.compile(r"^20[2-9][0-9]-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")

ROOT_KEYS = {
    "schema_version",
    "registry_type",
    "analytics_ui",
    "audit_source_of_truth",
    "privacy",
    "experiments",
}
PRIVACY_KEYS = {
    "aggregate_evidence_only",
    "pii_allowed",
    "event_or_device_ids_allowed",
    "customer_or_order_data_allowed",
}
EXPERIMENT_KEYS = {
    "experiment_id",
    "growthbook_experiment_id",
    "feature_key",
    "name",
    "hypothesis_version",
    "hypothesis",
    "status",
    "population",
    "allowed_change",
    "variations",
    "expected_variation_weights",
    "primary_metric",
    "business_guardrail",
    "diagnostic_dimensions_only",
    "decision_contract",
    "final_decision",
}
DECISION_CONTRACT_KEYS = {"path", "sha256"}
FINAL_KEYS = {
    "recorded_at_utc",
    "workflow_run_id",
    "main_commit",
    "assignment_started_at_utc",
    "assignment_ended_at_utc",
    "snapshot_sha256",
    "decision_sha256",
    "provenance_sha256",
    "verdict",
    "recommended_variation",
    "automatic_mutation_allowed",
    "aggregate_evidence",
}
DECISION_KEYS = {
    "schema_version",
    "evidence_type",
    "experiment_id",
    "evaluated_at_utc",
    "verdict",
    "final_decision",
    "recommended_variation",
    "automatic_mutation_allowed",
    "summary",
    "primary_metric",
    "supporting_diagnostics",
    "gates",
}
FORBIDDEN_IDENTITY_KEYS = {
    "device_id",
    "event_id",
    "transaction_id",
    "customer_id",
    "customer_email",
    "email",
    "order_id",
    "ip",
    "ip_address",
    "full_url",
    "click_id",
    "gclid",
    "fbclid",
    "phone",
    "first_name",
    "last_name",
    "address",
}


class HypothesisRegistryError(ValueError):
    """Raised when the durable registry is not exact, safe, or hash-bound."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise HypothesisRegistryError(message)


def _exact(value: Any, keys: set[str], field: str) -> Mapping[str, Any]:
    _require(isinstance(value, Mapping), f"{field} must be an object")
    _require(set(value) == keys, f"{field} keys drift")
    return value


def pretty_json_bytes(value: Mapping[str, Any]) -> bytes:
    return (json.dumps(value, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def _decision_json_bytes(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
    ).encode("utf-8")


def _reject_identity_data(value: Any, path: str = "registry") -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            normalized = str(key).lower()
            _require(
                normalized not in FORBIDDEN_IDENTITY_KEYS,
                f"{path} contains forbidden identity field: {key}",
            )
            _reject_identity_data(nested, f"{path}.{key}")
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            _reject_identity_data(nested, f"{path}[{index}]")
    elif isinstance(value, str):
        _require("@" not in value, f"{path} contains an email-like value")


def _validate_aggregate_decision(value: Any) -> Mapping[str, Any]:
    decision = _exact(value, DECISION_KEYS, "registry aggregate decision")
    _require(decision["schema_version"] == 1, "registry decision schema drift")
    _require(
        decision["evidence_type"] == "vevo_growthbook_cta_decision",
        "registry decision evidence type drift",
    )
    _require(
        decision["experiment_id"] == EXPERIMENT_ID, "registry decision experiment drift"
    )
    _require(
        UTC_RE.fullmatch(str(decision["evaluated_at_utc"] or "")) is not None,
        "registry decision time is invalid",
    )
    _require(
        decision["verdict"] in {"WIN", "LOSE", "INCONCLUSIVE"},
        "registry decision verdict drift",
    )
    _require(decision["final_decision"] is True, "registry decision is not final")
    _require(
        decision["recommended_variation"] in {"control", "brand_contrast"},
        "registry decision recommendation drift",
    )
    _require(
        decision["automatic_mutation_allowed"] is False,
        "registry decision opened automatic mutation",
    )
    _require(
        isinstance(decision["summary"], Mapping), "registry decision summary is invalid"
    )
    _require(
        isinstance(decision["primary_metric"], Mapping),
        "registry primary result is invalid",
    )
    _require(
        isinstance(decision["supporting_diagnostics"], Mapping),
        "registry diagnostics are invalid",
    )
    _require(
        isinstance(decision["gates"], list) and decision["gates"],
        "registry guardrails are missing",
    )
    return decision


def validate_registry(
    registry: Mapping[str, Any],
    final_manifest: Mapping[str, Any] | None = None,
    *,
    decision_contract_path: pathlib.Path = DEFAULT_DECISION_CONTRACT_PATH,
) -> None:
    root = _exact(registry, ROOT_KEYS, "hypothesis registry")
    _require(root["schema_version"] == 1, "hypothesis registry schema drift")
    _require(
        root["registry_type"] == "vevo_growthbook_experiment_hypothesis_registry",
        "hypothesis registry type drift",
    )
    _require(root["analytics_ui"] == "GrowthBook", "hypothesis analytics UI drift")
    _require(root["audit_source_of_truth"] == "Git", "hypothesis audit source drift")
    privacy = _exact(root["privacy"], PRIVACY_KEYS, "hypothesis registry privacy")
    _require(
        privacy
        == {
            "aggregate_evidence_only": True,
            "pii_allowed": False,
            "event_or_device_ids_allowed": False,
            "customer_or_order_data_allowed": False,
        },
        "hypothesis registry privacy boundary drift",
    )
    experiments = root["experiments"]
    _require(
        isinstance(experiments, list) and len(experiments) == 1,
        "hypothesis registry experiment set drift",
    )
    experiment = _exact(
        experiments[0], EXPERIMENT_KEYS, "hypothesis registry experiment"
    )
    _require(
        experiment["experiment_id"] == EXPERIMENT_ID, "hypothesis experiment ID drift"
    )
    _require(
        experiment["growthbook_experiment_id"] == GROWTHBOOK_EXPERIMENT_ID,
        "GrowthBook experiment ID drift",
    )
    _require(
        experiment["feature_key"] == "vevo-sk-product-cta-color",
        "hypothesis feature drift",
    )
    _require(experiment["name"] == "VEVO SK product CTA color", "hypothesis name drift")
    _require(experiment["hypothesis_version"] == 1, "hypothesis version drift")
    _require(
        experiment["hypothesis"]
        == "Changing only the product-detail add-to-cart CTA background from the current control color to one approved, accessible, high-contrast VEVO brand color will increase the share of exposed product viewers who add a product to cart because the primary action is easier to notice.",
        "hypothesis text drift",
    )
    _require(experiment["status"] in {PENDING, RECORDED}, "hypothesis status drift")
    _require(
        isinstance(experiment["population"], str) and experiment["population"],
        "hypothesis population missing",
    )
    _require(
        isinstance(experiment["allowed_change"], str) and experiment["allowed_change"],
        "hypothesis change boundary missing",
    )
    _require(
        experiment["variations"] == ["control", "brand_contrast"],
        "hypothesis variations drift",
    )
    _require(
        experiment["expected_variation_weights"]
        == {"control": 0.5, "brand_contrast": 0.5},
        "hypothesis weights drift",
    )
    _require(
        experiment["primary_metric"]
        == "add_to_cart_within_24h_per_first_rendered_cta_exposed_device",
        "hypothesis primary metric drift",
    )
    _require(
        experiment["business_guardrail"] == "vevo_cm1_per_exposed_device_7d",
        "hypothesis business guardrail drift",
    )
    _require(
        experiment["diagnostic_dimensions_only"]
        == [
            "meta_campaign_id",
            "meta_adset_id",
            "meta_ad_id",
            "meta_placement",
            "device_type",
            "new_or_returning_device",
        ],
        "hypothesis diagnostic dimensions drift",
    )
    contract = _exact(
        experiment["decision_contract"],
        DECISION_CONTRACT_KEYS,
        "hypothesis decision contract",
    )
    _require(
        contract["path"] == "projects/vevo/growthbook_cta_decision_contract.json",
        "hypothesis decision contract path drift",
    )
    _require(
        decision_contract_path.is_file(), "hypothesis decision contract is missing"
    )
    _require(
        contract["sha256"]
        == hashlib.sha256(decision_contract_path.read_bytes()).hexdigest(),
        "hypothesis decision contract hash drift",
    )

    final = experiment["final_decision"]
    if experiment["status"] == PENDING:
        _require(final is None, "pending hypothesis already has a final decision")
    else:
        record = _exact(final, FINAL_KEYS, "hypothesis final decision")
        for field in (
            "recorded_at_utc",
            "assignment_started_at_utc",
            "assignment_ended_at_utc",
        ):
            _require(
                UTC_RE.fullmatch(str(record[field] or "")) is not None,
                f"hypothesis {field} is invalid",
            )
        _require(
            RUN_ID_RE.fullmatch(str(record["workflow_run_id"] or "")) is not None,
            "hypothesis workflow run ID is invalid",
        )
        _require(
            COMMIT_RE.fullmatch(str(record["main_commit"] or "")) is not None,
            "hypothesis main commit is invalid",
        )
        for field in ("snapshot_sha256", "decision_sha256", "provenance_sha256"):
            _require(
                SHA256_RE.fullmatch(str(record[field] or "")) is not None,
                f"hypothesis {field} is invalid",
            )
        _require(
            record["verdict"] in {"WIN", "LOSE", "INCONCLUSIVE"},
            "hypothesis final verdict drift",
        )
        _require(
            record["recommended_variation"] in {"control", "brand_contrast"},
            "hypothesis final recommendation drift",
        )
        _require(
            record["automatic_mutation_allowed"] is False,
            "hypothesis final record opened mutation",
        )
        aggregate = _validate_aggregate_decision(record["aggregate_evidence"])
        _require(
            hashlib.sha256(_decision_json_bytes(aggregate)).hexdigest()
            == record["decision_sha256"],
            "hypothesis aggregate evidence hash drift",
        )
        _require(
            record["recorded_at_utc"] == aggregate["evaluated_at_utc"],
            "hypothesis record time differs from evaluation",
        )
        _require(
            record["verdict"] == aggregate["verdict"],
            "hypothesis verdict differs from aggregate evidence",
        )
        _require(
            record["recommended_variation"] == aggregate["recommended_variation"],
            "hypothesis recommendation differs from aggregate evidence",
        )

    _reject_identity_data(root)

    if final_manifest is None:
        return
    manifest_status = final_manifest.get("status")
    manifest_final = final_manifest.get("final_look") or {}
    if manifest_status == "final_snapshot_recorded_manual_action_pending":
        _require(
            experiment["status"] == RECORDED,
            "recorded final snapshot lacks registry decision",
        )
        record = experiment["final_decision"]
        for registry_field, manifest_field in (
            ("workflow_run_id", "successful_run_id"),
            ("main_commit", "main_commit"),
            ("snapshot_sha256", "snapshot_sha256"),
            ("decision_sha256", "decision_sha256"),
            ("provenance_sha256", "provenance_sha256"),
            ("verdict", "verdict"),
            ("recommended_variation", "recommended_variation"),
            ("assignment_started_at_utc", "assignment_started_at_utc"),
            ("assignment_ended_at_utc", "assignment_ended_at_utc"),
        ):
            _require(
                record[registry_field] == manifest_final.get(manifest_field),
                f"registry/final snapshot binding drift: {registry_field}",
            )
        _require(
            manifest_final.get("hypothesis_registry_sha256")
            == hashlib.sha256(pretty_json_bytes(root)).hexdigest(),
            "final snapshot hypothesis registry hash drift",
        )
    else:
        _require(
            experiment["status"] == PENDING,
            "registry decision recorded before final snapshot",
        )
        _require(
            manifest_final.get("hypothesis_registry_sha256") is None,
            "pending final snapshot has registry hash",
        )


def record_final_decision(
    registry: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    decision: Mapping[str, Any],
    *,
    snapshot_sha256: str,
    decision_sha256: str,
    provenance_sha256: str,
    workflow_run_id: str,
    main_commit: str,
) -> dict[str, Any]:
    validate_registry(registry)
    experiment = registry["experiments"][0]
    _require(
        experiment["status"] == PENDING, "hypothesis final decision is already recorded"
    )
    _require(decision.get("final_decision") is True, "hypothesis decision is not final")
    _require(
        decision.get("automatic_mutation_allowed") is False,
        "hypothesis decision opened mutation",
    )
    _require(
        hashlib.sha256(_decision_json_bytes(decision)).hexdigest() == decision_sha256,
        "hypothesis decision SHA-256 mismatch",
    )
    _require(
        SHA256_RE.fullmatch(snapshot_sha256) is not None,
        "hypothesis snapshot SHA-256 is invalid",
    )
    _require(
        SHA256_RE.fullmatch(provenance_sha256) is not None,
        "hypothesis provenance SHA-256 is invalid",
    )
    _require(
        RUN_ID_RE.fullmatch(workflow_run_id) is not None,
        "hypothesis workflow run ID is invalid",
    )
    _require(
        COMMIT_RE.fullmatch(main_commit) is not None,
        "hypothesis main commit is invalid",
    )
    updated = copy.deepcopy(dict(registry))
    updated_experiment = updated["experiments"][0]
    updated_experiment["status"] = RECORDED
    updated_experiment["final_decision"] = {
        "recorded_at_utc": decision["evaluated_at_utc"],
        "workflow_run_id": workflow_run_id,
        "main_commit": main_commit,
        "assignment_started_at_utc": snapshot.get("assignment_started_at_utc"),
        "assignment_ended_at_utc": snapshot.get("assignment_ended_at_utc"),
        "snapshot_sha256": snapshot_sha256,
        "decision_sha256": decision_sha256,
        "provenance_sha256": provenance_sha256,
        "verdict": decision["verdict"],
        "recommended_variation": decision["recommended_variation"],
        "automatic_mutation_allowed": False,
        "aggregate_evidence": copy.deepcopy(dict(decision)),
    }
    validate_registry(updated)
    return updated


def _load_canonical(path: pathlib.Path) -> Mapping[str, Any]:
    raw = path.read_bytes()
    value = json.loads(raw.decode("utf-8"))
    _require(isinstance(value, Mapping), "hypothesis registry must contain an object")
    _require(
        raw == pretty_json_bytes(value),
        "hypothesis registry is not canonical pretty JSON",
    )
    return value


def main(argv: Sequence[str] | None = None) -> int:
    path = pathlib.Path(argv[0]) if argv else DEFAULT_REGISTRY_PATH
    try:
        validate_registry(_load_canonical(path))
    except (
        OSError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        HypothesisRegistryError,
    ) as exc:
        print(f"VEVO_GROWTHBOOK_HYPOTHESIS_REGISTRY_INVALID:{exc}", file=sys.stderr)
        return 2
    print(
        "VEVO_GROWTHBOOK_HYPOTHESIS_REGISTRY_VALID:pii=false:automatic-mutation=false"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
