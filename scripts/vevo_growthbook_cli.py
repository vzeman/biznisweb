#!/usr/bin/env python3
"""Fail-closed GrowthBook CLI control plane for the VEVO Production A/A gate.

This wrapper intentionally exposes only read-only preflight and mutation-plan
commands. It cannot start or publish anything. The eventual mutation command
must be added separately after a live authenticated preflight is reviewed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import re
import subprocess
import sys
from collections.abc import Mapping, Sequence
from typing import Any


ROOT = pathlib.Path(__file__).resolve().parents[1]
ACTIVATION_PATH = ROOT / "projects" / "vevo" / "growthbook_production_aa_activation.json"
CLI_ROOT = ROOT / "tools" / "growthbook-cli"
CLI_ENTRYPOINT = CLI_ROOT / "node_modules" / "growthbook" / "bin" / "growthbook.js"
CLI_PACKAGE_LOCK = CLI_ROOT / "package-lock.json"

CLI_VERSION = "2.2.0"
CLI_PACKAGE_INTEGRITY = (
    "sha512-mgABUq0Qejj7ZA0i6G3YmL7LhCxDQQhdL5KI8elGs1iqeoPoGiLQPR/"
    "dYVTtz4LH+nhpcmYpssUWjfhNHGcYEA=="
)
SERVER_URL = "https://api.growthbook.io/api"
FEATURE_KEY = "vevo-sk-aa-assignment"
EXPERIMENT_ID = "exp_19g6mmt5wugpk"
TRACKING_KEY = "vevo-sk-aa-001"
DATA_SOURCE_ID = "ds_19g6mmt5stlp6"
FEATURE_LIVE_REVISION = 2
FEATURE_DRAFT_REVISION = 3
EXPECTED_VARIATIONS = ["control", "variant"]
EXPECTED_WEIGHTS = [0.5, 0.5]

START_ENDPOINT = f"POST {SERVER_URL}/v1/experiments/{EXPERIMENT_ID}/start"
PUBLISH_ENDPOINT = (
    f"POST {SERVER_URL}/v2/features/{FEATURE_KEY}/revisions/"
    f"{FEATURE_DRAFT_REVISION}/publish"
)
PUBLISH_COMMENT = "VEVO Production A/A activation from reviewed schema-9 gate"

_SECRET_PATTERN = re.compile(
    r"(?i)\b(?:secret|pat|token|key)_[A-Za-z0-9._~-]{8,}\b"
)


class GateError(RuntimeError):
    """Raised when a local or remote safety precondition is not exact."""


def _canonical_bytes(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise GateError(message)


def _read_json(path: pathlib.Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise GateError(f"cannot read valid JSON from {path}: {exc}") from exc
    _require(isinstance(value, dict), f"{path} must contain a JSON object")
    return value


def _validate_cli_lock() -> str:
    lock = _read_json(CLI_PACKAGE_LOCK)
    packages = lock.get("packages")
    _require(isinstance(packages, dict), "GrowthBook package-lock is missing packages")
    package = packages.get("node_modules/growthbook")
    _require(isinstance(package, dict), "GrowthBook package is absent from package-lock")
    _require(package.get("version") == CLI_VERSION, "GrowthBook CLI version drifted")
    _require(
        package.get("integrity") == CLI_PACKAGE_INTEGRITY,
        "GrowthBook CLI package integrity drifted",
    )
    return _sha256_bytes(CLI_PACKAGE_LOCK.read_bytes())


def _validate_local_gate() -> tuple[dict[str, Any], str]:
    activation = _read_json(ACTIVATION_PATH)
    manifest_sha256 = _sha256_bytes(_canonical_bytes(activation))

    _require(activation.get("schema_version") == 9, "schema-9 activation gate required")
    _require(activation.get("feature_key") == FEATURE_KEY, "feature key drifted")
    _require(activation.get("tracking_key") == TRACKING_KEY, "tracking key drifted")
    _require(
        activation.get("variations") == EXPECTED_VARIATIONS,
        "variation names drifted",
    )
    _require(
        activation.get("variation_weights") == EXPECTED_WEIGHTS,
        "variation weights drifted",
    )

    growthbook = activation.get("growthbook")
    _require(isinstance(growthbook, dict), "growthbook gate is missing")
    _require(growthbook.get("environment") == "production", "environment is not production")
    _require(growthbook.get("experiment_id") == EXPERIMENT_ID, "experiment id drifted")
    _require(
        growthbook.get("data_source_id") == DATA_SOURCE_ID,
        "Production data source id drifted",
    )
    _require(
        growthbook.get("feature_rule_revision") == FEATURE_DRAFT_REVISION,
        "draft feature revision drifted",
    )
    _require(growthbook.get("status") == "draft_not_started", "experiment is not draft")
    _require(
        growthbook.get("production_rule_publish_status") == "draft_not_published",
        "feature revision is no longer an unpublished draft",
    )
    _require(growthbook.get("allocation_percent") == 0, "allocation is not zero")

    preflight = activation.get("activation_preflight")
    _require(isinstance(preflight, dict), "activation preflight is missing")
    post_publish = preflight.get("post_publish_readback")
    _require(isinstance(post_publish, dict), "post-publish readback is missing")
    _require(
        post_publish.get("growthbook_start_allowed") is True,
        "reviewed GrowthBook start gate is closed",
    )

    scope = preflight.get("mutation_scope")
    _require(isinstance(scope, dict), "mutation scope is missing")
    _require(
        scope.get(f"start_growthbook_experiment_{EXPERIMENT_ID}") is True,
        "exact experiment start is not allowlisted",
    )
    _require(
        scope.get("publish_growthbook_feature_revision_3") is True,
        "exact feature revision publish is not allowlisted",
    )
    for forbidden in (
        "meta_ads",
        "biznisweb",
        "prices_or_product_content",
        "cart_checkout_or_orders",
        "cta_experiment",
        "collector_infrastructure",
    ):
        _require(scope.get(forbidden) is False, f"forbidden scope opened: {forbidden}")

    traffic = activation.get("traffic")
    _require(isinstance(traffic, dict), "traffic gate is missing")
    _require(traffic.get("production_allocation_percent") == 0, "traffic is not zero")
    _require(traffic.get("active_production_experiments") == [], "an experiment is active")
    _require(traffic.get("cta_experiment_started") is False, "CTA experiment is active")
    _require(traffic.get("activation_allowed") is False, "manifest already claims activation")

    safety = activation.get("safety")
    _require(isinstance(safety, dict), "safety gate is missing")
    _require(safety.get("price_tests_allowed") is False, "price tests unexpectedly allowed")
    for key in ("meta_ads_mutated", "biznisweb_mutated", "cart_checkout_mutated"):
        _require(safety.get(key) is False, f"external mutation already recorded: {key}")
    _require(safety.get("contains_credentials") is False, "manifest contains credentials")

    return activation, manifest_sha256


def _redact(text: str) -> str:
    redacted = text
    for variable in ("GBCLI_BEARER_AUTH", "GROWTHBOOK_API_KEY"):
        secret = os.environ.get(variable, "")
        if secret:
            redacted = redacted.replace(secret, "<redacted>")
    return _SECRET_PATTERN.sub("<redacted>", redacted)


def _cli_command(arguments: Sequence[str]) -> list[str]:
    _require(CLI_ENTRYPOINT.is_file(), "GrowthBook CLI is not installed; run the documented npm ci command")
    return [
        "node",
        str(CLI_ENTRYPOINT),
        *arguments,
        "--server-url",
        SERVER_URL,
        "--no-interactive",
        "--no-update-check",
        "--color",
        "never",
    ]


def _run_cli(arguments: Sequence[str], *, dry_run: bool = False) -> subprocess.CompletedProcess[str]:
    command = _cli_command(arguments)
    if dry_run:
        command.append("--dry-run")
    environment = os.environ.copy()
    environment["NO_COLOR"] = "1"
    environment["GBCLI_NO_UPDATE_CHECK"] = "1"
    completed = subprocess.run(
        command,
        cwd=ROOT,
        env=environment,
        capture_output=True,
        check=False,
        text=True,
        timeout=60,
    )
    if completed.returncode != 0:
        detail = _redact((completed.stderr or completed.stdout).strip())
        if "Missing Authorization" in detail or "Authorization header" in detail:
            raise GateError(
                "GrowthBook CLI authentication is missing; configure the OS-keychain profile first"
            )
        raise GateError(f"GrowthBook CLI failed ({completed.returncode}): {detail}")
    return completed


def _run_cli_json(arguments: Sequence[str]) -> dict[str, Any]:
    completed = _run_cli([*arguments, "--output-format", "json"])
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise GateError("GrowthBook CLI returned non-JSON output") from exc
    _require(isinstance(value, dict), "GrowthBook CLI response must be a JSON object")
    return value


def _unwrap(payload: Mapping[str, Any], keys: Sequence[str]) -> dict[str, Any]:
    for key in keys:
        nested = payload.get(key)
        if isinstance(nested, dict):
            return nested
    return dict(payload)


def _environment_enabled(feature: Mapping[str, Any], environment: str) -> bool | None:
    environments = feature.get("environments")
    if not isinstance(environments, dict):
        return None
    entry = environments.get(environment)
    if isinstance(entry, bool):
        return entry
    if isinstance(entry, dict) and isinstance(entry.get("enabled"), bool):
        return entry["enabled"]
    return None


def _rule_environments(rule: Mapping[str, Any]) -> list[str]:
    environments = rule.get("environments")
    if isinstance(environments, list) and all(isinstance(item, str) for item in environments):
        return sorted(environments)
    if rule.get("allEnvironments") is True:
        return ["*"]
    return []


def _validate_remote(
    experiment_payload: Mapping[str, Any],
    feature_payload: Mapping[str, Any],
    revision_payload: Mapping[str, Any],
    merge_payload: Mapping[str, Any],
) -> dict[str, Any]:
    experiment = _unwrap(experiment_payload, ("experiment", "data"))
    _require(experiment.get("id") == EXPERIMENT_ID, "remote experiment id drifted")
    _require(experiment.get("trackingKey") == TRACKING_KEY, "remote tracking key drifted")
    _require(experiment.get("status") == "draft", "remote experiment is not draft")
    _require(experiment.get("type") == "standard", "remote experiment is not standard A/B")
    _require(experiment.get("hashAttribute") == "id", "assignment attribute drifted")
    _require(
        experiment.get("disableStickyBucketing") is not True,
        "sticky bucketing is disabled",
    )

    variations = experiment.get("variations")
    _require(isinstance(variations, list) and len(variations) == 2, "expected two variations")
    variation_labels = [
        str(item.get("name") or item.get("key") or "").strip().lower()
        for item in variations
        if isinstance(item, dict)
    ]
    _require(variation_labels == EXPECTED_VARIATIONS, "remote variation labels drifted")

    phases = experiment.get("phases")
    _require(isinstance(phases, list) and len(phases) == 1, "expected one experiment phase")
    phase = phases[0]
    _require(isinstance(phase, dict), "experiment phase is invalid")
    _require(float(phase.get("coverage", -1)) == 1.0, "experiment coverage is not 100%")
    split = phase.get("trafficSplit")
    _require(isinstance(split, list) and len(split) == 2, "traffic split is invalid")
    weights = [float(item.get("weight", -1)) for item in split if isinstance(item, dict)]
    _require(weights == EXPECTED_WEIGHTS, "remote variation weights drifted")

    feature = _unwrap(feature_payload, ("feature", "data"))
    _require(feature.get("id") == FEATURE_KEY, "remote feature key drifted")
    revision_summary = feature.get("revision")
    _require(isinstance(revision_summary, dict), "live feature revision is missing")
    _require(
        revision_summary.get("version") == FEATURE_LIVE_REVISION,
        "live feature revision is no longer 2",
    )
    _require(
        _environment_enabled(feature, "production") is False,
        "live Production feature is no longer disabled",
    )
    live_rules = feature.get("rules")
    _require(isinstance(live_rules, list), "live feature rules are missing")
    production_live_rules = [
        rule
        for rule in live_rules
        if isinstance(rule, dict) and "production" in _rule_environments(rule)
    ]
    _require(production_live_rules == [], "live feature already has a Production rule")

    revision = _unwrap(revision_payload, ("revision", "data"))
    _require(revision.get("featureId") == FEATURE_KEY, "draft belongs to another feature")
    _require(revision.get("version") == FEATURE_DRAFT_REVISION, "draft revision drifted")
    _require(revision.get("baseVersion") == FEATURE_LIVE_REVISION, "draft base revision drifted")
    _require(revision.get("status") == "draft", "feature revision is not draft")
    enabled = revision.get("environmentsEnabled")
    _require(isinstance(enabled, dict), "draft environment state is missing")
    _require(enabled.get("production") is True, "draft does not enable Production")

    rules = revision.get("rules")
    _require(isinstance(rules, list), "draft feature rules are missing")
    production_experiment_rules = [
        rule
        for rule in rules
        if isinstance(rule, dict)
        and rule.get("type") == "experiment-ref"
        and rule.get("experimentId") == EXPERIMENT_ID
        and "production" in _rule_environments(rule)
    ]
    _require(
        len(production_experiment_rules) == 1,
        "draft must contain exactly one Production rule for the allowlisted experiment",
    )
    _require(production_experiment_rules[0].get("enabled") is True, "Production rule is disabled")

    merge = _unwrap(merge_payload, ("mergeStatus", "data"))
    _require(merge.get("rebaseRequired") is False, "draft requires rebase")
    conflicts = merge.get("conflicts")
    _require(conflicts in (None, [], {}), "draft has merge conflicts")

    return {
        "experiment_id": EXPERIMENT_ID,
        "experiment_status": "draft",
        "tracking_key": TRACKING_KEY,
        "coverage_percent": 100,
        "variation_weights": EXPECTED_WEIGHTS,
        "feature_key": FEATURE_KEY,
        "feature_live_revision": FEATURE_LIVE_REVISION,
        "feature_live_production_enabled": False,
        "feature_draft_revision": FEATURE_DRAFT_REVISION,
        "feature_draft_status": "draft",
        "feature_draft_base_revision": FEATURE_LIVE_REVISION,
        "feature_draft_production_rule_count": 1,
        "rebase_required": False,
        "merge_conflict_count": 0,
    }


def preflight() -> dict[str, Any]:
    _, manifest_sha256 = _validate_local_gate()
    lock_sha256 = _validate_cli_lock()
    experiment = _run_cli_json(["experiments", "get", "--id", EXPERIMENT_ID])
    feature = _run_cli_json(
        ["features", "get", "--id", FEATURE_KEY, "--with-revisions", "all"]
    )
    revision = _run_cli_json(
        [
            "feature-revisions",
            "get",
            "--id",
            FEATURE_KEY,
            "--version-param",
            str(FEATURE_DRAFT_REVISION),
        ]
    )
    merge = _run_cli_json(
        [
            "feature-revisions",
            "merge-status",
            "--id",
            FEATURE_KEY,
            "--version-param",
            str(FEATURE_DRAFT_REVISION),
        ]
    )
    diff = _run_cli_json(
        [
            "feature-revisions",
            "diff",
            "--id",
            FEATURE_KEY,
            "--version-param",
            str(FEATURE_DRAFT_REVISION),
            "--base",
            "live",
            "--format-param",
            "minimal",
        ]
    )
    remote = _validate_remote(experiment, feature, revision, merge)
    return {
        "schema_version": 1,
        "status": "authenticated_preflight_passed",
        "mutation_executed": False,
        "server_url": SERVER_URL,
        "cli": {
            "version": CLI_VERSION,
            "package_integrity": CLI_PACKAGE_INTEGRITY,
            "package_lock_sha256": lock_sha256,
        },
        "local_gate": {
            "activation_schema_version": 9,
            "manifest_sha256": manifest_sha256,
        },
        "remote": remote,
        "remote_payload_sha256": {
            "experiment": _sha256_bytes(_canonical_bytes(experiment)),
            "feature": _sha256_bytes(_canonical_bytes(feature)),
            "feature_revision": _sha256_bytes(_canonical_bytes(revision)),
            "merge_status": _sha256_bytes(_canonical_bytes(merge)),
            "minimal_diff": _sha256_bytes(_canonical_bytes(diff)),
        },
        "contains_credentials": False,
    }


def plan() -> dict[str, Any]:
    _, manifest_sha256 = _validate_local_gate()
    lock_sha256 = _validate_cli_lock()
    start = _run_cli(
        ["experiments", "start", "--id", EXPERIMENT_ID],
        dry_run=True,
    )
    publish = _run_cli(
        [
            "feature-revisions",
            "publish",
            "--id",
            FEATURE_KEY,
            "--version-param",
            str(FEATURE_DRAFT_REVISION),
            "--comment",
            PUBLISH_COMMENT,
        ],
        dry_run=True,
    )
    start_text = _redact(start.stdout + start.stderr)
    publish_text = _redact(publish.stdout + publish.stderr)
    _require(START_ENDPOINT in start_text, "start dry-run endpoint drifted")
    _require(PUBLISH_ENDPOINT in publish_text, "publish dry-run endpoint drifted")
    for forbidden in ("ignore-warnings", "skip-checklist", "skip-hooks", "skip-schema"):
        _require(forbidden not in start_text.lower(), f"unsafe start option present: {forbidden}")
        _require(forbidden not in publish_text.lower(), f"unsafe publish option present: {forbidden}")
    return {
        "schema_version": 1,
        "status": "mutation_dry_run_passed",
        "mutation_executed": False,
        "ordered_operations": [
            {"operation": "start_experiment", "endpoint": START_ENDPOINT},
            {"operation": "publish_feature_revision", "endpoint": PUBLISH_ENDPOINT},
        ],
        "cli": {
            "version": CLI_VERSION,
            "package_integrity": CLI_PACKAGE_INTEGRITY,
            "package_lock_sha256": lock_sha256,
        },
        "local_gate": {
            "activation_schema_version": 9,
            "manifest_sha256": manifest_sha256,
        },
        "dry_run_sha256": {
            "start_experiment": _sha256_bytes(start_text.encode()),
            "publish_feature_revision": _sha256_bytes(publish_text.encode()),
        },
        "contains_credentials": False,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("preflight", "plan"))
    parser.add_argument("--pretty", action="store_true", help="indent JSON output")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        result = preflight() if args.command == "preflight" else plan()
    except (GateError, subprocess.TimeoutExpired) as exc:
        print(f"VEVO_GROWTHBOOK_CLI_GATE_FAILED: {_redact(str(exc))}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2 if args.pretty else None, sort_keys=True))
    marker = "PREFLIGHT_OK" if args.command == "preflight" else "PLAN_OK"
    print(f"VEVO_GROWTHBOOK_CLI_{marker}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
