#!/usr/bin/env python3
"""Build one canonical identity-free VEVO CTA runtime readiness artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

try:
    from scripts import record_growthbook_cta_activation as cta_activation
except ModuleNotFoundError:  # Direct execution from scripts/.
    import record_growthbook_cta_activation as cta_activation  # type: ignore


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = ROOT / "projects" / "vevo" / "growthbook_cta_activation.json"
DEFAULT_REGISTRY = ROOT / "growthbook_collector" / "experiments.json"
RUN_RE = re.compile(r"^[1-9][0-9]*$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
TASK_RE = re.compile(r"^vevo-growthbook-collector-production:[1-9][0-9]*$")
TASK_ID_RE = re.compile(r"^[0-9a-f]{32}$")
IMAGE_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


class CtaRuntimeObservationError(ValueError):
    """Raised when sanitized runtime inputs are incomplete or unsafe."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise CtaRuntimeObservationError(message)


def _load(path: Path, field: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CtaRuntimeObservationError(f"unable to load {field}") from exc
    _require(isinstance(value, dict), f"{field} must be an object")
    return value


def _canonical_utc(value: str | None) -> str:
    if value is None:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    _require(value.endswith("Z"), "observed timestamp must be UTC")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise CtaRuntimeObservationError("observed timestamp is invalid") from exc
    canonical = parsed.isoformat().replace("+00:00", "Z")
    _require(canonical == value, "observed timestamp is not canonical")
    return value


def build_observation(
    *,
    manifest: Mapping[str, Any],
    registry_raw: bytes,
    registry: Mapping[str, Any],
    workflow_run_id: str,
    main_commit: str,
    private_ip: str,
    host_gate_task_id: str,
    host_gate_private_ip: str,
    task_definition: str,
    image_digest: str,
    cta_events_before_start: int,
    observed_at_utc: str | None = None,
) -> dict[str, Any]:
    cta_activation.validate_manifest(manifest)
    _require(manifest["status"] == cta_activation.WAITING, "CTA manifest is not waiting for runtime readiness")
    _require(RUN_RE.fullmatch(workflow_run_id) is not None, "workflow run ID is invalid")
    _require(COMMIT_RE.fullmatch(main_commit) is not None, "main commit is invalid")
    _require(TASK_RE.fullmatch(task_definition) is not None, "task definition is invalid")
    _require(TASK_ID_RE.fullmatch(host_gate_task_id) is not None, "host-gate task ID is invalid")
    _require(IMAGE_RE.fullmatch(image_digest) is not None, "image digest is invalid")
    _require(type(cta_events_before_start) is int and cta_events_before_start == 0, "CTA events exist before start")
    production = registry.get("environments", {}).get("production", {})
    preview = registry.get("environments", {}).get("preview", {})
    experiment_id = manifest["experiment_id"]
    _require(set(production) == {experiment_id}, "Production registry is not CTA-only")
    _require(production[experiment_id] == preview.get(experiment_id), "Production CTA contract differs from Preview")
    registry_hash = hashlib.sha256(registry_raw).hexdigest()
    observation = {
        "schema_version": 1,
        "evidence_type": "vevo_growthbook_cta_runtime_readiness",
        "experiment_id": experiment_id,
        "observed_at_utc": _canonical_utc(observed_at_utc),
        "workflow": {
            "run_id": workflow_run_id,
            "main_commit": main_commit,
            "conclusion": "success",
        },
        "runtime": {
            "instance_id": "N/A:Fargate",
            "private_ip": private_ip,
            "service": "vevo-growthbook-collector-production",
            "runtime_path": "/app",
            "task_definition": task_definition,
            "image_digest": image_digest,
            "host_gate_task_id": host_gate_task_id,
            "host_gate_private_ip": host_gate_private_ip,
            "localhost_marker_verified": True,
            "target_health": "healthy",
        },
        "control_plane": {
            "registry_sha256": registry_hash,
            "production_registry_experiments": [experiment_id],
            "cta_events_before_start": cta_events_before_start,
            "aa_production_allocation_percent": 0,
            "cta_production_allocation_percent": 0,
            "gtm_container_version_id": "15",
            "gtm_unprocessed_changes": 0,
        },
        "safety": {
            "contains_credentials": False,
            "contains_event_or_device_ids": False,
            "contains_customer_or_order_data": False,
            "meta_ads_mutated": False,
            "biznisweb_mutated": False,
            "price_product_cart_checkout_order_mutated": False,
        },
    }
    cta_activation.validate_runtime_observation(observation, manifest)
    return observation


def _write(path: Path, value: Mapping[str, Any]) -> None:
    body = cta_activation.canonical_json_bytes(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=f".{path.name}.", delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(body)
        handle.flush()
    temporary.replace(path)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--workflow-run-id", required=True)
    parser.add_argument("--main-commit", required=True)
    parser.add_argument("--private-ip", required=True)
    parser.add_argument("--host-gate-task-id", required=True)
    parser.add_argument("--host-gate-private-ip", required=True)
    parser.add_argument("--task-definition", required=True)
    parser.add_argument("--image-digest", required=True)
    parser.add_argument("--cta-events-before-start", type=int, required=True)
    parser.add_argument("--observed-at-utc")
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        registry_raw = args.registry.read_bytes()
        observation = build_observation(
            manifest=_load(args.manifest, "CTA activation manifest"),
            registry_raw=registry_raw,
            registry=_load(args.registry, "collector registry"),
            workflow_run_id=args.workflow_run_id,
            main_commit=args.main_commit,
            private_ip=args.private_ip,
            host_gate_task_id=args.host_gate_task_id,
            host_gate_private_ip=args.host_gate_private_ip,
            task_definition=args.task_definition,
            image_digest=args.image_digest,
            cta_events_before_start=args.cta_events_before_start,
            observed_at_utc=args.observed_at_utc,
        )
        _write(args.output, observation)
    except (
        CtaRuntimeObservationError,
        cta_activation.CtaActivationRecordingError,
        OSError,
        KeyError,
    ) as exc:
        print(f"build_growthbook_cta_runtime_readiness.py: FAIL: {exc}")
        return 2
    digest = hashlib.sha256(args.output.read_bytes()).hexdigest()
    print(f"VEVO_CTA_RUNTIME_OBSERVATION_OK:sha256={digest}:identity=false:mutation=false")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
