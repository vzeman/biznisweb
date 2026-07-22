#!/usr/bin/env python3
"""Lease-backed maintenance state for the live ROY dashboard."""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


MAINTENANCE_MARKER = "dashboard-maintenance-v1"
MAINTENANCE_VERSION = 1
DEFAULT_MAINTENANCE_MESSAGE = (
    "Na dashboarde prebiehajú úpravy. Práve nasadzujeme alebo aktualizujeme dashboard. "
    "Prosíme o strpenie a dočasne ho nepoužívajte. Stránka sa odblokuje automaticky po dokončení."
)
MAINTENANCE_STATUS_ERROR_MESSAGE = (
    "Stav dashboardu sa momentálne nedá bezpečne overiť. Prosíme, dashboard zatiaľ nepoužívajte; "
    "kontrolu automaticky zopakujeme."
)
MAINTENANCE_REASON_CODES = {
    "deployment",
    "data_update",
    "manual_maintenance",
    "incident",
}
MAINTENANCE_PHASES = {
    "starting",
    "refreshing",
    "deploying",
    "verifying",
    "failed",
    "complete",
    "idle",
}
MIN_LEASE_SECONDS = 60
MAX_LEASE_SECONDS = 1_800
DEFAULT_LEASE_SECONDS = 900
DEFAULT_MAX_LIFETIME_SECONDS = 9_000
MAX_MESSAGE_LENGTH = 500
MAX_EVENT_ROWS = 50
MAX_CLOCK_SKEW_SECONDS = 300
_OPERATION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,159}$")


class MaintenanceConflict(RuntimeError):
    """Raised when a controller attempts to change another active lease."""


class MaintenanceStorageError(RuntimeError):
    """Raised when the configured maintenance source cannot be read safely."""


def _utc_now(now: Optional[datetime] = None) -> datetime:
    value = now or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).replace(microsecond=0)


def _iso_utc(value: datetime) -> str:
    return _utc_now(value).isoformat().replace("+00:00", "Z")


def _parse_utc(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc).replace(microsecond=0)


def _bounded_text(value: Any, limit: int) -> str:
    raw_text = str(value or "")
    if any(ord(character) < 32 or ord(character) == 127 for character in raw_text):
        raise ValueError("Maintenance text contains control characters.")
    text = " ".join(raw_text.split())
    return text[:limit]


def _validated_operation_id(value: Any) -> str:
    operation_id = str(value or "").strip()
    if not _OPERATION_ID_RE.fullmatch(operation_id):
        raise ValueError("Invalid maintenance operation id.")
    return operation_id


def _validated_reason_code(value: Any) -> str:
    reason_code = str(value or "").strip().lower()
    if reason_code not in MAINTENANCE_REASON_CODES:
        raise ValueError(f"Unsupported maintenance reason code: {reason_code!r}")
    return reason_code


def _validated_phase(value: Any, *, default: str) -> str:
    phase = str(value or default).strip().lower()
    if phase not in MAINTENANCE_PHASES:
        raise ValueError(f"Unsupported maintenance phase: {phase!r}")
    return phase


def _empty_maintenance_state(project: str) -> Dict[str, Any]:
    return {
        "marker": MAINTENANCE_MARKER,
        "version": MAINTENANCE_VERSION,
        "project": str(project or "roy").strip() or "roy",
        "active": False,
        "stored_active": False,
        "expired": False,
        "schema_valid": False,
        "status_error": False,
        "operation_id": "",
        "reason_code": "",
        "phase": "idle",
        "message": "",
        "actor": "",
        "started_at": "",
        "updated_at": "",
        "expires_at": "",
        "hard_expires_at": "",
        "cleared_at": "",
        "outcome": "",
        "remaining_seconds": 0,
        "source_sha": "",
        "image_digest": "",
        "events": [],
    }


def normalize_maintenance_state(
    raw: Any,
    *,
    project: str = "roy",
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Normalize persisted state and derive whether its finite lease is active."""

    state = _empty_maintenance_state(project)
    if not isinstance(raw, dict):
        return state

    current = _utc_now(now)
    stored_project = str(raw.get("project") or project).strip() or project
    state["project"] = stored_project
    state["stored_active"] = raw.get("active") is True
    state["operation_id"] = str(raw.get("operation_id") or "").strip()[:160]
    state["reason_code"] = str(raw.get("reason_code") or "").strip().lower()
    state["phase"] = str(raw.get("phase") or ("starting" if state["stored_active"] else "idle")).strip().lower()
    state["message"] = str(raw.get("message") or "").strip()[:MAX_MESSAGE_LENGTH]
    state["actor"] = str(raw.get("actor") or "").strip()[:160]
    for field in (
        "started_at",
        "updated_at",
        "expires_at",
        "hard_expires_at",
        "cleared_at",
        "outcome",
        "source_sha",
        "image_digest",
    ):
        state[field] = str(raw.get(field) or "").strip()[:500]
    rows = raw.get("events")
    if isinstance(rows, list):
        state["events"] = [row for row in rows[-MAX_EVENT_ROWS:] if isinstance(row, dict)]

    operation_valid = bool(_OPERATION_ID_RE.fullmatch(state["operation_id"]))
    reason_valid = state["reason_code"] in MAINTENANCE_REASON_CODES
    phase_valid = state["phase"] in MAINTENANCE_PHASES
    marker_valid = raw.get("marker") == MAINTENANCE_MARKER
    version_valid = type(raw.get("version")) is int and raw.get("version") == MAINTENANCE_VERSION
    started_at = _parse_utc(state["started_at"])
    updated_at = _parse_utc(state["updated_at"])
    expires_at = _parse_utc(state["expires_at"])
    hard_expires_at = _parse_utc(state["hard_expires_at"])
    deadline_candidates = [value for value in (expires_at, hard_expires_at) if value is not None]
    deadline = min(deadline_candidates) if len(deadline_candidates) == 2 else None
    timeline_valid = bool(
        started_at is not None
        and updated_at is not None
        and expires_at is not None
        and hard_expires_at is not None
        and started_at <= updated_at <= expires_at <= hard_expires_at
        and updated_at <= current + timedelta(seconds=MAX_CLOCK_SKEW_SECONDS)
        and (expires_at - updated_at).total_seconds() <= MAX_LEASE_SECONDS
        and (hard_expires_at - started_at).total_seconds() <= DEFAULT_MAX_LIFETIME_SECONDS
    )
    base_schema_valid = bool(
        marker_valid
        and version_valid
        and not {
            "stored_active",
            "schema_valid",
            "status_error",
            "expired",
            "remaining_seconds",
        }.intersection(raw)
        and stored_project == project
        and operation_valid
        and reason_valid
        and phase_valid
    )
    cleared_at = _parse_utc(state["cleared_at"])
    inactive_timeline_valid = bool(
        started_at is not None
        and updated_at is not None
        and expires_at is not None
        and hard_expires_at is not None
        and cleared_at is not None
        and started_at <= expires_at <= hard_expires_at
        and started_at <= updated_at
        and abs((cleared_at - updated_at).total_seconds()) <= MAX_CLOCK_SKEW_SECONDS
        and (hard_expires_at - started_at).total_seconds() <= DEFAULT_MAX_LIFETIME_SECONDS
    )
    state["schema_valid"] = bool(
        base_schema_valid
        and (
            (raw.get("active") is True and timeline_valid)
            or (
                raw.get("active") is False
                and state["phase"] in {"complete", "failed"}
                and bool(state["outcome"])
                and inactive_timeline_valid
            )
        )
    )
    lease_valid = bool(
        state["stored_active"]
        and base_schema_valid
        and deadline is not None
        and timeline_valid
    )
    state["active"] = bool(lease_valid and current < deadline)
    state["expired"] = bool(state["stored_active"] and not state["active"])
    state["remaining_seconds"] = max(0, int((deadline - current).total_seconds())) if state["active"] else 0
    if state["active"] and not state["message"]:
        state["message"] = DEFAULT_MAINTENANCE_MESSAGE
    return state


def _event_rows(existing: Dict[str, Any], event: Dict[str, Any]) -> list[Dict[str, Any]]:
    rows = existing.get("events") if isinstance(existing.get("events"), list) else []
    return [row for row in rows[-(MAX_EVENT_ROWS - 1):] if isinstance(row, dict)] + [event]


def build_active_maintenance_state(
    existing: Any,
    *,
    project: str,
    operation_id: str,
    reason_code: str,
    message: str = DEFAULT_MAINTENANCE_MESSAGE,
    phase: str = "starting",
    actor: str = "deployment-controller",
    ttl_seconds: int = DEFAULT_LEASE_SECONDS,
    max_lifetime_seconds: int = DEFAULT_MAX_LIFETIME_SECONDS,
    source_sha: str = "",
    image_digest: str = "",
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    current = _utc_now(now)
    operation_id = _validated_operation_id(operation_id)
    reason_code = _validated_reason_code(reason_code)
    phase = _validated_phase(phase, default="starting")
    message = _bounded_text(message or DEFAULT_MAINTENANCE_MESSAGE, MAX_MESSAGE_LENGTH)
    actor = _bounded_text(actor, 160) or "deployment-controller"
    ttl_seconds = int(ttl_seconds)
    max_lifetime_seconds = int(max_lifetime_seconds)
    if not MIN_LEASE_SECONDS <= ttl_seconds <= MAX_LEASE_SECONDS:
        raise ValueError(f"Maintenance TTL must be {MIN_LEASE_SECONDS}..{MAX_LEASE_SECONDS} seconds.")
    if not ttl_seconds <= max_lifetime_seconds <= DEFAULT_MAX_LIFETIME_SECONDS:
        raise ValueError("Maintenance maximum lifetime is outside the safe range.")

    normalized = normalize_maintenance_state(existing, project=project, now=current)
    if normalized["active"] and normalized["operation_id"] != operation_id:
        raise MaintenanceConflict(
            f"Dashboard maintenance is owned by {normalized['operation_id']!r}."
        )
    same_operation = normalized["operation_id"] == operation_id
    started_at = _parse_utc(normalized.get("started_at")) if same_operation else None
    hard_expires_at = _parse_utc(normalized.get("hard_expires_at")) if same_operation else None
    started_at = started_at or current
    hard_expires_at = hard_expires_at or (started_at + timedelta(seconds=max_lifetime_seconds))
    if current >= hard_expires_at:
        raise MaintenanceConflict("Dashboard maintenance reached its maximum lifetime.")
    expires_at = min(current + timedelta(seconds=ttl_seconds), hard_expires_at)
    action = "renew" if same_operation and normalized["stored_active"] else "start"
    timestamp = _iso_utc(current)
    return {
        "marker": MAINTENANCE_MARKER,
        "version": MAINTENANCE_VERSION,
        "project": project,
        "active": True,
        "operation_id": operation_id,
        "reason_code": reason_code,
        "phase": phase,
        "message": message,
        "actor": actor,
        "started_at": _iso_utc(started_at),
        "updated_at": timestamp,
        "expires_at": _iso_utc(expires_at),
        "hard_expires_at": _iso_utc(hard_expires_at),
        "cleared_at": "",
        "outcome": "",
        "source_sha": _bounded_text(source_sha, 160),
        "image_digest": _bounded_text(image_digest, 200),
        "events": _event_rows(
            normalized,
            {
                "at": timestamp,
                "action": action,
                "operation_id": operation_id,
                "phase": phase,
            },
        ),
    }


def build_inactive_maintenance_state(
    existing: Any,
    *,
    project: str,
    operation_id: str,
    outcome: str = "complete",
    actor: str = "deployment-controller",
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    current = _utc_now(now)
    operation_id = _validated_operation_id(operation_id)
    normalized = normalize_maintenance_state(existing, project=project, now=current)
    existing_operation_id = str(normalized.get("operation_id") or "")
    if existing_operation_id and existing_operation_id != operation_id:
        raise MaintenanceConflict(
            f"Refusing to clear maintenance owned by {existing_operation_id!r}."
        )
    timestamp = _iso_utc(current)
    return {
        "marker": MAINTENANCE_MARKER,
        "version": MAINTENANCE_VERSION,
        "project": project,
        "active": False,
        "operation_id": operation_id,
        "reason_code": normalized.get("reason_code") or "deployment",
        "phase": "complete" if outcome == "complete" else "failed",
        "message": normalized.get("message") or DEFAULT_MAINTENANCE_MESSAGE,
        "actor": _bounded_text(actor, 160) or "deployment-controller",
        "started_at": normalized.get("started_at") or timestamp,
        "updated_at": timestamp,
        "expires_at": normalized.get("expires_at") or timestamp,
        "hard_expires_at": normalized.get("hard_expires_at") or timestamp,
        "cleared_at": timestamp,
        "outcome": _bounded_text(outcome, 80),
        "source_sha": normalized.get("source_sha") or "",
        "image_digest": normalized.get("image_digest") or "",
        "events": _event_rows(
            normalized,
            {
                "at": timestamp,
                "action": "stop",
                "operation_id": operation_id,
                "outcome": outcome,
            },
        ),
    }


def public_maintenance_status(state: Any, *, project: str = "roy", now: Optional[datetime] = None) -> Dict[str, Any]:
    if (
        isinstance(state, dict)
        and state.get("marker") == MAINTENANCE_MARKER
        and isinstance(state.get("schema_valid"), bool)
        and isinstance(state.get("stored_active"), bool)
    ):
        persisted = {
            key: value
            for key, value in state.items()
            if key
            not in {
                "stored_active",
                "schema_valid",
                "status_error",
                "expired",
                "remaining_seconds",
                "_storage_etag",
                "_storage_object_absent",
            }
        }
        persisted["active"] = state["stored_active"]
        normalized = normalize_maintenance_state(persisted, project=project, now=now)
    else:
        normalized = normalize_maintenance_state(state, project=project, now=now)
    return {
        key: normalized.get(key)
        for key in (
            "marker",
            "version",
            "project",
            "active",
            "expired",
            "status_error",
            "operation_id",
            "reason_code",
            "phase",
            "message",
            "started_at",
            "updated_at",
            "expires_at",
            "remaining_seconds",
        )
    }


def maintenance_fail_closed_status(
    project: str,
    error: Any = "",
    *,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    current = _utc_now(now)
    state = public_maintenance_status(_empty_maintenance_state(project), project=project, now=current)
    state.update(
        {
            "active": False,
            "status_error": True,
            "phase": "failed",
            "message": MAINTENANCE_STATUS_ERROR_MESSAGE,
            "error": str(error or "Maintenance status unavailable.")[:240],
            "ui_lock_expires_at": _iso_utc(
                current + timedelta(seconds=DEFAULT_LEASE_SECONDS)
            ),
        }
    )
    return state


def _project_env_name(project: str) -> str:
    return "".join(character if character.isalnum() else "_" for character in project.upper())


def maintenance_s3_location(project: str, project_settings: Dict[str, Any]) -> Optional[Tuple[str, str, str]]:
    s3_settings = project_settings.get("live_dashboard_artifacts") or {}
    env_project = _project_env_name(project)
    bucket = (
        os.getenv(f"LIVE_DASHBOARD_STATE_S3_BUCKET_{env_project}", "").strip()
        or os.getenv("LIVE_DASHBOARD_STATE_S3_BUCKET", "").strip()
        or os.getenv(f"LIVE_DASHBOARD_S3_BUCKET_{env_project}", "").strip()
        or os.getenv("LIVE_DASHBOARD_S3_BUCKET", "").strip()
        or os.getenv(f"REPORT_S3_BUCKET_{env_project}", "").strip()
        or os.getenv("REPORT_S3_BUCKET", "").strip()
        or str(s3_settings.get("s3_bucket") or "").strip()
    )
    prefix = (
        os.getenv(f"LIVE_DASHBOARD_STATE_S3_PREFIX_{env_project}", "").strip()
        or os.getenv("LIVE_DASHBOARD_STATE_S3_PREFIX", "").strip()
        or os.getenv(f"LIVE_DASHBOARD_S3_PREFIX_{env_project}", "").strip()
        or os.getenv("LIVE_DASHBOARD_S3_PREFIX", "").strip()
        or os.getenv(f"REPORT_S3_PREFIX_{env_project}", "").strip()
        or os.getenv("REPORT_S3_PREFIX", "").strip()
        or str(s3_settings.get("s3_prefix") or "").strip()
        or f"daily-reports/{project}"
    ).strip("/")
    region = (
        os.getenv(f"AWS_REGION_{env_project}", "").strip()
        or os.getenv("AWS_REGION", "eu-central-1").strip()
        or "eu-central-1"
    )
    if not bucket:
        return None
    return bucket, f"{prefix}/operations/maintenance.json", region


def _local_maintenance_path(project: str) -> Path:
    configured = os.getenv("LIVE_DASHBOARD_MAINTENANCE_PATH", "").strip()
    if configured:
        return Path(configured)
    return Path(__file__).resolve().parent / "data" / project / "maintenance_state.json"


def _s3_error_code(exc: Exception) -> str:
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        error = response.get("Error")
        if isinstance(error, dict):
            return str(error.get("Code") or "").strip()
    return ""


def load_dashboard_maintenance_state(
    project: str,
    project_settings: Optional[Dict[str, Any]] = None,
    *,
    require_configured_remote: bool = False,
) -> Dict[str, Any]:
    project = str(project or "roy").strip() or "roy"
    project_settings = project_settings or {}
    location = maintenance_s3_location(project, project_settings)
    if location is None and require_configured_remote:
        raise MaintenanceStorageError(
            f"Dashboard maintenance requires configured S3 storage for project {project!r}."
        )
    if location is not None:
        bucket, key, region = location
        try:
            import boto3  # type: ignore

            response = boto3.client("s3", region_name=region).get_object(Bucket=bucket, Key=key)
            raw = json.loads(response["Body"].read().decode("utf-8"))
            state = normalize_maintenance_state(raw, project=project)
            if not state["schema_valid"]:
                raise MaintenanceStorageError(
                    f"Invalid dashboard maintenance state at s3://{bucket}/{key}."
                )
            state["_storage_etag"] = str(response.get("ETag") or "").strip()
            return state
        except Exception as exc:
            if _s3_error_code(exc) in {"NoSuchKey", "404"}:
                if require_configured_remote:
                    capability_key = f"{key.rsplit('/', 1)[0]}/maintenance-capability-v1.json"
                    try:
                        capability_response = boto3.client("s3", region_name=region).get_object(
                            Bucket=bucket,
                            Key=capability_key,
                        )
                        capability = json.loads(
                            capability_response["Body"].read().decode("utf-8")
                        )
                    except Exception as capability_exc:
                        if _s3_error_code(capability_exc) not in {"NoSuchKey", "404"}:
                            raise MaintenanceStorageError(
                                f"Failed to verify dashboard maintenance capability from "
                                f"s3://{bucket}/{capability_key}: {capability_exc}"
                            ) from capability_exc
                    else:
                        if not isinstance(capability, dict) or capability.get("marker") != "dashboard-maintenance-capability-v1" or capability.get("project") != project:
                            raise MaintenanceStorageError(
                                f"Invalid dashboard maintenance capability at s3://{bucket}/{capability_key}."
                            )
                        raise MaintenanceStorageError(
                            f"Dashboard maintenance state is missing at s3://{bucket}/{key} "
                            "after maintenance capability was enabled."
                        )
                state = _empty_maintenance_state(project)
                state["_storage_object_absent"] = True
                return state
            if require_configured_remote:
                raise MaintenanceStorageError(
                    f"Failed to load dashboard maintenance from s3://{bucket}/{key}: {exc}"
                ) from exc

    path = _local_maintenance_path(project)
    if not path.exists():
        return _empty_maintenance_state(project)
    try:
        return normalize_maintenance_state(json.loads(path.read_text(encoding="utf-8")), project=project)
    except Exception as exc:
        if require_configured_remote:
            raise MaintenanceStorageError(f"Failed to load local dashboard maintenance state: {exc}") from exc
        return _empty_maintenance_state(project)
