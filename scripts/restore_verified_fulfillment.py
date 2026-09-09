#!/usr/bin/env python3
"""Restore one reviewed historical shipment silently; dry-run unless --apply.

Historical proof belongs in ignored data/<project>/order-automation/, never Git.
The shared private journal records intent before the one status mutation. An
uncertain outcome stays blocked for review; this tool never retries a write.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager, nullcontext
from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from invoice_automation_state import S3AutomationStateStore, parse_utc, resolve_automation_state_location
from order_status_safety import (
    change_status_verified, decide_recovery, execute_read, fetch_order_safety_context,
    normalize_status, status_write_block_reason,
)
from unpaid_order_cancellation import (
    LIST_ORDER_STATUSES_QUERY, build_client, resolve_unpaid_cancellation_settings,
)


class RestorationBlocked(RuntimeError):
    """A safe, non-sensitive explanation for a closed restoration gate."""


def validate_evidence(evidence, project, order_number, now):
    if not isinstance(evidence, dict) or (
        evidence.get("project") != project
        or evidence.get("order_number") != order_number
        or evidence.get("previous_status") != "Odoslaná"
        or evidence.get("source") != "authenticated_admin_order_history"
    ):
        raise RestorationBlocked("Historical shipment evidence identity or source mismatch")
    try:
        age = now - parse_utc(evidence["observed_at"])
    except (KeyError, TypeError, ValueError, RuntimeError) as exc:
        raise RestorationBlocked("Historical evidence timestamp is invalid") from exc
    if not timedelta(0) <= age <= timedelta(hours=6):
        raise RestorationBlocked("Historical evidence must be refreshed")
    # Do not copy unrelated admin/customer fields into the automation journal.
    return {key: evidence[key] for key in (
        "project", "order_number", "previous_status", "observed_at", "source",
    )}


def validate_prior(prior):
    if not isinstance(prior, dict):
        raise RestorationBlocked("Invalid order journal")
    if status_write_block_reason(prior):
        raise RestorationBlocked("Previous status operation requires review")
    if (prior.get("phase") in {"preparing", "prepare_ambiguous", "creating", "create_ambiguous"}
            or prior.get("email_state") in {"sending", "ambiguous"}):
        raise RestorationBlocked("Previous document or email operation is uncertain")


def restoration_target(client, settings):
    result = execute_read(client, LIST_ORDER_STATUSES_QUERY, variable_values={"lang_code": settings.lang_code})
    rows = result.get("listOrderStatuses")
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise RestorationBlocked("Status collection is incomplete")
    matches = [row for row in rows if normalize_status(row.get("name")) == "odoslana"]
    if len(matches) != 1 or matches[0].get("name") != "Odoslaná":
        raise RestorationBlocked("Historical shipment target is ambiguous")
    identity = matches[0].get("id")
    if isinstance(identity, bool) or not str(identity).isdigit() or int(identity) <= 0:
        raise RestorationBlocked("Historical shipment target identity is invalid")
    if sum(str(row.get("id")) == str(identity) for row in rows) != 1:
        raise RestorationBlocked("Status identities are duplicated")
    return int(identity)


def validate_order(order, order_number, expected_status_id, settings, creditnotes):
    if (not isinstance(order, dict) or str(order.get("order_num")) != order_number
            or not order.get("id") or order.get("blocked") is not False):
        raise RestorationBlocked("Current order identity or blocked flag is unsafe")
    status = order.get("status") or {}
    if isinstance(status.get("id"), bool) or str(status.get("id")) != str(expected_status_id):
        raise RestorationBlocked("Current status differs from the reviewed expected status")
    normalized = normalize_status(status.get("name"))
    # Failed attempts may recover; cancellations/refunds are never reversed here,
    # even if accidentally added to a configurable recovery source list.
    if any(word in normalized for word in ("cancel", "refund", "storno", "zrus", "vraten", "dobropis")):
        raise RestorationBlocked("Cancellation or refund status cannot be restored")
    sources = {settings.recovery_target_status_name, *settings.recovery_source_statuses}
    if normalized not in {normalize_status(name) for name in sources}:
        raise RestorationBlocked("Current status is outside the configured recovery sources")
    if not isinstance(creditnotes, dict) or any(not isinstance(value, list) for value in creditnotes.values()):
        raise RestorationBlocked("Complete creditnote evidence is required")
    decision = decide_recovery(order, sources, creditnote_status=(
        "present" if order_number in creditnotes else "clear"
    ), verified_previous_status="Odoslaná")
    if decision.action != "shipped":
        raise RestorationBlocked("Payment, fulfillment or creditnote evidence does not permit restoration")
    return decision


def restore_one(*, client, store, project_settings, project, order_number,
                expected_status_id, evidence, creditnote_loader, apply=False, now=None):
    now = now or datetime.now(timezone.utc)
    if (project not in {"roy", "vevo"} or not order_number.strip()
            or isinstance(expected_status_id, bool) or not isinstance(expected_status_id, int) or expected_status_id <= 0):
        raise RestorationBlocked("Invalid restoration scope")
    proof = validate_evidence(evidence, project, order_number, now)
    settings = resolve_unpaid_cancellation_settings(project_settings)
    state, _ = store.read()
    if state.get("project") != project or state.get("lease"):
        raise RestorationBlocked("Project state is mismatched or already leased")
    validate_prior(state["orders"].get(order_number, {}))
    lease = store.lease(owner="verified-fulfillment-restoration") if apply else nullcontext(None)
    with lease as journal:
        if journal:
            validate_prior(journal.get_order(order_number))
        heartbeat = journal.assert_owned if journal else None
        target_id = restoration_target(client, settings)
        # A complete creditnote scan can take time; heartbeat it, then retrieve
        # fresh order/payment context immediately before recording write intent.
        creditnotes = creditnote_loader(progress_callback=heartbeat)
        if heartbeat:
            heartbeat()
        order = fetch_order_safety_context(client, order_number)
        decision = validate_order(order, order_number, expected_status_id, settings, creditnotes)
        if not apply:
            return {"project": project, "dry_run": True, "eligible_orders": 1, "restored_orders": 0}
        journal.assert_owned()
        # A final read catches a native webhook/operator change during inspection.
        fresh = fetch_order_safety_context(client, order_number)
        validate_order(fresh, order_number, expected_status_id, settings, creditnotes)
        if str(fresh["id"]) != str(order["id"]):
            raise RestorationBlocked("Order identity changed during inspection")
        record = {
            "state": "pending", "reason": "verified_historical_fulfillment_restoration",
            "source_status_id": expected_status_id, "source_status_name": fresh["status"]["name"],
            "target_status_id": target_id, "target_status_name": "Odoslaná",
            "observed_at": now.isoformat(), "historical_evidence": proof,
            "evidence_sha256": hashlib.sha256(json.dumps(proof, sort_keys=True).encode()).hexdigest(),
            "decision_reason": decision.reason,
        }
        journal.update_order(order_number, status_mutation=record)
        try:
            journal.assert_owned()
            change_status_verified(client, order_number, target_id, "Odoslaná", silent=True)
        except Exception as exc:
            journal.update_order(order_number, status_mutation={**record, "state": "uncertain"})
            raise RestorationBlocked("Status outcome is uncertain; manual review required, no retry") from exc
        journal.update_order(
            order_number, status_mutation={**record, "state": "verified"},
            verified_fulfillment_status="Odoslaná", status_observation_source="verified_manual_restoration",
            status_observed_at=store.now().isoformat(), status_review_reason=None,
            status_review={"state": "closed", "reason": "verified_manual_restoration"},
        )
        if journal.get_order(order_number).get("status_mutation") != {**record, "state": "verified"}:
            raise RestorationBlocked("Verified status journal readback failed; review before any retry")
    return {"project": project, "dry_run": False, "eligible_orders": 1, "restored_orders": 1}


def require_reviewed_code(root):
    branch = subprocess.check_output(["git", "branch", "--show-current"], cwd=root, text=True).strip()
    if not branch.startswith("codex/"):
        raise RestorationBlocked("Use the reviewed restoration branch")
    subprocess.run(["git", "ls-files", "--error-unmatch", "scripts/restore_verified_fulfillment.py",
                    "invoice_automation_state.py", "order_status_safety.py", "creditnote_export.py"],
                   cwd=root, check=True, capture_output=True)
    subprocess.run(["git", "fetch", "origin", branch], cwd=root, check=True, capture_output=True)
    changed = subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=no"], cwd=root, text=True).strip()
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    remote = subprocess.check_output(["git", "rev-parse", f"origin/{branch}"], cwd=root, text=True).strip()
    if changed or head != remote:
        raise RestorationBlocked("Restoration code must be clean, committed and pushed")


def load_private_evidence(root, path, project):
    resolved = path.resolve(strict=True)
    private_root = (root / "data" / project / "order-automation").resolve()
    if not resolved.is_relative_to(private_root) or not resolved.is_file():
        raise RestorationBlocked("Evidence must remain in the selected private project directory")
    relative = resolved.relative_to(root).as_posix()
    ignored = subprocess.run(["git", "check-ignore", "--quiet", "--", relative], cwd=root)
    tracked = subprocess.check_output(["git", "ls-files", "--", relative], cwd=root, text=True).strip()
    if ignored.returncode != 0 or tracked:
        raise RestorationBlocked("Historical evidence must be ignored and untracked")
    return json.loads(resolved.read_text(encoding="utf-8"))


@contextmanager
def runtime_environment(project, secret, settings):
    url = secret.get("BIZNISWEB_API_URL", "")
    parsed = urlparse(url)
    allowed_hosts = {urlparse(settings["biznisweb_api_url"]).hostname, f"www.{project}.sk", f"{project}.sk"}
    if (parsed.scheme != "https" or parsed.hostname not in allowed_hosts or parsed.path != "/api/graphql"
            or parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.port not in (None, 443)):
        raise RestorationBlocked("Runtime API destination differs from the reviewed shop")
    keys = ("BIZNISWEB_API_URL", "BIZNISWEB_API_TOKEN", "BIZNISWEB_USERNAME", "BIZNISWEB_PASSWORD")
    if any(not isinstance(secret.get(key), str) or not secret[key] for key in keys):
        raise RestorationBlocked("Selected runtime credentials are incomplete")
    values = {"REPORT_SKIP_PROJECT_ENV": "true", "REPORT_PROJECT": project, "BIZNISWEB_API_TIMEOUT_SEC": "30"}
    for key in keys:
        values[key] = secret[key]
        values[f"{project.upper()}_{key}"] = secret[key]
    original = {key: os.environ.get(key) for key in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for key, value in original.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True, choices=("roy", "vevo"))
    parser.add_argument("--order-number", required=True)
    parser.add_argument("--expected-status-id", type=int, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    logging.disable(logging.CRITICAL)
    root = Path(__file__).resolve().parents[1]
    if args.apply:
        require_reviewed_code(root)
    evidence = load_private_evidence(root, args.evidence, args.project)
    import boto3
    from botocore.config import Config
    session = boto3.Session(profile_name=args.profile, region_name="eu-central-1")
    secret = json.loads(session.client("secretsmanager").get_secret_value(
        SecretId=f"{args.project}/reporting/runtime-env")["SecretString"])
    settings = json.loads((root / "projects" / args.project / "settings.json").read_text(encoding="utf-8"))
    from reporting_core.storage import resolve_report_s3_location
    report_bucket, _ = resolve_report_s3_location(args.project, settings, secret, required=True)
    bucket, key = resolve_automation_state_location(args.project, settings, secret)
    if bucket != report_bucket or key != f"data/{args.project}/order-automation/state.json":
        raise RestorationBlocked("State destination differs from the reviewed deployment")
    store = S3AutomationStateStore(session.client("s3", config=Config(
        connect_timeout=10, read_timeout=30, retries={"total_max_attempts": 1})), bucket, key, args.project)
    with runtime_environment(args.project, secret, settings):
        from creditnote_export import fetch_creditnote_automation_context
        client = build_client(args.project, settings)
        summary = restore_one(
            client=client, store=store, project_settings=settings, project=args.project,
            order_number=args.order_number, expected_status_id=args.expected_status_id, evidence=evidence,
            creditnote_loader=lambda **kwargs: fetch_creditnote_automation_context(args.project, **kwargs),
            apply=args.apply,
        )
    print(json.dumps(summary), flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # Third-party HTTP exceptions may contain private URLs or response data.
        print(json.dumps({"ok": False, "error_type": type(error).__name__, "message": (
            str(error) if isinstance(error, RestorationBlocked) else "Restoration stopped; inspect private runtime evidence"
        )}), flush=True)
        raise SystemExit(1) from None
