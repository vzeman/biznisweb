#!/usr/bin/env python3
"""Run the shared creditnote status guard without report export or email."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import logging
import os
from urllib.parse import urlparse

from creditnote_storno_guard import run_creditnote_storno_guard
from invoice_automation_state import build_automation_state_store
from reporting_core import (
    load_project_env, load_project_settings, put_metric, resolve_biznisweb_api_url,
    resolve_reporting_defaults,
)
from reporting_core.metrics import automation_metric_defaults


SUMMARY_PREFIX = "CREDITNOTE_STANDALONE_SUMMARY "
COUNTERS = {
    "fetched_creditnotes": "FetchedCreditnotes",
    "exported_creditnotes": "ExportedCreditnotes",
    "creditnoted_orders": "CreditnotedOrders",
    "checked_orders": "CheckedOrders",
    "eligible_orders": "EligibleOrders",
    "updated_orders": "UpdatedOrders",
    "failed_orders": "FailedOrders",
    "review_required_orders": "ReviewRequiredOrders",
    "partial_creditnote_orders": "PartialCreditnoteOrders",
    "audit_error_orders": "AuditErrorOrders",
}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True, choices=("roy", "vevo"))
    parser.add_argument("--dry-run", action="store_true", help="Inspect without status, audit or journal writes")
    return parser.parse_args(argv)


@contextmanager
def _aggregate_logging():
    # Shared business code logs private order identities. This scheduled entry
    # point emits aggregate diagnostics; detailed mutation evidence stays in S3.
    previous = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        yield
    finally:
        logging.disable(previous)


def _validate_runtime(project, settings):
    endpoint = urlparse(resolve_biznisweb_api_url(project, settings))
    if (endpoint.scheme != "https" or endpoint.hostname not in {
            f"{project}.flox.sk", f"{project}.sk", f"www.{project}.sk",
        } or endpoint.path != "/api/graphql" or endpoint.port not in (None, 443)
            or endpoint.username or endpoint.password or endpoint.query or endpoint.fragment):
        raise RuntimeError("Creditnote runtime API project mismatch")
    token = os.getenv("BIZNISWEB_API_TOKEN", "").strip()
    prefixed_token = os.getenv(f"{project.upper()}_BIZNISWEB_API_TOKEN", "").strip()
    if not token or (prefixed_token and token != prefixed_token):
        raise RuntimeError("Creditnote runtime token is missing or inconsistent")


def _aggregate_summary(summary, project, dry_run):
    if summary.project != project or summary.enabled is not True or summary.dry_run is not dry_run:
        raise RuntimeError("Creditnote guard summary identity mismatch")
    if (getattr(summary, "creditnote_scan_complete", True) is not True
            or getattr(summary, "skipped_locked", False) is not False):
        raise RuntimeError("Creditnote guard did not complete its inspection")
    counts = {name: getattr(summary, name) for name in COUNTERS if name != "audit_error_orders"}
    if not isinstance(summary.audit_errors, dict):
        raise RuntimeError("Creditnote guard audit summary is invalid")
    counts["audit_error_orders"] = len(summary.audit_errors)
    if any(type(value) is not int or value < 0 for value in counts.values()):
        raise RuntimeError("Creditnote guard counters are invalid")
    if (counts["checked_orders"] != counts["creditnoted_orders"]
            or counts["creditnoted_orders"] > counts["exported_creditnotes"]
            or counts["exported_creditnotes"] > counts["fetched_creditnotes"]
            or counts["eligible_orders"] > counts["checked_orders"]
            or counts["updated_orders"] > counts["eligible_orders"]
            or any(counts[name] > counts["checked_orders"] for name in (
                "failed_orders", "review_required_orders", "partial_creditnote_orders", "audit_error_orders",
            )) or (dry_run and counts["updated_orders"])):
        raise RuntimeError("Creditnote guard inspection is incomplete or inconsistent")
    # The shared guard returns only after its strict complete creditnote fetch;
    # pagination/context errors raise. Every attributed order must also be checked.
    return {"project": project, "enabled": True, "dry_run": dry_run,
            "creditnote_scan_complete": True, "skipped_locked": False, **counts}


def run_creditnote_runner(args):
    project, dry_run = args.project, args.dry_run
    if project not in {"roy", "vevo"} or type(dry_run) is not bool:
        raise ValueError("Creditnote runner requires an explicit project and run mode")
    defaults = automation_metric_defaults(resolve_reporting_defaults(project), dry_run)
    output = {"project": project, "dry_run": dry_run, "enabled": False,
              "creditnote_scan_complete": False, "skipped_locked": False, "ok": False}
    stage = "configuration"
    with _aggregate_logging():
        try:
            os.environ["REPORT_PROJECT"] = project
            load_project_env(project)
            settings = load_project_settings(project)
            defaults = automation_metric_defaults(resolve_reporting_defaults(project, settings), dry_run)
            if (settings.get("creditnote_storno_guard") or {}).get("enabled") is not True:
                raise RuntimeError("Standalone creditnote guard must be explicitly enabled")
            output["enabled"] = True
            _validate_runtime(project, settings)
            state_store = None
            if not dry_run:
                stage = "durable_state"
                state_store = build_automation_state_store(project, settings)
                state_store.read()  # Required even when the current candidate set is empty.
            stage = "guard"
            result = run_creditnote_storno_guard(
                project_name=project, project_settings=settings, dry_run=dry_run,
                automation_state_store=state_store,
            )
            stage = "summary"
            output = _aggregate_summary(result, project, dry_run)
            for name, metric in COUNTERS.items():
                put_metric("CreditnoteStandalone" + metric, output[name], project, defaults)
            if output["failed_orders"] or output["review_required_orders"] or output["audit_error_orders"]:
                raise RuntimeError("Creditnote guard has unresolved failures or review")
            output["ok"] = True
            put_metric("CreditnoteStandaloneRunSucceeded", 1, project, defaults)
            print(SUMMARY_PREFIX + json.dumps(output, sort_keys=True), flush=True)
            return output
        except Exception as error:
            output.update(ok=False, failure_stage=stage, error_type=type(error).__name__)
            put_metric("CreditnoteStandaloneRunFailed", 1, project, defaults)
            print(SUMMARY_PREFIX + json.dumps(output, sort_keys=True), flush=True)
            raise RuntimeError("Standalone creditnote guard failed; review aggregate diagnostics and private audit") from None


def main(argv=None):
    try:
        run_creditnote_runner(parse_args(argv))
    except Exception:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
