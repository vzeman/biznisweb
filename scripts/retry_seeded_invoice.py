#!/usr/bin/env python3
"""Preview or attempt one existing historical invoice seed; never email it.

The private journal selects the order. There is no arbitrary-order argument,
write retry, or alternate invoice endpoint. --apply delegates exactly one
attempt to the normal invoice generator under its shared project lease.
"""
from __future__ import annotations

import argparse
from contextlib import nullcontext
import json
import logging
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from invoice_automation_state import S3AutomationStateStore, resolve_automation_state_location
from reporting_core.storage import resolve_report_s3_location
from scripts.restore_verified_fulfillment import runtime_environment
from scripts.verify_invoice_discovery import require_pushed_source


SOURCE = "complete_historical_backlog_audit"
EXPECTED_COUNTS = {"roy": 6, "vevo": 5}


class SeededRetryBlocked(RuntimeError):
    """A fixed, non-sensitive explanation for a closed one-attempt gate."""


def select_seed(state, project, expected_count, *, owned_lease=False):
    if (project not in EXPECTED_COUNTS or not isinstance(expected_count, int) or isinstance(expected_count, bool)
            or expected_count != EXPECTED_COUNTS[project]):
        raise SeededRetryBlocked("Historical retry scope differs from the reviewed batch")
    if (not isinstance(state, dict) or state.get("project") != project
            or state.get("schema_version") != 1 or not isinstance(state.get("orders"), dict)):
        raise SeededRetryBlocked("Historical retry journal identity or schema mismatch")
    if state.get("lease") and not owned_lease:
        raise SeededRetryBlocked("Historical retry journal is already leased")
    if any(not isinstance(row, dict) for row in state["orders"].values()):
        raise SeededRetryBlocked("Historical retry journal contains an invalid record")
    seeds = {number: row for number, row in state["orders"].items() if row.get("source") == SOURCE}
    if len(seeds) != expected_count:
        raise SeededRetryBlocked("Historical retry seed count or source mismatch")
    candidates = []
    for number, row in seeds.items():
        if (not isinstance(number, str) or not number.strip() or row.get("order_num") != number
                or row.get("project", project) != project or row.get("email_policy") != "hold"):
            raise SeededRetryBlocked("Historical seed identity, project or email hold mismatch")
        if row.get("phase") not in {"pending", "create_failed", "complete"}:
            raise SeededRetryBlocked("Historical document operation requires review before retry")
        mutation = row.get("status_mutation")
        if mutation is not None and (not isinstance(mutation, dict) or mutation.get("state") != "verified"):
            raise SeededRetryBlocked("Historical status operation requires review before retry")
        if row.get("email_state") not in {None, "held"}:
            raise SeededRetryBlocked("Historical email operation requires review before retry")
        if row["phase"] in {"pending", "create_failed"}:
            if row.get("invoice_id") or row.get("invoice_num"):
                raise SeededRetryBlocked("Unfinished historical seed already has a document identity")
            candidates.append(number)
    if not candidates:
        raise SeededRetryBlocked("No unambiguous pending historical seed remains")
    return min(candidates)


def safe_creation_failure(record):
    failure = record.get("last_creation_failure")
    if not isinstance(failure, dict):
        return {}
    status = failure.get("http_status")
    stage, kind = failure.get("stage"), failure.get("kind")
    return {
        "stage": stage if isinstance(stage, str) and stage in {
            "preflight", "preparation", "finalization", "readback",
        } else "unknown",
        "http_status": status if isinstance(status, int) and not isinstance(status, bool) and 100 <= status <= 599 else None,
        "kind": kind if isinstance(kind, str) and kind in {
            "rejected", "unconfirmed", "final_invoice_readback_missing", "readback_failed",
            "invalid_internal_id", "order_identity_changed", "preinvoice_missing", "preinvoice_malformed",
            "preinvoice_multiple", "status_changed", "preinvoice_changed",
            "preinvoice_readback_failed", "preinvoice_readback_missing", "web_session_unavailable",
        } else "unknown",
    }


def retry_one_seed(*, store, project, expected_count, generator_factory, apply=False):
    state, _ = store.read()
    selected = select_seed(state, project, expected_count)
    lease = store.lease(owner="reviewed-seeded-invoice-attempt") if apply else nullcontext(None)
    with lease as journal:
        if journal:
            if select_seed(journal.snapshot(), project, expected_count, owned_lease=True) != selected:
                raise SeededRetryBlocked("Historical seed selection changed before lease acquisition")
            journal.assert_owned()
        generator = generator_factory(web_login=apply)
        try:
            generator.operation_journal = journal
            generator.send_invoice_email_enabled = False
            if journal:
                journal.assert_owned()
            if apply and (not generator.web_session or not generator.validate_session()):
                raise SeededRetryBlocked("Historical invoice web session is unverified")
            if not apply and generator.web_session:
                raise SeededRetryBlocked("Historical invoice preview must not open a web session")
            generator.resolve_eligible_status_ids()
            order = generator.fetch_order_for_invoice(selected)
            if (str(order.get("order_num") or "") != selected or order.get("invoices")
                    or not generator.filter_orders_for_invoice([order])[0]):
                raise SeededRetryBlocked("Selected historical order is no longer eligible for invoice creation")
            summary = {"project": project, "dry_run": not apply, "seeded_orders": expected_count,
                       "eligible_orders": 1, "attempted_orders": 0, "created_invoices": 0,
                       "failed_invoices": 0, "ambiguous_operations": 0, "emailed_invoices": 0}
            if not apply:
                return {**summary, "ok": True}
            # Revalidate the complete seed batch after authentication and API
            # inspection. The generator performs its own current/final checks.
            if select_seed(journal.snapshot(), project, expected_count, owned_lease=True) != selected:
                raise SeededRetryBlocked("Historical seed selection changed during inspection")
            journal.assert_owned()
            result = generator.create_invoice(order)
            journal.assert_owned()
            record = journal.get_order(selected)
            if record.get("source") != SOURCE or record.get("email_policy") != "hold" or result.email_sent:
                raise SeededRetryBlocked("Historical invoice attempt did not preserve its source and email hold")
            verified = False
            if result.created:
                current = generator.fetch_order_for_invoice(selected)
                invoices = current.get("invoices")
                verified = bool(
                    result.invoice_id and record.get("phase") == "complete" and record.get("email_state") == "held"
                    and str(record.get("invoice_id")) == str(result.invoice_id)
                    and isinstance(result.invoice_num, str) and result.invoice_num.strip()
                    and record.get("invoice_num") == result.invoice_num
                    and isinstance(invoices, list) and len(invoices) == 1
                    and str(invoices[0].get("id")) == str(result.invoice_id)
                    and invoices[0].get("invoice_num") == result.invoice_num
                )
                if not verified:
                    raise SeededRetryBlocked("Historical invoice journal or final document readback is unverified")
            summary.update(ok=verified, attempted_orders=1, created_invoices=int(verified),
                           failed_invoices=int(not verified), ambiguous_operations=int(result.ambiguous))
            if not verified and not result.skipped:
                summary["creation_failure"] = safe_creation_failure(record)
            return summary
        finally:
            if generator.web_session:
                generator.web_session.close()


def require_retry_source(root):
    subprocess.run(["git", "ls-files", "--error-unmatch", "scripts/retry_seeded_invoice.py"],
                   cwd=root, check=True, capture_output=True)
    return require_pushed_source(root)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True, choices=tuple(EXPECTED_COUNTS))
    parser.add_argument("--expected-count", required=True, type=int)
    parser.add_argument("--profile", default="codex")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    if args.expected_count != EXPECTED_COUNTS[args.project]:
        parser.error("--expected-count must match the reviewed project batch")
    logging.disable(logging.CRITICAL)
    root = Path(__file__).resolve().parents[1]
    require_retry_source(root)
    import boto3
    session = boto3.Session(profile_name=args.profile, region_name="eu-central-1")
    secret = json.loads(session.client("secretsmanager").get_secret_value(
        SecretId=f"{args.project}/reporting/runtime-env")["SecretString"])
    settings = json.loads((root / "projects" / args.project / "settings.json").read_text(encoding="utf-8"))
    bucket, key = resolve_automation_state_location(args.project, settings, secret)
    report_bucket, _ = resolve_report_s3_location(args.project, settings, secret, required=True)
    if bucket != report_bucket or key != f"data/{args.project}/order-automation/state.json":
        raise SeededRetryBlocked("Historical retry state destination differs from the reviewed deployment")
    s3 = session.client("s3")
    account = session.client("sts").get_caller_identity()["Account"]
    s3.head_bucket(Bucket=bucket, ExpectedBucketOwner=account)
    privacy = s3.get_public_access_block(Bucket=bucket, ExpectedBucketOwner=account)["PublicAccessBlockConfiguration"]
    if not all(privacy.get(name) is True for name in (
        "BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets",
    )):
        raise SeededRetryBlocked("Historical retry state bucket privacy is unverified")
    store = S3AutomationStateStore(s3, bucket, key, args.project)
    with runtime_environment(args.project, secret, settings):
        from generate_invoices import InvoiceGenerator, resolve_invoice_generation_settings
        from reporting_core import derive_biznisweb_base_url
        configured = resolve_invoice_generation_settings(settings)
        if not configured["enabled"] or not configured["safety_state_enabled"]:
            raise SeededRetryBlocked("Historical invoice safety automation must be enabled")

        def generator_factory(*, web_login):
            url = secret["BIZNISWEB_API_URL"]
            return InvoiceGenerator(url, secret["BIZNISWEB_API_TOKEN"], derive_biznisweb_base_url(url),
                username=secret["BIZNISWEB_USERNAME"] if web_login else None,
                password=secret["BIZNISWEB_PASSWORD"] if web_login else None,
                send_invoice_email=False, eligible_statuses=configured["eligible_statuses"],
                exclude_zero_total_orders=configured["exclude_zero_total_orders"],
                page_delay_seconds=configured["page_delay_seconds"], read_attempts=configured["read_attempts"])

        summary = retry_one_seed(store=store, project=args.project, expected_count=args.expected_count,
                                 generator_factory=generator_factory, apply=args.apply)
    print(json.dumps(summary, sort_keys=True), flush=True)
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(json.dumps({"ok": False, "error_type": type(error).__name__,
                          "message": "Seeded invoice attempt stopped; inspect the private journal before any retry"}), flush=True)
        raise SystemExit(1) from None
