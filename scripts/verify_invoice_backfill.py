#!/usr/bin/env python3
"""Read-only business verification of the seeded historical invoice backlog.

Only --publish-report writes to AWS, and only immutable private report objects.
This tool never acquires a lease or changes invoices, emails, orders or journals.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import html
import json
import logging
from pathlib import Path
import subprocess
import sys
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from invoice_automation_state import S3AutomationStateStore, resolve_automation_state_location
from reporting_core.storage import resolve_report_s3_location
from scripts.restore_verified_fulfillment import runtime_environment

SOURCE = "complete_historical_backlog_audit"
EXTERNAL_REASONS = {"invoice_exists", "invoice_created_elsewhere", "changed_before_finalization"}
INELIGIBLE_REASONS = {"eligibility_changed", "changed_before_finalization"}


def assess_order(number: str, record: dict, order: Any, shipped_status_ids: set[int]) -> dict:
    """Return minimal private evidence; unsupported or unfinished outcomes fail."""
    result = {"order_num": number, "ok": False, "outcome": "unverified", "issues": []}
    issues = result["issues"]
    if str(record.get("order_num") or "") != number:
        issues.append("journal_order_identity_mismatch")
    if record.get("email_policy") != "hold":
        issues.append("historical_email_hold_missing")
    if record.get("phase") != "complete":
        issues.append("journal_operation_unfinished")
    mutation = record.get("status_mutation")
    if mutation is not None and (not isinstance(mutation, dict) or mutation.get("state") != "verified"):
        issues.append("status_write_unresolved")
    if record.get("email_state") in {"sending", "ambiguous", "pending", "failed", "sent"}:
        issues.append("email_hold_not_preserved")
    result["journal"] = {key: record.get(key) for key in (
        "phase", "email_policy", "email_state", "invoice_id", "reason",
    )}
    if not isinstance(order, dict) or str(order.get("order_num") or "") != number or not order.get("id"):
        issues.append("fresh_order_identity_missing_or_mismatched")
        return result
    status = order.get("status")
    if (not isinstance(status, dict) or isinstance(status.get("id"), bool)
            or not str(status.get("id") or "").isdigit() or int(status["id"]) <= 0
            or not isinstance(order.get("blocked"), bool)):
        issues.append("fresh_eligibility_evidence_incomplete")
        return result
    money = order.get("sum")
    try:
        raw_amount = money["value"]
        if isinstance(raw_amount, bool):
            raise ValueError("Boolean amount")
        amount = Decimal(str(raw_amount))
        if not amount.is_finite():
            raise ValueError("Non-finite amount")
    except (KeyError, TypeError, ValueError, InvalidOperation):
        issues.append("fresh_amount_incomplete")
        return result
    invoices = order.get("invoices")
    if "invoices" not in order or (invoices is not None and not isinstance(invoices, list)):
        issues.append("fresh_invoice_collection_incomplete")
        return result
    invoices = invoices or []
    if any(not isinstance(row, dict) or not row.get("id") for row in invoices):
        issues.append("fresh_invoice_identity_incomplete")
        return result
    ids = [str(row["id"]) for row in invoices]
    result.update(status={"id": status["id"], "name": status.get("name")}, blocked=order["blocked"],
                  amount=str(amount), currency=(money.get("currency") or {}).get("code"),
                  invoices=[{"id": str(row["id"]), "invoice_num": row.get("invoice_num")} for row in invoices])
    ineligible = []
    if order["blocked"]:
        ineligible.append("blocked")
    if int(status["id"]) not in shipped_status_ids:
        ineligible.append("not_shipped")
    if amount <= 0:
        ineligible.append("nonpositive_total")
    result["currently_eligible"] = not ineligible
    result["ineligible_reasons"] = ineligible
    if len(ids) > 1:
        issues.append("multiple_final_invoices")
    if record.get("invoice_id") and ids != [str(record["invoice_id"])]:
        issues.append("journal_invoice_identity_mismatch")
    reason = record.get("reason")
    if ids:
        if record.get("email_state") == "held":
            result["outcome"] = "invoiced_now_ineligible" if ineligible else "verified_invoice"
        elif reason in EXTERNAL_REASONS and record.get("email_state") is None:
            result["outcome"] = "externally_invoiced_now_ineligible" if ineligible else "externally_invoiced"
        else:
            issues.append("invoice_completion_or_email_hold_unverified")
    elif ineligible and reason in INELIGIBLE_REASONS:
        result["outcome"] = "no_longer_eligible"
    else:
        issues.append("missing_final_invoice" if not ineligible else "ineligible_completion_unexplained")
    result["ok"] = not issues
    return result


def verify_backfill(*, store, project: str, expected_count: int, read_order, shipped_status_ids: set[int],
                    source_commit: str = "", now=None) -> dict:
    if expected_count <= 0 or not shipped_status_ids or any(
        not isinstance(value, int) or isinstance(value, bool) or value <= 0 for value in shipped_status_ids
    ):
        raise ValueError("Invalid verification scope")
    now = now or datetime.now(timezone.utc)
    report = {"schema_version": 1, "project": project, "observed_at": now.isoformat(),
              "source_commit": source_commit, "read_only_business_check": True, "ok": False,
              "expected_count": expected_count, "selected_count": 0, "checked_count": 0,
              "failed_orders": 0, "outcome_counts": {}, "global_issues": [], "orders": []}
    state, before_etag = store.read()
    if (not isinstance(state, dict) or state.get("project") != project or state.get("schema_version") != 1
            or not isinstance(state.get("orders"), dict) or not before_etag):
        report["global_issues"].append("journal_identity_or_schema_incomplete")
        return report
    if state.get("lease"):
        report["global_issues"].append("journal_run_or_retained_lease_present")
    report["journal_etag"] = before_etag
    if any(not isinstance(row, dict) for row in state["orders"].values()):
        report["global_issues"].append("journal_order_schema_incomplete")
        return report
    selected = {str(number): row for number, row in state["orders"].items() if row.get("source") == SOURCE}
    report["selected_count"] = len(selected)
    if len(selected) != expected_count:
        report["global_issues"].append("seeded_order_count_mismatch")
    for number, record in sorted(selected.items()):
        try:
            order = read_order(number)
            row = assess_order(number, record, order, shipped_status_ids)
        except Exception as error:
            # Never place a raw third-party exception/response in the report.
            row = {"order_num": number, "ok": False, "outcome": "unverified",
                   "issues": ["fresh_order_read_failed"], "error_type": type(error).__name__}
        report["orders"].append(row)
    report["checked_count"] = len(report["orders"])
    try:
        after, after_etag = store.read()
        report["journal_final_etag"] = after_etag
        if before_etag != after_etag or after != state:
            report["global_issues"].append("journal_changed_during_verification")
    except Exception:
        report["global_issues"].append("journal_final_read_failed")
    report["ok"] = (not report["global_issues"] and report["checked_count"] == expected_count
                    and all(row["ok"] for row in report["orders"]))
    report["outcome_counts"] = dict(Counter(row["outcome"] for row in report["orders"]))
    report["failed_orders"] = sum(not row["ok"] for row in report["orders"])
    return report


def report_markdown(report: dict) -> str:
    def cell(value):
        return html.escape(str(value if value is not None else "")).replace("|", "\\|").replace("\n", " ")
    lines = [f"# Historical invoice verification — {cell(report['project'])}", "",
             f"Result: **{'PASS' if report['ok'] else 'FAIL'}**", "",
             f"Observed: {cell(report['observed_at'])}", f"Source commit: {cell(report['source_commit'])}",
             f"Expected / selected / checked: {report['expected_count']} / {report['selected_count']} / {report['checked_count']}", "",
             "This report reads the live order API and private journal. It performs no business or journal writes.", ""]
    if report["global_issues"]:
        lines += ["Global issues: " + ", ".join(report["global_issues"]), ""]
    lines += ["| Order | Result | Outcome | Current invoice IDs | Issues |", "| --- | --- | --- | --- | --- |"]
    for row in report["orders"]:
        lines.append("| " + " | ".join(cell(value) for value in (
            row["order_num"], "PASS" if row["ok"] else "FAIL", row["outcome"],
            ", ".join(inv["id"] for inv in row.get("invoices", [])), ", ".join(row["issues"]),
        )) + " |")
    return "\n".join(lines) + "\n"


def write_private_report(root: Path, report: dict) -> list[Path]:
    project = report["project"]
    if project not in {"roy", "vevo"}:
        raise ValueError("Unsupported report project")
    folder = (root / "data" / project / "order-automation").resolve()
    if not folder.is_relative_to(root.resolve()):
        raise RuntimeError("Private report destination escaped the repository")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    paths = [folder / f"backfill-verification-{stamp}.{extension}" for extension in ("json", "md")]
    for path in paths:
        relative = path.relative_to(root.resolve()).as_posix()
        ignored = subprocess.run(["git", "check-ignore", "--quiet", "--", relative], cwd=root)
        tracked = subprocess.check_output(["git", "ls-files", "--", relative], cwd=root, text=True).strip()
        if ignored.returncode != 0 or tracked:
            raise RuntimeError("Private reports must be ignored and untracked")
    folder.mkdir(parents=True, exist_ok=True)
    paths[0].write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths[1].write_text(report_markdown(report), encoding="utf-8")
    return paths


def publish_private_report(client, *, bucket: str, project: str, account: str, paths: list[Path]) -> None:
    if project not in {"roy", "vevo"}:
        raise ValueError("Unsupported report project")
    client.head_bucket(Bucket=bucket, ExpectedBucketOwner=account)
    block = client.get_public_access_block(Bucket=bucket, ExpectedBucketOwner=account)["PublicAccessBlockConfiguration"]
    if any(block.get(key) is not True for key in ("BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets")):
        raise RuntimeError("Report bucket privacy is unverified")
    for path in paths:
        if path.suffix not in {".json", ".md"} or not path.name.startswith("backfill-verification-"):
            raise ValueError("Unexpected report artifact")
        payload = path.read_bytes()
        key = f"data/{project}/order-automation/verification/{path.name}"
        client.put_object(Bucket=bucket, Key=key, Body=payload, ExpectedBucketOwner=account,
                          ServerSideEncryption="AES256", IfNoneMatch="*",
                          ContentType="application/json" if path.suffix == ".json" else "text/markdown; charset=utf-8")
        received = client.get_object(Bucket=bucket, Key=key, ExpectedBucketOwner=account)["Body"].read()
        if received != payload:
            raise RuntimeError("Private report readback differs")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", choices=("roy", "vevo"), required=True)
    parser.add_argument("--profile", default="codex")
    parser.add_argument("--expected-count", type=int, required=True)
    parser.add_argument("--publish-report", action="store_true")
    args = parser.parse_args(argv)
    if not 1 <= args.expected_count <= 1000:
        parser.error("--expected-count must be between 1 and 1000")
    logging.disable(logging.CRITICAL)
    import boto3
    from botocore.config import Config
    from generate_invoices import InvoiceGenerator
    from reporting_core import derive_biznisweb_base_url
    root = Path(__file__).resolve().parents[1]
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    session = boto3.Session(profile_name=args.profile, region_name="eu-central-1")
    secret = json.loads(session.client("secretsmanager").get_secret_value(
        SecretId=f"{args.project}/reporting/runtime-env")["SecretString"])
    settings = json.loads((root / "projects" / args.project / "settings.json").read_text(encoding="utf-8"))
    bucket, key = resolve_automation_state_location(args.project, settings, secret)
    report_bucket, _ = resolve_report_s3_location(args.project, settings, secret, required=True)
    if bucket != report_bucket or key != f"data/{args.project}/order-automation/state.json":
        raise RuntimeError("Journal destination differs from canonical project storage")
    s3 = session.client("s3", config=Config(connect_timeout=10, read_timeout=30, retries={"total_max_attempts": 1}))
    store = S3AutomationStateStore(s3, bucket, key, args.project)
    with runtime_environment(args.project, secret, settings):
        url = secret["BIZNISWEB_API_URL"]
        generator = InvoiceGenerator(url, secret["BIZNISWEB_API_TOKEN"], derive_biznisweb_base_url(url), page_delay_seconds=2)
        shipped_ids = generator.resolve_eligible_status_ids()
        report = verify_backfill(store=store, project=args.project, expected_count=args.expected_count,
                                 read_order=generator.fetch_order_for_invoice, shipped_status_ids=shipped_ids,
                                 source_commit=commit)
    paths = write_private_report(root, report)
    if args.publish_report:
        account = session.client("sts").get_caller_identity()["Account"]
        publish_private_report(s3, bucket=bucket, project=args.project, account=account, paths=paths)
    summary = {key: report.get(key) for key in (
        "project", "ok", "expected_count", "selected_count", "checked_count", "failed_orders", "outcome_counts",
    )}
    summary.update(global_issue_count=len(report["global_issues"]), private_reports_written=2,
                   private_reports_published=args.publish_report)
    print(json.dumps(summary, sort_keys=True), flush=True)
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(json.dumps({"ok": False, "error_type": type(error).__name__,
                          "message": "Verification stopped; no business or journal state was changed"}), flush=True)
        raise SystemExit(1) from None
