#!/usr/bin/env python3
"""Verify real invoice discovery without web login or business/state writes.

Compare a complete private historical audit with the actual invoice generator's
full scan and fresh order reads. Only --publish-report writes to AWS, and only
an immutable encrypted private report; no journal or lease is accessed.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import logging
from pathlib import Path
import subprocess
import sys
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from invoice_automation_state import parse_utc
from scripts.restore_verified_fulfillment import load_private_evidence, runtime_environment


def audit_candidates(audit, project, now, expected_count=None):
    if (not isinstance(audit, dict) or audit.get("project") != project or audit.get("complete") is not True
            or not isinstance(audit.get("candidates"), list)):
        raise ValueError("A complete project-matched historical audit is required")
    age = now - parse_utc(audit["observed_at"])
    if not timedelta(0) <= age <= timedelta(hours=24):
        raise ValueError("Historical audit must be refreshed")
    selected, seen = [], set()
    for row in audit["candidates"]:
        if not isinstance(row, dict):
            raise ValueError("Invalid historical candidate")
        number = str(row.get("order_num") or "").strip()
        if not number or number in seen or row.get("blocked") is not False or row.get("invoices") != []:
            raise ValueError("Invalid or repeated historical candidate")
        seen.add(number)
        if row.get("older_than_7_days") is True:
            selected.append(number)
    if expected_count is not None and (isinstance(expected_count, bool) or not isinstance(expected_count, int)
                                       or not 1 <= expected_count <= 1000 or len(selected) != expected_count):
        raise ValueError("Historical audit candidate count differs from the expected count")
    if not selected:
        raise ValueError("No historical candidates available to verify discovery")
    return selected


def verify_discovery(*, generator, audit, project, expected_count=None, source_commit="", now=None):
    now = now or datetime.now(timezone.utc)
    selected = audit_candidates(audit, project, now, expected_count)
    if generator.web_session is not None or generator.operation_journal is not None:
        raise ValueError("Discovery verification must not have web or journal access")
    report = {"schema_version": 1, "project": project, "source_commit": source_commit,
              "observed_at": now.isoformat(), "audit_observed_at": audit["observed_at"],
              "read_only_discovery_check": True, "complete": False, "ok": False,
              "expected_count": len(selected), "scanned_orders": 0, "scan_pages": 0,
              "eligible_orders": 0, "checked_count": 0, "failed_orders": 0,
              "outcome_counts": {}, "global_issues": [], "orders": []}
    try:
        status_ids = generator.resolve_eligible_status_ids()
        if not status_ids:
            raise ValueError("No eligible status identity")
        scanned = generator.fetch_all_eligible_orders()
        numbers = [str(row.get("order_num") or "") for row in scanned]
        if not all(numbers) or len(set(numbers)) != len(numbers):
            raise ValueError("Full discovery contains missing or repeated identities")
        eligible, stats = generator.filter_orders_for_invoice(scanned)
        eligible_numbers = {str(row["order_num"]) for row in eligible}
        scanned_numbers = set(numbers)
        if not eligible_numbers <= scanned_numbers:
            raise ValueError("Invoice filter returned an order outside the completed scan")
        report.update(complete=True, scanned_orders=len(scanned), scan_pages=generator.scan_pages,
                      eligible_orders=len(eligible), filter_counts=stats)
    except Exception as error:
        report["global_issues"].append("full_discovery_failed")
        report["error_type"] = type(error).__name__
        report["scan_pages"] = generator.scan_pages
        return report
    for number in selected:
        row = {"order_num": number, "in_full_scan": number in scanned_numbers,
               "in_eligible_scan": number in eligible_numbers, "ok": False, "outcome": "unverified", "issues": []}
        try:
            fresh = generator.fetch_order_for_invoice(number)
            if str(fresh.get("order_num") or "") != number:
                raise ValueError("Fresh order identity mismatch")
            fresh_eligible, _ = generator.filter_orders_for_invoice([fresh])
            invoices = fresh["invoices"] or []
            row.update(status_id=fresh["status"]["id"], blocked=fresh["blocked"],
                       invoice_ids=[str(invoice["id"]) for invoice in invoices])
            if len(invoices) > 1:
                row["issues"].append("multiple_final_invoices")
            elif invoices:
                row["outcome"] = "already_invoiced"
            elif not fresh_eligible:
                row["outcome"] = "no_longer_eligible"
            elif number not in eligible_numbers:
                row["issues"].append("fresh_candidate_missing_from_discovery")
            else:
                row["outcome"] = "discovered_and_still_eligible"
            row["ok"] = not row["issues"]
        except Exception as error:
            row["issues"].append("fresh_order_read_failed")
            row["error_type"] = type(error).__name__
        report["orders"].append(row)
    report["checked_count"] = len(report["orders"])
    report["outcome_counts"] = dict(Counter(row["outcome"] for row in report["orders"]))
    report["failed_orders"] = sum(not row["ok"] for row in report["orders"])
    report["ok"] = report["complete"] and report["checked_count"] == len(selected) and not report["failed_orders"]
    report["completed_at"] = datetime.now(timezone.utc).isoformat()
    return report


def write_private_report(root, report):
    if report["project"] not in {"roy", "vevo"}:
        raise ValueError("Unsupported report project")
    folder = (root / "data" / report["project"] / "order-automation").resolve()
    if not folder.is_relative_to(root.resolve()):
        raise ValueError("Private report escaped the repository")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = folder / f"invoice-discovery-{stamp}-{uuid4().hex}.json"
    relative = path.relative_to(root.resolve()).as_posix()
    ignored = subprocess.run(["git", "check-ignore", "--quiet", "--", relative], cwd=root)
    tracked = subprocess.check_output(["git", "ls-files", "--", relative], cwd=root, text=True).strip()
    if ignored.returncode != 0 or tracked:
        raise ValueError("Private reports must be ignored and untracked")
    folder.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as output:
        json.dump(report, output, ensure_ascii=False, indent=2, sort_keys=True)
        output.write("\n")
    return path


def publish_private_report(client, *, bucket, project, account, path):
    if project not in {"roy", "vevo"} or not path.name.startswith("invoice-discovery-") or path.suffix != ".json":
        raise ValueError("Unexpected discovery report artifact")
    client.head_bucket(Bucket=bucket, ExpectedBucketOwner=account)
    block = client.get_public_access_block(Bucket=bucket, ExpectedBucketOwner=account)["PublicAccessBlockConfiguration"]
    if not all(block.get(key) is True for key in ("BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets")):
        raise ValueError("Report bucket privacy is unverified")
    payload = path.read_bytes()
    key = f"data/{project}/order-automation/verification/{path.name}"
    client.put_object(Bucket=bucket, Key=key, Body=payload, ExpectedBucketOwner=account,
                      ServerSideEncryption="AES256", IfNoneMatch="*", ContentType="application/json")
    if client.get_object(Bucket=bucket, Key=key, ExpectedBucketOwner=account)["Body"].read() != payload:
        raise ValueError("Private discovery report readback mismatch")


def require_pushed_source(root):
    subprocess.run(["git", "ls-files", "--error-unmatch", "scripts/verify_invoice_discovery.py", "generate_invoices.py"],
                   cwd=root, check=True, capture_output=True)
    if subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=no"], cwd=root, text=True).strip():
        raise ValueError("Discovery source must be committed before collecting execution evidence")
    branch = subprocess.check_output(["git", "branch", "--show-current"], cwd=root, text=True).strip()
    if not branch:
        raise ValueError("Discovery source must use a pushed review branch")
    upstream = subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"],
                                       cwd=root, text=True).strip()
    if upstream != f"origin/{branch}":
        raise ValueError("Discovery source must track its matching remote branch")
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    remote = subprocess.check_output(["git", "ls-remote", "--exit-code", "origin", f"refs/heads/{branch}"],
                                     cwd=root, text=True).strip().splitlines()
    if len(remote) != 1 or remote[0].split() != [head, f"refs/heads/{branch}"]:
        raise ValueError("Discovery source must match the pushed branch")
    return head


def attach_scan_progress(generator, project, emit):
    """Observe existing read calls without changing request/retry behavior."""
    original = generator.execute_read
    reads = 0

    def execute(query, variables):
        nonlocal reads
        result = original(query, variables)
        reads += 1
        if reads % 50 == 0:
            emit({"project": project, "scan_reads": reads, "completed_pages": generator.scan_pages})
        return result

    generator.execute_read = execute


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True, choices=("roy", "vevo"))
    parser.add_argument("--profile", default="codex")
    parser.add_argument("--expected-count", type=int)
    parser.add_argument("--publish-report", action="store_true")
    args = parser.parse_args(argv)
    if args.expected_count is not None and not 1 <= args.expected_count <= 1000:
        parser.error("--expected-count must be between 1 and 1000")
    logging.disable(logging.CRITICAL)
    root = Path(__file__).resolve().parents[1]
    commit = require_pushed_source(root)
    audit = load_private_evidence(root, root / "data" / args.project / "order-automation" / "backlog-audit.json", args.project)
    audit_candidates(audit, args.project, datetime.now(timezone.utc), args.expected_count)
    import boto3
    from generate_invoices import InvoiceGenerator, resolve_invoice_generation_settings
    from reporting_core import derive_biznisweb_base_url
    from reporting_core.storage import resolve_report_s3_location
    session = boto3.Session(profile_name=args.profile, region_name="eu-central-1")
    secret = json.loads(session.client("secretsmanager").get_secret_value(
        SecretId=f"{args.project}/reporting/runtime-env")["SecretString"])
    settings = json.loads((root / "projects" / args.project / "settings.json").read_text(encoding="utf-8"))
    with runtime_environment(args.project, secret, settings):
        configured = resolve_invoice_generation_settings(settings)
        url = secret["BIZNISWEB_API_URL"]
        generator = InvoiceGenerator(url, secret["BIZNISWEB_API_TOKEN"], derive_biznisweb_base_url(url),
            username=None, password=None, send_invoice_email=False,
            eligible_statuses=configured["eligible_statuses"], exclude_zero_total_orders=configured["exclude_zero_total_orders"],
            scan_max_pages=configured["scan_max_pages"], page_delay_seconds=configured["page_delay_seconds"],
            read_attempts=configured["read_attempts"])
        attach_scan_progress(generator, args.project, lambda progress: print(json.dumps(progress, sort_keys=True), flush=True))
        report = verify_discovery(generator=generator, audit=audit, project=args.project,
                                  expected_count=args.expected_count, source_commit=commit)
    path = write_private_report(root, report)
    if args.publish_report:
        bucket, _ = resolve_report_s3_location(args.project, settings, secret, required=True)
        publish_private_report(session.client("s3"), bucket=bucket, project=args.project,
                               account=session.client("sts").get_caller_identity()["Account"], path=path)
    summary = {key: report[key] for key in ("project", "ok", "complete", "expected_count", "scanned_orders",
               "scan_pages", "eligible_orders", "checked_count", "failed_orders", "outcome_counts")}
    summary.update(global_issue_count=len(report["global_issues"]), private_report_written=True,
                   private_report_published=args.publish_report)
    print(json.dumps(summary, sort_keys=True), flush=True)
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(json.dumps({"ok": False, "error_type": type(error).__name__,
                          "message": "Read-only discovery verification stopped; no business or journal state changed"}), flush=True)
        raise SystemExit(1) from None
