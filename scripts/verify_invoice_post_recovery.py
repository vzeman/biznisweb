#!/usr/bin/env python3
"""Read all-age invoice backlog after recovery; optionally publish private evidence.

No baseline audit, web login, journal access or business writes. This is a
sequential inventory/readback, not an atomic snapshot or deployment proof.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import logging
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from generate_invoices import InvoiceGenerator, resolve_invoice_generation_settings
from order_inventory import internal_order_id
from reporting_core import derive_biznisweb_base_url
from reporting_core.storage import resolve_report_s3_location
from scripts.restore_verified_fulfillment import runtime_environment
from scripts.verify_invoice_discovery import (
    attach_scan_progress, require_pushed_source, write_private_report,
)

ACCOUNT = "919341186960"
REGION = "eu-central-1"
BUCKET = "biznisweb-reporting-artifacts-919341186960-eu-central-1"


def validate_order(order, *, allow_null_status=False):
    InvoiceGenerator._validate_order_for_invoice_read(order, allow_null_status=allow_null_status)
    internal_order_id(order)
    if isinstance(order["sum"]["value"], bool):
        raise ValueError("Invalid boolean order amount")
    for invoice in order["invoices"]:
        InvoiceGenerator._positive_internal_id(invoice["id"])


def verify_post_recovery(*, generator, project, source_commit="", now=None):
    if project not in {"roy", "vevo"}:
        raise ValueError("Unsupported verification project")
    now = now or datetime.now(timezone.utc)
    report = dict(schema_version=1, project=project, source_commit=source_commit,
                  check="post_recovery_all_age_snapshot", read_only_business_check=True,
                  started_at=now.isoformat(), complete=False, ok=False, scanned_orders=0,
                  eligible_orders=0, checked_count=0, failed_orders=0,
                  candidate_rechecks_complete=False,
                  global_issues=[], orders=[], outcome_counts={})
    try:
        if generator.web_session is not None or generator.operation_journal is not None:
            raise ValueError("Verification must have no web session or journal")
        scanned = generator.fetch_all_eligible_orders()
        if (not isinstance(scanned, list) or not generator.eligible_status_ids
                or any(isinstance(value, bool) or not isinstance(value, int) or value <= 0
                       for value in generator.eligible_status_ids)):
            raise ValueError("Full discovery or eligible status identity is incomplete")
        for order in scanned:
            validate_order(order, allow_null_status=True)
        numbers = [str(row["order_num"]) for row in scanned]
        ids = [internal_order_id(row) for row in scanned]
        if len(set(numbers)) != len(numbers) or len(set(ids)) != len(ids):
            raise ValueError("Full discovery repeated an order identity")
        candidates, stats = generator.filter_orders_for_invoice(scanned)
        pairs = {(str(row["order_num"]), internal_order_id(row)) for row in candidates}
        if len(pairs) != len(candidates) or not pairs <= set(zip(numbers, ids)):
            raise ValueError("Invoice filter returned an order outside the inventory")
        report.update(complete=True, scanned_orders=len(scanned), eligible_orders=len(candidates),
                      filter_counts=stats, eligible_status_ids=sorted(generator.eligible_status_ids),
                      unknown_status_orders=sum(row["status"] is None for row in scanned))
        report["orders"] = [dict(order_num=str(row["order_num"]), pur_date=row.get("pur_date"),
                                 outcome="not_checked") for row in candidates]
        for candidate, row in zip(candidates, report["orders"]):
            row["outcome"] = "unverified"
            report["checked_count"] += 1
            try:
                fresh = generator.fetch_order_for_invoice(row["order_num"])
                validate_order(fresh)
                if (str(fresh["order_num"]) != row["order_num"]
                        or internal_order_id(fresh) != internal_order_id(candidate)):
                    raise ValueError("Fresh order identity differs")
                remaining, _ = generator.filter_orders_for_invoice([fresh])
                row["invoice_ids"] = [str(invoice["id"]) for invoice in fresh["invoices"]]
                if len(row["invoice_ids"]) > 1:
                    row["outcome"] = "multiple_final_invoices"
                elif fresh["invoices"] and (not isinstance(fresh["invoices"][0].get("invoice_num"), str)
                                             or not fresh["invoices"][0]["invoice_num"].strip()):
                    row["outcome"] = "final_invoice_number_missing"
                else:
                    row["outcome"] = ("still_eligible" if remaining else
                                      "already_invoiced" if row["invoice_ids"] else "no_longer_eligible")
            except Exception as error:
                row["error_type"] = type(error).__name__
                report["global_issues"].append("fresh_candidate_read_failed")
                break  # Never spend another read budget after an exhausted/unknown read.
    except Exception as error:
        report["global_issues"].append("full_verification_failed")
        report["error_type"] = type(error).__name__
    finally:
        try:
            generator.client.transport.close()
        except Exception as error:
            report["global_issues"].append("transport_close_failed")
            report["close_error_type"] = type(error).__name__
        report["scan_pages"] = generator.scan_pages
        report["completed_at"] = datetime.now(timezone.utc).isoformat()
    report["outcome_counts"] = dict(Counter(row["outcome"] for row in report["orders"]))
    report["candidate_rechecks_complete"] = report["complete"] and not any(
        row["outcome"] in {"unverified", "not_checked"} for row in report["orders"])
    report["failed_orders"] = sum(row["outcome"] not in {"already_invoiced", "no_longer_eligible"}
                                  for row in report["orders"])
    report["ok"] = report["complete"] and not report["global_issues"] and not report["failed_orders"]
    return report


def verify_bucket(client):
    client.head_bucket(Bucket=BUCKET, ExpectedBucketOwner=ACCOUNT)
    if client.get_bucket_location(Bucket=BUCKET, ExpectedBucketOwner=ACCOUNT)["LocationConstraint"] != REGION:
        raise ValueError("Unexpected evidence bucket region")
    block = client.get_public_access_block(Bucket=BUCKET, ExpectedBucketOwner=ACCOUNT)["PublicAccessBlockConfiguration"]
    if not all(block.get(key) is True for key in (
        "BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets"
    )):
        raise ValueError("Evidence bucket privacy is unverified")


def verify_project_url(project, url):
    parsed = urlparse(url)
    if (parsed.scheme != "https" or parsed.hostname not in {f"{project}.flox.sk", f"{project}.sk", f"www.{project}.sk"}
            or parsed.path != "/api/graphql" or parsed.username or parsed.password
            or parsed.query or parsed.fragment or parsed.port not in (None, 443)):
        raise ValueError("Runtime API destination differs from the selected shop")


def publish_report(client, *, project, path):
    if project not in {"roy", "vevo"} or not path.name.startswith("invoice-discovery-") or path.suffix != ".json":
        raise ValueError("Unexpected verification artifact")
    verify_bucket(client)
    payload = path.read_bytes()
    key = f"data/{project}/order-automation/verification/{path.name}"
    client.put_object(Bucket=BUCKET, Key=key, Body=payload, ExpectedBucketOwner=ACCOUNT,
                      ServerSideEncryption="AES256", IfNoneMatch="*", ContentType="application/json")
    response = client.get_object(Bucket=BUCKET, Key=key, ExpectedBucketOwner=ACCOUNT)
    try:
        if response.get("ServerSideEncryption") != "AES256" or response["Body"].read() != payload:
            raise ValueError("Private evidence encryption or readback differs")
    finally:
        response["Body"].close()
    return key


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True, choices=("roy", "vevo"))
    parser.add_argument("--profile", default="codex")
    parser.add_argument("--publish-report", action="store_true")
    args = parser.parse_args(argv)
    logging.disable(logging.CRITICAL)
    root = Path(__file__).resolve().parents[1]
    subprocess.run(["git", "ls-files", "--error-unmatch", "scripts/verify_invoice_post_recovery.py"],
                   cwd=root, check=True, capture_output=True)
    commit = require_pushed_source(root)
    import boto3
    from botocore.config import Config
    session = boto3.Session(profile_name=args.profile, region_name=REGION)
    config = Config(connect_timeout=10, read_timeout=30, retries={"total_max_attempts": 1})
    with closing(session.client("sts", config=config)) as sts:
        if sts.get_caller_identity()["Account"] != ACCOUNT:
            raise ValueError("Unexpected AWS account")
    with closing(session.client("s3", config=config)) as s3, closing(session.client("secretsmanager", config=config)) as secrets:
        verify_bucket(s3)
        settings = json.loads((root / "projects" / args.project / "settings.json").read_text(encoding="utf-8"))
        secret = json.loads(secrets.get_secret_value(SecretId=f"{args.project}/reporting/runtime-env")["SecretString"])
        if resolve_report_s3_location(args.project, settings, secret, required=True)[0] != BUCKET:
            raise ValueError("Project evidence storage differs")
        verify_project_url(args.project, secret["BIZNISWEB_API_URL"])
        with runtime_environment(args.project, secret, settings):
            configured = resolve_invoice_generation_settings(settings)
            if not configured["enabled"] or not configured["all_age_backlog_enabled"]:
                raise ValueError("Reviewed invoice discovery configuration differs")
            url = secret["BIZNISWEB_API_URL"]
            generator = InvoiceGenerator(
                url, secret["BIZNISWEB_API_TOKEN"], derive_biznisweb_base_url(url),
                username=None, password=None, send_invoice_email=False, project=args.project,
                **{key: configured[key] for key in (
                    "eligible_statuses", "exclude_zero_total_orders", "scan_max_pages",
                    "page_delay_seconds", "read_attempts")})
            attach_scan_progress(generator, args.project, lambda item: print(json.dumps(item), flush=True))
            report = verify_post_recovery(generator=generator, project=args.project, source_commit=commit)
        path = write_private_report(root, report)
        key = publish_report(s3, project=args.project, path=path) if args.publish_report else None
    summary = {name: report[name] for name in (
        "project", "complete", "ok", "scanned_orders", "scan_pages", "eligible_orders",
        "checked_count", "candidate_rechecks_complete", "failed_orders", "outcome_counts")}
    summary.update(global_issue_count=len(report["global_issues"]),
                   sha256=hashlib.sha256(path.read_bytes()).hexdigest(), private_evidence_key=key)
    print(json.dumps(summary, sort_keys=True), flush=True)
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(json.dumps({"ok": False, "error_type": type(error).__name__,
                          "message": "Read-only post-recovery verification stopped"}), flush=True)
        raise SystemExit(1) from None
