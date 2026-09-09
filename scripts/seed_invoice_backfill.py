#!/usr/bin/env python3
"""Queue a complete reviewed historical audit without emailing old invoices.

This only writes private automation state. The deployed invoice runner must
revalidate every order before it can create a document. Existing records and
uncertain operations are never overwritten by this migration.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from invoice_automation_state import S3AutomationStateStore, parse_utc, resolve_automation_state_location
from reporting_core.storage import resolve_report_s3_location


def seed_state(state, audit, project, now):
    if (audit.get("project") != project or audit.get("complete") is not True
            or not isinstance(audit.get("candidates"), list)):
        raise RuntimeError("A complete audit of the selected project is required")
    age = now - parse_utc(audit["observed_at"])
    if age < timedelta(0) or age > timedelta(hours=6):
        raise RuntimeError("Historical audit must be refreshed")
    if state.get("project") != project or state.get("schema_version") != 1 or not isinstance(state.get("orders"), dict):
        raise RuntimeError("Automation journal identity mismatch")
    if state.get("lease"):
        raise RuntimeError("A migration cannot alter a leased journal")
    result = deepcopy(state)
    added, existing = 0, 0
    seen = set()
    for row in audit["candidates"]:
        number = str(row.get("order_num") or "")
        if not number or number in seen or row.get("blocked") is not False or row.get("invoices") != []:
            raise RuntimeError("Invalid historical candidate")
        seen.add(number)
        if row.get("older_than_7_days") is not True:
            continue
        if number in result["orders"]:
            existing += 1
            continue
        result["orders"][number] = {"order_num": number, "phase": "pending", "email_policy": "hold",
                                   "source": "complete_historical_backlog_audit",
                                   "audit_observed_at": audit["observed_at"], "updated_at": now.isoformat()}
        added += 1
    return result, {"seeded_historical_orders": added, "existing_records_untouched": existing}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", choices=("roy", "vevo"), required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    branch = subprocess.check_output(["git", "branch", "--show-current"], cwd=root, text=True).strip()
    if not branch.startswith("codex/"):
        raise RuntimeError("Use the reviewed migration branch")
    import boto3
    session = boto3.Session(profile_name=args.profile, region_name="eu-central-1")
    secret = json.loads(session.client("secretsmanager").get_secret_value(
        SecretId=f"{args.project}/reporting/runtime-env")["SecretString"])
    settings = json.loads((root / "projects" / args.project / "settings.json").read_text(encoding="utf-8"))
    bucket, key = resolve_automation_state_location(args.project, settings, secret)
    report_bucket, _ = resolve_report_s3_location(args.project, settings, secret, required=True)
    if bucket != report_bucket or key != f"data/{args.project}/order-automation/state.json":
        raise RuntimeError("State destination differs from the reviewed deployment")
    store = S3AutomationStateStore(session.client("s3"), bucket, key, args.project)
    audit_path = root / "data" / args.project / "order-automation" / "backlog-audit.json"
    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    state, etag = store.read()
    proposed, summary = seed_state(state, audit, args.project, datetime.now(timezone.utc))
    if args.apply:
        subprocess.run(["git", "ls-files", "--error-unmatch", "scripts/seed_invoice_backfill.py",
                        "invoice_automation_state.py"], cwd=root, check=True, capture_output=True)
        subprocess.run(["git", "fetch", "origin", branch], cwd=root, check=True, capture_output=True)
        changed = subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=no"], cwd=root, text=True).strip()
        head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
        upstream = subprocess.check_output(["git", "rev-parse", "@{upstream}"], cwd=root, text=True).strip()
        if changed or head != upstream:
            raise RuntimeError("Migration code must be committed and pushed")
        store.put(proposed, etag)
        if store.read()[0] != proposed:
            raise RuntimeError("Migration state readback mismatch")
    print(json.dumps({"project": args.project, "applied": args.apply, **summary}), flush=True)


if __name__ == "__main__":
    main()
