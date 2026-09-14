#!/usr/bin/env python3
"""Run the standalone guard read-only and prove localhost on its exact host."""
import argparse
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.order_automation_host_gate import localhost_marker

MARKER = "CREDITNOTE_AUTOMATION_HOST_OK"


def verify_summary(summary, project, *, dry_run):
    if (summary.get("ok") is not True or summary.get("project") != project or summary.get("enabled") is not True
            or summary.get("dry_run") is not dry_run or summary.get("creditnote_scan_complete") is not True
            or summary.get("skipped_locked") is not False):
        raise RuntimeError("creditnote-run-incomplete")
    for field in ("failed_orders", "review_required_orders", "audit_error_orders"):
        if type(summary.get(field)) is not int or summary[field] != 0:
            raise RuntimeError("creditnote-run-needs-review")
    if dry_run and summary.get("updated_orders") != 0:
        raise RuntimeError("creditnote-dry-run-mutated")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", choices=("roy", "vevo"), required=True)
    args = parser.parse_args()
    if os.getcwd() != "/app":
        raise RuntimeError("creditnote-host-path-mismatch")
    from creditnote_storno_runner import parse_args, run_creditnote_runner
    summary = run_creditnote_runner(parse_args(["--project", args.project, "--dry-run"]))
    verify_summary(summary, args.project, dry_run=True)
    payload = {"marker": MARKER, "project": args.project, "path": "/app", "dry_run": True}
    localhost_marker(payload)
    print(MARKER + " " + json.dumps(payload, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
