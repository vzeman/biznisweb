#!/usr/bin/env python3
"""Finite, read-only application check on the exact candidate Fargate host."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

MARKER = "ORDER_AUTOMATION_HOST_OK"


def verify_summary(summary: dict, kind: str) -> None:
    if summary.get("enabled") is not True or summary.get("dry_run") is not True:
        raise RuntimeError("host-gate-mode-mismatch")
    failure_fields = (
        ("failed_invoices", "failed_invoice_emails", "failed_invoice_status_reconciliations",
         "missing_invoice_ids", "ambiguous_invoice_operations")
        if kind == "invoice"
        else ("failed_orders", "recovery_failed_orders")
    )
    if any(summary.get(field, 0) != 0 for field in failure_fields):
        raise RuntimeError("host-gate-application-failure")
    if summary.get("skipped_locked") or summary.get("scan_limit_reached"):
        raise RuntimeError("host-gate-incomplete-run")
    # The pre-upgrade pin probe uses the old runner, which has no scan field.
    # A new runner that exposes this evidence must explicitly report completion.
    if "invoice_scan_complete" in summary and summary["invoice_scan_complete"] is not True:
        raise RuntimeError("host-gate-incomplete-scan")


def localhost_marker(payload: dict) -> None:
    body = json.dumps(payload, sort_keys=True).encode("utf-8")

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/marker.json":
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 8000), Handler)
    worker = threading.Thread(target=server.serve_forever)
    worker.start()
    try:
        result = subprocess.run(
            ["curl", "--fail", "--silent", "--show-error", "--max-time", "10",
             "http://127.0.0.1:8000/marker.json"],
            capture_output=True, check=True, timeout=15,
        )
        if json.loads(result.stdout) != payload:
            raise RuntimeError("host-gate-localhost-mismatch")
    finally:
        server.shutdown()
        worker.join(timeout=10)
        server.server_close()
    if worker.is_alive():
        raise RuntimeError("host-gate-thread-not-stopped")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", choices=("roy", "vevo"), required=True)
    parser.add_argument("--kind", choices=("invoice", "cancellation"), required=True)
    parser.add_argument("--full-backlog", action="store_true")
    args = parser.parse_args()
    if args.full_backlog and args.kind != "invoice":
        raise RuntimeError("host-gate-full-backlog-kind-mismatch")
    if args.kind == "cancellation" and args.project != "roy":
        raise RuntimeError("host-gate-service-not-allowed")
    if os.getcwd() != "/app":
        raise RuntimeError("host-gate-path-mismatch")
    if args.kind == "invoice":
        from invoice_runner import parse_args, run_invoice_runner
        runner_args = ["--project", args.project, "--dry-run"]
        if args.full_backlog:
            runner_args.append("--full-backlog")
        summary = run_invoice_runner(parse_args(runner_args))
    else:
        from unpaid_order_cancellation_runner import parse_args, run_unpaid_cancellation_runner
        summary = run_unpaid_cancellation_runner(parse_args(["--project", args.project, "--dry-run"]))
    verify_summary(summary, args.kind)
    if args.full_backlog and summary.get("invoice_scan_all_ages") is not True:
        raise RuntimeError("host-gate-full-backlog-not-verified")
    payload = {"marker": MARKER, "project": args.project, "kind": args.kind,
               "path": "/app", "dry_run": True}
    if args.full_backlog:
        payload["full_backlog"] = True
    localhost_marker(payload)
    print(MARKER + " " + json.dumps(payload, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
