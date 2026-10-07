#!/usr/bin/env python3
"""Finite live report entry: prove this host before a report-only publication."""
from __future__ import annotations

import argparse
from datetime import date
import json
import os
from pathlib import Path
import re
import sys
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import reporting_migration_host_gate as probe  # noqa: E402
from scripts.order_automation_host_gate import localhost_marker  # noqa: E402

MARKER = "VEVO_REPORT_IMAGE_LIVE_HOST_OK"


def report_arguments(to_date):
    date.fromisoformat(to_date)
    return ["daily_report_runner.py", "--project", "vevo", "--to-date", to_date,
            "--no-cache", "--clear-cache", "--skip-email", "--skip-invoices",
            "--skip-creditnote-storno-guard", "--output-tag", ""]


def main():
    parser = argparse.ArgumentParser()
    for name in ("release-id", "source-commit", "image-digest", "gate-sha256", "to-date"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    probe.require(re.fullmatch(r"[a-f0-9]{32}", args.release_id)
                  and re.fullmatch(r"[a-f0-9]{40}", args.source_commit), "image-host-source-invalid")
    argv = report_arguments(args.to_date)
    import requests
    uri = os.environ.get("ECS_CONTAINER_METADATA_URI_V4", "")
    probe.require(uri.startswith("http://169.254.170.2/v4/"), "image-host-metadata-endpoint")
    response = requests.get(uri + "/task", timeout=10)
    response.raise_for_status()
    identity = probe.host_identity(response.json(), release_id=args.release_id,
        source_commit=args.source_commit, image_digest=args.image_digest, gate_sha256=args.gate_sha256)
    identity.update(marker=MARKER, report_to_date=args.to_date, skip_email=True,
                    skip_invoices=True, skip_inline_guard=True)
    localhost_marker(identity)
    print(MARKER + " " + json.dumps(identity, sort_keys=True), flush=True)
    import daily_report_runner as runner
    flags = runner.parse_args(argv[1:])
    probe.require(flags.skip_email and flags.skip_invoices and flags.skip_creditnote_storno_guard
                  and not flags.skip_export and not flags.output_tag, "image-host-report-flags")
    with patch.object(sys, "argv", argv):
        runner.main()


if __name__ == "__main__":
    main()
