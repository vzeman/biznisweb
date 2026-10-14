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
from scripts.reporting_image_release_policy import PROJECTS, VEVO, project_policy
from scripts.order_automation_host_gate import localhost_marker  # noqa: E402

MARKER = "VEVO_REPORT_IMAGE_LIVE_HOST_OK"


def report_arguments(to_date, *, policy=VEVO):
    date.fromisoformat(to_date)
    return ["daily_report_runner.py", "--project", policy.project, "--to-date", to_date,
            "--no-cache", "--clear-cache", "--skip-email", "--skip-invoices",
            "--skip-creditnote-storno-guard", "--output-tag", ""]


def put_live_source(s3, prefix, key, raw):
    """Use the production role's existing private report sink without IAM changes."""
    probe.require(re.fullmatch(r"daily-reports/vevo/source-inputs/[a-f0-9]{32}/", prefix)
                  and ".." not in key and (key.startswith(prefix + "meta/") or key == prefix + "source-complete.json"),
                  "image-live-source-scope")
    s3.put_object(Bucket=probe.BUCKET, Key=key, Body=raw, ExpectedBucketOwner=probe.ACCOUNT,
                  ServerSideEncryption="AES256", IfNoneMatch="*", ContentType="application/json")
    probe.require(probe.read_private(s3, key, len(raw) + 1) == raw, "image-live-source-readback")


def run_live_report(runner, identity, argv, *, policy=VEVO):
    """Retain actual live Meta inputs before publication; keep other projects' CLI."""
    if policy != VEVO:
        with patch.object(sys, "argv", argv):
            return runner.main()
    from scripts.reporting_meta_source_snapshot import MetaSourceSnapshot
    import boto3
    import export_orders
    import requests
    flags = runner.parse_args(argv[1:])
    first = runner.normalize_date(flags.from_date)
    last = runner.normalize_date(runner.resolve_to_date(flags.to_date, flags.timezone))
    probe.require(flags.project == "vevo" and flags.no_cache and flags.clear_cache and flags.skip_email
                  and flags.skip_invoices and flags.skip_creditnote_storno_guard and not flags.skip_export
                  and not flags.output_tag and last == identity["report_to_date"], "image-live-source-flags")
    prefix = policy.sink + "/source-inputs/" + identity["release_id"] + "/"
    s3 = boto3.client("s3", region_name=probe.REGION)
    snapshot = MetaSourceSnapshot(identity, probe.sha(probe.canonical(identity)), first, last, prefix,
        lambda key, raw: put_live_source(s3, prefix, key, raw), capture_mode="live",
        credentials=tuple(os.environ.get(key, "") for key in ("FACEBOOK_ACCESS_TOKEN", "FACEBOOK_APP_SECRET")))
    send, upload = requests.Session.send, runner.s3_upload_outputs
    published = False

    def observe(session, request, **kwargs):
        try:
            response = send(session, request, **kwargs)
        except BaseException:
            snapshot.observe_failure(request)
            raise
        snapshot.observe(request, response)
        return response

    def export_in_process(**kwargs):
        probe.require(kwargs == {"project": "vevo", "from_date": first, "to_date": last,
                                 "clear_cache": True, "no_cache": True, "output_tag": ""},
                      "image-live-export-context")
        # The same exporter CLI used by the probe and ordinary subprocess entry.
        command = ["export_orders.py", "--project", "vevo", "--from-date", first, "--to-date", last,
                   "--clear-cache", "--no-cache"]
        with patch.object(sys, "argv", command):
            export_orders.main()

    def publish(project, paths):
        nonlocal published
        probe.require(project == "vevo" and not published, "image-live-source-publication")
        manifest = snapshot.finalize()
        marker = {"schema_version": 1, "phase": "live-native-inputs-complete", "capture_mode": "live",
                  "host_identity": identity, "host_identity_sha256": probe.sha(probe.canonical(identity)),
                  "meta_source_manifest": manifest, "publication_verified": False}
        put_live_source(s3, prefix, prefix + "source-complete.json", probe.canonical(marker))
        result = upload(project, paths)
        published = True
        return result

    with patch.object(requests.Session, "send", observe), patch.object(runner, "run_export", export_in_process), \
         patch.object(runner, "s3_upload_outputs", publish), patch.object(sys, "argv", argv):
        result = runner.main()
    probe.require(published, "image-live-source-not-published")
    return result


def main():
    parser = argparse.ArgumentParser()
    for name in ("release-id", "source-commit", "image-digest", "gate-sha256", "to-date"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--project", choices=tuple(PROJECTS), default="vevo")
    args = parser.parse_args()
    policy = project_policy(args.project)
    probe.require(re.fullmatch(r"[a-f0-9]{32}", args.release_id)
                  and re.fullmatch(r"[a-f0-9]{40}", args.source_commit), "image-host-source-invalid")
    argv = report_arguments(args.to_date, policy=policy)
    import requests
    uri = os.environ.get("ECS_CONTAINER_METADATA_URI_V4", "")
    probe.require(uri.startswith("http://169.254.170.2/v4/"), "image-host-metadata-endpoint")
    response = requests.get(uri + "/task", timeout=10)
    response.raise_for_status()
    identity = probe.host_identity(response.json(), release_id=args.release_id,
        source_commit=args.source_commit, image_digest=args.image_digest, gate_sha256=args.gate_sha256, policy=policy)
    identity.update(marker=policy.live_marker, report_to_date=args.to_date, skip_email=True,
                    skip_invoices=True, skip_inline_guard=True)
    localhost_marker(identity)
    print(policy.live_marker + " " + json.dumps(identity, sort_keys=True), flush=True)
    import daily_report_runner as runner
    flags = runner.parse_args(argv[1:])
    probe.require(flags.skip_email and flags.skip_invoices and flags.skip_creditnote_storno_guard
                  and not flags.skip_export and not flags.output_tag, "image-host-report-flags")
    run_live_report(runner, identity, argv, policy=policy)


if __name__ == "__main__":
    main()
