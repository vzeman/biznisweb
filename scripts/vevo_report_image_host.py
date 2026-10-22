#!/usr/bin/env python3
"""Finite live report entry: prove this host before a report-only publication."""
from __future__ import annotations

import argparse
from datetime import date
import json
import math
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
QA_LABELS = {
    "order_revenue_reconciliation": "Source order revenue reconciliation",
    "attribution": "Campaign Attribution QA", "geo": "Geo confidence",
    "data_assertions": "Data assertions", "product_expense_coverage": "Product cost coverage",
    "margin_stability": "Margin stability",
}
LIVE_IDENTITY_FIELDS = frozenset({"marker", "release_id", "source_commit", "image_digest", "task_arn",
    "task_definition", "private_ip", "instance_id", "service", "path", "project", "gate_sha256",
    "report_to_date", "skip_email", "skip_invoices", "skip_inline_guard"})
SOURCE_KEYS = frozenset({"biznisweb_orders", "creditnotes", "facebook_ads", "google_ads", "weather"})
STATUSES = frozenset({"ok", "pass", "warning", "error", "critical", "full", "partial", "unavailable", "disabled", "estimated"})
QA_NUMBERS = frozenset({"failure_count", "warning_count", "coverage_ratio", "oversubscription_ratio",
    "daily_source_spend", "campaign_source_spend", "total_orders", "campaign_rows", "platform_rows_checked",
    "platform_cost_mismatch_count", "day_name_missing", "attach_anchor_missing", "attach_attached_missing",
    "anchor_orders_missing", "country_missing", "geo_country_missing", "missing_label_total", "null_label_rate_pct",
    "platform_cpa_mismatches", "attributed_cpa_mismatches", "attributed_cpa_unavailable_rows", "attributed_orders_ratio",
    "total_item_rows", "fallback_rows", "raw_extreme_days", "smoothed_extreme_days",
    "min_smoothed_margin_pct", "max_smoothed_margin_pct", "total_spend", "account_total_spend",
    "reported_geo_total_spend", "unallocated_spend", "reported_unknown_spend", "country_coverage_ratio",
    "reconciliation_delta_eur", "outside_sk_cz_hu_eur"})
COUNTRY_ERRORS = frozenset({"country spend source unavailable", "country spend basis or EUR currency unverified",
    "country spend date range mismatch", "country spend coverage unavailable", "invalid country spend totals",
    "invalid country spend amount", "country spend does not reconcile to daily/account spend",
    "Google country coverage/account evidence invalid or unavailable"})


def _safe_check(value):
    """Keep aggregate predicates, never arbitrary messages, rows or source responses."""
    if not isinstance(value, dict):
        return {"invalid": True}
    result = {key: item for key, item in value.items() if key in QA_NUMBERS
              and type(item) in (int, float) and -1e15 <= item <= 1e15 and math.isfinite(item)}
    if isinstance(value.get("status"), str) and value["status"] in STATUSES:
        result["status"] = value["status"]
    if type(value.get("healthy")) is bool:
        result["healthy"] = value["healthy"]
    if isinstance(value.get("validation_error"), str) and value["validation_error"] in COUNTRY_ERRORS:
        result["validation_error"] = value["validation_error"]
    for key in ("failures", "warnings", "missing_financial_keys", "shell_parity_failures"):
        if isinstance(value.get(key), list):
            result[key + "_count"] = len(value[key])
    return result


def _safe_health(value):
    if not isinstance(value, dict):
        return {"invalid": True}
    result = {}
    for key in ("is_partial",):
        if type(value.get(key)) is bool:
            result[key] = value[key]
    for key in ("overall_status", "qa_status"):
        if isinstance(value.get(key), str) and value[key] in STATUSES:
            result[key] = value[key]
    for key in ("qa_failure_count", "qa_warning_count", "qa_check_count"):
        if type(value.get(key)) is int and 0 <= value[key] <= 1000000000:
            result[key] = value[key]
    errors = value.get("qa_errors")
    if isinstance(errors, list):
        result["qa_errors"] = [label for label in errors if isinstance(label, str) and label in QA_LABELS.values()]
        result["unrecognized_qa_error_count"] = len(errors) - len(result["qa_errors"])
    for group, allowed in (("qa", QA_LABELS), ("sources", SOURCE_KEYS)):
        entries = value.get(group)
        if isinstance(entries, dict):
            result[group] = {key: _safe_check(entries[key]) for key in allowed if key in entries}
            result["unrecognized_" + group + "_count"] = len(set(entries) - set(allowed))
    qa = value.get("qa")
    geo = qa.get("geo") if isinstance(qa, dict) else None
    attribution = geo.get("spend_attribution") if isinstance(geo, dict) else None
    if isinstance(attribution, dict):
        result["qa"]["geo"]["spend_attribution"] = {
            key: _safe_check(attribution[key]) for key in ("facebook_ads", "google_ads") if key in attribution}
    return result


def safe_publication_diagnostics(value, first, last):
    """Project only allowlisted all-period QA fields from the bound exception."""
    probe.require(isinstance(value, dict) and set(value) == {"schema_version", "project", "report_from_date", "report_to_date", "periods"}
                  and type(value["schema_version"]) is int and value["schema_version"] == 1 and value["project"] == "vevo"
                  and value["report_from_date"] == first and value["report_to_date"] == last
                  and isinstance(value["periods"], dict) and set(value["periods"]) == {"latest", "7d", "30d", "90d"},
                  "image-live-diagnostic-binding")
    result = {key: value[key] for key in ("schema_version", "project", "report_from_date", "report_to_date")}
    result["periods"] = {}
    invalid_fields = {"artifact", "project", "period_switcher.current_key", "date_range", "date_from", "source_health",
                      "source_health.is_partial", "source_health.qa_status", "source_health.qa_failure_count", "source_health.qa_errors"}
    for period, entry in value["periods"].items():
        probe.require(isinstance(entry, dict), "image-live-diagnostic-period")
        projected = {"source_health": _safe_health(entry.get("source_health"))}
        for key in ("date_from", "date_to"):
            if isinstance(entry.get(key), str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", entry[key]):
                date.fromisoformat(entry[key])
                projected[key] = entry[key]
        if isinstance(entry.get("sha256"), str) and re.fullmatch(r"[a-f0-9]{64}", entry["sha256"]):
            projected["sha256"] = entry["sha256"]
        if type(entry.get("size")) is int and 0 <= entry["size"] <= 64 * 1024 * 1024:
            projected["size"] = entry["size"]
        status = entry.get("read_status")
        projected["read_status"] = status if isinstance(status, str) and status in probe.DIAGNOSTIC_STATUSES else "invalid-json"
        fields = entry.get("invalid_fields")
        if isinstance(fields, list):
            projected["invalid_fields"] = [item for item in fields if isinstance(item, str) and item in invalid_fields]
            projected["unrecognized_invalid_field_count"] = len(fields) - len(projected["invalid_fields"])
        result["periods"][period] = projected
    return result


def retain_live_failure(s3, prefix, identity, first, last, exc):
    """A rejected run stays rejected even when evidence cannot be retained."""
    date.fromisoformat(first)
    date.fromisoformat(last)
    probe.require(set(identity) == LIVE_IDENTITY_FIELDS
                  and prefix == "daily-reports/vevo/source-inputs/" + identity["release_id"] + "/"
                  and identity.get("project") == "vevo" and identity.get("marker") == MARKER
                  and identity.get("report_to_date") == last
                  and all(identity.get(key) is True for key in ("skip_email", "skip_invoices", "skip_inline_guard")),
                  "image-live-diagnostic-identity")
    diagnostic = {"status": "missing"}
    content = None
    value = getattr(exc, "publication_diagnostics", None)
    if value is not None:
        try:
            raw = probe.canonical(value)
            if len(raw) > probe.DIAGNOSTIC_LIMIT:
                diagnostic = {"status": "too-large"}
            else:
                content = probe.canonical(safe_publication_diagnostics(value, first, last))
                if len(content) > probe.DIAGNOSTIC_LIMIT:
                    diagnostic, content = {"status": "too-large"}, None
        except (TypeError, ValueError, RuntimeError):
            diagnostic = {"status": "invalid-json"}
        if content is not None:
            key = prefix + "diagnostics/publication.json"
            put_live_source(s3, prefix, key, content)
            diagnostic = {"status": "available", "key": key, "sha256": probe.sha(content), "size": len(content),
                          "original_diagnostics_sha256": probe.sha(raw), "projection": "allowlisted-qa-v1"}
    marker = {**identity, "schema_version": 1, "phase": "live-report-rejected-before-publication",
              "localhost_marker_sha256": probe.sha(probe.canonical(identity)), "report_from_date": first,
              "report_to_date": last, "failure_type": type(exc).__name__ if type(exc).__name__ in probe.FAILURE_TYPES else "other",
              "publication_attempted": False, "diagnostic": diagnostic}
    put_live_source(s3, prefix, prefix + "diagnostics/failed.json", probe.canonical(marker))


def report_arguments(to_date, *, policy=VEVO):
    date.fromisoformat(to_date)
    return ["daily_report_runner.py", "--project", policy.project, "--to-date", to_date,
            "--no-cache", "--clear-cache", "--skip-email", "--skip-invoices",
            "--skip-creditnote-storno-guard", "--output-tag", ""]


def put_live_source(s3, prefix, key, raw):
    """Use the production role's existing private report sink without IAM changes."""
    probe.require(re.fullmatch(r"daily-reports/vevo/source-inputs/[a-f0-9]{32}/", prefix)
                  and ".." not in key and (key.startswith(prefix + "meta/") or key in {
                      prefix + "source-complete.json", prefix + "diagnostics/publication.json", prefix + "diagnostics/failed.json"}),
                  "image-live-source-scope")
    if key.startswith(prefix + "diagnostics/"):
        probe.require(isinstance(raw, bytes) and 0 < len(raw) <= probe.DIAGNOSTIC_LIMIT,
                      "image-live-diagnostic-size")
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
    publication_attempted = False

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
        nonlocal published, publication_attempted
        probe.require(project == "vevo" and not published, "image-live-source-publication")
        publication_attempted = True
        manifest = snapshot.finalize()
        marker = {"schema_version": 1, "phase": "live-native-inputs-complete", "capture_mode": "live",
                  "host_identity": identity, "host_identity_sha256": probe.sha(probe.canonical(identity)),
                  "meta_source_manifest": manifest, "publication_verified": False}
        put_live_source(s3, prefix, prefix + "source-complete.json", probe.canonical(marker))
        result = upload(project, paths)
        published = True
        return result

    try:
        with patch.object(requests.Session, "send", observe), patch.object(runner, "run_export", export_in_process), \
             patch.object(runner, "s3_upload_outputs", publish), patch.object(sys, "argv", argv):
            result = runner.main()
        probe.require(published, "image-live-source-not-published")
    except BaseException as exc:
        if not publication_attempted:
            try:
                retain_live_failure(s3, prefix, identity, first, last, exc)
            except Exception:
                print("REPORT_LIVE_FAILURE_DIAGNOSTICS_UNAVAILABLE", flush=True)
        raise
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
