#!/usr/bin/env python3
"""Finite VEVO report probe: localhost identity, release handshake, private outputs.

Production input locations are preserved. Only the upload sink is replaced.
The ECS diagnostic role independently denies live output, journal, SES and metric
writes; application guards are an additional boundary, not an IAM substitute.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack, closing
from datetime import datetime, timezone
import hashlib
import json
import mimetypes
import os
from pathlib import Path
import re
import sys
import time
from unittest.mock import patch
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
ACCOUNT = "919341186960"
REGION = "eu-central-1"
BUCKET = f"biznisweb-reporting-artifacts-{ACCOUNT}-{REGION}"
PREFIX = "data/vevo/reporting/runtime/probes/"
SERVICE = "vevo-daily-report-email"
MARKER = "VEVO_REPORT_PROBE_HOST_OK"


def require(value, code):
    if not value:
        raise RuntimeError(code)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def put_private(s3, key, raw):
    require(key.startswith(PREFIX) and ".." not in key, "probe-output-scope-invalid")
    s3.put_object(Bucket=BUCKET, Key=key, Body=raw, ExpectedBucketOwner=ACCOUNT,
                  ServerSideEncryption="AES256", IfNoneMatch="*", ContentType=mimetypes.guess_type(key)[0] or "application/octet-stream")
    require(read_private(s3, key, len(raw) + 1) == raw, "probe-output-readback-mismatch")


def read_private(s3, key, limit=256 * 1024):
    result = s3.get_object(Bucket=BUCKET, Key=key, ExpectedBucketOwner=ACCOUNT)
    with closing(result["Body"]) as body:
        require(result.get("ServerSideEncryption") == "AES256", "probe-encryption-invalid")
        raw = body.read(limit + 1)
    require(len(raw) <= limit, "probe-object-too-large")
    return raw


def host_identity(metadata, *, release_id, source_commit, image_digest, gate_sha256):
    require(os.getcwd() == "/app", "probe-path-invalid")
    require(sha(Path(__file__).read_bytes()) == gate_sha256, "probe-source-file-mismatch")
    require(metadata.get("Family") == "vevo-reporting-daily" and metadata.get("LaunchType") == "FARGATE",
            "probe-host-family-invalid")
    containers = [row for row in metadata.get("Containers", []) if row.get("Name") == "reporting"]
    require(len(containers) == 1, "probe-host-container-invalid")
    container = containers[0]
    addresses = [ip for network in container.get("Networks", []) for ip in network.get("IPv4Addresses", [])]
    require(len(addresses) == 1 and container.get("ImageID") == image_digest, "probe-host-image-or-ip-invalid")
    task = metadata.get("TaskARN", "")
    require(task.startswith(f"arn:aws:ecs:{REGION}:{ACCOUNT}:task/vevo-reporting-cluster/"), "probe-host-task-invalid")
    return {"marker": MARKER, "release_id": release_id, "source_commit": source_commit,
            "task_arn": task, "private_ip": addresses[0], "image_digest": image_digest,
            "task_definition": f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/vevo-reporting-daily:{metadata['Revision']}",
            "instance_id": "N/A:Fargate", "service": SERVICE, "path": "/app", "project": "vevo",
            "gate_sha256": gate_sha256}


def await_authorization(s3, prefix, identity, *, clock=time.monotonic, sleep=time.sleep):
    from scripts.order_automation_host_gate import localhost_marker
    localhost_marker(identity)  # curl on this host; its server is closed before any provider call.
    ready = canonical(identity)
    put_private(s3, prefix + "markers/ready.json", ready)
    expected = {"phase": "host-authorized", "release_id": identity["release_id"],
                "task_arn": identity["task_arn"], "ready_sha256": sha(ready)}
    deadline = clock() + 600
    while clock() < deadline:
        try:
            raw = read_private(s3, prefix + "authorize.json")
        except Exception as exc:
            require(getattr(exc, "response", {}).get("Error", {}).get("Code") in {"NoSuchKey", "404"},
                    "probe-authorization-read-failed")
            sleep(5)
            continue
        require(raw == canonical(expected), "probe-authorization-binding-invalid")
        return sha(ready)
    raise RuntimeError("probe-authorization-timeout")


def verify_quality(value):
    require(isinstance(value, dict) and value.get("is_partial") is False,
            "probe-report-incomplete")
    require(value.get("qa_status") in {"ok", "pass", "warning"}
            and type(value.get("qa_failure_count")) is int and value["qa_failure_count"] == 0
            and value.get("qa_errors") == [], "probe-report-qa-failed")


def isolate_outputs(s3, prefix, runner, paths):
    """No call to the ordinary uploader, aliases, presigning or live generation."""
    artifacts = runner._canonical_live_artifact_paths("vevo", paths)
    expected = set(runner.STABLE_LIVE_ARTIFACT_NAMES)
    for names in runner.PERIOD_LIVE_ARTIFACT_NAMES.values():
        expected.update(names.values())
    require(set(artifacts) == expected and bool(expected), "probe-required-period-artifacts-missing")
    verify_quality(runner.load_data_quality(paths.get("data_quality_json")))
    artifacts["data_quality.json"] = paths["data_quality_json"]
    entries = {}
    for name, path in sorted(artifacts.items()):
        require(Path(name).name == name and path.is_file() and path.resolve().is_relative_to(ROOT / "data" / "vevo"),
                "probe-artifact-path-invalid")
        raw = path.read_bytes()
        require(0 < len(raw) <= 64 * 1024 * 1024, "probe-artifact-empty-or-too-large")
        if name.endswith("data_quality.json"):
            verify_quality(json.loads(raw))
        if name.endswith(".html"):
            require(b"<html" in raw.lower(), "probe-html-invalid")
        if name.startswith("dashboard_payload_"):
            payload = json.loads(raw)
            require(payload.get("project") == "vevo", "probe-payload-project-invalid")
            verify_quality(payload.get("source_health"))
        key = prefix + "artifacts/" + name
        put_private(s3, key, raw)
        entries[name] = {"key": key, "sha256": sha(raw), "size": len(raw)}
    manifest = {"schema_version": 1, "project": "vevo", "artifacts": entries,
                "generated_at": datetime.now(timezone.utc).isoformat()}
    raw = canonical(manifest)
    key = prefix + "artifacts/output-manifest.json"
    put_private(s3, key, raw)
    return {"key": key, "sha256": sha(raw)}


def report_probe(s3, prefix, release_id):
    import daily_report_runner as runner
    import export_orders
    import requests
    from gql import Client
    from graphql import OperationDefinitionNode, OperationType
    from reporting_core import metrics

    required = {"REPORT_PROJECT": "vevo", "REPORT_SKIP_INVOICES": "true",
                "REPORT_SKIP_CREDITNOTE_STORNO_GUARD": "true", "REPORT_SKIP_EMAIL": "true"}
    require(all(os.environ.get(k) == v for k, v in required.items()), "probe-skip-environment-invalid")
    original_input = (os.environ.get("REPORT_S3_BUCKET"), os.environ.get("REPORT_S3_PREFIX"))
    require(original_input[0] == BUCKET and original_input[1] and not original_input[1].startswith(PREFIX),
            "probe-production-input-location-invalid")
    result = {}
    attempts = {"provider_writes": 0, "email": 0, "live_output": 0}
    incomplete_reads = []
    pending_reads = set()
    inventory = {}

    def forbidden(kind):
        def reject(*_args, **_kwargs):
            attempts[kind] += 1
            raise RuntimeError("probe-forbidden-" + kind)
        return reject

    execute = Client.execute
    def query_only(client, query, *args, **kwargs):
        doc = getattr(query, "document", query)
        definitions = getattr(doc, "definitions", ())
        operations = [d for d in definitions if isinstance(d, OperationDefinitionNode)]
        if not operations or any(d.operation != OperationType.QUERY for d in operations):
            return forbidden("provider_writes")()
        roots = tuple(sorted(selection.name.value for operation in operations
                             for selection in operation.selection_set.selections if hasattr(selection, "name")))
        variables = kwargs.get("variable_values", args[0] if args else None) or {}
        read_key = (id(client), roots, sha(canonical(variables)))
        try:
            value = execute(client, query, *args, **kwargs)
        except Exception:
            # A successful retry or supported reduced-field read of this exact
            # page resolves its error. Other successful pages cannot hide it.
            pending_reads.add(read_key)
            raise
        pending_reads.discard(read_key)
        if isinstance(value, dict) and "getOrderList" in value:
            page = value["getOrderList"]
            rows, info = page.get("data"), page.get("pageInfo", {})
            valid = isinstance(rows, list) and type(info.get("hasNextPage")) is bool
            if valid and info["hasNextPage"]:
                valid = bool(rows) and bool(info.get("nextCursor"))
            params = (kwargs.get("variable_values") or {}).get("params", {})
            key = (id(client), params.get("sort"), params.get("order_by"))
            if not params.get("cursor"):
                inventory[key] = set()
            seen = inventory.setdefault(key, set())
            ids = [str(row.get("id") or "") for row in rows] if isinstance(rows, list) else []
            valid = valid and all(ids) and len(ids) == len(set(ids)) and not seen.intersection(ids)
            if not valid:
                incomplete_reads.append("inventory-page-invalid")
                raise RuntimeError("probe-inventory-page-invalid")
            seen.update(ids)
        return value

    send = requests.Session.send
    def read_native(session, request, **kwargs):
        parsed = urlparse(request.url)
        if parsed.hostname in {"vevo.sk", "www.vevo.sk", "vevo.flox.sk"} and parsed.path != "/api/graphql":
            allowed = {("GET", "/erp/main/login"), ("POST", "/admin/login/authenticate/"),
                       ("POST", "/erp/orders/creditnotes/getListJson"), ("GET", "/erp/main/")}
            if (request.method, parsed.path) not in allowed:
                return forbidden("provider_writes")()
        if parsed.hostname in {"roy.sk", "www.roy.sk", "roy.flox.sk"}:
            return forbidden("provider_writes")()
        return send(session, request, **kwargs)

    def export_in_process(**kwargs):
        require(kwargs["project"] == "vevo" and kwargs["output_tag"] == "migration_" + release_id,
                "probe-export-context-invalid")
        argv = ["export_orders.py", "--project", "vevo", "--from-date", kwargs["from_date"],
                "--to-date", kwargs["to_date"], "--output-tag", kwargs["output_tag"]]
        if kwargs["no_cache"]:
            argv.append("--no-cache")
        if kwargs["clear_cache"]:
            argv.append("--clear-cache")
        with patch.object(sys, "argv", argv):
            export_orders.main()  # Same production CLI, guarded in this process.

    def publish(project, paths):
        require(project == "vevo", "probe-output-project-invalid")
        require((os.environ.get("REPORT_S3_BUCKET"), os.environ.get("REPORT_S3_PREFIX")) == original_input,
                "probe-input-location-changed")
        require(not incomplete_reads and not pending_reads, "probe-provider-reads-incomplete")
        result.update(isolate_outputs(s3, prefix, runner, paths))
        return {}

    argv = ["daily_report_runner.py", "--project", "vevo", "--skip-email", "--skip-invoices",
            "--skip-creditnote-storno-guard", "--output-tag", "migration_" + release_id]
    flags = runner.parse_args(argv[1:])
    require(flags.skip_email and flags.skip_invoices and flags.skip_creditnote_storno_guard and not flags.skip_export,
            "probe-runner-skips-invalid")
    warning = export_orders.logger.warning
    def guard_inventory_limit(message, *args, **kwargs):
        if str(message).startswith("Stopped after max_batches="):
            incomplete_reads.append("inventory-safety-cap")
        return warning(message, *args, **kwargs)
    with ExitStack() as stack:
        for target, name, replacement in (
            (runner, "maybe_run_invoice_automation", forbidden("provider_writes")),
            (runner, "maybe_run_creditnote_storno_guard", forbidden("provider_writes")),
            (runner, "send_email_ses", forbidden("email")),
            (runner, "put_metric", lambda *_a, **_k: None),
            (metrics, "put_metric", lambda *_a, **_k: None),
            (runner, "run_export", export_in_process), (runner, "s3_upload_outputs", publish),
            (Client, "execute", query_only), (requests.Session, "send", read_native),
            (export_orders.logger, "warning", guard_inventory_limit),
        ):
            stack.enter_context(patch.object(target, name, replacement))
        stack.enter_context(patch.object(sys, "argv", argv))
        runner.main()
    require(result and not any(attempts.values()) and not incomplete_reads and not pending_reads, "probe-report-not-verified")
    return result


def main():
    parser = argparse.ArgumentParser()
    for name in ("release-id", "source-commit", "image-digest", "gate-sha256"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    require(re.fullmatch(r"[a-f0-9]{32}", args.release_id), "probe-release-invalid")
    require(re.fullmatch(r"[a-f0-9]{40}", args.source_commit), "probe-source-invalid")
    require(re.fullmatch(r"sha256:[a-f0-9]{64}", args.image_digest), "probe-image-invalid")
    import boto3
    from botocore.config import Config
    import requests
    uri = os.environ.get("ECS_CONTAINER_METADATA_URI_V4", "")
    parsed = urlparse(uri)
    require(parsed.scheme == "http" and parsed.hostname == "169.254.170.2" and parsed.path.startswith("/v4/"),
            "probe-metadata-uri-invalid")
    with closing(requests.get(uri + "/task", timeout=(5, 10), allow_redirects=False)) as response:
        response.raise_for_status()
        identity = host_identity(response.json(), release_id=args.release_id, source_commit=args.source_commit,
                                 image_digest=args.image_digest, gate_sha256=args.gate_sha256)
    prefix = PREFIX + args.release_id + "/"
    with closing(boto3.client("s3", region_name=REGION, config=Config(connect_timeout=5, read_timeout=20,
                    retries={"total_max_attempts": 1}))) as s3:
        ready_sha = await_authorization(s3, prefix, identity)
        manifest = report_probe(s3, prefix, args.release_id)
        complete = {**identity, "phase": "report-verified", "localhost_marker_sha256": ready_sha,
                    "output_manifest_key": manifest["key"], "output_manifest_sha256": manifest["sha256"],
                    "provider_writes": False, "email_sent": False, "live_outputs_changed": False,
                    "skip_invoices": True, "skip_inline_guard": True}
        put_private(s3, prefix + "markers/complete.json", canonical(complete))
    print("VEVO_REPORT_PROBE_COMPLETE:private-evidence:zero-business-writes", flush=True)


if __name__ == "__main__":
    main()
