#!/usr/bin/env python3
"""Finite report probe: localhost identity, release handshake, private outputs.

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
from scripts.reporting_image_release_policy import PROJECTS, VEVO, project_policy

ACCOUNT = "919341186960"
REGION = "eu-central-1"
BUCKET = f"biznisweb-reporting-artifacts-{ACCOUNT}-{REGION}"
PREFIX = "data/vevo/reporting/runtime/probes/"
SERVICE = "vevo-daily-report-email"
MARKER = "VEVO_REPORT_PROBE_HOST_OK"
DIAGNOSTIC_LIMIT = 8 * 1024 * 1024
DIAGNOSTIC_STATUSES = frozenset({"available", "missing", "invalid-path", "invalid-json", "too-large", "unreadable"})
FAILURE_TYPES = frozenset({"RuntimeError", "ValueError", "FileNotFoundError", "SystemExit", "KeyboardInterrupt", "other"})


def require(value, code):
    if not value:
        raise RuntimeError(code)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def put_private(s3, key, raw, *, policy=VEVO):
    require(key.startswith(policy.probe_prefix) and ".." not in key, "probe-output-scope-invalid")
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


def host_identity(metadata, *, release_id, source_commit, image_digest, gate_sha256, policy=VEVO):
    require(os.getcwd() == "/app", "probe-path-invalid")
    require(sha(Path(__file__).read_bytes()) == gate_sha256, "probe-source-file-mismatch")
    require(metadata.get("Family") == policy.family and metadata.get("LaunchType") == "FARGATE",
            "probe-host-family-invalid")
    containers = [row for row in metadata.get("Containers", []) if row.get("Name") == "reporting"]
    require(len(containers) == 1, "probe-host-container-invalid")
    container = containers[0]
    addresses = [ip for network in container.get("Networks", []) for ip in network.get("IPv4Addresses", [])]
    require(len(addresses) == 1 and container.get("ImageID") == image_digest, "probe-host-image-or-ip-invalid")
    task = metadata.get("TaskARN", "")
    require(task.startswith(f"arn:aws:ecs:{REGION}:{ACCOUNT}:task/vevo-reporting-cluster/"), "probe-host-task-invalid")
    return {"marker": policy.probe_marker, "release_id": release_id, "source_commit": source_commit,
            "task_arn": task, "private_ip": addresses[0], "image_digest": image_digest,
            "task_definition": f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/{policy.family}:{metadata['Revision']}",
            "instance_id": "N/A:Fargate", "service": policy.service, "path": "/app", "project": policy.project,
            "gate_sha256": gate_sha256}


def await_authorization(s3, prefix, identity, *, clock=time.monotonic, sleep=time.sleep, policy=VEVO):
    from scripts.order_automation_host_gate import localhost_marker
    localhost_marker(identity)  # curl on this host; its server is closed before any provider call.
    ready = canonical(identity)
    put_private(s3, prefix + "markers/ready.json", ready, policy=policy)
    expected = {"phase": "host-authorized", "release_id": identity["release_id"],
                "task_arn": identity["task_arn"], "ready_sha256": sha(ready)}
    deadline = clock() + 600
    while clock() < deadline:
        try:
            raw = read_private(s3, prefix + "authorize.json")
        except Exception as exc:
            # Without ListBucket, S3 reports a not-yet-created object as 403.
            # A denied read grants nothing: only the exact bound signal below
            # permits provider access; a persistent denial hits the same deadline.
            require(getattr(exc, "response", {}).get("Error", {}).get("Code") in {"NoSuchKey", "404", "AccessDenied", "403"},
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


def retain_failure_diagnostics(s3, prefix, identity, ready_sha, path, *, from_date, to_date,
                               failure_type, policy=VEVO, publication_diagnostics=None):
    """Keep this release's private QA evidence; never authorize rejected output."""
    release_id = identity["release_id"]
    require(prefix == policy.probe_prefix + release_id + "/"
            and re.fullmatch(r"[a-f0-9]{32}", release_id)
            and identity.get("project") == policy.project
            and ready_sha == sha(canonical(identity)), "probe-diagnostic-binding-invalid")
    for value in (from_date, to_date):
        require(re.fullmatch(r"\d{4}-\d{2}-\d{2}", value), "probe-diagnostic-date-invalid")
        datetime.strptime(value, "%Y-%m-%d")
    expected_name = f"data_quality_{from_date.replace('-', '')}-{to_date.replace('-', '')}__migration_{release_id}.json"
    path = Path(path)
    diagnostic = {"status": "missing"}
    content = None
    try:
        if path.name != expected_name or path.resolve().parent != (ROOT / "data" / policy.project).resolve() or path.is_symlink():
            diagnostic = {"status": "invalid-path"}
        elif path.exists():
            with path.open("rb") as source:
                raw = source.read(DIAGNOSTIC_LIMIT + 1)
            if not raw or len(raw) > DIAGNOSTIC_LIMIT:
                diagnostic = {"status": "too-large"}
            else:
                try:
                    require(isinstance(json.loads(raw), dict), "probe-diagnostic-json-invalid")
                except (ValueError, UnicodeError, RuntimeError):
                    diagnostic = {"status": "invalid-json"}
                else:
                    content = raw
    except OSError:
        diagnostic = {"status": "unreadable"}
    if publication_diagnostics is not None:
        require(isinstance(publication_diagnostics, dict)
                and publication_diagnostics.get("schema_version") == 1
                and publication_diagnostics.get("project") == policy.project
                and publication_diagnostics.get("report_from_date") == from_date
                and publication_diagnostics.get("report_to_date") == to_date
                and set(publication_diagnostics.get("periods", {})) == {"latest", "7d", "30d", "90d"},
                "probe-period-diagnostic-binding-invalid")
        full_quality = json.loads(content) if content is not None else {"full_data_quality_read_status": diagnostic["status"]}
        content = canonical({**full_quality, "publication_diagnostics": publication_diagnostics})
    if content is not None:
        if len(content) > DIAGNOSTIC_LIMIT:
            diagnostic = {"status": "too-large"}
        else:
            key = prefix + "artifacts/diagnostics/data_quality.json"
            put_private(s3, key, content, policy=policy)
            diagnostic = {"status": "available", "key": key, "sha256": sha(content), "size": len(content)}
    failure = {**identity, "schema_version": 1, "phase": "report-rejected",
               "localhost_marker_sha256": ready_sha, "report_from_date": from_date, "report_to_date": to_date,
               "failure_type": failure_type if failure_type in FAILURE_TYPES else "other", "diagnostic": diagnostic}
    raw = canonical(failure)
    key = prefix + "markers/failed.json"
    put_private(s3, key, raw, policy=policy)
    return {"key": key, "sha256": sha(raw), "diagnostic": diagnostic}


def run_authorized_probe(s3, prefix, identity, ready_sha, *, policy=VEVO):
    """Preserve bounded private failure evidence even when runner rejects before upload."""
    import daily_report_runner as runner
    flags = runner.parse_args(["--project", policy.project])
    from_date = runner.normalize_date(flags.from_date)
    to_date = runner.normalize_date(runner.resolve_to_date(flags.to_date, flags.timezone))
    paths = runner.build_artifact_set(policy.project, from_date, to_date,
                                      output_tag="migration_" + identity["release_id"]).as_dict()
    meta_snapshot = None
    if policy == VEVO:
        from scripts.reporting_meta_source_snapshot import MetaSourceSnapshot
        meta_snapshot = MetaSourceSnapshot(identity, ready_sha, from_date, to_date, prefix,
            lambda key, raw: put_private(s3, key, raw, policy=policy),
            credentials=tuple(os.environ.get(key, '') for key in ('FACEBOOK_ACCESS_TOKEN', 'FACEBOOK_APP_SECRET')))
    try:
        return report_probe(s3, prefix, identity["release_id"], policy=policy, meta_snapshot=meta_snapshot)
    except BaseException as exc:
        try:
            diagnostics = None
            if paths.get("dashboard_payload_latest_json") is not None:
                diagnostics = getattr(exc, "publication_diagnostics", None)
                if diagnostics is None:
                    diagnostics = runner.collect_publication_diagnostics(policy.project, paths, from_date, to_date)
            retain_failure_diagnostics(s3, prefix, identity, ready_sha, paths["data_quality_json"],
                                       from_date=from_date, to_date=to_date, failure_type=type(exc).__name__, policy=policy,
                                       publication_diagnostics=diagnostics)
        except Exception:
            # Do not replace the original failure or emit provider/financial details.
            # The controller records missing evidence and still rejects the probe.
            print("REPORT_PROBE_FAILURE_DIAGNOSTICS_UNAVAILABLE", flush=True)
        raise


def isolate_outputs(s3, prefix, runner, paths, *, policy=VEVO):
    """No call to the ordinary uploader, aliases, presigning or live generation."""
    artifacts = runner._canonical_live_artifact_paths(policy.project, paths)
    expected = set(runner.STABLE_LIVE_ARTIFACT_NAMES)
    for names in runner.PERIOD_LIVE_ARTIFACT_NAMES.values():
        expected.update(names.values())
    require(set(artifacts) == expected and bool(expected), "probe-required-period-artifacts-missing")
    verify_quality(runner.load_data_quality(paths.get("data_quality_json")))
    artifacts["data_quality.json"] = paths["data_quality_json"]
    entries = {}
    for name, path in sorted(artifacts.items()):
        require(Path(name).name == name and path.is_file() and path.resolve().is_relative_to(ROOT / "data" / policy.project),
                "probe-artifact-path-invalid")
        raw = path.read_bytes()
        require(0 < len(raw) <= 64 * 1024 * 1024, "probe-artifact-empty-or-too-large")
        if name.endswith("data_quality.json"):
            verify_quality(json.loads(raw))
        if name.endswith(".html"):
            require(b"<html" in raw.lower(), "probe-html-invalid")
        if name.startswith("dashboard_payload_"):
            payload = json.loads(raw)
            require(payload.get("project") == policy.project, "probe-payload-project-invalid")
            verify_quality(payload.get("source_health"))
        key = prefix + "artifacts/" + name
        put_private(s3, key, raw, policy=policy)
        entries[name] = {"key": key, "sha256": sha(raw), "size": len(raw)}
    manifest = {"schema_version": 1, "project": policy.project, "artifacts": entries,
                "generated_at": datetime.now(timezone.utc).isoformat()}
    raw = canonical(manifest)
    key = prefix + "artifacts/output-manifest.json"
    put_private(s3, key, raw, policy=policy)
    return {"key": key, "sha256": sha(raw)}


class InventoryProbeAudit:
    """Authorize overlap only inside the completed reporting inventory hook."""

    def __init__(self, incomplete, diagnostic):
        self.incomplete, self.diagnostic = incomplete, diagnostic
        self.inventory, self.sessions, self.current = {}, [], None

    def reject(self, code):
        self.incomplete.append(code)
        if self.current is not None:
            self.current["state"] = "failed"
        raise RuntimeError("probe-" + code)

    def context(self, client, roots, variables):
        session = self.current
        if session is None or session["variables"] is None or "getOrderList" not in roots:
            return None
        if roots != ("getOrderList",) or canonical(variables) != session["variables"]:
            self.reject("inventory-context-invalid")
        if session["client"] is None:
            session["client"] = client
        if session["client"] is not client:
            self.reject("inventory-client-changed")
        return session

    def observe(self, client, roots, variables, value, query_sha, session):
        if "getOrderList" not in roots and not (isinstance(value, dict) and "getOrderList" in value):
            return
        page = value.get("getOrderList") if isinstance(value, dict) else None
        rows = page.get("data") if isinstance(page, dict) else None
        info = page.get("pageInfo") if isinstance(page, dict) else None
        ids = [str(row.get("id") or "") if isinstance(row, dict) else "" for row in rows] if isinstance(rows, list) else []
        flags = {"page_object": isinstance(page, dict), "rows_list": isinstance(rows, list),
                 "page_info_object": isinstance(info, dict),
                 "has_next_boolean": isinstance(info, dict) and type(info.get("hasNextPage")) is bool,
                 "row_objects": isinstance(rows, list) and all(isinstance(row, dict) for row in rows),
                 "ids_present": all(ids), "ids_unique_in_page": len(ids) == len(set(ids))}
        more = isinstance(info, dict) and info.get("hasNextPage") is True
        flags.update(nonempty_if_more=not more or bool(rows), next_cursor_if_more=not more or bool(info.get("nextCursor")))
        params = variables.get("params", {}) if isinstance(variables, dict) else {}
        if not isinstance(params, dict):
            params = {}
        key = (id(client), params.get("sort"), params.get("order_by"))
        if session is None and not params.get("cursor"):
            self.inventory[key] = set()
        seen = session["seen_ids"] if session is not None else self.inventory.setdefault(key, set())
        overlap = sorted(seen.intersection(ids))
        flags["cross_page_disjoint"] = not overlap
        flags["scoped_overlap"] = session is not None
        valid = all(flags[key] for key in ("page_object", "rows_list", "page_info_object", "has_next_boolean",
                    "row_objects", "ids_present", "ids_unique_in_page", "nonempty_if_more", "next_cursor_if_more"))
        pairs = set()
        if session is not None:
            from order_inventory import internal_order_id
            try:
                normalized = [str(internal_order_id(row)) for row in rows]
                pairs = {(str(internal_order_id(row)), str(row.get("order_num") or "").strip()) for row in rows}
                flags["internal_ids_valid"] = True
                flags["order_numbers_present"] = all(number for _, number in pairs)
                flags["normalized_ids_unique"] = len(normalized) == len(set(normalized))
            except (TypeError, AttributeError, RuntimeError):
                flags.update(internal_ids_valid=False, order_numbers_present=False, normalized_ids_unique=False)
                normalized = []
            valid = valid and flags["internal_ids_valid"] and flags["order_numbers_present"] and flags["normalized_ids_unique"]
            flags["total_records_nonnegative_integer"] = (isinstance(info, dict)
                and type(info.get("totalRecords")) is int and info["totalRecords"] >= 0)
        else:
            valid = valid and not overlap
        detail = {"variables": json.loads(canonical(variables)), "query_sha256": query_sha,
                  "response": value, "predicate_flags": flags, "cross_page_intersection": overlap,
                  "scope": "reporting-inventory" if session is not None else "ordinary-query"}
        if session is not None:
            session["last_page"] = detail
        if not valid:
            self.diagnostic(detail)
            self.reject("inventory-page-invalid")
        seen.update(ids)
        if session is not None:
            session["seen_pairs"].update(pairs)
            session["read_count"] += 1

    def scan(self, scanner, read, validate_order, **kwargs):
        from order_inventory import internal_order_id
        if self.current is not None:
            self.reject("inventory-scan-nested")
        session = {"state": "pending", "variables": None, "client": None,
                   "seen_ids": set(), "seen_pairs": set(), "read_count": 0, "last_page": None}
        self.sessions.append(session)
        self.current = session

        def scoped_read(variables, deadline):
            if session["variables"] is not None:
                self.reject("inventory-read-nested")
            session["variables"] = canonical(variables)
            try:
                return read(variables, deadline)
            finally:
                session["variables"] = None

        try:
            result = scanner(scoped_read, validate_order, **kwargs)
            require(session["state"] == "pending" and session["read_count"] > 0 and isinstance(result, list),
                    "probe-inventory-scan-unproven")
            ids = [str(internal_order_id(row)) for row in result]
            numbers = [str(row.get("order_num") or "").strip() for row in result]
            require(all(numbers) and len(ids) == len(set(ids)) and len(numbers) == len(set(numbers))
                    and set(zip(ids, numbers)).issubset(session["seen_pairs"]), "probe-inventory-result-unproven")
            session["state"] = "completed"
            return result
        except BaseException as exc:
            session["state"] = "failed"
            self.incomplete.append("inventory-scan-incomplete")
            if session["last_page"] is not None:
                self.diagnostic({**session["last_page"], "scanner_exception_type": type(exc).__name__})
            raise
        finally:
            self.current = None

    def complete(self):
        return self.current is None and all(session["state"] == "completed" for session in self.sessions)


def report_probe(s3, prefix, release_id, *, policy=VEVO, meta_snapshot=None):
    import daily_report_runner as runner
    import export_orders
    import requests
    from gql import Client
    from graphql import OperationDefinitionNode, OperationType, print_ast
    from reporting_core import metrics

    required = {"REPORT_PROJECT": policy.project, "REPORT_SKIP_INVOICES": "true",
                "REPORT_SKIP_CREDITNOTE_STORNO_GUARD": "true", "REPORT_SKIP_EMAIL": "true"}
    require(all(os.environ.get(k) == v for k, v in required.items()), "probe-skip-environment-invalid")
    original_input = (os.environ.get("REPORT_S3_BUCKET"), os.environ.get("REPORT_S3_PREFIX"))
    require(original_input[0] == BUCKET and original_input[1] == policy.sink,
            "probe-production-input-location-invalid")
    result = {}
    attempts = {"provider_writes": 0, "email": 0, "live_output": 0}
    incomplete_reads = []
    pending_reads = set()
    diagnostic_count = 0

    def inventory_diagnostic(detail):
        nonlocal diagnostic_count
        if diagnostic_count >= 8:
            return
        diagnostic_count += 1
        try:
            require(prefix == policy.probe_prefix + release_id + "/", "probe-inventory-diagnostic-scope")
            content = {"schema_version": 1, "project": policy.project, "release_id": release_id, **detail}
            raw = canonical(content)
            if len(raw) > DIAGNOSTIC_LIMIT:
                content.pop("response")
                content["response_retention"] = {"status": "too-large", "size": len(raw), "sha256": sha(raw)}
                raw = canonical(content)
            require(len(raw) <= DIAGNOSTIC_LIMIT, "probe-inventory-diagnostic-too-large")
            put_private(s3, prefix + f"artifacts/diagnostics/inventory-page-{diagnostic_count:04d}.json", raw, policy=policy)
        except Exception:
            # Evidence is optional; the original invalid page remains fatal.
            print("REPORT_PROBE_INVENTORY_DIAGNOSTICS_UNAVAILABLE", flush=True)

    inventory = InventoryProbeAudit(incomplete_reads, inventory_diagnostic)
    reporting_scan = export_orders.scan_reporting_order_inventory

    def scan_inventory(read, validate_order, **kwargs):
        return inventory.scan(reporting_scan, read, validate_order, **kwargs)

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
        session = inventory.context(client, roots, variables)
        read_key = (id(client), roots, sha(canonical(variables)))
        try:
            value = execute(client, query, *args, **kwargs)
        except Exception:
            # A successful retry or supported reduced-field read of this exact
            # page resolves its error. Other successful pages cannot hide it.
            pending_reads.add(read_key)
            raise
        pending_reads.discard(read_key)
        inventory.observe(client, roots, variables, value, sha(print_ast(doc).encode()), session)
        return value

    send = requests.Session.send
    def read_native(session, request, **kwargs):
        parsed = urlparse(request.url)
        if parsed.hostname in policy.native_hosts and parsed.path != "/api/graphql":
            allowed = {("GET", "/erp/main/login"), ("POST", "/admin/login/authenticate/"),
                       ("POST", "/erp/orders/creditnotes/getListJson"), ("GET", "/erp/main/")}
            if (request.method, parsed.path) not in allowed:
                return forbidden("provider_writes")()
        if any(parsed.hostname in other.native_hosts for other in PROJECTS.values() if other != policy):
            return forbidden("provider_writes")()
        try:
            response = send(session, request, **kwargs)
        except BaseException:
            if meta_snapshot is not None:
                meta_snapshot.observe_failure(request)
            raise
        if meta_snapshot is not None:
            meta_snapshot.observe(request, response)
        return response

    def export_in_process(**kwargs):
        require(kwargs["project"] == policy.project and kwargs["output_tag"] == "migration_" + release_id,
                "probe-export-context-invalid")
        argv = ["export_orders.py", "--project", policy.project, "--from-date", kwargs["from_date"],
                "--to-date", kwargs["to_date"], "--output-tag", kwargs["output_tag"]]
        if kwargs["no_cache"]:
            argv.append("--no-cache")
        if kwargs["clear_cache"]:
            argv.append("--clear-cache")
        with patch.object(sys, "argv", argv):
            export_orders.main()  # Same production CLI, guarded in this process.

    def publish(project, paths):
        require(project == policy.project, "probe-output-project-invalid")
        require((os.environ.get("REPORT_S3_BUCKET"), os.environ.get("REPORT_S3_PREFIX")) == original_input,
                "probe-input-location-changed")
        require(not incomplete_reads and not pending_reads and inventory.complete(), "probe-provider-reads-incomplete")
        if meta_snapshot is not None:
            result["meta_source_manifest"] = meta_snapshot.finalize()
        result.update(isolate_outputs(s3, prefix, runner, paths, policy=policy))
        return {}

    argv = ["daily_report_runner.py", "--project", policy.project, "--skip-email", "--skip-invoices",
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
            (export_orders, "scan_reporting_order_inventory", scan_inventory),
            (export_orders.logger, "warning", guard_inventory_limit),
        ):
            stack.enter_context(patch.object(target, name, replacement))
        stack.enter_context(patch.object(sys, "argv", argv))
        runner.main()
    require(result and not any(attempts.values()) and not incomplete_reads and not pending_reads and inventory.complete(),
            "probe-report-not-verified")
    return result


def main():
    parser = argparse.ArgumentParser()
    for name in ("release-id", "source-commit", "image-digest", "gate-sha256"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--project", choices=tuple(PROJECTS), default="vevo")
    args = parser.parse_args()
    policy = project_policy(args.project)
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
                                 image_digest=args.image_digest, gate_sha256=args.gate_sha256, policy=policy)
    prefix = policy.probe_prefix + args.release_id + "/"
    with closing(boto3.client("s3", region_name=REGION, config=Config(connect_timeout=5, read_timeout=20,
                    retries={"total_max_attempts": 1}))) as s3:
        ready_sha = await_authorization(s3, prefix, identity, policy=policy)
        manifest = run_authorized_probe(s3, prefix, identity, ready_sha, policy=policy)
        complete = {**identity, "phase": "report-verified", "localhost_marker_sha256": ready_sha,
                    "output_manifest_key": manifest["key"], "output_manifest_sha256": manifest["sha256"],
                    "provider_writes": False, "email_sent": False, "live_outputs_changed": False,
                    "skip_invoices": True, "skip_inline_guard": True}
        if policy == VEVO:
            complete["meta_source_manifest"] = manifest["meta_source_manifest"]
        put_private(s3, prefix + "markers/complete.json", canonical(complete), policy=policy)
    print(policy.project.upper() + "_REPORT_PROBE_COMPLETE:private-evidence:zero-business-writes", flush=True)


if __name__ == "__main__":
    main()
