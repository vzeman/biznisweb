#!/usr/bin/env python3
"""Reconcile the fixed ROY incident evidence, without automatic write retries.

Preview is read-only. ``confirm-emails --apply`` records two provider-confirmed
sends without sending. Both actions use read-only native row lookups, including
in preview. ``finalize-one --apply`` reconciles one
deterministically selected existing document, never prepares one, and holds its
email. An intent is permanently consumed before the single native GET, including
a crash before transmission. This is not a provider idempotency guarantee.

Apply requires the exact promoted source/digest and its private deployment proof.
Original attempts remain in the journal; uncertainty is never reset to pending.
No customer identity, response body, credentials or token-bearing URL is printed.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import ipaddress
import json
from pathlib import Path
import re
import subprocess
import sys
from uuid import uuid4
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from invoice_automation_state import AutomationStateError, S3AutomationStateStore, parse_utc
from scripts.deploy_order_automations import SCHEDULES, command_for, schedule_request
from scripts.restore_verified_fulfillment import runtime_environment
from scripts.verify_invoice_discovery import require_pushed_source

ACCOUNT = "919341186960"
BUCKET = "biznisweb-reporting-artifacts-919341186960-eu-central-1"
CLUSTER = "vevo-reporting-cluster"
REVIEWED_RELEASE_COMMIT = "47c3da775ff7018a1cf8950024c867310b91ae8a"
REVIEWED_RELEASE_DIGEST = "sha256:c3842dda5a831c0ddf4e7d4fc6dd5b8a8507ec4e22c97723e1577023f04c25f3"
EVIDENCE_KEY = "data/roy/order-automation/audits/2026-09-13/uncertain-operations-readback-20260913.json"
EVIDENCE_SHA = "b7a4bca333d919c2d7c16cd9269cc45851a8fbd6d7d3d4417cd98264cfa28545"
IDENTITY_KEY = "data/roy/order-automation/audits/2026-09-13/native-document-key-contract-20260913.json"
IDENTITY_SHA = "a0fa9c3391e13baf0527a64c05ea8bb9e0b9451681dfc60d93618082f4fcff36"
NATIVE_EVIDENCE_KEY = "data/roy/order-automation/audits/2026-09-13/reviewed-native-six-case-evidence-20260913.json"
NATIVE_EVIDENCE_SHA = "ce520d23ca3f3ac1b55208f838f644f81356745ae451cb494bc22911ccca9af3"
NATIVE_CONTRACT = {"find": "o#<order_num>", "limit": 20, "list_method": "POST",
                   "native_finalize_field": "pre_inv_id", "native_send_field": "order_id",
                   "path": "/erp/orders/invoices/getListJson", "start": 0, "token_required_by_native_code": True}
RECONCILIATION = "reviewed_invoice_reconciliation"
OPERATION_FIELDS = ("order_num", "phase", "email_state", "invoice_id", "invoice_num", "attempted_at", "email_policy")


class ReconciliationBlocked(RuntimeError):
    """A fixed, non-sensitive gate failure."""


def require(condition, code):
    if not condition:
        raise ReconciliationBlocked(code)


def identity(value):
    require(not isinstance(value, bool) and isinstance(value, (str, int))
            and re.fullmatch(r"[1-9][0-9]*", str(value)) is not None, "invalid-document-identity")
    return str(value)


def projection(record):
    return {key: record.get(key) for key in OPERATION_FIELDS}


def validate_manifest(evidence):
    require(isinstance(evidence, dict) and evidence.get("read_only") is True, "unverified-private-evidence")
    rows = evidence.get("cases")
    require(isinstance(rows, list) and len(rows) == 6, "reviewed-case-count-mismatch")
    require({row.get("case") for row in rows} == {f"UNCERTAIN-{i}" for i in range(1, 7)}, "reviewed-case-scope-mismatch")
    cases, numbers, kinds = [], set(), []
    for row in rows:
        prior, fresh, history = row.get("journal", {}), row.get("fresh", {}), row.get("admin_history_readback", {})
        number = row.get("order_num")
        require(isinstance(number, str) and number.isdigit() and number not in numbers
                and prior.get("order_num") == number and fresh.get("order_num") == number, "evidence-order-binding-mismatch")
        numbers.add(number)
        order_id = identity(fresh.get("id"))
        preinvoices = fresh.get("preinvoices")
        require(isinstance(preinvoices, list) and len(preinvoices) == 1, "evidence-preinvoice-binding-missing")
        preinvoice_id = identity(preinvoices[0].get("id"))
        require(history.get("source") == "https://roy.flox.sk/erp/main/orders"
                and history.get("timezone") == "Europe/Bratislava"
                and history.get("all_displayed_history_reviewed") is True
                and history.get("dialog_canceled_without_save") is True, "native-history-evidence-incomplete")
        if prior.get("phase") == "email" and prior.get("email_state") == "ambiguous":
            kind = "email"
            invoices = fresh.get("invoices")
            require(isinstance(invoices, list) and len(invoices) == 1
                    and identity(invoices[0].get("id")) == identity(prior.get("invoice_id")) == preinvoice_id
                    and invoices[0].get("invoice_num") == prior.get("invoice_num")
                    and isinstance(prior.get("invoice_num"), str) and bool(prior["invoice_num"].strip()), "email-document-evidence-mismatch")
            events = history.get("invoice_email_events")
            require(isinstance(events, list) and len(events) == 2 and history.get("final_invoice_creation_events") == 1,
                    "provider-send-evidence-missing")
            times = [datetime.fromisoformat(value).replace(tzinfo=ZoneInfo("Europe/Bratislava")) for value in events]
            require(times[0] < times[1] and abs((times[1] - parse_utc(prior["updated_at"])).total_seconds()) < 2,
                    "provider-send-time-mismatch")
        else:
            kind = "finalize"
            require(prior.get("phase") == "create_ambiguous" and prior.get("email_state") is None
                    and not prior.get("invoice_id") and fresh.get("invoices") == []
                    and history.get("final_invoice_creation_events") == 0 and history.get("invoice_email_events") == [],
                    "unreviewed-finalization-case")
        kinds.append(kind)
        cases.append({"label": row["case"], "number": number, "kind": kind, "order_id": order_id,
                      "preinvoice_id": preinvoice_id, "original": deepcopy(prior), "history": deepcopy(history)})
    require(kinds.count("email") == 2 and kinds.count("finalize") == 4, "reviewed-outcome-count-mismatch")
    return sorted(cases, key=lambda row: row["number"])


def private_json(s3, key, *, expected_sha=None):
    response = s3.get_object(Bucket=BUCKET, Key=key, ExpectedBucketOwner=ACCOUNT)
    try:
        require(response.get("ServerSideEncryption") in {"AES256", "aws:kms"}, "private-evidence-encryption-unverified")
        payload = response["Body"].read(2_000_001)
    finally:
        response["Body"].close()
    require(len(payload) <= 2_000_000, "private-evidence-too-large")
    if expected_sha:
        require(hashlib.sha256(payload).hexdigest() == expected_sha, "private-evidence-hash-mismatch")
    return json.loads(payload)


def bind_native_manifest(cases, evidence, contract):
    """Bind historical outcomes to separate native keys, never an API-ID alias."""
    require(isinstance(contract, dict) and contract.get("read_only") is True
            and type(contract.get("financial_requests")) is int and contract["financial_requests"] == 0
            and contract.get("contract") == NATIVE_CONTRACT and len(contract.get("cases", [])) == 4,
            "native-document-identity-contract-unverified")
    require(isinstance(evidence, dict) and evidence.get("read_only") is True and evidence.get("complete") is True
            and evidence.get("schema_version") == 1 and evidence.get("project") == "roy"
            and all(type(evidence.get(key)) is int and evidence[key] == 0 for key in ("financial_requests", "journal_mutations"))
            and evidence.get("manifest") == {"key": EVIDENCE_KEY, "sha256": EVIDENCE_SHA}
            and evidence.get("native_contract_reference") == {"contract": NATIVE_CONTRACT,
                "filename": IDENTITY_KEY.rsplit("/", 1)[-1], "sha256": IDENTITY_SHA}, "native-case-evidence-unverified")
    rows = evidence.get("cases")
    require(isinstance(rows, list) and len(rows) == 6 and all(isinstance(row, dict) for row in rows)
            and {row.get("case") for row in rows} == {case["label"] for case in cases}, "native-case-scope-mismatch")
    bound, native_keys = [], set()
    for case in cases:
        row = next(row for row in rows if row["case"] == case["label"])
        api, native = row.get("api", {}), row.get("native", {})
        require(row.get("ok") is True and row.get("arf_present") is True and row.get("native_total") == "1"
                and row.get("order_num") == api.get("order_num") == native.get("order_num") == case["number"]
                and identity(api.get("order_id")) == identity(native.get("order_id")) == case["order_id"]
                and api.get("preinvoices") == [{"id": case["preinvoice_id"]}], "native-case-order-binding-mismatch")
        key = identity(native.get("pre_inv_id"))
        require(key not in native_keys, "native-case-key-reused")
        native_keys.add(key)
        if case["kind"] == "email":
            require(row.get("kind") == "confirmed_email" and row.get("outcome") == "native_and_api_final_confirmed"
                    and api.get("invoices") == [{"id": case["original"]["invoice_id"], "invoice_num": case["original"]["invoice_num"]}]
                    and native.get("inv_id") == case["original"]["invoice_num"], "native-email-document-evidence-mismatch")
        else:
            require(row.get("kind") == "ambiguous_creation" and row.get("outcome") == "unique_pending_native_document"
                    and api.get("invoices") == [] and native.get("inv_id") == "", "native-pending-document-evidence-mismatch")
        bound.append({**deepcopy(case), "native_preinvoice_key": key})
    return bound


def load_cases(s3):
    evidence = private_json(s3, EVIDENCE_KEY, expected_sha=EVIDENCE_SHA)
    contract = private_json(s3, IDENTITY_KEY, expected_sha=IDENTITY_SHA)
    native = private_json(s3, NATIVE_EVIDENCE_KEY, expected_sha=NATIVE_EVIDENCE_SHA)
    return bind_native_manifest(validate_manifest(evidence), native, contract)


def verify_release(session, *, digest, commit, evidence_key):
    """Read actual promotion, all five exact targets, ECR source and active jobs."""
    require(re.fullmatch(r"sha256:[0-9a-f]{64}", digest or "") is not None
            and re.fullmatch(r"[0-9a-f]{40}", commit or "") is not None, "immutable-release-required")
    require((commit, digest) == (REVIEWED_RELEASE_COMMIT, REVIEWED_RELEASE_DIGEST), "release-outside-reviewed-migration")
    require(re.fullmatch(rf"data/roy/order-automation/deployments/{commit}/[0-9a-f]{{32}}\.json", evidence_key or "") is not None,
            "promotion-evidence-path-mismatch")
    require(session.client("sts").get_caller_identity()["Account"] == ACCOUNT, "aws-account-mismatch")
    evidence = private_json(session.client("s3"), evidence_key)
    require(evidence.get("schema") == 1 and evidence.get("commit") == commit
            and evidence.get("image_digest") == digest and evidence.get("phase") == "promotion-readback-verified",
            "release-not-verified-promoted")
    hosts = evidence.get("hosts", [])
    require(isinstance(hosts, list) and len(hosts) == 3 and all(isinstance(row, dict) for row in hosts)
            and {row.get("service") for row in hosts} == set(SCHEDULES.values()), "release-host-scope-incomplete")
    for row in hosts:
        family = row["service"]
        invoice = family.endswith("invoice-daily")
        marker = {"marker": "ORDER_AUTOMATION_HOST_OK", "project": family.split("-")[0],
                  "kind": "invoice" if invoice else "cancellation", "path": "/app", "dry_run": True}
        if invoice:
            marker["full_backlog"] = True
        require(row.get("image_digest") == digest and row.get("path") == "/app" and type(row.get("exit_code")) is int
                and row["exit_code"] == 0 and row.get("marker") == marker and row.get("instance_id") == "N/A:FARGATE",
                "release-host-gates-incomplete")
        ip = ipaddress.ip_address(row.get("private_ip", ""))
        require(ip.version == 4 and ip.is_private and not ip.is_loopback and not ip.is_unspecified,
                "release-host-ip-unverified")
        require(re.fullmatch(rf"arn:aws:ecs:eu-central-1:{ACCOUNT}:task/{CLUSTER}/[0-9a-f]{{32}}", row.get("task", "")) is not None
                and re.fullmatch(rf"arn:aws:ecs:eu-central-1:{ACCOUNT}:task-definition/{family}:[1-9][0-9]*", row.get("task_definition", "")) is not None,
                "release-host-identity-unverified")
    require(len({row["task"] for row in hosts}) == 3, "release-host-task-reused")
    for name in ("candidate_drain", "drain"):
        drain = evidence.get(name, {})
        require(type(drain.get("quiet_seconds")) is int and drain["quiet_seconds"] >= 120
                and type(drain.get("unfinished_tasks")) is int and drain["unfinished_tasks"] == 0,
                "release-drain-unverified")
        parse_utc(drain["verified_at"])
    require(parse_utc(evidence["candidate_drain"]["verified_at"]) <= parse_utc(evidence["drain"]["verified_at"]),
            "release-drain-order-invalid")
    image = f"{ACCOUNT}.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting@{digest}"
    images = session.client("ecr").describe_images(repositoryName="vevo-reporting", imageIds=[{"imageTag": f"git-{commit}"}])["imageDetails"]
    require(len(images) == 1 and images[0].get("imageDigest") == digest, "release-source-image-mismatch")
    ecs, scheduler = session.client("ecs"), session.client("scheduler")
    desired = evidence.get("desired_schedules", {})
    require(set(desired) == set(SCHEDULES), "promotion-schedule-scope-mismatch")
    for name, family in SCHEDULES.items():
        schedule = scheduler.get_schedule(Name=name)
        require(schedule_request(schedule) == desired[name] and schedule.get("State") == "ENABLED", "live-schedule-drift")
        require(schedule["Target"]["Arn"] == f"arn:aws:ecs:eu-central-1:{ACCOUNT}:cluster/{CLUSTER}", "live-cluster-drift")
        definition = ecs.describe_task_definition(taskDefinition=schedule["Target"]["EcsParameters"]["TaskDefinitionArn"])["taskDefinition"]
        require(next(row for row in hosts if row["service"] == family)["task_definition"]
                == schedule["Target"]["EcsParameters"]["TaskDefinitionArn"], "promoted-host-definition-mismatch")
        containers = definition.get("containerDefinitions", [])
        require(definition.get("family") == family and len(containers) == 1 and containers[0].get("name") == "reporting"
                and containers[0].get("image") == image and containers[0].get("workingDirectory") == "/app"
                and containers[0].get("command") == command_for(family), "live-task-definition-drift")
    for family in sorted(set(SCHEDULES.values())):
        for desired_status in ("RUNNING", "STOPPED"):
            token = None
            for _ in range(20):
                request = {"cluster": CLUSTER, "family": family, "desiredStatus": desired_status, "maxResults": 100}
                if token:
                    request["nextToken"] = token
                page = ecs.list_tasks(**request)
                if page["taskArns"]:
                    tasks = ecs.describe_tasks(cluster=CLUSTER, tasks=page["taskArns"])
                    require(not tasks.get("failures") and len(tasks.get("tasks", [])) == len(page["taskArns"])
                            and {row.get("taskArn") for row in tasks["tasks"]} == set(page["taskArns"]), "active-task-inspection-incomplete")
                    for task in tasks["tasks"]:
                        require(task.get("clusterArn") == f"arn:aws:ecs:eu-central-1:{ACCOUNT}:cluster/{CLUSTER}"
                                and bool(task.get("lastStatus")) and re.fullmatch(
                                    rf"arn:aws:ecs:eu-central-1:{ACCOUNT}:task-definition/{family}:[1-9][0-9]*",
                                    task.get("taskDefinitionArn", "")) is not None, "active-task-identity-mismatch")
                        if task.get("lastStatus") == "STOPPED":
                            continue
                        containers = task.get("containers", [])
                        require(len(containers) == 1 and containers[0].get("image") == image
                                and containers[0].get("imageDigest") == digest and task["taskDefinitionArn"]
                                == next(row for row in hosts if row["service"] == family)["task_definition"], "old-or-unverified-active-writer")
                        overrides = task.get("overrides", {}).get("containerOverrides", [])
                        require(not overrides or len(overrides) == 1 and overrides[0].get("name") == "reporting"
                                and not overrides[0].get("environment") and not overrides[0].get("environmentFiles")
                                and overrides[0].get("command", command_for(family)) == command_for(family), "active-writer-override-unverified")
                next_token = page.get("nextToken")
                if not next_token:
                    break
                require(next_token != token, "active-task-pagination-cycle")
                token = next_token
            else:
                raise ReconciliationBlocked("active-task-pagination-incomplete")


def validate_state(state):
    require(isinstance(state, dict) and state.get("project") == "roy" and state.get("schema_version") == 1
            and isinstance(state.get("last_scan"), dict) and isinstance(state.get("orders"), dict)
            and all(isinstance(row, dict) for row in state["orders"].values()), "journal-identity-or-schema-mismatch")


def validate_record(record, case):
    mutation = record.get("status_mutation")
    require(mutation is None or isinstance(mutation, dict) and mutation.get("state") == "verified", "unresolved-status-operation")
    resolution = record.get(RECONCILIATION)
    if resolution:
        require(isinstance(resolution, dict) and resolution.get("evidence_sha256") == EVIDENCE_SHA
                and resolution.get("schema_version") == 2 and resolution.get("evidence_key") == EVIDENCE_KEY
                and resolution.get("native_evidence_key") == NATIVE_EVIDENCE_KEY
                and resolution.get("native_evidence_sha256") == NATIVE_EVIDENCE_SHA
                and resolution.get("identity_evidence_sha256") == IDENTITY_SHA
                and resolution.get("state") in {"readback_only", "attempt_started", "uncertain", "verified"}
                and resolution.get("case") == case["label"] and resolution.get("kind") == case["kind"]
                and projection(resolution.get("original_record", {})) == projection(case["original"]), "reconciliation-intent-drift")
        require(record.get("order_num") == case["number"] and record.get("attempted_at") == case["original"].get("attempted_at")
                and resolution.get("order_id") == case["order_id"] and resolution.get("preinvoice_id") == case["preinvoice_id"]
                and resolution.get("native_preinvoice_key") == case["native_preinvoice_key"],
                "reconciliation-order-binding-drift")
        if case["kind"] == "finalize":
            require(record.get("email_policy") == "hold" and record.get("email_state") in {None, "held"}, "reconciliation-email-hold-lost")
        if resolution.get("state") in {"attempt_started", "uncertain"}:
            require(re.fullmatch(r"[0-9a-f]{32}", resolution.get("attempt_id", "")) is not None,
                    "reconciliation-attempt-identity-missing")
        if resolution.get("state") == "verified":
            expected = case["original"] if case["kind"] == "email" else resolution
            require(record.get("invoice_id") == expected.get("invoice_id")
                    and record.get("invoice_num") == expected.get("invoice_num"), "completed-document-state-drift")
    else:
        # A final invoice may have appeared elsewhere. Only its final readback
        # can authorize documenting that outcome, never a new financial request.
        expected = projection(case["original"])
        actual = projection(record)
        if case["kind"] == "finalize" and record.get("phase") == "complete" and record.get("email_state") in {None, "held"}:
            for key in ("phase", "invoice_id", "invoice_num", "email_state"):
                actual[key] = expected[key]
        require(actual == expected, "original-operation-changed")
    return record


def prior_record(journal, case):
    snapshot = journal.snapshot()
    validate_state(snapshot)
    return validate_record(snapshot["orders"].get(case["number"], {}), case)


def read_bound(generator, case):
    current = generator.fetch_order_for_invoice(case["number"])
    require(current.get("order_num") == case["number"] and identity(current.get("id")) == case["order_id"], "fresh-order-identity-drift")
    invoices = current.get("invoices")
    preinvoices = current.get("preinvoices")
    require(isinstance(invoices, list) and len(invoices) <= 1 and isinstance(preinvoices, list)
            and len(preinvoices) == 1 and identity(preinvoices[0].get("id")) == case["preinvoice_id"], "fresh-document-association-drift")
    native = generator.fetch_native_invoice_context(case["number"], case["order_id"])
    require(native.get("order_id") == case["order_id"] and native.get("order_num") == case["number"]
            and native.get("preinvoice_key") == case["native_preinvoice_key"], "fresh-native-document-binding-drift")
    if invoices:
        invoice = invoices[0]
        # ID equality confirms the reviewed API association only. The native
        # route key is separate; final-number agreement proves the native result.
        require(identity(invoice.get("id")) == case["preinvoice_id"]
                and isinstance(invoice.get("invoice_num"), str) and bool(invoice["invoice_num"].strip())
                and invoice["invoice_num"] == native.get("invoice_number"), "final-document-binding-unverified")
    else:
        require(native.get("invoice_number") == "", "native-final-without-api-confirmation")
    return current


def require_eligible(generator, order):
    try:
        value = order["sum"]["value"]
        amount = Decimal(str(value))
        currency = order["sum"]["currency"]["code"]
        valid = not isinstance(value, bool) and amount.is_finite() and amount > 0 and isinstance(currency, str) and bool(currency)
    except (KeyError, TypeError, ValueError, InvalidOperation):
        valid = False
    require(valid and order.get("blocked") is False and generator._status_is_eligible(order), "fresh-order-ineligible")


def response_fingerprint(response):
    """Bounded diagnostic classification; never retain a response body or URL."""
    raw = response.content
    result = {"http_status": response.status_code, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
    if len(raw) > 8192:
        return {**result, "format": "oversized"}
    text = raw.decode("utf-8", errors="replace")
    try:
        payload = json.loads(text)
        result["format"] = "json"
    except ValueError:
        try:
            payload = json.loads(text.lstrip("\ufeff"))
            result["format"] = "bom_json"
        except ValueError:
            payload = None
            result["format"] = "object_literal_like" if text.lstrip("\ufeff \r\n\t").startswith("{") else "other"
    success = payload.get("success") if isinstance(payload, dict) else None
    result["success_representation"] = "boolean_true" if success is True else "string_true" if success == "true" else "other"
    return result


def resolution_record(case, current, *, state, source_commit):
    return {"schema_version": 2, "case": case["label"], "kind": case["kind"], "state": state,
            "evidence_key": EVIDENCE_KEY, "evidence_sha256": EVIDENCE_SHA, "source_commit": source_commit,
            "native_evidence_key": NATIVE_EVIDENCE_KEY, "native_evidence_sha256": NATIVE_EVIDENCE_SHA,
            "identity_evidence_sha256": IDENTITY_SHA, "native_preinvoice_key": case["native_preinvoice_key"],
            "original_record": deepcopy(current), "original_outcome": "unconfirmed",
            "order_id": case["order_id"], "preinvoice_id": case["preinvoice_id"],
            "recorded_at": datetime.now(timezone.utc).isoformat()}


def complete_document(journal, case, current, resolution):
    invoice = current["invoices"][0]
    resolution = {**resolution, "state": "verified", "invoice_id": str(invoice["id"]), "invoice_num": invoice["invoice_num"]}
    journal.assert_owned()
    journal.update_order(case["number"], phase="complete", email_policy="hold", email_state="held",
                         invoice_id=str(invoice["id"]), invoice_num=invoice["invoice_num"], **{RECONCILIATION: resolution})
    saved = prior_record(journal, case)
    require(saved.get("phase") == "complete" and saved.get("email_state") == "held"
            and saved.get(RECONCILIATION) == resolution, "document-journal-readback-mismatch")


def confirm_emails(journal, generator, cases, source_commit):
    prepared = []
    for case in cases:
        record = prior_record(journal, case)
        current = read_bound(generator, case)
        invoice = current["invoices"][0] if current["invoices"] else {}
        require(str(invoice.get("id")) == str(case["original"]["invoice_id"])
                and invoice.get("invoice_num") == case["original"]["invoice_num"], "confirmed-email-document-changed")
        existing = record.get(RECONCILIATION)
        if existing:
            require(existing.get("state") == "verified" and record.get("phase") == "complete"
                    and record.get("email_state") == "sent", "email-resolution-state-drift")
            continue
        resolution = resolution_record(case, record, state="verified", source_commit=source_commit)
        resolution.update(confirmed_by="provider_order_history", provider_send_events=case["history"]["invoice_email_events"],
                          original_outcome="provider_send_confirmed", recipient_delivery_confirmed=False)
        prepared.append((case, resolution))
    # Validate both first. A partial CAS failure is safely resumable: no email
    # operation exists in this path and an already verified row is never resent.
    for case, resolution in prepared:
        prior_record(journal, case)
        current = read_bound(generator, case)
        require(current["invoices"] and str(current["invoices"][0]["id"]) == str(case["original"]["invoice_id"])
                and current["invoices"][0]["invoice_num"] == case["original"]["invoice_num"], "confirmed-email-document-changed")
        journal.assert_owned()
        journal.update_order(case["number"], phase="complete", email_state="sent", **{RECONCILIATION: resolution})
        saved = prior_record(journal, case)
        require(saved.get("phase") == "complete" and saved.get("email_state") == "sent"
                and saved.get(RECONCILIATION) == resolution, "email-journal-readback-mismatch")
    return {"ok": True, "confirmed_emails": len(prepared), "finalization_requests": 0, "final_documents_verified": 0}


def finalize_one(journal, generator, cases, source_commit, release_gate):
    selected = None
    for case in cases:
        record = prior_record(journal, case)
        resolution = record.get(RECONCILIATION)
        if resolution and resolution.get("state") == "verified":
            require(record.get("phase") == "complete" and record.get("email_policy") == "hold"
                    and record.get("email_state") == "held", "completed-migration-state-drift")
            current = read_bound(generator, case)
            require(current["invoices"] and current["invoices"][0]["invoice_num"] == record.get("invoice_num"),
                    "completed-document-readback-mismatch")
            continue
        selected = (case, record)
        break
    if selected is None:
        return {"ok": True, "finalization_requests": 0, "final_documents_verified": 0, "remaining_cases": 0}
    case, record = selected
    current = read_bound(generator, case)
    resolution = record.get(RECONCILIATION) or resolution_record(case, record, state="readback_only", source_commit=source_commit)
    if current["invoices"]:
        complete_document(journal, case, current, resolution)
        return {"ok": True, "finalization_requests": 0, "final_documents_verified": 1}
    require(not record.get(RECONCILIATION), "previous-intent-consumed-no-replay")
    require(record.get("phase") == "create_ambiguous" and record.get("email_state") is None, "creation-operation-state-drift")
    require_eligible(generator, current)
    web = generator.web_session
    require(web is not None and generator.validate_session() and bool(generator.arf_token), "native-session-unverified")
    require(all(web.get_adapter(prefix).max_retries.total == 0 for prefix in ("https://", "http://")), "native-write-retries-enabled")
    latest = read_bound(generator, case)
    if latest["invoices"]:
        complete_document(journal, case, latest, resolution)
        return {"ok": True, "finalization_requests": 0, "final_documents_verified": 1}
    require_eligible(generator, latest)
    require(latest["status"]["id"] == current["status"]["id"] and latest["sum"] == current["sum"], "eligibility-changed-during-review")
    release_gate()
    prior_record(journal, case)
    journal.assert_owned()
    resolution.update(state="attempt_started", attempt_id=uuid4().hex, email_policy_before=record.get("email_policy"),
                      attempted_at=datetime.now(timezone.utc).isoformat(), preparation_forbidden=True, email_forbidden=True)
    # Keep original attempted_at and create_ambiguous; ordinary writers cannot
    # replay this intent, and the durable hold also protects crash recovery.
    journal.update_order(case["number"], email_policy="hold", **{RECONCILIATION: resolution})
    final = read_bound(generator, case)
    if final["invoices"]:
        complete_document(journal, case, final, resolution)
        return {"ok": True, "finalization_requests": 0, "final_documents_verified": 1}
    require_eligible(generator, final)
    require(final["status"]["id"] == latest["status"]["id"] and final["sum"] == latest["sum"], "eligibility-changed-after-intent")
    journal.assert_owned()
    try:
        response = web.get(f"{generator.base_url}/erp/orders/invoices/finalize/{case['native_preinvoice_key']}",
                           params={"arf": generator.arf_token}, allow_redirects=False, timeout=(10, 30),
                           headers={"X-Requested-With": "XMLHttpRequest", "Accept": "application/json",
                                    "Referer": f"{generator.base_url}/erp/orders/orders/detail/{case['order_id']}"})
        resolution["response"] = response_fingerprint(response)
    except AutomationStateError:
        raise
    except Exception as error:
        resolution["response"] = {"exception_type": type(error).__name__}
    journal.assert_owned()
    resolution["state"] = "uncertain"
    journal.update_order(case["number"], **{RECONCILIATION: resolution})
    current = read_bound(generator, case)
    if current["invoices"]:
        complete_document(journal, case, current, resolution)
        return {"ok": True, "finalization_requests": 1, "final_documents_verified": 1}
    return {"ok": False, "finalization_requests": 1, "final_documents_verified": 0, "uncertain_operations": 1}


def reconcile(*, store, cases, action, generator_factory, apply=False, source_commit="", release_gate=None):
    require(action in {"confirm-emails", "finalize-one"}, "unsupported-reconciliation-action")
    selected = [case for case in cases if case["kind"] == ("email" if action == "confirm-emails" else "finalize")]
    require(len(selected) == (2 if action == "confirm-emails" else 4), "reviewed-action-count-mismatch")
    selected = sorted(selected, key=lambda case: case["number"])
    if not apply:
        state, before_etag = store.read()
        validate_state(state)
        records = [validate_record(state["orders"].get(case["number"], {}), case) for case in selected]
        if state.get("lease"):
            return {"ok": False, "dry_run": True, "reviewed_cases": len(selected), "emailed_invoices": 0,
                    "finalization_requests": 0, "blocked_reason": "journal_lease_present"}
        generator = generator_factory(web_login=True)
        try:
            require(generator.operation_journal is None, "preview-opened-journal-writer")
            generator.send_invoice_email_enabled = False
            generator.resolve_eligible_status_ids()
            current = [read_bound(generator, case) for case in selected]
            blocked, pending = None, 0
            for case, record, order in zip(selected, records, current):
                resolution = record.get(RECONCILIATION)
                if resolution and resolution.get("state") == "verified":
                    require(record.get("phase") == "complete" and order["invoices"], "completed-migration-state-drift")
                    require(record.get("email_state") == ("sent" if action == "confirm-emails" else "held"),
                            "completed-email-state-drift")
                    require(record.get("invoice_id") == str(order["invoices"][0]["id"])
                            and record.get("invoice_num") == order["invoices"][0]["invoice_num"], "completed-document-state-drift")
                    continue
                pending += 1
                if action == "confirm-emails":
                    require(order["invoices"] and order["invoices"][0]["invoice_num"] == case["original"]["invoice_num"],
                            "confirmed-email-document-changed")
                elif not order["invoices"]:
                    require_eligible(generator, order)
                    if resolution:
                        blocked = "previous_intent_consumed"
                    else:
                        require(record.get("phase") == "create_ambiguous", "creation-operation-state-drift")
            after, after_etag = store.read()
            require(before_etag == after_etag and state == after, "journal-changed-during-preview")
            return {"ok": blocked is None, "dry_run": True, "reviewed_cases": len(selected), "emailed_invoices": 0,
                    "pending_cases": pending, "blocked_reason": blocked,
                    "finalization_requests": 0, "final_documents_present": sum(bool(row["invoices"]) for row in current)}
        finally:
            try:
                if generator.web_session:
                    generator.web_session.close()
            finally:
                generator.client.transport.close()
    require(callable(release_gate), "verified-release-gate-required")
    release_gate()
    with store.lease(owner="reviewed-invoice-reconciliation") as journal:
        validate_state(journal.snapshot())
        release_gate()
        generator = generator_factory(web_login=True)
        try:
            generator.operation_journal = journal
            generator.send_invoice_email_enabled = False
            generator.resolve_eligible_status_ids()
            result = (confirm_emails(journal, generator, selected, source_commit) if action == "confirm-emails"
                      else finalize_one(journal, generator, selected, source_commit, release_gate))
            return {**result, "dry_run": False, "reviewed_cases": len(selected), "emailed_invoices": 0}
        finally:
            try:
                if generator.web_session:
                    generator.web_session.close()
            finally:
                generator.client.transport.close()


def require_source(root):
    subprocess.run(["git", "ls-files", "--error-unmatch", "scripts/reconcile_reviewed_invoice_outcomes.py",
                    "tests/test_reviewed_invoice_reconciliation.py"], cwd=root, check=True, capture_output=True)
    branch = subprocess.check_output(["git", "branch", "--show-current"], cwd=root, text=True).strip()
    require(branch.startswith("codex/"), "reviewed-codex-source-required")
    return require_pushed_source(root)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("confirm-emails", "finalize-one"))
    parser.add_argument("--profile", default="codex")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expected-image-digest")
    parser.add_argument("--expected-source-commit")
    parser.add_argument("--deployment-evidence-key")
    args = parser.parse_args(argv)
    if args.apply and not all((args.expected_image_digest, args.expected_source_commit, args.deployment_evidence_key)):
        parser.error("Apply requires promoted image, source commit and deployment evidence key")
    import logging
    logging.disable(logging.CRITICAL)
    import boto3
    from botocore.config import Config
    from generate_invoices import InvoiceGenerator, resolve_invoice_generation_settings
    from reporting_core import derive_biznisweb_base_url
    root = Path(__file__).resolve().parents[1]
    commit = require_source(root)
    session = boto3.Session(profile_name=args.profile, region_name="eu-central-1")
    require(session.client("sts").get_caller_identity()["Account"] == ACCOUNT, "aws-account-mismatch")
    s3 = session.client("s3", config=Config(connect_timeout=10, read_timeout=30, retries={"total_max_attempts": 1}))
    privacy = s3.get_public_access_block(Bucket=BUCKET, ExpectedBucketOwner=ACCOUNT)["PublicAccessBlockConfiguration"]
    require(all(privacy.get(key) is True for key in ("BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets")),
            "private-bucket-access-unverified")
    cases = load_cases(s3)
    secret = json.loads(session.client("secretsmanager").get_secret_value(SecretId="roy/reporting/runtime-env")["SecretString"])
    settings = json.loads((root / "projects" / "roy" / "settings.json").read_text(encoding="utf-8"))
    configured = resolve_invoice_generation_settings(settings)
    require(configured["enabled"] and configured["safety_state_enabled"], "invoice-safety-automation-disabled")
    store = S3AutomationStateStore(s3, BUCKET, "data/roy/order-automation/state.json", "roy")
    with runtime_environment("roy", secret, settings):
        def factory(*, web_login):
            url = secret["BIZNISWEB_API_URL"]
            return InvoiceGenerator(url, secret["BIZNISWEB_API_TOKEN"], derive_biznisweb_base_url(url),
                username=secret["BIZNISWEB_USERNAME"] if web_login else None,
                password=secret["BIZNISWEB_PASSWORD"] if web_login else None, send_invoice_email=False,
                eligible_statuses=configured["eligible_statuses"], exclude_zero_total_orders=True,
                page_delay_seconds=configured["page_delay_seconds"], read_attempts=configured["read_attempts"])

        result = reconcile(store=store, cases=cases, action=args.action, generator_factory=factory, apply=args.apply,
                           source_commit=commit, release_gate=lambda: verify_release(session, digest=args.expected_image_digest,
                               commit=args.expected_source_commit, evidence_key=args.deployment_evidence_key))
    print(json.dumps(result, sort_keys=True), flush=True)
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(json.dumps({"ok": False, "error_type": type(error).__name__,
                          "message": "Reconciliation stopped; preserve the journal and inspect the private evidence before any further attempt"}))
        raise SystemExit(1) from None
