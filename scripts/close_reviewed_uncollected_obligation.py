#!/usr/bin/env python3
"""Close the single sealed uncollected COD obligation with one silent status write.

Preview is read-only. Any durable intent permits only readback on subsequent
runs, even if the process stopped before transmission. The original uncertain
preparation, document fields and email policy are never changed. No invoice,
receipt, refund or email request exists in this helper.

The explicit user confirmation is essential: no API document means no linked
receipt evidence, not proof that every possible external payment is absent.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack, closing, contextmanager
from copy import deepcopy
from datetime import datetime
from decimal import Decimal
import ipaddress
import json
from pathlib import Path
import re
import subprocess
import sys
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import reviewed_invoice_obligations as obligations
from invoice_automation_state import AutomationStateError, S3AutomationStateStore, parse_utc, resolve_automation_state_location
from order_status_safety import assess_fulfillment_evidence, assess_payment_evidence, change_status_verified
from scripts.deploy_order_automations import SCHEDULES, command_for, schedule_request
from scripts.reconcile_reviewed_invoice_outcomes import private_json
from scripts.restore_verified_fulfillment import runtime_environment
from scripts.verify_invoice_discovery import require_pushed_source
from unpaid_order_cancellation import LIST_ORDER_STATUSES_QUERY

ACCOUNT = "919341186960"
BUCKET = "biznisweb-reporting-artifacts-919341186960-eu-central-1"
REGION = "eu-central-1"
CLUSTER = "vevo-reporting-cluster"
# Pin only after an independently verified deployment containing these guards.
REVIEWED_RELEASE_COMMIT = ""
REVIEWED_RELEASE_DIGEST = ""
RECEIPT_CONTRACT_KEY = "data/vevo/order-automation/audits/2026-09-13/uncollected-native-zero-receipts-contract-20260913.json"
RECEIPT_CONTRACT_SHA256 = "2104d03fe34ccaac3dd3a3c280a0e807179a959b12c9198ab1e17471613c75db"


class ClosureBlocked(RuntimeError):
    """A fixed, non-sensitive validation failure."""


def require(condition, code):
    if not condition:
        raise ClosureBlocked(code)


def status_identity(value):
    require(not isinstance(value, bool) and isinstance(value, (str, int))
            and re.fullmatch(r"[1-9][0-9]*", str(value)) is not None, "invalid-status-identity")
    return str(value)


def load_confirmation(s3):
    require(bool(obligations.CONFIRMATION_KEY) and bool(obligations.CONFIRMATION_SHA256), "confirmation-unpinned")
    document = private_json(s3, obligations.CONFIRMATION_KEY, expected_sha=obligations.CONFIRMATION_SHA256)
    require(obligations.validate_confirmation(document), "confirmation-invalid")
    for ref in document["evidence_refs"]:
        require(isinstance(private_json(s3, ref["key"], expected_sha=ref["sha256"]), dict), "supporting-evidence-invalid")
    require(bool(RECEIPT_CONTRACT_KEY) and bool(RECEIPT_CONTRACT_SHA256), "native-receipt-contract-unpinned")
    receipt = private_json(s3, RECEIPT_CONTRACT_KEY, expected_sha=RECEIPT_CONTRACT_SHA256)
    require(isinstance(receipt, dict) and receipt.get("read_only") is True and receipt.get("project") == "vevo"
            and receipt.get("order_id") == document["order_id"] and receipt.get("order_num") == document["order_num"]
            and all(type(receipt.get(key)) is int and receipt[key] == 0
                    for key in ("financial_requests", "status_writes", "journal_mutations")), "native-receipt-contract-invalid")
    native = receipt.get("native_receipts", {})
    require(native.get("method") == "POST" and native.get("parameters") == ["arf"]
            and native.get("route") == f"/erp/orders/receipts/getListByOrderJson/{document['order_id']}"
            and native.get("response") == {"rows": []}, "native-receipt-contract-changed")
    return document


def require_private_bucket(s3):
    args = {"Bucket": BUCKET, "ExpectedBucketOwner": ACCOUNT}
    s3.head_bucket(**args)
    require(s3.get_bucket_location(**args).get("LocationConstraint") == REGION, "private-bucket-region-changed")
    block = s3.get_public_access_block(**args).get("PublicAccessBlockConfiguration", {})
    require(all(block.get(key) is True for key in (
        "BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets")), "private-bucket-access-unverified")
    try:
        require(s3.get_bucket_policy_status(**args).get("PolicyStatus", {}).get("IsPublic") is False,
                "private-bucket-policy-unverified")
    except Exception as error:
        if getattr(error, "response", {}).get("Error", {}).get("Code") != "NoSuchBucketPolicy":
            raise
    acl = s3.get_bucket_acl(**args)
    owner, grants = acl.get("Owner", {}).get("ID"), acl.get("Grants")
    require(bool(owner) and isinstance(grants, list) and bool(grants) and all(
        isinstance(row, dict) and row.get("Grantee", {}).get("Type") == "CanonicalUser"
        and row["Grantee"].get("ID") == owner and row.get("Permission") == "FULL_CONTROL"
        for row in grants), "private-bucket-acl-unverified")


def validate_state(state):
    require(isinstance(state, dict) and state.get("project") == "vevo"
            and type(state.get("schema_version")) is int and state["schema_version"] == 1
            and isinstance(state.get("orders"), dict), "journal-project-or-schema-mismatch")


def validate_prior(record, document):
    require(isinstance(record, dict) and obligations.financial_projection(record)
            == document["original_financial_operation"], "original-financial-operation-changed")
    require(record.get("manual_settlement") is None, "manual-settlement-conflicts-with-uncollected-review")
    if obligations.has_reviewed_closure(record):
        decision = obligations.closure_decision(record, project="vevo", order_num=document["order_num"])
        require(decision.reason != "reviewed_closure_invalid", "existing-closure-invalid")
        require(record[obligations.CLOSURE_FIELD].get("receipt_contract") == {
            "key": RECEIPT_CONTRACT_KEY, "sha256": RECEIPT_CONTRACT_SHA256}, "existing-receipt-contract-changed")
    else:
        require(record.get("status_mutation") is None, "previous-status-operation-requires-review")
    return record


def resolve_target(generator):
    result = generator.execute_read(LIST_ORDER_STATUSES_QUERY, {"lang_code": "SK"})
    rows = result.get("listOrderStatuses") if isinstance(result, dict) else None
    require(isinstance(rows, list) and rows and all(isinstance(row, dict) for row in rows), "status-catalogue-incomplete")
    statuses = [{"id": status_identity(row.get("id")), "name": row.get("name")} for row in rows]
    require(all(isinstance(row["name"], str) and row["name"].strip() for row in statuses)
            and len({row["id"] for row in statuses}) == len(statuses), "status-catalogue-invalid")
    candidates = [row for row in statuses if obligations.normalized_status_name(row["name"]) == "neprevzate storno"]
    if not candidates:
        candidates = [row for row in statuses if obligations.normalized_status_name(row["name"]) == "storno"]
    require(len(candidates) == 1, "noncollection-target-not-unique")
    return candidates[0]


def require_transports(generator):
    require(getattr(generator.client.transport, "retries", None) == 0, "api-mutation-retries-enabled")
    generator._require_native_token()
    require(all(generator.web_session.get_adapter(prefix).max_retries.total == 0
                for prefix in ("http://", "https://")), "native-retries-enabled")


def read_native_empty(generator, document):
    """One served native invoice-grid read; neither preparation nor finalization."""
    from generate_invoices import _parse_native_response_object
    generator._require_native_token()
    response = generator.web_session.post(
        f"{generator.base_url}/erp/orders/invoices/getListJson",
        data={"find": f"o#{document['order_num']}", "start": 0, "limit": 20, "arf": generator.arf_token},
        headers={"X-Requested-With": "XMLHttpRequest", "Accept": "application/json",
                 "Referer": f"{generator.base_url}/erp/main/orders"},
        allow_redirects=False, timeout=(10, 30),
    )
    try:
        require(response.status_code == 200, "native-document-read-failed")
        require(isinstance(response.text, str) and len(response.text.encode("utf-8")) <= 65536,
                "native-document-response-too-large")
        payload = _parse_native_response_object(response.text)
        require(("success" not in payload or payload["success"] is True or payload["success"] == "true")
                and not payload.get("errors") and type(payload.get("total")) in (str, int)
                and str(payload["total"]) == "0" and payload.get("rows") == [], "native-document-context-not-empty")
    finally:
        response.close()


def validate_order(order, document, allowed_statuses):
    require(isinstance(order, dict) and status_identity(order.get("id")) == document["order_id"]
            and order.get("order_num") == document["order_num"] and order.get("blocked") is False,
            "current-order-identity-or-block-changed")
    status = order.get("status")
    require(isinstance(status, dict), "current-status-missing")
    actual = {"id": status_identity(status.get("id")), "name": status.get("name")}
    require(actual in allowed_statuses, "current-status-outside-reviewed-scope")
    total = order.get("sum")
    require(isinstance(total, dict) and not isinstance(total.get("value"), bool)
            and isinstance(total.get("value"), (str, int, float))
            and isinstance(total.get("currency"), dict) and total["currency"].get("code") == "EUR"
            and Decimal(str(total["value"])).is_finite()
            and Decimal(str(total["value"])) == Decimal(document["total"]["value"]), "current-total-changed")
    require(all(key in order and (order[key] is None or order[key] == []) for key in ("invoices", "preinvoices")),
            "current-document-context-not-empty")
    payment = assess_payment_evidence(order)
    require(payment.state == "unpaid" and payment.reason == "no_settlement_evidence", "current-payment-context-conflicts")
    shipments = order.get("shipments")
    require("shipments" in order and (shipments is None or isinstance(shipments, list)), "current-shipment-context-missing")
    require(all(isinstance(row, dict) and "status" in row and "shipment_number" in row
                and (row["status"] is None or isinstance(row["status"], str))
                and (row["shipment_number"] is None or isinstance(row["shipment_number"], str))
                and str(row["status"] or "").strip().lower() != "delivered" for row in shipments or []),
            "current-shipment-context-conflicts")
    fulfillment = assess_fulfillment_evidence(order)
    require(fulfillment.state in {"none", "exception"} or fulfillment.reason == "shipment_not_proven_delivered",
            "current-fulfillment-unverified")
    elements = order.get("price_elements")
    require(isinstance(elements, list) and all(isinstance(row, dict) for row in elements), "current-payment-method-missing")
    payments = [row for row in elements if row.get("type") == "payment"]
    require(len(payments) == 1 and status_identity(payments[0].get("reference_id")) == document["payment_reference_id"],
            "current-cod-payment-method-changed")
    require(isinstance(order.get("last_change"), str), "current-status-generation-missing")
    datetime.fromisoformat(order["last_change"])
    return actual


def read_native_receipts_empty(generator, document):
    """Observed readonly POST returns precisely {rows: []} for no receipts."""
    from generate_invoices import _parse_native_response_object
    generator._require_native_token()
    response = generator.web_session.post(
        f"{generator.base_url}/erp/orders/receipts/getListByOrderJson/{document['order_id']}",
        data={"arf": generator.arf_token},
        headers={"X-Requested-With": "XMLHttpRequest", "Accept": "application/json",
                 "Referer": f"{generator.base_url}/erp/main/orders"},
        allow_redirects=False, timeout=(10, 30),
    )
    try:
        require(response.status_code == 200 and isinstance(response.text, str)
                and len(response.text.encode("utf-8")) <= 65536, "native-receipt-read-failed")
        require(_parse_native_response_object(response.text) == {"rows": []}, "native-receipts-not-proved-empty")
    finally:
        response.close()


def read_bound(generator, document, allowed_statuses):
    order = generator.fetch_order_safety_context(document["order_num"])
    validate_order(order, document, allowed_statuses)
    read_native_empty(generator, document)
    read_native_receipts_empty(generator, document)
    return order


def initial_marker(record, document, target, source_commit, now):
    require(re.fullmatch(r"[0-9a-f]{40}", source_commit or "") is not None, "closure-source-commit-invalid")
    stamp = now.isoformat()
    mutation = {"schema_version": 1, "intent_id": uuid4().hex, "state": "intent", "silent": True,
                "attempted_at": stamp, "source_status": deepcopy(document["source_status"]),
                "target_status": deepcopy(target)}
    return {
        "schema_version": 1, "state": "intent", "invoice_obligation": "closing", "financial_outcome": "unknown",
        "source_commit": source_commit,
        "receipt_contract": {"key": RECEIPT_CONTRACT_KEY, "sha256": RECEIPT_CONTRACT_SHA256},
        "confirmation": {"key": obligations.CONFIRMATION_KEY, "sha256": obligations.CONFIRMATION_SHA256,
                         "document": deepcopy(document)},
        "original_financial_sha256": obligations.canonical_sha256(obligations.financial_projection(record)),
        "status_mutation": mutation,
    }


def persist(journal, document, marker, *, close_related_review=False):
    current = journal.get_order(document["order_num"])
    validate_prior(current, document)
    candidate = {**current, "status_mutation": deepcopy(marker["status_mutation"]),
                 obligations.CLOSURE_FIELD: deepcopy(marker)}
    require(obligations.closure_decision(candidate, project="vevo", order_num=document["order_num"]).reason
            != "reviewed_closure_invalid", "proposed-closure-marker-invalid")
    journal.assert_owned()
    fields = {"status_mutation": deepcopy(marker["status_mutation"]), obligations.CLOSURE_FIELD: deepcopy(marker)}
    review = current.get("status_review")
    if close_related_review and isinstance(review, dict) and review.get("state") == "open" and review.get("reason") == "reviewed_closure_unconfirmed":
        fields["status_review"] = {**review, "state": "closed", "reason": "reviewed_obligation_closed"}
    if close_related_review and current.get("status_review_reason") == "reviewed_closure_unconfirmed":
        fields["status_review_reason"] = None
    journal.update_order(document["order_num"], **fields)
    saved = validate_prior(journal.get_order(document["order_num"]), document)
    require(saved.get(obligations.CLOSURE_FIELD) == marker, "closure-journal-readback-differs")


def complete(journal, document, marker):
    marker = {**deepcopy(marker), "state": "verified", "invoice_obligation": "closed",
              "verified_at": journal.store.now().isoformat()}
    marker["status_mutation"]["state"] = "verified"
    persist(journal, document, marker, close_related_review=True)
    return marker


def close_obligation(*, store, document, generator_factory, apply=False, source_commit="", release_gate=None):
    require(obligations.validate_confirmation(document), "confirmation-invalid")
    require(store.project == "vevo", "journal-project-mismatch")
    if apply:
        require(callable(release_gate), "verified-release-gate-required")
        release_gate()
    state, etag = store.read()
    validate_state(state)
    require(not state.get("lease"), "closure-journal-is-leased")
    record = validate_prior(state["orders"].get(document["order_num"]), document)
    generator = generator_factory()
    try:
        require_transports(generator)
        target = resolve_target(generator)
        require(target["id"] != document["source_status"]["id"], "closure-target-equals-source")
        marker = record.get(obligations.CLOSURE_FIELD)
        if marker:
            require(marker["status_mutation"]["target_status"] == target, "closure-target-catalogue-changed")
        allowed = [document["source_status"], target] if marker else [document["source_status"]]
        current = read_bound(generator, document, allowed)
        if not apply:
            after, after_etag = store.read()
            require(after == state and after_etag == etag, "preview-journal-changed")
            at_target = str(current["status"]["id"]) == target["id"]
            durable_closed = obligations.closure_decision(record, project="vevo", order_num=document["order_num"]).closed
            return {"ok": not marker or at_target, "dry_run": True, "status_requests": 0,
                    "closed_obligations": int(durable_closed), "readback_ready": bool(marker) and at_target,
                    "consumed_intent": bool(marker),
                    "financial_requests": 0, "emailed_invoices": 0}
        with store.lease(owner="reviewed-uncollected-closure") as journal:
            generator.operation_journal = journal
            snapshot = journal.snapshot()
            validate_state(snapshot)
            record = validate_prior(snapshot["orders"].get(document["order_num"]), document)
            marker = record.get(obligations.CLOSURE_FIELD)
            if marker:
                require(marker["status_mutation"]["target_status"] == target, "closure-target-catalogue-changed")
                current = read_bound(generator, document, [document["source_status"], target])
                at_target = str(current["status"]["id"]) == target["id"]
                if at_target and marker["state"] != "verified":
                    complete(journal, document, marker)
                durable_closed = marker["state"] == "verified" or at_target
                return {"ok": at_target, "dry_run": False, "status_requests": 0,
                        "closed_obligations": int(durable_closed), "regression": not at_target,
                        "consumed_intent": True,
                        "financial_requests": 0, "emailed_invoices": 0}
            release_gate()
            current = read_bound(generator, document, [document["source_status"]])
            latest = read_bound(generator, document, [document["source_status"]])
            require(latest == current, "current-context-changed-before-intent")
            marker = initial_marker(record, document, target, source_commit, store.now())
            persist(journal, document, marker)
            # A crash from this point permanently forbids another status request.
            final = read_bound(generator, document, [document["source_status"], target])
            if str(final["status"]["id"]) == target["id"]:
                complete(journal, document, marker)
                return {"ok": True, "dry_run": False, "status_requests": 0, "closed_obligations": 1,
                        "consumed_intent": True, "financial_requests": 0, "emailed_invoices": 0}
            require(final == latest, "current-context-changed-after-intent")
            release_gate()
            journal.assert_owned()
            marker = deepcopy(marker)
            marker["state"] = marker["status_mutation"]["state"] = "uncertain"
            try:
                change_status_verified(generator.client, document["order_num"], int(target["id"]), target["name"],
                                       silent=True, progress_callback=journal.assert_owned)
            except AutomationStateError:
                raise
            except Exception:
                pass  # Only fresh readback can resolve a consumed status effect.
            persist(journal, document, marker)
            current = read_bound(generator, document, [document["source_status"], target])
            at_target = str(current["status"]["id"]) == target["id"]
            if at_target:
                complete(journal, document, marker)
            return {"ok": at_target, "dry_run": False, "status_requests": 1,
                    "closed_obligations": int(at_target), "consumed_intent": True,
                    "financial_requests": 0, "emailed_invoices": 0}
    finally:
        generator.operation_journal = None
        try:
            if generator.web_session:
                generator.web_session.close()
        finally:
            generator.client.transport.close()


def require_source(root):
    require(subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=all"],
                                    cwd=root, text=True).strip() == "", "closure-source-not-clean")
    require(subprocess.check_output(["git", "remote", "get-url", "origin"], cwd=root, text=True).strip()
            in {"https://github.com/vzeman/biznisweb.git", "git@github.com:vzeman/biznisweb.git"}, "closure-source-repository-changed")
    require(subprocess.check_output(["git", "branch", "--show-current"], cwd=root, text=True).strip().startswith("codex/"),
            "reviewed-codex-source-required")
    subprocess.run(["git", "ls-files", "--error-unmatch", "scripts/close_reviewed_uncollected_obligation.py",
                    "tests/test_reviewed_uncollected_closure.py", "reviewed_invoice_obligations.py"],
                   cwd=root, check=True, capture_output=True)
    head = require_pushed_source(root)
    branch = subprocess.check_output(["git", "branch", "--show-current"], cwd=root, text=True).strip()
    require(subprocess.check_output(["git", "ls-remote", "--exit-code", "origin", f"refs/heads/{branch}"],
                                    cwd=root, text=True).split() == [head, f"refs/heads/{branch}"], "source-remote-changed")
    return head


@contextmanager
def bounded_aws_clients(session, config):
    """Cache bounded clients and close every one, including release-gate clients."""
    cache = {}
    with ExitStack() as stack:
        class Clients:
            def client(self, name):
                if name not in cache:
                    cache[name] = stack.enter_context(closing(session.client(name, config=config)))
                return cache[name]
        yield Clients()


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
                raise ClosureBlocked("active-task-pagination-incomplete")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default="codex")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expected-source-commit")
    parser.add_argument("--expected-image-digest")
    parser.add_argument("--deployment-evidence-key")
    args = parser.parse_args(argv)
    if args.apply:
        require(bool(REVIEWED_RELEASE_COMMIT) and bool(REVIEWED_RELEASE_DIGEST), "compatible-release-not-pinned")
    import boto3
    from botocore.config import Config
    from generate_invoices import InvoiceGenerator
    from reporting_core import derive_biznisweb_base_url
    root = Path(__file__).resolve().parents[1]
    source = require_source(root)
    aws_session = boto3.Session(profile_name=args.profile, region_name=REGION)
    config = Config(connect_timeout=10, read_timeout=30, retries={"total_max_attempts": 1})
    with bounded_aws_clients(aws_session, config) as session:
        require(session.client("sts").get_caller_identity()["Account"] == ACCOUNT, "aws-account-mismatch")
        s3 = session.client("s3")
        require_private_bucket(s3)
        document = load_confirmation(s3)
        secret = json.loads(session.client("secretsmanager").get_secret_value(
            SecretId="vevo/reporting/runtime-env")["SecretString"])
        settings = json.loads((root / "projects" / "vevo" / "settings.json").read_text(encoding="utf-8"))
        require(resolve_automation_state_location("vevo", settings, secret) == (BUCKET, "data/vevo/order-automation/state.json"),
                "journal-destination-changed")
        store = S3AutomationStateStore(s3, BUCKET, "data/vevo/order-automation/state.json", "vevo")
        with runtime_environment("vevo", secret, settings):
            url = secret["BIZNISWEB_API_URL"]
            require(url == settings["biznisweb_api_url"], "runtime-api-project-changed")
            report = close_obligation(store=store, document=document, apply=args.apply, source_commit=source,
                generator_factory=lambda: InvoiceGenerator(url, secret["BIZNISWEB_API_TOKEN"], derive_biznisweb_base_url(url),
                    username=secret["BIZNISWEB_USERNAME"], password=secret["BIZNISWEB_PASSWORD"], send_invoice_email=False),
                release_gate=lambda: verify_release(session, digest=args.expected_image_digest,
                    commit=args.expected_source_commit, evidence_key=args.deployment_evidence_key))
    print(json.dumps(report, sort_keys=True), flush=True)
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    import logging
    logging.disable(logging.CRITICAL)
    try:
        raise SystemExit(main())
    except Exception as error:
        print(json.dumps({"ok": False, "error_type": type(error).__name__, "outcome": "inspect_journal_before_further_action"}), flush=True)
        raise SystemExit(1) from None
