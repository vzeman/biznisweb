#!/usr/bin/env python3
"""Record or revoke one explicit private settlement proof; no provider writes.

Default is read-only. --apply changes only provenance in the leased S3 journal.
It never marks an invoice paid, creates a receipt, sends mail or changes status.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager, closing, ExitStack
import hashlib
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from invoice_automation_state import S3AutomationStateStore, resolve_automation_state_location
from manual_settlement import canonical_bytes, proof_reference, uncertainty_reason, updated_record
from order_status_safety import assess_fulfillment_evidence, assess_payment_evidence, fetch_order_safety_context

ACCOUNT = "919341186960"
REGION = "eu-central-1"
BUCKET = "biznisweb-reporting-artifacts-919341186960-eu-central-1"


def require_source(root: Path) -> str:
    def git(*args):
        return subprocess.check_output(["git", *args], cwd=root, text=True).strip()
    if (Path(git("rev-parse", "--show-toplevel")).resolve() != root.resolve()
            or git("remote", "get-url", "origin") not in {
                "https://github.com/vzeman/biznisweb.git", "git@github.com:vzeman/biznisweb.git"}
            or git("status", "--porcelain", "--untracked-files=all")
            or not git("branch", "--show-current").startswith("codex/")):
        raise ValueError("manual_settlement_source_not_clean_owned_branch")
    git("ls-files", "--error-unmatch", "scripts/record_verified_manual_settlement.py")
    head = git("rev-parse", "HEAD")
    branch = git("branch", "--show-current")
    if (git("rev-parse", "--abbrev-ref", "@{upstream}") != f"origin/{branch}"
            or head != git("rev-parse", "@{upstream}")
            or git("ls-remote", "--exit-code", "origin", f"refs/heads/{branch}").split() != [head, f"refs/heads/{branch}"]):
        raise ValueError("manual_settlement_source_not_pushed")
    return head


def require_private_bucket(s3) -> None:
    kwargs = {"Bucket": BUCKET, "ExpectedBucketOwner": ACCOUNT}
    s3.head_bucket(**kwargs)
    if s3.get_bucket_location(**kwargs).get("LocationConstraint") != REGION:
        raise ValueError("manual_settlement_bucket_region_changed")
    block = s3.get_public_access_block(**kwargs).get("PublicAccessBlockConfiguration", {})
    if any(block.get(key) is not True for key in (
            "BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets")):
        raise ValueError("manual_settlement_bucket_not_private")
    try:
        if s3.get_bucket_policy_status(**kwargs).get("PolicyStatus", {}).get("IsPublic") is not False:
            raise ValueError("manual_settlement_bucket_policy_not_private")
    except Exception as exc:
        if getattr(exc, "response", {}).get("Error", {}).get("Code") != "NoSuchBucketPolicy":
            raise
    acl = s3.get_bucket_acl(**kwargs)
    owner = acl.get("Owner", {}).get("ID")
    grants = acl.get("Grants")
    if (not owner or not isinstance(grants, list) or not grants or any(
            row.get("Grantee", {}).get("Type") != "CanonicalUser"
            or row["Grantee"].get("ID") != owner or row.get("Permission") != "FULL_CONTROL" for row in grants)):
        raise ValueError("manual_settlement_bucket_acl_not_private")


def read_private(s3, key: str, digest: str, *, limit: int) -> bytes:
    response = s3.get_object(Bucket=BUCKET, Key=key, ExpectedBucketOwner=ACCOUNT)
    body = response["Body"]
    try:
        length = response.get("ContentLength")
        if (response.get("ServerSideEncryption") != "AES256" or type(length) is not int
                or not 0 < length <= limit):
            raise ValueError("manual_settlement_private_object_metadata_invalid")
        raw = body.read(limit + 1)
        if len(raw) != length or hashlib.sha256(raw).hexdigest() != digest:
            raise ValueError("manual_settlement_private_object_hash_changed")
        return raw
    finally:
        body.close()


def load_reference(s3, project: str, digest: str) -> dict:
    import re
    if re.fullmatch(r"[a-f0-9]{64}", digest) is None:
        raise ValueError("manual_settlement_hash_invalid")
    key = f"data/{project}/order-automation/manual-settlements/{digest}.json"
    raw = read_private(s3, key, digest, limit=65536)
    proof = json.loads(raw)
    reference = proof_reference(proof, key, digest)
    if raw != canonical_bytes(proof) or proof["project"] != project:
        raise ValueError("manual_settlement_proof_not_canonical")
    for ref in proof["supporting_evidence"]:
        if not isinstance(json.loads(read_private(s3, ref["key"], ref["sha256"], limit=2 * 1024 * 1024)), dict):
            raise ValueError("manual_settlement_supporting_evidence_not_object")
    return reference


@contextmanager
def api_environment(project: str, secret: dict, settings: dict):
    parsed = urlparse(secret.get("BIZNISWEB_API_URL", ""))
    if (parsed.scheme != "https" or parsed.hostname not in {f"{project}.flox.sk", f"{project}.sk", f"www.{project}.sk"}
            or parsed.path != "/api/graphql" or parsed.username or parsed.password or parsed.query or parsed.fragment
            or parsed.port not in (None, 443) or parsed.hostname != urlparse(settings["biznisweb_api_url"]).hostname
            or not isinstance(secret.get("BIZNISWEB_API_TOKEN"), str) or not secret["BIZNISWEB_API_TOKEN"].strip()):
        raise ValueError("manual_settlement_runtime_project_changed")
    values = {"REPORT_PROJECT": project, "REPORT_SKIP_PROJECT_ENV": "true", "BIZNISWEB_API_TIMEOUT_SEC": "30"}
    for key in ("BIZNISWEB_API_URL", "BIZNISWEB_API_TOKEN"):
        values[key] = values[f"{project.upper()}_{key}"] = secret[key]
    previous = {key: os.environ.get(key) for key in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def check_current(order: dict, record: dict, reference: dict, project: str) -> dict:
    candidate = updated_record(record.get("manual_settlement"), reference, project=project, order=order)
    if reference["proof"]["operation"] == "revoke":
        return candidate  # Withdrawing a claim cannot restore an order status.
    target = reference["proof"]["recovery_status"]
    status = order.get("status") or {}
    if (order.get("blocked") is not False or str(status.get("id")) != target["id"] or status.get("name") != target["name"]
            or uncertainty_reason(record) or (record.get("status_mutation") or {}).get("state") not in {None, "verified"}):
        raise ValueError("manual_settlement_current_status_or_operation_unverified")
    payment = assess_payment_evidence(order)
    if (payment.state != "confirmed" and payment.reason != "no_settlement_evidence"):
        raise ValueError("manual_settlement_native_payment_context_conflicts")
    if assess_fulfillment_evidence(order).state not in {"none", "fulfilled"}:
        raise ValueError("manual_settlement_fulfillment_context_conflicts")
    return candidate


def record_proof(*, store, reference: dict, project: str, read_order, apply: bool) -> dict:
    number = reference["proof"]["order_num"]
    if store.project != project:
        raise ValueError("manual_settlement_journal_project_changed")
    if apply:
        with store.lease(owner="record-verified-manual-settlement") as journal:
            order = read_order(number, progress_callback=journal.assert_owned)
            record = journal.get_order(number)
            candidate = check_current(order, record, reference, project)
            journal.assert_owned()
            journal.record_manual_settlement(order, reference)
            if journal.get_order(number).get("manual_settlement") != candidate:
                raise ValueError("manual_settlement_journal_readback_differs")
    else:
        state, etag = store.read()
        if state.get("lease"):
            raise ValueError("manual_settlement_preview_requires_free_lease")
        check_current(read_order(number), state["orders"].get(number, {}), reference, project)
        after, after_etag = store.read()
        if after_etag != etag or after != state:
            raise ValueError("manual_settlement_preview_journal_changed")
    return {"ok": True, "project": project, "applied": apply, "operation": reference["proof"]["operation"],
            "evidence_sha256": reference["evidence_sha256"], "provider_writes": 0}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True, choices=("roy", "vevo"))
    parser.add_argument("--proof-sha256", required=True)
    parser.add_argument("--profile", default="codex")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[1]
    require_source(root)
    import boto3
    from botocore.config import Config
    from unpaid_order_cancellation import build_client
    session = boto3.Session(profile_name=args.profile, region_name=REGION)
    config = Config(connect_timeout=10, read_timeout=30, retries={"total_max_attempts": 1})
    with ExitStack() as stack:
        sts = stack.enter_context(closing(session.client("sts", config=config)))
        if sts.get_caller_identity()["Account"] != ACCOUNT:
            raise ValueError("manual_settlement_aws_account_changed")
        s3 = stack.enter_context(closing(session.client("s3", config=config)))
        require_private_bucket(s3)
        reference = load_reference(s3, args.project, args.proof_sha256)
        secrets = stack.enter_context(closing(session.client("secretsmanager", config=config)))
        secret = json.loads(secrets.get_secret_value(SecretId=f"{args.project}/reporting/runtime-env")["SecretString"])
        settings = json.loads((root / "projects" / args.project / "settings.json").read_text(encoding="utf-8"))
        location = resolve_automation_state_location(args.project, settings, secret)
        if location != (BUCKET, f"data/{args.project}/order-automation/state.json"):
            raise ValueError("manual_settlement_journal_destination_changed")
        store = S3AutomationStateStore(s3, *location, args.project)
        with api_environment(args.project, secret, settings):
            client = build_client(args.project, settings)
            try:
                report = record_proof(store=store, reference=reference, project=args.project, apply=args.apply,
                                      read_order=lambda number, **kwargs: fetch_order_safety_context(client, number, **kwargs))
            finally:
                client.transport.close()
    print(json.dumps(report, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    logging.disable(logging.CRITICAL)
    try:
        raise SystemExit(main())
    except Exception as error:
        print(json.dumps({"ok": False, "error_type": type(error).__name__, "provider_writes": 0,
                          "journal_outcome": "inspect_before_retry"}), flush=True)
        raise SystemExit(1) from None
