#!/usr/bin/env python3
"""Prepare a verified VEVO report email, then send once under an immutable intent.

Preparation performs reads only. An intent without acceptance is uncertain and
requires manual reconciliation; this CLI never retries the SES call.
"""
from __future__ import annotations

import argparse
from contextlib import redirect_stdout
from datetime import date, datetime, timezone
from email import policy as email_policy
from email.parser import BytesParser
from email.utils import getaddresses
import importlib
import io
import json
import os
from pathlib import Path
import re
import sys
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import reporting_runtime_binding as binding  # noqa: E402
from scripts.reporting_image_release_policy import ACCOUNT, BUCKET, REGION, VEVO  # noqa: E402
from scripts.reporting_migration_host_gate import canonical, read_private, require, sha, verify_quality  # noqa: E402
from scripts.vevo_report_image_release import EXPECTED_ARTIFACTS, image_only_definition, verify_artifacts  # noqa: E402

LIMIT = 64 * 1024 * 1024
CONTACTS = ("REPORT_EMAIL_FROM", "REPORT_EMAIL_TO")
PREFIX = "data/vevo/reporting/email-resends/"


def decode(raw):
    def unique(pairs):
        require(len(pairs) == len(dict(pairs)), "email-duplicate-json-key")
        return dict(pairs)
    return json.loads(raw, object_pairs_hook=unique)


def private_path(value, *, exists=True):
    requested = Path(value)
    require(".." not in requested.parts, "email-local-path-scope")
    path = requested.absolute()
    require(path.is_relative_to(ROOT / "data") and path.resolve(strict=False) == path, "email-local-path-scope")
    for part in (path, *path.parents):
        require(not part.is_symlink() and not (part.exists() and getattr(part.stat(), "st_file_attributes", 0) & 1024),
                "email-local-path-alias")
    require(not exists or path.is_file(), "email-local-file-missing")
    return path


def read_local(path, expected):
    path = private_path(path)
    require(0 < path.stat().st_size <= LIMIT, "email-local-size")
    raw = path.read_bytes()
    require(sha(raw) == expected, "email-local-hash")
    return raw


def optional(s3, key):
    try:
        return read_private(s3, key, LIMIT)
    except Exception as exc:
        if binding.error_code(exc) in {"NoSuchKey", "404"}:
            return None
        raise


def put_once(s3, key, raw):
    require(key.startswith(PREFIX) and ".." not in key, "email-write-scope")
    s3.put_object(Bucket=BUCKET, Key=key, Body=raw, ExpectedBucketOwner=ACCOUNT,
                  ServerSideEncryption="AES256", IfNoneMatch="*", ContentType="application/json")
    require(read_private(s3, key, LIMIT) == raw, "email-write-readback")


def code_hashes():
    return {name: sha((ROOT / name).read_bytes()) for name in (
        "scripts/resend_reporting_email.py", "daily_report_runner.py", "projects/vevo/settings.json")}


def release_context(clients, release_id, manifest_sha, to_date):
    require(re.fullmatch("[a-f0-9]{32}", release_id) and re.fullmatch("[a-f0-9]{64}", manifest_sha), "email-input-identity")
    require(date.fromisoformat(to_date).isoformat() == to_date, "email-input-date")
    require(clients["sts"].get_caller_identity()["Account"] == ACCOUNT, "email-account")
    s3 = clients["s3"]
    prefix = VEVO.release_prefix + release_id + "/"
    listing = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, ExpectedBucketOwner=ACCOUNT, MaxKeys=100)
    require(not listing.get("IsTruncated"), "email-release-inventory")
    keys = sorted(row["Key"] for row in listing.get("Contents", []))
    require(1 <= len(keys) <= 100 and len(keys) == len(set(keys)), "email-release-inventory")
    records, receipt_hashes = [], {}
    for index, key in enumerate(keys, 1):
        require(re.fullmatch(re.escape(prefix) + rf"{index:04d}-[a-z-]+\.json", key), "email-release-sequence")
        raw = read_private(s3, key, 4 * 1024 * 1024)
        record = decode(raw)
        require(record.get("release_id") == release_id and record.get("project") == "vevo"
                and record.get("report_to_date") == to_date and record.get("schema_version") == 1, "email-release-binding")
        records.append(record)
        receipt_hashes[key] = sha(raw)
    final = records[-1]
    live_rows = [row for row in records if row.get("phase") == "live-complete"]
    require(final.get("phase") == "release-complete" and final.get("probe_role_removed") is True
            and final.get("all_owned_tasks_stopped") is True and len(live_rows) == 1, "email-release-incomplete")
    live = live_rows[0]
    require(records[0].get("phase") == "preflight-complete"
            and all(row.get("source_commit") == live.get("source_commit") for row in records)
            and re.fullmatch("[a-f0-9]{40}", live.get("source_commit", "")), "email-release-source")
    require(live.get("generation_manifest_sha256") == manifest_sha and live.get("email_sent") is False,
            "email-live-manifest")
    task, identity = live["terminal_task"], live["identity"]
    ips = [entry["value"] for attachment in task.get("attachments", []) for entry in attachment.get("details", [])
           if entry.get("name") == "privateIPv4Address"]
    require(task.get("lastStatus") == "STOPPED" and len(task.get("containers", [])) == 1
            and task["containers"][0].get("exitCode") == 0 and identity.get("path") == "/app"
            and identity.get("service") == VEVO.service and identity.get("task_arn") == task.get("taskArn")
            and task.get("taskDefinitionArn") == identity.get("task_definition")
            and re.fullmatch(rf"arn:aws:ecs:{REGION}:{ACCOUNT}:task/vevo-reporting-cluster/[a-f0-9]{{32}}", task.get("taskArn", ""))
            and identity.get("project") == "vevo" and identity.get("release_id") == release_id
            and identity.get("report_to_date") == to_date and ips == [identity.get("private_ip")]
            and task["containers"][0].get("imageDigest") == identity.get("image_digest")
            and all(identity.get(key) is True for key in ("skip_email", "skip_invoices", "skip_inline_guard"))
            and identity.get("source_commit") == live["source_commit"]
            and identity.get("marker") == VEVO.live_marker, "email-live-host")
    latest_key = VEVO.sink + "/latest/generation.json"
    manifest_raw = read_private(s3, latest_key)
    manifest = decode(manifest_raw)
    require(sha(manifest_raw) == manifest_sha and manifest == live["generation_manifest"]
            and manifest.get("project") == "vevo"
            and re.fullmatch("[0-9]{8}T[0-9]{6}Z", manifest.get("generation_id", "")), "email-latest-drift")
    schedule = clients["scheduler"].get_schedule(Name=VEVO.service)
    require(binding.schedule_snapshot(schedule) == binding.schedule_snapshot(final["final_schedule"])
            and schedule.get("State") == "ENABLED", "email-schedule-drift")
    overrides = decode(schedule["Target"].get("Input", "{}"))
    for row in overrides.get("containerOverrides", []):
        require(not (set(CONTACTS) | {"SES_CONFIGURATION_SET"}).intersection(
            item["name"] for item in row.get("environment", [])), "email-contact-schedule-override")
    arn = schedule["Target"]["EcsParameters"]["TaskDefinitionArn"]
    require(arn == identity.get("task_definition"), "email-definition-drift")
    definition = clients["ecs"].describe_task_definition(taskDefinition=arn)["taskDefinition"]
    require(definition.get("status") == "ACTIVE" and definition.get("taskDefinitionArn") == arn, "email-definition-status")
    image_only_definition(definition, definition["containerDefinitions"][0]["image"])
    require(definition["containerDefinitions"][0]["image"].endswith("@" + identity["image_digest"]), "email-image-drift")
    lease = decode(read_private(s3, VEVO.lease_key))
    require(lease.get("state") == "released" and lease.get("owner") == release_id, "email-release-lease")
    return {"release_id": release_id, "source_commit": live["source_commit"], "to_date": to_date,
            "generation_id": manifest["generation_id"], "manifest_sha256": manifest_sha,
            "receipt_hashes": receipt_hashes, "schedule": binding.schedule_snapshot(schedule),
            "definition_sha256": sha(canonical(binding.definition_snapshot(definition)))}, manifest, definition


def runtime_contacts(clients, definition):
    container = definition["containerDefinitions"][0]
    env = binding.unique_environment(container.get("environment", []))
    references = {row["name"]: row["valueFrom"] for row in container.get("secrets", [])}
    require(len(references) == len(container.get("secrets", [])), "email-secret-duplicate")
    require(not set(CONTACTS).intersection(env) and set(CONTACTS).issubset(references), "email-contact-not-secret")
    values, cache = {}, {}
    for name in CONTACTS:
        parts = references[name].split(":")
        require(len(parts) in {7, 10} and ":".join(parts[:6]) == f"arn:aws:secretsmanager:{REGION}:{ACCOUNT}:secret"
                and parts[6].startswith("vevo/") and (len(parts) == 7 or parts[7:] == [name, "", ""]), "email-secret-reference")
        arn = ":".join(parts[:7])
        if arn not in cache:
            cache[arn] = clients["secretsmanager"].get_secret_value(SecretId=arn)["SecretString"]
        values[name] = decode(cache[arn])[name] if len(parts) == 10 else cache[arn]
        require(isinstance(values[name], str) and values[name].strip() and not any(c in values[name] for c in "\r\n"), "email-contact-value")
        values[name] = values[name].strip()
    addresses = [value.strip() for value in values["REPORT_EMAIL_TO"].split(",") if value.strip()]
    require(addresses and all(re.fullmatch(r"[^\s<>@,]+@[^\s<>@,]+\.[^\s<>@,]+", value)
                              for value in [values["REPORT_EMAIL_FROM"], *addresses]), "email-contact-format")
    settings = decode((ROOT / "projects/vevo/settings.json").read_bytes())
    values["SES_CONFIGURATION_SET"] = env.get("SES_CONFIGURATION_SET", settings.get("ses_configuration_set", ""))
    return values, {"contact_sha256": sha(canonical(values)), "recipient_count": len(addresses),
                    "contact_reference_sha256": sha(canonical({name: references[name] for name in CONTACTS}))}


def country_payload(raw, *, html=False):
    if html:
        match = re.search(rb'<script\s+id=["\']report-dashboard-json["\']\s+type=["\']application/json["\']>(.*?)</script>', raw, re.S | re.I)
        require(match is not None, "email-html-payload")
        raw = match[1]
    payload = decode(raw.decode("utf-8-sig"))
    dashboard = payload.get("dashboard", payload)
    countries = dashboard.get("kpis", {}).get("countries", {})
    require(set(countries) == {"sk", "cz", "hu"}, "email-country-cards")
    for value in countries.values():
        require(value.get("status") == "ok" and set(value.get("windows", {})) == {"daily", "weekly", "monthly", "all_time"}, "email-country-status")
    return payload


class CaptureSES:
    def __init__(self):
        self.calls = []

    def send_raw_email(self, **kwargs):
        require(not self.calls, "email-formatter-called-twice")
        self.calls.append(kwargs)
        return {"MessageId": "prepared-not-sent"}


def prepare(clients, *, release_id, manifest_sha256, to_date, scope, output):
    require(re.fullmatch("[a-z0-9][a-z0-9-]{2,79}", scope), "email-scope")
    output = private_path(output, exists=False)
    require(not output.exists(), "email-output-exists")
    context, manifest, definition = release_context(clients, release_id, manifest_sha256, to_date)
    contacts, contact_proof = runtime_contacts(clients, definition)
    artifacts = {}
    def fetch(key, limit):
        raw = read_private(clients["s3"], key, limit)
        artifacts[key.rsplit("/", 1)[1]] = raw
        return raw
    verify_artifacts(manifest, fetch, prefix=VEVO.sink + "/" + context["generation_id"] + "/", to_date=to_date, probe=False)
    require(set(artifacts) == EXPECTED_ARTIFACTS, "email-artifact-inventory")
    for name, raw in artifacts.items():
        parsed = country_payload(raw, html=name.endswith(".html"))
        require(parsed.get("project") == "vevo" and str(parsed.get("date_to", ""))[:10] == to_date, "email-artifact-dates")
    payload = decode(artifacts["dashboard_payload_latest.json"])
    quality = payload["source_health"]
    verify_quality(quality)
    output.mkdir(parents=True)
    for name, raw in artifacts.items():
        (output / name).write_bytes(raw)
    with patch("dotenv.load_dotenv", return_value=False):
        runner = importlib.import_module("daily_report_runner")
    defaults = runner.resolve_reporting_defaults("vevo", runner.load_project_settings("vevo"))
    paths = {"report_html": output / "report_latest.html", "dashboard_payload_json": output / "dashboard_payload_latest.json"}
    summary = "Overena generacia: " + context["generation_id"] + "\n" + runner._build_creditnote_summary(paths)
    body = runner.build_email_body(str(payload["date_from"])[:10], to_date, summary, defaults, quality)
    capture = CaptureSES()
    with patch.dict(os.environ, {**contacts, "AWS_REGION": REGION}), patch("boto3.client", return_value=capture), redirect_stdout(io.StringIO()):
        runner.send_email_ses(runner.build_email_subject(defaults, quality), body, paths, defaults)
    require(len(capture.calls) == 1, "email-formatter-capture")
    mime = capture.calls[0]["RawMessage"]["Data"]
    require(release_context(clients, release_id, manifest_sha256, to_date)[0] == context, "email-prepare-drift")
    proof = {"schema_version": 1, "project": "vevo", "scope": scope, "prepared_at": datetime.now(timezone.utc).isoformat(),
             "context": context, **contact_proof, "mime_sha256": sha(mime), "mime_size": len(mime), "code_sha256": code_hashes(),
             "artifacts": manifest["artifacts"], "email_sent": False}
    (output / "message.eml").write_bytes(mime)
    raw = canonical(proof)
    (output / "prepared.json").write_bytes(raw)
    return {"prepared": str(output / "prepared.json"), "prepared_sha256": sha(raw), "recipient_count": proof["recipient_count"], "email_sent": False}


def send(clients, *, prepared, prepared_sha256, authorize_send):
    require(authorize_send is True, "email-explicit-send-required")
    path = private_path(prepared)
    proof = decode(read_local(path, prepared_sha256))
    require(proof.get("schema_version") == 1 and proof.get("project") == "vevo" and proof.get("email_sent") is False
            and re.fullmatch("[a-z0-9][a-z0-9-]{2,79}", proof.get("scope", "")), "email-prepared-schema")
    prefix = PREFIX + proof["scope"] + "/"
    identity = {"prepared_sha256": prepared_sha256, "mime_sha256": proof["mime_sha256"],
                "release_id": proof["context"]["release_id"], "generation_id": proof["context"]["generation_id"]}
    s3 = clients["s3"]
    require(clients["sts"].get_caller_identity()["Account"] == ACCOUNT, "email-account")
    accepted = optional(s3, prefix + "accepted.json")
    if accepted is not None:
        value = decode(accepted)
        intent_raw = optional(s3, prefix + "intent.json")
        require(value.get("schema_version") == 1 and value.get("status") == "ses-accepted-not-delivery-confirmed"
                and value.get("identity") == identity and isinstance(value.get("message_id"), str) and value["message_id"]
                and intent_raw is not None and decode(intent_raw).get("identity") == identity, "email-accepted-binding")
        return {"status": "already-accepted", "message_id": value["message_id"], "email_sent": False}
    require(not os.path.lexists(path.parent / "accepted.json"), "email-local-acceptance-exists-reconcile")
    require(optional(s3, prefix + "intent.json") is None, "email-intent-uncertain-do-not-resend")
    require(proof["code_sha256"] == code_hashes(), "email-preparer-code-drift")
    expected = proof["context"]
    context, manifest, definition = release_context(clients, expected["release_id"], expected["manifest_sha256"], expected["to_date"])
    require(context == expected and manifest["artifacts"] == proof["artifacts"], "email-send-runtime-drift")
    contacts, contact_proof = runtime_contacts(clients, definition)
    require(all(proof[key] == value for key, value in contact_proof.items()), "email-contacts-changed")
    mime = read_local(path.parent / "message.eml", proof["mime_sha256"])
    require(len(mime) == proof["mime_size"], "email-mime-size")
    message = BytesParser(policy=email_policy.default).parsebytes(mime)
    destinations = [value.strip() for value in contacts["REPORT_EMAIL_TO"].split(",") if value.strip()]
    require(getaddresses(message.get_all("From", [])) == [("", contacts["REPORT_EMAIL_FROM"])]
            and getaddresses(message.get_all("To", [])) == [("", value) for value in destinations]
            and not message.get_all("Bcc") and not message.get_all("Cc"), "email-mime-contacts")
    intent = {"schema_version": 1, "identity": identity, "contact_sha256": proof["contact_sha256"],
              "created_at": datetime.now(timezone.utc).isoformat(), "status": "send-intent-no-retry"}
    put_once(s3, prefix + "intent.json", canonical(intent))
    require(release_context(clients, expected["release_id"], expected["manifest_sha256"], expected["to_date"])[0] == context,
            "email-post-intent-runtime-drift")
    require(runtime_contacts(clients, definition)[1] == contact_proof, "email-post-intent-contacts-drift")
    args = {"Source": contacts["REPORT_EMAIL_FROM"], "Destinations": destinations, "RawMessage": {"Data": mime}}
    if contacts["SES_CONFIGURATION_SET"]:
        args["ConfigurationSetName"] = contacts["SES_CONFIGURATION_SET"]
    response = clients["ses"].send_raw_email(**args)  # Exactly one SDK call; any failure leaves the intent uncertain.
    message_id = response.get("MessageId")
    require(isinstance(message_id, str) and message_id, "email-acceptance-uncertain")
    receipt = {"schema_version": 1, "identity": identity, "message_id": message_id,
               "accepted_at": datetime.now(timezone.utc).isoformat(), "status": "ses-accepted-not-delivery-confirmed"}
    raw = canonical(receipt)
    # Preserve known acceptance locally even if its private S3 PUT/readback fails.
    with (path.parent / "accepted.json").open("xb") as output:
        output.write(raw)
    put_once(s3, prefix + "accepted.json", raw)
    return {"status": "accepted", "message_id": message_id, "email_sent": True, "receipt_sha256": sha(raw)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "send"))
    parser.add_argument("--profile", default="codex")
    parser.add_argument("--project", choices=("vevo",), default="vevo")
    for name in ("release-id", "expected-manifest-sha256", "to-date", "scope", "output", "prepared", "prepared-sha256"):
        parser.add_argument("--" + name)
    parser.add_argument("--authorize-send", action="store_true")
    args = parser.parse_args()
    import boto3
    from botocore.config import Config
    session = boto3.Session(profile_name=args.profile, region_name=REGION)
    config = Config(connect_timeout=10, read_timeout=45, retries={"total_max_attempts": 1})
    names = ("sts", "s3", "ecs", "scheduler", "secretsmanager") + (("ses",) if args.mode == "send" else ())
    clients = {name: session.client(name, config=config) for name in names}
    if args.mode == "prepare":
        require(all((args.release_id, args.expected_manifest_sha256, args.to_date, args.scope, args.output)), "email-prepare-arguments")
        result = prepare(clients, release_id=args.release_id, manifest_sha256=args.expected_manifest_sha256,
                         to_date=args.to_date, scope=args.scope, output=args.output)
    else:
        require(args.prepared and args.prepared_sha256, "email-send-arguments")
        result = send(clients, prepared=args.prepared, prepared_sha256=args.prepared_sha256, authorize_send=args.authorize_send)
    print(json.dumps(result))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        # SDK/formatter exceptions can contain recipients, secrets or MIME. Do not print them.
        reason = str(exc)
        print(json.dumps({"status": "stopped", "reason": reason if re.fullmatch("email-[a-z-]+", reason) else type(exc).__name__}))
        raise SystemExit(1) from None
