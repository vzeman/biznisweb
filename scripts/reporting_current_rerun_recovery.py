"""Read-only admission of a failed same-definition VEVO rerun into image release.

This is a separate predecessor format, not a relaxation of image-release probe
proof. A reviewed ZIP may prove an expired task through the saved observer's
projection; it is never presented as a raw or fresh ECS response. No archived
code is executed and this module performs no cloud writes.
"""
from __future__ import annotations

import copy
import io
import ipaddress
import json
import re
import zipfile

from scripts import reporting_runtime_binding as binding
from scripts.reporting_image_release_lease import read_lease
from scripts.reporting_image_release_policy import ACCOUNT, REGION, VEVO
from scripts.reporting_migration_host_gate import canonical, read_private, require, sha

PHASES = ("preflight-complete", "lease-acquire-requested", "report-pause-requested",
          "report-pause-verified", "live-dispatch-requested", "live-dispatched",
          "rerun-failure-paused-review-required")
ARCHIVE_LIMIT = 32 * 1024 * 1024
MEMBER_LIMIT = 64 * 1024 * 1024
TOTAL_LIMIT = 128 * 1024 * 1024
# Reviewed observer used by the 2026-10-22 evidence format. Other projection
# producers need a separately reviewed adapter, not a claimed verified boolean.
OBSERVER_SHA256 = "52417f3f39b6a2839bf6bd67e5932b53c418ef2972b5259fdd02f042b195c515"
PREPROBE_PHASES = ("preflight-complete", "paused-recovery-handoff-requested", "report-pause-requested",
                  "report-pause-verified", "candidate-register-requested", "candidate-registered",
                  "probe-role-create-requested", "release-failed-inspection-required",
                  "paused-recovery-lease-return-requested", "failed-before-live-restored")


def normalized(value):
    return json.loads(canonical(binding.normalized(value)))


def validate_chain(records, owner, schedule, definition, gate_sha):
    from scripts.vevo_report_current_rerun import validate_current
    from scripts.vevo_report_image_release import CLUSTER, EXPECTED_ARTIFACTS, task_overrides
    require(len(records) == 7 and tuple(row.get("phase") for row in records) == PHASES,
            "image-release-current-recovery-chain")
    before, failed = records[0], records[-1]
    require(re.fullmatch(r"[a-f0-9]{40}", before.get("source_commit", ""))
            and re.fullmatch(r"[a-f0-9]{40}", before.get("controller_commit", "")),
            "image-release-current-recovery-source")
    require(all(row.get("schema_version") == 1 and row.get("project") == "vevo"
                and row.get("release_id") == owner and row.get("source_commit") == before["source_commit"]
                and row.get("controller_commit") == before["controller_commit"]
                and row.get("report_to_date") == before["report_to_date"]
                and row.get("runtime_reused") is True and row.get("promoted") is False
                and row.get("legacy_current_binding_changed") is False for row in records),
            "image-release-current-recovery-chain-identity")
    times = [binding.stamp(row["at"]) for row in records]
    require(times == sorted(times), "image-release-current-recovery-time-order")
    arn = before["source_definition"]["taskDefinitionArn"]
    env = validate_current(before["schedule"], before["source_definition"], arn, before["image"])
    require(not env.get("REPORT_TO_DATE") and before["schedule"].get("ScheduleExpression", "").startswith("cron("),
            "image-release-current-recovery-dynamic-cron")
    expected = binding.schedule_snapshot(before["schedule"])
    expected["State"] = "DISABLED"
    require(all(binding.schedule_snapshot(value) == expected for value in
                (records[2]["schedule"], records[3]["schedule"], failed["known_schedule"], schedule)),
            "image-release-current-recovery-paused-schedule")
    require(definition.get("status") == "ACTIVE"
            and binding.definition_snapshot(definition) == binding.definition_snapshot(before["source_definition"]),
            "image-release-current-recovery-definition")
    expected_outputs = {VEVO.sink + "/latest/" + name for name in EXPECTED_ARTIFACTS | {"generation.json"}}
    require(set(before["original_live_outputs"]) == expected_outputs
            and all(type(row.get("ContentLength")) is int and row["ContentLength"] > 0
                    and row.get("ETag") and row.get("LastModified") for row in before["original_live_outputs"].values()),
            "image-release-current-recovery-output-set")
    task = records[5]["task_arn"]
    require(re.fullmatch(re.escape(CLUSTER.replace(":cluster/", ":task/") + "/") + r"[a-f0-9]{32}", task)
            and records[1]["lease_key"] == VEVO.lease_key
            and records[4]["task_definition"] == arn and records[4]["client_token"] == owner + "-live"
            and records[4]["overrides"] == task_overrides(owner, before["source_commit"], before["image"],
                                                         gate_sha, before["report_to_date"], probe=False)
            and all(row.get("known_task_arns") == {} and row.get("rerun_attempted") is False for row in records[:5])
            and all(row.get("known_task_arns") == {"live": task} and row.get("rerun_attempted") is True for row in records[5:])
            and failed.get("start_uncertain") is False, "image-release-current-recovery-dispatch")
    return {"kind": "current-rerun", "project": "vevo", "before": before, "failed": failed,
            "tasks": {"live": task}, "candidate_arn": arn, "gate_sha": gate_sha, "previous_owner": owner}


def archive_members(raw, expected_sha):
    require(sha(raw) == expected_sha and len(raw) <= ARCHIVE_LIMIT, "image-release-current-recovery-archive-hash")
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        rows = archive.infolist()
        names = [row.filename for row in rows]
        require(1 < len(rows) <= 128 and len(set(names)) == len(names)
                and all(not row.is_dir() and 0 <= row.file_size <= MEMBER_LIMIT and not row.flag_bits & 1
                        and not row.filename.startswith("/") and "\\" not in row.filename
                        and not {"", ".", ".."}.intersection(row.filename.split("/")) for row in rows)
                and sum(row.file_size for row in rows) <= TOTAL_LIMIT, "image-release-current-recovery-archive-members")
        contents = {row.filename: archive.read(row) for row in rows}
    manifest = json.loads(contents["evidence-manifest.json"])
    inventory = manifest["files"]
    require(set(contents) == set(inventory) | {"evidence-manifest.json"}
            and all(item == {"sha256": sha(contents[name]), "bytes": len(contents[name])}
                    for name, item in inventory.items()), "image-release-current-recovery-archive-inventory")
    return manifest, contents


def validate_archive(raw, expected_sha, recovery, raw_records, receipt_hashes):
    manifest, members = archive_members(raw, expected_sha)
    before, failed, owner = recovery["before"], recovery["failed"], recovery["previous_owner"]
    bases = [name.removesuffix("observer-latest.json") for name in members if name.endswith("/observer-latest.json")]
    require(len(bases) == 1 and re.fullmatch(r"data/vevo-report-rerun-[0-9]{8}/", bases[0]),
            "image-release-current-recovery-archive-layout")
    base = bases[0]
    def document(name):
        return json.loads(members[base + name])
    require(sha(members[base + "observe_current_rerun.py"]) == OBSERVER_SHA256,
            "image-release-current-recovery-observer-source")
    require(manifest.get("project") == "vevo" and manifest.get("release_id") == owner
            and manifest.get("through") == before["report_to_date"] and manifest.get("email_sent") is False
            and manifest.get("status") == "failed_boundary_verified", "image-release-current-recovery-archive-identity")
    require(all(members[base + "failure/journal/" + key.rsplit("/", 1)[1]] == raw
                for key, raw in raw_records.items()), "image-release-current-recovery-archived-journal")
    observed, host, audit = document("observer-latest.json"), document("observer-host-proof.json"), document("failure/boundary-audit.json")
    identity = host["marker"]
    task = recovery["tasks"]["live"]
    digest = before["image"].rsplit("@", 1)[1]
    require(identity.get("marker") == VEVO.live_marker and identity.get("project") == "vevo"
            and identity.get("release_id") == owner and identity.get("source_commit") == before["source_commit"]
            and identity.get("task_arn") == task and identity.get("task_definition") == recovery["candidate_arn"]
            and identity.get("image_digest") == digest and identity.get("gate_sha256") == recovery["gate_sha"]
            and identity.get("instance_id") == "N/A:Fargate" and identity.get("service") == VEVO.service
            and identity.get("path") == "/app" and identity.get("report_to_date") == before["report_to_date"]
            and all(identity.get(key) is True for key in ("skip_email", "skip_invoices", "skip_inline_guard")),
            "image-release-current-recovery-host")
    require(ipaddress.ip_address(identity["private_ip"]).version == 4, "image-release-current-recovery-host-ip")
    for row in (observed, host["task_identity"]):
        require(row.get("release_id") == owner and row.get("task_arn") == task
                and row.get("task_definition") == recovery["candidate_arn"] and row.get("image_digest") == digest
                and row.get("private_ips") == [identity["private_ip"]]
                and row.get("host_marker_verified") is True and row.get("task_overrides_verified") is True,
                "image-release-current-recovery-observer-identity")
    require(observed.get("phase") == PHASES[-1] and observed.get("receipt_count") == 7
            and observed.get("receipt_key") == next(reversed(receipt_hashes))
            and observed.get("receipt_sha256") == recovery["receipt_sha256"]
            and observed.get("task_status") == observed.get("desired_status") == observed.get("container_status") == "STOPPED"
            and type(observed.get("exit_code")) is int and observed["exit_code"] == 1,
            "image-release-current-recovery-observer-terminal")
    started, stopped = (binding.stamp(observed[key]) for key in ("task_started_at", "task_stopped_at"))
    require(started <= binding.stamp(host["checked_at"]) <= stopped <= binding.stamp(failed["at"])
            <= binding.stamp(observed["checked_at"]) <= binding.stamp(audit["checked_at"]) <= binding.stamp(manifest["at"]),
            "image-release-current-recovery-observation-times")
    require(audit.get("status") == "failed_boundary_verified" and audit.get("release_id") == owner
            and audit.get("controller_commit") == before["controller_commit"] and audit.get("runtime_source") == before["source_commit"]
            and audit.get("task_arn") == task and audit.get("task_definition") == recovery["candidate_arn"]
            and audit.get("image_digest") == digest and audit.get("task_stopped_exit_code") == 1
            and audit.get("vevo_lease_state") == "uncertain" and audit.get("vevo_lease_owner") == owner
            and isinstance(audit.get("vevo_lease_etag"), str) and audit["vevo_lease_etag"]
            and audit.get("active_vevo_task_count") == 0 and audit.get("verified_output_count") == 9
            and audit.get("receipt_hashes") == receipt_hashes
            and all(audit.get(key) is True for key in ("host_marker_verified", "controller_process_ended",
                    "schedule_paused_exact_original_except_state", "task_definition_exact_original",
                    "latest_pointer_and_all_eight_artifacts_exact_original", "roy_schedule_outputs_lease_unchanged")),
            "image-release-current-recovery-boundary-audit")
    terminal = audit["task_terminal_evidence"]
    require(terminal.get("source") == "observer-latest.json"
            and terminal.get("sha256") == sha(members[base + "observer-latest.json"])
            and terminal.get("observed_at") == observed["checked_at"], "image-release-current-recovery-terminal-binding")
    require(document("failure/unchanged-output-heads.json") == before["original_live_outputs"]
            and document("schedule-before.json") == binding.schedule_snapshot(before["schedule"]),
            "image-release-current-recovery-archived-boundaries")
    generation = document("failure/generation.json")
    require(generation.get("project") == "vevo" and generation.get("generation_id") == manifest["generation"] == audit["unchanged_generation_id"]
            and sha(members[base + "failure/generation.json"]) == audit["generation_manifest_sha256"],
            "image-release-current-recovery-generation")
    return {"identity": identity, "terminal_observation": observed, "lease_etag": audit["vevo_lease_etag"],
            "archive_manifest_sha256": sha(members["evidence-manifest.json"]),
            "terminal_observation_sha256": terminal["sha256"], "observer_script_sha256": OBSERVER_SHA256}


def inspect(controller):
    from scripts.vevo_report_image_release import committed_gate_sha
    owner = controller.recover_current_rerun
    require(controller.policy == VEVO and not controller.recover_paused_release
            and re.fullmatch(r"[a-f0-9]{32}", owner or "") and owner != controller.release_id
            and re.fullmatch(r"[a-f0-9]{64}", controller.recovery_receipt_sha256 or ""),
            "image-release-current-recovery-arguments")
    prefix = "data/vevo/reporting/current-reruns/" + owner + "/"
    page = controller.s3.list_objects_v2(Bucket=binding.BUCKET, Prefix=prefix, MaxKeys=8, ExpectedBucketOwner=ACCOUNT)
    keys = sorted(row["Key"] for row in page.get("Contents", []))
    require(not page.get("IsTruncated") and keys == [prefix + f"{index:04d}-{phase}.json" for index, phase in enumerate(PHASES, 1)],
            "image-release-current-recovery-receipt-sequence")
    raws = {key: read_private(controller.s3, key, 1024 * 1024) for key in keys}
    require(sha(raws[keys[-1]]) == controller.recovery_receipt_sha256, "image-release-current-recovery-receipt-hash")
    records = [json.loads(raw) for raw in raws.values()]
    recovery = validate_chain(records, owner, controller.original, controller.original_source,
                              committed_gate_sha(records[0]["source_commit"]))
    recovery.update(receipt_key=keys[-1], receipt_sha256=sha(raws[keys[-1]]), preflight_sha256=sha(raws[keys[0]]))
    key, digest = controller.recovery_readback_key, controller.recovery_readback_sha256
    require(re.fullmatch(r"[a-f0-9]{64}", digest or "") and isinstance(key, str)
            and re.fullmatch(r"data/vevo/reporting/reruns/[a-z0-9-]+/" + re.escape(digest) + r"/failure-evidence\.zip", key),
            "image-release-current-recovery-archive-key")
    raw = read_private(controller.s3, key, ARCHIVE_LIMIT)
    recovery.update(validate_archive(raw, digest, recovery, raws, {name: sha(value) for name, value in raws.items()}))
    # The original archive remains authoritative for its old lease generation.
    # A single separately reviewed pre-probe return may prove the subsequent
    # generation; never infer it from the current ETag or recursively recover.
    recovery.update(original_readback_key=key, original_readback_sha256=digest)
    inspect_preprobe_return(controller, recovery)
    controller.recovery = recovery
    verify_boundary(controller)
    proof = {"kind": "current-rerun", "release_id": owner, "receipt_key": keys[-1], "receipt_sha256": recovery["receipt_sha256"],
            "preflight_sha256": recovery["preflight_sha256"], "readback_key": key, "readback_sha256": digest,
            "lease_etag": recovery["lease_etag"], "terminal_evidence_kind": recovery["terminal_evidence_kind"],
            "terminal_observation_sha256": recovery["terminal_observation_sha256"],
            "observer_script_sha256": recovery["observer_script_sha256"], "archive_manifest_sha256": recovery["archive_manifest_sha256"]}
    if "preprobe_return" in recovery:
        returned = recovery["preprobe_return"]
        proof["preprobe_return"] = {"key": returned["key"], "sha256": returned["sha256"],
            "failed_release_id": returned["document"]["failed_release_id"],
            "terminal_receipt_sha256": returned["document"]["receipts"][-1]["sha256"],
            "original_lease_etag": returned["original_lease_etag"], "returned_lease_etag": recovery["lease_etag"],
            "returned_lease_sha256": sha(canonical(returned["document"]["returned_lease"]["value"]))}
    return proof


def preprobe_journal(controller, failed_id):
    prefix = VEVO.release_prefix + failed_id + "/"
    page = controller.s3.list_objects_v2(Bucket=binding.BUCKET, Prefix=prefix, MaxKeys=11, ExpectedBucketOwner=ACCOUNT)
    keys = sorted(row["Key"] for row in page.get("Contents", []))
    require(not page.get("IsTruncated") and keys == [prefix + f"{index:04d}-{phase}.json"
            for index, phase in enumerate(PREPROBE_PHASES, 1)], "image-release-preprobe-return-receipt-sequence")
    return {key: read_private(controller.s3, key, 1024 * 1024) for key in keys}


def validate_preprobe_return(document, raw_records, recovery, new_owner):
    """Validate exactly one no-dispatch rollback, never a general ETag override."""
    from scripts.vevo_report_image_release import image_only_definition
    failed_id = document.get("failed_release_id", "")
    require(document.get("schema_version") == 1 and document.get("kind") == "current-rerun-preprobe-return"
            and document.get("project") == "vevo" and re.fullmatch(r"[a-f0-9]{32}", failed_id)
            and failed_id not in (recovery["previous_owner"], new_owner)
            and re.fullmatch(r"[a-f0-9]{40}", document.get("source_commit", "")),
            "image-release-preprobe-return-identity")
    records = [json.loads(raw) for raw in raw_records.values()]
    require(len(records) == 10 and tuple(row.get("phase") for row in records) == PREPROBE_PHASES
            and document["receipts"] == [{"key": key, "sha256": sha(raw)} for key, raw in raw_records.items()],
            "image-release-preprobe-return-journal")
    prefix = VEVO.release_prefix + failed_id + "/"
    require(list(raw_records) == [prefix + f"{index:04d}-{phase}.json" for index, phase in enumerate(PREPROBE_PHASES, 1)],
            "image-release-preprobe-return-journal-keys")
    require(all(row.get("schema_version") == 1 and row.get("project") == "vevo"
                and row.get("release_id") == failed_id and row.get("source_commit") == document["source_commit"]
                and row.get("report_to_date") == document["report_to_date"] == recovery["before"]["report_to_date"]
                and row.get("known_task_arns") == {} and row.get("promoted") is False
                and row.get("rerun_attempted") is False and row.get("legacy_current_binding_changed") is False
                for row in records), "image-release-preprobe-return-no-dispatch")
    times = [binding.stamp(row["at"]) for row in records]
    require(times == sorted(times) and times[0] >= binding.stamp(recovery["failed"]["at"])
            and binding.stamp(document["checked_at"]) >= times[-1], "image-release-preprobe-return-time-order")
    before, final = records[0], records[-1]
    old_etag = recovery["lease_etag"]
    original = {"kind": "current-rerun", "release_id": recovery["previous_owner"],
        "receipt_key": recovery["receipt_key"], "receipt_sha256": recovery["receipt_sha256"],
        "preflight_sha256": recovery["preflight_sha256"], "readback_key": recovery["original_readback_key"],
        "readback_sha256": recovery["original_readback_sha256"], "lease_etag": old_etag,
        "terminal_evidence_kind": before.get("recovered_from", {}).get("terminal_evidence_kind"),
        "terminal_observation_sha256": recovery["terminal_observation_sha256"],
        "observer_script_sha256": recovery["observer_script_sha256"], "archive_manifest_sha256": recovery["archive_manifest_sha256"]}
    require(original["terminal_evidence_kind"] in {"archived-reviewed-observer-projection-ecs-missing",
                "fresh-ecs-plus-archived-observer-projection"}
            and document["original_recovery"] == before["recovered_from"] == original,
            "image-release-preprobe-return-original-proof")
    require(records[1].get("previous_owner") == records[8].get("previous_owner")
            == final.get("paused_recovery_owner") == recovery["previous_owner"]
            and records[1].get("previous_lease_etag") == old_etag
            and records[1].get("recovery_receipt_sha256") == recovery["receipt_sha256"]
            and records[7].get("start_uncertain") is False and final.get("all_owned_tasks_stopped") is True,
            "image-release-preprobe-return-handoff")
    expected_schedule = binding.schedule_snapshot(recovery["before"]["schedule"])
    expected_schedule["State"] = "DISABLED"
    require(all(binding.schedule_snapshot(value) == expected_schedule for value in
                (before["schedule"], records[2]["schedule"], records[3]["schedule"], final["schedule"], document["schedule"]))
            and document["source_definition"].get("status") == "ACTIVE"
            and document["source_definition"].get("taskDefinitionArn") == recovery["candidate_arn"]
            and before["source_definition"].get("taskDefinitionArn") == recovery["candidate_arn"]
            and binding.definition_snapshot(document["source_definition"]) == binding.definition_snapshot(before["source_definition"])
            == binding.definition_snapshot(recovery["before"]["source_definition"]),
            "image-release-preprobe-return-source-boundary")
    require(before["original_live_outputs"] == document["live_outputs"] == recovery["before"]["original_live_outputs"]
            and before["protected_schedules"] == document["protected_schedules"] == recovery["before"]["protected_schedules"]
            and before["protected_peer"] == document["protected_peer"] == recovery["before"]["protected_peer"],
            "image-release-preprobe-return-protected-boundary")
    candidate_arn = records[5]["task_definition"]
    require(re.fullmatch(rf"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/{VEVO.family}:[1-9][0-9]*", candidate_arn)
            and candidate_arn != recovery["candidate_arn"] and before["image"] != recovery["before"]["image"]
            and document["candidate_definition"].get("taskDefinitionArn") == candidate_arn
            and document["candidate_definition"].get("status") == "INACTIVE", "image-release-preprobe-return-candidate")
    expected_candidate = image_only_definition(before["source_definition"], before["image"])
    require(normalized(records[4]["definition"]) == normalized(expected_candidate)
            and binding.definition_snapshot(document["candidate_definition"]) == binding.definition_snapshot(
                {**expected_candidate, "taskDefinitionArn": candidate_arn})
            and records[6].get("role_arn") == f"arn:aws:iam::{ACCOUNT}:role/{VEVO.role_prefix}{failed_id}"
            and document.get("candidate_probe_role_absent") is True
            and document.get("owned_tasks") == document.get("active_reporting_tasks") == [],
            "image-release-preprobe-return-cleanup")
    returned = document["returned_lease"]
    binding.validate_lock(returned["value"])
    require(isinstance(returned.get("etag"), str) and returned["etag"] and returned["etag"] != old_etag
            and returned["value"]["owner"] == recovery["previous_owner"] and returned["value"]["state"] == "uncertain"
            and times[8] <= binding.stamp(returned["value"]["updated_at"]) <= times[9],
            "image-release-preprobe-return-lease-generation")
    return candidate_arn


def inspect_preprobe_return(controller, recovery):
    key = getattr(controller, "recovery_preprobe_return_key", None)
    digest = getattr(controller, "recovery_preprobe_return_sha256", None)
    if key is None and digest is None:
        return
    require(controller.policy == VEVO and controller.recover_current_rerun and not controller.recover_paused_release
            and isinstance(key, str) and re.fullmatch(r"[a-f0-9]{64}", digest or "")
            and re.fullmatch(r"data/vevo/reporting/recovery-handoffs/[a-f0-9]{32}/" + re.escape(digest)
                             + r"/preprobe-return\.json", key), "image-release-preprobe-return-arguments")
    raw = read_private(controller.s3, key, 2 * 1024 * 1024)
    require(sha(raw) == digest, "image-release-preprobe-return-readback-hash")
    document = json.loads(raw)
    failed_id = document.get("failed_release_id", "")
    require(re.fullmatch(r"[a-f0-9]{32}", failed_id) and key.split("/")[4] == failed_id,
            "image-release-preprobe-return-key-identity")
    raws = preprobe_journal(controller, failed_id)
    candidate = validate_preprobe_return(document, raws, recovery, controller.release_id)
    recovery["preprobe_return"] = {"key": key, "sha256": digest, "document": document,
        "journal_raws": raws, "candidate_arn": candidate, "original_lease_etag": recovery["lease_etag"]}
    recovery["lease_etag"] = document["returned_lease"]["etag"]


def verify_preprobe_return_boundary(controller):
    returned = controller.recovery.get("preprobe_return")
    if returned is None:
        return
    document = returned["document"]
    require(sha(read_private(controller.s3, returned["key"], 2 * 1024 * 1024)) == returned["sha256"],
            "image-release-preprobe-return-document-drift")
    require(preprobe_journal(controller, document["failed_release_id"]) == returned["journal_raws"],
            "image-release-preprobe-return-journal-drift")
    candidate = controller.definition(returned["candidate_arn"])
    require(candidate.get("status") == "INACTIVE" and candidate.get("taskDefinitionArn") == returned["candidate_arn"]
            and binding.definition_snapshot(candidate) == binding.definition_snapshot(document["candidate_definition"]),
            "image-release-preprobe-return-candidate-drift")
    try:
        controller.iam.get_role(RoleName=VEVO.role_prefix + document["failed_release_id"])
    except Exception as error:
        require(binding.error_code(error) == "NoSuchEntity", "image-release-preprobe-return-role-read")
    else:
        raise RuntimeError("image-release-preprobe-return-role-present")
    require(not [task for task in controller.tasks(VEVO.family) if task.get("startedBy") == document["failed_release_id"]],
            "image-release-preprobe-return-task-present")
    objects = controller.s3.list_objects_v2(Bucket=binding.BUCKET,
        Prefix=VEVO.probe_prefix + document["failed_release_id"] + "/", MaxKeys=1, ExpectedBucketOwner=ACCOUNT)
    require(not objects.get("Contents") and not objects.get("IsTruncated"), "image-release-preprobe-return-probe-objects")
    lease, etag = read_lease(controller.s3, "vevo")
    require(normalized(lease) == document["returned_lease"]["value"] and etag == document["returned_lease"]["etag"],
            "image-release-preprobe-return-lease-drift")


def verify_boundary(controller):
    from scripts.vevo_report_image_release import CLUSTER, task_overrides
    recovery = controller.recovery
    before, observed = recovery["before"], recovery["terminal_observation"]
    require(binding.schedule_snapshot(controller.schedule()) == binding.schedule_snapshot(controller.original),
            "image-release-current-recovery-schedule-drift")
    definition = controller.definition(recovery["candidate_arn"])
    require(definition.get("status") == "ACTIVE"
            and binding.definition_snapshot(definition) == binding.definition_snapshot(controller.original_source),
            "image-release-current-recovery-definition-drift")
    require(controller.live_outputs() == before["original_live_outputs"], "image-release-current-recovery-output-drift")
    require(controller.protected == before["protected_schedules"]
            and {key: value for key, value in controller.all_schedules().items() if key != VEVO.service} == controller.protected
            and normalized(controller.peer_snapshot) == before["protected_peer"], "image-release-current-recovery-protected-drift")
    controller.verify_peer_boundary()
    controller.no_report_tasks()
    verify_preprobe_return_boundary(controller)
    task_arn = recovery["tasks"]["live"]
    response = controller.ecs.describe_tasks(cluster=CLUSTER, tasks=[task_arn])
    tasks, failures = response.get("tasks", []), response.get("failures", [])
    if failures:
        require(tasks == [] and len(failures) == 1 and failures[0].get("arn") == task_arn
                and failures[0].get("reason") == "MISSING", "image-release-current-recovery-task-read-failed")
        recovery["terminal_evidence_kind"] = "archived-reviewed-observer-projection-ecs-missing"
    else:
        require(len(tasks) == 1, "image-release-current-recovery-task-count")
        task = tasks[0]
        containers = task.get("containers", [])
        require(task.get("taskArn") == task_arn and task.get("clusterArn") == CLUSTER
                and task.get("taskDefinitionArn") == recovery["candidate_arn"] and task.get("startedBy") == recovery["previous_owner"]
                and task.get("launchType") == "FARGATE" and task.get("lastStatus") == task.get("desiredStatus") == "STOPPED"
                and len(containers) == 1 and containers[0].get("name") == "reporting" and containers[0].get("lastStatus") == "STOPPED"
                and type(containers[0].get("exitCode")) is int and containers[0]["exitCode"] == 1
                and containers[0].get("imageDigest") == observed["image_digest"] and containers[0].get("image") == before["image"]
                and [row.get("privateIpv4Address") for row in containers[0].get("networkInterfaces", [])] == observed["private_ips"]
                and all(binding.stamp(binding.normalized(task[key])) == binding.stamp(observed[target]) for key, target in
                        (("startedAt", "task_started_at"), ("stoppedAt", "task_stopped_at"))),
                "image-release-current-recovery-task-not-owned-stopped")
        expected = task_overrides(recovery["previous_owner"], before["source_commit"], before["image"], recovery["gate_sha"], before["report_to_date"], probe=False)
        actual = copy.deepcopy(task.get("overrides", {}))
        for row in actual.get("containerOverrides", []):
            row["environment"] = [{"name": key, "value": value} for key, value in sorted(binding.unique_environment(row.get("environment", [])).items())]
        require(actual.get("containerOverrides") == expected["containerOverrides"]
                and actual.get("taskRoleArn") in (None, before["source_definition"]["taskRoleArn"]),
                "image-release-current-recovery-task-overrides")
        recovery["terminal_evidence_kind"] = "fresh-ecs-plus-archived-observer-projection"
    lease, etag = read_lease(controller.s3, "vevo")
    require(lease["state"] == "uncertain" and lease["owner"] == recovery["previous_owner"] and etag == recovery["lease_etag"],
            "image-release-current-recovery-lease-drift")
