"""Synthetic admission proofs: no AWS, archived code execution or production files."""
import copy
import io
import json
import unittest
import zipfile
from unittest.mock import Mock, patch

from scripts import reporting_current_rerun_recovery as recovery
from scripts import vevo_report_image_release as release
from scripts import reporting_runtime_binding as binding
from tests.test_vevo_report_current_rerun import fixture, SOURCE, HEAD, IMAGE, DEFINITION
from tests.test_reporting_runtime_binding import MemoryS3, NOW, StoreError

OWNER, NEW_OWNER, GATE = "c" * 32, "d" * 32, "e" * 64
DAY = "2026-10-21"
BASE = "data/vevo-report-rerun-20261022/"
OBSERVER = b"reviewed observer fixture - never executed"
PREFIX = "data/vevo/reporting/current-reruns/" + OWNER + "/"
START = "2026-10-22T06:07:00+00:00"
STOP = "2026-10-22T06:49:00+00:00"


def evidence():
    original, definition = fixture()
    paused = copy.deepcopy(original)
    paused["State"] = "DISABLED"
    outputs = {release.VEVO.sink + "/latest/" + name:
               {"ETag": "original", "ContentLength": 10, "LastModified": "2026-10-21T23:31:00+00:00"}
               for name in release.EXPECTED_ARTIFACTS | {"generation.json"}}
    protected, peer = {"roy-daily-report-email": {"State": "ENABLED"}}, {"project": "roy", "lease": None}
    task_arn = release.CLUSTER.replace(":cluster/", ":task/") + "/" + "f" * 32
    common = {"schema_version": 1, "project": "vevo", "release_id": OWNER, "source_commit": SOURCE,
              "controller_commit": HEAD, "report_to_date": DAY, "runtime_reused": True, "promoted": False,
              "legacy_current_binding_changed": False, "at": "2026-10-22T06:04:00+00:00",
              "known_task_arns": {}, "rerun_attempted": False}
    records = [{**copy.deepcopy(common), "phase": phase} for phase in recovery.PHASES]
    records[0].update(schedule=original, source_definition=definition, image=IMAGE, original_live_outputs=outputs,
                      protected_schedules=protected, protected_peer=peer)
    records[1]["lease_key"] = release.VEVO.lease_key
    for row in records[2:4]:
        row["schedule"] = copy.deepcopy(paused)
    expected = release.task_overrides(OWNER, SOURCE, IMAGE, GATE, DAY, probe=False)
    records[4].update(task_definition=DEFINITION, client_token=OWNER + "-live", overrides=expected)
    for row in records[5:]:
        row.update(known_task_arns={"live": task_arn}, rerun_attempted=True)
    records[5]["task_arn"] = task_arn
    records[6].update(at="2026-10-22T06:49:02+00:00", start_uncertain=False, known_schedule=copy.deepcopy(paused))
    raws = {PREFIX + f"{index:04d}-{row['phase']}.json": release.canonical(row) for index, row in enumerate(records, 1)}
    hashes = {key: release.sha(raw) for key, raw in raws.items()}
    observed = {"checked_at": "2026-10-22T06:49:05+00:00", "release_id": OWNER, "phase": recovery.PHASES[-1],
                "receipt_key": list(raws)[-1], "receipt_sha256": hashes[list(raws)[-1]], "receipt_count": 7,
                "task_arn": task_arn, "task_definition": DEFINITION, "image_digest": IMAGE.rsplit("@", 1)[1],
                "private_ips": ["172.31.6.219"], "host_marker_verified": True, "task_overrides_verified": True,
                "task_status": "STOPPED", "desired_status": "STOPPED", "container_status": "STOPPED", "exit_code": 1,
                "task_started_at": START, "task_stopped_at": STOP}
    identity = {"marker": release.VEVO.live_marker, "project": "vevo", "release_id": OWNER, "source_commit": SOURCE,
                "task_arn": task_arn, "task_definition": DEFINITION, "image_digest": observed["image_digest"],
                "gate_sha256": GATE, "instance_id": "N/A:Fargate", "service": release.VEVO.service, "path": "/app",
                "report_to_date": DAY, "private_ip": observed["private_ips"][0],
                "skip_email": True, "skip_invoices": True, "skip_inline_guard": True}
    host = {"checked_at": "2026-10-22T06:08:00+00:00", "marker": identity, "task_identity": copy.deepcopy(observed)}
    generation = {"project": "vevo", "generation_id": "20261021T233137Z"}
    audit = {"checked_at": "2026-10-22T10:15:00+00:00", "status": "failed_boundary_verified", "release_id": OWNER,
             "controller_commit": HEAD, "runtime_source": SOURCE, "task_arn": task_arn, "task_definition": DEFINITION,
             "image_digest": observed["image_digest"], "task_stopped_exit_code": 1, "vevo_lease_state": "uncertain",
             "vevo_lease_owner": OWNER, "vevo_lease_etag": '"old-etag"', "active_vevo_task_count": 0,
             "verified_output_count": 9, "receipt_hashes": hashes, "unchanged_generation_id": generation["generation_id"],
             "generation_manifest_sha256": release.sha(release.canonical(generation)),
             "task_terminal_evidence": {"source": "observer-latest.json", "observed_at": observed["checked_at"],
                                       "sha256": release.sha(release.canonical(observed))}}
    for key in ("host_marker_verified", "controller_process_ended", "schedule_paused_exact_original_except_state",
                "task_definition_exact_original", "latest_pointer_and_all_eight_artifacts_exact_original", "roy_schedule_outputs_lease_unchanged"):
        audit[key] = True
    members = {BASE + "observe_current_rerun.py": OBSERVER}
    for name, value in {"observer-latest.json": observed, "observer-host-proof.json": host,
                        "failure/boundary-audit.json": audit, "failure/unchanged-output-heads.json": outputs,
                        "schedule-before.json": original, "failure/generation.json": generation}.items():
        members[BASE + name] = release.canonical(value)
    members.update({BASE + "failure/journal/" + key.rsplit("/", 1)[1]: raw for key, raw in raws.items()})
    task = {"taskArn": task_arn, "clusterArn": release.CLUSTER, "taskDefinitionArn": DEFINITION, "startedBy": OWNER,
            "launchType": "FARGATE", "lastStatus": "STOPPED", "desiredStatus": "STOPPED", "startedAt": START, "stoppedAt": STOP,
            "overrides": expected, "containers": [{"name": "reporting", "lastStatus": "STOPPED", "exitCode": 1,
                "imageDigest": identity["image_digest"], "image": IMAGE,
                "networkInterfaces": [{"privateIpv4Address": identity["private_ip"]}]}]}
    return records, paused, definition, raws, members, task


def bundle(members):
    manifest = {"at": "2026-10-22T10:20:00+00:00", "project": "vevo", "release_id": OWNER, "through": DAY,
                "email_sent": False, "status": "failed_boundary_verified", "generation": "20261021T233137Z",
                "files": {name: {"sha256": release.sha(raw), "bytes": len(raw)} for name, raw in members.items()}}
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, raw in {**members, "evidence-manifest.json": release.canonical(manifest)}.items():
            archive.writestr(name, raw)
    return stream.getvalue()


class AdmissionTests(unittest.TestCase):
    def setUp(self):
        self.records, self.paused, self.definition, self.raws, self.members, self.task = evidence()

    def chain(self):
        value = recovery.validate_chain(self.records, OWNER, self.paused, self.definition, GATE)
        value.update(receipt_sha256=release.sha(list(self.raws.values())[-1]), receipt_key=list(self.raws)[-1])
        return value

    def archive(self, value=None):
        raw = bundle(self.members)
        with patch.object(recovery, "OBSERVER_SHA256", release.sha(OBSERVER)):
            return recovery.validate_archive(raw, release.sha(raw), value or self.chain(), self.raws,
                                               {key: release.sha(raw) for key, raw in self.raws.items()})

    def test_exact_seven_receipts_and_hash_bound_projection_accept(self):
        result = self.archive()
        self.assertEqual(1, result["terminal_observation"]["exit_code"])
        self.assertEqual('"old-etag"', result["lease_etag"])
        self.assertNotIn("terminal_task", result)

    def test_chain_rejects_missing_extra_uncertain_or_wrong_identity(self):
        for change in ("missing", "extra", "project", "source", "controller", "phase", "promoted", "uncertain",
                       "command", "token", "task", "schedule", "image", "output", "fixed-date"):
            self.setUp()
            if change == "missing": self.records.pop(1)
            elif change == "extra": self.records.append(copy.deepcopy(self.records[-1]))
            elif change in {"project", "source", "controller", "phase", "promoted"}:
                key, value = {"project": ("project", "roy"), "source": ("source_commit", "0" * 40),
                              "controller": ("controller_commit", "0" * 40), "phase": ("phase", "candidate-registered"),
                              "promoted": ("promoted", True)}[change]
                self.records[3][key] = value
            elif change == "uncertain": self.records[-1]["start_uncertain"] = True
            elif change == "command": self.records[4]["overrides"]["containerOverrides"][0]["command"].append("unsafe")
            elif change == "token": self.records[4]["client_token"] = "foreign"
            elif change == "task": self.records[-1]["known_task_arns"]["live"] += "foreign"
            elif change == "schedule": self.paused["ScheduleExpression"] = "cron(1 1 * * ? *)"
            elif change == "image": self.definition["containerDefinitions"][0]["image"] = IMAGE[:-1] + "9"
            elif change == "output": self.records[0]["original_live_outputs"].pop(next(iter(self.records[0]["original_live_outputs"])))
            else: self.definition["containerDefinitions"][0]["environment"].append({"name": "REPORT_TO_DATE", "value": DAY})
            with self.subTest(change=change), self.assertRaises((RuntimeError, binding.BindingError)):
                self.chain()

    def test_archived_projection_must_match_host_journal_audit_and_approved_observer(self):
        changes = (("observer-latest.json", "exit_code", 0), ("observer-latest.json", "task_status", "RUNNING"),
                   ("observer-latest.json", "receipt_sha256", "0" * 64), ("observer-latest.json", "task_arn", "foreign"),
                   ("observer-latest.json", "task_overrides_verified", False),
                   ("failure/boundary-audit.json", "active_vevo_task_count", 1),
                   ("failure/boundary-audit.json", "vevo_lease_owner", NEW_OWNER),
                   ("failure/boundary-audit.json", "roy_schedule_outputs_lease_unchanged", False))
        for name, key, value in changes:
            self.setUp()
            row = json.loads(self.members[BASE + name]); row[key] = value
            self.members[BASE + name] = release.canonical(row)
            with self.subTest(name=name, key=key), self.assertRaises(RuntimeError): self.archive()
        self.setUp()
        self.members[BASE + "observe_current_rerun.py"] += b"changed"
        with self.assertRaisesRegex(RuntimeError, "observer-source"): self.archive()
        self.setUp()
        self.members[BASE + "failure/journal/0007-" + recovery.PHASES[-1] + ".json"] += b" "
        with self.assertRaisesRegex(RuntimeError, "archived-journal"): self.archive()

    def test_archive_hash_inventory_and_traversal_are_not_trusted(self):
        raw = bundle(self.members)
        with self.assertRaisesRegex(RuntimeError, "archive-hash"): recovery.archive_members(raw, "0" * 64)
        self.members["../escape"] = b"bad"
        raw = bundle(self.members)
        with self.assertRaisesRegex(RuntimeError, "archive-members"): recovery.archive_members(raw, release.sha(raw))
        self.members.pop("../escape")
        raw = bundle(self.members)
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            members = {name: archive.read(name) for name in archive.namelist()}
        manifest = json.loads(members["evidence-manifest.json"])
        manifest["files"][BASE + "observer-latest.json"]["sha256"] = "0" * 64
        out = io.BytesIO()
        with zipfile.ZipFile(out, "w") as archive:
            for name, value in members.items(): archive.writestr(name, release.canonical(manifest) if name == "evidence-manifest.json" else value)
        raw = out.getvalue()
        with self.assertRaisesRegex(RuntimeError, "archive-inventory"): recovery.archive_members(raw, release.sha(raw))

    def test_empty_optional_log_is_valid_evidence_but_not_a_required_json_document(self):
        self.members[BASE + "empty.stderr.log"] = b""
        self.archive()
        self.members[BASE + "observer-latest.json"] = b""
        with self.assertRaises(ValueError): self.archive()

    def controller(self):
        obj = object.__new__(release.ImageRelease)
        obj.policy, obj.release_id, obj.recover_current_rerun, obj.recover_paused_release = release.VEVO, NEW_OWNER, OWNER, None
        obj.original, obj.original_source = self.paused, self.definition
        obj.protected, obj.peer_snapshot = self.records[0]["protected_schedules"], self.records[0]["protected_peer"]
        obj.schedule, obj.definition = Mock(return_value=self.paused), Mock(return_value=self.definition)
        obj.all_schedules = Mock(return_value={release.VEVO.service: self.paused, **obj.protected})
        obj.live_outputs = Mock(return_value=self.records[0]["original_live_outputs"])
        obj.verify_peer_boundary, obj.no_report_tasks, obj.ecs, obj.s3 = Mock(), Mock(), Mock(), Mock()
        obj.ecs.describe_tasks.return_value = {"tasks": [self.task], "failures": []}
        obj.recovery_receipt_sha256 = release.sha(list(self.raws.values())[-1])
        raw = bundle(self.members)
        obj.recovery_readback_sha256 = release.sha(raw)
        obj.recovery_readback_key = "data/vevo/reporting/reruns/20261022-through-20261021/" + release.sha(raw) + "/failure-evidence.zip"
        blobs = {**self.raws, obj.recovery_readback_key: raw}
        obj.s3.list_objects_v2.return_value = {"Contents": [{"Key": key} for key in self.raws]}
        return obj, blobs

    def inspect(self, obj, blobs):
        with patch.object(recovery, "OBSERVER_SHA256", release.sha(OBSERVER)), \
             patch.object(recovery, "read_private", side_effect=lambda _s3, key, _limit: blobs[key]), \
             patch.object(release, "committed_gate_sha", return_value=GATE), \
             patch.object(recovery, "read_lease", return_value=({"owner": OWNER, "state": "uncertain"}, '"old-etag"')):
            return obj.inspect_paused_recovery()

    def test_adapter_reads_without_writes_and_labels_fresh_vs_expired_evidence(self):
        for missing in (False, True):
            obj, blobs = self.controller()
            if missing: obj.ecs.describe_tasks.return_value = {"tasks": [], "failures": [{"arn": self.task["taskArn"], "reason": "MISSING"}]}
            proof = self.inspect(obj, blobs)
            self.assertEqual("current-rerun", proof["kind"])
            self.assertEqual(missing, proof["terminal_evidence_kind"].endswith("ecs-missing"))
            obj.ecs.run_task.assert_not_called(); obj.s3.put_object.assert_not_called()
            obj.verify_peer_boundary.assert_called_once(); obj.no_report_tasks.assert_called_once()

    def test_fresh_task_conflicts_or_non_missing_failure_never_fall_back(self):
        for change in ("denied", "foreign-missing", "empty", "running", "exit", "owner", "image", "override", "time", "ip"):
            self.setUp(); obj, blobs = self.controller()
            if change == "denied": obj.ecs.describe_tasks.side_effect = OSError("denied")
            elif change == "foreign-missing": obj.ecs.describe_tasks.return_value = {"tasks": [], "failures": [{"arn": "foreign", "reason": "MISSING"}]}
            elif change == "empty": obj.ecs.describe_tasks.return_value = {"tasks": [], "failures": []}
            elif change == "running": self.task["lastStatus"] = "RUNNING"
            elif change == "exit": self.task["containers"][0]["exitCode"] = 0
            elif change == "owner": self.task["startedBy"] = NEW_OWNER
            elif change == "image": self.task["containers"][0]["imageDigest"] = "foreign"
            elif change == "override": self.task["overrides"] = {}
            elif change == "time": self.task["stoppedAt"] = START
            else: self.task["containers"][0]["networkInterfaces"][0]["privateIpv4Address"] = "172.31.9.9"
            with self.subTest(change=change), self.assertRaises((RuntimeError, OSError)): self.inspect(obj, blobs)
            obj.s3.put_object.assert_not_called(); obj.ecs.run_task.assert_not_called()

    def test_archive_receipt_protected_output_and_lease_drift_fail_before_handoff(self):
        for change in ("receipt", "archive", "truncated", "schedule", "outputs", "protected", "peer", "etag"):
            self.setUp(); obj, blobs = self.controller()
            if change == "receipt": obj.recovery_receipt_sha256 = "0" * 64
            elif change == "archive": blobs[obj.recovery_readback_key] += b"changed"
            elif change == "truncated": obj.s3.list_objects_v2.return_value["IsTruncated"] = True
            elif change == "schedule": obj.schedule.return_value = {**self.paused, "State": "ENABLED"}
            elif change == "outputs": obj.live_outputs.return_value = {}
            elif change == "protected": obj.all_schedules.return_value = {release.VEVO.service: self.paused}
            elif change == "peer": obj.peer_snapshot = {"project": "foreign"}
            else:
                audit = json.loads(self.members[BASE + "failure/boundary-audit.json"]); audit["vevo_lease_etag"] = "foreign"
                self.members[BASE + "failure/boundary-audit.json"] = release.canonical(audit)
                obj, blobs = self.controller()
            with self.subTest(change=change), self.assertRaises(RuntimeError): self.inspect(obj, blobs)
            obj.s3.put_object.assert_not_called(); obj.ecs.run_task.assert_not_called()

    def test_cross_project_or_mixed_recovery_modes_reject_before_clients(self):
        for fields in ({"project": "roy"}, {"recover_paused_release": OWNER}, {"recovery_readback_key": None}):
            kwargs = {"recover_current_rerun": OWNER, "recovery_receipt_sha256": "a" * 64,
                      "recovery_readback_key": "private", "recovery_readback_sha256": "b" * 64, **fields}
            session = Mock()
            with self.subTest(fields=fields), self.assertRaisesRegex(RuntimeError, "current-recovery-arguments"):
                release.ImageRelease(session, HEAD, DAY, 7200, **kwargs)
            session.client.assert_not_called()


class OwnershipTests(unittest.TestCase):
    def test_new_cli_path_disables_sdk_retries_without_changing_existing_path(self):
        for recovering in (False, True):
            session, original_client = Mock(), Mock()
            session.client = original_client
            argv = ["release", "run", "--commit", HEAD, "--to-date", DAY]
            if recovering:
                argv += ["--recover-current-rerun", OWNER, "--recovery-receipt-sha256", "a" * 64,
                         "--recovery-readback-key", "private-archive", "--recovery-readback-sha256", "b" * 64]
            with self.subTest(recovering=recovering), patch.object(release.sys, "argv", argv), \
                 patch("boto3.Session", return_value=session), patch.object(release, "ImageRelease") as controller:
                release.main()
                session.client("ecs")
                self.assertEqual(1 if recovering else 2, original_client.call_args.kwargs["config"].retries["total_max_attempts"])
                controller.return_value.run.assert_called_once_with()

    def test_existing_cas_handoff_never_releases_and_foreign_etag_blocks(self):
        store = MemoryS3()
        old = binding.MigrationLease(store, owner=OWNER, now=lambda: NOW).acquire()
        old.retain_uncertain()
        new = binding.MigrationLease(store, owner=NEW_OWNER, now=lambda: NOW)
        store.writes.clear()
        with self.assertRaisesRegex(RuntimeError, "lease-drift"):
            release.transfer_recovery_lease(new, OWNER, '"stale"')
        self.assertEqual([], store.writes)
        release.transfer_recovery_lease(new, OWNER, old.etag)
        self.assertEqual([binding.LOCK_KEY], store.writes)
        value = json.loads(store.objects[binding.LOCK_KEY][0])
        self.assertEqual((NEW_OWNER, "active"), (value["owner"], value["state"]))
        self.assertNotEqual(old.etag, new.etag)

    def test_cas_race_does_not_overwrite_foreign_owner(self):
        store = MemoryS3()
        old = binding.MigrationLease(store, owner=OWNER, now=lambda: NOW).acquire(); old.retain_uncertain()
        new = binding.MigrationLease(store, owner=NEW_OWNER, now=lambda: NOW)
        store.writes.clear()
        def race(key):
            raw, _, encryption = store.objects[key]
            body = json.loads(raw); body.update(owner="9" * 32, state="active")
            store.objects[key] = (binding.canonical_bytes(body), '"foreign"', encryption)
        store.before_put = race
        with self.assertRaises(StoreError): release.transfer_recovery_lease(new, OWNER, old.etag)
        self.assertEqual([], store.writes)


if __name__ == "__main__":
    unittest.main()
