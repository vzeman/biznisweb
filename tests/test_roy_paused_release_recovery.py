"""Synthetic cross-project and conditional-ownership recovery regressions."""
import copy
import json
import unittest
from unittest.mock import Mock, patch

from botocore.exceptions import ClientError

from scripts import reporting_runtime_binding as binding
from scripts import vevo_report_image_release as release
from scripts.reporting_image_release_lease import ScopedImageLease, read_lease
from scripts.reporting_image_release_policy import PROJECTS, VEVO
from tests.test_reporting_runtime_binding import MemoryS3, NOW, StoreError
from tests.test_roy_report_image_release import roy_definition
from tests.test_vevo_report_image_release import DATE, IMAGE


ROY = PROJECTS["roy"]
OLD_OWNER, NEW_OWNER = "c" * 32, "9" * 32
COMMIT, GATE = "d" * 40, "e" * 64


def stopped_roy_fixture():
    source = roy_definition()
    prefix = f"arn:aws:ecs:{binding.REGION}:{binding.ACCOUNT}:task-definition/"
    source["taskDefinitionArn"] = prefix + ROY.family + ":42"
    candidate_arn = prefix + ROY.family + ":44"
    scheduled = {"Name": ROY.service, "State": "ENABLED", "Target": {
        "Arn": release.CLUSTER, "EcsParameters": {
            "TaskDefinitionArn": source["taskDefinitionArn"], "LaunchType": "FARGATE"}}}
    paused = copy.deepcopy(scheduled)
    paused["State"] = "DISABLED"
    paused["Target"]["EcsParameters"]["TaskDefinitionArn"] = candidate_arn
    current = copy.deepcopy(source)
    current["containerDefinitions"][0]["image"] = IMAGE
    current.update(taskDefinitionArn=candidate_arn, status="ACTIVE")
    task_prefix = release.CLUSTER.replace(":cluster/", ":task/") + "/"
    identity = {"project": "roy", "marker": ROY.probe_marker, "release_id": OLD_OWNER,
                "source_commit": COMMIT, "task_arn": task_prefix + "a" * 32,
                "task_definition": candidate_arn, "private_ip": "172.31.1.2",
                "image_digest": IMAGE.rsplit("@", 1)[1], "instance_id": "N/A:Fargate",
                "service": ROY.service, "path": "/app", "gate_sha256": GATE}
    tasks = []
    for mode, letter in (("probe", "a"), ("live", "b")):
        tasks.append({"taskArn": task_prefix + letter * 32, "clusterArn": release.CLUSTER,
                      "taskDefinitionArn": candidate_arn, "startedBy": OLD_OWNER, "launchType": "FARGATE",
                      "lastStatus": "STOPPED", "desiredStatus": "STOPPED",
                      "startedAt": "2026-10-07T12:00:00+00:00", "stoppedAt": "2026-10-07T13:00:00+00:00",
                      "overrides": release.task_overrides(OLD_OWNER, COMMIT, IMAGE, GATE, DATE,
                                                          probe=mode == "probe", policy=ROY),
                      "containers": [{"name": "reporting", "lastStatus": "STOPPED",
                                      "exitCode": 0 if mode == "probe" else 137,
                                      "imageDigest": identity["image_digest"], "image": IMAGE,
                                      "networkInterfaces": [{"privateIpv4Address":
                                          "172.31.1.2" if mode == "probe" else "172.31.1.3"}]}]})
    complete = {**identity, "phase": "report-verified",
                "localhost_marker_sha256": release.sha(release.canonical(identity)),
                "provider_writes": False, "email_sent": False, "live_outputs_changed": False,
                "skip_invoices": True, "skip_inline_guard": True}
    common = {"schema_version": 1, "project": "roy", "release_id": OLD_OWNER,
              "source_commit": COMMIT, "report_to_date": DATE, "at": "2026-10-07T13:01:00+00:00"}
    outputs = {ROY.sink + "/latest/generation.json": {"ETag": "original"}}
    records = [
        {**common, "phase": "preflight-complete", "schedule": scheduled, "source_definition": source,
         "image": IMAGE, "original_live_outputs": outputs, "protected_schedules": {}},
        {**common, "phase": "candidate-registered", "task_definition": candidate_arn},
        {**common, "phase": "probe-complete", "terminal_task": tasks[0], "identity": identity, "complete": complete},
        {**common, "phase": "live-dispatch-requested", "task_definition": candidate_arn, "overrides": tasks[1]["overrides"]},
        {**common, "phase": "live-dispatched", "task_arn": tasks[1]["taskArn"]},
        {**common, "phase": "live-failure-paused-review-required", "promoted": True, "rerun_attempted": True,
         "known_schedule": paused, "known_task_arns": dict(zip(("probe", "live"), [t["taskArn"] for t in tasks])),
         "live_outputs": copy.deepcopy(outputs)},
    ]
    return records, paused, current, tasks


def lease_store():
    store = MemoryS3()
    peer = binding.MigrationLease(store, owner="a" * 32, now=lambda: NOW).acquire()
    peer.release()
    old = ScopedImageLease(store, owner=OLD_OWNER, project="roy", now=lambda: NOW).acquire()
    old.retain_uncertain()
    new = ScopedImageLease(store, owner=NEW_OWNER, project="roy", now=lambda: NOW)
    store.writes.clear()
    return store, old, new


class RoyRecoveryIdentityTests(unittest.TestCase):
    def validate(self, records, paused, current):
        return release.validate_recovery_receipts(records, OLD_OWNER, paused, current, policy=ROY)

    def test_exact_roy_chain_and_tasks_accept_without_live_success_claim(self):
        records, paused, current, tasks = stopped_roy_fixture()
        recovery = self.validate(records, paused, current)
        self.assertEqual(tasks, release.validate_stopped_recovery_tasks(recovery, tasks, policy=ROY))
        self.assertNotIn("live-complete", [record["phase"] for record in records])
        with self.assertRaises(RuntimeError):
            release.validate_recovery_receipts(records, OLD_OWNER, paused, current, policy=VEVO)

    def test_missing_or_cross_project_receipt_is_not_legacy_roy_evidence(self):
        for index in range(6):
            for project in (None, "vevo", "unconfigured"):
                records, paused, current, _ = stopped_roy_fixture()
                if project is None:
                    records[index].pop("project")
                else:
                    records[index]["project"] = project
                with self.subTest(index=index, project=project), self.assertRaises(RuntimeError):
                    self.validate(records, paused, current)

    def test_cross_project_schedule_definition_marker_and_commands_reject(self):
        for change in ("service", "family", "role", "sink", "marker", "live-command", "date", "outputs"):
            records, paused, current, _ = stopped_roy_fixture()
            if change == "service":
                records[0]["schedule"]["Name"] = VEVO.service
            elif change == "family":
                records[1]["task_definition"] = records[1]["task_definition"].replace(ROY.family, VEVO.family)
            elif change == "role":
                records[0]["source_definition"]["taskRoleArn"] += "-foreign"
            elif change == "sink":
                for row in records[0]["source_definition"]["containerDefinitions"][0]["environment"]:
                    if row["name"] == "REPORT_S3_PREFIX":
                        row["value"] = VEVO.sink
            elif change == "marker":
                records[2]["identity"]["marker"] = VEVO.probe_marker
            elif change == "live-command":
                records[3]["overrides"] = release.task_overrides(OLD_OWNER, COMMIT, IMAGE, GATE, DATE, probe=False)
            elif change == "date":
                records[3]["report_to_date"] = "2026-10-05"
            else:
                records[-1]["live_outputs"] = {"new-generation": {"ETag": "changed"}}
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                self.validate(records, paused, current)

    def test_roy_tasks_require_project_role_command_owner_and_probe_zero_exit(self):
        for change in ("probe-role", "live-command", "owner", "running", "exit", "ip", "digest"):
            records, paused, current, tasks = stopped_roy_fixture()
            recovery = self.validate(records, paused, current)
            if change == "probe-role":
                tasks[0]["overrides"]["taskRoleArn"] = tasks[0]["overrides"]["taskRoleArn"].replace(ROY.role_prefix, VEVO.role_prefix)
            elif change == "live-command":
                tasks[1]["overrides"] = release.task_overrides(OLD_OWNER, COMMIT, IMAGE, GATE, DATE, probe=False)
            elif change == "owner":
                tasks[1]["startedBy"] = NEW_OWNER
            elif change == "running":
                tasks[1]["desiredStatus"] = "RUNNING"
            elif change == "exit":
                tasks[0]["containers"][0]["exitCode"] = 137
            elif change == "ip":
                tasks[0]["containers"][0]["networkInterfaces"][0]["privateIpv4Address"] = "172.31.1.9"
            else:
                tasks[1]["containers"][0]["imageDigest"] = "sha256:" + "0" * 64
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                release.validate_stopped_recovery_tasks(recovery, tasks, policy=ROY)

    def inspection(self):
        records, paused, current, tasks = stopped_roy_fixture()
        store, old, lease = lease_store()
        obj = object.__new__(release.ImageRelease)
        obj.policy, obj.s3, obj.lease = ROY, store, lease
        obj.release_id, obj.recover_paused_release = NEW_OWNER, OLD_OWNER
        obj.original, obj.original_source, obj.protected = paused, current, {}
        obj.schedule, obj.definition = Mock(return_value=paused), Mock(return_value=current)
        obj.live_outputs = Mock(return_value=records[0]["original_live_outputs"])
        obj.no_report_tasks, obj.ecs, obj.iam = Mock(), Mock(), Mock()
        obj.ecs.describe_tasks.return_value = {"tasks": tasks, "failures": []}
        obj.iam.get_role.side_effect = ClientError({"Error": {"Code": "NoSuchEntity"}}, "GetRole")
        prefix = ROY.release_prefix + OLD_OWNER + "/"
        blobs = {prefix + f"{index:04d}-{row['phase']}.json": release.canonical(row)
                 for index, row in enumerate(records, 1)}
        store.list_objects_v2 = Mock(return_value={"Contents": [{"Key": key} for key in blobs]})
        obj.recovery_receipt_sha256 = release.sha(blobs[sorted(blobs)[-1]])
        archive = {"project": "roy", "release_id": OLD_OWNER, "source_commit": COMMIT, "read_only": True,
                   "preflight_sha256": release.sha(blobs[sorted(blobs)[0]]),
                   "failed_receipt_sha256": obj.recovery_receipt_sha256, "schedule": paused,
                   "live_outputs": records[0]["original_live_outputs"], "active_reporting_task_arns": [],
                   "probe_role_absent": True, "candidate_image": IMAGE, "candidate_status": "ACTIVE",
                   "lease": old._read()[0], "at": "2026-10-07T13:05:00+00:00",
                   "owned_tasks": dict(zip(("probe", "live"), tasks))}
        obj.recovery_readback_key = "data/roy/audit/stopped.json"
        blobs[obj.recovery_readback_key] = release.canonical(archive)
        obj.recovery_readback_sha256 = release.sha(blobs[obj.recovery_readback_key])
        return obj, blobs, archive

    def inspect(self, obj, blobs):
        with patch.object(release, "read_private", side_effect=lambda _s3, key, _limit: blobs[key]), \
             patch.object(release, "committed_gate_sha", return_value=GATE):
            return obj.inspect_paused_recovery()

    def test_fresh_and_archived_roy_boundary_use_only_matching_role_and_lease(self):
        for expired in (False, True):
            obj, blobs, _ = self.inspection()
            before = copy.deepcopy(obj.s3.objects)
            if expired:
                tasks = obj.ecs.describe_tasks.return_value["tasks"]
                obj.ecs.describe_tasks.return_value = {"tasks": [], "failures": [
                    {"arn": task["taskArn"], "reason": "MISSING"} for task in tasks]}
            with self.subTest(expired=expired):
                proof = self.inspect(obj, blobs)
            self.assertEqual(2 if expired else 0, len(proof["archived_task_arns"]))
            obj.iam.get_role.assert_called_once_with(RoleName=ROY.role_prefix + OLD_OWNER)
            obj.no_report_tasks.assert_called_once()
            obj.ecs.run_task.assert_not_called()
            self.assertEqual(before, obj.s3.objects)
            self.assertEqual([], obj.s3.writes)

    def test_archive_hash_project_provenance_and_namespace_tampering_never_writes(self):
        for change in ("receipt-hash", "archive-hash", "project", "missing-project", "prefix", "preflight",
                       "source", "owner", "outputs", "time", "active", "role", "sequence", "truncated"):
            obj, blobs, archive = self.inspection()
            before = copy.deepcopy(obj.s3.objects)
            if change == "receipt-hash":
                obj.recovery_receipt_sha256 = "0" * 64
            elif change == "archive-hash":
                obj.recovery_readback_sha256 = "0" * 64
            elif change == "prefix":
                obj.recovery_readback_key = "data/vevo/audit/stopped.json"
            elif change == "sequence":
                obj.s3.list_objects_v2.return_value["Contents"].pop(1)
            elif change == "truncated":
                obj.s3.list_objects_v2.return_value["IsTruncated"] = True
            else:
                if change == "missing-project":
                    archive.pop("project")
                else:
                    field, value = {"project": ("project", "vevo"), "preflight": ("preflight_sha256", "0" * 64),
                                    "source": ("source_commit", "0" * 40), "owner": ("release_id", NEW_OWNER),
                                    "outputs": ("live_outputs", {}), "time": ("at", "2026-10-07T12:00:00+00:00"),
                                    "active": ("active_reporting_task_arns", ["foreign"]),
                                    "role": ("probe_role_absent", False)}[change]
                    archive[field] = value
                blobs[obj.recovery_readback_key] = release.canonical(archive)
                obj.recovery_readback_sha256 = release.sha(blobs[obj.recovery_readback_key])
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                self.inspect(obj, blobs)
            self.assertEqual(before, obj.s3.objects)
            self.assertEqual([], obj.s3.writes)

    def test_archive_never_masks_live_task_or_authorization_failure(self):
        for change in ("access-denied", "running", "foreign-owner", "role-read-error"):
            obj, blobs, _ = self.inspection()
            if change == "access-denied":
                tasks = obj.ecs.describe_tasks.return_value["tasks"]
                obj.ecs.describe_tasks.return_value = {"tasks": [], "failures": [
                    {"arn": tasks[0]["taskArn"], "reason": "AccessDenied"}]}
            elif change == "role-read-error":
                obj.iam.get_role.side_effect = ClientError({"Error": {"Code": "AccessDenied"}}, "GetRole")
            else:
                observed = copy.deepcopy(obj.ecs.describe_tasks.return_value)
                observed["tasks"][1]["lastStatus" if change == "running" else "startedBy"] = (
                    "RUNNING" if change == "running" else NEW_OWNER)
                obj.ecs.describe_tasks.return_value = observed
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                self.inspect(obj, blobs)
            self.assertEqual([], obj.s3.writes)


class RoyRecoveryLeaseTests(unittest.TestCase):
    def test_handoff_and_restore_use_roy_cas_without_touching_peer_or_releasing(self):
        store, old, new = lease_store()
        peer_before = store.objects[VEVO.lease_key]
        with patch.object(store, "put_object", wraps=store.put_object) as put:
            release.transfer_recovery_lease(new, OLD_OWNER, old.etag, policy=ROY)
            self.assertEqual(old.etag, put.call_args.kwargs["IfMatch"])
            self.assertEqual(ROY.lease_key, put.call_args.kwargs["Key"])
        self.assertEqual(NEW_OWNER, read_lease(store, "roy")[0]["owner"])
        obj = object.__new__(release.ImageRelease)
        obj.policy, obj.s3, obj.lease, obj.event, obj.lease_owned = ROY, store, new, Mock(), True
        obj.recovery = {"previous_owner": OLD_OWNER}
        obj.verify_peer_boundary = Mock()
        with patch.object(new, "_check", wraps=new._check) as check:
            obj.restore_paused_recovery_lease()
        check.assert_called_once_with(owned=True)
        obj.verify_peer_boundary.assert_called_once()
        restored, _ = read_lease(store, "roy")
        self.assertEqual((OLD_OWNER, "uncertain"), (restored["owner"], restored["state"]))
        self.assertFalse(obj.lease_owned)
        self.assertEqual([ROY.lease_key, ROY.lease_key], store.writes)
        self.assertEqual(peer_before, store.objects[VEVO.lease_key])

    def test_changed_owner_state_or_etag_cannot_be_stolen(self):
        for change in ("owner", "state", "etag"):
            store, old, new = lease_store()
            raw, etag, encryption = store.objects[ROY.lease_key]
            body = json.loads(raw)
            if change == "etag":
                etag = '"newer"'
            else:
                body[change] = "8" * 32 if change == "owner" else "active"
            store.objects[ROY.lease_key] = (binding.canonical_bytes(body), etag, encryption)
            before = copy.deepcopy(store.objects)
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                release.transfer_recovery_lease(new, OLD_OWNER, old.etag, policy=ROY)
            self.assertEqual(before, store.objects)
            self.assertEqual([], store.writes)

    def test_cas_race_preserves_foreign_generation_and_does_not_retry(self):
        store, old, new = lease_store()
        peer_before = store.objects[VEVO.lease_key]
        calls = []

        def race(key):
            calls.append(key)
            raw, _, encryption = store.objects[key]
            body = json.loads(raw)
            body.update(owner="8" * 32, state="active", generation="7" * 32)
            store.objects[key] = (binding.canonical_bytes(body), '"foreign"', encryption)

        store.before_put = race
        with self.assertRaises(StoreError):
            release.transfer_recovery_lease(new, OLD_OWNER, old.etag, policy=ROY)
        self.assertEqual([ROY.lease_key], calls)
        self.assertEqual([], store.writes)
        self.assertEqual("8" * 32, read_lease(store, "roy")[0]["owner"])
        self.assertEqual(peer_before, store.objects[VEVO.lease_key])

    def recovery_controller(self, store, old, new):
        obj = object.__new__(release.ImageRelease)
        obj.policy, obj.s3, obj.lease, obj.release_id = ROY, store, new, NEW_OWNER
        obj.recovery = {"previous_owner": OLD_OWNER, "lease_etag": old.etag}
        obj.recovery_receipt_sha256 = "e" * 64
        obj.verify_paused_boundary, obj.verify_peer_boundary = Mock(), Mock()
        obj.all_schedules, obj.protected = Mock(return_value={ROY.service: {}}), {}
        obj.event, obj.lease_owned, obj.ecs = Mock(), False, Mock()
        return obj

    def test_post_transfer_peer_change_retains_only_new_roy_uncertain_owner(self):
        store, old, new = lease_store()
        peer_before = store.objects[VEVO.lease_key]
        obj = self.recovery_controller(store, old, new)
        obj.verify_peer_boundary.side_effect = [None, RuntimeError("peer-changed")]
        with self.assertRaisesRegex(RuntimeError, "peer-changed"):
            obj.acquire_release_lease()
        body, _ = read_lease(store, "roy")
        self.assertEqual((NEW_OWNER, "uncertain"), (body["owner"], body["state"]))
        self.assertEqual([ROY.lease_key, ROY.lease_key], store.writes)
        self.assertEqual(peer_before, store.objects[VEVO.lease_key])
        obj.ecs.run_task.assert_not_called()

    def test_lost_transfer_ack_never_replays_active_write_or_changes_peer(self):
        for inspection_available in (True, False):
            store, old, new = lease_store()
            peer_before = store.objects[VEVO.lease_key]
            obj = self.recovery_controller(store, old, new)
            put = store.put_object
            attempted_states = []
            reads_after_commit = 0

            def lost_ack(**kwargs):
                state = json.loads(kwargs["Body"])["state"]
                attempted_states.append(state)
                result = put(**kwargs)
                if state == "active":
                    raise OSError("lost-write-response")
                return result

            def unavailable_read(key):
                nonlocal reads_after_commit
                if key == ROY.lease_key and attempted_states:
                    reads_after_commit += 1
                    if reads_after_commit == 1 or not inspection_available:
                        raise OSError("read-unavailable")

            store.before_get = unavailable_read
            with self.subTest(inspection_available=inspection_available), \
                 patch.object(store, "put_object", side_effect=lost_ack), self.assertRaises(OSError):
                obj.acquire_release_lease()
            store.before_get = None
            body, _ = read_lease(store, "roy")
            self.assertEqual(["active", "uncertain"] if inspection_available else ["active"], attempted_states)
            self.assertEqual("uncertain" if inspection_available else "active", body["state"])
            self.assertEqual(NEW_OWNER, body["owner"])
            self.assertFalse(obj.lease_owned)
            self.assertEqual(peer_before, store.objects[VEVO.lease_key])
            obj.ecs.run_task.assert_not_called()

    def test_restore_rejects_foreign_or_stale_owned_generation(self):
        for change in ("owner", "etag", "state"):
            store, old, new = lease_store()
            release.transfer_recovery_lease(new, OLD_OWNER, old.etag, policy=ROY)
            raw, etag, encryption = store.objects[ROY.lease_key]
            body = json.loads(raw)
            if change == "etag":
                etag = '"newer"'
            else:
                body[change] = "8" * 32 if change == "owner" else "uncertain"
            store.objects[ROY.lease_key] = (binding.canonical_bytes(body), etag, encryption)
            before = copy.deepcopy(store.objects)
            obj = object.__new__(release.ImageRelease)
            obj.policy, obj.s3, obj.lease, obj.event = ROY, store, new, Mock()
            obj.recovery = {"previous_owner": OLD_OWNER}
            obj.verify_peer_boundary = Mock()
            with self.subTest(change=change), self.assertRaisesRegex(binding.BindingError, "runtime-lease-not-owned"):
                obj.restore_paused_recovery_lease()
            self.assertEqual(before, store.objects)
            self.assertEqual([ROY.lease_key], store.writes)

    def test_restore_peer_drift_blocks_before_old_owner_is_returned(self):
        store, old, new = lease_store()
        release.transfer_recovery_lease(new, OLD_OWNER, old.etag, policy=ROY)
        before = copy.deepcopy(store.objects)
        obj = self.recovery_controller(store, old, new)
        obj.verify_peer_boundary.side_effect = RuntimeError("peer-changed")
        with self.assertRaisesRegex(RuntimeError, "peer-changed"):
            obj.restore_paused_recovery_lease()
        self.assertEqual(before, store.objects)
        self.assertEqual([ROY.lease_key], store.writes)


if __name__ == "__main__":
    unittest.main()
