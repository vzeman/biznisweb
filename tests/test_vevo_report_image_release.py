import copy
import json
import unittest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError

from scripts import vevo_report_image_release as release
from scripts.vevo_report_image_host import report_arguments


IMAGE = f"{release.ACCOUNT}.dkr.ecr.{release.REGION}.amazonaws.com/vevo-reporting@sha256:" + "a" * 64
OLD_IMAGE = IMAGE[:-64] + "b" * 64
DATE = "2026-10-06"


class ProbeFailureEvidenceTests(unittest.TestCase):
    def fixture(self):
        obj = object.__new__(release.ImageRelease)
        obj.prefix = release.VEVO.probe_prefix + 'a' * 32 + '/'
        obj.report_from_date, obj.to_date = '2025-05-03', DATE
        obj.s3, obj.event = Mock(), Mock()
        ready = {'release_id': 'a' * 32, 'project': 'vevo', 'task_arn': 'owned-task', 'source_commit': 'b' * 40}
        diagnostic = b'{"qa_status":"critical","qa_errors":["synthetic private failure"]}'
        entry = {'status': 'available', 'key': obj.prefix + 'artifacts/diagnostics/data_quality.json',
                 'sha256': release.sha(diagnostic), 'size': len(diagnostic)}
        failed = {**ready, 'schema_version': 1, 'phase': 'report-rejected',
                  'localhost_marker_sha256': release.sha(release.canonical(ready)),
                  'report_from_date': obj.report_from_date, 'report_to_date': DATE,
                  'failure_type': 'RuntimeError', 'diagnostic': entry}
        obj.fetch = Mock(return_value=diagnostic)
        return obj, ready, failed

    def test_failure_receipt_contains_only_private_references_and_hashes(self):
        obj, ready, failed = self.fixture()
        with patch.object(release, 'read_private', return_value=release.canonical(failed)):
            obj.record_probe_failure(ready)
        self.assertEqual('probe-failure-diagnostics', obj.event.call_args.args[0])
        self.assertEqual(failed['diagnostic'], obj.event.call_args.kwargs['diagnostic'])
        self.assertNotIn('synthetic private failure', repr(obj.event.call_args))

    def test_foreign_task_project_source_dates_or_success_phase_cannot_be_evidence(self):
        for key, value in [('task_arn', 'foreign'), ('project', 'roy'), ('source_commit', 'f' * 40),
                           ('report_from_date', DATE), ('report_to_date', '2026-10-05'), ('phase', 'report-verified'),
                           ('localhost_marker_sha256', '0' * 64), ('output_manifest_key', 'untrusted')]:
            obj, ready, failed = self.fixture()
            failed[key] = value
            with self.subTest(field=key), patch.object(release, 'read_private', return_value=release.canonical(failed)):
                with self.assertRaisesRegex(RuntimeError, 'binding'):
                    obj.record_probe_failure(ready)
            obj.fetch.assert_not_called()
            obj.event.assert_not_called()

    def test_cross_project_diagnostic_path_size_or_hash_tampering_rejects(self):
        for field, value in [('key', 'daily-reports/vevo/latest/data_quality.json'), ('size', True),
                             ('size', release.DIAGNOSTIC_LIMIT + 1), ('sha256', '0' * 64)]:
            obj, ready, failed = self.fixture()
            failed['diagnostic'][field] = value
            with self.subTest(field=field), patch.object(release, 'read_private', return_value=release.canonical(failed)):
                with self.assertRaises(RuntimeError):
                    obj.record_probe_failure(ready)
            obj.event.assert_not_called()

    def test_absent_evidence_is_recorded_but_access_denied_is_not_treated_as_missing(self):
        for code in ('NoSuchKey', 'AccessDenied'):
            obj, ready, _failed = self.fixture()
            with self.subTest(code=code), patch.object(release, 'read_private', side_effect=ClientError({'Error': {'Code': code}}, 'GetObject')):
                if code == 'NoSuchKey':
                    obj.record_probe_failure(ready)
                    obj.event.assert_called_once_with('probe-failure-diagnostics', diagnostic_status='missing')
                else:
                    with self.assertRaisesRegex(RuntimeError, 'read-failed'):
                        obj.record_probe_failure(ready)

    def test_unavailable_file_marker_cannot_smuggle_an_extra_file_reference(self):
        obj, ready, failed = self.fixture()
        failed['diagnostic'] = {'status': 'missing'}
        with patch.object(release, 'read_private', return_value=release.canonical(failed)):
            obj.record_probe_failure(ready)
        obj.fetch.assert_not_called()
        failed['diagnostic']['key'] = 'foreign'
        with patch.object(release, 'read_private', return_value=release.canonical(failed)), self.assertRaisesRegex(RuntimeError, 'scope'):
            obj.record_probe_failure(ready)

    def test_terminal_failed_probe_still_rejects_after_preserving_diagnostics(self):
        obj, ready, _failed = self.fixture()
        ready['marker'] = obj.policy.probe_marker
        obj.release_id, obj.owned_task = ready['release_id'], ready['task_arn']
        obj.clock, obj.timeout = Mock(return_value=0), 100
        obj.checkpoint, obj.verify_host, obj.record_probe_failure = Mock(), Mock(), Mock()
        obj.task = Mock(return_value={'lastStatus': 'STOPPED', 'containers': [{'exitCode': 1}]})
        raw = release.canonical(ready)
        signal = release.canonical({'phase': 'host-authorized', 'release_id': obj.release_id,
                                    'task_arn': obj.owned_task, 'ready_sha256': release.sha(raw)})
        with patch.object(release, 'read_private', side_effect=[raw, signal]), self.assertRaisesRegex(RuntimeError, 'probe-not-successful'):
            obj.wait_probe()
        obj.record_probe_failure.assert_called_once_with(ready)
        self.assertFalse(any(call.args[0] == 'probe-complete' for call in obj.event.call_args_list))


def definition():
    return {"family": "vevo-reporting-daily", "networkMode": "awsvpc", "cpu": "1024", "memory": "2048",
            "taskRoleArn": f"arn:aws:iam::{release.ACCOUNT}:role/BiznisWebReportingTaskRole-vevo",
            "executionRoleArn": f"arn:aws:iam::{release.ACCOUNT}:role/ecsTaskExecutionRole",
            "containerDefinitions": [{"name": "reporting", "image": OLD_IMAGE,
                "environment": [{"name": key, "value": value} for key, value in {
                    "REPORT_PROJECT": "vevo", "REPORT_S3_BUCKET": release.BUCKET,
                    "REPORT_S3_PREFIX": "daily-reports/vevo", "REPORT_SKIP_INVOICES": "true"}.items()],
                "secrets": [{"name": "BIZNISWEB_API_TOKEN", "valueFrom": "existing-reference"}]}]}


def artifact_set(*, probe=False, end=DATE):
    prefix = "private/probe/" if probe else "daily-reports/vevo/20261007T120000Z/"
    quality = {"is_partial": False, "qa_status": "warning", "qa_failure_count": 0, "qa_errors": []}
    blobs = {}
    for name in release.EXPECTED_ARTIFACTS:
        if name.endswith(".html"):
            raw = b"<html>synthetic report</html>"
        else:
            period = "full" if name == "dashboard_payload_latest.json" else name.split("_")[-1].split(".")[0]
            raw = json.dumps({"project": "vevo", "date_to": end, "source_health": quality,
                              "period_switcher": {"current_key": period}}).encode()
        blobs[prefix + name] = raw
    if probe:
        blobs[prefix + "data_quality.json"] = json.dumps(quality).encode()
    entries = {key.removeprefix(prefix): {"key": key, "size": len(raw), "sha256": release.sha(raw)}
               for key, raw in blobs.items()}
    return {"project": "vevo", "artifacts": entries}, blobs, prefix


def stopped_release_fixture():
    old_id = "c" * 32
    commit, gate = "d" * 40, "e" * 64
    arn = f"arn:aws:ecs:{release.REGION}:{release.ACCOUNT}:task-definition/vevo-reporting-daily:44"
    task_prefix = f"arn:aws:ecs:{release.REGION}:{release.ACCOUNT}:task/vevo-reporting-cluster/"
    source = definition()
    source["taskDefinitionArn"] = arn[:-2] + "42"
    scheduled = {"Name": release.SERVICE, "State": "ENABLED", "Target": {
        "Arn": release.CLUSTER, "EcsParameters": {"TaskDefinitionArn": source["taskDefinitionArn"], "LaunchType": "FARGATE"}}}
    paused = copy.deepcopy(scheduled)
    paused["State"] = "DISABLED"
    paused["Target"]["EcsParameters"]["TaskDefinitionArn"] = arn
    current = release.image_only_definition(source, IMAGE)
    current.update(taskDefinitionArn=arn, status="ACTIVE")
    identity = {"marker": "VEVO_REPORT_PROBE_HOST_OK", "release_id": old_id, "source_commit": commit,
                "task_arn": task_prefix + "a" * 32, "task_definition": arn, "private_ip": "172.31.1.2",
                "image_digest": IMAGE.rsplit("@", 1)[1], "instance_id": "N/A:Fargate", "service": release.SERVICE,
                "path": "/app", "gate_sha256": gate}
    tasks = []
    for mode, letter in (("probe", "a"), ("live", "b")):
        tasks.append({"taskArn": task_prefix + letter * 32, "clusterArn": release.CLUSTER, "taskDefinitionArn": arn,
                      "startedBy": old_id, "launchType": "FARGATE", "lastStatus": "STOPPED", "desiredStatus": "STOPPED",
                      "stoppedAt": "2026-10-07T13:00:00+00:00", "startedAt": "2026-10-07T12:00:00+00:00",
                      "overrides": release.task_overrides(old_id, commit, IMAGE, gate, DATE, probe=mode == "probe"),
                      "containers": [{"name": "reporting", "lastStatus": "STOPPED", "exitCode": 0 if mode == "probe" else 137,
                                      "imageDigest": identity["image_digest"], "image": IMAGE,
                                      "networkInterfaces": [{"privateIpv4Address": "172.31.1.2" if mode == "probe" else "172.31.1.3"}]}]})
    complete = {**identity, "phase": "report-verified", "localhost_marker_sha256": release.sha(release.canonical(identity)),
                "provider_writes": False, "email_sent": False, "live_outputs_changed": False,
                "skip_invoices": True, "skip_inline_guard": True}
    common = {"schema_version": 1, "release_id": old_id, "source_commit": commit, "report_to_date": DATE,
              "at": "2026-10-07T13:01:00+00:00"}
    refs = {mode: task["taskArn"] for mode, task in zip(("probe", "live"), tasks)}
    records = [
        {**common, "phase": "preflight-complete", "schedule": scheduled, "source_definition": source,
         "image": IMAGE, "original_live_outputs": {"generation": {"ETag": "old"}}, "protected_schedules": {}},
        {**common, "phase": "candidate-registered", "task_definition": arn},
        {**common, "phase": "probe-complete", "terminal_task": tasks[0], "identity": identity, "complete": complete},
        {**common, "phase": "live-dispatch-requested", "task_definition": arn, "overrides": tasks[1]["overrides"]},
        {**common, "phase": "live-dispatched", "task_arn": tasks[1]["taskArn"]},
        {**common, "phase": "live-failure-paused-review-required", "promoted": True, "rerun_attempted": True,
         "known_schedule": paused, "known_task_arns": refs, "live_outputs": {"generation": {"ETag": "old"}}},
    ]
    return old_id, records, paused, current, tasks


class PausedRecoveryTests(unittest.TestCase):
    def fixture(self):
        old_id, records, paused, current, tasks = stopped_release_fixture()
        recovery = release.validate_recovery_receipts(records, old_id, paused, current)
        recovery.update(previous_owner=old_id, archived_readback=None)
        obj = object.__new__(release.ImageRelease)
        obj.recovery, obj.original, obj.original_source = recovery, paused, current
        obj.schedule = Mock(return_value=paused)
        obj.definition = Mock(return_value=current)
        obj.live_outputs = Mock(return_value=records[0]["original_live_outputs"])
        obj.no_report_tasks = Mock()
        obj.ecs, obj.iam, obj.s3 = Mock(), Mock(), Mock()
        obj.ecs.describe_tasks.return_value = {"tasks": tasks, "failures": []}
        obj.iam.get_role.side_effect = ClientError({"Error": {"Code": "NoSuchEntity"}}, "GetRole")
        lock = {"schema_version": 1, "owner": old_id, "state": "uncertain", "generation": "f" * 32,
                "updated_at": "2026-10-07T13:00:00+00:00", "expires_at": "2026-10-07T13:20:00+00:00"}
        return obj, lock, records, tasks

    def inspect_fixture(self, *, archived=False):
        obj, lock, records, tasks = self.fixture()
        obj.release_id, obj.recover_paused_release = "9" * 32, records[0]["release_id"]
        obj.protected = {}
        prefix = release.RELEASE_PREFIX + obj.recover_paused_release + "/"
        blobs = {prefix + f"{index:04d}-{row['phase']}.json": release.canonical(row) for index, row in enumerate(records, 1)}
        obj.recovery_receipt_sha256 = release.sha(blobs[sorted(blobs)[-1]])
        obj.s3.list_objects_v2.return_value = {"Contents": [{"Key": key} for key in blobs]}
        obj.recovery_readback_key = obj.recovery_readback_sha256 = None
        archive = None
        if archived:
            obj.recovery_readback_key = "data/vevo/audit/stopped.json"
            archive = {"release_id": obj.recover_paused_release, "source_commit": records[0]["source_commit"],
                       "read_only": True, "preflight_sha256": release.sha(blobs[sorted(blobs)[0]]),
                       "failed_receipt_sha256": obj.recovery_receipt_sha256,
                       "schedule": obj.original, "live_outputs": records[0]["original_live_outputs"],
                       "active_reporting_task_arns": [], "probe_role_absent": True, "candidate_image": IMAGE,
                       "candidate_status": "ACTIVE", "lease": lock, "at": "2026-10-07T13:05:00+00:00",
                       "owned_tasks": dict(zip(("probe", "live"), tasks))}
            blobs[obj.recovery_readback_key] = release.canonical(archive)
            obj.recovery_readback_sha256 = release.sha(blobs[obj.recovery_readback_key])
        return obj, lock, blobs, archive

    def test_inspection_binds_exact_failure_hash_and_explicit_archive(self):
        obj, lock, blobs, _ = self.inspect_fixture(archived=True)
        tasks = obj.ecs.describe_tasks.return_value["tasks"]
        obj.ecs.describe_tasks.return_value = {"tasks": [], "failures": [{"arn": task["taskArn"], "reason": "MISSING"} for task in tasks]}
        with patch.object(release, "read_private", side_effect=lambda s3, key, limit: blobs[key]), \
             patch.object(release, "committed_gate_sha", return_value="e" * 64), \
             patch.object(release.binding, "read_object", return_value=(lock, "etag")):
            proof = obj.inspect_paused_recovery()
        self.assertEqual(obj.recovery_receipt_sha256, proof["receipt_sha256"])
        self.assertEqual(obj.recovery_readback_sha256, proof["readback_sha256"])
        self.assertEqual(2, len(proof["archived_task_arns"]))
        obj.s3.put_object.assert_not_called()

    def test_bad_receipt_archive_hash_or_archive_identity_never_transfers_lease(self):
        for change in ("receipt-hash", "archive-hash", "archive-owner", "archive-outputs", "archive-active", "truncated", "sequence"):
            obj, lock, blobs, archive = self.inspect_fixture(archived=True)
            if change == "receipt-hash":
                obj.recovery_receipt_sha256 = "0" * 64
            elif change == "archive-hash":
                obj.recovery_readback_sha256 = "0" * 64
            elif change == "truncated":
                obj.s3.list_objects_v2.return_value["IsTruncated"] = True
            elif change == "sequence":
                obj.s3.list_objects_v2.return_value["Contents"].pop(1)
            else:
                if change == "archive-owner":
                    archive["release_id"] = "8" * 32
                elif change == "archive-outputs":
                    archive["live_outputs"] = {"new": True}
                else:
                    archive["owned_tasks"]["live"]["lastStatus"] = "RUNNING"
                blobs[obj.recovery_readback_key] = release.canonical(archive)
                obj.recovery_readback_sha256 = release.sha(blobs[obj.recovery_readback_key])
            with self.subTest(change=change), patch.object(release, "read_private", side_effect=lambda s3, key, limit: blobs[key]), \
                 patch.object(release, "committed_gate_sha", return_value="e" * 64), \
                 patch.object(release.binding, "read_object", return_value=(lock, "etag")), self.assertRaises(RuntimeError):
                obj.inspect_paused_recovery()
            obj.s3.put_object.assert_not_called()

    def test_repeated_recovery_requires_prior_explicit_enable_after_success_intent(self):
        old_id, records, paused, current, _ = stopped_release_fixture()
        records[0]["schedule"]["State"] = "DISABLED"
        with self.assertRaisesRegex(RuntimeError, "original-schedule"):
            release.validate_recovery_receipts(records, old_id, paused, current)
        records[0].update(final_schedule_state="ENABLED", recovered_from={"release_id": "8" * 32, "receipt_sha256": "f" * 64})
        release.validate_recovery_receipts(records, old_id, paused, current)

    def test_exact_stopped_chain_accepts_without_claiming_live_completion(self):
        old_id, records, paused, current, tasks = stopped_release_fixture()
        recovery = release.validate_recovery_receipts(records, old_id, paused, current)
        self.assertEqual(tasks, release.validate_stopped_recovery_tasks(recovery, tasks))
        self.assertNotIn("live-complete", [row["phase"] for row in records])

    def test_receipt_identity_schedule_definition_and_publication_drift_reject(self):
        for change in ("release", "source", "latest", "schedule", "definition", "published", "probe-marker", "live-command"):
            old_id, records, paused, current, _ = stopped_release_fixture()
            if change == "release":
                records[2]["release_id"] = "9" * 32
            elif change == "source":
                records[2]["source_commit"] = "9" * 40
            elif change == "latest":
                records.append({**records[-1], "phase": "unreviewed-later-action"})
            elif change == "schedule":
                paused = copy.deepcopy(paused)
                paused["State"] = "ENABLED"
            elif change == "definition":
                current["memory"] = "4096"
            elif change == "published":
                records[-1]["live_outputs"] = {"generation": {"ETag": "new"}}
            elif change == "probe-marker":
                records[2]["complete"]["localhost_marker_sha256"] = "0" * 64
            else:
                records[3]["overrides"] = {"containerOverrides": []}
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                release.validate_recovery_receipts(records, old_id, paused, current)

    def test_fresh_stopped_tasks_require_exact_owner_ip_digest_and_safe_command(self):
        for change in ("active", "foreign", "ip", "digest", "command"):
            old_id, records, paused, current, tasks = stopped_release_fixture()
            recovery = release.validate_recovery_receipts(records, old_id, paused, current)
            if change == "active":
                tasks[1]["lastStatus"] = "RUNNING"
            elif change == "foreign":
                tasks[1]["startedBy"] = "9" * 32
            elif change == "ip":
                tasks[0]["containers"][0]["networkInterfaces"] = [{"privateIpv4Address": "172.31.1.9"}]
            elif change == "digest":
                tasks[1]["containers"][0]["imageDigest"] = "sha256:" + "9" * 64
            else:
                tasks[1]["overrides"]["containerOverrides"][0]["command"] = ["python", "invoice_runner.py"]
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                release.validate_stopped_recovery_tasks(recovery, tasks)

    def test_current_boundary_is_fresh_and_read_only(self):
        obj, lock, _, _ = self.fixture()
        with patch.object(release.binding, "read_object", return_value=(lock, "etag")):
            obj.verify_paused_boundary()
        self.assertEqual("etag", obj.recovery["lease_etag"])
        obj.no_report_tasks.assert_called_once()
        obj.ecs.run_task.assert_not_called()
        obj.s3.put_object.assert_not_called()

    def test_current_output_foreign_lease_role_or_running_task_blocks_recovery(self):
        for change in ("outputs", "lease-owner", "lease-state", "lease-etag", "role", "running"):
            obj, lock, _, _ = self.fixture()
            if change == "outputs":
                obj.live_outputs.return_value = {"new": True}
            elif change == "lease-owner":
                lock["owner"] = "9" * 32
            elif change == "lease-state":
                lock["state"] = "active"
            elif change == "lease-etag":
                obj.recovery["lease_etag"] = "older-etag"
            elif change == "role":
                obj.iam.get_role.side_effect = None
            else:
                obj.no_report_tasks.side_effect = RuntimeError("image-release-report-already-running")
            with self.subTest(change=change), patch.object(release.binding, "read_object", return_value=(lock, "etag")), self.assertRaises(RuntimeError):
                obj.verify_paused_boundary()
            obj.s3.put_object.assert_not_called()

    def test_expired_tasks_need_explicit_archive_and_other_failures_never_fallback(self):
        for reason, archived, succeeds in (("MISSING", False, False), ("MISSING", True, True), ("AccessDenied", True, False)):
            obj, lock, _, tasks = self.fixture()
            obj.ecs.describe_tasks.return_value = {"tasks": [], "failures": [{"arn": task["taskArn"], "reason": reason} for task in tasks]}
            if archived:
                obj.recovery["archived_readback"] = {"owned_tasks": dict(zip(("probe", "live"), tasks))}
            with self.subTest(reason=reason, archived=archived), patch.object(release.binding, "read_object", return_value=(lock, "etag")):
                if succeeds:
                    obj.verify_paused_boundary()
                    self.assertEqual(sorted(task["taskArn"] for task in tasks), obj.recovery["archived_task_arns"])
                else:
                    with self.assertRaises(RuntimeError):
                        obj.verify_paused_boundary()

    def test_lease_handoff_is_compare_and_swap_without_release(self):
        _, lock, _, _ = self.fixture()
        lease = Mock(owner="9" * 32)
        with patch.object(release.binding, "read_object", return_value=(lock, "etag")):
            release.transfer_recovery_lease(lease, lock["owner"], "etag")
        lease._write.assert_called_once_with("active", "etag")
        lease.release.assert_not_called()
        lease.s3.delete_object.assert_not_called()

    def test_foreign_lease_or_cas_race_is_not_retried(self):
        _, lock, _, _ = self.fixture()
        lease = Mock(owner="9" * 32)
        with patch.object(release.binding, "read_object", return_value=(lock, "newer-etag")), self.assertRaisesRegex(RuntimeError, "lease-drift"):
            release.transfer_recovery_lease(lease, lock["owner"], "etag")
        lease._write.assert_not_called()
        lease._write.side_effect = RuntimeError("conditional-write-failed")
        with patch.object(release.binding, "read_object", return_value=(lock, "etag")), self.assertRaisesRegex(RuntimeError, "conditional-write-failed"):
            release.transfer_recovery_lease(lease, lock["owner"], "etag")
        lease._write.assert_called_once()

    def test_handoff_commit_with_lost_write_and_read_response_retains_owned_uncertain(self):
        for recovery_read_fails in (False, True):
            obj, old_lock, _, _ = self.fixture()
            obj.release_id = "9" * 32
            obj.recovery["lease_etag"] = "old-etag"
            obj.recovery_receipt_sha256 = "e" * 64
            obj.lease = release.binding.MigrationLease(obj.s3, owner=obj.release_id)
            obj.lease_owned = False
            obj.verify_paused_boundary = Mock()
            obj.verify_peer_boundary = Mock()
            obj.all_schedules = Mock(return_value={release.SERVICE: obj.original})
            obj.protected, obj.event = {}, Mock()
            state = {"value": old_lock, "etag": "old-etag", "reads_after_commit": 0}
            writes = []

            def write(s3, key, value, *, etag):
                self.assertEqual(state["etag"], etag)
                writes.append(value["state"])
                state.update(value=value, etag="committed-" + value["state"])
                if value["state"] == "active":
                    raise OSError("lost-write-response")
                return state["etag"]

            def read(s3, key, **kwargs):
                if writes:
                    state["reads_after_commit"] += 1
                    if state["reads_after_commit"] == 1 or recovery_read_fails:
                        raise OSError("read-unavailable")
                return state["value"], state["etag"]

            with self.subTest(recovery_read_fails=recovery_read_fails), \
                 patch.object(release.binding, "read_object", side_effect=read), \
                 patch.object(release.binding, "write_object", side_effect=write), self.assertRaises(OSError):
                obj.acquire_release_lease()
            self.assertEqual(["active"] if recovery_read_fails else ["active", "uncertain"], writes)
            self.assertEqual("ownership-unconfirmed" if recovery_read_fails else "owned-uncertain",
                             obj.event.call_args.kwargs["lease_outcome"])
            self.assertFalse(obj.lease_owned)
            obj.ecs.run_task.assert_not_called()

    def test_failed_handoff_never_changes_foreign_generation(self):
        obj, old_lock, _, _ = self.fixture()
        obj.release_id = "9" * 32
        obj.recovery["lease_etag"] = "old-etag"
        obj.recovery_receipt_sha256 = "e" * 64
        obj.lease = release.binding.MigrationLease(obj.s3, owner=obj.release_id)
        obj.verify_paused_boundary = Mock()
        obj.verify_peer_boundary = Mock()
        obj.all_schedules = Mock(return_value={release.SERVICE: obj.original})
        obj.protected, obj.event = {}, Mock()
        foreign = {**old_lock, "owner": "8" * 32, "state": "active", "generation": "7" * 32}
        with patch.object(release.binding, "read_object", side_effect=[(old_lock, "old-etag"), (foreign, "foreign-etag"), (foreign, "foreign-etag")]), \
             patch.object(release.binding, "write_object", side_effect=RuntimeError("cas-race")) as write, self.assertRaisesRegex(RuntimeError, "cas-race"):
            obj.acquire_release_lease()
        write.assert_called_once()
        self.assertEqual("not-owned-no-write", obj.event.call_args.kwargs["lease_outcome"])
        obj.ecs.run_task.assert_not_called()

    def test_paused_pre_live_failure_restores_disabled_and_returns_uncertain_owner(self):
        obj = ImageReleaseTests().failure_object()
        obj.original["State"] = "DISABLED"
        obj.recovery = {"previous_owner": "c" * 32}
        obj.restore_paused_recovery_lease = Mock()
        obj.recover_failure()
        obj.update.assert_called_once_with(obj.original)
        self.assertEqual("DISABLED", obj.update.call_args.args[0]["State"])
        obj.restore_paused_recovery_lease.assert_called_once()
        obj.lease.release.assert_not_called()


class ImageReleaseTests(unittest.TestCase):
    def test_production_definition_changes_only_image_and_does_not_mutate_source(self):
        source = definition()
        before = copy.deepcopy(source)
        result = release.image_only_definition(source, IMAGE)
        expected = copy.deepcopy(before)
        expected["containerDefinitions"][0]["image"] = IMAGE
        self.assertEqual(expected, result)
        self.assertEqual(before, source)

    def test_host_gate_hash_uses_committed_blob_not_windows_working_file(self):
        with patch.object(release.subprocess, "check_output", return_value=b"source\n") as command:
            self.assertEqual(release.sha(b"source\n"), release.committed_gate_sha("a" * 40))
        self.assertEqual(["git", "show", "a" * 40 + ":scripts/reporting_migration_host_gate.py"], command.call_args.args[0])

    def test_foreign_sink_mutable_source_and_secret_control_collision_reject(self):
        for change in ("foreign", "mutable", "secret", "financial"):
            source = definition()
            container = source["containerDefinitions"][0]
            if change == "foreign":
                container["environment"][0]["value"] = "roy"
            elif change == "mutable":
                container["image"] = "registry/vevo-reporting:latest"
            elif change == "secret":
                container["secrets"].append({"name": "REPORT_SKIP_INVOICES", "valueFrom": "secret"})
            else:
                container["environment"][-1]["value"] = "false"
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                release.image_only_definition(source, IMAGE)

    def test_probe_uses_isolated_role_and_live_never_overrides_production_role(self):
        for probe in (True, False):
            result = release.task_overrides("c" * 32, "d" * 40, IMAGE, "e" * 64, DATE, probe=probe)
            env = {row["name"]: row["value"] for row in result["containerOverrides"][0]["environment"]}
            self.assertEqual(DATE, env["REPORT_TO_DATE"])
            self.assertTrue(all(env[key] == "true" for key in ("REPORT_SKIP_EMAIL", "REPORT_SKIP_INVOICES",
                                 "REPORT_SKIP_CREDITNOTE_STORNO_GUARD", "REPORT_FORCE_NO_CACHE", "REPORT_FORCE_CLEAR_CACHE")))
            self.assertEqual(probe, "taskRoleArn" in result)

    def test_live_arguments_skip_every_financial_runner_and_email_explicitly(self):
        args = report_arguments(DATE)
        for flag in ("--skip-email", "--skip-invoices", "--skip-creditnote-storno-guard", "--clear-cache", "--no-cache"):
            self.assertIn(flag, args)
        self.assertNotIn("--skip-export", args)
        self.assertEqual("", args[args.index("--output-tag") + 1])
        self.assertEqual(DATE, args[args.index("--to-date") + 1])
        with self.assertRaises(ValueError):
            report_arguments("yesterday")

    def test_all_periods_and_exact_end_date_required(self):
        for probe in (True, False):
            manifest, blobs, prefix = artifact_set(probe=probe)
            def fetch(key, limit):
                return blobs[key]
            release.verify_artifacts(manifest, fetch, prefix=prefix, to_date=DATE, probe=probe)
            del manifest["artifacts"]["dashboard_payload_90d.json"]
            with self.assertRaisesRegex(RuntimeError, "manifest-identity"):
                release.verify_artifacts(manifest, fetch, prefix=prefix, to_date=DATE, probe=probe)
        manifest, blobs, prefix = artifact_set(end="2026-10-05")
        with self.assertRaisesRegex(RuntimeError, "end-date"):
            release.verify_artifacts(manifest, lambda key, limit: blobs[key], prefix=prefix, to_date=DATE, probe=False)

    def test_cross_prefix_and_corrupt_artifacts_reject(self):
        for mutation in ("prefix", "hash"):
            manifest, blobs, prefix = artifact_set()
            if mutation == "prefix":
                manifest["artifacts"]["report_latest.html"]["key"] = "daily-reports/roy/report_latest.html"
            else:
                blobs[prefix + "report_latest.html"] = b"<html>tampered</html>"
            with self.subTest(mutation=mutation), self.assertRaises(RuntimeError):
                release.verify_artifacts(manifest, lambda key, limit: blobs[key], prefix=prefix, to_date=DATE, probe=False)

    def test_partial_quality_cannot_pass_even_with_consistent_hashes(self):
        manifest, blobs, prefix = artifact_set()
        key = prefix + "dashboard_payload_latest.json"
        payload = json.loads(blobs[key])
        payload["source_health"]["is_partial"] = True
        blobs[key] = json.dumps(payload).encode()
        manifest["artifacts"]["dashboard_payload_latest.json"].update(size=len(blobs[key]), sha256=release.sha(blobs[key]))
        with self.assertRaisesRegex(RuntimeError, "report-incomplete"):
            release.verify_artifacts(manifest, lambda key, limit: blobs[key], prefix=prefix, to_date=DATE, probe=False)

    def test_all_other_schedule_revisions_are_dynamic_and_protected(self):
        obj = object.__new__(release.ImageRelease)
        obj.release_id, obj.task_mode, obj.owned_task = "c" * 32, None, None
        obj.clock = lambda: 100
        obj.lease = Mock()
        obj.verify_peer_boundary, obj.exclusion = Mock(), Mock()
        obj.known_schedule = {"Name": release.SERVICE, "State": "DISABLED"}
        obj.protected = {"roy-daily-report-email": {"Name": "roy-daily-report-email", "State": "ENABLED",
                         "Target": {"EcsParameters": {"TaskDefinitionArn": "roy:777"}}}}
        obj.all_schedules = Mock(return_value={release.SERVICE: obj.known_schedule, **obj.protected})
        with patch("builtins.print"):
            obj.checkpoint()
        changed = copy.deepcopy(obj.all_schedules.return_value)
        changed["roy-daily-report-email"]["Target"]["EcsParameters"]["TaskDefinitionArn"] = "roy:778"
        obj.all_schedules.return_value = changed
        with self.assertRaisesRegex(RuntimeError, "other-schedule-drift"):
            obj.checkpoint()

    def failure_object(self, *, live=False, uncertain=False):
        obj = object.__new__(release.ImageRelease)
        obj.lease_owned, obj.start_uncertain, obj.rerun_attempted = True, uncertain, live
        obj.event, obj.cleanup_task, obj.cleanup_role = Mock(), Mock(), Mock()
        obj.lease, obj.ecs = Mock(), Mock()
        obj.original = {"Name": release.SERVICE, "State": "ENABLED", "Target": {"EcsParameters": {"TaskDefinitionArn": "old"}}}
        obj.known_schedule = copy.deepcopy(obj.original)
        obj.known_schedule["State"] = "DISABLED"
        obj.schedule = Mock(return_value=obj.known_schedule)
        obj.update = Mock()
        obj.candidate = None
        obj.live_outputs = Mock(return_value={})
        return obj

    def test_uncertain_dispatch_does_not_restore_enable_or_redispatch(self):
        obj = self.failure_object(uncertain=True)
        obj.recover_failure()
        obj.lease.retain_uncertain.assert_called_once()
        obj.update.assert_not_called()
        obj.cleanup_task.assert_not_called()
        obj.lease.release.assert_not_called()

    def test_failed_live_publication_preserves_candidate_paused_and_requires_review(self):
        obj = self.failure_object(live=True)
        obj.known_schedule["Target"]["EcsParameters"]["TaskDefinitionArn"] = "new"
        obj.recover_failure()
        desired = obj.update.call_args.args[0]
        self.assertEqual("new", desired["Target"]["EcsParameters"]["TaskDefinitionArn"])
        self.assertEqual("DISABLED", desired["State"])
        obj.lease.retain_uncertain.assert_called_once()
        obj.lease.release.assert_not_called()

    def test_failure_before_live_restores_only_owned_original_schedule(self):
        obj = self.failure_object()
        obj.recover_failure()
        obj.update.assert_called_once_with(obj.original)
        obj.lease.release.assert_called_once()
        obj.ecs.run_task.assert_not_called()

    def test_foreign_schedule_is_never_overwritten_during_failure(self):
        obj = self.failure_object()
        obj.schedule.return_value = {"Name": release.SERVICE, "State": "ENABLED", "Target": {"foreign": True}}
        with self.assertRaisesRegex(RuntimeError, "foreign-schedule"):
            obj.recover_failure()
        obj.update.assert_not_called()
        obj.lease.retain_uncertain.assert_called_once()


class IndependentReviewTests(unittest.TestCase):
    def fixture(self, project="vevo"):
        obj = object.__new__(release.ImageRelease)
        obj.policy = release.PROJECTS[project]
        obj.release_id, obj.commit, obj.image = "a" * 32, "b" * 40, IMAGE
        obj.prefix = obj.policy.probe_prefix + obj.release_id + "/"
        obj.independent_contract_sha256 = "c" * 64
        obj.report_from_date, obj.to_date = "2025-05-03", DATE
        obj.owned_task, obj.known_schedule = None, {"State": "DISABLED"}
        obj.s3, obj.event, obj.checkpoint, obj.no_report_tasks = Mock(), Mock(), Mock(), Mock()
        obj.original_outputs = {"old": {"ETag": "unchanged"}}
        obj.live_outputs = Mock(return_value=obj.original_outputs)
        elapsed = [0]
        obj.clock = lambda: elapsed[0]
        obj.sleep = lambda seconds: elapsed.__setitem__(0, elapsed[0] + seconds)
        obj.timeout = 20
        manifest = release.canonical({"project": project, "artifacts": "already-verified-by-wait-probe"})
        obj.probe_manifest_sha256 = release.sha(manifest)
        checks = {name: True for name in ("financial_aggregates", "country_attribution", "shared_fixed_costs", "advertising_reconciliation")}
        review = {"schema_version": 1, "project": project, "release_id": obj.release_id, "source_commit": obj.commit,
                  "image_digest": IMAGE.rsplit("@", 1)[1], "report_from_date": obj.report_from_date, "report_to_date": DATE,
                  "probe_manifest_sha256": obj.probe_manifest_sha256, "source_contract_sha256": obj.independent_contract_sha256,
                  "verification_script_sha256": "d" * 64, "approved": True,
                  "period_checks": {period: checks.copy() for period in ("latest", "7d", "30d", "90d")},
                  "evidence": {"key": f"data/{project}/analyses/test/source-proof.json", "sha256": "pending"}}
        return obj, review, manifest

    def blobs(self, obj, review, manifest, proof=None):
        proof = proof if proof is not None else {key: copy.deepcopy(value) for key, value in review.items() if key != "evidence"}
        evidence_raw = release.canonical(proof)
        review["evidence"]["sha256"] = release.sha(evidence_raw)
        return {obj.prefix + "artifacts/output-manifest.json": manifest,
                obj.prefix + "review/independent-source.json": release.canonical(review), review["evidence"]["key"]: evidence_raw}

    def verify(self, obj, blobs):
        with patch.object(release, "read_private", side_effect=lambda _s3, key, *args: blobs[key]):
            obj.wait_independent_review()

    def test_matching_source_proof_passes_both_projects_and_is_outside_probe_write_scope(self):
        for project in ("vevo", "roy"):
            obj, review, manifest = self.fixture(project)
            blobs = self.blobs(obj, review, manifest)
            with self.subTest(project=project):
                self.verify(obj, blobs)
            self.assertEqual(["independent-review-requested", "independent-review-verified"], [call.args[0] for call in obj.event.call_args_list])
            self.assertEqual(release.sha(blobs[obj.prefix + "review/independent-source.json"]), obj.event.call_args.kwargs["review_sha256"])
            self.assertEqual(review["evidence"], obj.event.call_args.kwargs["evidence"])
            self.assertEqual(2, obj.no_report_tasks.call_count)
            writes = [statement for statement in obj.policy.probe_policy(obj.release_id)["Statement"] if "s3:PutObject" in statement["Action"]]
            self.assertTrue(all("/review/" not in resource for statement in writes for resource in statement["Resource"]))
            self.assertTrue(all(resource.startswith(f"arn:aws:s3:::{release.BUCKET}/" + obj.prefix + "artifacts/")
                                or resource.startswith(f"arn:aws:s3:::{release.BUCKET}/" + obj.prefix + "markers/")
                                for statement in writes for resource in statement["Resource"]))

    def test_stale_foreign_unapproved_and_wrong_manifest_binding_fail_before_approval(self):
        changes = {"schema_version": True, "project": "roy", "release_id": "f" * 32, "source_commit": "f" * 40,
                   "image_digest": "sha256:" + "f" * 64, "report_from_date": DATE, "report_to_date": "2026-10-05",
                   "probe_manifest_sha256": "f" * 64, "source_contract_sha256": "f" * 64,
                   "verification_script_sha256": "unhashed", "approved": False, "unknown": True}
        for field, value in changes.items():
            obj, review, manifest = self.fixture()
            review[field] = value
            with self.subTest(field=field), self.assertRaisesRegex(RuntimeError, "review-binding"):
                self.verify(obj, self.blobs(obj, review, manifest))
            self.assertEqual(1, obj.event.call_count)

    def test_every_period_and_check_must_be_present_and_literal_true(self):
        for change in ("missing-period", "extra-period", "missing-check", "extra-check", "false", "number", "text"):
            obj, review, manifest = self.fixture()
            checks = review["period_checks"]
            if change == "missing-period":
                del checks["90d"]
            elif change == "extra-period":
                checks["full"] = checks["latest"]
            elif change == "missing-check":
                del checks["latest"]["country_attribution"]
            elif change == "extra-check":
                checks["latest"]["individual_membership"] = True
            else:
                checks["latest"]["country_attribution"] = {"false": False, "number": 1, "text": "true"}[change]
            with self.subTest(change=change), self.assertRaisesRegex(RuntimeError, "review-checks"):
                self.verify(obj, self.blobs(obj, review, manifest))

    def test_evidence_cannot_point_to_other_project_public_sink_probe_or_traversal(self):
        for key in ("data/roy/analyses/other.json", "daily-reports/vevo/latest/generation.json",
                    "data/vevo/reporting/runtime/probes/forged.json", "data/vevo/analyses/../other.json",
                    "data/vevo/analyses/a\\other.json", "data/vevo/analyses//other.json"):
            obj, review, manifest = self.fixture()
            review["evidence"]["key"] = key
            with self.subTest(key=key), self.assertRaisesRegex(RuntimeError, "evidence-scope"):
                self.verify(obj, self.blobs(obj, review, manifest))

    def test_shared_private_archive_is_allowed_but_hash_and_content_must_both_match(self):
        for change in ("none", "hash", "country", "integer", "errors"):
            obj, review, manifest = self.fixture()
            review["evidence"]["key"] = "data/reporting/analyses/a/source-proof.json"
            proof = {key: copy.deepcopy(value) for key, value in review.items() if key != "evidence"}
            if change in ("country", "integer"):
                proof["period_checks"]["latest"]["country_attribution"] = False if change == "country" else 1
            elif change == "errors":
                proof["errors"] = ["synthetic source mismatch"]
            blobs = self.blobs(obj, review, manifest, proof)
            if change == "hash":
                blobs[review["evidence"]["key"]] += b" "
            with self.subTest(change=change):
                if change == "none":
                    self.verify(obj, blobs)
                else:
                    with self.assertRaisesRegex(RuntimeError, "evidence-(hash|binding)"):
                        self.verify(obj, blobs)

    def test_missing_review_waits_with_checks_but_access_denied_fails_immediately(self):
        for code in ("NoSuchKey", "AccessDenied"):
            obj, review, manifest = self.fixture()
            blobs = self.blobs(obj, review, manifest)
            count = [0]
            def fetch(_s3, key, *args):
                if key.endswith("review/independent-source.json"):
                    count[0] += 1
                    if count[0] == 1:
                        raise ClientError({"Error": {"Code": code}}, "GetObject")
                return blobs[key]
            with self.subTest(code=code), patch.object(release, "read_private", side_effect=fetch):
                if code == "NoSuchKey":
                    obj.wait_independent_review()
                    self.assertEqual(10, obj.clock())
                    self.assertEqual(3, obj.no_report_tasks.call_count)
                else:
                    with self.assertRaisesRegex(RuntimeError, "review-read-failed"):
                        obj.wait_independent_review()
                    self.assertEqual(0, obj.clock())

    def test_missing_review_times_out_without_any_approval(self):
        obj, _review, manifest = self.fixture()
        def fetch(_s3, key, *args):
            if key.endswith("output-manifest.json"):
                return manifest
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        with patch.object(release, "read_private", side_effect=fetch), self.assertRaisesRegex(RuntimeError, "review-timeout"):
            obj.wait_independent_review()
        self.assertEqual(20, obj.clock())
        self.assertEqual(["independent-review-requested"], [call.args[0] for call in obj.event.call_args_list])

    def test_changed_manifest_outputs_or_protected_schedule_cannot_authorize_live(self):
        for change in ("manifest", "outputs", "protected"):
            obj, review, manifest = self.fixture()
            blobs = self.blobs(obj, review, manifest)
            if change == "manifest":
                blobs[obj.prefix + "artifacts/output-manifest.json"] += b" "
            elif change == "outputs":
                obj.live_outputs.return_value = {"new": "foreign publication"}
            else:
                obj.checkpoint.side_effect = RuntimeError("image-release-other-schedule-drift")
            with self.subTest(change=change), self.assertRaisesRegex(RuntimeError, "drift"):
                self.verify(obj, blobs)
            self.assertFalse(any(call.args[0] == "independent-review-verified" for call in obj.event.call_args_list))

    def test_duplicate_review_fields_and_review_changed_during_validation_fail(self):
        for change in ("duplicate", "changed"):
            obj, review, manifest = self.fixture()
            blobs = self.blobs(obj, review, manifest)
            key = obj.prefix + "review/independent-source.json"
            if change == "duplicate":
                blobs[key] = b'{"approved":false,' + blobs[key][1:]
            count = [0]
            def fetch(_s3, requested, *args):
                if requested == key:
                    count[0] += 1
                    if change == "changed" and count[0] == 2:
                        return blobs[requested] + b" "
                return blobs[requested]
            with self.subTest(change=change), patch.object(release, "read_private", side_effect=fetch), self.assertRaises(RuntimeError):
                obj.wait_independent_review()

    def test_optional_legacy_path_does_not_read_or_wait_and_bad_contract_rejects_before_clients(self):
        obj = object.__new__(release.ImageRelease)
        with patch.object(release, "read_private") as fetch:
            obj.wait_independent_review()
        fetch.assert_not_called()
        for value in ("", "bad", "A" * 64, True, "a" * 63):
            session = Mock()
            with self.subTest(value=value), self.assertRaisesRegex(RuntimeError, "independent-contract-invalid"):
                release.ImageRelease(session, "b" * 40, DATE, 300, independent_contract_sha256=value)
            session.client.assert_not_called()

    def test_run_places_source_review_after_probe_cleanup_and_timeout_restores_original_state(self):
        for paused in (False, True):
            for approved in (False, True):
                obj, review, manifest = self.fixture()
                obj.recover_paused_release = "e" * 32 if paused else None
                obj.recovery = {"previous_owner": "e" * 32} if paused else None
                obj.promoted = obj.rerun_attempted = obj.lease_owned = obj.start_uncertain = False
                obj.task_records, obj.peer_snapshot = {}, {}
                obj.session, obj.ecs, obj.lease = Mock(), Mock(), Mock()
                obj.session.client.return_value.get_caller_identity.return_value = {"Account": release.ACCOUNT}
                source = definition()
                source.update(status="ACTIVE", taskDefinitionArn="arn:synthetic:vevo-reporting-daily:42")
                source["containerDefinitions"][0]["environment"] += [
                    {"name": "REPORT_FROM_DATE", "value": obj.report_from_date},
                    {"name": "REPORT_SKIP_CREDITNOTE_STORNO_GUARD", "value": "true"}]
                original = {"Name": release.SERVICE, "State": "DISABLED" if paused else "ENABLED",
                            "Target": {"Arn": release.CLUSTER, "EcsParameters": {"TaskDefinitionArn": source["taskDefinitionArn"], "LaunchType": "FARGATE"}}}
                current = [copy.deepcopy(original)]
                obj.schedule = lambda: copy.deepcopy(current[0])
                obj.all_schedules = lambda: {release.SERVICE: copy.deepcopy(current[0])}
                transitions = []
                def update(desired):
                    current[0], obj.known_schedule = copy.deepcopy(desired), copy.deepcopy(desired)
                obj.update = update
                obj.event = Mock(side_effect=lambda phase, **details: transitions.append(phase))
                obj.exact_image = Mock(return_value=(IMAGE, "123"))
                obj.definition = Mock(side_effect=lambda arn: source if arn == source["taskDefinitionArn"] else {"status": "INACTIVE"})
                obj.ecs.register_task_definition.side_effect = lambda **value: {"taskDefinition": {**value, "taskDefinitionArn": "arn:synthetic:vevo-reporting-daily:43"}}
                obj.inspect_peer_boundary, obj.exclusion, obj.inspect_paused_recovery = Mock(), Mock(), Mock()
                obj.acquire_release_lease = Mock(side_effect=lambda: setattr(obj, "lease_owned", True))
                obj.create_role, obj.cleanup_task = Mock(), Mock()
                obj.cleanup_role = Mock(side_effect=lambda: transitions.append("probe-role-cleaned"))
                obj.restore_paused_recovery_lease = Mock(side_effect=lambda: setattr(obj, "lease_owned", False))
                def start(mode):
                    transitions.append("start-" + mode)
                    obj.owned_task = mode
                    self.assertEqual("DISABLED", current[0]["State"])
                    if mode == "live":
                        self.assertIn("independent-review-verified", transitions)
                        obj.rerun_attempted = True
                obj.start_task = Mock(side_effect=start)
                obj.wait_probe = Mock(side_effect=lambda: transitions.append("probe-verified"))
                obj.wait_live = Mock()
                blobs = self.blobs(obj, review, manifest)
                def fetch(_s3, key, *args):
                    if key == obj.policy.sink + "/latest/generation.json":
                        return b'{"generation_id":"old"}'
                    if not approved and key.endswith("review/independent-source.json"):
                        raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
                    return blobs[key]
                with self.subTest(paused=paused, approved=approved), patch.object(release, "exact_source"), \
                     patch.object(release, "committed_gate_sha", return_value="d" * 64), \
                     patch.object(release.binding, "require_private_bucket"), patch.object(release, "read_private", side_effect=fetch):
                    if approved:
                        obj.run()
                        self.assertLess(transitions.index("probe-verified"), transitions.index("probe-role-cleaned"))
                        self.assertLess(transitions.index("probe-role-cleaned"), transitions.index("independent-review-requested"))
                        self.assertLess(transitions.index("independent-review-verified"), transitions.index("image-promotion-requested"))
                        self.assertLess(transitions.index("image-promotion-verified"), transitions.index("start-live"))
                    else:
                        with self.assertRaisesRegex(RuntimeError, "independent-review-timeout"):
                            obj.run()
                        self.assertNotIn("image-promotion-requested", transitions)
                        self.assertNotIn("start-live", transitions)
                        self.assertEqual(original, current[0])
                        obj.ecs.deregister_task_definition.assert_called_once()
                        self.assertEqual(paused, obj.restore_paused_recovery_lease.called)
                        self.assertEqual(not paused, obj.lease.release.called)


if __name__ == "__main__":
    unittest.main()
