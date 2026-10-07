import copy
import json
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

from scripts import reporting_migration_host_gate as host
from scripts import reporting_runtime_binding as binding
from scripts import vevo_report_image_release as release
from scripts.reporting_image_release_policy import PROJECTS, VEVO
from scripts.reporting_image_release_lease import ScopedImageLease, read_lease
from scripts.vevo_report_image_host import report_arguments
from tests.test_reporting_migration_host_gate import S3
from tests.test_reporting_runtime_binding import MemoryS3, NOW
from tests.test_vevo_report_image_release import definition, artifact_set, IMAGE, DATE

ROY = PROJECTS["roy"]
RID = "b" * 32


def roy_definition():
    result = definition()
    result["family"] = ROY.family
    result["taskRoleArn"] = f"arn:aws:iam::{binding.ACCOUNT}:role/BiznisWebReportingTaskRole-roy"
    for row in result["containerDefinitions"][0]["environment"]:
        if row["name"] == "REPORT_PROJECT":
            row["value"] = "roy"
        if row["name"] == "REPORT_S3_PREFIX":
            row["value"] = ROY.sink
    return result


class RoyReleaseTests(unittest.TestCase):
    def controller(self):
        s3 = MemoryS3()
        old = binding.MigrationLease(s3, owner="a" * 32, now=lambda: NOW).acquire()
        old.retain_uncertain()
        found = read_lease(s3, "vevo")
        obj = object.__new__(release.ImageRelease)
        obj.policy, obj.s3, obj.release_id, obj.owned_task = ROY, s3, RID, None
        obj.peer_snapshot = None
        obj.retained_peer_release = old.owner
        obj.retained_peer_lease_sha256 = release.sha(binding.canonical_bytes(found[0]))
        obj.retained_peer_lease_etag = found[1]
        schedule = {"Name": VEVO.service, "State": "DISABLED", "Target": {"Arn": release.CLUSTER,
                    "EcsParameters": {"TaskDefinitionArn": f"arn:aws:ecs:{binding.REGION}:{binding.ACCOUNT}:task-definition/{VEVO.family}:44"}}}
        obj.protected = {VEVO.service: schedule}
        obj.schedule = Mock(return_value=schedule)
        obj.output_snapshot = Mock(return_value={"old-generation": {"ETag": "frozen"}})
        obj.tasks = Mock(return_value=[])
        obj.lease = ScopedImageLease(s3, owner=RID, project="roy", now=lambda: NOW)
        obj.recovery, obj.event, obj.lease_owned = None, Mock(), False
        return obj

    def test_roy_definition_is_image_only_and_cross_project_inputs_reject(self):
        source = roy_definition()
        before = copy.deepcopy(source)
        candidate = release.image_only_definition(source, IMAGE, policy=ROY)
        expected = copy.deepcopy(before)
        expected["containerDefinitions"][0]["image"] = IMAGE
        self.assertEqual(expected, candidate)
        self.assertEqual(before, source)
        for field in ("family", "taskRoleArn", "sink", "project"):
            bad = copy.deepcopy(source)
            if field == "family":
                bad[field] = VEVO.family
            elif field == "taskRoleArn":
                bad[field] = definition()[field]
            else:
                key = "REPORT_S3_PREFIX" if field == "sink" else "REPORT_PROJECT"
                for row in bad["containerDefinitions"][0]["environment"]:
                    if row["name"] == key:
                        row["value"] = VEVO.sink if field == "sink" else "vevo"
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                release.image_only_definition(bad, IMAGE, policy=ROY)

    def test_project_specific_role_and_command_preserve_all_report_only_flags(self):
        for probe in (False, True):
            result = release.task_overrides(RID, "c" * 40, IMAGE, "d" * 64, DATE, probe=probe, policy=ROY)
            container = result["containerOverrides"][0]
            self.assertEqual("roy", container["command"][container["command"].index("--project") + 1])
            env = {row["name"]: row["value"] for row in container["environment"]}
            self.assertEqual(DATE, env["REPORT_TO_DATE"])
            self.assertEqual("roy", env["REPORT_PROJECT"])
            self.assertTrue(all(env[k] == "true" for k in ("REPORT_SKIP_EMAIL", "REPORT_SKIP_INVOICES",
                "REPORT_SKIP_CREDITNOTE_STORNO_GUARD", "REPORT_FORCE_NO_CACHE", "REPORT_FORCE_CLEAR_CACHE")))
            self.assertEqual(probe, "taskRoleArn" in result)
            if probe:
                self.assertEqual(f"arn:aws:iam::{binding.ACCOUNT}:role/RoyReportProbe-{RID}", result["taskRoleArn"])
        args = report_arguments(DATE, policy=ROY)
        self.assertEqual("roy", args[args.index("--project") + 1])
        self.assertEqual("", args[args.index("--output-tag") + 1])
        for flag in ("--skip-email", "--skip-invoices", "--skip-creditnote-storno-guard", "--clear-cache", "--no-cache"):
            self.assertIn(flag, args)

    def test_peer_proof_required_before_any_lease_mutation(self):
        for field, value in (("retained_peer_release", "c" * 32), ("retained_peer_lease_sha256", "0" * 64),
                             ("retained_peer_lease_etag", "foreign"), ("retained_peer_release", None)):
            obj = self.controller()
            setattr(obj, field, value)
            before = copy.deepcopy(obj.s3.objects)
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                obj.inspect_peer_boundary()
            self.assertEqual(before, obj.s3.objects)
        obj = self.controller()
        del obj.s3.objects[binding.LOCK_KEY]
        with self.assertRaisesRegex(RuntimeError, "peer-arguments-invalid"):
            obj.inspect_peer_boundary()

    def test_every_checkpoint_binds_full_peer_lease_schedule_outputs_and_tasks(self):
        for change in ("body", "etag", "schedule", "outputs", "task"):
            obj = self.controller()
            obj.inspect_peer_boundary()
            if change in {"body", "etag"}:
                raw, etag, encryption = obj.s3.objects[binding.LOCK_KEY]
                if change == "body":
                    value = json.loads(raw)
                    value["generation"] = "c" * 32
                    raw = binding.canonical_bytes(value)
                else:
                    etag = "foreign"
                obj.s3.objects[binding.LOCK_KEY] = raw, etag, encryption
            elif change == "schedule":
                obj.schedule.return_value = {**obj.protected[VEVO.service], "State": "ENABLED"}
            elif change == "outputs":
                obj.output_snapshot.return_value = {"new-generation": {}}
            else:
                obj.tasks.return_value = [{"taskArn": "foreign-task", "lastStatus": "RUNNING"}]
            before = copy.deepcopy(obj.s3.objects)
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                obj.verify_peer_boundary()
            self.assertEqual(before, obj.s3.objects)

    def test_future_ordinary_roy_release_accepts_only_absent_or_released_peer_without_override(self):
        for state in (None, "released", "active", "uncertain"):
            obj = self.controller()
            obj.retained_peer_release = obj.retained_peer_lease_sha256 = obj.retained_peer_lease_etag = None
            if state is None:
                del obj.s3.objects[binding.LOCK_KEY]
            else:
                raw, etag, encryption = obj.s3.objects[binding.LOCK_KEY]
                value = json.loads(raw)
                value["state"] = state
                obj.s3.objects[binding.LOCK_KEY] = binding.canonical_bytes(value), etag, encryption
            with self.subTest(state=state):
                if state in (None, "released"):
                    obj.inspect_peer_boundary()
                    obj.acquire_release_lease()
                    self.assertEqual("active", read_lease(obj.s3, "roy")[0]["state"])
                else:
                    with self.assertRaises(RuntimeError):
                        obj.inspect_peer_boundary()
                    self.assertNotIn(ROY.lease_key, obj.s3.objects)

    def test_uncertain_peer_must_be_disabled_and_active_peer_never_accepted_with_proof(self):
        obj = self.controller()
        obj.protected[VEVO.service]["State"] = "ENABLED"
        with self.assertRaisesRegex(RuntimeError, "retained-peer-schedule"):
            obj.inspect_peer_boundary()
        obj = self.controller()
        raw, etag, encryption = obj.s3.objects[binding.LOCK_KEY]
        value = json.loads(raw)
        value["state"] = "active"
        raw = binding.canonical_bytes(value)
        obj.s3.objects[binding.LOCK_KEY] = raw, etag, encryption
        obj.retained_peer_lease_sha256 = release.sha(raw)
        with self.assertRaisesRegex(RuntimeError, "peer-active"):
            obj.inspect_peer_boundary()

    def test_post_cas_peer_race_retains_only_new_roy_uncertain_lease(self):
        obj = self.controller()
        obj.inspect_peer_boundary()
        old = copy.deepcopy(obj.s3.objects[binding.LOCK_KEY])
        original_acquire = obj.lease.acquire
        def acquire_then_peer_task():
            original_acquire()
            obj.tasks.return_value = [{"taskArn": "foreign-task", "lastStatus": "RUNNING"}]
        obj.lease.acquire = acquire_then_peer_task
        with self.assertRaisesRegex(RuntimeError, "foreign-report-running"):
            obj.acquire_release_lease()
        self.assertEqual("uncertain", read_lease(obj.s3, "roy")[0]["state"])
        self.assertEqual(old, obj.s3.objects[binding.LOCK_KEY])

    def test_roy_lost_ack_inspection_never_writes_foreign_lease(self):
        obj = self.controller()
        obj.inspect_peer_boundary()
        foreign = ScopedImageLease(obj.s3, owner="c" * 32, project="roy", now=lambda: NOW).acquire()
        before = copy.deepcopy(obj.s3.objects)
        obj.inspect_failed_lease_handoff()
        self.assertEqual(before, obj.s3.objects)
        self.assertEqual("not-owned-no-write", obj.event.call_args.kwargs["lease_outcome"])

    def test_role_ownership_validation_uses_project_name_and_shared_cleanup(self):
        obj = self.controller()
        obj.role_trust = {"trusted": "ecs"}
        role = {"Arn": f"arn:aws:iam::{binding.ACCOUNT}:role/RoyReportProbe-{RID}",
                "Tags": [{"Key": "ManagedReportProbe", "Value": RID}], "AssumeRolePolicyDocument": obj.role_trust}
        obj.validate_probe_role(role)
        role["Arn"] = role["Arn"].replace("RoyReport", "VevoReport")
        with self.assertRaisesRegex(RuntimeError, "ownership-invalid"):
            obj.validate_probe_role(role)
        obj.iam = Mock()
        obj.iam.get_role.return_value = {"Role": {"Arn": role["Arn"]}}
        obj.probe_role()
        obj.iam.get_role.assert_called_once_with(RoleName="RoyReportProbe-" + RID)

    def test_cleanup_requires_exact_roy_command_role_and_task_ownership(self):
        for change in (None, "startedBy", "role", "project"):
            obj = self.controller()
            obj.commit, obj.image, obj.gate_sha, obj.to_date = "c" * 40, IMAGE, "d" * 64, DATE
            obj.task_mode, obj.sleep = "probe", Mock()
            obj.candidate = roy_definition()
            obj.candidate["taskDefinitionArn"] = f"arn:aws:ecs:{binding.REGION}:{binding.ACCOUNT}:task-definition/{ROY.family}:78"
            obj.probe_definition = obj.candidate
            obj.owned_task = release.CLUSTER.replace(":cluster/", ":task/") + "/" + RID
            task = {"taskArn": obj.owned_task, "clusterArn": release.CLUSTER,
                    "taskDefinitionArn": obj.candidate["taskDefinitionArn"], "launchType": "FARGATE",
                    "startedBy": RID, "lastStatus": "RUNNING", "overrides": release.task_overrides(
                        RID, obj.commit, IMAGE, obj.gate_sha, DATE, probe=True, policy=ROY)}
            if change == "startedBy":
                task["startedBy"] = "foreign"
            elif change == "role":
                task["overrides"]["taskRoleArn"] = task["overrides"]["taskRoleArn"].replace("RoyReport", "VevoReport")
            elif change == "project":
                task["overrides"]["containerOverrides"][0]["command"][-1] = "vevo"
            obj.ecs = Mock()
            obj.ecs.describe_tasks.side_effect = [{"tasks": [task]}, {"tasks": [{**task, "lastStatus": "STOPPED"}]},
                                                {"tasks": [{**task, "lastStatus": "STOPPED"}]}]
            with self.subTest(change=change):
                if change is None:
                    obj.cleanup_task()
                    self.assertEqual(obj.owned_task, obj.ecs.stop_task.call_args.kwargs["task"])
                    self.assertEqual(1, obj.ecs.stop_task.call_count)
                else:
                    with self.assertRaises(RuntimeError):
                        obj.cleanup_task()
                    obj.ecs.stop_task.assert_not_called()

    def test_source_drift_stops_before_any_runtime_mutation(self):
        obj = release.ImageRelease(Mock(), "c" * 40, DATE, 7200, project="roy", release_id=RID)
        with patch.object(release.subprocess, "check_output", side_effect=["", "d" * 40]), \
             patch.object(release.subprocess, "run") as fetch, self.assertRaisesRegex(RuntimeError, "main-drift"):
            obj.run()
        fetch.assert_called_once()
        obj.ecs.register_task_definition.assert_not_called()
        obj.ecs.run_task.assert_not_called()
        obj.scheduler.update_schedule.assert_not_called()
        obj.s3.put_object.assert_not_called()

    def test_competing_workflow_is_rejected_and_non_deploy_build_is_allowed(self):
        obj = self.controller()
        for name, expected in (("deploy-monthly-accounting-export.yml", False), ("production-reporting-smoke.yml", False),
                               ("build-and-push-ecr.yml", True)):
            rows = {"workflow_runs": [{"id": "999", "path": ".github/workflows/" + name}]}
            with self.subTest(name=name), patch.object(release, "gh", return_value=rows), patch.dict(release.os.environ, {}, clear=True):
                if expected:
                    obj.exclusion()
                else:
                    with self.assertRaisesRegex(RuntimeError, "competing-workflow"):
                        obj.exclusion()

    def test_full_history_and_project_identity_are_required_in_published_payload(self):
        manifest, blobs, prefix = artifact_set()
        manifest["project"] = "roy"
        for name, entry in manifest["artifacts"].items():
            if name.endswith(".json"):
                payload = json.loads(blobs[entry["key"]])
                payload.update(project="roy", date_from="2025-09-24")
                raw = json.dumps(payload).encode()
                blobs[entry["key"]] = raw
                entry.update(size=len(raw), sha256=release.sha(raw))
        fetch = lambda key, limit: blobs[key]
        release.verify_artifacts(manifest, fetch, prefix=prefix, to_date=DATE, probe=False, policy=ROY, from_date="2025-09-24")
        with self.assertRaisesRegex(RuntimeError, "start-date"):
            release.verify_artifacts(manifest, fetch, prefix=prefix, to_date=DATE, probe=False, policy=ROY, from_date="2025-09-01")
        with self.assertRaisesRegex(RuntimeError, "manifest-identity"):
            release.verify_artifacts(manifest, fetch, prefix=prefix, to_date=DATE, probe=False)

    def test_release_lifecycle_never_promotes_before_probe_and_scopes_failure_recovery(self):
        for failure in (None, "probe", "live"):
            obj = self.controller()
            retained = copy.deepcopy(obj.s3.objects[binding.LOCK_KEY])
            obj.commit, obj.to_date, obj.timeout = "c" * 40, DATE, 7200
            obj.prefix, obj.receipt_prefix = ROY.probe_prefix + RID + "/", ROY.release_prefix + RID + "/"
            obj.recover_paused_release = None
            obj.promoted = obj.rerun_attempted = obj.start_uncertain = False
            obj.role_created = obj.role_attempted = False
            obj.task_records, obj.task_mode, obj.candidate = {}, None, None
            elapsed = [0.0]
            obj.clock = lambda: elapsed[0]
            obj.sleep = lambda seconds: elapsed.__setitem__(0, elapsed[0] + seconds)
            obj.last_checkpoint = 0.0
            source = roy_definition()
            source.update(status="ACTIVE", taskDefinitionArn=f"arn:aws:ecs:{binding.REGION}:{binding.ACCOUNT}:task-definition/{ROY.family}:77")
            source["containerDefinitions"][0]["environment"] += [
                {"name": "REPORT_FROM_DATE", "value": "2025-09-24"},
                {"name": "REPORT_SKIP_CREDITNOTE_STORNO_GUARD", "value": "true"}]
            original = {"Name": ROY.service, "State": "ENABLED", "ScheduleExpression": "cron(30 1 * * ? *)",
                        "ScheduleExpressionTimezone": "Europe/Bratislava", "Target": {"Arn": release.CLUSTER,
                        "EcsParameters": {"TaskDefinitionArn": source["taskDefinitionArn"], "LaunchType": "FARGATE"}}}
            schedules = {ROY.service: copy.deepcopy(original), **copy.deepcopy(obj.protected)}
            obj.schedule = lambda name=None: copy.deepcopy(schedules[name or ROY.service])
            obj.all_schedules = lambda: copy.deepcopy(schedules)
            def update(desired):
                self.assertEqual(ROY.service, desired["Name"])
                schedules[ROY.service] = copy.deepcopy(desired)
                obj.known_schedule = copy.deepcopy(desired)
            obj.update = update
            obj.definition = Mock(side_effect=lambda arn: source if arn == source["taskDefinitionArn"]
                                  else {**obj.candidate, "status": "INACTIVE"})
            obj.session = Mock()
            obj.session.client.return_value.get_caller_identity.return_value = {"Account": binding.ACCOUNT}
            obj.exact_image = Mock(return_value=(IMAGE, "123"))
            obj.exclusion = Mock()
            obj.ecs = Mock()
            obj.ecs.register_task_definition.side_effect = lambda **value: {"taskDefinition": {**value, "status": "ACTIVE",
                "taskDefinitionArn": source["taskDefinitionArn"].rsplit(":", 1)[0] + ":78"}}
            obj.create_role, obj.cleanup_role, obj.cleanup_task = Mock(), Mock(), Mock()
            transitions = []
            def start(mode):
                transitions.append(mode)
                self.assertEqual("DISABLED", schedules[ROY.service]["State"])
                if mode == "live":
                    self.assertEqual(obj.candidate["taskDefinitionArn"], schedules[ROY.service]["Target"]["EcsParameters"]["TaskDefinitionArn"])
                    obj.rerun_attempted = True
            obj.start_task = start
            obj.wait_probe = Mock(side_effect=RuntimeError("synthetic-probe-failure") if failure == "probe" else None)
            obj.wait_live = Mock(side_effect=RuntimeError("synthetic-live-failure") if failure == "live" else None)
            with self.subTest(failure=failure), patch.object(release, "exact_source"), \
                 patch.object(release, "committed_gate_sha", return_value="d" * 64), \
                 patch.object(release, "read_private", return_value=b'{"generation_id":"old"}'), patch("builtins.print"):
                if failure:
                    with self.assertRaisesRegex(RuntimeError, "synthetic-"):
                        obj.run()
                else:
                    obj.run()
            self.assertEqual(retained, obj.s3.objects[binding.LOCK_KEY])
            self.assertEqual(obj.protected[VEVO.service], schedules[VEVO.service])
            self.assertEqual(["probe"] if failure == "probe" else ["probe", "live"], transitions)
            self.assertEqual("uncertain" if failure == "live" else "released", read_lease(obj.s3, "roy")[0]["state"])
            self.assertEqual("DISABLED" if failure == "live" else "ENABLED", schedules[ROY.service]["State"])
            if failure == "probe":
                self.assertEqual(original, schedules[ROY.service])
                obj.ecs.deregister_task_definition.assert_called_once()
            else:
                self.assertEqual(obj.candidate["taskDefinitionArn"], schedules[ROY.service]["Target"]["EcsParameters"]["TaskDefinitionArn"])
            self.assertEqual(original["ScheduleExpression"], schedules[ROY.service]["ScheduleExpression"])
            self.assertEqual(original["ScheduleExpressionTimezone"], schedules[ROY.service]["ScheduleExpressionTimezone"])


class RoyHostTests(unittest.TestCase):
    def test_host_marker_binds_real_roy_family_service_ip_and_source(self):
        metadata = {"Family": ROY.family, "Revision": "78", "LaunchType": "FARGATE",
                    "TaskARN": release.CLUSTER.replace(":cluster/", ":task/") + "/" + RID,
                    "Containers": [{"Name": "reporting", "ImageID": IMAGE.split("@")[1],
                                    "Networks": [{"IPv4Addresses": ["172.31.2.3"]}]}]}
        kwargs = {"release_id": RID, "source_commit": "c" * 40, "image_digest": IMAGE.split("@")[1],
                  "gate_sha256": host.sha(Path(host.__file__).read_bytes())}
        with patch.object(host.os, "getcwd", return_value="/app"):
            identity = host.host_identity(metadata, **kwargs, policy=ROY)
            self.assertEqual(ROY.service, identity["service"])
            self.assertEqual(ROY.probe_marker, identity["marker"])
            self.assertEqual("roy", identity["project"])
            self.assertEqual("172.31.2.3", identity["private_ip"])
            with self.assertRaisesRegex(RuntimeError, "family-invalid"):
                host.host_identity(metadata, **kwargs)

    def test_private_writer_cannot_cross_project_prefix(self):
        s3 = S3()
        key = ROY.probe_prefix + RID + "/markers/ready.json"
        host.put_private(s3, key, b"{}", policy=ROY)
        with self.assertRaisesRegex(RuntimeError, "scope-invalid"):
            host.put_private(s3, VEVO.probe_prefix + RID + "/markers/ready.json", b"{}", policy=ROY)
        self.assertEqual([key], s3.puts)

    def test_roy_probe_blocks_other_shop_and_mutations_but_preserves_query_read_flow(self):
        import daily_report_runner as runner
        import requests
        from gql import Client, gql
        required = {"REPORT_PROJECT": "roy", "REPORT_S3_BUCKET": binding.BUCKET, "REPORT_S3_PREFIX": ROY.sink,
                    "REPORT_SKIP_EMAIL": "true", "REPORT_SKIP_INVOICES": "true", "REPORT_SKIP_CREDITNOTE_STORNO_GUARD": "true"}
        actions = [lambda: requests.Session().send(requests.Request("GET", "https://vevo.flox.sk/api/graphql").prepare()),
                   lambda: requests.Session().send(requests.Request("GET", "https://roy.flox.sk/erp/orders/invoices/finalize/1").prepare()),
                   lambda: Client.execute(object(), gql('mutation { preinvoiceOrder(order_num: "synthetic") { id } }')),
                   lambda: runner.send_email_ses(), lambda: runner.maybe_run_creditnote_storno_guard()]
        for action in actions:
            with self.subTest(action=action), patch.dict(host.os.environ, required), patch.object(runner, "main", action):
                with self.assertRaisesRegex(RuntimeError, "probe-forbidden"):
                    host.report_probe(S3(), ROY.probe_prefix + RID + "/", RID, policy=ROY)
        def ordinary_path():
            self.assertEqual("roy", runner.parse_args().project)
            runner.s3_upload_outputs("roy", {})
        with patch.dict(host.os.environ, required), patch.object(runner, "main", ordinary_path), \
             patch.object(host, "isolate_outputs", return_value={"key": "private", "sha256": "hash"}) as isolate:
            self.assertEqual("private", host.report_probe(S3(), ROY.probe_prefix + RID + "/", RID, policy=ROY)["key"])
            self.assertEqual(ROY, isolate.call_args.kwargs["policy"])


if __name__ == "__main__":
    unittest.main()
