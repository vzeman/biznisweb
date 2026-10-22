import copy
import json
import unittest
from unittest.mock import Mock, patch

from scripts import vevo_report_current_rerun as rerun


release = rerun.release
SOURCE, HEAD = "a" * 40, "b" * 40
IMAGE = f"{release.ACCOUNT}.dkr.ecr.{release.REGION}.amazonaws.com/vevo-reporting@sha256:" + "c" * 64
DEFINITION = f"arn:aws:ecs:{release.REGION}:{release.ACCOUNT}:task-definition/vevo-reporting-daily:53"


def fixture():
    env = {"REPORT_PROJECT": "vevo", "REPORT_S3_BUCKET": release.BUCKET,
           "REPORT_S3_PREFIX": "daily-reports/vevo", "REPORT_SKIP_INVOICES": "true",
           "REPORT_SKIP_CREDITNOTE_STORNO_GUARD": "true", "REPORT_FROM_DATE": "2025-05-03"}
    definition = {"family": "vevo-reporting-daily", "networkMode": "awsvpc", "status": "ACTIVE",
                  "taskDefinitionArn": DEFINITION,
                  "taskRoleArn": f"arn:aws:iam::{release.ACCOUNT}:role/BiznisWebReportingTaskRole-vevo",
                  "executionRoleArn": f"arn:aws:iam::{release.ACCOUNT}:role/ecsTaskExecutionRole",
                  "containerDefinitions": [{"name": "reporting", "image": IMAGE, "workingDirectory": "/app",
                    "environment": [{"name": key, "value": value} for key, value in env.items()]}]}
    schedule = {"Name": release.VEVO.service, "State": "ENABLED", "ScheduleExpression": "cron(0 1 * * ? *)",
                "Target": {"Arn": release.CLUSTER, "Input": "{}", "EcsParameters": {
                    "TaskDefinitionArn": DEFINITION, "LaunchType": "FARGATE", "NetworkConfiguration": {
                        "awsvpcConfiguration": {"Subnets": ["subnet-original"], "SecurityGroups": ["sg-original"],
                                                "AssignPublicIp": "ENABLED"}}}}}
    return schedule, definition


class CurrentIdentityTests(unittest.TestCase):
    def test_active_identity_and_financial_boundary_pass(self):
        schedule, definition = fixture()
        effective = rerun.validate_current(schedule, definition, DEFINITION, IMAGE)
        self.assertEqual("2025-05-03", effective["REPORT_FROM_DATE"])

    def test_stale_definition_image_or_wrong_project_rejected(self):
        for mutation in ("target", "definition", "inactive", "image", "project"):
            schedule, definition = fixture()
            if mutation == "target":
                schedule["Target"]["EcsParameters"]["TaskDefinitionArn"] = DEFINITION[:-2] + "54"
            elif mutation == "definition":
                definition["taskDefinitionArn"] = DEFINITION[:-2] + "54"
            elif mutation == "inactive":
                definition["status"] = "INACTIVE"
            elif mutation == "image":
                definition["containerDefinitions"][0]["image"] = IMAGE[:-64] + "d" * 64
            else:
                definition["containerDefinitions"][0]["environment"][0]["value"] = "roy"
            with self.subTest(mutation=mutation), self.assertRaises(RuntimeError):
                rerun.validate_current(schedule, definition, DEFINITION, IMAGE)

    def test_financial_and_command_overrides_rejected(self):
        for name in ("REPORT_SKIP_INVOICES", "REPORT_SKIP_CREDITNOTE_STORNO_GUARD"):
            schedule, definition = fixture()
            schedule["Target"]["Input"] = json.dumps({"containerOverrides": [{"name": "reporting",
                "environment": [{"name": name, "value": "false"}]}]})
            with self.subTest(name=name), self.assertRaisesRegex(RuntimeError, "financial-boundary"):
                rerun.validate_current(schedule, definition, DEFINITION, IMAGE)
        schedule, definition = fixture()
        schedule["Target"]["Input"] = json.dumps({"containerOverrides": [{"name": "reporting", "command": []}]})
        with self.assertRaisesRegex(RuntimeError, "container-override"):
            rerun.validate_current(schedule, definition, DEFINITION, IMAGE)

    def test_schedule_only_config_cannot_be_silently_lost(self):
        schedule, definition = fixture()
        schedule["Target"]["Input"] = json.dumps({"containerOverrides": [{"name": "reporting",
            "environment": [{"name": "REPORT_FROM_DATE", "value": "2026-01-01"}]}]})
        with self.assertRaisesRegex(RuntimeError, "unpreserved-schedule-environment"):
            rerun.validate_current(schedule, definition, DEFINITION, IMAGE)

    def test_image_source_must_be_ancestor_of_clean_merged_controller(self):
        with patch.object(release, "exact_source") as exact, patch.object(rerun.subprocess, "run") as run:
            run.return_value.returncode = 0
            rerun.controller_source(HEAD, SOURCE)
            exact.assert_called_once_with(HEAD)
            self.assertEqual(["git", "merge-base", "--is-ancestor", SOURCE, HEAD], run.call_args.args[0])
            run.return_value.returncode = 1
            with self.assertRaisesRegex(RuntimeError, "source-not-ancestor"):
                rerun.controller_source(HEAD, SOURCE)


class CurrentRerunLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.schedule, self.definition = fixture()
        session = Mock()
        clients = {name: Mock() for name in ("ecs", "scheduler", "s3", "iam", "sts")}
        session.client.side_effect = clients.__getitem__
        clients["sts"].get_caller_identity.return_value = {"Account": release.ACCOUNT}
        obj = rerun.CurrentRerun(session, HEAD, SOURCE, "2026-10-21", 7200,
                                expected_definition=DEFINITION, expected_image=IMAGE, release_id="e" * 32)
        self.obj, self.clients = obj, clients
        obj.schedule = Mock(side_effect=lambda: copy.deepcopy(self.schedule))
        obj.definition = Mock(side_effect=lambda arn: copy.deepcopy(self.definition))
        obj.all_schedules = Mock(return_value={release.VEVO.service: self.schedule, "peer": {"State": "ENABLED"}})
        obj.exact_image = Mock(return_value=(IMAGE, "build-success"))
        obj.inspect_peer_boundary = Mock()
        obj.exclusion = Mock()
        obj.live_outputs = Mock(return_value={"original": "version"})
        obj.no_report_tasks = Mock()
        obj.event = Mock()
        obj.checkpoint = Mock()
        obj.sleep = Mock()
        obj.lease = Mock()

        def acquire():
            obj.lease.acquire()
            obj.lease_owned = True
        obj.acquire_release_lease = Mock(side_effect=acquire)

        def update(desired):
            if self.schedule != obj.known_schedule:
                raise RuntimeError("foreign-schedule")
            self.schedule = copy.deepcopy(desired)
            obj.known_schedule = copy.deepcopy(desired)
        obj.update = Mock(side_effect=update)
        clients["ecs"].run_task.return_value = {"tasks": [{"taskArn": "owned-task"}], "failures": []}
        obj.wait_live = Mock()
        self.patches = [patch.object(rerun, "controller_source"),
                        patch.object(release.binding, "require_private_bucket"),
                        patch.object(release, "committed_gate_sha", return_value="f" * 64),
                        patch.object(release, "read_private", return_value=b'{"generation_id":"20261021T233137Z"}')]
        for context in self.patches:
            context.start()
            self.addCleanup(context.stop)

    def assert_no_deployment(self):
        self.clients["ecs"].register_task_definition.assert_not_called()
        self.clients["ecs"].deregister_task_definition.assert_not_called()
        self.assertEqual([], self.clients["iam"].mock_calls)

    def test_success_reuses_definition_fresh_read_skips_side_effects_restores_schedule(self):
        original = copy.deepcopy(self.schedule)
        self.obj.run()
        self.assertEqual(original, self.schedule)
        self.assertEqual(12, self.obj.sleep.call_count)
        dispatch = self.clients["ecs"].run_task.call_args.kwargs
        self.assertEqual(DEFINITION, dispatch["taskDefinition"])
        self.assertEqual("e" * 32 + "-live", dispatch["clientToken"])
        self.assertEqual({"awsvpcConfiguration": {"subnets": ["subnet-original"],
                         "securityGroups": ["sg-original"], "assignPublicIp": "ENABLED"}},
                         dispatch["networkConfiguration"])
        override = dispatch["overrides"]["containerOverrides"][0]
        self.assertIn("scripts/vevo_report_image_host.py", override["command"])
        self.assertIn(SOURCE, override["command"])
        self.assertNotIn(HEAD, override["command"])
        env = {row["name"]: row["value"] for row in override["environment"]}
        for name in ("REPORT_SKIP_EMAIL", "REPORT_SKIP_INVOICES", "REPORT_SKIP_CREDITNOTE_STORNO_GUARD",
                     "REPORT_FORCE_NO_CACHE", "REPORT_FORCE_CLEAR_CACHE"):
            self.assertEqual("true", env[name])
        self.assertEqual("2026-10-21", env["REPORT_TO_DATE"])
        self.obj.wait_live.assert_called_once_with()
        self.obj.lease.release.assert_called_once_with()
        self.assertEqual("rerun-complete", self.obj.event.call_args.args[0])
        self.assert_no_deployment()

    def test_stale_expected_image_fails_before_lease_or_dispatch(self):
        self.obj.exact_image.return_value = (IMAGE[:-64] + "d" * 64, "build-other")
        with self.assertRaisesRegex(RuntimeError, "source-image-mismatch"):
            self.obj.run()
        self.obj.acquire_release_lease.assert_not_called()
        self.clients["ecs"].run_task.assert_not_called()
        self.assert_no_deployment()

    def test_pre_dispatch_failure_restores_without_deregistering_active_definition(self):
        original = copy.deepcopy(self.schedule)
        self.obj.sleep.side_effect = RuntimeError("quiet-window-failed")
        with self.assertRaisesRegex(RuntimeError, "quiet-window-failed"):
            self.obj.run()
        self.assertEqual(original, self.schedule)
        self.clients["ecs"].run_task.assert_not_called()
        self.obj.lease.release.assert_called_once_with()
        self.obj.lease.retain_uncertain.assert_not_called()
        self.assert_no_deployment()

    def test_failed_live_or_uncertain_dispatch_remains_paused_and_never_replays(self):
        for mode in ("dispatch", "live"):
            with self.subTest(mode=mode):
                if mode == "dispatch":
                    self.clients["ecs"].run_task.side_effect = RuntimeError("dispatch-unknown")
                else:
                    self.clients["ecs"].run_task.side_effect = None
                    self.obj.wait_live.side_effect = RuntimeError("live-failed")
                with self.assertRaises(RuntimeError):
                    self.obj.run()
                self.assertEqual("DISABLED", self.schedule["State"])
                self.assertEqual(DEFINITION, self.schedule["Target"]["EcsParameters"]["TaskDefinitionArn"])
                self.obj.lease.release.assert_not_called()
                self.obj.lease.retain_uncertain.assert_called()
                self.assertEqual(1, self.clients["ecs"].run_task.call_count)
                self.assert_no_deployment()
                if mode == "dispatch":
                    # Fresh isolated fixture for the second failure phase.
                    self.doCleanups()
                    self.setUp()

    def test_foreign_schedule_on_failure_is_not_overwritten(self):
        def fail():
            self.schedule["Target"]["EcsParameters"]["TaskDefinitionArn"] = DEFINITION[:-2] + "54"
            raise RuntimeError("live-failed")
        self.obj.wait_live.side_effect = fail
        with self.assertRaisesRegex(RuntimeError, "foreign-schedule"):
            self.obj.run()
        self.assertTrue(self.schedule["Target"]["EcsParameters"]["TaskDefinitionArn"].endswith(":54"))
        self.obj.lease.release.assert_not_called()
        self.obj.lease.retain_uncertain.assert_called()
        self.assert_no_deployment()

    def test_definition_drift_detected_by_checkpoint_guard(self):
        self.obj.original_source = copy.deepcopy(self.definition)
        self.definition["containerDefinitions"][0]["environment"].append({"name": "OTHER", "value": "changed"})
        with self.assertRaisesRegex(RuntimeError, "active-definition-drift"):
            self.obj.verify_definition()


if __name__ == "__main__":
    unittest.main()
