import copy
import json
from pathlib import Path
import unittest
from unittest.mock import Mock

from scripts.deploy_order_automations import (
    Deployment, SCHEDULES, SERVICES, candidate_definition, command_for,
    desired_schedule, established_alarm_route, promote_schedules, schedule_request,
)
from scripts.order_automation_host_gate import verify_summary


ACCOUNT = "123456789012"
IMAGE = f"{ACCOUNT}.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting@sha256:" + "a" * 64


def snapshot(name):
    family = SCHEDULES[name]
    return {
        "Name": name, "GroupName": "default", "State": "ENABLED",
        "ScheduleExpression": "cron(0 7 * * ? *)", "ScheduleExpressionTimezone": "Europe/Bratislava",
        "FlexibleTimeWindow": {"Mode": "OFF"},
        "Arn": f"arn:aws:scheduler:eu-central-1:{ACCOUNT}:schedule/default/{name}",
        "Target": {"Arn": f"arn:aws:ecs:eu-central-1:{ACCOUNT}:cluster/vevo-reporting-cluster",
                   "RoleArn": f"arn:aws:iam::{ACCOUNT}:role/scheduler",
                   "EcsParameters": {"TaskDefinitionArn": f"arn:aws:ecs:eu-central-1:{ACCOUNT}:task-definition/{family}:2"}},
    }


class FakeScheduler:
    def __init__(self, fail_call=None, ambiguous=False):
        self.values = {name: snapshot(name) for name in SCHEDULES}
        self.writes = []
        self.fail_call, self.ambiguous = fail_call, ambiguous

    def get_schedule(self, Name):
        return copy.deepcopy(self.values[Name])

    def update_schedule(self, **request):
        self.writes.append(request["Name"])
        fail = len(self.writes) == self.fail_call
        if not fail or self.ambiguous:
            self.values[request["Name"]] = copy.deepcopy(request)
        if fail:
            raise TimeoutError("synthetic lost response")


class FakeAWSFailure(Exception):
    def __init__(self, code):
        self.response = {"Error": {"Code": code}}


class FakeStateClients:
    def __init__(self):
        self.policies = {}
        self.objects = {}
        self.puts = []

    def get_role_policy(self, RoleName, PolicyName):
        key = (RoleName, PolicyName)
        if key not in self.policies:
            raise FakeAWSFailure("NoSuchEntity")
        return {"PolicyDocument": copy.deepcopy(self.policies[key])}

    def put_role_policy(self, RoleName, PolicyName, PolicyDocument):
        self.policies[(RoleName, PolicyName)] = json.loads(PolicyDocument)

    def delete_role_policy(self, RoleName, PolicyName):
        del self.policies[(RoleName, PolicyName)]

    def put_object(self, **request):
        self.puts.append(request)
        key = (request["Bucket"], request["Key"])
        if key in self.objects and request.get("IfNoneMatch") == "*":
            raise FakeAWSFailure("PreconditionFailed")
        self.objects[key] = request["Body"]


class OrderAutomationDeploymentTests(unittest.TestCase):
    def candidates(self):
        original = {name: snapshot(name) for name in SCHEDULES}
        desired = {name: schedule_request(value) for name, value in original.items()}
        for value in desired.values():
            value["Target"]["EcsParameters"]["TaskDefinitionArn"] = value["Target"]["EcsParameters"]["TaskDefinitionArn"][:-1] + "9"
        return original, desired

    def test_successful_promotion_verifies_every_exact_schedule(self):
        scheduler = FakeScheduler()
        original, desired = self.candidates()
        promote_schedules(scheduler, original, desired)
        self.assertEqual(scheduler.writes, list(SCHEDULES))
        self.assertEqual(scheduler.values, desired)

    def test_failure_restores_preceding_changes_and_ambiguous_write(self):
        for ambiguous in (False, True):
            with self.subTest(ambiguous=ambiguous):
                scheduler = FakeScheduler(fail_call=3, ambiguous=ambiguous)
                original, desired = self.candidates()
                with self.assertRaisesRegex(RuntimeError, "rollback-verified"):
                    promote_schedules(scheduler, original, desired)
                for name in SCHEDULES:
                    self.assertEqual(schedule_request(scheduler.values[name]), schedule_request(original[name]))

    def test_concurrent_change_before_promotion_makes_no_writes(self):
        scheduler = FakeScheduler()
        original, desired = self.candidates()
        scheduler.values[next(iter(SCHEDULES))]["State"] = "DISABLED"
        with self.assertRaisesRegex(RuntimeError, "changed-before-promotion"):
            promote_schedules(scheduler, original, desired)
        self.assertEqual(scheduler.writes, [])

    def test_rollback_does_not_overwrite_another_operator(self):
        scheduler = FakeScheduler(fail_call=2)
        original, desired = self.candidates()
        base_update = scheduler.update_schedule

        def update(**request):
            try:
                return base_update(**request)
            except TimeoutError:
                scheduler.values[request["Name"]]["Description"] = "different operator"
                raise

        scheduler.update_schedule = update
        with self.assertRaisesRegex(RuntimeError, "rollback-requires-operator"):
            promote_schedules(scheduler, original, desired)
        name = list(SCHEDULES)[1]
        self.assertEqual(scheduler.values[name]["Description"], "different operator")

    def test_partial_promotion_set_is_rejected(self):
        original, desired = self.candidates()
        del desired[next(iter(desired))]
        scheduler = FakeScheduler()
        with self.assertRaisesRegex(RuntimeError, "incomplete-promotion-set"):
            promote_schedules(scheduler, original, desired)
        self.assertEqual(scheduler.writes, [])

    def pin_deployment(self):
        deployment = object.__new__(Deployment)
        deployment.account = ACCOUNT
        deployment.session = Mock()
        deployment.session.client.return_value.describe_images.return_value = {"imageDetails": [{"imageDigest": "sha256:" + "a" * 64}]}
        deployment.scheduler = FakeScheduler()
        deployment.ecs = Mock()
        definitions = []
        for family, (_, kind, _) in SERVICES.items():
            definitions.append({"taskDefinition": {"family": family, "networkMode": "awsvpc", "taskRoleArn": "role/test", "containerDefinitions": [{
                "name": "reporting", "image": IMAGE if kind == "cancellation" else IMAGE.split("@")[0] + ":latest",
                "command": command_for(family), "environment": [{"name": "UNCHANGED", "value": "keep"}],
                "secrets": [{"name": "EXISTING_SECRET_REFERENCE", "valueFrom": "keep-reference"}],
            }]}})
        deployment.ecs.describe_task_definition.side_effect = definitions
        deployment.ecs.register_task_definition.side_effect = [
            {"taskDefinition": {"taskDefinitionArn": f"arn:aws:ecs:eu-central-1:{ACCOUNT}:task-definition/{family}:9"}}
            for family in ("roy-invoice-daily", "vevo-invoice-daily")
        ]
        deployment.evidence = {"hosts": []}
        deployment.bucket = Mock(return_value="private-evidence")
        deployment.save_private = Mock()
        deployment.host_gate = Mock(return_value={"marker": "synthetic-verified"})
        return deployment, definitions

    def test_pin_current_changes_only_four_invoice_refs_after_both_hosts(self):
        deployment, definitions = self.pin_deployment()
        initial = copy.deepcopy(deployment.scheduler.values)
        deployment.pin_current("sha256:" + "a" * 64)
        self.assertEqual(deployment.host_gate.call_count, 2)
        self.assertTrue(all(call.kwargs["old_image"] for call in deployment.host_gate.call_args_list))
        self.assertEqual(len(deployment.scheduler.writes), 4)
        self.assertNotIn("roy-unpaid-order-cancellation", deployment.scheduler.writes)
        for index, call in enumerate(deployment.ecs.register_task_definition.call_args_list):
            registered = call.kwargs
            expected = copy.deepcopy(definitions[index]["taskDefinition"])
            expected["containerDefinitions"][0]["image"] = IMAGE
            self.assertEqual(registered, expected)
        for name, value in deployment.scheduler.values.items():
            expected = schedule_request(initial[name])
            if name != "roy-unpaid-order-cancellation":
                expected["Target"]["EcsParameters"]["TaskDefinitionArn"] = expected["Target"]["EcsParameters"]["TaskDefinitionArn"][:-1] + "9"
            self.assertEqual(schedule_request(value), expected)

    def test_pin_failed_second_host_never_changes_schedules(self):
        deployment, _ = self.pin_deployment()
        deployment.host_gate.side_effect = [{"first": "verified"}, RuntimeError("second-host-failed")]
        with self.assertRaisesRegex(RuntimeError, "second-host-failed"):
            deployment.pin_current("sha256:" + "a" * 64)
        self.assertEqual(deployment.scheduler.writes, [])

    def test_pin_rejects_new_image_published_during_host_checks(self):
        deployment, _ = self.pin_deployment()
        deployment.session.client.return_value.describe_images.side_effect = [
            {"imageDetails": [{"imageDigest": "sha256:" + "a" * 64}]},
            {"imageDetails": [{"imageDigest": "sha256:" + "b" * 64}]},
        ]
        with self.assertRaisesRegex(RuntimeError, "changed-during-pin"):
            deployment.pin_current("sha256:" + "a" * 64)
        self.assertEqual(deployment.scheduler.writes, [])

    def test_candidate_pins_image_project_path_and_removes_one_off_flags(self):
        family = "roy-invoice-daily"
        source = {"family": family, "revision": 2, "networkMode": "awsvpc", "containerDefinitions": [{
            "name": "reporting", "image": "mutable:latest", "command": command_for(family),
            "environment": [{"name": "INVOICE_FROM_DATE", "value": "2020-01-01"},
                            {"name": "REPORT_INVOICE_DRY_RUN", "value": "true"}],
            "secrets": [{"name": "BIZNISWEB_API_TOKEN", "valueFrom": "synthetic"},
                        {"name": "FACEBOOK_ACCESS_TOKEN", "valueFrom": "unneeded"}],
        }]}
        result = candidate_definition(source, family, IMAGE)
        container = result["containerDefinitions"][0]
        self.assertEqual(container["image"], IMAGE)
        self.assertEqual(container["workingDirectory"], "/app")
        env = {item["name"]: item["value"] for item in container["environment"]}
        self.assertEqual(env["ORDER_AUTOMATION_STATE_PREFIX"], "data/roy/order-automation")
        self.assertNotIn("INVOICE_FROM_DATE", env)
        self.assertNotIn("REPORT_INVOICE_DRY_RUN", env)
        self.assertEqual([item["name"] for item in container["secrets"]], ["BIZNISWEB_API_TOKEN"])
        self.assertNotIn("revision", result)
        self.assertEqual(source["containerDefinitions"][0]["image"], "mutable:latest")
        with self.assertRaisesRegex(RuntimeError, "not-immutable"):
            candidate_definition(source, family, "mutable:latest")
        source["containerDefinitions"][0]["command"] = ["python", "different.py"]
        with self.assertRaisesRegex(RuntimeError, "command-drift"):
            candidate_definition(source, family, IMAGE)

    def test_schedule_uses_explicit_timezone_cadence_dlq_and_short_delivery_retry(self):
        name = "roy-daily-invoice-generation"
        settings = {"invoice_generation": {"schedule_name": name, "schedule_expression": "cron(5/15 * * * ? *)", "timezone": "Europe/Bratislava"}}
        result = desired_schedule(snapshot(name), f"arn:aws:ecs:eu-central-1:{ACCOUNT}:task-definition/roy-invoice-daily:9", "test-dlq", settings)
        self.assertEqual(result["ScheduleExpression"], "cron(5/15 * * * ? *)")
        self.assertEqual(result["Target"]["RetryPolicy"]["MaximumEventAgeInSeconds"], 900)
        self.assertEqual(result["Target"]["DeadLetterConfig"], {"Arn": "test-dlq"})
        settings["invoice_generation"]["schedule_expression"] = "rate(1 minute)"
        with self.assertRaisesRegex(RuntimeError, "cadence-drift"):
            desired_schedule(snapshot(name), f"arn:aws:ecs:eu-central-1:{ACCOUNT}:task-definition/roy-invoice-daily:9", "test-dlq", settings)

    def test_host_probe_rejects_live_mode_and_partial_application_failure(self):
        for summary in ({"enabled": True, "dry_run": False}, {"enabled": False, "dry_run": True},
                        {"enabled": True, "dry_run": True, "failed_invoice_emails": 1},
                        {"enabled": True, "dry_run": True, "missing_invoice_ids": 1},
                        {"enabled": True, "dry_run": True, "ambiguous_invoice_operations": 1},
                        {"enabled": True, "dry_run": True, "skipped_locked": True},
                        {"enabled": True, "dry_run": True, "invoice_scan_complete": False}):
            with self.subTest(summary=summary), self.assertRaises(RuntimeError):
                verify_summary(summary, "invoice")
        verify_summary({"enabled": True, "dry_run": True, "failed_orders": 0}, "cancellation")

    def test_read_bucket_accepts_only_exact_json_key_without_exporting_secret_bundle(self):
        deployment = object.__new__(Deployment)
        deployment.account = ACCOUNT
        session = Mock()
        session.client.return_value.get_secret_value.return_value = {"SecretString": json.dumps({"REPORT_S3_BUCKET": "private-test-bucket", "UNRELATED_SECRET": "do-not-return"})}
        deployment.session = session
        ref = f"arn:aws:secretsmanager:eu-central-1:{ACCOUNT}:secret:test-secret:REPORT_S3_BUCKET::"
        definition = {"containerDefinitions": [{"secrets": [{"name": "REPORT_S3_BUCKET", "valueFrom": ref}]}]}
        self.assertEqual(deployment.bucket(definition), "private-test-bucket")
        definition["containerDefinitions"][0]["secrets"][0]["valueFrom"] = ref.replace(":REPORT_S3_BUCKET::", ":UNRELATED_SECRET::")
        with self.assertRaisesRegex(RuntimeError, "reference-type"):
            deployment.bucket(definition)

    def test_state_bootstrap_never_overwrites_existing_journal_and_iam_is_reversible(self):
        for existing in (False, True):
            with self.subTest(existing=existing):
                clients = FakeStateClients()
                deployment = object.__new__(Deployment)
                deployment.account = ACCOUNT
                deployment.session = Mock()
                deployment.session.client.return_value = clients
                deployment.evidence = {}
                deployment.evidence_bucket = "private-evidence"
                deployment.save_private = Mock()
                key = ("private-state", "data/roy/order-automation/state.json")
                old = b'{"synthetic_existing_journal":true}'
                if existing:
                    clients.objects[key] = old
                deployment.prepare_state("roy", "private-state", "role/test")
                if existing:
                    self.assertEqual(clients.objects[key], old)
                else:
                    self.assertEqual(json.loads(clients.objects[key])["project"], "roy")
                self.assertEqual(clients.puts[0]["IfNoneMatch"], "*")
                statement = clients.policies[("test", "OrderAutomationState-roy")]["Statement"][0]
                self.assertEqual(statement["Action"], ["s3:GetObject", "s3:PutObject"])
                self.assertEqual(statement["Resource"], "arn:aws:s3:::private-state/data/roy/order-automation/state.json")
                deployment.restore_state_policies()
                self.assertEqual(clients.policies, {})
                self.assertIn(key, clients.objects)

    def test_state_policy_rollback_preserves_concurrent_operator_change(self):
        clients = FakeStateClients()
        deployment = object.__new__(Deployment)
        deployment.session = Mock()
        deployment.session.client.return_value = clients
        deployment.evidence = {"state_policies": [{"role": "test", "name": "owned-policy", "previous": None,
                                                  "candidate": {"candidate": True}}]}
        clients.policies[("test", "owned-policy")] = {"other_operator": True}
        with self.assertRaisesRegex(RuntimeError, "rollback-requires-operator"):
            deployment.restore_state_policies()
        self.assertEqual(clients.policies[("test", "owned-policy")], {"other_operator": True})

    def test_failed_candidate_cannot_reach_scheduler_promotion(self):
        deployment = object.__new__(Deployment)
        deployment.commit = "a" * 40
        deployment.account = ACCOUNT
        deployment.session = Mock()
        deployment.session.client.return_value.describe_images.return_value = {"imageDetails": [{"imageDigest": "sha256:" + "a" * 64}]}
        deployment.scheduler = FakeScheduler()
        deployment.ecs = Mock()
        definitions = []
        for family in SERVICES:
            definitions.append({"taskDefinition": {"family": family, "networkMode": "awsvpc", "taskRoleArn": "role/test",
                "containerDefinitions": [{"name": "reporting", "command": command_for(family)}]}})
        deployment.ecs.describe_task_definition.side_effect = definitions
        deployment.ecs.register_task_definition.return_value = {"taskDefinition": {"taskDefinitionArn": "candidate"}}
        deployment.evidence = {"hosts": []}
        deployment.bucket = Mock(return_value="private-test-bucket")
        deployment.save_private = Mock()
        deployment.prepare_state = Mock()
        deployment.restore_state_policies = Mock()
        deployment.host_gate = Mock(side_effect=RuntimeError("synthetic-candidate-failure"))
        deployment.provision_monitoring = Mock()
        with self.assertRaisesRegex(RuntimeError, "candidate-failure"):
            deployment.run()
        self.assertEqual(deployment.scheduler.writes, [])
        deployment.provision_monitoring.assert_not_called()
        deployment.restore_state_policies.assert_called_once()

    def test_live_alarms_use_mode_dimension_and_only_established_operator_route(self):
        deployment = object.__new__(Deployment)
        deployment.account = ACCOUNT
        deployment.session = Mock()
        deployment.alarm_actions = [f"arn:aws:sns:eu-central-1:{ACCOUNT}:verified-test-operator-route"]
        client = deployment.session.client.return_value
        client.create_queue.return_value = {"QueueUrl": "queue-url"}
        client.get_queue_attributes.return_value = {"Attributes": {"QueueArn": "queue-arn"}}
        task = {"taskRoleArn": "role/task", "executionRoleArn": "role/execution"}
        deployment.provision_monitoring("roy-invoice-daily", [snapshot("roy-daily-invoice-generation")], "private-test-bucket", task)
        alarms = [call.kwargs for call in client.put_metric_alarm.call_args_list]
        live = [item for item in alarms if item["Namespace"] == "BizniswebReporting"]
        self.assertGreaterEqual(len(live), 5)
        for alarm in live:
            self.assertIn({"Name": "RunMode", "Value": "live"}, alarm["Dimensions"])
            self.assertEqual(alarm["AlarmActions"], deployment.alarm_actions)
        policy = json.loads(client.set_queue_attributes.call_args.kwargs["Attributes"]["Policy"])
        self.assertEqual(policy["Statement"][0]["Condition"]["StringEquals"]["aws:SourceAccount"], ACCOUNT)

    def test_operator_route_requires_existing_matching_alarms_and_confirmed_lambda(self):
        session = Mock()
        client = session.client.return_value
        topic = f"arn:aws:sns:eu-central-1:{ACCOUNT}:vevo-reporting-alerts-mil-final"
        client.describe_alarms.return_value = {"MetricAlarms": [
            {"AlarmName": name, "ActionsEnabled": True, "AlarmActions": [topic]}
            for name in ("roy-reporting-run-failed", "vevo-reporting-run-failed")
        ]}
        client.get_topic_attributes.return_value = {"Attributes": {"TopicArn": topic}}
        row = {"TopicArn": topic, "Protocol": "lambda", "SubscriptionArn": topic + ":confirmed-test-subscription",
               "Endpoint": f"arn:aws:lambda:eu-central-1:{ACCOUNT}:function:existing-operator-route"}
        client.list_subscriptions_by_topic.return_value = {"Subscriptions": [row]}
        self.assertEqual(established_alarm_route(session, ACCOUNT), topic)
        client.subscribe.assert_not_called()
        client.publish.assert_not_called()
        row["SubscriptionArn"] = "PendingConfirmation"
        with self.assertRaisesRegex(RuntimeError, "subscription-drift"):
            established_alarm_route(session, ACCOUNT)

    def test_managed_workflow_is_main_only_and_legacy_has_no_aws_path(self):
        root = Path(__file__).resolve().parents[1]
        workflow = (root / ".github/workflows/deploy-order-automations.yml").read_text(encoding="utf-8")
        legacy = (root / ".github/workflows/deploy-unpaid-order-cancellation.yml").read_text(encoding="utf-8")
        self.assertIn("github.ref == 'refs/heads/main'", workflow)
        self.assertIn("cancel-in-progress: false", workflow)
        self.assertNotIn("\n  push:", workflow)
        self.assertNotIn("configure-aws-credentials", legacy)
        self.assertNotIn("\n  push:", legacy)
        self.assertNotIn("aws ecs", legacy)


if __name__ == "__main__":
    unittest.main()
