import json
import unittest

from scripts.deploy_invoice_controls import (
    DLQ_RETENTION_SECONDS,
    DEFAULT_NAMESPACE,
    EXPECTED_AUTOMATION_START_DATE,
    HEARTBEAT_DATAPOINTS_TO_ALARM,
    HEARTBEAT_EVALUATION_PERIODS,
    HEARTBEAT_PERIOD_SECONDS,
    InvoiceControlDeployer,
    InvoiceRuntime,
    LOG_RETENTION_DAYS,
    _schedule_update_request,
    assert_alarm_matches,
    build_application_alarm,
    build_dlq_alarm,
    build_failure_metric_filter,
    build_heartbeat_alarm,
    build_queue_policy,
    build_queue_policy_for_account,
    build_schedule_target,
    build_scheduler_dlq_policy,
    build_task_definition_registration,
    deployment_validation_image,
    ecr_repository_name,
    expected_invoice_command,
    load_control_config,
    parse_ecr_image_uri,
    pinned_image_uri,
    task_definition_family,
)


class InvoiceControlTests(unittest.TestCase):
    def test_project_controls_use_separate_after_midnight_reconciliation_slots(self) -> None:
        vevo = load_control_config("vevo")
        roy = load_control_config("roy")

        self.assertEqual("cron(30 0 * * ? *)", vevo.final_schedule_expression)
        self.assertEqual("cron(45 0 * * ? *)", roy.final_schedule_expression)
        self.assertEqual("Europe/Bratislava", vevo.timezone)
        self.assertEqual("Europe/Bratislava", roy.timezone)
        self.assertEqual("2026-07-17", EXPECTED_AUTOMATION_START_DATE)
        self.assertNotEqual(vevo.task_family, roy.task_family)

    def test_schedule_target_sets_dlq_and_explicit_regular_command(self) -> None:
        target = build_schedule_target(
            {
                "Arn": "arn:aws:ecs:eu-central-1:123:cluster/reporting",
                "RoleArn": "arn:aws:iam::123:role/scheduler",
                "RetryPolicy": {
                    "MaximumEventAgeInSeconds": 86400,
                    "MaximumRetryAttempts": 185,
                },
                "EcsParameters": {
                    "TaskDefinitionArn": (
                        "arn:aws:ecs:eu-central-1:123:"
                        "task-definition/vevo-invoice-daily:2"
                    ),
                    "TaskCount": 1,
                    "EnableExecuteCommand": False,
                },
                "Input": "stale",
                "DeadLetterConfig": {"Arn": "stale"},
            },
            queue_arn="arn:aws:sqs:eu-central-1:123:vevo-invoice-scheduler-dlq",
            container_name="reporting",
            project="vevo",
            reconcile=False,
        )

        self.assertEqual(
            "arn:aws:sqs:eu-central-1:123:vevo-invoice-scheduler-dlq",
            target["DeadLetterConfig"]["Arn"],
        )
        payload = json.loads(target["Input"])
        self.assertEqual(
            expected_invoice_command("vevo", reconcile=False),
            payload["containerOverrides"][0]["command"],
        )
        self.assertNotIn("--reconcile", payload["containerOverrides"][0]["command"])
        self.assertFalse(target["EcsParameters"]["EnableExecuteCommand"])

    def test_schedule_target_sets_reconciliation_command(self) -> None:
        target = build_schedule_target(
            {
                "Arn": "arn:aws:ecs:eu-central-1:123:cluster/reporting",
                "RoleArn": "arn:aws:iam::123:role/scheduler",
                "EcsParameters": {
                    "TaskDefinitionArn": (
                        "arn:aws:ecs:eu-central-1:123:"
                        "task-definition/roy-invoice-daily:2"
                    )
                },
            },
            queue_arn="arn:aws:sqs:eu-central-1:123:roy-invoice-scheduler-dlq",
            container_name="reporting",
            project="roy",
            reconcile=True,
        )

        payload = json.loads(target["Input"])
        self.assertEqual(
            ["python", "invoice_runner.py", "--project", "roy", "--reconcile"],
            payload["containerOverrides"][0]["command"],
        )

    def test_queue_policy_allows_only_both_project_schedules(self) -> None:
        queue_arn = "arn:aws:sqs:eu-central-1:123:vevo-invoice-scheduler-dlq"
        schedule_arns = [
            "arn:aws:scheduler:eu-central-1:123:schedule/default/vevo-primary",
            "arn:aws:scheduler:eu-central-1:123:schedule/default/vevo-final",
        ]
        policy = build_queue_policy(queue_arn, schedule_arns)
        statement = policy["Statement"][0]

        self.assertEqual("scheduler.amazonaws.com", statement["Principal"]["Service"])
        self.assertEqual("sqs:SendMessage", statement["Action"])
        self.assertEqual(queue_arn, statement["Resource"])
        self.assertEqual(
            sorted(schedule_arns),
            statement["Condition"]["ArnEquals"]["aws:SourceArn"],
        )

    def test_queue_policy_restricts_source_account(self) -> None:
        policy = build_queue_policy_for_account(
            "arn:aws:sqs:eu-central-1:123:vevo-invoice-scheduler-dlq",
            [
                "arn:aws:scheduler:eu-central-1:123:schedule/default/primary",
                "arn:aws:scheduler:eu-central-1:123:schedule/default/final",
            ],
            "123",
        )
        self.assertEqual(
            {"aws:SourceAccount": "123"},
            policy["Statement"][0]["Condition"]["StringEquals"],
        )

    def test_scheduler_role_policy_deduplicates_queue_arns(self) -> None:
        first = "arn:aws:sqs:eu-central-1:123:vevo-invoice-scheduler-dlq"
        second = "arn:aws:sqs:eu-central-1:123:roy-invoice-scheduler-dlq"
        policy = build_scheduler_dlq_policy([first, second, first])
        self.assertEqual([second, first], policy["Statement"][0]["Resource"])

    def test_schedule_update_uses_only_mutable_scheduler_fields(self) -> None:
        request = _schedule_update_request(
            {
                "Name": "vevo-final",
                "Arn": "immutable",
                "CreationDate": "immutable",
                "GroupName": "default",
                "FlexibleTimeWindow": {"Mode": "OFF"},
                "Description": "invoice sweep",
            },
            expression="cron(30 0 * * ? *)",
            timezone="Europe/Bratislava",
            target={"Arn": "cluster", "RoleArn": "role", "EcsParameters": {}},
        )

        self.assertNotIn("Arn", request)
        self.assertNotIn("CreationDate", request)
        self.assertEqual("ENABLED", request["State"])
        self.assertEqual("invoice sweep", request["Description"])
        self.assertEqual("cron(30 0 * * ? *)", request["ScheduleExpression"])
        disabled = _schedule_update_request(
            {"Name": "vevo-final", "FlexibleTimeWindow": {"Mode": "OFF"}},
            expression="cron(30 0 * * ? *)",
            timezone="Europe/Bratislava",
            target={"Arn": "cluster", "RoleArn": "role", "EcsParameters": {}},
            state="DISABLED",
        )
        self.assertEqual("DISABLED", disabled["State"])

    def test_task_definition_is_cloned_with_immutable_image_digest(self) -> None:
        digest = "sha256:" + ("a" * 64)
        request = build_task_definition_registration(
            {
                "taskDefinitionArn": "ignored",
                "revision": 2,
                "status": "ACTIVE",
                "family": "vevo-invoice-daily",
                "networkMode": "awsvpc",
                "requiresCompatibilities": ["FARGATE"],
                "cpu": "256",
                "memory": "512",
                "containerDefinitions": [
                    {
                        "name": "reporting",
                        "image": "123.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting:latest",
                        "essential": True,
                    }
                ],
            },
            container_name="reporting",
            image_digest=digest,
            tags=[{"key": "Purpose", "value": "invoice"}],
        )

        self.assertNotIn("taskDefinitionArn", request)
        self.assertNotIn("revision", request)
        self.assertEqual(
            (
                "123.dkr.ecr.eu-central-1.amazonaws.com/"
                f"vevo-reporting@{digest}"
            ),
            request["containerDefinitions"][0]["image"],
        )
        self.assertEqual([{"key": "Purpose", "value": "invoice"}], request["tags"])
        self.assertEqual(
            f"123.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting@{digest}",
            pinned_image_uri(
                "123.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting@sha256:"
                + ("b" * 64),
                digest,
            ),
        )

    def test_deployment_validation_replaces_previous_runtime_digest(self) -> None:
        old_digest = "sha256:" + ("a" * 64)
        new_digest = "sha256:" + ("b" * 64)
        old_runtime_image = (
            "123.dkr.ecr.eu-central-1.amazonaws.com/"
            f"vevo-reporting@{old_digest}"
        )

        self.assertEqual(
            (
                "123.dkr.ecr.eu-central-1.amazonaws.com/"
                f"vevo-reporting@{new_digest}"
            ),
            deployment_validation_image(old_runtime_image, new_digest),
        )

    def test_task_definition_family_parses_revisioned_arn(self) -> None:
        self.assertEqual(
            "vevo-invoice-daily",
            task_definition_family(
                "arn:aws:ecs:eu-central-1:123:"
                "task-definition/vevo-invoice-daily:42"
            ),
        )

    def test_deferred_bootstrap_waits_for_active_invoice_tasks(self) -> None:
        class FakeEcs:
            def __init__(self) -> None:
                self.calls = 0
                self.stopped: list[str] = []

            def list_tasks(self, **_kwargs):
                self.calls += 1
                return {
                    "taskArns": (
                        ["arn:aws:ecs:eu-central-1:123:task/active"]
                        if self.calls <= 2
                        else []
                    )
                }

            def stop_task(self, **kwargs):
                self.stopped.append(kwargs["task"])
                return {}

        deployer = InvoiceControlDeployer.__new__(InvoiceControlDeployer)
        deployer.ecs = FakeEcs()
        config = load_control_config("vevo")
        target = {
            "Arn": "arn:aws:ecs:eu-central-1:123:cluster/reporting",
            "EcsParameters": {
                "TaskDefinitionArn": (
                    "arn:aws:ecs:eu-central-1:123:"
                    "task-definition/vevo-invoice-daily:2"
                )
            },
        }
        schedule = {"Name": "vevo-invoice", "Target": target}
        runtime = InvoiceRuntime(
            container_name="reporting",
            log_group="/ecs/vevo-invoice-daily",
            task_definition_arn=target["EcsParameters"]["TaskDefinitionArn"],
            image=(
                "123.dkr.ecr.eu-central-1.amazonaws.com/"
                f"vevo-reporting@sha256:{'a' * 64}"
            ),
        )

        deployer._wait_for_invoice_tasks_to_quiesce(
            [(config, schedule, schedule, runtime)],
            timeout_seconds=1,
            poll_seconds=0,
            stable_empty_polls=2,
        )

        self.assertEqual(6, deployer.ecs.calls)
        self.assertEqual(
            ["arn:aws:ecs:eu-central-1:123:task/active"],
            deployer.ecs.stopped,
        )

    def test_bootstrap_failure_arms_heartbeat_for_disabled_schedule(self) -> None:
        class FakeSns:
            def get_topic_attributes(self, **_kwargs):
                return {}

        class FakeScheduler:
            def get_schedule(self, **_kwargs):
                return {"State": "DISABLED"}

        class FakeCloudWatch:
            def __init__(self) -> None:
                self.alarm = None

            def put_metric_alarm(self, **kwargs):
                self.alarm = kwargs

            def describe_alarms(self, **_kwargs):
                return {"MetricAlarms": [self.alarm] if self.alarm else []}

        deployer = InvoiceControlDeployer.__new__(InvoiceControlDeployer)
        deployer.sns = FakeSns()
        deployer.scheduler = FakeScheduler()
        deployer.cloudwatch = FakeCloudWatch()
        deployer.alert_topic_arn = (
            "arn:aws:sns:eu-central-1:123:invoice-alerts"
        )

        deployer.arm_bootstrap_failure_alerts([load_control_config("vevo")])

        self.assertIsNotNone(deployer.cloudwatch.alarm)
        self.assertTrue(deployer.cloudwatch.alarm["ActionsEnabled"])

    def test_ecr_repository_name_accepts_tagged_and_pinned_images(self) -> None:
        tagged = (
            "123000000000.dkr.ecr.eu-central-1.amazonaws.com/"
            "platform/vevo-reporting:latest"
        )
        pinned = (
            "123000000000.dkr.ecr.eu-central-1.amazonaws.com/"
            f"platform/vevo-reporting@sha256:{'a' * 64}"
        )

        self.assertEqual("platform/vevo-reporting", ecr_repository_name(tagged))
        self.assertEqual("platform/vevo-reporting", ecr_repository_name(pinned))
        self.assertEqual(
            (
                "123000000000",
                "eu-central-1",
                "platform/vevo-reporting",
                "",
            ),
            parse_ecr_image_uri(tagged),
        )
        self.assertEqual(
            "sha256:" + ("a" * 64),
            parse_ecr_image_uri(pinned)[3],
        )

    def test_ecr_digest_verification_checks_registry_existence_and_latest(self) -> None:
        digest = "sha256:" + ("a" * 64)

        class FakeEcr:
            def describe_images(self, **kwargs):
                self.request = kwargs
                return {
                    "imageDetails": [
                        {
                            "imageDigest": digest,
                            "imageTags": ["latest", "sha-source"],
                        }
                    ]
                }

        deployer = InvoiceControlDeployer.__new__(InvoiceControlDeployer)
        deployer.account_id = "123000000000"
        deployer.region = "eu-central-1"
        deployer.ecr = FakeEcr()
        image = (
            "123000000000.dkr.ecr.eu-central-1.amazonaws.com/"
            f"vevo-reporting@{digest}"
        )

        deployer._assert_ecr_digest_exists(
            image,
            digest,
            require_latest=True,
        )
        self.assertEqual(
            "123000000000",
            deployer.ecr.request["registryId"],
        )
        self.assertEqual(
            "vevo-reporting",
            deployer.ecr.request["repositoryName"],
        )
        with self.assertRaises(ValueError):
            deployer._assert_ecr_digest_exists(
                image.replace("123000000000", "999999999999"),
                digest,
                require_latest=False,
            )

    def test_observability_specs_are_complete_and_drift_is_rejected(self) -> None:
        config = load_control_config("vevo")
        topic = "arn:aws:sns:eu-central-1:123:invoice-alerts"
        metric_filter = build_failure_metric_filter(config)

        self.assertEqual('"INVOICE_RUN_FAILED"', metric_filter["filterPattern"])
        self.assertEqual(
            "1",
            metric_filter["metricTransformations"][0]["metricValue"],
        )
        for alarm in (
            build_dlq_alarm(config, topic),
            build_application_alarm(config, topic),
            build_heartbeat_alarm(config, topic),
        ):
            self.assertTrue(alarm["ActionsEnabled"])
            self.assertEqual([topic], alarm["AlarmActions"])
            self.assertEqual([topic], alarm["OKActions"])
            assert_alarm_matches(dict(alarm), alarm)

            drifted = dict(alarm)
            drifted["ActionsEnabled"] = False
            with self.assertRaises(AssertionError):
                assert_alarm_matches(drifted, alarm)

        application_alarm = build_application_alarm(config, topic)
        extra_dimensions = dict(application_alarm)
        extra_dimensions["Dimensions"] = [
            {"Name": "Project", "Value": "wrong"}
        ]
        with self.assertRaises(AssertionError):
            assert_alarm_matches(extra_dimensions, application_alarm)

    def test_observability_constants_cover_recovery_window(self) -> None:
        self.assertEqual(1_209_600, DLQ_RETENTION_SECONDS)
        self.assertEqual(90, LOG_RETENTION_DAYS)
        self.assertEqual(3_600, HEARTBEAT_PERIOD_SECONDS)
        self.assertEqual(27, HEARTBEAT_EVALUATION_PERIODS)
        self.assertEqual(27, HEARTBEAT_DATAPOINTS_TO_ALARM)
        self.assertEqual("BizniswebReporting", DEFAULT_NAMESPACE)


if __name__ == "__main__":
    unittest.main()
