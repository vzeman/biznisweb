from __future__ import annotations

import unittest

from scripts.build_growthbook_reconciliation_parameters import (
    PARAMETER_KEYS,
    build_activation_parameters,
    build_candidate_parameters,
)


class GrowthBookReconciliationParameterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.environ = {
            "GROWTHBOOK_ENVIRONMENT": "preview",
            "CLUSTER_ARN": "arn:aws:ecs:eu-central-1:123456789012:cluster/vevo",
            "CANDIDATE_TASK_DEFINITION": "arn:aws:ecs:eu-central-1:123456789012:task-definition/vevo:2",
            "TASK_ROLE_ARN": "arn:aws:iam::123456789012:role/task",
            "EXECUTION_ROLE_ARN": "arn:aws:iam::123456789012:role/execution",
            "CONTAINER_NAME": "reporting",
            "EVENT_BUCKET": "vevo-preview-events",
            "LOG_GROUP": "/ecs/vevo-reporting-daily",
            "SUBNET_IDS": "subnet-001,subnet-002,subnet-003",
            "SECURITY_GROUP_IDS": "sg-001,sg-002",
            "ASSIGN_PUBLIC_IP": "ENABLED",
            "PLATFORM_VERSION": "LATEST",
        }

    def test_candidate_preserves_comma_delimited_cloudformation_values_as_strings(self) -> None:
        parameters = build_candidate_parameters(self.environ)
        by_key = {row["ParameterKey"]: row["ParameterValue"] for row in parameters}
        self.assertEqual(tuple(by_key), PARAMETER_KEYS)
        self.assertEqual(by_key["SubnetIds"], "subnet-001,subnet-002,subnet-003")
        self.assertEqual(by_key["SecurityGroupIds"], "sg-001,sg-002")
        self.assertIsInstance(by_key["SubnetIds"], str)
        self.assertEqual(by_key["ScheduleState"], "DISABLED")
        self.assertEqual(by_key["Environment"], "preview")
        self.assertEqual(by_key["ScheduleExpression"], "cron(30 3 * * ? *)")

    def test_production_candidate_uses_the_separate_schedule_contract(self) -> None:
        environ = dict(self.environ)
        environ["GROWTHBOOK_ENVIRONMENT"] = "production"
        parameters = build_candidate_parameters(environ)
        by_key = {row["ParameterKey"]: row["ParameterValue"] for row in parameters}
        self.assertEqual(by_key["Environment"], "production")
        self.assertEqual(by_key["ScheduleExpression"], "cron(45 3 * * ? *)")

    def test_candidate_rejects_missing_or_multiline_runtime_values(self) -> None:
        for key, value in (("SUBNET_IDS", ""), ("TASK_ROLE_ARN", "role\nother")):
            with self.subTest(key=key):
                environ = dict(self.environ)
                environ[key] = value
                with self.assertRaises(ValueError):
                    build_candidate_parameters(environ)

        environ = dict(self.environ)
        environ["GROWTHBOOK_ENVIRONMENT"] = "other"
        with self.assertRaisesRegex(ValueError, "unsupported"):
            build_candidate_parameters(environ)

    def test_activation_changes_only_schedule_state(self) -> None:
        parameters = build_activation_parameters()
        self.assertEqual(tuple(row["ParameterKey"] for row in parameters), PARAMETER_KEYS)
        changed = [row for row in parameters if "ParameterValue" in row]
        self.assertEqual(changed, [{"ParameterKey": "ScheduleState", "ParameterValue": "ENABLED"}])
        self.assertTrue(
            all(row.get("UsePreviousValue") is True for row in parameters if row not in changed)
        )


if __name__ == "__main__":
    unittest.main()
