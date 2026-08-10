import json
import unittest

from scripts.reporting_scheduler_passrole import (
    build_passrole_documents,
    validate_passrole_simulation,
    validate_role_arn,
)


ACCOUNT_ID = "919341186960"
SCHEDULER_ROLE_ARN = (
    f"arn:aws:iam::{ACCOUNT_ID}:role/vevo-reporting-scheduler-role"
)
TASK_ROLE_ARN = (
    f"arn:aws:iam::{ACCOUNT_ID}:role/BiznisWebReportingTaskRole-vevo"
)
EXECUTION_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/ecsTaskExecutionRole"
SCHEDULER_ROLE_DOCUMENT = {
    "Role": {
        "Arn": SCHEDULER_ROLE_ARN,
        "AssumeRolePolicyDocument": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "scheduler.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        },
    }
}


class ReportingSchedulerPassRoleTests(unittest.TestCase):
    def test_accepts_one_action_with_two_allowed_resource_results(self) -> None:
        simulation = {
            "EvaluationResults": [
                {
                    "EvalActionName": "iam:PassRole",
                    "EvalResourceName": "arn:aws:iam::${Account}:role/${RoleName}",
                    "EvalDecision": "allowed",
                    "ResourceSpecificResults": [
                        {
                            "EvalResourceName": TASK_ROLE_ARN,
                            "EvalResourceDecision": "allowed",
                        },
                        {
                            "EvalResourceName": EXECUTION_ROLE_ARN,
                            "EvalResourceDecision": "allowed",
                        },
                    ],
                }
            ]
        }

        validate_passrole_simulation(
            simulation, [TASK_ROLE_ARN, EXECUTION_ROLE_ARN]
        )

    def test_rejects_denied_or_missing_passrole_resource(self) -> None:
        denied = {
            "EvaluationResults": [
                {
                    "EvalActionName": "iam:PassRole",
                    "EvalDecision": "allowed",
                    "ResourceSpecificResults": [
                        {
                            "EvalResourceName": TASK_ROLE_ARN,
                            "EvalResourceDecision": "allowed",
                        },
                        {
                            "EvalResourceName": EXECUTION_ROLE_ARN,
                            "EvalResourceDecision": "implicitDeny",
                        },
                    ],
                }
            ]
        }
        with self.assertRaisesRegex(ValueError, "not allowed for expected roles"):
            validate_passrole_simulation(
                denied, [TASK_ROLE_ARN, EXECUTION_ROLE_ARN]
            )

        missing = json.loads(json.dumps(denied))
        missing["EvaluationResults"][0]["ResourceSpecificResults"].pop()
        with self.assertRaisesRegex(ValueError, "do not match"):
            validate_passrole_simulation(
                missing, [TASK_ROLE_ARN, EXECUTION_ROLE_ARN]
            )

    def test_builds_exact_account_local_ecs_passrole_policy(self) -> None:
        schedule = {"Target": {"RoleArn": SCHEDULER_ROLE_ARN}}
        task_definition = {
            "taskDefinition": {
                "taskRoleArn": TASK_ROLE_ARN,
                "executionRoleArn": EXECUTION_ROLE_ARN,
            }
        }

        policy, metadata = build_passrole_documents(
            schedule,
            task_definition,
            SCHEDULER_ROLE_DOCUMENT,
            TASK_ROLE_ARN,
            ACCOUNT_ID,
            "vevo-reporting-scheduler-role",
        )

        statement = policy["Statement"][0]
        self.assertEqual(statement["Action"], "iam:PassRole")
        self.assertEqual(
            statement["Resource"], [TASK_ROLE_ARN, EXECUTION_ROLE_ARN]
        )
        self.assertEqual(
            statement["Condition"]["StringEquals"]["iam:PassedToService"],
            "ecs-tasks.amazonaws.com",
        )
        self.assertEqual(
            metadata["scheduler_role_name"], "vevo-reporting-scheduler-role"
        )

    def test_rejects_task_definition_role_mismatch(self) -> None:
        schedule = {"Target": {"RoleArn": SCHEDULER_ROLE_ARN}}
        task_definition = {
            "taskDefinition": {
                "taskRoleArn": f"arn:aws:iam::{ACCOUNT_ID}:role/unexpected",
                "executionRoleArn": EXECUTION_ROLE_ARN,
            }
        }

        with self.assertRaisesRegex(ValueError, "does not use the expected"):
            build_passrole_documents(
                schedule,
                task_definition,
                SCHEDULER_ROLE_DOCUMENT,
                TASK_ROLE_ARN,
                ACCOUNT_ID,
                "vevo-reporting-scheduler-role",
            )

    def test_rejects_unexpected_same_account_scheduler_role(self) -> None:
        unexpected_arn = f"arn:aws:iam::{ACCOUNT_ID}:role/unexpected-scheduler"
        schedule = {"Target": {"RoleArn": unexpected_arn}}
        scheduler_role = {
            "Role": {
                "Arn": unexpected_arn,
                "AssumeRolePolicyDocument": SCHEDULER_ROLE_DOCUMENT["Role"][
                    "AssumeRolePolicyDocument"
                ],
            }
        }
        task_definition = {
            "taskDefinition": {
                "taskRoleArn": TASK_ROLE_ARN,
                "executionRoleArn": EXECUTION_ROLE_ARN,
            }
        }

        with self.assertRaisesRegex(ValueError, "unexpected scheduler role"):
            build_passrole_documents(
                schedule,
                task_definition,
                scheduler_role,
                TASK_ROLE_ARN,
                ACCOUNT_ID,
                "vevo-reporting-scheduler-role",
            )

    def test_rejects_broadened_scheduler_trust(self) -> None:
        schedule = {"Target": {"RoleArn": SCHEDULER_ROLE_ARN}}
        task_definition = {
            "taskDefinition": {
                "taskRoleArn": TASK_ROLE_ARN,
                "executionRoleArn": EXECUTION_ROLE_ARN,
            }
        }
        scheduler_role = json.loads(json.dumps(SCHEDULER_ROLE_DOCUMENT))
        scheduler_role["Role"]["AssumeRolePolicyDocument"]["Statement"].append(
            {
                "Effect": "Allow",
                "Principal": {"AWS": f"arn:aws:iam::{ACCOUNT_ID}:root"},
                "Action": "sts:AssumeRole",
            }
        )

        with self.assertRaisesRegex(ValueError, "one exact trust statement"):
            build_passrole_documents(
                schedule,
                task_definition,
                scheduler_role,
                TASK_ROLE_ARN,
                ACCOUNT_ID,
                "vevo-reporting-scheduler-role",
            )

    def test_rejects_cross_account_and_wildcard_role_arns(self) -> None:
        with self.assertRaisesRegex(ValueError, "expected account"):
            validate_role_arn(
                "arn:aws:iam::111111111111:role/cross-account", ACCOUNT_ID
            )
        with self.assertRaisesRegex(ValueError, "Invalid IAM role ARN"):
            validate_role_arn(
                f"arn:aws:iam::{ACCOUNT_ID}:role/BiznisWebReportingTaskRole-*",
                ACCOUNT_ID,
            )


if __name__ == "__main__":
    unittest.main()
