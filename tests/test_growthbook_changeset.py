from __future__ import annotations

import copy
import unittest

from scripts.validate_growthbook_changeset import EXPECTED_CREATE_RESOURCES, validate


def change(
    logical_id: str,
    resource_type: str,
    *,
    action: str = "Add",
    replacement: str = "False",
) -> dict[str, object]:
    return {
        "ResourceChange": {
            "Action": action,
            "LogicalResourceId": logical_id,
            "Replacement": replacement,
            "ResourceType": resource_type,
        }
    }


def payload(*changes: dict[str, object], change_set_type: str = "UPDATE") -> dict[str, object]:
    return {
        "Status": "CREATE_COMPLETE",
        "ChangeSetType": change_set_type,
        "Changes": list(changes),
    }


class GrowthBookChangeSetTests(unittest.TestCase):
    def test_candidate_allows_exact_route_disabled_create(self) -> None:
        changes = [
            change(logical_id, resource_type)
            for logical_id, resource_type in EXPECTED_CREATE_RESOURCES.items()
        ]
        result = validate(payload(*changes, change_set_type="CREATE"), "candidate")
        self.assertEqual(len(EXPECTED_CREATE_RESOURCES), len(result))

    def test_candidate_allows_only_runtime_update(self) -> None:
        result = validate(
            payload(
                change(
                    "CollectorService",
                    "AWS::ECS::Service",
                    action="Modify",
                ),
                change(
                    "CollectorTaskDefinition",
                    "AWS::ECS::TaskDefinition",
                    action="Modify",
                    replacement="True",
                ),
            ),
            "candidate",
        )
        self.assertEqual(2, len(result))

    def test_candidate_rejects_remove_and_unrelated_update(self) -> None:
        with self.assertRaisesRegex(AssertionError, "destructive change"):
            validate(
                payload(change("ExperimentDataBucket", "AWS::S3::Bucket", action="Remove")),
                "candidate",
            )
        with self.assertRaisesRegex(AssertionError, "unrelated changes"):
            validate(
                payload(
                    change(
                        "ExperimentDataBucket",
                        "AWS::S3::Bucket",
                        action="Modify",
                        replacement="True",
                    )
                ),
                "candidate",
            )

    def test_activation_allows_only_one_route_add(self) -> None:
        result = validate(
            payload(change("CollectorPostRoute", "AWS::ApiGatewayV2::Route")),
            "activate",
        )
        self.assertEqual("CollectorPostRoute", result[0]["logical_id"])

    def test_activation_rejects_any_unrelated_change(self) -> None:
        unsafe = payload(change("CollectorPostRoute", "AWS::ApiGatewayV2::Route"))
        unsafe = copy.deepcopy(unsafe)
        unsafe["Changes"].append(change("CollectorService", "AWS::ECS::Service"))
        with self.assertRaisesRegex(AssertionError, "unrelated changes"):
            validate(unsafe, "activate")

    def test_incomplete_or_empty_change_set_is_rejected(self) -> None:
        with self.assertRaisesRegex(AssertionError, "not ready"):
            validate(
                {"Status": "CREATE_PENDING", "ChangeSetType": "CREATE", "Changes": []},
                "candidate",
            )
        with self.assertRaisesRegex(AssertionError, "no resource changes"):
            validate(
                {"Status": "CREATE_COMPLETE", "ChangeSetType": "CREATE", "Changes": []},
                "candidate",
            )

    def test_missing_change_set_type_is_rejected(self) -> None:
        with self.assertRaisesRegex(AssertionError, "type is missing"):
            validate(
                {
                    "Status": "CREATE_COMPLETE",
                    "Changes": [change("CollectorService", "AWS::ECS::Service")],
                },
                "candidate",
            )


if __name__ == "__main__":
    unittest.main()
