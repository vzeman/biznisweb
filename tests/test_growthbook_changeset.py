from __future__ import annotations

import copy
import unittest

from scripts.validate_growthbook_changeset import (
    EXPECTED_CREATE_RESOURCES,
    validate,
    with_change_set_type,
)


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

    def test_production_foundation_allows_only_exact_route_disabled_create(self) -> None:
        changes = [
            change(logical_id, resource_type)
            for logical_id, resource_type in EXPECTED_CREATE_RESOURCES.items()
        ]
        result = validate(
            payload(*changes, change_set_type="CREATE"),
            "production-foundation",
        )
        self.assertEqual(len(EXPECTED_CREATE_RESOURCES), len(result))

        with self.assertRaisesRegex(AssertionError, "must be a CREATE"):
            validate(
                payload(
                    change(
                        "CollectorService",
                        "AWS::ECS::Service",
                        action="Modify",
                    ),
                    change_set_type="UPDATE",
                ),
                "production-foundation",
            )

        with self.assertRaisesRegex(AssertionError, "resource contract mismatch"):
            validate(
                payload(
                    *changes,
                    change("CollectorPostRoute", "AWS::ApiGatewayV2::Route"),
                    change_set_type="CREATE",
                ),
                "production-foundation",
            )

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

    def test_deactivation_allows_only_the_public_route_removal(self) -> None:
        result = validate(
            payload(
                change(
                    "CollectorPostRoute",
                    "AWS::ApiGatewayV2::Route",
                    action="Remove",
                )
            ),
            "deactivate",
        )
        self.assertEqual(
            {
                "action": "Remove",
                "logical_id": "CollectorPostRoute",
                "replacement": "False",
                "resource_type": "AWS::ApiGatewayV2::Route",
            },
            result[0],
        )

    def test_deactivation_rejects_any_unrelated_or_replacement_change(self) -> None:
        with self.assertRaisesRegex(AssertionError, "unrelated changes"):
            validate(
                payload(
                    change(
                        "CollectorPostRoute",
                        "AWS::ApiGatewayV2::Route",
                        action="Remove",
                    ),
                    change(
                        "CollectorService",
                        "AWS::ECS::Service",
                        action="Modify",
                    ),
                ),
                "deactivate",
            )
        with self.assertRaisesRegex(AssertionError, "unexpected route deactivation"):
            validate(
                payload(
                    change(
                        "CollectorPostRoute",
                        "AWS::ApiGatewayV2::Route",
                        action="Remove",
                        replacement="True",
                    )
                ),
                "deactivate",
            )

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

    def test_workflow_supplies_type_omitted_by_describe_change_set(self) -> None:
        raw = {
            "Status": "CREATE_COMPLETE",
            "Changes": [
                change(logical_id, resource_type)
                for logical_id, resource_type in EXPECTED_CREATE_RESOURCES.items()
            ],
        }
        normalized = with_change_set_type(raw, "CREATE")
        self.assertEqual("CREATE", normalized["ChangeSetType"])
        self.assertNotIn("ChangeSetType", raw)
        self.assertEqual(len(EXPECTED_CREATE_RESOURCES), len(validate(normalized, "candidate")))

    def test_workflow_rejects_api_type_mismatch(self) -> None:
        with self.assertRaisesRegex(AssertionError, "type mismatch"):
            with_change_set_type({"ChangeSetType": "UPDATE"}, "CREATE")


if __name__ == "__main__":
    unittest.main()
