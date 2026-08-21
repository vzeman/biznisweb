from __future__ import annotations

import unittest

from scripts.validate_growthbook_reconciliation_changeset import (
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


def payload(*changes: dict[str, object], change_set_type: str) -> dict[str, object]:
    return {
        "Status": "CREATE_COMPLETE",
        "ChangeSetType": change_set_type,
        "Changes": list(changes),
    }


class GrowthBookReconciliationChangeSetTests(unittest.TestCase):
    def test_candidate_allows_only_exact_disabled_first_create(self) -> None:
        result = validate(
            payload(
                *(change(key, value) for key, value in EXPECTED_CREATE_RESOURCES.items()),
                change_set_type="CREATE",
            ),
            "candidate",
        )
        self.assertEqual(len(EXPECTED_CREATE_RESOURCES), len(result))

    def test_candidate_update_is_limited_to_schedule_and_role(self) -> None:
        result = validate(
            payload(
                change(
                    "ReconciliationSchedule",
                    "AWS::Scheduler::Schedule",
                    action="Modify",
                ),
                change(
                    "ReconciliationSchedulerRole",
                    "AWS::IAM::Role",
                    action="Modify",
                ),
                change_set_type="UPDATE",
            ),
            "candidate",
        )
        self.assertEqual(2, len(result))

    def test_candidate_rejects_delete_replacement_and_unrelated_resource(self) -> None:
        with self.assertRaisesRegex(AssertionError, "destructive"):
            validate(
                payload(
                    change("ReconciliationSchedule", "AWS::Scheduler::Schedule", action="Remove"),
                    change_set_type="UPDATE",
                ),
                "candidate",
            )
        with self.assertRaisesRegex(AssertionError, "must not replace"):
            validate(
                payload(
                    change(
                        "ReconciliationSchedule",
                        "AWS::Scheduler::Schedule",
                        action="Modify",
                        replacement="True",
                    ),
                    change_set_type="UPDATE",
                ),
                "candidate",
            )
        with self.assertRaisesRegex(AssertionError, "unrelated"):
            validate(
                payload(
                    change("OtherBucket", "AWS::S3::Bucket", action="Modify"),
                    change_set_type="UPDATE",
                ),
                "candidate",
            )

    def test_activation_allows_only_nonreplacement_schedule_enable(self) -> None:
        result = validate(
            payload(
                change(
                    "ReconciliationSchedule",
                    "AWS::Scheduler::Schedule",
                    action="Modify",
                ),
                change_set_type="UPDATE",
            ),
            "activate",
        )
        self.assertEqual("ReconciliationSchedule", result[0]["logical_id"])

    def test_activation_rejects_role_or_replace(self) -> None:
        with self.assertRaisesRegex(AssertionError, "unexpected"):
            validate(
                payload(
                    change(
                        "ReconciliationSchedulerRole",
                        "AWS::IAM::Role",
                        action="Modify",
                    ),
                    change_set_type="UPDATE",
                ),
                "activate",
            )

    def test_expected_change_set_type_is_injected_and_mismatch_rejected(self) -> None:
        raw = {
            "Status": "CREATE_COMPLETE",
            "Changes": [change(key, value) for key, value in EXPECTED_CREATE_RESOURCES.items()],
        }
        normalized = with_change_set_type(raw, "CREATE")
        self.assertEqual("CREATE", normalized["ChangeSetType"])
        with self.assertRaisesRegex(AssertionError, "type mismatch"):
            with_change_set_type({"ChangeSetType": "UPDATE"}, "CREATE")


if __name__ == "__main__":
    unittest.main()
