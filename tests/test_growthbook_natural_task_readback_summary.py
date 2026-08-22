from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from scripts import summarize_growthbook_natural_task_readback as summary


TASK_ID = "a" * 32
CLUSTER_ARN = (
    "arn:aws:ecs:eu-central-1:919341186960:cluster/vevo-reporting-cluster"
)
TASK_ARN = (
    "arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/"
    + TASK_ID
)


class GrowthBookNaturalTaskReadbackSummaryTests(unittest.TestCase):
    def summarize(self, payload: dict) -> str:
        return summary.summarize_task_readback(
            payload,
            expected_task_id=TASK_ID,
            expected_cluster_arn=CLUSTER_ARN,
        )

    def test_accepts_exact_available_task_without_claiming_evidence(self) -> None:
        marker = self.summarize(
            {"tasks": [{"taskArn": TASK_ARN}], "failures": []}
        )

        self.assertEqual(
            f"NATURAL_ECS_TASK_READBACK_AVAILABLE:task={TASK_ID}:raw=false:evidence=false",
            marker,
        )

    def test_accepts_exact_documented_missing_state_and_hides_detail(self) -> None:
        marker = self.summarize(
            {
                "tasks": [],
                "failures": [
                    {
                        "arn": TASK_ARN,
                        "reason": "MISSING",
                        "detail": "secret-shaped diagnostic must not be printed",
                    }
                ],
            }
        )

        self.assertEqual(
            f"NATURAL_ECS_TASK_READBACK_EXPIRED:reason=MISSING:task={TASK_ID}:raw=false:evidence=false",
            marker,
        )
        self.assertNotIn("secret-shaped", marker)

    def test_rejects_wrong_reason_task_or_root_shape(self) -> None:
        cases = [
            {
                "tasks": [],
                "failures": [{"arn": TASK_ARN, "reason": "ACCESS_DENIED"}],
            },
            {
                "tasks": [],
                "failures": [{"arn": TASK_ARN[:-1] + "b", "reason": "MISSING"}],
            },
            {"tasks": [], "failures": [], "raw_response": {}},
        ]

        for payload in cases:
            with self.subTest(payload=payload):
                with self.assertRaises(summary.TaskReadbackSummaryError):
                    self.summarize(payload)

    def test_cli_emits_only_sanitized_marker(self) -> None:
        payload = {
            "tasks": [],
            "failures": [
                {"arn": TASK_ARN, "reason": "MISSING", "detail": "must-not-appear"}
            ],
        }
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "task-state.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                exit_code = summary.main(
                    [
                        "--task-state",
                        str(path),
                        "--expected-task-id",
                        TASK_ID,
                        "--expected-cluster-arn",
                        CLUSTER_ARN,
                    ]
                )

        self.assertEqual(0, exit_code)
        self.assertNotIn("must-not-appear", output.getvalue())
        self.assertIn("raw=false:evidence=false", output.getvalue())


if __name__ == "__main__":
    unittest.main()
