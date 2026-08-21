from __future__ import annotations

import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "recover-vevo-growthbook-reconciliation-rollback.yml"


class GrowthBookReconciliationRecoveryWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow = WORKFLOW.read_text(encoding="utf-8")

    def test_cleanup_is_main_only_exact_and_fail_closed(self) -> None:
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "confirm_cleanup:",
            "STACK_NAME: vevo-growthbook-reconciliation-preview",
            "QUEUE_NAME: vevo-growthbook-reconcile-preview-dlq",
            'stack.get("StackStatus") != "ROLLBACK_COMPLETE"',
            'attributes.get(key) != "0"',
            'tags.get("Project") != "vevo-growthbook"',
            "ROLLBACK_CLEANUP_HARD_GATE_OK:",
        ):
            self.assertIn(marker, self.workflow)

    def test_exact_readback_precedes_bounded_cleanup(self) -> None:
        gate = self.workflow.index("ROLLBACK_CLEANUP_HARD_GATE_OK:")
        stack_delete = self.workflow.index("aws cloudformation delete-stack", gate)
        stack_wait = self.workflow.index("aws cloudformation wait stack-delete-complete", stack_delete)
        queue_delete = self.workflow.index("aws sqs delete-queue", stack_wait)
        source_readback = self.workflow.index("cmp -s source-schedule-before.json", queue_delete)
        self.assertLess(gate, stack_delete)
        self.assertLess(stack_delete, stack_wait)
        self.assertLess(stack_wait, queue_delete)
        self.assertLess(queue_delete, source_readback)

    def test_cleanup_cannot_touch_runtime_or_unrelated_data(self) -> None:
        lowered = self.workflow.lower()
        for forbidden in (
            "ecs run-task",
            "scheduler delete-schedule",
            "s3api delete-object",
            "iam delete-role",
        ):
            self.assertNotIn(forbidden, lowered)


if __name__ == "__main__":
    unittest.main()
