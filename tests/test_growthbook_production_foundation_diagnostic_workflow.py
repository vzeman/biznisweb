from __future__ import annotations

import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOW = (
    ROOT
    / ".github"
    / "workflows"
    / "diagnose-vevo-growthbook-production-foundation.yml"
)


class GrowthBookProductionFoundationDiagnosticWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow = WORKFLOW.read_text(encoding="utf-8")

    def test_is_main_only_confirmed_and_locally_gated_before_aws(self) -> None:
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "confirm_diagnostic:",
            '[[ "${CONFIRM_DIAGNOSTIC}" == "true" ]]',
            "foundation evidence is already recorded",
            "GrowthBook Production allocation must remain zero",
            "Production experiment registry must remain empty",
            "Configure AWS credentials for read-only diagnostic",
        ):
            self.assertIn(marker, self.workflow)
        local_gate = self.workflow.index("FOUNDATION_DIAGNOSTIC_LOCAL_GATE_OK:")
        credentials = self.workflow.index(
            "Configure AWS credentials for read-only diagnostic"
        )
        self.assertLess(local_gate, credentials)

    def test_reads_only_exact_runtime_route_and_bucket_boundaries(self) -> None:
        for marker in (
            "cloudformation describe-stacks",
            "ecs describe-services",
            "ecs list-tasks",
            "ecs describe-tasks",
            "elbv2 describe-target-health",
            "apigatewayv2 get-routes",
            "s3api get-public-access-block",
            "s3api get-bucket-policy-status",
            "s3api get-bucket-encryption",
            "s3api list-objects-v2",
            "scripts/summarize_growthbook_foundation_bucket.py",
            "FOUNDATION_DIAGNOSTIC_RUNTIME_OK:",
            "target=healthy:route=false:bucket-public=false:mutation=none",
        ):
            self.assertIn(marker, self.workflow)

    def test_contains_no_aws_or_external_mutation(self) -> None:
        lowered = self.workflow.lower()
        for forbidden in (
            "cloudformation create-",
            "cloudformation update-",
            "cloudformation execute-",
            "cloudformation delete-",
            "ecs run-task",
            "ecs update-service",
            "s3api put-",
            "s3api delete-",
            "iam create-",
            "iam delete-",
            "athena start-query-execution",
            "scheduler update-",
            "ads_update",
            "submit",
            "upload-artifact",
        ):
            self.assertNotIn(forbidden, lowered)
        self.assertIn("AWS mutations: `none`", self.workflow)
        self.assertIn("no keys or content", self.workflow)


if __name__ == "__main__":
    unittest.main()
