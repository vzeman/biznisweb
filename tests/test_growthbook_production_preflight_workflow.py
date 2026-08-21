from __future__ import annotations

import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "preflight-vevo-growthbook-production-foundation.yml"


class GrowthBookProductionPreflightWorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workflow = WORKFLOW.read_text(encoding="utf-8")

    def test_preflight_is_main_only_read_only_and_fail_closed(self) -> None:
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "confirm_preflight",
            "PRODUCTION_STACK_NAME: vevo-growthbook-production",
            "PRODUCTION_SERVICE_NAME: vevo-growthbook-collector-production",
            "GROWTHBOOK_ENVIRONMENT=production",
            "Production experiment registry must remain empty",
            "GrowthBook Production allocation must remain zero",
            "GTM Preview workspace must remain unpublished",
            "get('publish_status') != 'not_published'",
            "Production activation gate must remain false",
            "get('decision_gates', {}).get('production_activation_allowed') is not False",
            "parameters.get('PublicRouteEnabled') != 'false'",
            "'CollectorEndpoint' in outputs",
            "PRODUCTION_FOUNDATION_PREFLIGHT_OK:",
            "PLANNED_PRODUCTION_IDENTITY:instance-id=N/A:Fargate",
            "AWS mutations: `none`",
        ):
            self.assertIn(marker, self.workflow)

    def test_preflight_contains_no_aws_mutation_command(self) -> None:
        lowered = self.workflow.lower()
        for forbidden in (
            "aws cloudformation create-",
            "aws cloudformation update-",
            "aws cloudformation delete-",
            "aws cloudformation execute-",
            "aws ecs run-task",
            "aws ecs register-task-definition",
            "aws iam create-",
            "aws iam attach-",
            "aws iam put-",
            "aws iam delete-",
            "aws glue create-",
            "aws athena start-query-execution",
            "aws s3api put-",
            "aws s3api delete-",
        ):
            self.assertNotIn(forbidden, lowered)


if __name__ == "__main__":
    unittest.main()
