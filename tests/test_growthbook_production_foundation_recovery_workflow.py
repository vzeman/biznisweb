from __future__ import annotations

import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOW = (
    ROOT
    / ".github"
    / "workflows"
    / "recover-vevo-growthbook-production-foundation-evidence.yml"
)


class GrowthBookProductionFoundationRecoveryWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow = WORKFLOW.read_text(encoding="utf-8")

    def test_is_main_only_exact_run_bound_and_locally_gated_before_external_access(self) -> None:
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "RECOVER_32612205628",
            "CREATION_RUN_ID: '32612205628'",
            "CREATION_MAIN_COMMIT: 82d1f04c85f43d007f03090eefbeb0feb09fc140",
            "foundation evidence is already recorded",
            "Production allocation must remain zero",
            "Production experiment registry must remain empty",
            "Configure authenticated AWS recovery identity",
        ):
            self.assertIn(marker, self.workflow)
        local_gate = self.workflow.index("FOUNDATION_RECOVERY_LOCAL_GATE_OK:")
        github_read = self.workflow.index("gh api", local_gate)
        aws_credentials = self.workflow.index(
            "Configure authenticated AWS recovery identity", github_read
        )
        self.assertLess(local_gate, github_read)
        self.assertLess(github_read, aws_credentials)

    def test_binds_historical_steps_and_exact_live_resource_allowlist(self) -> None:
        for marker in (
            "actions: read",
            "GITHUB_RUN_ATTEMPT",
            "jobs?per_page=100",
            "artifacts?per_page=100",
            "status=success",
            "historical CREATE run unexpectedly has an artifact",
            "a successful foundation evidence recovery already exists",
            "FOUNDATION_RECOVERY_SINGLE_SUCCESS_GATE_OK:",
            "scripts/verify_growthbook_foundation_recovery.py",
            "cloudformation list-stack-resources",
            "CREATE_COMPLETE",
            "'PublicRouteEnabled': 'false'",
        ):
            self.assertIn(marker, self.workflow)
        self.assertNotIn("/logs", self.workflow)

    def test_direct_localhost_gate_precedes_ui_route_and_bucket_checks(self) -> None:
        host = self.workflow.index("aws ecs run-task")
        localhost = self.workflow.index("FOUNDATION_RECOVERY_HOST_GATE_OK:", host)
        route = self.workflow.index("apigatewayv2 get-routes", localhost)
        bucket = self.workflow.index("s3api list-objects-v2", route)
        external = self.workflow.index("ROUTE_STATUS=", bucket)
        for marker in (
            "scripts/resolve_growthbook_host_gate_runtime.py",
            "COLLECTOR_LOCALHOST_HEALTH_OK:production:",
            "COLLECTOR_LOCALHOST_MARKER_OK:/app:",
            "--expected-private-cidr 172.31.0.0/16",
            "s3api list-multipart-uploads",
            "bucket has incomplete multipart uploads",
            "Production GrowthBook reader absence could not be proven.",
            "FOUNDATION_RECOVERY_READER_ABSENCE_OK:",
            "FOUNDATION_RECOVERY_RUNTIME_OK:",
            "route=false:bucket=empty:credentials=none:allocation=0",
        ):
            self.assertIn(marker, self.workflow)
        self.assertLess(host, localhost)
        self.assertLess(localhost, route)
        self.assertLess(route, bucket)
        self.assertLess(bucket, external)

    def test_uploads_only_one_canonical_sanitized_schema_v2_artifact(self) -> None:
        for marker in (
            "build_foundation_recovery_evidence",
            "canonical_evidence_bytes",
            "GROWTHBOOK_FOUNDATION_EVIDENCE_READY:",
            "schema=2",
            "Upload sanitized Production foundation recovery evidence only",
            "uses: actions/upload-artifact@v4.6.2",
            "path: vevo-growthbook-production-foundation-evidence.json",
            "retention-days: 14",
        ):
            self.assertIn(marker, self.workflow)
        self.assertEqual(1, self.workflow.count("uses: actions/upload-artifact@v4.6.2"))
        for raw_path in (
            "path: creation-run.json",
            "path: creation-jobs.json",
            "path: prior-recovery-runs.json",
            "path: recovery-stack.json",
            "path: recovery-stack-resources.json",
            "path: recovery-service-task.json",
            "path: recovery-host-gate.log",
            "path: recovery-bucket-listing.json",
            "path: recovery-multipart-uploads.json",
        ):
            self.assertNotIn(raw_path, self.workflow)

    def test_allows_only_temporary_ecs_task_and_no_external_mutations(self) -> None:
        lowered = self.workflow.lower()
        self.assertEqual(1, lowered.count("aws ecs run-task"))
        for forbidden in (
            "cloudformation create-",
            "cloudformation update-",
            "cloudformation delete-",
            "aws s3api put-",
            "aws s3api delete-",
            "aws iam create-",
            "aws iam attach-",
            "aws iam put-",
            "aws iam delete-",
            "aws athena start-query-execution",
            "aws scheduler create-",
            "aws scheduler update-",
            "aws scheduler delete-",
            "docker push",
            "ads_update",
            "adcreatives_create",
            "submit",
        ):
            self.assertNotIn(forbidden, lowered)
        self.assertIn(
            "CloudFormation, GrowthBook, GTM, Meta Ads, BiznisWeb, prices, cart, checkout, orders: `unchanged`",
            self.workflow,
        )


if __name__ == "__main__":
    unittest.main()
