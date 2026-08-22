from __future__ import annotations

import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOW = (
    ROOT
    / ".github"
    / "workflows"
    / "deploy-vevo-growthbook-production-foundation.yml"
)


class GrowthBookProductionFoundationWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow = WORKFLOW.read_text(encoding="utf-8")

    def test_is_main_only_explicit_and_natural_run_gated_before_aws(self) -> None:
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "confirm_deploy:",
            "natural retention recovery must be verified before foundation deploy",
            "protected natural reconciliation verifier has not passed",
            "sanitized natural reconciliation evidence is absent",
            "natural reconciliation artifact identity is incomplete",
            "natural reconciliation evidence SHA-256 mismatch",
            "natural reconciliation evidence identity/safety drift",
            "natural reconciliation evidence timestamp schema drift",
            "natural reconciliation sanitized count drift",
            "natural reconciliation runtime/control evidence drift",
            "Production deployment gate is false",
            "Production foundation deployment gate is false",
            "successful read-only Production preflight evidence drift",
            "workspace.get('reconciliation_checkpoint', {}).get(",
            "Configure authenticated AWS deployment identity",
        ):
            self.assertIn(marker, self.workflow)
        natural_gate = self.workflow.index(
            "natural retention recovery must be verified before foundation deploy"
        )
        evidence_gate = self.workflow.index(
            "natural reconciliation runtime/control evidence drift"
        )
        credentials = self.workflow.index("Configure authenticated AWS deployment identity")
        self.assertLess(natural_gate, credentials)
        self.assertLess(natural_gate, evidence_gate)
        self.assertLess(evidence_gate, credentials)
        self.assertNotIn(
            "workspace.get('workspace', {}).get('recurring_schedule'", self.workflow
        )

    def test_requires_hash_bound_sanitized_natural_evidence(self) -> None:
        for marker in (
            "verified_downloaded_sha256_recorded",
            "natural_evidence_artifact_sha256",
            "re.fullmatch(r'[0-9a-f]{64}', evidence_sha256)",
            "hashlib.sha256(canonical_evidence).hexdigest() != evidence_sha256",
            "natural_evidence.get('schema_version') != 2",
            "vevo_growthbook_natural_reconciliation_retention_recovery",
            "contains_raw_aws_payloads",
            "contains_credentials",
            "cloudtrail_scheduler_run_task_verified",
            "vevo-growthbook-reconcile-preview:4",
            "verification_window.get('target_run_due_utc') != '2026-08-23T01:30:00Z'",
            "verification_window.get('not_before_utc') != '2026-08-23T01:40:00Z'",
            "verification_window.get('before_utc') != '2026-08-23T02:20:00Z'",
            "reconciliation.get('event_from') != '2026-07-14'",
            "reconciliation.get('event_through') != '2026-08-22'",
            "sha256:cabba3b0bd57f6be322f3a5ff62f0327c7cf8e7bb2b6b5e78686305339fdd041",
        ):
            self.assertIn(marker, self.workflow)

    def test_requires_preview_and_planned_production_hard_gate_before_create(self) -> None:
        preview = self.workflow.index("PREVIEW_RUNTIME_IDENTITY_OK:")
        planned = self.workflow.index("PLANNED_PRODUCTION_IDENTITY:", preview)
        production_mode_host = self.workflow.index(
            "PREDEPLOY_PRODUCTION_MODE_HARD_GATE_OK:", planned
        )
        create = self.workflow.index("cloudformation create-change-set", production_mode_host)
        for marker in (
            "PREVIEW_STACK_NAME: vevo-growthbook-preview",
            "PREVIEW_SERVICE_NAME: vevo-growthbook-collector-preview",
            "PRODUCTION_STACK_NAME: vevo-growthbook-production",
            "PRODUCTION_SERVICE_NAME: vevo-growthbook-collector-production",
            "path=${RUNTIME_PATH}",
            "Production stack absence was not proven",
            "Preview target is not healthy",
            "COLLECTOR_LOCALHOST_HEALTH_OK:production:",
            "COLLECTOR_LOCALHOST_MARKER_OK:/app:",
        ):
            self.assertIn(marker, self.workflow)
        self.assertLess(preview, planned)
        self.assertLess(planned, production_mode_host)
        self.assertLess(production_mode_host, create)

    def test_is_create_only_route_disabled_and_reuses_verified_image(self) -> None:
        for marker in (
            "EXPECTED_IMAGE_DIGEST: sha256:9478acd98a8caf06374b018c563ee51fa896b9cc92148238579f04aa28a134e1",
            "'Environment': 'production'",
            "'PublicRouteEnabled': 'false'",
            "--change-set-type CREATE",
            "--phase production-foundation",
            "Production public collector endpoint must not exist",
            "PRODUCTION_FOUNDATION_ROUTE_DISABLED_OK:",
        ):
            self.assertIn(marker, self.workflow)
        lowered = self.workflow.lower()
        for forbidden in (
            "--change-set-type update",
            "publicrouteenabled': 'true'",
            "docker push",
            "ecr create-repository",
            "cloudformation update-stack",
            "cloudformation delete-stack",
        ):
            self.assertNotIn(forbidden, lowered)

    def test_localhost_marker_precedes_external_route_and_data_checks(self) -> None:
        host = self.workflow.index("PRODUCTION_FOUNDATION_HARD_GATE_OK:")
        route = self.workflow.index("apigatewayv2 get-routes", host)
        curl = self.workflow.index("ROUTE_STATUS=", route)
        for marker in (
            "COLLECTOR_LOCALHOST_HEALTH_OK:production:",
            "COLLECTOR_LOCALHOST_MARKER_OK:/app:",
            "PRODUCTION_SERVICE_IDENTITY_OK:",
            "route=false:bucket=empty:credentials=none:allocation=0",
        ):
            self.assertIn(marker, self.workflow)
        self.assertLess(host, route)
        self.assertLess(route, curl)

    def test_uploads_only_one_sanitized_hashable_foundation_artifact(self) -> None:
        for marker in (
            "FOUNDATION_EVIDENCE_FILE: vevo-growthbook-production-foundation-evidence.json",
            "build_foundation_evidence",
            "canonical_evidence_bytes",
            "GROWTHBOOK_FOUNDATION_EVIDENCE_READY:",
            "Upload sanitized Production foundation evidence only",
            "uses: actions/upload-artifact@v4.6.2",
            "path: vevo-growthbook-production-foundation-evidence.json",
            "retention-days: 14",
        ):
            self.assertIn(marker, self.workflow)
        self.assertEqual(1, self.workflow.count("uses: actions/upload-artifact@v4.6.2"))
        for raw_path in (
            "path: deployed-production-stack.json",
            "path: production-service.json",
            "path: production-service-task.json",
            "path: production-host-gate-task.json",
            "path: production-host-gate.log",
            "path: production-task-definition.json",
        ):
            self.assertNotIn(raw_path, self.workflow)

    def test_external_and_existing_runtime_mutations_are_absent(self) -> None:
        lowered = self.workflow.lower()
        for forbidden in (
            "aws scheduler update-",
            "aws scheduler create-",
            "aws scheduler delete-",
            "aws s3api put-",
            "aws s3api delete-",
            "aws athena start-query-execution",
            "aws iam attach-",
            "aws iam put-",
            "aws iam delete-",
            "ads_update",
            "adcreatives_create",
            "submit",
        ):
            self.assertNotIn(forbidden, lowered)
        self.assertIn("Meta Ads and BiznisWeb commerce state: `unchanged`", self.workflow)
        self.assertIn("GrowthBook reader credentials: `not created`", self.workflow)


if __name__ == "__main__":
    unittest.main()
