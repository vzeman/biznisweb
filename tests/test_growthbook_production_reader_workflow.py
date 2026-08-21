from __future__ import annotations

import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOW = (
    ROOT
    / ".github"
    / "workflows"
    / "provision-vevo-growthbook-production-reader.yml"
)


class GrowthBookProductionReaderWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow = WORKFLOW.read_text(encoding="utf-8")

    def test_is_main_only_and_all_local_release_gates_precede_aws_credentials(self) -> None:
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "confirm_identity:",
            "first natural reconciliation must be verified before reader provisioning",
            "Production foundation deployment is not recorded as verified",
            "Production reader provisioning gate is false",
            "successful Production foundation deployment evidence drift",
            "verified_downloaded_sha256_recorded",
            "foundation_evidence_artifact_sha256",
            "validate_foundation_evidence",
            "hashlib.sha256(canonical_evidence_bytes(deployment)).hexdigest()",
            "Production foundation redeployment gate must be closed",
            "workspace.get('reconciliation_checkpoint', {}).get(",
            "GrowthBook clone must remain disabled during reader provisioning",
            "Configure authenticated AWS reader-provisioning identity",
        ):
            self.assertIn(marker, self.workflow)
        local_gate = self.workflow.index("PRODUCTION_READER_LOCAL_RELEASE_GATE_OK:")
        credentials = self.workflow.index(
            "Configure authenticated AWS reader-provisioning identity"
        )
        self.assertLess(local_gate, credentials)
        self.assertNotIn(
            "workspace.get('workspace', {}).get('recurring_schedule'", self.workflow
        )

    def test_confirms_exact_route_disabled_runtime_before_iam_creation(self) -> None:
        service_gate = self.workflow.index("PRODUCTION_READER_SERVICE_IDENTITY_OK:")
        host_gate = self.workflow.index(
            "PRODUCTION_READER_PREPROVISION_HARD_GATE_OK:", service_gate
        )
        create_user = self.workflow.index("aws iam create-user", host_gate)
        for marker in (
            "STACK_NAME: vevo-growthbook-production",
            "SERVICE_NAME: vevo-growthbook-collector-production",
            "IAM_USER_NAME: vevo-growthbook-production-reader",
            "IAM_USER_PATH: /vevo/growthbook/production/",
            "parameters.get('PublicRouteEnabled') != 'false'",
            "Production public collector endpoint must not exist",
            "Production API unexpectedly has a route",
            "Production experiment bucket is not empty",
            "COLLECTOR_LOCALHOST_HEALTH_OK:production:",
            "COLLECTOR_LOCALHOST_MARKER_OK:${RUNTIME_PATH}:",
        ):
            self.assertIn(marker, self.workflow)
        self.assertLess(service_gate, host_gate)
        self.assertLess(host_gate, create_user)

    def test_provisions_only_one_separate_least_privilege_reader(self) -> None:
        for marker in (
            "if not set(resources) <= allowed_resources:",
            "if observed_actions != allowed_actions:",
            "if observed_resources != allowed_resources:",
            "aws iam create-user",
            "aws iam attach-user-policy",
            "aws iam create-access-key",
            "aws iam list-access-keys",
            "aws iam list-user-tags",
            "GrowthBook Production user must have exactly one active access key",
        ):
            self.assertIn(marker, self.workflow)
        self.assertNotIn("vevo-growthbook-preview-reader", self.workflow)
        self.assertNotIn("/vevo/growthbook/preview/", self.workflow)
        self.assertNotIn("s3:DeleteObject", self.workflow)

    def test_handoff_is_encrypted_short_lived_and_plaintext_is_not_exposed(self) -> None:
        for marker in (
            "openssl cms -encrypt -binary -aes-256-cbc",
            "uses: actions/upload-artifact@v4.6.2",
            "retention-days: 1",
            "contains_plaintext_credentials': False",
            "growthbook_control_plane_mutated': False",
            "GROWTHBOOK_PRODUCTION_READER_ACTIVE",
            "GROWTHBOOK_PRODUCTION_READER_FAILED_RUN_REVOKED",
        ):
            self.assertIn(marker, self.workflow)
        for forbidden in (
            'cat ${CREDENTIAL_JSON}',
            'cat "${CREDENTIAL_JSON}"',
            'GITHUB_ENV} < ${CREDENTIAL_JSON}',
            'GITHUB_OUTPUT} < ${CREDENTIAL_JSON}',
        ):
            self.assertNotIn(forbidden, self.workflow)

    def test_does_not_mutate_growthbook_gtm_meta_biznisweb_or_foundation(self) -> None:
        lowered = self.workflow.lower()
        for forbidden in (
            "cloudformation create-",
            "cloudformation update-",
            "cloudformation delete-",
            "apigatewayv2 create-",
            "apigatewayv2 update-",
            "apigatewayv2 delete-",
            "aws s3api put-",
            "aws s3api delete-",
            "aws scheduler create-",
            "aws scheduler update-",
            "aws scheduler delete-",
            "tagmanager",
            "ads_update",
            "adcreatives_create",
        ):
            self.assertNotIn(forbidden, lowered)
        self.assertIn("GrowthBook control plane: `unchanged`", self.workflow)
        self.assertIn("Meta Ads and BiznisWeb commerce state: `unchanged`", self.workflow)


if __name__ == "__main__":
    unittest.main()
