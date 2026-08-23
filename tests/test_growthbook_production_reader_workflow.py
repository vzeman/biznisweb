from __future__ import annotations

import pathlib
import textwrap
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
            "natural retention recovery must be verified before reader provisioning",
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
            "Production reader evidence state must remain pending",
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
        bucket_summary = self.workflow.index(
            "python scripts/summarize_growthbook_foundation_bucket.py"
        )
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
            "scripts/summarize_growthbook_foundation_bucket.py",
            "s3api list-multipart-uploads",
            "Production experiment bucket has incomplete multipart uploads",
            "scripts/resolve_growthbook_host_gate_runtime.py",
            "--expected-private-cidr 172.31.0.0/16",
            "log-stream-source=${PRODUCTION_READER_HOST_LOG_STREAM_SOURCE}",
            "COLLECTOR_LOCALHOST_HEALTH_OK:production:",
            "COLLECTOR_LOCALHOST_MARKER_OK:${RUNTIME_PATH}:",
        ):
            self.assertIn(marker, self.workflow)
        self.assertNotIn("bucket.get('KeyCount') != 0", self.workflow)
        self.assertLess(bucket_summary, service_gate)
        self.assertLess(service_gate, host_gate)
        self.assertLess(host_gate, create_user)

    def test_iam_absence_is_proven_fail_closed_twice_before_creation(self) -> None:
        pre_host = self.workflow.index(
            "PRODUCTION_READER_IAM_ABSENCE_OK:phase=pre-host:"
        )
        host_gate = self.workflow.index("PRODUCTION_READER_PREPROVISION_HARD_GATE_OK:")
        pre_create = self.workflow.index(
            "PRODUCTION_READER_IAM_ABSENCE_OK:phase=pre-create:"
        )
        create_user = self.workflow.index("aws iam create-user")
        for marker in (
            "production-reader-user-preprovision.err",
            "production-reader-user-final-check.err",
            '[[ "${READER_LOOKUP_STATUS}" -ne 254 ]]',
            "grep -Fq 'NoSuchEntity'",
            "Production GrowthBook reader absence could not be proven before host gate.",
            "Production GrowthBook reader absence could not be proven immediately before creation.",
            "PRODUCTION_READER_IAM_ABSENCE_OK:phase=pre-host:",
            "PRODUCTION_READER_IAM_ABSENCE_OK:phase=pre-create:",
        ):
            self.assertIn(marker, self.workflow)
        self.assertEqual(2, self.workflow.count("READER_LOOKUP_STATUS=$?"))
        self.assertEqual(2, self.workflow.count("grep -Fq 'NoSuchEntity'"))
        self.assertEqual(2, self.workflow.count('[[ "${READER_LOOKUP_STATUS}" -ne 254 ]]'))
        self.assertNotIn(
            'if aws iam get-user --user-name "${IAM_USER_NAME}" >/dev/null 2>&1;',
            self.workflow,
        )
        self.assertLess(pre_host, host_gate)
        self.assertLess(host_gate, pre_create)
        self.assertLess(pre_create, create_user)

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
            "name: vevo-growthbook-production-reader-credentials-${{ github.run_id }}",
            "path: vevo-growthbook-production-reader.cms",
            "retention-days: 1",
            "READER_EVIDENCE_FILE: vevo-growthbook-production-reader-evidence.json",
            "scripts/record_growthbook_production_reader_evidence.py build",
            '--foundation-workflow-run-id "${FOUNDATION_WORKFLOW_RUN_ID}"',
            '--foundation-sha256 "${FOUNDATION_EVIDENCE_SHA256}"',
            "name: vevo-growthbook-production-reader-evidence-${{ github.run_id }}",
            "path: vevo-growthbook-production-reader-evidence.json",
            "retention-days: 14",
            "GROWTHBOOK_PRODUCTION_READER_ACTIVE",
            "GROWTHBOOK_PRODUCTION_READER_FAILED_RUN_REVOKED",
        ):
            self.assertIn(marker, self.workflow)
        self.assertEqual(2, self.workflow.count("uses: actions/upload-artifact@v4.6.2"))
        credential_upload = self.workflow.index(
            "Upload encrypted one-time Production credential handoff"
        )
        evidence_upload = self.workflow.index(
            "Upload sanitized Production reader evidence", credential_upload
        )
        success = self.workflow.index(
            "Confirm successful Production reader provisioning", evidence_upload
        )
        self.assertLess(credential_upload, evidence_upload)
        self.assertLess(evidence_upload, success)
        credential_block = self.workflow[credential_upload:evidence_upload]
        evidence_block = self.workflow[evidence_upload:success]
        self.assertNotIn("vevo-growthbook-production-reader-evidence.json", credential_block)
        self.assertNotIn("vevo-growthbook-production-reader.cms", evidence_block)
        for forbidden in (
            'cat ${CREDENTIAL_JSON}',
            'cat "${CREDENTIAL_JSON}"',
            'GITHUB_ENV} < ${CREDENTIAL_JSON}',
            'GITHUB_OUTPUT} < ${CREDENTIAL_JSON}',
        ):
            self.assertNotIn(forbidden, self.workflow)

    def test_failure_cleanup_can_revoke_only_an_identity_created_by_this_run(self) -> None:
        cleanup_start = self.workflow.index("cleanup_failed_provision() {")
        cleanup_end = self.workflow.index("trap cleanup_failed_provision ERR", cleanup_start)
        cleanup = self.workflow[cleanup_start:cleanup_end]
        marker_guard = cleanup.index('if [[ -f "${CREATED_MARKER}" ]]')
        delete_key = cleanup.index("aws iam delete-access-key", marker_guard)
        detach_policy = cleanup.index("aws iam detach-user-policy", delete_key)
        delete_user = cleanup.index("aws iam delete-user", detach_policy)
        self.assertLess(marker_guard, delete_key)
        self.assertLess(delete_key, detach_policy)
        self.assertLess(detach_policy, delete_user)

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

    def test_every_inline_python_block_compiles(self) -> None:
        lines = self.workflow.splitlines()
        blocks: list[str] = []
        index = 0
        while index < len(lines):
            if "python - <<'PY'" not in lines[index]:
                index += 1
                continue
            index += 1
            body: list[str] = []
            while index < len(lines) and lines[index].strip() != "PY":
                body.append(lines[index])
                index += 1
            self.assertLess(index, len(lines), "unterminated inline Python block")
            blocks.append(textwrap.dedent("\n".join(body)))
            index += 1
        self.assertGreaterEqual(len(blocks), 6)
        for block_index, source in enumerate(blocks):
            compile(source, f"production-reader-inline-{block_index}.py", "exec")


if __name__ == "__main__":
    unittest.main()
