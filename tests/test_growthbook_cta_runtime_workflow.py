from __future__ import annotations

import pathlib
import textwrap
import unittest

import yaml


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOW_PATH = (
    ROOT
    / ".github"
    / "workflows"
    / "deploy-vevo-growthbook-production-cta-runtime.yml"
)
WORKFLOW = WORKFLOW_PATH.read_text(encoding="utf-8")


class GrowthBookCtaRuntimeWorkflowTests(unittest.TestCase):
    def test_yaml_and_every_inline_python_block_are_valid(self) -> None:
        payload = yaml.safe_load(WORKFLOW)
        self.assertEqual({"contents": "read"}, payload["permissions"])
        self.assertEqual(
            "${{ github.ref == 'refs/heads/main' }}",
            payload["jobs"]["deploy-cta-runtime"]["if"],
        )
        lines = WORKFLOW.splitlines()
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
        self.assertGreaterEqual(len(blocks), 12)
        for block_index, source in enumerate(blocks):
            compile(source, f"cta-runtime-workflow-inline-{block_index}.py", "exec")

    def test_local_gate_precedes_credentials_and_deploy_hard_gate_precedes_build(self) -> None:
        release = WORKFLOW.index("validate_growthbook_cta_runtime_release.py")
        credentials = WORKFLOW.index("Configure authenticated AWS deployment identity")
        hard_gate = WORKFLOW.index("VEVO_CTA_PREDEPLOY_HARD_GATE_OK:")
        build = WORKFLOW.index("Build and publish the immutable CTA-only collector image")
        self.assertLess(release, credentials)
        self.assertLess(credentials, hard_gate)
        self.assertLess(hard_gate, build)
        for marker in (
            "instance-id=N/A:Fargate:private-ip=",
            "service={os.environ[\"SERVICE_NAME\"]}:path=/app",
            "route=POST_/v1/events:aa=0:cta=0",
        ):
            self.assertIn(marker, WORKFLOW)

    def test_runtime_update_preserves_route_and_allows_only_candidate_resources(self) -> None:
        self.assertIn("candidate['PublicRouteEnabled'] = 'true'", WORKFLOW)
        self.assertIn("values['PublicRouteEnabled'] = 'true'", WORKFLOW)
        self.assertEqual(2, WORKFLOW.count("--phase candidate"))
        self.assertNotIn("--phase activate", WORKFLOW)
        self.assertNotIn("--phase deactivate", WORKFLOW)
        self.assertNotIn("apigatewayv2 create-route", WORKFLOW)
        self.assertNotIn("apigatewayv2 delete-route", WORKFLOW)
        self.assertNotIn("ecs update-service", WORKFLOW)
        self.assertNotIn("register-task-definition", WORKFLOW)

    def test_host_gate_binds_packaged_registry_before_service_and_query(self) -> None:
        localhost = WORKFLOW.index("VEVO_CTA_HOST_GATE_OK:")
        service = WORKFLOW.index("VEVO_CTA_SERVICE_RUNTIME_OK:")
        zero = WORKFLOW.index("VEVO_CTA_ZERO_EVENTS_OK:")
        evidence = WORKFLOW.index("build_growthbook_cta_runtime_readiness.py")
        self.assertLess(localhost, service)
        self.assertLess(service, zero)
        self.assertLess(zero, evidence)
        for marker in (
            "COLLECTOR_LOCALHOST_HEALTH_OK:production:git-${GITHUB_SHA}",
            "COLLECTOR_LOCALHOST_MARKER_OK:/app:git-${GITHUB_SHA}",
            "COLLECTOR_REGISTRY_OK:production:${REGISTRY_SHA256}:vevo-sk-product-cta-color-001",
            "SELECT COUNT(*) AS cta_events_before_start",
            "header != ['cta_events_before_start'] or values != ['0']",
            '--host-gate-task-id "${HOST_GATE_TASK_ID}"',
            '--host-gate-private-ip "${HOST_GATE_PRIVATE_IP}"',
            '--private-ip "${SERVICE_PRIVATE_IP}"',
        ):
            self.assertIn(marker, WORKFLOW)

    def test_failure_restores_exact_previous_runtime_and_repeats_localhost_gate(self) -> None:
        rollback = WORKFLOW.index("Restore the exact preceding collector runtime")
        upload = WORKFLOW.index("Upload only the sanitized canonical readiness observation")
        self.assertGreater(rollback, upload)
        for marker in (
            "env.RUNTIME_UPDATED == 'true'",
            "PREVIOUS_IMAGE_IDENTIFIER",
            "PREVIOUS_COLLECTOR_VERSION",
            "PREVIOUS_IMAGE_DIGEST",
            "rollback-parameters.json",
            "/app/growthbook_collector/host_gate.sh",
            "VEVO_CTA_RUNTIME_ROLLBACK_OK:",
            "route=preserved",
        ):
            self.assertIn(marker, WORKFLOW)

    def test_uploads_only_one_canonical_identity_free_artifact(self) -> None:
        self.assertEqual(1, WORKFLOW.count("uses: actions/upload-artifact@v4.6.2"))
        self.assertIn("path: ${{ env.EVIDENCE_FILE }}", WORKFLOW)
        self.assertIn("retention-days: 90", WORKFLOW)
        self.assertNotIn("path: ${{ env.TEMP_DIR }}", WORKFLOW)
        for forbidden in (
            "path: host-gate.log",
            "path: service-task.json",
            "path: cta-zero-results.json",
            "path: stack-candidate.json",
        ):
            self.assertNotIn(forbidden, WORKFLOW)

    def test_has_no_growthbook_gtm_meta_biznisweb_or_commerce_mutation_client(self) -> None:
        lowered = WORKFLOW.lower()
        for forbidden in (
            "cloudformation delete-stack",
            "tagmanager",
            "growthbook api",
            "api.growthbook.io",
            "graph.facebook.com",
            "ads_update",
            "adcreatives_create",
            "biznisweb_api_token",
            "add_to_cart(",
            "create-order",
        ):
            self.assertNotIn(forbidden, lowered)


if __name__ == "__main__":
    unittest.main()
