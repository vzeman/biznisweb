from __future__ import annotations

import json
import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOW_PATH = (
    ROOT
    / ".github"
    / "workflows"
    / "build-vevo-growthbook-production-aa-snapshot.yml"
)
MANIFEST_PATH = ROOT / "projects" / "vevo" / "growthbook_aa_snapshot.json"


class GrowthBookAaSnapshotWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
        cls.manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))

    def test_manifest_is_disabled_and_closes_every_mutation_boundary(self) -> None:
        self.assertEqual(2, self.manifest["schema_version"])
        self.assertEqual("vevo-sk-aa-001", self.manifest["experiment_id"])
        self.assertFalse(self.manifest["snapshot_build_allowed"])
        self.assertEqual("not_recorded", self.manifest["automated_evidence"]["status"])
        self.assertEqual("not_recorded", self.manifest["manual_qa_evidence"]["status"])
        for group in ("automated_evidence", "manual_qa_evidence"):
            for field in ("run_id", "main_commit", "sha256"):
                self.assertIsNone(self.manifest[group][field])
        boundaries = self.manifest["release_boundaries"]
        self.assertTrue(boundaries["main_only"])
        self.assertTrue(boundaries["github_artifact_reads_only"])
        for field in (
            "aws_credentials_allowed",
            "aws_api_calls_allowed",
            "growthbook_mutation_allowed",
            "gtm_mutation_allowed",
            "meta_ads_mutation_allowed",
            "biznisweb_mutation_allowed",
            "winner_calls_allowed",
            "cta_activation_allowed",
        ):
            self.assertFalse(boundaries[field])
        output = self.manifest["output"]
        self.assertEqual("vevo-growthbook-aa-snapshot", output["artifact_name"])
        self.assertEqual(14, output["retention_days"])
        for field in (
            "contains_component_artifacts",
            "contains_raw_aws_payloads",
            "contains_cloudwatch_messages",
            "contains_event_or_device_ids",
            "contains_customer_or_order_data",
        ):
            self.assertFalse(output[field])

    def test_main_only_local_gate_precedes_all_artifact_access(self) -> None:
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "confirm_snapshot:",
            "snapshot_build_allowed') is not True",
            "validate_growthbook_aa_measurement_window.py",
            "pre-registered A/A stopping rule is not resolved",
            "production_allocation_percent') != 100",
            "Production foundation evidence is missing",
            "Production reader evidence is missing",
            "Production GrowthBook clone must be complete and re-closed",
            "PRODUCTION_AA_SNAPSHOT_LOCAL_GATE_OK:",
            "gh api \"repos/${GITHUB_REPOSITORY}/actions/runs/${AUTOMATED_RUN_ID}\"",
        ):
            self.assertIn(marker, self.workflow)
        local_gate = self.workflow.index("PRODUCTION_AA_SNAPSHOT_LOCAL_GATE_OK:")
        artifact_access = self.workflow.index(
            "gh api \"repos/${GITHUB_REPOSITORY}/actions/runs/${AUTOMATED_RUN_ID}\""
        )
        self.assertLess(local_gate, artifact_access)

    def test_independently_binds_both_components_to_run_commit_and_sha(self) -> None:
        for marker in (
            "AUTOMATED_RUN_ID",
            "AUTOMATED_SHA256",
            "MANUAL_QA_RUN_ID",
            "MANUAL_QA_SHA256",
            "run.get('head_branch') != 'main'",
            "run.get('head_sha') != expected['main_commit']",
            "run.get('event') != 'workflow_dispatch'",
            "run.get('name') != expected['workflow_name']",
            "sha256sum \"automated-component/${AUTOMATED_EVIDENCE_FILE}\"",
            "sha256sum \"manual-component/${MANUAL_QA_EVIDENCE_FILE}\"",
            "find automated-component -mindepth 1 -type d -o -type l",
            "find manual-component -mindepth 1 -type d -o -type l",
            "scripts/assemble_growthbook_aa_snapshot.py",
        ):
            self.assertIn(marker, self.workflow)

    def test_uploads_only_snapshot_and_decision_not_source_components(self) -> None:
        upload = self.workflow.index("Upload only sanitized snapshot and decision")
        self.assertEqual(1, self.workflow.count("uses: actions/upload-artifact@v4.6.2"))
        for marker in (
            "name: vevo-growthbook-aa-snapshot",
            "vevo-growthbook-aa-snapshot.json",
            "vevo-growthbook-aa-decision.json",
            "retention-days: 14",
        ):
            self.assertIn(marker, self.workflow[upload:])
        for forbidden in (
            "path: automated-component",
            "path: manual-component",
            "path: automated-run.json",
            "path: manual-run.json",
        ):
            self.assertNotIn(forbidden, self.workflow)

    def test_workflow_has_no_aws_or_external_control_plane_mutation_path(self) -> None:
        lowered = self.workflow.lower()
        for forbidden in (
            "configure-aws-credentials",
            "aws ",
            "boto3",
            "start-query-execution",
            "ecs run-task",
            "register-task-definition",
            "cloudformation create-",
            "cloudformation update-",
            "cloudformation delete-",
            "growthbook api",
            "tagmanager",
            "ads_update",
            "adcreatives_create",
            "biznisweb_api_token",
            "curl ",
            "wget ",
        ):
            self.assertNotIn(forbidden, lowered)
        self.assertIn("winner=false:cta=unchanged", self.workflow)
        self.assertIn(
            "GrowthBook, GTM, Meta Ads, BiznisWeb and Production traffic: `unchanged`",
            self.workflow,
        )


if __name__ == "__main__":
    unittest.main()
