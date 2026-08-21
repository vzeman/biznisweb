from __future__ import annotations

import json
import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOW = (
    ROOT
    / ".github"
    / "workflows"
    / "verify-vevo-growthbook-production-aa-manual-qa.yml"
).read_text(encoding="utf-8")
MANIFEST = json.loads(
    (ROOT / "projects" / "vevo" / "growthbook_aa_snapshot.json").read_text(
        encoding="utf-8"
    )
)


class GrowthBookAaManualQaWorkflowTests(unittest.TestCase):
    def test_producer_and_observation_are_disabled_and_absent_by_default(self) -> None:
        manual = MANIFEST["manual_qa_evidence"]
        self.assertFalse(manual["producer_allowed"])
        self.assertEqual("not_recorded", manual["observation_status"])
        self.assertIsNone(manual["observation_sha256"])
        self.assertEqual("not_recorded", manual["status"])
        self.assertFalse(
            (ROOT / "projects" / "vevo" / "growthbook_aa_manual_qa_observation.json").exists()
        )

    def test_main_only_gate_precedes_evidence_creation(self) -> None:
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "confirm_manual_qa:",
            "manual QA evidence producer gate is closed",
            "reviewed browser QA observation is not recorded",
            "manual QA observation SHA-256 is missing",
            "Production foundation evidence is missing",
            "Production reader evidence is missing",
            "Production GrowthBook clone must be complete and re-closed",
            "Production A/A is not the only running experiment",
            "CTA A/B must remain unstarted during manual A/A QA",
            "PRODUCTION_AA_MANUAL_QA_LOCAL_GATE_OK:",
            "scripts/build_growthbook_aa_manual_qa_evidence.py",
        ):
            self.assertIn(marker, WORKFLOW)
        gate = WORKFLOW.index("PRODUCTION_AA_MANUAL_QA_LOCAL_GATE_OK:")
        build = WORKFLOW.index("scripts/build_growthbook_aa_manual_qa_evidence.py")
        upload = WORKFLOW.index("Upload sanitized manual QA evidence only")
        self.assertLess(gate, build)
        self.assertLess(build, upload)

    def test_hash_binds_reviewed_observation_and_current_workflow_provenance(self) -> None:
        for marker in (
            "OBSERVATION_SHA256",
            "--observation-sha256 \"${OBSERVATION_SHA256}\"",
            "--workflow-run-id \"${GITHUB_RUN_ID}\"",
            "--main-commit \"${GITHUB_SHA}\"",
            "evidence.get('source_run_id') != os.environ['GITHUB_RUN_ID']",
            "evidence.get('source_main_commit') != os.environ['GITHUB_SHA']",
            "hashlib.sha256(raw).hexdigest()",
        ):
            self.assertIn(marker, WORKFLOW)

    def test_uploads_only_one_sanitized_manual_component(self) -> None:
        self.assertEqual(1, WORKFLOW.count("uses: actions/upload-artifact@v4.6.2"))
        upload = WORKFLOW.index("Upload sanitized manual QA evidence only")
        for marker in (
            "name: vevo-growthbook-aa-manual-qa-evidence",
            "path: vevo-growthbook-aa-manual-qa-evidence.json",
            "retention-days: 14",
        ):
            self.assertIn(marker, WORKFLOW[upload:])
        self.assertNotIn("path: projects/vevo/growthbook_aa_manual_qa_observation.json", WORKFLOW)

    def test_has_no_browser_automation_network_or_control_plane_mutation(self) -> None:
        lowered = WORKFLOW.lower()
        for forbidden in (
            "configure-aws-credentials",
            "aws ",
            "boto3",
            "curl ",
            "wget ",
            "requests",
            "httpx",
            "playwright",
            "selenium",
            "tagmanager",
            "ads_update",
            "adcreatives_create",
            "biznisweb_api_token",
            "gh api",
            "gh run",
        ):
            self.assertNotIn(forbidden, lowered)
        self.assertIn("GrowthBook, GTM, Meta Ads and BiznisWeb mutation: `none`", WORKFLOW)
        self.assertIn("Winner calls allowed: `false`", WORKFLOW)


if __name__ == "__main__":
    unittest.main()
