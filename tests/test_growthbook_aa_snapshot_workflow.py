from __future__ import annotations

import contextlib
import hashlib
import io
import json
import os
import pathlib
import tempfile
import textwrap
import unittest
from unittest import mock

import yaml


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOW_PATH = (
    ROOT
    / ".github"
    / "workflows"
    / "build-vevo-growthbook-production-aa-snapshot.yml"
)
MANIFEST_PATH = ROOT / "projects" / "vevo" / "growthbook_aa_snapshot.json"


def inline_python_blocks(workflow: str) -> list[str]:
    lines = workflow.splitlines()
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
        if index >= len(lines):
            raise AssertionError("unterminated inline Python block")
        blocks.append(textwrap.dedent("\n".join(body)))
        index += 1
    return blocks


def inline_python_block_containing(workflow: str, marker: str) -> str:
    matches = [source for source in inline_python_blocks(workflow) if marker in source]
    if len(matches) != 1:
        raise AssertionError(f"expected one inline Python block containing {marker!r}")
    return matches[0]


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
        self.assertEqual(
            "vevo-growthbook-aa-provenance.json", output["provenance_file"]
        )
        self.assertEqual(90, output["retention_days"])
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
            "[[ \"${GITHUB_RUN_ATTEMPT}\" == '1' ]]",
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

    def test_yaml_and_every_inline_python_block_are_valid(self) -> None:
        payload = yaml.safe_load(self.workflow)
        self.assertEqual(
            {"contents": "read", "actions": "read"}, payload["permissions"]
        )
        for block_index, source in enumerate(inline_python_blocks(self.workflow)):
            compile(source, f"aa-snapshot-workflow-inline-{block_index}.py", "exec")

    def test_provenance_binds_run_commit_components_and_file_hashes(self) -> None:
        source = inline_python_block_containing(
            self.workflow, "PRODUCTION_AA_SNAPSHOT_PROVENANCE_BOUND"
        )
        validator = inline_python_block_containing(
            self.workflow, "PRODUCTION_AA_SNAPSHOT_ARTIFACTS_OK"
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = pathlib.Path(temporary_directory)
            snapshot = b'{"snapshot":true}\n'
            decision = b'{"decision":true}\n'
            (root / "vevo-growthbook-aa-snapshot.json").write_bytes(snapshot)
            (root / "vevo-growthbook-aa-decision.json").write_bytes(decision)
            manifest = {
                "automated_evidence": {
                    "workflow": ".github/workflows/automated.yml",
                    "run_id": "32840000001",
                    "main_commit": "b" * 40,
                    "artifact_name": "automated-artifact",
                    "sha256": "c" * 64,
                },
                "manual_qa_evidence": {
                    "workflow": ".github/workflows/manual.yml",
                    "run_id": "32840000002",
                    "main_commit": "d" * 40,
                    "artifact_name": "manual-artifact",
                    "sha256": "e" * 64,
                },
            }
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            env = {
                "GITHUB_REPOSITORY": "vzeman/biznisweb",
                "GITHUB_REF": "refs/heads/main",
                "GITHUB_RUN_ATTEMPT": "1",
                "GITHUB_RUN_ID": "32850000001",
                "GITHUB_SHA": "a" * 40,
                "SNAPSHOT_MANIFEST": str(manifest_path),
                "SNAPSHOT_FILE": "vevo-growthbook-aa-snapshot.json",
                "DECISION_FILE": "vevo-growthbook-aa-decision.json",
                "PROVENANCE_FILE": "vevo-growthbook-aa-provenance.json",
            }
            output = io.StringIO()
            with contextlib.chdir(root), mock.patch.dict(os.environ, env, clear=False):
                with contextlib.redirect_stdout(output):
                    producer_namespace: dict[str, object] = {}
                    exec(
                        compile(source, "aa-snapshot-provenance.py", "exec"),
                        producer_namespace,
                        producer_namespace,
                    )
                    validator_namespace: dict[str, object] = {}
                    exec(
                        compile(validator, "aa-snapshot-artifacts.py", "exec"),
                        validator_namespace,
                        validator_namespace,
                    )
                    (root / "vevo-growthbook-aa-decision.json").write_bytes(
                        b'{"decision":false}\n'
                    )
                    with self.assertRaisesRegex(SystemExit, "file hash drift"):
                        validator_namespace = {}
                        exec(
                            compile(validator, "aa-snapshot-artifacts.py", "exec"),
                            validator_namespace,
                            validator_namespace,
                        )
            raw = (root / "vevo-growthbook-aa-provenance.json").read_bytes()
            provenance = json.loads(raw)

        self.assertEqual(
            raw,
            (
                json.dumps(
                    provenance,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                )
                + "\n"
            ).encode("utf-8"),
        )
        self.assertEqual("32850000001", provenance["workflow_run_id"])
        self.assertEqual(1, provenance["workflow_run_attempt"])
        self.assertEqual("a" * 40, provenance["main_commit"])
        self.assertEqual(
            {"sha256": hashlib.sha256(snapshot).hexdigest()},
            provenance["files"]["vevo-growthbook-aa-snapshot.json"],
        )
        self.assertEqual(
            "32840000001",
            provenance["source_components"]["automated_evidence"][
                "workflow_run_id"
            ],
        )
        self.assertIn(
            "PRODUCTION_AA_SNAPSHOT_PROVENANCE_BOUND:"
            "run=true:commit=true:components=true:file-hashes=true",
            output.getvalue(),
        )
        self.assertIn(
            "PRODUCTION_AA_SNAPSHOT_ARTIFACTS_OK:"
            "canonical=true:identities=false:provenance=true",
            output.getvalue(),
        )

    def test_uploads_only_snapshot_decision_and_provenance_not_components(self) -> None:
        upload = self.workflow.index(
            "Upload only sanitized snapshot decision and provenance"
        )
        self.assertEqual(1, self.workflow.count("uses: actions/upload-artifact@v4.6.2"))
        for marker in (
            "name: vevo-growthbook-aa-snapshot",
            "vevo-growthbook-aa-snapshot.json",
            "vevo-growthbook-aa-decision.json",
            "vevo-growthbook-aa-provenance.json",
            "retention-days: 90",
        ):
            self.assertIn(marker, self.workflow[upload:])
        self.assertIn(
            "A/A snapshot artifact contains an identity field", self.workflow
        )
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
