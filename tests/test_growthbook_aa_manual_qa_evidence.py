from __future__ import annotations

import contextlib
import hashlib
import io
import json
import pathlib
import tempfile
import unittest

from scripts.build_growthbook_aa_manual_qa_evidence import (
    ManualQaEvidenceError,
    build_manual_qa_evidence,
    main,
)
from tests.test_growthbook_aa_snapshot_assembler import (
    MANUAL_COMMIT,
    MANUAL_RUN_ID,
    assemble,
    canonical,
    manual_evidence,
)


def observation() -> dict[str, object]:
    evidence = manual_evidence()
    evidence.pop("source_run_id")
    evidence.pop("source_main_commit")
    evidence["evidence_type"] = "pending_workflow_provenance"
    evidence["observation_type"] = "vevo_growthbook_aa_manual_qa_observation"
    return evidence


class GrowthBookAaManualQaEvidenceTests(unittest.TestCase):
    def test_builds_exact_hashable_component_and_preserves_snapshot_contract(self) -> None:
        evidence = build_manual_qa_evidence(
            observation(), workflow_run_id=MANUAL_RUN_ID, main_commit=MANUAL_COMMIT
        )
        self.assertEqual("vevo_growthbook_aa_manual_qa_evidence", evidence["evidence_type"])
        self.assertEqual(MANUAL_RUN_ID, evidence["source_run_id"])
        self.assertEqual(MANUAL_COMMIT, evidence["source_main_commit"])
        self.assertNotIn("observation_type", evidence)
        self.assertEqual("vevo-sk-aa-001", assemble(manual=evidence)["experiment_id"])
        serialized = json.dumps(evidence, sort_keys=True).lower()
        for forbidden in (
            '"event_id":',
            '"device_id":',
            '"transaction_id":',
            '"email":',
            "fbclid",
        ):
            self.assertNotIn(forbidden, serialized)

    def test_rejects_extended_false_safety_and_nonproduction_observations(self) -> None:
        extended = observation()
        extended["notes"] = "not allowed"
        with self.assertRaisesRegex(ManualQaEvidenceError, "field set drift"):
            build_manual_qa_evidence(
                extended, workflow_run_id=MANUAL_RUN_ID, main_commit=MANUAL_COMMIT
            )

        unsafe = observation()
        unsafe["unplanned_mutation_observed"] = True
        with self.assertRaisesRegex(ManualQaEvidenceError, "must be false"):
            build_manual_qa_evidence(
                unsafe, workflow_run_id=MANUAL_RUN_ID, main_commit=MANUAL_COMMIT
            )

        wrong_allocation = observation()
        wrong_allocation["production_allocation_percent"] = 0
        with self.assertRaisesRegex(ManualQaEvidenceError, "100 percent"):
            build_manual_qa_evidence(
                wrong_allocation,
                workflow_run_id=MANUAL_RUN_ID,
                main_commit=MANUAL_COMMIT,
            )

    def test_cli_requires_canonical_observation_and_independent_digest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary = pathlib.Path(temporary_directory)
            observation_path = temporary / "observation.json"
            output_path = temporary / "evidence.json"
            raw = canonical(observation())
            observation_path.write_bytes(raw)
            arguments = [
                "--observation",
                str(observation_path),
                "--observation-sha256",
                hashlib.sha256(raw).hexdigest(),
                "--workflow-run-id",
                MANUAL_RUN_ID,
                "--main-commit",
                MANUAL_COMMIT,
                "--output",
                str(output_path),
            ]
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(0, main(arguments))
            output = output_path.read_bytes()
            self.assertEqual(output, canonical(json.loads(output)))

            observation_path.write_text(json.dumps(observation(), indent=2), encoding="utf-8")
            arguments[arguments.index("--observation-sha256") + 1] = hashlib.sha256(
                observation_path.read_bytes()
            ).hexdigest()
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(2, main(arguments))

    def test_cli_rejects_tampered_observation_digest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary = pathlib.Path(temporary_directory)
            observation_path = temporary / "observation.json"
            observation_path.write_bytes(canonical(observation()))
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(
                    2,
                    main(
                        [
                            "--observation",
                            str(observation_path),
                            "--observation-sha256",
                            "0" * 64,
                            "--workflow-run-id",
                            MANUAL_RUN_ID,
                            "--main-commit",
                            MANUAL_COMMIT,
                            "--output",
                            str(temporary / "evidence.json"),
                        ]
                    ),
                )


if __name__ == "__main__":
    unittest.main()
