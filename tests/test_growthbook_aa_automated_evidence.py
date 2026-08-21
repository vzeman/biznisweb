from __future__ import annotations

import contextlib
import copy
import hashlib
import io
import json
import pathlib
import tempfile
import unittest

from scripts.build_growthbook_aa_automated_evidence import (
    AutomatedEvidenceError,
    build_automated_evidence,
    load_canonical_observation,
    main,
)
from tests.test_growthbook_aa_snapshot_assembler import automated_evidence


RUN_ID = "32490000332"
COMMIT = "a" * 40


def observation() -> dict[str, object]:
    value = automated_evidence()
    del value["source_run_id"]
    del value["source_main_commit"]
    value["observation_type"] = "vevo_growthbook_aa_automated_observation"
    value["evidence_type"] = "pending_workflow_provenance"
    return value


def canonical(payload: dict[str, object]) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


class GrowthBookAaAutomatedEvidenceTests(unittest.TestCase):
    def test_builds_exact_sanitized_component_with_current_provenance(self) -> None:
        evidence = build_automated_evidence(
            observation(), workflow_run_id=RUN_ID, main_commit=COMMIT
        )
        self.assertEqual("vevo_growthbook_aa_automated_evidence", evidence["evidence_type"])
        self.assertEqual(RUN_ID, evidence["source_run_id"])
        self.assertEqual(COMMIT, evidence["source_main_commit"])
        def keys(value: object) -> set[str]:
            if isinstance(value, dict):
                return set(value) | {key for row in value.values() for key in keys(row)}
            if isinstance(value, list):
                return {key for row in value for key in keys(row)}
            return set()

        all_keys = keys(evidence)
        for forbidden in (
            "event_id",
            "device_id",
            "transaction_id",
            "customer_email",
            "fbclid",
            "cloudwatch_messages",
        ):
            self.assertNotIn(forbidden, all_keys)

    def test_rejects_extended_or_unsafe_observations(self) -> None:
        extended = observation()
        extended["device_id"] = "forbidden"
        with self.assertRaisesRegex(AutomatedEvidenceError, "field set drift"):
            build_automated_evidence(extended, workflow_run_id=RUN_ID, main_commit=COMMIT)

        unsafe = observation()
        unsafe["contains_cloudwatch_messages"] = True
        with self.assertRaisesRegex(AutomatedEvidenceError, "must be false"):
            build_automated_evidence(unsafe, workflow_run_id=RUN_ID, main_commit=COMMIT)

        mutation = observation()
        mutation["mutation_observed"] = True
        with self.assertRaisesRegex(AutomatedEvidenceError, "must be false"):
            build_automated_evidence(mutation, workflow_run_id=RUN_ID, main_commit=COMMIT)

    def test_rejects_runtime_count_and_provenance_drift(self) -> None:
        wrong_runtime = observation()
        wrong_runtime["production_runtime"]["path"] = "/tmp"
        with self.assertRaisesRegex(AutomatedEvidenceError, "runtime path drift"):
            build_automated_evidence(wrong_runtime, workflow_run_id=RUN_ID, main_commit=COMMIT)

        wrong_counts = observation()
        wrong_counts["pipeline_counts"]["collector_duplicate_event_count"] = 11
        with self.assertRaisesRegex(AutomatedEvidenceError, "receipt identity drift"):
            build_automated_evidence(wrong_counts, workflow_run_id=RUN_ID, main_commit=COMMIT)

        with self.assertRaisesRegex(AutomatedEvidenceError, "run ID is invalid"):
            build_automated_evidence(observation(), workflow_run_id="local", main_commit=COMMIT)

    def test_loader_requires_canonical_hash_bound_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "observation.json"
            raw = canonical(observation())
            path.write_bytes(raw)
            self.assertEqual(
                observation(), load_canonical_observation(path, hashlib.sha256(raw).hexdigest())
            )
            with self.assertRaisesRegex(AutomatedEvidenceError, "SHA-256 mismatch"):
                load_canonical_observation(path, "0" * 64)
            path.write_text(json.dumps(observation(), indent=2), encoding="utf-8")
            with self.assertRaisesRegex(AutomatedEvidenceError, "canonical JSON bytes"):
                load_canonical_observation(path, hashlib.sha256(path.read_bytes()).hexdigest())

    def test_cli_writes_only_canonical_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source = root / "observation.json"
            output = root / "evidence.json"
            raw = canonical(observation())
            source.write_bytes(raw)
            arguments = [
                "--observation",
                str(source),
                "--observation-sha256",
                hashlib.sha256(raw).hexdigest(),
                "--workflow-run-id",
                RUN_ID,
                "--main-commit",
                COMMIT,
                "--output",
                str(output),
            ]
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(0, main(arguments))
            evidence = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(output.read_bytes(), canonical(evidence))

            tampered = copy.deepcopy(observation())
            tampered["source_read_only"] = False
            source.write_bytes(canonical(tampered))
            arguments[arguments.index("--observation-sha256") + 1] = hashlib.sha256(
                source.read_bytes()
            ).hexdigest()
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(2, main(arguments))


if __name__ == "__main__":
    unittest.main()
