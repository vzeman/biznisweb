from __future__ import annotations

import copy
import hashlib
import io
import json
import stat
import unittest
import zipfile
from unittest.mock import Mock, patch

from scripts import growthbook_aa_source_binding as binding
from scripts.build_growthbook_aa_quality_source import canonical_source_bytes
from scripts.record_growthbook_aa_evidence_gates import (
    EvidenceGateRecordingError, open_automated_producer, record_component,
)
from scripts.validate_growthbook_aa_measurement_window import (
    MeasurementWindowError, canonical_evidence_bytes, validate_measurement_window,
)
from tests.growthbook_aa_source_fixtures import source_bundle
from tests.test_growthbook_aa_evidence_gate_recorder import (
    AUTOMATED_RUN_ID, AUTOMATED_COMMIT, component_evidence, load, resolved_snapshot,
)


class SourceBindingTests(unittest.TestCase):
    def test_both_independent_archives_and_original_snapshot_bytes_are_bound(self):
        bundle = source_bundle()
        verified = binding.verify_source_bundle(**bundle)
        self.assertEqual(resolved_snapshot(), verified.snapshot)
        self.assertEqual(bundle["expected_evidence_sha256"], verified.binding["json_sha256"])
        self.assertEqual(hashlib.sha256(bundle["source_zip"]).hexdigest(), verified.binding["zip_sha256"])
        self.assertEqual(hashlib.sha256(bundle["health_zip"]).hexdigest(), verified.binding["health_zip_sha256"])
        self.assertEqual(hashlib.sha256(bundle["source_inputs"]["growthbook_aa_snapshot.json"]).hexdigest(),
                         verified.binding["snapshot_sha256"])
        self.assertNotIn("quality", verified.binding)
        self.assertNotIn("capture=", repr(verified))

    def test_independent_source_and_health_provenance_cannot_be_swapped(self):
        mutations = [("id", 999999), ("head_sha", "f" * 40), ("head_branch", "other"),
                     ("path", "other.yml"), ("status", "in_progress"), ("conclusion", "failure"),
                     ("repository", {"full_name": "unrelated/repo"}), ("head_repository", {"full_name": "unrelated/repo"}),
                     ("event", "pull_request")]
        for prefix in ("source", "health"):
            for field, value in mutations:
                bundle = source_bundle()
                bundle[prefix + "_run"][field] = value
                with self.subTest(prefix=prefix, field=field), self.assertRaises(ValueError):
                    binding.verify_source_bundle(**bundle)
        bundle = source_bundle()
        bundle["source_run"]["run_attempt"] = 2
        with self.assertRaisesRegex(ValueError, "recapture"):
            binding.verify_source_bundle(**bundle)

    def test_failed_expired_ambiguous_or_wrong_owner_artifact_is_rejected(self):
        for prefix in ("source", "health"):
            for change in (lambda a: a.update(total_count=2),
                           lambda a: a["artifacts"][0].update(expired=True),
                           lambda a: a["artifacts"][0].update(name="other"),
                           lambda a: a["artifacts"][0]["workflow_run"].update(id=999999),
                           lambda a: a["artifacts"][0]["workflow_run"].update(head_sha="f" * 40),
                           lambda a: a["artifacts"][0].update(digest="sha256:" + "f" * 64)):
                bundle = source_bundle()
                change(bundle[prefix + "_artifacts"])
                with self.subTest(prefix=prefix), self.assertRaises(ValueError):
                    binding.verify_source_bundle(**bundle)

    def test_zip_must_contain_exactly_one_regular_canonical_json(self):
        for mode in ("extra", "traversal", "symlink", "noncanonical", "oversized"):
            bundle = source_bundle()
            with zipfile.ZipFile(io.BytesIO(bundle["source_zip"])) as original:
                raw = original.read(binding.ARTIFACT + ".json")
            buffer = io.BytesIO()
            with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                entry = zipfile.ZipInfo(("../" if mode == "traversal" else "") + binding.ARTIFACT + ".json")
                if mode == "symlink":
                    entry.external_attr = (stat.S_IFLNK | 0o777) << 16
                payload = raw + b" " if mode == "noncanonical" else raw
                if mode == "oversized":
                    payload = b" " * (1024 * 1024 + 1)
                archive.writestr(entry, payload)
                if mode == "extra":
                    archive.writestr("extra.json", b"{}")
            bundle["source_zip"] = buffer.getvalue()
            bundle["source_artifacts"]["artifacts"][0]["digest"] = "sha256:" + hashlib.sha256(buffer.getvalue()).hexdigest()
            bundle["expected_evidence_sha256"] = hashlib.sha256(payload).hexdigest()
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                binding.verify_source_bundle(**bundle)

    def test_later_working_manifest_or_changed_source_bytes_cannot_replace_original(self):
        bundle = source_bundle()
        bundle["source_inputs"]["growthbook_aa_snapshot.json"] += b" "
        with self.assertRaises(ValueError):
            binding.verify_source_bundle(**bundle)
        bundle = source_bundle()
        snapshot = json.loads(bundle["source_inputs"]["growthbook_aa_snapshot.json"])
        snapshot["measurement_window"]["resolved_eligible_devices"] += 1
        bundle["source_inputs"]["growthbook_aa_snapshot.json"] = canonical_source_bytes(snapshot)
        with self.assertRaises(ValueError):
            binding.verify_source_bundle(**bundle)

    def test_source_foundation_and_health_deploy_provenance_are_independent(self):
        for name in ("growthbook_workspace.json", "growthbook_production_reconciliation_deploy_evidence.json"):
            bundle = source_bundle()
            if "workspace" in name:
                workspace = json.loads(bundle["source_inputs"][name])
                workspace["athena"]["production"]["successful_foundation_deployment"]["deployment"]["event_bucket_empty"] = False
                bundle["source_inputs"][name] = canonical_source_bytes(workspace)
            else:
                bundle["source_inputs"][name] += b" "
            with self.subTest(name=name), self.assertRaises(ValueError):
                binding.verify_source_bundle(**bundle)

    def test_capture_timestamp_must_fall_inside_independently_observed_run(self):
        for field, value in (("run_started_at", "2026-09-05T08:00:01Z"), ("updated_at", "2026-09-05T07:59:59Z")):
            bundle = source_bundle()
            bundle["source_run"][field] = value
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "outside"):
                binding.verify_source_bundle(**bundle)

    def test_quality_and_health_hashes_are_not_trusted_from_the_capture(self):
        for field in ("expected_evidence_sha256", "expected_health_sha256"):
            bundle = source_bundle()
            bundle[field] = "f" * 64
            with self.subTest(field=field), self.assertRaises(ValueError):
                binding.verify_source_bundle(**bundle)

    def test_transition_preserves_window_and_does_not_open_manual_or_snapshot(self):
        original = resolved_snapshot()
        saved = copy.deepcopy(original)
        opened = open_automated_producer(original, **source_bundle())
        self.assertEqual(saved, original)
        self.assertEqual(saved["measurement_window"], opened["measurement_window"])
        self.assertEqual(saved["manual_qa_evidence"], opened["manual_qa_evidence"])
        self.assertFalse(opened["snapshot_build_allowed"])
        self.assertNotEqual(hashlib.sha256(canonical_source_bytes(opened)).hexdigest(),
                            opened["automated_evidence"]["quality_source"]["snapshot_sha256"])

    def test_component_with_different_source_hash_cannot_be_recorded(self):
        opened = open_automated_producer(resolved_snapshot(), **source_bundle())
        component = component_evidence("automated")
        component["quality_source_sha256"] = "f" * 64
        with self.assertRaisesRegex(EvidenceGateRecordingError, "bound exact-window"):
            record_component(opened, component, component_name="automated",
                evidence_sha256=hashlib.sha256(canonical_evidence_bytes(component)).hexdigest(),
                expected_workflow_run_id=AUTOMATED_RUN_ID, expected_main_commit=AUTOMATED_COMMIT)

    def test_legacy_open_gate_and_source_binding_schema_drift_are_rejected(self):
        snapshot = resolved_snapshot()
        snapshot["automated_evidence"]["quality_report_status"] = "verified_canonical_reporting_quality"
        with self.assertRaisesRegex(MeasurementWindowError, "legacy quality"):
            self.validate(snapshot)
        opened = open_automated_producer(resolved_snapshot(), **source_bundle())
        for field, value in (("checkpoint_sha256", "f" * 64), ("through_utc", "2026-09-02T22:00:00Z"),
                             ("run_id", "bad"), ("json_sha256", "bad")):
            altered = copy.deepcopy(opened)
            altered["automated_evidence"]["quality_source"][field] = value
            with self.subTest(field=field), self.assertRaises(MeasurementWindowError):
                self.validate(altered)

    def test_git_loader_reads_exact_ancestor_blobs_without_working_tree_fallback(self):
        commit = "a" * 40
        responses = [str(binding.ROOT).encode(), commit.encode(), b""] + [b"{}" for _ in binding.INPUT_FILES]
        with patch.object(binding.subprocess, "run", side_effect=[Mock(returncode=0, stdout=raw) for raw in responses]) as git:
            result = binding.read_git_source_inputs(commit)
        self.assertEqual(set(binding.INPUT_FILES), set(result))
        commands = [call.args[0] for call in git.call_args_list]
        self.assertIn(["git", "--no-replace-objects", "merge-base", "--is-ancestor", commit, "HEAD"], commands)
        self.assertEqual(5, sum(command[2] == "show" for command in commands))
        self.assertTrue(all(command[2] not in {"fetch", "checkout", "pull"} for command in commands))
        with patch.object(binding.subprocess, "run", return_value=Mock(returncode=1, stdout=b"")):
            with self.assertRaisesRegex(ValueError, "Git object"):
                binding.read_git_source_inputs(commit)

    def test_consumer_support_gate_precedes_aws_credentials(self):
        workflow = (binding.ROOT / ".github/workflows/collect-vevo-growthbook-production-aa-evidence.yml").read_text()
        self.assertLess(workflow.index("exact-window automated consumer migration is not reviewed"),
                        workflow.index("configure-aws-credentials"))

    def test_health_cannot_finish_after_the_source_started(self):
        bundle = source_bundle()
        bundle["health_run"]["updated_at"] = "2026-09-05T08:00:01Z"
        with self.assertRaisesRegex(ValueError, "health must complete"):
            binding.verify_source_bundle(**bundle)

    def test_stale_health_is_rejected_even_when_both_artifacts_are_canonical(self):
        bundle = source_bundle()
        with zipfile.ZipFile(io.BytesIO(bundle["source_zip"])) as archive:
            capture = json.loads(archive.read(binding.ARTIFACT + ".json"))
        late = "2026-09-05T18:00:00Z"
        capture["acquisition"].update(started_at_utc=late, completed_at_utc=late)
        capture["source"]["provenance"]["generated_at_utc"] = late
        capture["source"]["quality"]["facts_generated_at"] = late
        raw = canonical_source_bytes(capture)
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr(binding.ARTIFACT + ".json", raw)
        bundle["source_zip"] = buffer.getvalue()
        bundle["expected_evidence_sha256"] = hashlib.sha256(raw).hexdigest()
        bundle["source_artifacts"]["artifacts"][0]["digest"] = "sha256:" + hashlib.sha256(buffer.getvalue()).hexdigest()
        bundle["source_run"].update(run_started_at=late, updated_at=late)
        with self.assertRaisesRegex(ValueError, "not fresh"):
            binding.verify_source_bundle(**bundle)

    def validate(self, snapshot):
        validate_measurement_window(snapshot, load("growthbook_production_aa_activation.json"),
                                    load("growthbook_aa_acceptance.json"),
                                    load("growthbook_production_reconciliation_deploy_evidence.json"))


if __name__ == "__main__":
    unittest.main()
