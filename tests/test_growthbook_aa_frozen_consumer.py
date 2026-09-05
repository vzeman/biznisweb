"""Synthetic executable coverage of the protected consumer, no live sources."""
import copy
import hashlib
import io
import json
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
from contextlib import redirect_stdout, redirect_stderr
from datetime import timedelta
from pathlib import Path
from unittest.mock import Mock, patch

from scripts import consume_growthbook_aa_quality_source as consumer
from scripts.build_growthbook_aa_quality_source import canonical_source_bytes, _input_digest
from scripts.build_growthbook_aa_automated_evidence import build_automated_evidence
from scripts.growthbook_aa_source_binding import verify_source_bundle
from scripts.record_growthbook_aa_evidence_gates import open_automated_producer
from reporting_core.experiments import build_experiment_facts
from tests.growthbook_aa_source_fixtures import source_bundle
from tests.test_growthbook_aa_quality_source import START, END, WINDOW, CONFIG, order
from tests.test_growthbook_pipeline import event
from tests.test_growthbook_quality_source_io import MemoryS3

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = (ROOT / consumer.WORKFLOW).read_text(encoding="utf-8")


def opened():
    bundle = source_bundle()
    original = json.loads(bundle["source_inputs"]["growthbook_aa_snapshot.json"])
    return open_automated_producer(original, **bundle), bundle


def capture_for(rows, orders=()):
    quality = build_experiment_facts(rows, orders, config=CONFIG, generated_at=END + timedelta(hours=1),
                                     measurement_window=WINDOW).quality_reports[0]
    return {"source": {"window": WINDOW.as_dict(), "quality": quality,
            "provenance": {"raw_extract_sha256": _input_digest(rows),
                           "generated_at_utc": (END + timedelta(hours=1)).isoformat().replace("+00:00", "Z")}}}


def inline_block(marker):
    lines = WORKFLOW[WORKFLOW.index(marker):].splitlines()
    start = next(index for index, line in enumerate(lines) if "python - <<'PY'" in line) + 1
    end = next(index for index in range(start, len(lines)) if lines[index].strip() == "PY")
    return textwrap.dedent("\n".join(lines[start:end]))


class FrozenConsumerTests(unittest.TestCase):
    def test_downloads_both_independent_archives_and_original_git_commit(self):
        snapshot, bundle = opened()
        binding = snapshot["automated_evidence"]["quality_source"]
        metadata = {}
        archives = {}
        for kind, run_id, artifact_id in (("source", binding["run_id"], binding["artifact_id"]),
                                        ("health", binding["health_run_id"], binding["health_artifact_id"])):
            metadata[f"repos/{consumer.REPO}/actions/runs/{run_id}"] = bundle[kind + "_run"]
            metadata[f"repos/{consumer.REPO}/actions/runs/{run_id}/artifacts?per_page=100"] = bundle[kind + "_artifacts"]
            archives[f"repos/{consumer.REPO}/actions/artifacts/{artifact_id}/zip"] = bundle[kind + "_zip"]
        read_json, read_zip = Mock(side_effect=metadata.__getitem__), Mock(side_effect=archives.__getitem__)
        git = Mock(return_value=bundle["source_inputs"])
        capture = consumer.verify_recorded_source(snapshot, read_json=read_json, read_zip=read_zip, read_inputs=git)
        self.assertEqual(verify_source_bundle(**bundle).capture, capture)
        git.assert_called_once_with(binding["main_commit"])
        self.assertEqual(4, read_json.call_count)
        self.assertEqual(2, read_zip.call_count)
        # Neither an artifact's own claims nor a later working-tree manifest
        # can replace the independent original input bytes.
        wrong = copy.deepcopy(bundle["source_inputs"])
        wrong["growthbook_aa_snapshot.json"] = canonical_source_bytes(snapshot)
        with self.assertRaises(ValueError):
            consumer.verify_recorded_source(snapshot, read_json=read_json, read_zip=read_zip,
                                            read_inputs=Mock(return_value=wrong))
        for key in ("json_sha256", "zip_sha256", "health_zip_sha256", "foundation_sha256"):
            altered = copy.deepcopy(snapshot)
            altered["automated_evidence"]["quality_source"][key] = "a" * 64
            with self.subTest(key=key), self.assertRaises(ValueError):
                consumer.verify_recorded_source(altered, read_json=read_json, read_zip=read_zip, read_inputs=git)

    def test_runtime_gate_requires_hosted_exact_main_confirmation_and_source_schema(self):
        snapshot, bundle = opened()
        inputs = {key: json.loads(raw) for key, raw in bundle["source_inputs"].items()}
        env = {"GITHUB_ACTIONS": "true", "RUNNER_ENVIRONMENT": "github-hosted",
               "GITHUB_REPOSITORY": consumer.REPO, "GITHUB_REF": "refs/heads/main",
               "GITHUB_EVENT_NAME": "workflow_dispatch", "GITHUB_RUN_ATTEMPT": "1",
               "GITHUB_WORKFLOW_REF": f"{consumer.REPO}/{consumer.WORKFLOW}@refs/heads/main",
               "GITHUB_SHA": "b" * 40, "GITHUB_RUN_ID": "123456789", "CONFIRM_COLLECTION": "true"}
        args = [snapshot, inputs["growthbook_production_aa_activation.json"], inputs["growthbook_aa_acceptance.json"],
                inputs["growthbook_production_reconciliation_deploy_evidence.json"]]
        self.assertEqual(snapshot["automated_evidence"]["quality_source"], consumer.validate_consumer_gate(*args, env))
        for key in env:
            with self.subTest(key=key), self.assertRaises(ValueError):
                consumer.validate_consumer_gate(*args, {**env, key: "wrong"})
        for key, value in (("producer_allowed", False), ("status", "verified")):
            bad = copy.deepcopy(snapshot)
            bad["automated_evidence"][key] = value
            with self.assertRaises(ValueError):
                consumer.validate_consumer_gate(bad, *args[1:], env)
        with self.assertRaises(ValueError):
            consumer.validate_consumer_gate(inputs["growthbook_aa_snapshot.json"], *args[1:], env)

    def test_closed_cli_cannot_construct_client_or_download_source(self):
        result = subprocess.run([sys.executable, "scripts/consume_growthbook_aa_quality_source.py", "--help"],
                                cwd=ROOT, capture_output=True, text=True, timeout=30)
        self.assertEqual(0, result.returncode)
        with patch('scripts.record_growthbook_aa_evidence_gates.EXACT_WINDOW_SOURCE_CAPTURE_SUPPORTED', False), \
             patch.object(sys, 'argv', ['consumer', 'verify-source']), patch.object(consumer, 'github_bytes') as read, \
             redirect_stderr(io.StringIO()) as stderr:
            self.assertEqual(1, consumer.main())
        self.assertEqual("VEVO_AA_CONSUMER_BLOCKED:stage=verify-source:details=suppressed", stderr.getvalue().strip())
        read.assert_not_called()

    def test_actual_pre_aws_cli_prepares_only_verified_capture_or_nothing(self):
        snapshot, bundle = opened()
        binding = snapshot['automated_evidence']['quality_source']
        metadata, archives = {}, {}
        for kind, run_id, artifact_id in (("source", binding["run_id"], binding["artifact_id"]),
                                        ("health", binding["health_run_id"], binding["health_artifact_id"])):
            metadata[f"repos/{consumer.REPO}/actions/runs/{run_id}"] = bundle[kind + "_run"]
            metadata[f"repos/{consumer.REPO}/actions/runs/{run_id}/artifacts?per_page=100"] = bundle[kind + "_artifacts"]
            archives[f"repos/{consumer.REPO}/actions/artifacts/{artifact_id}/zip"] = bundle[kind + "_zip"]
        for corrupt in (False, True):
            with self.subTest(corrupt=corrupt), tempfile.TemporaryDirectory(prefix='vevo-consumer-cli-') as directory:
                checkout = Path(directory) / 'checkout'
                project = checkout / 'projects/vevo'
                project.mkdir(parents=True)
                for name, raw in bundle['source_inputs'].items():
                    (project / name).write_bytes(raw)
                (project / 'growthbook_aa_snapshot.json').write_bytes(canonical_source_bytes(snapshot))
                run_temp = Path(directory) / 'runner'
                temp = run_temp / 'vevo-growthbook-aa-automated-123456789'
                temp.mkdir(parents=True)
                env = {'GITHUB_ACTIONS': 'true', 'RUNNER_ENVIRONMENT': 'github-hosted', 'GITHUB_REPOSITORY': consumer.REPO,
                       'GITHUB_REF': 'refs/heads/main', 'GITHUB_EVENT_NAME': 'workflow_dispatch', 'GITHUB_RUN_ATTEMPT': '1',
                       'GITHUB_WORKFLOW_REF': f'{consumer.REPO}/{consumer.WORKFLOW}@refs/heads/main',
                       'GITHUB_SHA': 'b' * 40, 'GITHUB_RUN_ID': '123456789', 'CONFIRM_COLLECTION': 'true',
                       'GITHUB_WORKSPACE': str(checkout), 'RUNNER_TEMP': str(run_temp), 'TEMP_EVIDENCE_DIR': str(temp)}
                def git(command, **kwargs):
                    return Mock(returncode=0, stdout=(('b' * 40) if command == ['git', 'rev-parse', 'HEAD'] else '').encode())
                data = copy.deepcopy(metadata)
                if corrupt:
                    data[f"repos/{consumer.REPO}/actions/runs/{binding['health_run_id']}"]['head_sha'] = 'a' * 40
                with patch.object(consumer, 'ROOT', checkout), patch.dict(os.environ, env), \
                     patch.object(consumer.subprocess, 'run', side_effect=git), \
                     patch.object(consumer, 'github_json', side_effect=data.__getitem__), \
                     patch.object(consumer, 'github_bytes', side_effect=archives.__getitem__), \
                     patch.object(consumer, 'read_git_source_inputs', return_value=bundle['source_inputs']) as git_inputs, \
                     patch.object(consumer, 'reject_previous_collection'), \
                     patch('scripts.record_growthbook_aa_evidence_gates.EXACT_WINDOW_SOURCE_CAPTURE_SUPPORTED', True), \
                     patch.object(sys, 'argv', ['consumer', 'verify-source']), \
                     redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()) as stderr:
                    result = consumer.main()
                git_inputs.assert_called_once_with(binding['main_commit'])
                self.assertEqual(int(corrupt), result)
                if corrupt:
                    self.assertEqual([], list(temp.iterdir()))
                    self.assertNotIn('Traceback', stderr.getvalue())
                else:
                    self.assertEqual({'verified-source.json', 'reporting-quality.json'}, {p.name for p in temp.iterdir()})
                    self.assertEqual(binding['json_sha256'], hashlib.sha256((temp / 'verified-source.json').read_bytes()).hexdigest())

    def test_duplicate_collection_history_fails_closed(self):
        for status, conclusion, artifacts in (("in_progress", None, 0), ("queued", None, 0),
                                             ("completed", "success", 0), ("completed", "failure", 1)):
            read = Mock(side_effect=[{"workflow_runs": [{"id": 123456789, "status": status, "conclusion": conclusion}]},
                                     {"total_count": artifacts}])
            with self.subTest(status=status, conclusion=conclusion), self.assertRaises(consumer.ConsumerError):
                consumer.reject_previous_collection("987654321", read_json=read)
        read = Mock(return_value={"workflow_runs": [{"id": 987654321, "status": "in_progress"}]})
        consumer.reject_previous_collection("987654321", read_json=read)

    def test_frozen_meta_uses_first_exposure_and_prior_context_not_rolling_cohort(self):
        before = event(received_at=START - timedelta(hours=1))
        repeated = event(received_at=START, device_id=before["device_id"])
        partial = event(received_at=START, meta_ad_id=None)
        new = event(received_at=END - timedelta(seconds=1))
        late = event(received_at=END, device_id=new["device_id"], variation_id="variant")
        # Later complete dimensions do not replace a partial first exposure.
        changed = event(received_at=START + timedelta(minutes=1), device_id=partial["device_id"])
        rows = [before, repeated, partial, changed, new, late]
        capture = capture_for(rows)
        expected = {"meta_exposure_count": 2, "complete_stable_dimension_exposure_count": 1,
                    "invalid_dimension_row_count": 0}
        self.assertEqual(expected, consumer.frozen_meta_audit(rows, capture, CONFIG))
        ordinary = build_experiment_facts(rows, (), config=CONFIG, generated_at=END + timedelta(hours=1))
        # Rolling facts contaminate the late device and retain the old device.
        self.assertEqual(1, ordinary.quality_reports[0]["contaminated_device_count"])
        self.assertEqual(0, capture["source"]["quality"]["contaminated_device_count"])

    def test_within_window_contamination_and_no_meta_remain_visible(self):
        first = event(received_at=START)
        crossed = event(received_at=START + timedelta(seconds=1), device_id=first["device_id"], variation_id="variant")
        organic = event(received_at=START, meta_campaign_id=None, meta_adset_id=None, meta_ad_id=None, meta_placement=None)
        rows = [first, crossed, organic]
        audit = consumer.frozen_meta_audit(rows, capture_for(rows), CONFIG)
        self.assertEqual({"meta_exposure_count": 0, "complete_stable_dimension_exposure_count": 0,
                          "invalid_dimension_row_count": 0}, audit)

    def test_input_substitution_and_population_drift_rejected_even_with_equal_totals(self):
        rows = [event(received_at=START)]
        capture = capture_for(rows)
        with self.assertRaisesRegex(consumer.ConsumerError, "retained input differs"):
            consumer.frozen_meta_audit([event(received_at=START)], capture, CONFIG)
        for key in ("eligible_device_count", "contaminated_device_count", "raw_event_count", "unique_event_count", "duplicate_event_count"):
            bad = copy.deepcopy(capture)
            bad["source"]["quality"][key] += 1
            with self.subTest(key=key), self.assertRaisesRegex(consumer.ConsumerError, "parity"):
                consumer.frozen_meta_audit(rows, bad, CONFIG)

    def test_meta_audit_preserves_captured_order_outcomes_and_input_privacy(self):
        first = event(received_at=START)
        purchase = event("order_completed", received_at=START + timedelta(minutes=1),
                         device_id=first["device_id"], transaction_id="only-test-order")
        rows = [first, purchase]
        capture = capture_for(rows, [order("only-test-order", START + timedelta(minutes=1))])
        saved = copy.deepcopy(capture)
        self.assertEqual(1, consumer.frozen_meta_audit(rows, capture, CONFIG)["meta_exposure_count"])
        self.assertEqual(saved, capture)
        audit = canonical_source_bytes(consumer.frozen_meta_audit(rows, capture, CONFIG)).decode()
        for identity in (first["event_id"], first["device_id"], "only-test-order", "789", "instagram_feed"):
            self.assertNotIn(identity, audit)

    def test_stable_s3_adapter_is_used_without_order_or_publisher(self):
        rows = [event(received_at=START)]
        s3 = MemoryS3(rows)
        capture = capture_for(rows)
        capture['source']['window']['context_from_utc'] = WINDOW.context_from_utc.replace(hour=0).isoformat().replace('+00:00', 'Z')
        audit = consumer.read_frozen_meta(s3, bucket="vevo-test-events", capture=capture, config=CONFIG)
        self.assertEqual(1, audit["meta_exposure_count"])
        self.assertTrue(s3.get_calls)
        self.assertTrue(all("IfMatch" in call for call in s3.get_calls))

    def test_activated_revision_two_is_accepted_and_old_foundation_rejected(self):
        activation = json.loads((ROOT / "projects/vevo/growthbook_production_aa_activation.json").read_bytes())
        current = activation["collector"]
        arn = "arn:aws:ecs:eu-central-1:919341186960:task-definition/" + current["task_definition"]
        service = {"status": "ACTIVE", "desiredCount": 1, "runningCount": 1, "pendingCount": 0,
                   "taskDefinition": arn, "enableExecuteCommand": False}
        task = {"taskArn": "synthetic/task/" + "1" * 32, "taskDefinitionArn": arn, "lastStatus": "RUNNING",
                "launchType": "FARGATE", "group": "service:" + consumer.SERVICE,
                "containers": [{"name": "collector", "imageDigest": current["image_digest"]}],
                "attachments": [{"details": [{"name": "privateIPv4Address", "value": "172.31.1.2"}]}]}
        definition = {"taskDefinitionArn": arn, "containerDefinitions": [{"name": "collector", "readonlyRootFilesystem": True,
                      "user": "10001:10001", "image": "synthetic@" + current["image_digest"]}]}
        result = consumer.validate_activated_runtime(service, task, definition, activation, task_definition_arn=arn)
        self.assertEqual(current["image_digest"], result["image_digest"])
        for target, key, value in ((service, "pendingCount", 1), (service, "enableExecuteCommand", True),
                                  (task, "launchType", "EC2"), (task, "group", "service:preview"),
                                  (definition, "taskDefinitionArn", arn.rsplit(":", 1)[0] + ":1")):
            saved = target[key]
            target[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                consumer.validate_activated_runtime(service, task, definition, activation, task_definition_arn=arn)
            target[key] = saved
        task["containers"][0]["imageDigest"] = "sha256:" + "9" * 64
        with self.assertRaisesRegex(ValueError, "image drift"):
            consumer.validate_activated_runtime(service, task, definition, activation, task_definition_arn=arn)

    def test_actual_workflow_observation_emits_source_schema_and_hash(self):
        snapshot, bundle = opened()
        capture = verify_source_bundle(**bundle).capture
        quality = capture["source"]["quality"]
        from tests.test_growthbook_aa_snapshot_assembler import automated_evidence
        sample = automated_evidence()
        columns = ["athena_unique_event_count", "total_stored_row_count", "audited_row_count", "pii_finding_count",
                   "full_url_stored_count", "click_identifier_stored_count", "forbidden_click_identifier_count",
                   "non_analytical_consent_exposure_count"]
        values = [quality["unique_event_count"], quality["raw_event_count"], quality["raw_event_count"], 0, 0, 0, 0, 0]
        athena = {"ResultSet": {"Rows": [{"Data": [{"VarCharValue": str(v)} for v in row]} for row in (columns, values)]}}
        with tempfile.TemporaryDirectory(prefix="vevo-consumer-test-") as directory:
            root = Path(directory)
            files = {"reporting-quality.json": quality, "verified-source.json": capture, "snapshot.json": snapshot,
                     "runtime.json": sample["production_runtime"], "frozen-meta.json": {"meta_exposure_count": 10,
                         "complete_stable_dimension_exposure_count": 10, "invalid_dimension_row_count": 0},
                     "access-reject-count.json": {"conservative_rejected_request_count": 0},
                     "receipt-counts.json": sample["pipeline_counts"], "athena-aggregate-results.json": athena}
            for name, data in files.items():
                (root / name).write_bytes(canonical_source_bytes(data))
            env = {"TEMP_EVIDENCE_DIR": str(root), "SNAPSHOT_MANIFEST": str(root / "snapshot.json"),
                   "EXPERIMENT_ID": consumer.EXPERIMENT, "AA_FROM_UTC": snapshot["automated_evidence"]["from_utc"],
                   "AA_THROUGH_UTC": snapshot["automated_evidence"]["through_utc"]}
            code = inline_block("Assemble exact sanitized observation")
            with patch.dict(os.environ, env), redirect_stdout(io.StringIO()):
                exec(compile(code, "actual-workflow-observation", "exec"), {})
            observation = json.loads((root / "automated-observation.json").read_bytes())
            evidence = build_automated_evidence(observation, workflow_run_id="123456789", main_commit="b" * 40)
            self.assertEqual(2, evidence["schema_version"])
            self.assertEqual(bundle["expected_evidence_sha256"], evidence["quality_source_sha256"])
            self.assertEqual(10, evidence["meta_dimension_audit"]["meta_exposure_count"])
            # A source-preserving population total is not enough: the actual
            # assembler rejects an independently queried raw mismatch as well.
            athena["ResultSet"]["Rows"][1]["Data"][0]["VarCharValue"] = "1"
            (root / "athena-aggregate-results.json").write_bytes(canonical_source_bytes(athena))
            with patch.dict(os.environ, env), self.assertRaisesRegex(SystemExit, "differs from the frozen"):
                exec(compile(code, "actual-workflow-observation", "exec"), {})


if __name__ == "__main__":
    unittest.main()
