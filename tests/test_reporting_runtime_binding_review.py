"""Independent failure-boundary regressions; all AWS/GitHub responses are fake."""
from __future__ import annotations

from copy import deepcopy
import hashlib
import os
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

from scripts import bootstrap_reporting_runtime as bootstrap
from scripts import reporting_runtime_binding as binding
from tests.test_reporting_runtime_binding import MemoryS3, NOW, RELEASE, baseline, loaded, promotion


class LostReleaseAcknowledgement(MemoryS3):
    def put_object(self, **kwargs):
        result = super().put_object(**kwargs)
        if kwargs["Key"] == binding.LOCK_KEY and binding.decode_json(kwargs["Body"])["state"] == "released":
            raise TimeoutError("synthetic lost acknowledgement after committed release")
        return result


class RuntimeBindingReviewTests(unittest.TestCase):
    def establish(self, store):
        record = baseline()
        lease = binding.MigrationLease(store, owner=record["release_id"], now=lambda: NOW).acquire()
        with patch.object(binding, "now_utc", return_value=NOW):
            binding.publish_verified_binding(store, record, expected_pointer_etag=None, lease=lease)
        lease.release()
        return binding.load_current_binding(store)

    def test_failed_candidate_without_pointer_change_invalidates_observation_window(self):
        store = MemoryS3()
        entry = self.establish(store)
        lease = binding.MigrationLease(store, owner="synthetic-migration", now=lambda: NOW).acquire()
        lease.release()
        exit_binding = binding.load_current_binding(store)
        self.assertEqual(entry["record"], exit_binding["record"])
        self.assertEqual(entry["pointer_etag"], exit_binding["pointer_etag"])
        self.assertNotEqual(entry["migration_etag"], exit_binding["migration_etag"])
        self.assertNotEqual(entry, exit_binding)

    def test_active_or_uncertain_generation_blocks_unchanged_successful_record(self):
        for state in ("active", "uncertain"):
            with self.subTest(state=state):
                store = MemoryS3()
                self.establish(store)
                lease = binding.MigrationLease(store, owner="synthetic-migration", now=lambda: NOW).acquire()
                if state == "uncertain":
                    lease.retain_uncertain()
                with self.assertRaisesRegex(binding.BindingError, "migration-active-or-uncertain"):
                    binding.load_current_binding(store)

    def test_lost_release_acknowledgement_is_reconciled_or_stays_blocked(self):
        store = LostReleaseAcknowledgement()
        lease = binding.MigrationLease(store, owner="synthetic-migration", now=lambda: NOW).acquire()
        try:
            lease.release()
        except Exception:
            # A reported unresolved release must never leave a consumable authority.
            with self.assertRaises(binding.BindingError):
                binding.check_migration(store)
        else:
            observed, etag = binding.read_object(store, binding.LOCK_KEY)
            self.assertEqual(observed["state"], "released")
            self.assertEqual(observed["owner"], lease.owner)
            self.assertEqual(etag, lease.etag)
        self.assertEqual(len(store.writes), 2, "do not replay the consumed release PUT")

    def test_foreign_generation_is_never_overwritten_by_uncertainty_cleanup(self):
        store = MemoryS3()
        lease = binding.MigrationLease(store, owner="first-migration", now=lambda: NOW).acquire()
        foreign = {"schema_version": 1, "owner": "foreign-migration", "state": "active",
                   "updated_at": NOW.isoformat(), "expires_at": "2026-09-13T10:20:00+00:00", "generation": "9" * 32}
        binding.validate_lock(foreign)
        foreign_etag = binding.write_object(store, binding.LOCK_KEY, foreign, etag=lease.etag)
        before = list(store.writes)
        with self.assertRaises(binding.BindingError):
            lease.retain_uncertain()
        self.assertEqual(binding.read_object(store, binding.LOCK_KEY), (foreign, foreign_etag))
        self.assertEqual(store.writes, before)

    def test_external_provenance_requires_terminal_success(self):
        record = baseline()
        run = {"id": int(record["workflow_run_id"]), "head_branch": "main",
               "repository": {"full_name": "vzeman/biznisweb"},
               "path": ".github/workflows/deploy-vevo-report.yml", "event": "workflow_dispatch",
               "status": "in_progress", "conclusion": None}
        with self.assertRaisesRegex(binding.BindingError, "managed-run-unverified"):
            binding.verify_managed_provenance(loaded(record), fetch_run=lambda _: run)
        run.update(status="completed", conclusion="success")
        binding.verify_managed_provenance(loaded(record), fetch_run=lambda _: run)
        for conclusion in ("failure", "cancelled", "timed_out", None):
            with self.subTest(conclusion=conclusion):
                run["conclusion"] = conclusion
                with self.assertRaises(binding.BindingError):
                    binding.verify_managed_provenance(loaded(record), fetch_run=lambda _: run)

    def test_in_progress_opt_in_is_bound_to_own_managed_run(self):
        record = baseline()
        run = {"id": int(record["workflow_run_id"]), "head_branch": "main", "head_sha": "8" * 40,
               "repository": {"full_name": "vzeman/biznisweb"},
               "path": ".github/workflows/deploy-vevo-report.yml", "event": "workflow_dispatch",
               "status": "in_progress", "conclusion": None}
        env = {"GITHUB_ACTIONS": "true", "GITHUB_REF": "refs/heads/main", "GITHUB_SHA": run["head_sha"],
               "GITHUB_RUN_ID": record["workflow_run_id"],
               "GITHUB_WORKFLOW_REF": "vzeman/biznisweb/.github/workflows/deploy-vevo-report.yml@refs/heads/main"}
        with patch.dict(os.environ, env, clear=True):
            binding.verify_managed_provenance(loaded(record), fetch_run=lambda _: run, require_completed=False)
        for field, value in (("GITHUB_ACTIONS", "false"), ("GITHUB_RUN_ID", "34799999999"),
                             ("GITHUB_REF", "refs/heads/review"), ("GITHUB_SHA", "9" * 40),
                             ("GITHUB_WORKFLOW_REF", "vzeman/biznisweb/.github/workflows/other.yml@refs/heads/main")):
            with self.subTest(field=field), patch.dict(os.environ, {**env, field: value}, clear=True):
                with self.assertRaises(binding.BindingError):
                    binding.verify_managed_provenance(loaded(record), fetch_run=lambda _: run, require_completed=False)

    def test_publisher_rejects_credential_role_resource_or_schedule_drift_from_predecessor(self):
        for drift in ("execution-role", "secret", "memory", "scheduler-role"):
            with self.subTest(drift=drift):
                store = MemoryS3()
                previous = self.establish(store)
                candidate = promotion(previous)
                if drift == "scheduler-role":
                    candidate["schedule"]["Target"]["RoleArn"] += "-changed"
                else:
                    for field in ("task_definition", "candidate_task_definition"):
                        definition = candidate[field]
                        if drift == "execution-role":
                            definition["executionRoleArn"] += "-changed"
                        elif drift == "secret":
                            definition["containerDefinitions"][0]["secrets"][0]["valueFrom"] += "changed"
                        else:
                            definition["memory"] = "8192"
                lease = binding.MigrationLease(store, owner=RELEASE, now=lambda: NOW).acquire()
                writes = list(store.writes)
                with patch.object(binding, "now_utc", return_value=NOW), self.assertRaises(binding.BindingError):
                    binding.publish_verified_binding(store, candidate, expected_pointer_etag=previous["pointer_etag"], lease=lease)
                self.assertEqual(store.writes, writes, "invalid transition must be rejected before publication")


class BootstrapHostReviewTests(unittest.TestCase):
    def setUp(self):
        self.record = baseline()
        self.source = "raise SystemExit('synthetic reviewed probe source')\n"
        proof = self.record["candidate_proof"]
        proof["command"] = binding.build_baseline_probe_command(
            self.source, binding.POLICY["baseline_runner_sha256"], self.record["image_digest"])
        self.marker = {"marker": "VEVO_REPORT_BASELINE_HOST_OK", "task_arn": proof["task_arn"],
                       "private_ip": proof["private_ip"], "image_digest": proof["image_digest"],
                       "path": "/app", "service": binding.POLICY["service"], "instance_id": "N/A:Fargate",
                       "runner_sha256": binding.POLICY["baseline_runner_sha256"], "provider_reads": 0}
        proof["localhost_marker_sha256"] = hashlib.sha256(binding.canonical_bytes(self.marker).rstrip(b"\n")).hexdigest()
        self.task = {"taskArn": proof["task_arn"], "taskDefinitionArn": proof["definition_arn"],
                     "clusterArn": binding.POLICY["cluster"], "launchType": "FARGATE", "lastStatus": "STOPPED",
                     "containers": [{"name": "reporting", "imageDigest": proof["image_digest"], "exitCode": 0,
                                     "networkInterfaces": [{"privateIpv4Address": proof["private_ip"]}]}],
                     "overrides": {"containerOverrides": [{"name": "reporting", "command": deepcopy(proof["command"])}]}}
        self.ecs = Mock(describe_tasks=Mock(return_value={"tasks": [self.task]}))

    def logs(self, markers):
        events = [{"message": "VEVO_REPORT_BASELINE_HOST_OK " + binding.canonical_bytes(marker).decode().strip()}
                  for marker in markers]
        return Mock(get_log_events=Mock(side_effect=[
            {"events": events, "nextForwardToken": "complete-page"},
            {"events": [], "nextForwardToken": "complete-page"}]))

    def verify(self, logs):
        # Only source retrieval is faked; command, ECS identity, pagination and marker validation are real.
        with patch.object(Path, "read_text", return_value=self.source):
            bootstrap.verify_host(self.ecs, logs, self.record)

    def test_complete_matching_marker_on_exact_stopped_host_passes(self):
        logs = self.logs([self.marker])
        self.verify(logs)
        self.assertEqual(logs.get_log_events.call_count, 2)
        for call in logs.get_log_events.call_args_list:
            self.assertEqual(call.kwargs["logGroupName"], "/ecs/vevo-reporting-daily")
            self.assertEqual(call.kwargs["logStreamName"], "ecs/reporting/" + self.task["taskArn"].rsplit("/", 1)[1])

    def test_missing_duplicate_or_false_zero_marker_is_rejected(self):
        false_zero = {**self.marker, "provider_reads": False}
        for markers in ([], [self.marker, self.marker], [false_zero]):
            with self.subTest(markers=len(markers)), self.assertRaisesRegex(binding.BindingError, "localhost-marker-invalid"):
                self.verify(self.logs(markers))

    def test_marker_for_another_task_ip_path_or_runner_is_rejected(self):
        for field, value in (("task_arn", self.task["taskArn"][:-1] + "0"), ("private_ip", "172.31.9.9"),
                             ("path", "/tmp"), ("runner_sha256", "0" * 64)):
            with self.subTest(field=field), self.assertRaisesRegex(binding.BindingError, "localhost-marker-invalid"):
                self.verify(self.logs([{**self.marker, field: value}]))

    def test_arbitrary_successful_command_cannot_supply_a_fabricated_marker(self):
        self.record["candidate_proof"]["command"] = ["python", "-c", "pass"]
        self.task["overrides"]["containerOverrides"][0]["command"] = ["python", "-c", "pass"]
        with self.assertRaisesRegex(binding.BindingError, "probe-source-contract"):
            self.verify(self.logs([self.marker]))
        self.ecs.describe_tasks.assert_not_called()

    def test_marker_hash_and_actual_container_exit_are_required(self):
        self.record["candidate_proof"]["localhost_marker_sha256"] = "0" * 64
        with self.assertRaisesRegex(binding.BindingError, "localhost-marker-hash"):
            self.verify(self.logs([self.marker]))
        for exit_code in (False, 1, None):
            self.task["containers"][0]["exitCode"] = exit_code
            with self.subTest(exit_code=exit_code), self.assertRaisesRegex(binding.BindingError, "container-identity"):
                self.verify(self.logs([self.marker]))

    def test_partial_log_stream_is_not_complete_even_if_first_page_has_marker(self):
        messages = [{"message": "VEVO_REPORT_BASELINE_HOST_OK " + binding.canonical_bytes(self.marker).decode().strip()}]
        logs = Mock(get_log_events=Mock(side_effect=[
            {"events": messages if index == 0 else [], "nextForwardToken": f"page-{index}"} for index in range(10)]))
        with self.assertRaisesRegex(binding.BindingError, "log-pagination-incomplete"):
            self.verify(logs)
        self.assertEqual(logs.get_log_events.call_count, 10)


if __name__ == "__main__":
    unittest.main()
