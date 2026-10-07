import copy
import json
import unittest
from unittest.mock import Mock, patch

from scripts import vevo_report_image_release as release
from scripts.vevo_report_image_host import report_arguments


IMAGE = f"{release.ACCOUNT}.dkr.ecr.{release.REGION}.amazonaws.com/vevo-reporting@sha256:" + "a" * 64
OLD_IMAGE = IMAGE[:-64] + "b" * 64
DATE = "2026-10-06"


def definition():
    return {"family": "vevo-reporting-daily", "networkMode": "awsvpc", "cpu": "1024", "memory": "2048",
            "taskRoleArn": "production-role", "executionRoleArn": "execution-role",
            "containerDefinitions": [{"name": "reporting", "image": OLD_IMAGE,
                "environment": [{"name": key, "value": value} for key, value in {
                    "REPORT_PROJECT": "vevo", "REPORT_S3_BUCKET": release.BUCKET,
                    "REPORT_S3_PREFIX": "daily-reports/vevo", "REPORT_SKIP_INVOICES": "true"}.items()],
                "secrets": [{"name": "BIZNISWEB_API_TOKEN", "valueFrom": "existing-reference"}]}]}


def artifact_set(*, probe=False, end=DATE):
    prefix = "private/probe/" if probe else "daily-reports/vevo/20261007T120000Z/"
    quality = {"is_partial": False, "qa_status": "warning", "qa_failure_count": 0, "qa_errors": []}
    blobs = {}
    for name in release.EXPECTED_ARTIFACTS:
        if name.endswith(".html"):
            raw = b"<html>synthetic report</html>"
        else:
            period = "full" if name == "dashboard_payload_latest.json" else name.split("_")[-1].split(".")[0]
            raw = json.dumps({"project": "vevo", "date_to": end, "source_health": quality,
                              "period_switcher": {"current_key": period}}).encode()
        blobs[prefix + name] = raw
    if probe:
        blobs[prefix + "data_quality.json"] = json.dumps(quality).encode()
    entries = {key.removeprefix(prefix): {"key": key, "size": len(raw), "sha256": release.sha(raw)}
               for key, raw in blobs.items()}
    return {"project": "vevo", "artifacts": entries}, blobs, prefix


class ImageReleaseTests(unittest.TestCase):
    def test_production_definition_changes_only_image_and_does_not_mutate_source(self):
        source = definition()
        before = copy.deepcopy(source)
        result = release.image_only_definition(source, IMAGE)
        expected = copy.deepcopy(before)
        expected["containerDefinitions"][0]["image"] = IMAGE
        self.assertEqual(expected, result)
        self.assertEqual(before, source)

    def test_host_gate_hash_uses_committed_blob_not_windows_working_file(self):
        with patch.object(release.subprocess, "check_output", return_value=b"source\n") as command:
            self.assertEqual(release.sha(b"source\n"), release.committed_gate_sha("a" * 40))
        self.assertEqual(["git", "show", "a" * 40 + ":scripts/reporting_migration_host_gate.py"], command.call_args.args[0])

    def test_foreign_sink_mutable_source_and_secret_control_collision_reject(self):
        for change in ("foreign", "mutable", "secret", "financial"):
            source = definition()
            container = source["containerDefinitions"][0]
            if change == "foreign":
                container["environment"][0]["value"] = "roy"
            elif change == "mutable":
                container["image"] = "registry/vevo-reporting:latest"
            elif change == "secret":
                container["secrets"].append({"name": "REPORT_SKIP_INVOICES", "valueFrom": "secret"})
            else:
                container["environment"][-1]["value"] = "false"
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                release.image_only_definition(source, IMAGE)

    def test_probe_uses_isolated_role_and_live_never_overrides_production_role(self):
        for probe in (True, False):
            result = release.task_overrides("c" * 32, "d" * 40, IMAGE, "e" * 64, DATE, probe=probe)
            env = {row["name"]: row["value"] for row in result["containerOverrides"][0]["environment"]}
            self.assertEqual(DATE, env["REPORT_TO_DATE"])
            self.assertTrue(all(env[key] == "true" for key in ("REPORT_SKIP_EMAIL", "REPORT_SKIP_INVOICES",
                                 "REPORT_SKIP_CREDITNOTE_STORNO_GUARD", "REPORT_FORCE_NO_CACHE", "REPORT_FORCE_CLEAR_CACHE")))
            self.assertEqual(probe, "taskRoleArn" in result)

    def test_live_arguments_skip_every_financial_runner_and_email_explicitly(self):
        args = report_arguments(DATE)
        for flag in ("--skip-email", "--skip-invoices", "--skip-creditnote-storno-guard", "--clear-cache", "--no-cache"):
            self.assertIn(flag, args)
        self.assertNotIn("--skip-export", args)
        self.assertEqual("", args[args.index("--output-tag") + 1])
        self.assertEqual(DATE, args[args.index("--to-date") + 1])
        with self.assertRaises(ValueError):
            report_arguments("yesterday")

    def test_all_periods_and_exact_end_date_required(self):
        for probe in (True, False):
            manifest, blobs, prefix = artifact_set(probe=probe)
            def fetch(key, limit):
                return blobs[key]
            release.verify_artifacts(manifest, fetch, prefix=prefix, to_date=DATE, probe=probe)
            del manifest["artifacts"]["dashboard_payload_90d.json"]
            with self.assertRaisesRegex(RuntimeError, "manifest-identity"):
                release.verify_artifacts(manifest, fetch, prefix=prefix, to_date=DATE, probe=probe)
        manifest, blobs, prefix = artifact_set(end="2026-10-05")
        with self.assertRaisesRegex(RuntimeError, "end-date"):
            release.verify_artifacts(manifest, lambda key, limit: blobs[key], prefix=prefix, to_date=DATE, probe=False)

    def test_cross_prefix_and_corrupt_artifacts_reject(self):
        for mutation in ("prefix", "hash"):
            manifest, blobs, prefix = artifact_set()
            if mutation == "prefix":
                manifest["artifacts"]["report_latest.html"]["key"] = "daily-reports/roy/report_latest.html"
            else:
                blobs[prefix + "report_latest.html"] = b"<html>tampered</html>"
            with self.subTest(mutation=mutation), self.assertRaises(RuntimeError):
                release.verify_artifacts(manifest, lambda key, limit: blobs[key], prefix=prefix, to_date=DATE, probe=False)

    def test_partial_quality_cannot_pass_even_with_consistent_hashes(self):
        manifest, blobs, prefix = artifact_set()
        key = prefix + "dashboard_payload_latest.json"
        payload = json.loads(blobs[key])
        payload["source_health"]["is_partial"] = True
        blobs[key] = json.dumps(payload).encode()
        manifest["artifacts"]["dashboard_payload_latest.json"].update(size=len(blobs[key]), sha256=release.sha(blobs[key]))
        with self.assertRaisesRegex(RuntimeError, "report-incomplete"):
            release.verify_artifacts(manifest, lambda key, limit: blobs[key], prefix=prefix, to_date=DATE, probe=False)

    def test_all_other_schedule_revisions_are_dynamic_and_protected(self):
        obj = object.__new__(release.ImageRelease)
        obj.release_id, obj.task_mode, obj.owned_task = "c" * 32, None, None
        obj.clock = lambda: 100
        obj.lease = Mock()
        obj.known_schedule = {"Name": release.SERVICE, "State": "DISABLED"}
        obj.protected = {"roy-daily-report-email": {"Name": "roy-daily-report-email", "State": "ENABLED",
                         "Target": {"EcsParameters": {"TaskDefinitionArn": "roy:777"}}}}
        obj.all_schedules = Mock(return_value={release.SERVICE: obj.known_schedule, **obj.protected})
        with patch("builtins.print"):
            obj.checkpoint()
        changed = copy.deepcopy(obj.all_schedules.return_value)
        changed["roy-daily-report-email"]["Target"]["EcsParameters"]["TaskDefinitionArn"] = "roy:778"
        obj.all_schedules.return_value = changed
        with self.assertRaisesRegex(RuntimeError, "other-schedule-drift"):
            obj.checkpoint()

    def failure_object(self, *, live=False, uncertain=False):
        obj = object.__new__(release.ImageRelease)
        obj.lease_owned, obj.start_uncertain, obj.rerun_attempted = True, uncertain, live
        obj.event, obj.cleanup_task, obj.cleanup_role = Mock(), Mock(), Mock()
        obj.lease, obj.ecs = Mock(), Mock()
        obj.original = {"Name": release.SERVICE, "State": "ENABLED", "Target": {"EcsParameters": {"TaskDefinitionArn": "old"}}}
        obj.known_schedule = copy.deepcopy(obj.original)
        obj.known_schedule["State"] = "DISABLED"
        obj.schedule = Mock(return_value=obj.known_schedule)
        obj.update = Mock()
        obj.candidate = None
        obj.live_outputs = Mock(return_value={})
        return obj

    def test_uncertain_dispatch_does_not_restore_enable_or_redispatch(self):
        obj = self.failure_object(uncertain=True)
        obj.recover_failure()
        obj.lease.retain_uncertain.assert_called_once()
        obj.update.assert_not_called()
        obj.cleanup_task.assert_not_called()
        obj.lease.release.assert_not_called()

    def test_failed_live_publication_preserves_candidate_paused_and_requires_review(self):
        obj = self.failure_object(live=True)
        obj.known_schedule["Target"]["EcsParameters"]["TaskDefinitionArn"] = "new"
        obj.recover_failure()
        desired = obj.update.call_args.args[0]
        self.assertEqual("new", desired["Target"]["EcsParameters"]["TaskDefinitionArn"])
        self.assertEqual("DISABLED", desired["State"])
        obj.lease.retain_uncertain.assert_called_once()
        obj.lease.release.assert_not_called()

    def test_failure_before_live_restores_only_owned_original_schedule(self):
        obj = self.failure_object()
        obj.recover_failure()
        obj.update.assert_called_once_with(obj.original)
        obj.lease.release.assert_called_once()
        obj.ecs.run_task.assert_not_called()

    def test_foreign_schedule_is_never_overwritten_during_failure(self):
        obj = self.failure_object()
        obj.schedule.return_value = {"Name": release.SERVICE, "State": "ENABLED", "Target": {"foreign": True}}
        with self.assertRaisesRegex(RuntimeError, "foreign-schedule"):
            obj.recover_failure()
        obj.update.assert_not_called()
        obj.lease.retain_uncertain.assert_called_once()


if __name__ == "__main__":
    unittest.main()
