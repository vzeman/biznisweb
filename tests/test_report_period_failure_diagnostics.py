"""Retain all periods on rejection without publishing an incomplete generation."""
import copy
from io import BytesIO
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import daily_report_runner as runner
from scripts import reporting_migration_host_gate as host
from scripts import vevo_report_image_release as controller


GOOD = {"is_partial": False, "qa_status": "warning", "qa_failure_count": 0, "qa_errors": []}
RID = "a" * 32
START, END = "2025-05-03", "2026-10-07"


def fixture(root, project="vevo"):
    folder = root / "data" / project
    folder.mkdir(parents=True)
    paths, specs = {}, []
    for period in ("7d", "30d", "90d", "full"):
        report = folder / f"report_{period}__migration_{RID}.html"
        payload = report.with_name(report.name.replace("report_", "dashboard_payload_").replace(".html", ".json"))
        report.write_text("synthetic", encoding="utf-8")
        payload.write_text(json.dumps({"project": project, "date_from": START, "date_to": END,
            "source_health": GOOD, "customers": ["MUST_NOT_BE_RETAINED"],
            "period_switcher": {"current_key": period, "_embedded_specs": list(specs)}}), encoding="utf-8")
        if period == "full":
            paths.update(report_latest_html=report, dashboard_payload_latest_json=payload)
        else:
            specs.append({"key": period, "report_path": str(report)})
    quality = folder / f"data_quality_20250503-20261007__migration_{RID}.json"
    quality.write_text(json.dumps(GOOD), encoding="utf-8")
    paths["data_quality_json"] = quality
    return paths


def change(paths, period, **updates):
    target = paths["dashboard_payload_latest_json"].with_name(f"dashboard_payload_{period}__migration_{RID}.json")
    value = json.loads(target.read_bytes())
    value.update(updates)
    target.write_text(json.dumps(value), encoding="utf-8")
    return target


class PrivateS3:
    def __init__(self):
        self.objects = {}

    def put_object(self, **kw):
        assert kw["IfNoneMatch"] == "*" and kw["ServerSideEncryption"] == "AES256"
        assert kw["ExpectedBucketOwner"] == host.ACCOUNT
        assert kw["Key"] not in self.objects
        self.objects[kw["Key"]] = kw["Body"]

    def get_object(self, **kw):
        return {"Body": BytesIO(self.objects[kw["Key"]]), "ServerSideEncryption": "AES256"}


class PeriodDiagnosticsTests(unittest.TestCase):
    def test_first_rejection_contains_every_period_and_exact_bad_fields(self):
        with TemporaryDirectory() as folder:
            paths = fixture(Path(folder))
            bad = {**GOOD, "is_partial": True, "qa_status": "critical", "qa_failure_count": 2,
                   "qa_errors": ["synthetic credit source unavailable"]}
            change(paths, "7d", source_health=bad)
            change(paths, "90d", source_health={**GOOD, "qa_failure_count": False})
            with self.assertRaisesRegex(RuntimeError, "dashboard_payload_7d.json") as caught:
                runner.validate_generated_report("vevo", paths, GOOD, START, END)
            evidence = caught.exception.publication_diagnostics
            periods = evidence["periods"]
            self.assertEqual({"latest", "7d", "30d", "90d"}, set(periods))
            self.assertEqual(bad, periods["7d"]["source_health"])
            self.assertEqual(["source_health.is_partial", "source_health.qa_status", "source_health.qa_failure_count", "source_health.qa_errors"], periods["7d"]["invalid_fields"])
            self.assertEqual(["source_health.qa_failure_count"], periods["90d"]["invalid_fields"])
            self.assertEqual([], periods["latest"]["invalid_fields"])
            self.assertNotIn("MUST_NOT_BE_RETAINED", json.dumps(evidence))

    def test_missing_malformed_foreign_and_stale_artifacts_are_diagnostic_not_success(self):
        with TemporaryDirectory() as folder:
            paths = fixture(Path(folder))
            missing = change(paths, "7d")
            missing.unlink()
            change(paths, "30d", project="roy", date_to="2026-10-06")
            change(paths, "90d").write_text("{invalid", encoding="utf-8")
            periods = runner.collect_publication_diagnostics("vevo", paths, START, END)["periods"]
            self.assertEqual("missing", periods["7d"]["read_status"])
            self.assertIn("project", periods["30d"]["invalid_fields"])
            self.assertIn("date_range", periods["30d"]["invalid_fields"])
            self.assertEqual("invalid-json", periods["90d"]["read_status"])
            with self.assertRaises(RuntimeError):
                runner.validate_generated_report("vevo", paths, GOOD, START, END)

    def test_embedded_path_escape_never_reads_outside_generation(self):
        with TemporaryDirectory() as folder:
            root = Path(folder)
            paths = fixture(root)
            secret = root / "dashboard_payload_secret.json"
            secret.write_text('{"source_health":{"qa_errors":["PRIVATE_OUTSIDE"]}}')
            full = json.loads(paths["dashboard_payload_latest_json"].read_bytes())
            full["period_switcher"]["_embedded_specs"][0]["report_path"] = str(root / "report_secret.html")
            paths["dashboard_payload_latest_json"].write_text(json.dumps(full))
            evidence = runner.collect_publication_diagnostics("vevo", paths, START, END)
            self.assertEqual("invalid-path", evidence["periods"]["7d"]["read_status"])
            self.assertNotIn("PRIVATE_OUTSIDE", json.dumps(evidence))

    def test_strict_quality_predicate_rejects_malformed_types(self):
        for field, value in [("qa_status", []), ("qa_failure_count", False), ("qa_failure_count", 0.0),
                             ("is_partial", 0), ("qa_errors", None)]:
            with self.subTest(field=field, value=value):
                quality = {**GOOD, field: value}
                self.assertEqual([field], runner.publish_quality_invalid_fields(quality))
                with self.assertRaises(RuntimeError):
                    runner.validate_publish_quality(quality, "synthetic")

    def test_actual_runner_reject_before_uploader_retains_bound_all_periods_for_both_projects(self):
        import export_orders
        for project, policy in host.PROJECTS.items():
            with self.subTest(project=project), TemporaryDirectory() as folder:
                root, s3 = Path(folder).resolve(), PrivateS3()
                paths = fixture(root, project)
                change(paths, "7d", source_health={**GOOD, "qa_status": "critical", "qa_failure_count": 1,
                                                  "qa_errors": ["synthetic private QA detail"]})
                identity = {"release_id": RID, "project": project, "source_commit": "b" * 40,
                            "image_digest": "sha256:" + "c" * 64, "task_arn": "owned-task"}
                prefix = policy.probe_prefix + RID + "/"
                artifact = SimpleNamespace(as_dict=lambda: paths, required_daily_runner_outputs=lambda: {})
                env = {"REPORT_PROJECT": project, "REPORT_SKIP_INVOICES": "true", "REPORT_SKIP_EMAIL": "true",
                       "REPORT_SKIP_CREDITNOTE_STORNO_GUARD": "true", "REPORT_FROM_DATE": START, "REPORT_TO_DATE": END,
                       "REPORT_S3_BUCKET": host.BUCKET, "REPORT_S3_PREFIX": policy.sink}
                with patch.object(host, "ROOT", root), patch.dict(host.os.environ, env), \
                     patch.object(runner, "build_artifact_set", return_value=artifact), \
                     patch.object(runner, "load_dotenv"), patch.object(runner, "load_project_env"), \
                     patch.object(runner, "load_project_settings", return_value={}), \
                     patch.object(runner, "resolve_reporting_defaults", return_value={}), \
                     patch.object(export_orders, "main"), patch.object(host, "isolate_outputs") as publisher:
                    with self.assertRaisesRegex(RuntimeError, "dashboard_payload_7d.json"):
                        host.run_authorized_probe(s3, prefix, identity, host.sha(host.canonical(identity)), policy=policy)
                publisher.assert_not_called()
                self.assertEqual({prefix + "markers/failed.json", prefix + "artifacts/diagnostics/data_quality.json"}, set(s3.objects))
                marker = json.loads(s3.objects[prefix + "markers/failed.json"])
                content = s3.objects[marker["diagnostic"]["key"]]
                evidence = json.loads(content)
                self.assertEqual(host.sha(content), marker["diagnostic"]["sha256"])
                self.assertEqual(GOOD["qa_status"], evidence["qa_status"])
                self.assertEqual(["synthetic private QA detail"], evidence["publication_diagnostics"]["periods"]["7d"]["source_health"]["qa_errors"])
                self.assertNotIn("synthetic private QA detail", json.dumps(marker))
                release = object.__new__(controller.ImageRelease)
                release.prefix, release.report_from_date, release.to_date = prefix, START, END
                release.s3, release.fetch, release.event = s3, Mock(return_value=content), Mock()
                with patch.object(controller, "read_private", return_value=host.canonical(marker)):
                    release.record_probe_failure(identity)
                release.event.assert_called_once()

    def test_wrong_project_or_window_snapshot_cannot_be_retained(self):
        with TemporaryDirectory() as folder:
            root, s3 = Path(folder).resolve(), PrivateS3()
            paths = fixture(root)
            identity = {"project": "vevo", "release_id": RID}
            snapshot = runner.collect_publication_diagnostics("vevo", paths, START, END)
            for field, value in [("project", "roy"), ("report_to_date", "2026-10-06")]:
                bad = copy.deepcopy(snapshot)
                bad[field] = value
                with self.subTest(field=field), patch.object(host, "ROOT", root), self.assertRaisesRegex(RuntimeError, "binding"):
                    host.retain_failure_diagnostics(s3, host.VEVO.probe_prefix + RID + "/", identity,
                        host.sha(host.canonical(identity)), paths["data_quality_json"], from_date=START, to_date=END,
                        failure_type="RuntimeError", publication_diagnostics=bad)
            self.assertEqual({}, s3.objects)


if __name__ == "__main__":
    unittest.main()
