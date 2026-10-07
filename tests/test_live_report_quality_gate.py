"""The last healthy generation must survive any incomplete replacement."""
import json
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import daily_report_runner as runner


HEALTHY = {"is_partial": False, "qa_status": "warning", "qa_failure_count": 0, "qa_errors": []}


def generation(root):
    paths = {}
    specs = []
    for period in ("7d", "30d", "90d", "full"):
        report = root / f"report_{period}.html"
        payload = root / f"dashboard_payload_{period}.json"
        report.write_text("<html>synthetic report</html>", encoding="utf-8")
        payload.write_text(json.dumps({
            "project": "vevo", "date_from": "2026-09-01", "date_to": "2026-10-06",
            "source_health": HEALTHY,
            "period_switcher": {"current_key": period, "_embedded_specs": list(specs)},
        }), encoding="utf-8")
        if period == "full":
            paths.update(report_latest_html=report, dashboard_payload_latest_json=payload)
        else:
            specs.append({"key": period, "report_path": str(report)})
    quality = root / "quality.json"
    quality.write_text(json.dumps(HEALTHY), encoding="utf-8")
    paths["data_quality_json"] = quality
    return paths


class LiveReportQualityGateTests(unittest.TestCase):
    def test_any_bad_period_prevents_every_s3_write(self):
        variants = [None, {}, {**HEALTHY, "is_partial": True},
                    {**HEALTHY, "qa_failure_count": 1}, {**HEALTHY, "qa_status": "critical"},
                    {**HEALTHY, "qa_errors": ["source_reconciliation"]}]
        for bad in variants:
            with self.subTest(bad=bad), TemporaryDirectory() as directory:
                root = Path(directory)
                paths = generation(root)
                child = root / "dashboard_payload_30d.json"
                payload = json.loads(child.read_text(encoding="utf-8"))
                payload["source_health"] = bad
                child.write_text(json.dumps(payload), encoding="utf-8")
                s3 = Mock()
                with patch.dict(os.environ, {"REPORT_S3_BUCKET": "synthetic-bucket"}), \
                     patch.dict(sys.modules, {"boto3": SimpleNamespace(client=lambda *a, **kw: s3)}):
                    with self.assertRaises(RuntimeError):
                        runner.s3_upload_outputs("vevo", paths)
                self.assertEqual([], s3.mock_calls)

    def test_ordinary_cost_warnings_are_publishable(self):
        with TemporaryDirectory() as directory:
            self.assertEqual(8, len(runner._canonical_live_artifact_paths("vevo", generation(Path(directory)))))

    def test_missing_full_quality_blocks_publication(self):
        with TemporaryDirectory() as directory:
            paths = generation(Path(directory))
            paths["data_quality_json"].unlink()
            s3 = Mock()
            with patch.dict(os.environ, {"REPORT_S3_BUCKET": "synthetic-bucket"}), \
                 patch.dict(sys.modules, {"boto3": SimpleNamespace(client=lambda *a, **kw: s3)}):
                with self.assertRaises(RuntimeError):
                    runner.s3_upload_outputs("vevo", paths)
            self.assertEqual([], s3.mock_calls)

    def test_stale_period_date_is_rejected(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            paths = generation(root)
            child = root / "dashboard_payload_7d.json"
            payload = json.loads(child.read_text(encoding="utf-8"))
            payload["date_to"] = "2026-10-05"
            child.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "end mismatch"):
                runner._canonical_live_artifact_paths("vevo", paths)


if __name__ == "__main__":
    unittest.main()
