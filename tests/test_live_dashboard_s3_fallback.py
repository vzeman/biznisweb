import hashlib
import io
import json
import os
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import call, patch

import live_dashboard_server as server


class LiveDashboardS3FallbackTests(unittest.TestCase):
    def test_generation_manifest_selects_and_verifies_immutable_artifact(self) -> None:
        artifact = b'{"project":"vevo","period_switcher":{"current_key":"30d"}}'
        artifact_key = "daily-reports/vevo/20260715T120000Z/dashboard_payload_30d.json"
        manifest_key = "daily-reports/vevo/latest/generation.json"
        manifest = json.dumps(
            {
                "schema_version": 1,
                "project": "vevo",
                "generation_id": "20260715T120000Z",
                "artifacts": {
                    "dashboard_payload_30d.json": {
                        "key": artifact_key,
                        "sha256": hashlib.sha256(artifact).hexdigest(),
                        "size": len(artifact),
                    }
                },
            }
        ).encode("utf-8")

        class FakeS3:
            def __init__(self) -> None:
                self.calls: list[str] = []

            def get_object(self, *, Bucket, Key):
                self.calls.append(Key)
                if Key == manifest_key:
                    return {"Body": io.BytesIO(manifest)}
                if Key == artifact_key:
                    return {"Body": io.BytesIO(artifact)}
                raise KeyError(Key)

        fake_s3 = FakeS3()
        fake_boto3 = SimpleNamespace(client=lambda *_args, **_kwargs: fake_s3)
        with patch.dict(
            os.environ,
            {
                "LIVE_DASHBOARD_S3_BUCKET_VEVO": "reporting-bucket",
                "LIVE_DASHBOARD_S3_PREFIX_VEVO": "daily-reports/vevo",
            },
            clear=False,
        ), patch.dict(sys.modules, {"boto3": fake_boto3}):
            result = server._latest_s3_artifact_bytes("vevo", "dashboard_payload_30d.json")

        self.assertEqual(artifact, result)
        self.assertEqual([manifest_key, artifact_key], fake_s3.calls)

    def test_missing_generation_manifest_uses_legacy_stable_alias(self) -> None:
        manifest_key = "daily-reports/vevo/latest/generation.json"
        alias_key = "daily-reports/vevo/latest/report_latest.html"
        expected = b"<!doctype html><title>legacy</title>"

        class MissingObjectError(Exception):
            response = {
                "Error": {"Code": "NoSuchKey"},
                "ResponseMetadata": {"HTTPStatusCode": 404},
            }

        class FakeS3:
            def __init__(self) -> None:
                self.calls: list[str] = []

            def get_object(self, *, Bucket, Key):
                self.calls.append(Key)
                if Key == manifest_key:
                    raise MissingObjectError(Key)
                if Key == alias_key:
                    return {"Body": io.BytesIO(expected)}
                raise KeyError(Key)

        fake_s3 = FakeS3()
        fake_boto3 = SimpleNamespace(client=lambda *_args, **_kwargs: fake_s3)
        with patch.dict(
            os.environ,
            {
                "LIVE_DASHBOARD_S3_BUCKET_VEVO": "reporting-bucket",
                "LIVE_DASHBOARD_S3_PREFIX_VEVO": "daily-reports/vevo",
            },
            clear=False,
        ), patch.dict(sys.modules, {"boto3": fake_boto3}):
            result = server._latest_s3_artifact_bytes("vevo", "report_latest.html")

        self.assertEqual(expected, result)
        self.assertEqual([manifest_key, alias_key], fake_s3.calls)

    def test_existing_invalid_generation_manifest_fails_closed(self) -> None:
        manifest_key = "daily-reports/vevo/latest/generation.json"

        class FakeS3:
            def __init__(self) -> None:
                self.calls: list[str] = []

            def get_object(self, *, Bucket, Key):
                self.calls.append(Key)
                if Key == manifest_key:
                    return {"Body": io.BytesIO(b"not-json")}
                return {"Body": io.BytesIO(b"mutable-alias-must-not-be-served")}

        fake_s3 = FakeS3()
        fake_boto3 = SimpleNamespace(client=lambda *_args, **_kwargs: fake_s3)
        with patch.dict(
            os.environ,
            {
                "LIVE_DASHBOARD_S3_BUCKET_VEVO": "reporting-bucket",
                "LIVE_DASHBOARD_S3_PREFIX_VEVO": "daily-reports/vevo",
            },
            clear=False,
        ), patch.dict(sys.modules, {"boto3": fake_boto3}):
            result = server._latest_s3_artifact_bytes("vevo", "report_latest.html")

        self.assertIsNone(result)
        self.assertEqual([manifest_key], fake_s3.calls)

    def test_live_period_href_map_replaces_filesystem_navigation_base(self) -> None:
        original = b"<!doctype html><html><head><title>VEVO</title></head><body></body></html>"
        rendered = server.inject_live_period_href_map(original, "vevo").decode("utf-8")

        self.assertIn("window.__PERIOD_HREF_BASE_MAP__", rendered)
        for period in ("7d", "30d", "90d", "full"):
            self.assertIn(f'"{period}": "/report/vevo?period={period}"', rendered)
        self.assertLess(rendered.index("window.__PERIOD_HREF_BASE_MAP__"), rendered.index("</head>"))

    def test_full_payload_uses_s3_when_local_artifact_is_absent(self) -> None:
        expected = b'{"project":"vevo"}'
        with patch.object(server, "resolve_period_payload_path", return_value=None), patch.object(
            server,
            "_latest_s3_artifact_bytes",
            return_value=expected,
        ) as s3_read:
            result = server.read_period_dashboard_payload_bytes("vevo", "full")

        self.assertEqual(expected, result)
        s3_read.assert_called_once_with("vevo", "dashboard_payload_latest.json")

    def test_full_report_uses_s3_when_local_artifact_is_absent(self) -> None:
        expected = b"<!doctype html><title>VEVO</title>"
        with patch.object(server, "resolve_period_report_path", return_value=None), patch.object(
            server,
            "_latest_s3_artifact_bytes",
            return_value=expected,
        ) as s3_read:
            result = server.read_period_report_bytes("vevo", "full")

        self.assertEqual(expected, result)
        s3_read.assert_called_once_with("vevo", "report_latest.html")

    def test_each_non_full_period_uses_its_exact_stable_s3_artifacts(self) -> None:
        for period in ("7d", "30d", "90d"):
            with self.subTest(period=period), patch.object(
                server,
                "resolve_period_payload_path",
                return_value=None,
            ), patch.object(
                server,
                "resolve_period_report_path",
                return_value=None,
            ), patch.object(
                server,
                "_latest_s3_artifact_bytes",
                side_effect=[b"payload", b"report"],
            ) as s3_read:
                payload = server.read_period_dashboard_payload_bytes("vevo", period)
                report = server.read_period_report_bytes("vevo", period)

            self.assertEqual(b"payload", payload)
            self.assertEqual(b"report", report)
            self.assertEqual(
                [
                    call("vevo", f"dashboard_payload_{period}.json"),
                    call("vevo", f"report_{period}.html"),
                ],
                s3_read.call_args_list,
            )

    def test_unknown_period_does_not_silently_serve_full_s3_artifact(self) -> None:
        with patch.object(server, "resolve_period_payload_path", return_value=None), patch.object(
            server,
            "_latest_s3_artifact_bytes",
        ) as s3_read:
            result = server.read_period_dashboard_payload_bytes("vevo", "unexpected")

        self.assertIsNone(result)
        s3_read.assert_not_called()


if __name__ == "__main__":
    unittest.main()
