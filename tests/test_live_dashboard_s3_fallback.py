import hashlib
import io
import json
import os
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

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


class LiveDashboardProjectRoutingTests(unittest.TestCase):
    vevo_origin = "https://2mhmsmgq3m.eu-central-1.awsapprunner.com"
    roy_origin = "https://qvfzvh82c3.eu-central-1.awsapprunner.com"

    def handler(self, path):
        handler = server.LiveDashboardHandler.__new__(server.LiveDashboardHandler)
        handler.path = path
        handler.headers = {}
        handler.wfile = io.BytesIO()
        handler.send_response = Mock()
        handler.send_header = Mock()
        handler.end_headers = Mock()
        return handler

    @patch.dict(os.environ, {"REPORT_PROJECT": "roy"}, clear=True)
    def test_old_vevo_report_bookmark_redirects_without_reading_roy_artifacts(self):
        handler = self.handler("/report/vevo?period=full&next=https://untrusted.invalid")
        with patch.object(server, "read_period_report_bytes") as read:
            handler.do_GET()
        handler.send_response.assert_called_once_with(302)
        self.assertIn(call("Location", self.vevo_origin + "/report/vevo?period=full"), handler.send_header.call_args_list)
        self.assertEqual(b"", handler.wfile.getvalue())
        read.assert_not_called()

    @patch.dict(os.environ, {"REPORT_PROJECT": "vevo"}, clear=True)
    def test_reverse_navigation_preserves_selected_period(self):
        handler = self.handler("/dashboard/roy?period=30d")
        handler.do_GET()
        self.assertIn(call("Location", self.roy_origin + "/dashboard/roy?period=30d"), handler.send_header.call_args_list)

    @patch.dict(os.environ, {"REPORT_PROJECT": "roy"}, clear=True)
    def test_own_report_is_served_without_redirect(self):
        handler = self.handler("/report/roy?period=7d")
        with patch.object(server, "read_period_report_bytes", return_value=b"<html><head></head></html>") as read:
            handler.do_GET()
        handler.send_response.assert_called_once_with(200)
        read.assert_called_once_with("roy", "7d")
        self.assertNotIn("Location", [c.args[0] for c in handler.send_header.call_args_list])

    @patch.dict(os.environ, {"REPORT_PROJECT": "roy"}, clear=True)
    def test_foreign_api_returns_navigation_hint_without_reading_data(self):
        handler = self.handler("/api/vevo/latest?period=full")
        with patch.object(server, "read_period_dashboard_payload_bytes") as read:
            handler.do_GET()
        handler.send_response.assert_called_once_with(409)
        self.assertEqual(self.vevo_origin + "/dashboard/vevo", json.loads(handler.wfile.getvalue())["dashboard_url"])
        read.assert_not_called()

    @patch.dict(os.environ, {"REPORT_PROJECT": "roy", "LIVE_DASHBOARD_AUTH_USER": "user", "LIVE_DASHBOARD_AUTH_PASSWORD": "test-password"}, clear=True)
    def test_redirect_does_not_bypass_existing_authentication(self):
        handler = self.handler("/report/vevo?period=full")
        handler.do_GET()
        handler.send_response.assert_called_once_with(401)

    @patch.dict(os.environ, {"REPORT_PROJECT": "roy"}, clear=True)
    def test_index_links_and_project_switcher_use_project_origins(self):
        index = server.build_index_html(["roy", "vevo"])
        self.assertIn("href='/report/roy'", index)
        self.assertIn("href='" + self.vevo_origin + "/report/vevo'", index)
        self.assertNotIn("HTML report: missing", index)
        dashboard = server.build_live_dashboard_html(["roy", "vevo"], "roy", "30d")
        self.assertIn('"project_origins": {"roy": "", "vevo": "' + self.vevo_origin + '"}', dashboard)
        self.assertIn("window.location.assign(target.href)", dashboard)

    @patch.dict(os.environ, {}, clear=True)
    def test_local_multi_project_server_keeps_relative_navigation(self):
        self.assertEqual("/report/vevo", server.dashboard_project_href("vevo", "report"))

    @patch.dict(os.environ, {"REPORT_PROJECT": "roy"}, clear=True)
    def test_untrusted_url_shapes_are_not_used_as_redirect_targets(self):
        for origin in ["http://example.test", "https://user:pass@example.test", "https://example.test/?next=x", "https://example.test/\r\nLocation: x"]:
            with self.subTest(origin=origin), patch.object(server, "load_project_settings", return_value={"live_dashboard": {"public_origin": origin}}):
                self.assertEqual("", server.remote_dashboard_origin("vevo"))


if __name__ == "__main__":
    unittest.main()
