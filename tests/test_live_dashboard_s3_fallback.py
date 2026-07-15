import unittest
from unittest.mock import patch

import live_dashboard_server as server


class LiveDashboardS3FallbackTests(unittest.TestCase):
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

    def test_non_full_period_does_not_silently_serve_full_s3_artifact(self) -> None:
        with patch.object(server, "resolve_period_payload_path", return_value=None), patch.object(
            server,
            "_latest_s3_artifact_bytes",
        ) as s3_read:
            result = server.read_period_dashboard_payload_bytes("vevo", "30d")

        self.assertIsNone(result)
        s3_read.assert_not_called()


if __name__ == "__main__":
    unittest.main()
