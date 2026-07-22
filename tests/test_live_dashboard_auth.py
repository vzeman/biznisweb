import base64
import http.client
import json
import os
import threading
import unittest
from http.server import ThreadingHTTPServer
from unittest.mock import patch

from live_dashboard_server import (
    LiveDashboardHandler,
    is_authorized_basic_header,
    is_trusted_roy_operations_action_request,
    live_dashboard_auth_credentials,
)


def basic_header(user: str, password: str) -> str:
    token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
    return f"Basic {token}"


class LiveDashboardAuthTests(unittest.TestCase):
    @patch.dict(os.environ, {}, clear=True)
    def test_auth_is_disabled_when_credentials_are_unset(self) -> None:
        self.assertIsNone(live_dashboard_auth_credentials())
        self.assertTrue(is_authorized_basic_header(None, None))

    @patch.dict(
        os.environ,
        {"LIVE_DASHBOARD_AUTH_USER": "vevo", "LIVE_DASHBOARD_AUTH_PASSWORD": "secret"},
        clear=True,
    )
    def test_basic_auth_accepts_valid_credentials(self) -> None:
        credentials = live_dashboard_auth_credentials()
        self.assertEqual(("vevo", "secret"), credentials)
        self.assertTrue(is_authorized_basic_header(basic_header("vevo", "secret"), credentials))

    @patch.dict(
        os.environ,
        {"LIVE_DASHBOARD_AUTH_USER": "roy21", "LIVE_DASHBOARD_AUTH_PASSWORD": "tajne-heslo-žľš"},
        clear=True,
    )
    def test_basic_auth_accepts_utf8_credentials(self) -> None:
        credentials = live_dashboard_auth_credentials()
        self.assertTrue(
            is_authorized_basic_header(basic_header("roy21", "tajne-heslo-žľš"), credentials)
        )
        self.assertFalse(
            is_authorized_basic_header(basic_header("roy21", "tajne-heslo-zls"), credentials)
        )

    @patch.dict(
        os.environ,
        {"LIVE_DASHBOARD_AUTH_USER": "vevo", "LIVE_DASHBOARD_AUTH_PASSWORD": "secret"},
        clear=True,
    )
    def test_basic_auth_rejects_invalid_or_malformed_credentials(self) -> None:
        credentials = live_dashboard_auth_credentials()
        self.assertFalse(is_authorized_basic_header(None, credentials))
        self.assertFalse(is_authorized_basic_header("Bearer token", credentials))
        self.assertFalse(is_authorized_basic_header("Basic not-base64", credentials))
        self.assertFalse(is_authorized_basic_header(basic_header("vevo", "wrong"), credentials))
        self.assertFalse(is_authorized_basic_header(basic_header("vevo", "wröng"), credentials))
        self.assertFalse(is_authorized_basic_header(basic_header("other", "secret"), credentials))

    @patch.dict(os.environ, {"LIVE_DASHBOARD_AUTH_USER": "vevo"}, clear=True)
    def test_partial_auth_configuration_rejects_all_requests(self) -> None:
        credentials = live_dashboard_auth_credentials()
        self.assertEqual(("", ""), credentials)
        self.assertFalse(is_authorized_basic_header(basic_header("vevo", "anything"), credentials))

    def test_inventory_restock_write_requires_json_and_same_origin_action_header(self) -> None:
        trusted = {
            "content_type": "application/json; charset=utf-8",
            "action_header": "inventory-restock-preference",
            "sec_fetch_site": "same-origin",
            "origin": "https://dashboard.example.test",
            "host": "dashboard.example.test",
        }

        self.assertTrue(is_trusted_roy_operations_action_request(**trusted))
        self.assertFalse(
            is_trusted_roy_operations_action_request(
                **{**trusted, "content_type": "application/x-www-form-urlencoded"}
            )
        )
        self.assertFalse(
            is_trusted_roy_operations_action_request(
                **{**trusted, "action_header": ""}
            )
        )
        self.assertFalse(
            is_trusted_roy_operations_action_request(
                **{**trusted, "sec_fetch_site": "cross-site"}
            )
        )
        self.assertFalse(
            is_trusted_roy_operations_action_request(
                **{**trusted, "origin": "https://evil.example", "host": "dashboard.example.test"}
            )
        )

    def test_inventory_restock_route_rejects_untrusted_request_and_calls_action(self) -> None:
        class QuietHandler(LiveDashboardHandler):
            def log_message(self, _format, *_args) -> None:
                return

        server = ThreadingHTTPServer(("127.0.0.1", 0), QuietHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        try:
            with (
                patch("live_dashboard_server.available_projects", return_value=["roy"]),
                patch("live_dashboard_server.live_dashboard_auth_credentials", return_value=None),
                patch(
                    "live_dashboard_server.get_live_dashboard_maintenance_status",
                    return_value={"active": False, "status_error": False},
                ),
                patch(
                    "live_dashboard_server.exclude_inventory_restock_alert",
                    return_value={"ok": True, "project": "roy", "sku": "SKU-1", "excluded": True},
                ) as exclude,
            ):
                connection = http.client.HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/operations/roy/inventory-restock/SKU-1/exclude",
                    body='{"product":"Test"}',
                    headers={"Content-Type": "application/json"},
                )
                rejected = connection.getresponse()
                rejected.read()
                self.assertEqual(403, rejected.status)
                exclude.assert_not_called()

                connection.request(
                    "POST",
                    "/api/operations/roy/inventory-restock/SKU-1/exclude",
                    body='{"product":"Test"}',
                    headers={
                        "Content-Type": "application/json",
                        "X-ROY-Operations-Action": "inventory-restock-preference",
                        "Sec-Fetch-Site": "same-origin",
                    },
                )
                accepted = connection.getresponse()
                accepted_payload = accepted.read().decode("utf-8")
                self.assertEqual(200, accepted.status)
                self.assertIn('"excluded": true', accepted_payload)
                exclude.assert_called_once_with("roy", "SKU-1", product="Test")
                connection.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_maintenance_status_route_requires_authentication_and_returns_status(self) -> None:
        class QuietHandler(LiveDashboardHandler):
            def log_message(self, _format, *_args) -> None:
                return

        maintenance = {
            "marker": "dashboard-maintenance-v1",
            "version": 1,
            "project": "roy",
            "active": True,
            "status_error": False,
            "operation_id": "gh-123-1",
            "reason_code": "deployment",
            "phase": "deploying",
            "message": "Nasadzujeme aktualizáciu.",
            "expires_at": "2026-07-22T12:15:00Z",
            "remaining_seconds": 600,
        }
        server = ThreadingHTTPServer(("127.0.0.1", 0), QuietHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        try:
            with (
                patch("live_dashboard_server.available_projects", return_value=["roy"]),
                patch(
                    "live_dashboard_server.live_dashboard_auth_credentials",
                    return_value=("roy21", "secret"),
                ),
                patch(
                    "live_dashboard_server.get_live_dashboard_maintenance_status",
                    return_value=maintenance,
                ) as get_status,
            ):
                unauthorized_connection = http.client.HTTPConnection(host, port, timeout=5)
                unauthorized_connection.request("GET", "/api/operations/roy/maintenance")
                unauthorized = unauthorized_connection.getresponse()
                unauthorized.read()
                self.assertEqual(401, unauthorized.status)
                self.assertIn("Basic", unauthorized.getheader("WWW-Authenticate") or "")
                get_status.assert_not_called()
                unauthorized_connection.close()

                authorized_connection = http.client.HTTPConnection(host, port, timeout=5)
                authorized_connection.request(
                    "GET",
                    "/api/operations/roy/maintenance",
                    headers={"Authorization": basic_header("roy21", "secret")},
                )
                accepted = authorized_connection.getresponse()
                payload = json.loads(accepted.read().decode("utf-8"))
                self.assertEqual(200, accepted.status)
                self.assertEqual("dashboard-maintenance-v1", payload["marker"])
                self.assertTrue(payload["active"])
                self.assertEqual("gh-123-1", payload["operation_id"])
                get_status.assert_called_once_with("roy")
                authorized_connection.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_active_maintenance_blocks_roy_post_before_action(self) -> None:
        class QuietHandler(LiveDashboardHandler):
            def log_message(self, _format, *_args) -> None:
                return

        maintenance = {
            "marker": "dashboard-maintenance-v1",
            "project": "roy",
            "active": True,
            "status_error": False,
            "operation_id": "gh-123-1",
            "phase": "deploying",
            "message": "Nasadzujeme aktualizáciu.",
            "remaining_seconds": 600,
        }
        server = ThreadingHTTPServer(("127.0.0.1", 0), QuietHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        try:
            with (
                patch("live_dashboard_server.available_projects", return_value=["roy"]),
                patch("live_dashboard_server.live_dashboard_auth_credentials", return_value=None),
                patch(
                    "live_dashboard_server.get_live_dashboard_maintenance_status",
                    return_value=maintenance,
                ),
                patch("live_dashboard_server.exclude_inventory_restock_alert") as exclude,
            ):
                connection = http.client.HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/operations/roy/inventory-restock/SKU-1/exclude",
                    body='{"product":"Test"}',
                    headers={
                        "Content-Type": "application/json",
                        "X-ROY-Operations-Action": "inventory-restock-preference",
                        "Sec-Fetch-Site": "same-origin",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(423, response.status)
                self.assertEqual("gh-123-1", payload["maintenance"]["operation_id"])
                self.assertIn("maintenance is active", payload["error"])
                exclude.assert_not_called()
                connection.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_maintenance_state_failure_blocks_roy_post_before_action(self) -> None:
        class QuietHandler(LiveDashboardHandler):
            def log_message(self, _format, *_args) -> None:
                return

        server = ThreadingHTTPServer(("127.0.0.1", 0), QuietHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        try:
            with (
                patch("live_dashboard_server.available_projects", return_value=["roy"]),
                patch("live_dashboard_server.live_dashboard_auth_credentials", return_value=None),
                patch(
                    "live_dashboard_server.get_live_dashboard_maintenance_status",
                    side_effect=RuntimeError("S3 unavailable"),
                ),
                patch("live_dashboard_server.exclude_inventory_restock_alert") as exclude,
            ):
                connection = http.client.HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/operations/roy/inventory-restock/SKU-1/exclude",
                    body='{"product":"Test"}',
                    headers={
                        "Content-Type": "application/json",
                        "X-ROY-Operations-Action": "inventory-restock-preference",
                        "Sec-Fetch-Site": "same-origin",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(503, response.status)
                self.assertTrue(payload["maintenance"]["status_error"])
                self.assertIn("status is unavailable", payload["error"])
                exclude.assert_not_called()
                connection.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
