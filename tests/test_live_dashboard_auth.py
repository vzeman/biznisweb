import base64
import os
import unittest
from unittest.mock import patch

from live_dashboard_server import is_authorized_basic_header, live_dashboard_auth_credentials


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


if __name__ == "__main__":
    unittest.main()
