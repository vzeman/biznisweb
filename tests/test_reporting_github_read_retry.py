"""Read-only transport recovery must never waive workflow exclusion."""
import json
import os
import subprocess
import unittest
from unittest.mock import call, patch

from scripts import vevo_report_image_release as release


PATH = "repos/vzeman/biznisweb/actions/runs?status=queued&per_page=100&page=1"
EMPTY = b'{"workflow_runs": []}'


def failure(message):
    return subprocess.CalledProcessError(1, ["gh", "api", "--method", "GET", PATH], stderr=message.encode())


class GitHubReadRetryTests(unittest.TestCase):
    def test_success_is_explicit_get_with_bounded_timeout_and_private_stderr(self):
        with patch.object(release.subprocess, "check_output", return_value=EMPTY) as query, \
             patch("time.sleep") as sleep:
            self.assertEqual({"workflow_runs": []}, release.gh(PATH))
        args, kwargs = query.call_args
        self.assertEqual(["gh", "api"], args[0][:2])
        self.assertIn(PATH, args[0])
        self.assertEqual("GET", args[0][args[0].index("--method") + 1])
        self.assertEqual(45, kwargs["timeout"])
        self.assertEqual(subprocess.PIPE, kwargs["stderr"])
        self.assertEqual(release.ROOT, kwargs["cwd"])
        sleep.assert_not_called()

    def test_each_recognized_transport_failure_retries_only_the_get(self):
        messages = ("net/http: TLS handshake timeout", "read tcp: i/o timeout", "connection reset by peer",
                    "dial tcp: lookup api.github.com: temporary failure in name resolution",
                    "gh: Bad Gateway (HTTP 502)", "gh: Service Unavailable (HTTP 503)",
                    "gh: Gateway Timeout (HTTP 504)")
        for message in messages:
            with self.subTest(message=message), \
                 patch.object(release.subprocess, "check_output", side_effect=[failure(message), EMPTY]) as query, \
                 patch("time.sleep") as sleep:
                self.assertEqual({"workflow_runs": []}, release.gh(PATH))
            self.assertEqual(2, query.call_count)
            self.assertEqual(query.call_args_list[0], query.call_args_list[1])
            sleep.assert_called_once_with(2)

    def test_timeout_and_tls_retry_at_most_three_times_with_fixed_backoff(self):
        timeout = subprocess.TimeoutExpired(["gh", "api", PATH], 45)
        with patch.object(release.subprocess, "check_output", side_effect=[timeout, failure("TLS handshake timeout"), EMPTY]) as query, \
             patch("time.sleep") as sleep:
            self.assertEqual({"workflow_runs": []}, release.gh(PATH))
        self.assertEqual(3, query.call_count)
        self.assertEqual([call(2), call(5)], sleep.call_args_list)
        with patch.object(release.subprocess, "check_output", side_effect=timeout) as query, \
             patch("time.sleep") as sleep, self.assertRaises(subprocess.TimeoutExpired):
            release.gh(PATH)
        self.assertEqual(3, query.call_count)
        self.assertEqual([call(2), call(5)], sleep.call_args_list)

    def test_auth_rate_limit_certificate_and_permanent_failures_do_not_retry(self):
        for message in ("gh: Bad credentials (HTTP 401)", "gh: Forbidden (HTTP 403)",
                        "gh: Not Found (HTTP 404)", "gh: Too Many Requests (HTTP 429)",
                        "x509: certificate signed by unknown authority", "certificate verify failed",
                        "dial tcp: lookup api.github.com: no such host", "gh: Internal Server Error (HTTP 500)",
                        "TLS handshake timeout; gh: Forbidden (HTTP 403)"):
            with self.subTest(message=message), \
                 patch.object(release.subprocess, "check_output", side_effect=failure(message)) as query, \
                 patch("time.sleep") as sleep, self.assertRaises(subprocess.CalledProcessError):
                release.gh(PATH)
            query.assert_called_once()
            sleep.assert_not_called()

    def test_malformed_json_is_not_retried_or_replaced_with_empty_inventory(self):
        with patch.object(release.subprocess, "check_output", return_value=b"<html>not JSON</html>") as query, \
             patch("time.sleep") as sleep, self.assertRaises(json.JSONDecodeError):
            release.gh(PATH)
        query.assert_called_once()
        sleep.assert_not_called()

    def test_recovered_read_still_blocks_a_real_competing_workflow(self):
        competing = json.dumps({"workflow_runs": [{"id": 42, "path": ".github/workflows/deploy-reporting.yml"}]}).encode()
        obj = object.__new__(release.ImageRelease)
        with patch.dict(os.environ, {}, clear=True), \
             patch.object(release.subprocess, "check_output", side_effect=[failure("TLS handshake timeout"), competing]) as query, \
             patch("time.sleep") as sleep, self.assertRaisesRegex(RuntimeError, "competing-workflow"):
            obj.exclusion()
        self.assertEqual(2, query.call_count)
        sleep.assert_called_once_with(2)

    def test_recovered_own_workflow_does_not_hide_another_deployment(self):
        rows = [{"id": 41, "path": ".github/workflows/deploy-reporting.yml"},
                {"id": 42, "path": ".github/workflows/production-reporting-smoke.yml"}]
        obj = object.__new__(release.ImageRelease)
        with patch.dict(os.environ, {"GITHUB_RUN_ID": "41"}), \
             patch.object(release.subprocess, "check_output", return_value=json.dumps({"workflow_runs": rows}).encode()), \
             self.assertRaisesRegex(RuntimeError, "competing-workflow"):
            obj.exclusion()

    def test_second_page_transport_failure_does_not_accept_partial_inventory(self):
        page = json.dumps({"workflow_runs": [{"id": i, "path": ".github/workflows/unit-tests.yml"} for i in range(100)]}).encode()
        obj = object.__new__(release.ImageRelease)
        with patch.object(release.subprocess, "check_output", side_effect=[page] + [failure("TLS handshake timeout")] * 3) as query, \
             patch("time.sleep") as sleep, self.assertRaises(subprocess.CalledProcessError):
            obj.exclusion()
        self.assertEqual(4, query.call_count)
        self.assertTrue(all(any("page=2" in part for part in row.args[0]) for row in query.call_args_list[1:]))
        self.assertEqual([call(2), call(5)], sleep.call_args_list)

    def test_missing_inventory_schema_remains_an_error_after_transport_recovery(self):
        obj = object.__new__(release.ImageRelease)
        with patch.object(release.subprocess, "check_output", side_effect=[failure("TLS handshake timeout"), b"{}"]), \
             patch("time.sleep"), self.assertRaises((KeyError, RuntimeError)):
            obj.exclusion()


if __name__ == "__main__":
    unittest.main()
