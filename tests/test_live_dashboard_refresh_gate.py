import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]


class LiveDashboardRefreshGateTests(unittest.TestCase):
    def test_repo_script_keeps_the_host_and_period_hard_gates(self) -> None:
        script = (ROOT_DIR / "scripts" / "live_dashboard_refresh_gate.sh").read_text(encoding="utf-8")

        self.assertIn("set -euo pipefail", script)
        self.assertIn("python daily_report_runner.py", script)
        self.assertIn("python live_dashboard_server.py --host 127.0.0.1 --port 8080", script)
        self.assertIn("http://127.0.0.1:8080/health", script)
        self.assertIn("http://127.0.0.1:8000/marker.json", script)
        self.assertIn("LOCALHOST_LIVE_DASHBOARD_OK", script)
        self.assertIn("LIVE_ARTIFACT_MARKER_OK", script)
        for period in ("7d", "30d", "90d", "full"):
            self.assertIn(period, script)

    def test_deploy_uses_short_repo_script_override(self) -> None:
        workflow = (
            ROOT_DIR / ".github" / "workflows" / "deploy-live-dashboard-apprunner.yml"
        ).read_text(encoding="utf-8")

        self.assertNotIn("REFRESH_SCRIPT", workflow)
        self.assertIn(
            '"command": ["/bin/bash", "/app/scripts/live_dashboard_refresh_gate.sh"]',
            workflow,
        )
        self.assertIn("ECS_REFRESH_OVERRIDES_BYTES:", workflow)
        self.assertIn("8192", workflow)

    def test_ecr_build_watches_the_repo_script(self) -> None:
        workflow = (
            ROOT_DIR / ".github" / "workflows" / "build-and-push-ecr.yml"
        ).read_text(encoding="utf-8")

        self.assertIn("- scripts/live_dashboard_refresh_gate.sh", workflow)


if __name__ == "__main__":
    unittest.main()
