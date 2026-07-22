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
        self.assertIn("LOCALHOST_DASHBOARD_MAINTENANCE_OK", script)
        self.assertIn("EXPECTED_DASHBOARD_MAINTENANCE_OPERATION_ID", script)
        self.assertIn('data-maintenance-active="true"', script)
        self.assertIn('inert aria-hidden="true"', script)
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
        self.assertIn("- scripts/dashboard_maintenance_state.py", workflow)
        self.assertIn("- live_dashboard_maintenance.py", workflow)
        self.assertIn("tests.test_live_dashboard_maintenance", workflow)

    def test_roy_deploy_maintenance_wraps_runtime_mutations_and_clears_before_write_smoke(self) -> None:
        workflow = (
            ROOT_DIR / ".github" / "workflows" / "deploy-live-dashboard-apprunner.yml"
        ).read_text(encoding="utf-8")

        start = "maintenance_controller start"
        fargate_mutation = 'aws ecs run-task "${RUN_ARGS[@]}"'
        scheduler_mutation = 'aws scheduler update-schedule "${UPDATE_SCHEDULE_ARGS[@]}"'
        app_runner_mutation = "aws apprunner update-service"
        stop = "maintenance_controller stop"
        restock_smoke = 'RESTOCK_SENTINEL="CODEX-RESTOCK-SMOKE-'
        self.assertIn("dashboard-maintenance-capability-v1", workflow)
        self.assertIn("--ttl-seconds 900", workflow)
        self.assertIn("--max-lifetime-seconds 9000", workflow)
        self.assertIn("CURRENT_APP_RUNNER_MAINTENANCE_ACTIVE_OK", workflow)
        self.assertIn("APP_RUNNER_MAINTENANCE_ACTIVE_OK", workflow)
        self.assertIn("APP_RUNNER_MAINTENANCE_WRITE_BLOCK_OK:http=423", workflow)
        self.assertIn("APP_RUNNER_MAINTENANCE_INACTIVE_OK", workflow)
        self.assertIn("EXPECTED_DASHBOARD_MAINTENANCE_OPERATION_ID", workflow)
        self.assertLess(
            workflow.index("DASHBOARD_MAINTENANCE_ACTIVE=true"),
            workflow.index(start),
        )
        self.assertLess(workflow.index(start), workflow.index(fargate_mutation))
        self.assertLess(workflow.index(start), workflow.index(scheduler_mutation))
        self.assertLess(workflow.index(start), workflow.index(app_runner_mutation))
        self.assertLess(workflow.rindex(stop), workflow.index(restock_smoke))
        self.assertNotIn("trap 'cleanup_restock_sentinel || true' EXIT", workflow)

    def test_deploy_repairs_scheduler_passrole_before_schedule_promotion(self) -> None:
        workflow = (
            ROOT_DIR / ".github" / "workflows" / "deploy-live-dashboard-apprunner.yml"
        ).read_text(encoding="utf-8")

        helper_call = "python scripts/reporting_scheduler_passrole.py"
        policy_ready = "REPORTING_SCHEDULER_PASSROLE_READY:"
        schedule_update = 'aws scheduler update-schedule "${UPDATE_SCHEDULE_ARGS[@]}"'
        self.assertIn(helper_call, workflow)
        self.assertIn('"ReportingSchedulePassRole-${PROJECT}"', workflow)
        self.assertIn("aws iam put-role-policy", workflow)
        self.assertIn("aws iam get-role-policy", workflow)
        self.assertIn("aws iam simulate-principal-policy", workflow)
        self.assertIn("role drifted from", workflow)
        self.assertIn("changed during deployment", workflow)
        unconditional_verify = "schedule-passrole-verified.json"
        conditional_promotion = 'if [[ "${SKIP_ARTIFACT_REFRESH:-false}"'
        self.assertIn(unconditional_verify, workflow)
        self.assertLess(
            workflow.index(unconditional_verify), workflow.index(policy_ready)
        )
        policy_ready_index = workflow.index(policy_ready)
        self.assertLess(
            policy_ready_index,
            workflow.index(conditional_promotion, policy_ready_index),
        )
        self.assertLess(workflow.index(helper_call), workflow.index(policy_ready))
        self.assertLess(workflow.index(policy_ready), workflow.index(schedule_update))


if __name__ == "__main__":
    unittest.main()
