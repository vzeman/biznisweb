import io
import json
import re
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import live_dashboard_maintenance as maintenance
import scripts.dashboard_maintenance_state as controller
from live_dashboard_server import build_roy_operations_dashboard_html


NOW = datetime(2026, 7, 22, 8, 0, tzinfo=timezone.utc)


class FakeS3Error(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class LiveDashboardMaintenanceTests(unittest.TestCase):
    def test_finite_lease_is_active_then_expires_automatically(self) -> None:
        state = maintenance.build_active_maintenance_state(
            {},
            project="roy",
            operation_id="gha:123:1",
            reason_code="deployment",
            ttl_seconds=900,
            now=NOW,
        )

        active = maintenance.normalize_maintenance_state(state, project="roy", now=NOW)
        expired = maintenance.normalize_maintenance_state(
            state,
            project="roy",
            now=NOW + timedelta(seconds=901),
        )

        self.assertTrue(active["active"])
        self.assertEqual(900, active["remaining_seconds"])
        self.assertFalse(expired["active"])
        self.assertTrue(expired["expired"])
        self.assertTrue(
            maintenance.public_maintenance_status(expired, project="roy", now=NOW + timedelta(seconds=901))["expired"]
        )

    def test_invalid_or_unbounded_active_state_never_locks_dashboard(self) -> None:
        for raw in (
            {"active": True},
            {
                "active": True,
                "project": "roy",
                "operation_id": "bad owner with spaces",
                "reason_code": "deployment",
                "phase": "deploying",
                "expires_at": "2099-01-01T00:00:00Z",
                "hard_expires_at": "2099-01-01T00:00:00Z",
            },
            {
                "active": True,
                "project": "vevo",
                "operation_id": "gha:123:1",
                "reason_code": "deployment",
                "phase": "deploying",
                "expires_at": "2099-01-01T00:00:00Z",
                "hard_expires_at": "2099-01-01T00:00:00Z",
            },
        ):
            self.assertFalse(
                maintenance.normalize_maintenance_state(raw, project="roy", now=NOW)["active"]
            )

    def test_tampered_far_future_lease_never_locks_dashboard(self) -> None:
        raw = maintenance.build_active_maintenance_state(
            {},
            project="roy",
            operation_id="gha:123:1",
            reason_code="deployment",
            now=NOW,
        )
        raw["expires_at"] = "2099-01-01T00:00:00Z"
        raw["hard_expires_at"] = "2099-01-01T00:00:00Z"

        normalized = maintenance.normalize_maintenance_state(raw, project="roy", now=NOW)

        self.assertFalse(normalized["active"])
        self.assertTrue(normalized["expired"])

    def test_persisted_internal_fields_cannot_override_active_state(self) -> None:
        raw = maintenance.build_active_maintenance_state(
            {},
            project="roy",
            operation_id="gha:123:1",
            reason_code="deployment",
            now=NOW,
        )
        raw["stored_active"] = False

        normalized = maintenance.normalize_maintenance_state(raw, project="roy", now=NOW)

        self.assertFalse(normalized["schema_valid"])
        self.assertTrue(normalized["stored_active"])

    def test_owner_conflict_and_owner_only_clear(self) -> None:
        state = maintenance.build_active_maintenance_state(
            {},
            project="roy",
            operation_id="gha:123:1",
            reason_code="deployment",
            now=NOW,
        )
        with self.assertRaises(maintenance.MaintenanceConflict):
            maintenance.build_active_maintenance_state(
                state,
                project="roy",
                operation_id="gha:456:1",
                reason_code="deployment",
                now=NOW + timedelta(minutes=1),
            )
        with self.assertRaises(maintenance.MaintenanceConflict):
            maintenance.build_inactive_maintenance_state(
                state,
                project="roy",
                operation_id="gha:456:1",
                now=NOW + timedelta(minutes=1),
            )

        cleared = maintenance.build_inactive_maintenance_state(
            state,
            project="roy",
            operation_id="gha:123:1",
            now=NOW + timedelta(minutes=1),
        )
        self.assertFalse(
            maintenance.normalize_maintenance_state(
                cleared,
                project="roy",
                now=NOW + timedelta(minutes=1),
            )["active"]
        )

    def test_renew_preserves_start_and_absolute_deadline(self) -> None:
        initial = maintenance.build_active_maintenance_state(
            {},
            project="roy",
            operation_id="gha:123:1",
            reason_code="deployment",
            ttl_seconds=600,
            max_lifetime_seconds=1_800,
            now=NOW,
        )
        renewed = maintenance.build_active_maintenance_state(
            initial,
            project="roy",
            operation_id="gha:123:1",
            reason_code="deployment",
            phase="deploying",
            ttl_seconds=600,
            now=NOW + timedelta(minutes=5),
        )

        self.assertEqual(initial["started_at"], renewed["started_at"])
        self.assertEqual(initial["hard_expires_at"], renewed["hard_expires_at"])
        self.assertEqual("renew", renewed["events"][-1]["action"])

    def test_remote_missing_is_inactive_but_other_errors_fail_closed(self) -> None:
        class MissingS3:
            def get_object(self, **_kwargs):
                raise FakeS3Error("NoSuchKey")

        class BrokenS3:
            def get_object(self, **_kwargs):
                raise FakeS3Error("AccessDenied")

        settings = {"live_dashboard_artifacts": {}}
        with (
            patch.object(
                maintenance,
                "maintenance_s3_location",
                return_value=("bucket", "daily-reports/roy-sk/operations/maintenance.json", "eu-central-1"),
            ),
            patch("boto3.client", return_value=MissingS3()),
        ):
            state = maintenance.load_dashboard_maintenance_state(
                "roy",
                settings,
                require_configured_remote=True,
            )
        self.assertFalse(state["active"])
        self.assertTrue(state["_storage_object_absent"])

        with (
            patch.object(
                maintenance,
                "maintenance_s3_location",
                return_value=("bucket", "daily-reports/roy-sk/operations/maintenance.json", "eu-central-1"),
            ),
            patch("boto3.client", return_value=BrokenS3()),
            self.assertRaises(maintenance.MaintenanceStorageError),
        ):
            maintenance.load_dashboard_maintenance_state(
                "roy",
                settings,
                require_configured_remote=True,
            )

    def test_remote_missing_fails_closed_after_capability_is_enabled(self) -> None:
        class CapabilityS3:
            def get_object(self, **kwargs):
                if kwargs["Key"].endswith("maintenance.json"):
                    raise FakeS3Error("NoSuchKey")
                return {
                    "Body": io.BytesIO(
                        json.dumps(
                            {
                                "marker": "dashboard-maintenance-capability-v1",
                                "project": "roy",
                            }
                        ).encode("utf-8")
                    )
                }

        with (
            patch.object(
                maintenance,
                "maintenance_s3_location",
                return_value=("bucket", "daily-reports/roy-sk/operations/maintenance.json", "eu-central-1"),
            ),
            patch("boto3.client", return_value=CapabilityS3()),
            self.assertRaises(maintenance.MaintenanceStorageError),
        ):
            maintenance.load_dashboard_maintenance_state(
                "roy",
                {},
                require_configured_remote=True,
            )

    def test_required_remote_configuration_cannot_fall_back_to_local_state(self) -> None:
        with (
            patch.object(maintenance, "maintenance_s3_location", return_value=None),
            self.assertRaises(maintenance.MaintenanceStorageError),
        ):
            maintenance.load_dashboard_maintenance_state(
                "roy",
                {},
                require_configured_remote=True,
            )

    def test_remote_active_state_exposes_only_public_status(self) -> None:
        raw = maintenance.build_active_maintenance_state(
            {},
            project="roy",
            operation_id="gha:123:1",
            reason_code="deployment",
            now=datetime.now(timezone.utc),
        )

        class FakeS3:
            def get_object(self, **_kwargs):
                return {
                    "Body": io.BytesIO(json.dumps(raw).encode("utf-8")),
                    "ETag": '"lease-etag"',
                }

        with (
            patch.object(
                maintenance,
                "maintenance_s3_location",
                return_value=("bucket", "daily-reports/roy-sk/operations/maintenance.json", "eu-central-1"),
            ),
            patch("boto3.client", return_value=FakeS3()),
        ):
            state = maintenance.load_dashboard_maintenance_state(
                "roy",
                {},
                require_configured_remote=True,
            )
        public = maintenance.public_maintenance_status(state, project="roy")
        self.assertTrue(public["active"])
        self.assertEqual("dashboard-maintenance-v1", public["marker"])
        self.assertNotIn("_storage_etag", public)
        self.assertNotIn("events", public)

    def test_invalid_existing_remote_state_fails_closed(self) -> None:
        class InvalidS3:
            def get_object(self, **_kwargs):
                return {
                    "Body": io.BytesIO(
                        json.dumps(
                            {
                                "active": True,
                                "project": "roy",
                                "operation_id": "gha:123:1",
                                "reason_code": "deployment",
                                "phase": "deploying",
                                "expires_at": "2099-01-01T00:00:00Z",
                                "hard_expires_at": "2099-01-01T00:00:00Z",
                            }
                        ).encode("utf-8")
                    ),
                    "ETag": '"invalid"',
                }

        with (
            patch.object(
                maintenance,
                "maintenance_s3_location",
                return_value=("bucket", "daily-reports/roy-sk/operations/maintenance.json", "eu-central-1"),
            ),
            patch("boto3.client", return_value=InvalidS3()),
            self.assertRaises(maintenance.MaintenanceStorageError),
        ):
            maintenance.load_dashboard_maintenance_state(
                "roy",
                {},
                require_configured_remote=True,
            )

    def test_status_error_uses_a_bounded_ui_lock_without_faking_an_active_lease(self) -> None:
        status = maintenance.maintenance_fail_closed_status("roy", "S3 unavailable", now=NOW)

        self.assertFalse(status["active"])
        self.assertTrue(status["status_error"])
        self.assertEqual("2026-07-22T08:15:00Z", status["ui_lock_expires_at"])

    def test_active_html_blocks_pointer_keyboard_and_polls_status(self) -> None:
        raw = maintenance.build_active_maintenance_state(
            {},
            project="roy",
            operation_id="gha:123:1",
            reason_code="deployment",
            now=datetime.now(timezone.utc),
        )
        html = build_roy_operations_dashboard_html(
            "roy",
            maintenance.public_maintenance_status(raw, project="roy"),
        )

        self.assertIn('data-marker="roy-maintenance-overlay"', html)
        self.assertIn('data-maintenance-active="true"', html)
        self.assertIn('id="dashboardRoot"', html)
        self.assertIn('inert aria-hidden="true"', html)
        self.assertIn("pointer-events:none", html)
        self.assertIn("/maintenance", html)
        self.assertIn("MAINTENANCE_POLL_MS = 5000", html)
        self.assertIn("controller.abort()", html)
        self.assertIn("signal:controller.signal", html)
        self.assertIn("boundedStatusErrorDeadline", html)
        self.assertIn("statusErrorLocksDashboard", html)
        self.assertIn("visibilitychange", html)
        self.assertIn("dashboard-maintenance-v1", html)

    def test_inactive_html_is_unlocked_on_first_paint(self) -> None:
        html = build_roy_operations_dashboard_html(
            "roy",
            maintenance.public_maintenance_status({}, project="roy"),
        )
        self.assertIn('data-maintenance-active="false"', html)
        self.assertIn('id="maintenanceOverlay"', html)
        self.assertIn('tabindex="-1" hidden', html)
        self.assertNotIn('id="dashboardRoot" data-marker="roy-operations-dashboard" inert', html)

    def test_placeholder_like_message_cannot_corrupt_bootstrap_json(self) -> None:
        raw = maintenance.build_active_maintenance_state(
            {},
            project="roy",
            operation_id="gha:123:1",
            reason_code="deployment",
            message="Token __MAINTENANCE_MESSAGE__ a __BOOTSTRAP_JSON__ \\",
            now=datetime.now(timezone.utc),
        )
        html = build_roy_operations_dashboard_html(
            "roy",
            maintenance.public_maintenance_status(raw, project="roy"),
        )

        match = re.search(
            r'<script id="roy-operations-bootstrap" type="application/json">(.*?)</script>',
            html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(match)
        bootstrap = json.loads(match.group(1))
        self.assertEqual(raw["message"], bootstrap["maintenance"]["message"])

    def test_controller_retries_a_conditional_write_conflict(self) -> None:
        state = maintenance.build_active_maintenance_state(
            {},
            project="roy",
            operation_id="gha:123:1",
            reason_code="deployment",
            now=datetime.now(timezone.utc),
        )
        reads = [({}, '"old"'), ({}, '"new"'), (state, '"saved"')]
        put_calls = []

        def fake_read(**_kwargs):
            return reads.pop(0)

        def fake_put(_state, **kwargs):
            put_calls.append(kwargs)
            if len(put_calls) == 1:
                raise controller.AwsCliError("conflict", output="PreconditionFailed status code: 412")

        with (
            patch.object(controller, "read_s3_state", side_effect=fake_read),
            patch.object(controller, "conditional_put_s3_state", side_effect=fake_put),
        ):
            saved = controller.mutate_s3_state(
                lambda _current: state,
                lambda raw: raw.get("operation_id") == "gha:123:1",
                bucket="bucket",
                key="key",
                region="eu-central-1",
            )

        self.assertEqual("gha:123:1", saved["operation_id"])
        self.assertEqual(2, len(put_calls))
        self.assertEqual('"old"', put_calls[0]["expected_etag"])
        self.assertEqual('"new"', put_calls[1]["expected_etag"])


if __name__ == "__main__":
    unittest.main()
