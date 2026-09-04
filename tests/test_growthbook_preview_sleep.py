from __future__ import annotations

import ast
import pathlib
import unittest

from scripts.inspect_growthbook_preview_sleep import canonical, digest, require, schedule_fingerprint, summarize_changes, validate_command

ROOT = pathlib.Path(__file__).resolve().parents[1]


class PreviewSleepPreflightTests(unittest.TestCase):
    def test_change_summary_excludes_physical_ids_and_property_values(self):
        payload = {"Changes": [{"ResourceChange": {"PhysicalResourceId": "FORBIDDEN", "LogicalResourceId": "CollectorService", "Action": "Modify", "Details": [{"CausingEntity": "FORBIDDEN", "Target": {"Name": "DesiredCount", "BeforeValue": "FORBIDDEN", "AfterValue": "FORBIDDEN"}}]}}]}
        summary = canonical(summarize_changes(payload))
        self.assertNotIn("FORBIDDEN", summary)
        self.assertNotIn("BeforeValue", summary)
        self.assertIn("DesiredCount", summary)

    def test_image_default_command_serializations(self):
        for container in ({}, {"command": None}, {"command": []}, {"entryPoint": []}):
            self.assertEqual(validate_command(container), "image_default")
        self.assertEqual(validate_command({"command": ["python", "-m", "growthbook_collector.server"]}), "explicit_server_command")

    def test_any_other_command_or_entrypoint_is_rejected(self):
        for container in ({"command": ["sh"]}, {"command": ""}, {"entryPoint": ["sh"]}):
            with self.assertRaises(ValueError):
                validate_command(container)

    def test_only_read_only_aws_calls_exist(self):
        tree = ast.parse((ROOT / "scripts/inspect_growthbook_preview_sleep.py").read_text(encoding="utf-8"))
        calls = {n.func.attr for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
        forbidden = {"update_service", "stop_task", "run_task", "update_schedule", "delete_stack", "create_change_set", "execute_change_set", "get_object", "start_query_execution", "filter_log_events"}
        self.assertFalse(calls & forbidden)
        self.assertIn("describe_tasks", calls)

    def test_canonical_hash_is_key_order_independent(self):
        self.assertEqual(digest({"a": 1, "b": 2}), digest({"b": 2, "a": 1}))
        self.assertTrue(canonical({}).endswith("\n"))

    def test_schedule_timestamp_does_not_hide_configuration_drift(self):
        self.assertEqual(schedule_fingerprint({"State": "ENABLED", "CreationDate": "a"}), schedule_fingerprint({"State": "ENABLED", "CreationDate": "b"}))
        self.assertNotEqual(schedule_fingerprint({"State": "ENABLED"}), schedule_fingerprint({"State": "DISABLED"}))

    def test_fail_closed(self):
        with self.assertRaisesRegex(ValueError, "closed"):
            require(False, "closed")

    def test_workflow_is_manual_main_only_and_single_artifact(self):
        source = (ROOT / ".github/workflows/inspect-vevo-growthbook-preview-sleep.yml").read_text(encoding="utf-8")
        self.assertIn("github.ref == 'refs/heads/main' && inputs.confirm_inspect == true", source)
        self.assertNotIn("schedule:", source)
        self.assertEqual(source.count("actions/upload-artifact@"), 1)
        self.assertLess(source.index("tests.test_growthbook_preview_sleep"), source.index("aws-actions/configure-aws-credentials"))


if __name__ == "__main__":
    unittest.main()
