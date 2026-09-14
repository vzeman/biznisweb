from io import BytesIO
import json
import unittest
from unittest.mock import Mock, patch

from scripts import observe_creditnote_release as observer


class ObserverTests(unittest.TestCase):
    def test_actual_log_pages_retain_both_host_marker_and_summary(self):
        logs = Mock()
        logs.get_log_events.side_effect = [
            {"events": [{"message": 'CREDITNOTE_AUTOMATION_HOST_OK {"dry_run":true}'}], "nextForwardToken": "a"},
            {"events": [{"message": 'CREDITNOTE_STANDALONE_SUMMARY {"ok":true}'}], "nextForwardToken": "b"},
            {"events": [], "nextForwardToken": "b"}]
        definition = {"containerDefinitions": [{"name": "reporting", "logConfiguration": {
            "options": {"awslogs-group": "/ecs/test", "awslogs-stream-prefix": "ecs"}}}]}
        actual = observer.read_markers(logs, definition, "task/123")
        self.assertEqual([{"dry_run": True}], actual["CREDITNOTE_AUTOMATION_HOST_OK"])
        self.assertEqual([{"ok": True}], actual["CREDITNOTE_STANDALONE_SUMMARY"])
        self.assertEqual("ecs/reporting/123", logs.get_log_events.call_args.kwargs["logStreamName"])

    def test_log_pagination_cycle_rejects_incomplete_observation(self):
        logs = Mock()
        logs.get_log_events.side_effect = [{"events": [], "nextForwardToken": token} for token in ("a", "b", "a")]
        definition = {"containerDefinitions": [{"name": "reporting", "logConfiguration": {
            "options": {"awslogs-group": "/ecs/test", "awslogs-stream-prefix": "ecs"}}}]}
        with self.assertRaisesRegex(ValueError, "pagination-invalid"):
            observer.read_markers(logs, definition, "task/123")

    def test_failed_semantic_readiness_never_publishes_audit(self):
        session, s3 = Mock(), Mock()
        session.client.side_effect = lambda name: s3 if name == "s3" else Mock()
        managed = {"commit": "a" * 40, "expected_schedules": {}, "hosts": [], "candidate_definitions": {
            "vevo": {"definition": {"containerDefinitions": [{"image": "image@sha256:" + "b" * 64}]}}}}
        s3.get_object.return_value = {"Body": BytesIO(json.dumps(managed).encode()), "ServerSideEncryption": "AES256"}
        key = "data/roy/order-automation/creditnote-deployments/" + "a" * 40 + "/" + "c" * 32 + ".json"
        with patch.object(observer.readiness, "require_source"), \
                patch.object(observer.readiness.binding, "require_private_bucket"), \
                patch.object(observer.readiness, "read_proof", return_value=managed), \
                patch.object(observer.readiness, "gh", return_value={"status": "completed", "conclusion": "success", "head_sha": "a" * 40}), \
                patch.object(observer.readiness, "prepare", side_effect=ValueError("invalid-six-host-proof")):
            with self.assertRaisesRegex(ValueError, "six-host"):
                observer.observe(session, managed_key=key, run_id=123, primary={}, publish=True)
        s3.put_object.assert_not_called()


if __name__ == "__main__":
    unittest.main()
