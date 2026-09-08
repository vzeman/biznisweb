from __future__ import annotations

from contextlib import ExitStack, redirect_stdout, redirect_stderr
from datetime import UTC, datetime
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest
from unittest.mock import Mock, patch

from scripts import growthbook_aa_health_log_io as health


WORKFLOW = (health.ROOT / health.WORKFLOW).read_text(encoding="utf-8")
SHA = "a" * 40
TASK = "b" * 32
NOW = datetime(2026, 9, 8, 13, 0, tzinfo=UTC)


class HealthLogCliTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.runner = Path(self.temporary.name).resolve()
        self.root = self.runner / "vevo-aa-infra-123456789"
        self.root.mkdir()
        self.env = dict(
            GITHUB_ACTIONS="true", RUNNER_ENVIRONMENT="github-hosted",
            GITHUB_REPOSITORY="vzeman/biznisweb", GITHUB_REF="refs/heads/main",
            GITHUB_WORKFLOW_REF=f"vzeman/biznisweb/{health.WORKFLOW}@refs/heads/main",
            RUN_INFRA_HEALTH="true", GITHUB_WORKSPACE=str(health.ROOT),
            RUNNER_TEMP=str(self.runner), TEMP_HEALTH_DIR=str(self.root),
            GITHUB_RUN_ID="123456789", GITHUB_SHA=SHA,
            HEALTH_PHASE="natural_reconciliation_verified", AWS_REGION="eu-central-1",
            CHECKED_DUE_LOCAL="2026-09-08T03:45:00+02:00",
            RECONCILIATION_LOG_GROUP="/test/health",
            RECONCILIATION_LOG_STREAM="test/reconciliation/" + TASK,
            RECONCILIATION_TASK_ID=TASK,
        )

    def run_main(self, args=(), *, env=None, fetch=None, head=SHA):
        output, errors = io.StringIO(), io.StringIO()
        with patch.dict(os.environ, env or self.env, clear=True), \
                patch.object(health, "cli_fetch", fetch or Mock()) as client, \
                patch.object(health.subprocess, "run", return_value=Mock(stdout=head.encode())) as git, \
                patch.object(health, "datetime") as clock, \
                redirect_stdout(output), redirect_stderr(errors):
            clock.now.return_value = NOW
            clock.combine.side_effect = datetime.combine
            result = health.main(list(args))
        self.assertEqual("", errors.getvalue())
        return result, output.getvalue(), client, git

    def test_cli_completes_before_writing_and_emits_no_raw_messages(self):
        marker = dict(timestamp=int(NOW.timestamp() * 1000) - 1,
                      ingestionTime=int(NOW.timestamp() * 1000) + 1, message="synthetic-secret-marker")
        pages = iter([dict(events=[], nextForwardToken="first"),
                      dict(events=[marker], nextForwardToken="last"),
                      dict(events=[], nextForwardToken="last")])
        def fetch(request):
            self.assertFalse((self.root / "task-logs.json").exists())
            return json.dumps(next(pages)).encode()
        result, output, _, git = self.run_main(fetch=Mock(side_effect=fetch))
        self.assertEqual(0, result)
        self.assertIn("complete=true", output)
        self.assertNotIn(marker["message"], output)
        self.assertEqual({"events": [marker]}, json.loads((self.root / "task-logs.json").read_bytes()))
        self.assertEqual(["task-logs.json"], [p.name for p in self.root.iterdir()])
        self.assertEqual(["git", "rev-parse", "HEAD"], git.call_args.args[0])

    def test_managed_gates_reject_before_git_or_aws(self):
        for field, value in dict(GITHUB_ACTIONS="false", RUNNER_ENVIRONMENT="self-hosted",
                                GITHUB_REPOSITORY="other/repo", GITHUB_REF="refs/heads/topic",
                                GITHUB_WORKFLOW_REF="other", RUN_INFRA_HEALTH="false",
                                GITHUB_WORKSPACE=str(self.runner), GITHUB_RUN_ID="../x",
                                TEMP_HEALTH_DIR=str(self.runner)).items():
            with self.subTest(field=field):
                env = dict(self.env, **{field: value})
                result, output, client, git = self.run_main(env=env)
                self.assertEqual(2, result)
                client.assert_not_called()
                git.assert_not_called()
                self.assertIn("raw-emitted=false", output)
                self.assertEqual([], list(self.root.iterdir()))

    def test_source_identity_and_window_fail_before_aws(self):
        for changed in ({"GITHUB_SHA": "c" * 40}, {"AWS_REGION": "wrong"},
                        {"HEALTH_PHASE": "waiting_for_first_natural_run"},
                        {"CHECKED_DUE_LOCAL": "2026-09-07T03:45:00+02:00"},
                        {"RECONCILIATION_TASK_ID": "c" * 32}):
            with self.subTest(changed=changed):
                result, _, client, _ = self.run_main(env=dict(self.env, **changed))
                self.assertEqual(2, result)
                client.assert_not_called()
                self.assertEqual([], list(self.root.iterdir()))

    def test_cli_failure_never_emits_exception_or_partial_output(self):
        for error in (RuntimeError("synthetic-sensitive-response"),
                      subprocess.TimeoutExpired(["aws", "synthetic-token"], 35,
                                                output=b"synthetic-sensitive-response")):
            result, output, _, _ = self.run_main(fetch=Mock(side_effect=error))
            self.assertEqual(2, result)
            self.assertNotIn("synthetic", output)
            self.assertEqual([], list(self.root.iterdir()))

    def test_cli_emits_only_allowlisted_local_failure_codes(self):
        for error, code in ((health.HealthLogError("page-limit"), "page-limit"),
                            (health.HealthLogError("synthetic-private-value"), "unclassified-error"),
                            (ValueError("page-limit"), "unclassified-error")):
            with patch.object(health, "managed_scope", side_effect=error):
                result, output, client, git = self.run_main()
            self.assertEqual(2, result)
            self.assertEqual(f"PRODUCTION_AA_INFRA_LOG_IO_STOPPED:code={code}:raw-emitted=false\n", output)
            client.assert_not_called()
            git.assert_not_called()

    def test_existing_output_is_never_overwritten(self):
        file = self.root / "task-logs.json"
        file.write_text("existing synthetic evidence", encoding="utf-8")
        result, _, client, _ = self.run_main()
        self.assertEqual(2, result)
        client.assert_not_called()
        self.assertEqual("existing synthetic evidence", file.read_text())

    def test_failure_cleanup_needs_no_task_health_evidence_git_or_aws(self):
        (self.root / "failed-response.json").write_text("synthetic", encoding="utf-8")
        env = {k: v for k, v in self.env.items() if not k.startswith("RECONCILIATION_")
               and k not in {"HEALTH_PHASE", "GITHUB_SHA", "CHECKED_DUE_LOCAL", "AWS_REGION"}}
        result, output, client, git = self.run_main(["--cleanup-only"], env=env)
        self.assertEqual(0, result)
        self.assertIn("raw-retained=false", output)
        self.assertFalse(self.root.exists())
        client.assert_not_called()
        git.assert_not_called()

    def test_single_page_adapter_uses_fixed_read_only_cli_with_suppressed_output(self):
        request = {"logGroupName": "synthetic", "nextToken": "private-test-token"}
        with patch.object(health.subprocess, "run", return_value=Mock(returncode=0, stdout=b"{}")) as run:
            self.assertEqual(b"{}", health.cli_fetch(request))
        args, options = run.call_args.args[0], run.call_args.kwargs
        self.assertEqual(["aws", "logs", "get-log-events"], args[:3])
        self.assertEqual(request, json.loads(args[args.index("--cli-input-json") + 1]))
        self.assertIn("--no-paginate", args)
        self.assertIn("--no-cli-pager", args)
        self.assertEqual("eu-central-1", args[args.index("--region") + 1])
        self.assertEqual(subprocess.PIPE, options["stdout"])
        self.assertEqual(subprocess.DEVNULL, options["stderr"])
        self.assertEqual(35, options["timeout"])
        self.assertNotIn("shell", options)
        self.assertNotIn("--profile", args)
        self.assertNotIn("--endpoint-url", args)

    def test_read_window_uses_local_due_and_fixed_capture_across_dst(self):
        for due, now in (
            ("2026-09-08T03:45:00+02:00", NOW),
            ("2026-10-25T03:45:00+01:00", datetime(2026, 10, 25, 8, 0, tzinfo=UTC)),
            ("2026-10-24T03:45:00+02:00", datetime(2026, 10, 25, 2, 30, tzinfo=UTC)),
            ("2027-03-28T03:45:00+02:00", datetime(2027, 3, 28, 8, 0, tzinfo=UTC)),
        ):
            with self.subTest(due=due):
                start, end = health.read_window(due, now)
                self.assertEqual(int(datetime.fromisoformat(due).timestamp() * 1000), start)
                self.assertEqual(int(now.timestamp() * 1000), end)


class HealthLogWorkflowIntegrationTests(unittest.TestCase):
    def test_original_marker_parity_and_hash_logic_with_complete_pages(self):
        step = WORKFLOW.split("- name: Verify natural success marker", 1)[1]
        block = textwrap.dedent(step.split("python - <<'PY'\n", 1)[1].split("          PY", 1)[0])
        marker = "GROWTHBOOK_SCHEDULED_RECONCILIATION_OK:synthetic=true"
        summary = {"mode": "publish", "device_facts": 1, "performance_facts": 2,
                   "quality_reports": 3, "event_partitions": 40,
                   "published": {"device_facts": 1, "performance_facts": 2, "quality_reports": 3}}
        summary_text = json.dumps(summary, sort_keys=True, separators=(",", ":"))
        def row(message):
            return dict(message=message, timestamp=10, ingestionTime=11)
        for messages, valid in (([marker, summary_text], True), ([summary_text], False),
                                ([marker, marker, summary_text], False),
                                ([marker, summary_text, summary_text], False),
                                ([marker, summary_text.replace('"event_partitions":40', '"event_partitions":39')], False)):
            with self.subTest(valid=valid, message_count=len(messages)), tempfile.TemporaryDirectory() as directory:
                path = Path(directory)
                pages = [dict(events=[], nextForwardToken="initial")]
                pages += [dict(events=[row(message)], nextForwardToken=str(i)) for i, message in enumerate(messages)]
                pages += [dict(events=[], nextForwardToken=str(len(messages) - 1))]
                fetch = Mock(side_effect=[json.dumps(p).encode() for p in pages])
                payload = health.read_complete_log(fetch, group="test", stream="test/" + TASK,
                                                   task_id=TASK, start_ms=0, end_ms=100)
                (path / "task-logs.json").write_text(json.dumps(payload), encoding="utf-8")
                # The unchanged inline block uses open(); close its test handles
                # explicitly without altering the production acceptance source.
                with patch.dict(os.environ, TEMP_HEALTH_DIR=str(path)), redirect_stdout(io.StringIO()), ExitStack() as handles:
                    namespace = {"open": lambda *a, **kw: handles.enter_context(open(*a, **kw))}
                    if valid:
                        exec(compile(block, "original-marker-parity", "exec"), namespace)
                        control = json.loads((path / "natural-control.json").read_bytes())
                        self.assertEqual(hashlib.sha256(marker.encode()).hexdigest(), control["success_marker_sha256"])
                        self.assertEqual(hashlib.sha256((summary_text + "\n").encode()).hexdigest(), control["publish_summary_sha256"])
                    else:
                        with self.assertRaises(SystemExit):
                            exec(compile(block, "original-marker-parity", "exec"), namespace)
                        self.assertFalse((path / "natural-control.json").exists())

    def test_always_cleanup_cannot_resurrect_failed_evidence_or_upload(self):
        selected = WORKFLOW.index("PRODUCTION_AA_INFRA_RUNTIME_OK:")
        read = WORKFLOW.index("python scripts/growthbook_aa_health_log_io.py\n")
        cleanup = WORKFLOW.index("- name: Remove every temporary AWS response")
        validate = WORKFLOW.index("- name: Revalidate canonical evidence after successful cleanup")
        upload = WORKFLOW.index("- name: Upload only canonical sanitized")
        self.assertLess(selected, read)
        self.assertLess(read, cleanup)
        self.assertLess(cleanup, validate)
        self.assertLess(validate, upload)
        self.assertIn("if: ${{ always() && env.RUN_INFRA_HEALTH == 'true' }}", WORKFLOW[cleanup:validate])
        self.assertIn("--cleanup-only", WORKFLOW[cleanup:validate])
        self.assertNotIn("rm -rf", WORKFLOW[cleanup:validate])
        self.assertNotIn("always()", WORKFLOW[validate:])
        self.assertNotIn("continue-on-error", WORKFLOW)
        self.assertEqual(1, WORKFLOW.count("uses: actions/upload-artifact@"))


if __name__ == "__main__":
    unittest.main()
