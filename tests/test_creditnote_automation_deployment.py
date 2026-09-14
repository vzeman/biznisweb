import copy
import hashlib
import io
import json
import os
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

from scripts import deploy_creditnote_automations as deploy
from scripts.creditnote_automation_host_gate import verify_summary
from scripts.reporting_guard_identity_probe import SKIP, probe_payload


ACCOUNT = "123456789012"
CLUSTER = f"arn:aws:ecs:eu-central-1:{ACCOUNT}:cluster/vevo-reporting-cluster"
DIGEST = "sha256:" + "a" * 64
IMAGE = f"{ACCOUNT}.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting@{DIGEST}"


def schedule(name, project="roy", service=None):
    return {"Name": name, "GroupName": "default", "State": "ENABLED", "Description": "synthetic existing schedule",
            "ScheduleExpression": "cron(30 1 * * ? *)", "ScheduleExpressionTimezone": "Europe/Bratislava",
            "FlexibleTimeWindow": {"Mode": "OFF"}, "Target": {"Arn": CLUSTER,
            "RoleArn": f"arn:aws:iam::{ACCOUNT}:role/existing-scheduler",
            "RetryPolicy": {"MaximumRetryAttempts": 1}, "EcsParameters": {
                "TaskDefinitionArn": f"arn:aws:ecs:eu-central-1:{ACCOUNT}:task-definition/{service or project + '-invoice-daily'}:8",
                "TaskCount": 1, "LaunchType": "FARGATE", "NetworkConfiguration": {"awsvpcConfiguration": {
                    "Subnets": ["subnet-synthetic"], "SecurityGroups": ["sg-synthetic"], "AssignPublicIp": "ENABLED"}}}}}


def summary(project="roy", dry_run=False):
    return {"ok": True, "project": project, "enabled": True, "dry_run": dry_run,
            "creditnote_scan_complete": True, "skipped_locked": False, "failed_orders": 0,
            "review_required_orders": 0, "audit_error_orders": 0, "updated_orders": 0}


def source_definition(project="roy"):
    return {"family": f"{project}-invoice-daily", "networkMode": "awsvpc", "cpu": "256", "memory": "512",
            "taskRoleArn": f"arn:aws:iam::{ACCOUNT}:role/existing-task",
            "executionRoleArn": f"arn:aws:iam::{ACCOUNT}:role/existing-execution", "containerDefinitions": [{
                "name": "reporting", "image": IMAGE, "command": ["python", "invoice_runner.py", "--project", project],
                "environment": [{"name": "REPORT_INVOICE_DRY_RUN", "value": "true"}],
                "secrets": [{"name": name,
                            "valueFrom": f"arn:aws:secretsmanager:eu-central-1:{ACCOUNT}:secret:{project}/reporting/runtime-env-ABC123:{name}::"}
                            for name in sorted(deploy.GUARD_SECRET_NAMES)] + [
                            {"name": "EMAIL_PASSWORD", "valueFrom": "synthetic-unrelated-reference"}]}]}


class Missing(Exception):
    def __init__(self, code="ResourceNotFoundException"):
        self.response = {"Error": {"Code": code}}


class FakeScheduler:
    def __init__(self, values):
        self.values = copy.deepcopy(values)
        self.writes = []
        self.fail_call = None
        self.ambiguous = False

    def get_schedule(self, Name):
        if Name not in self.values:
            raise Missing()
        return copy.deepcopy(self.values[Name])

    def update_schedule(self, **request):
        self.writes.append(copy.deepcopy(request))
        fail = len(self.writes) == self.fail_call
        if not fail or self.ambiguous:
            self.values[request["Name"]] = copy.deepcopy(request)
        if fail:
            raise TimeoutError("synthetic private transport detail")

    create_schedule = update_schedule


def controller():
    value = object.__new__(deploy.CreditnoteDeployment)
    value.account, value.cluster, value.commit = ACCOUNT, CLUSTER, "b" * 40
    value.originals = {name: schedule(name, "vevo" if name.startswith("vevo") else "roy") for name in deploy.PROTECTED}
    for project in deploy.PROJECTS:
        value.originals[f"{project}-daily-report-email"] = schedule(f"{project}-daily-report-email", project, f"{project}-reporting-daily")
    value.old_guards = {deploy.family(project): None for project in deploy.PROJECTS}
    value.scheduler = FakeScheduler(value.originals)
    value.expected = {name: copy.deepcopy(row) for name, row in value.originals.items()}
    value.expected.update(value.old_guards)
    value.evidence = {"hosts": []}
    value.save = Mock()
    value.check_sources = Mock()
    return value


class CreditnoteSummaryAndProbeTests(unittest.TestCase):
    def test_new_role_waits_for_propagation_and_retains_attempt_ownership(self):
        value = controller()
        value.key = "private/" + "c" * 32 + ".json"
        value.session, value.sleep = Mock(), Mock()
        iam = value.session.client.return_value
        name = "BiznisWebCreditnoteGuard-roy-" + "c" * 12
        arn = f"arn:aws:iam::{ACCOUNT}:role/{name}"
        trust = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow",
            "Principal": {"Service": "ecs-tasks.amazonaws.com"}, "Action": "sts:AssumeRole",
            "Condition": {"StringEquals": {"aws:SourceAccount": ACCOUNT}}}]}
        role = {"Role": {"Arn": arn, "AssumeRolePolicyDocument": trust}}
        iam.get_role.side_effect = [Missing("NoSuchEntity"), role, role]
        iam.create_role.return_value = role
        policy = {"Version": "2012-10-17", "Statement": []}
        iam.get_role_policy.return_value = {"PolicyDocument": policy}
        self.assertEqual(arn, value.create_role("BiznisWebCreditnoteGuard-roy", "ecs-tasks.amazonaws.com", policy))
        self.assertEqual([((10,), {})] * 6, value.sleep.call_args_list)
        self.assertEqual(name, iam.create_role.call_args.kwargs["RoleName"])
        self.assertEqual([arn], value.evidence["created_roles"])
        self.assertEqual(2, iam.get_role_policy.call_count)
        iam.delete_role.assert_not_called()

    def test_existing_attempt_role_is_not_adopted_or_overwritten(self):
        value = controller()
        value.key = "private/" + "d" * 32 + ".json"
        value.session, value.sleep = Mock(), Mock()
        iam = value.session.client.return_value
        iam.get_role.return_value = {"Role": {"Arn": "existing"}}
        with self.assertRaisesRegex(RuntimeError, "already-exists"):
            value.create_role("BiznisWebCreditnoteGuard-roy", "ecs-tasks.amazonaws.com", {})
        iam.create_role.assert_not_called()
        iam.put_role_policy.assert_not_called()
        value.sleep.assert_not_called()

    def test_host_gate_requires_explicit_success_and_each_failure_counter(self):
        verify_summary(summary(dry_run=True), "roy", dry_run=True)
        for fields in ({"ok": False}, {"ok": None}, {"audit_error_orders": 1}, {"audit_error_orders": None},
                       {"failed_orders": False}, {"review_required_orders": 1}, {"skipped_locked": None},
                       {"creditnote_scan_complete": False}, {"updated_orders": 1}, {"project": "vevo"}):
            with self.subTest(fields=fields), self.assertRaises(RuntimeError):
                verify_summary({**summary(dry_run=True), **fields}, "roy", dry_run=True)

    def test_hash_verified_source_parser_executes_without_business_import_or_run(self):
        root = Path(__file__).resolve().parents[1]
        for project, (revision, _, _) in deploy.REPORT_PINS.items():
            with self.subTest(project=project):
                source = (root / "daily_report_runner.py").read_bytes()
                task = {"Family": f"{project}-reporting-daily", "Revision": str(revision), "TaskARN": "synthetic-task",
                        "Containers": [{"Networks": [{"IPv4Addresses": ["192.0.2.10"]}]}]}
                payload = probe_payload(task, project, revision, source, hashlib.sha256(source).hexdigest(),
                                        {SKIP: "true", "REPORT_PROJECT": project}, "/app")
                self.assertTrue(payload["inline_guard_skipped"])
                self.assertFalse(payload["business_runner_started"])
                self.assertEqual("192.0.2.10", payload["private_ip"])
                for environment, path, digest in (({SKIP: "false", "REPORT_PROJECT": project}, "/app", hashlib.sha256(source).hexdigest()),
                                                  ({SKIP: "true", "REPORT_PROJECT": "other"}, "/app", hashlib.sha256(source).hexdigest()),
                                                  ({SKIP: "true", "REPORT_PROJECT": project}, "/other", hashlib.sha256(source).hexdigest()),
                                                  ({SKIP: "true", "REPORT_PROJECT": project}, "/app", "wrong")):
                    with self.assertRaises(RuntimeError):
                        probe_payload(task, project, revision, source, digest, environment, path)

    def test_injected_probe_stays_within_ecs_override_limit_with_existing_env(self):
        source = (Path(__file__).resolve().parents[1] / "scripts/reporting_guard_identity_probe.py").read_text()
        command = ["python", "-c", f"exec(compile({source!r}, '<verified-report-identity-probe>', 'exec'), {{'__name__': '__main__'}})",
                   "--project", "vevo", "--revision", "33", "--source-sha256", "a" * 64]
        payload = {"containerOverrides": [{"name": "reporting", "command": command,
                    "environment": [{"name": SKIP, "value": "true"}]}]}
        self.assertLess(len(json.dumps(payload).encode()), 8192)


class CreditnoteDefinitionTests(unittest.TestCase):
    def test_named_environment_order_is_semantic_and_original_evidence_unchanged(self):
        original = deploy.guard_definition(source_definition(), "roy", IMAGE, "role", "bucket", "prefix")
        observed = copy.deepcopy(original)
        observed["containerDefinitions"][0]["environment"].reverse()
        frozen_original, frozen_observed = copy.deepcopy(original), copy.deepcopy(observed)
        self.assertNotEqual(original, observed)
        self.assertEqual(deploy.comparable_definition(original), deploy.comparable_definition(observed))
        self.assertEqual(frozen_original, original)
        self.assertEqual(frozen_observed, observed)

    def test_named_environment_duplicate_or_malformed_entries_fail_closed(self):
        valid = [{"name": "A", "value": ""}, {"name": "B", "value": "value"}]
        for rows in (valid + [valid[0]], [{"name": "A", "value": 1}], [{"name": "", "value": "x"}],
                     [{"name": "A"}], [{"name": "A", "value": "x", "extra": "x"}], [None], None):
            with self.subTest(rows=rows), self.assertRaisesRegex(RuntimeError, "environment-invalid"):
                deploy.comparable_definition({"containerDefinitions": [{"environment": rows}]})

    def test_comparison_rejects_changed_missing_and_order_sensitive_runtime_fields(self):
        original = deploy.guard_definition(source_definition(), "roy", IMAGE, "role", "bucket", "prefix")
        expected = deploy.comparable_definition(original)
        for field in ("command", "secrets", "environment-value", "environment-missing", "image"):
            observed = copy.deepcopy(original)
            container = observed["containerDefinitions"][0]
            if field in {"command", "secrets"}:
                container[field].reverse()
            elif field == "environment-value":
                container["environment"][0]["value"] += "-changed"
            elif field == "environment-missing":
                container["environment"].pop()
            else:
                container["image"] = "unreviewed-image"
            with self.subTest(field=field):
                self.assertNotEqual(expected, deploy.comparable_definition(observed))

    def test_frozen_report_readback_accepts_only_environment_reordering(self):
        value = controller()
        original = deploy.guard_definition(source_definition(), "roy", IMAGE, "role", "bucket", "prefix")
        value.definitions = {"synthetic-arn": original}
        value.ecs = Mock()
        observed = copy.deepcopy(original)
        observed["containerDefinitions"][0]["environment"].reverse()
        value.ecs.describe_task_definition.return_value = {"taskDefinition": observed}
        deploy.CreditnoteDeployment.check_sources(value)
        observed["containerDefinitions"][0]["command"].reverse()
        with self.assertRaisesRegex(RuntimeError, "frozen-report-definition-drift"):
            deploy.CreditnoteDeployment.check_sources(value)

    def test_historical_source_fetch_works_in_shallow_checkout_only_for_pinned_commit(self):
        commit = deploy.REPORT_PINS["roy"][2]
        for available in (True, False):
            with patch.object(deploy.subprocess, "run", return_value=Mock(returncode=0 if available else 1)) as run, \
                    patch.object(deploy.subprocess, "check_output", return_value=b"synthetic source"):
                self.assertEqual(hashlib.sha256(b"synthetic source").hexdigest(), deploy.report_source_hash(commit))
                self.assertEqual(1 if available else 2, run.call_count)
                if not available:
                    self.assertEqual(["git", "fetch", "--no-tags", "origin", commit], run.call_args.args[0])
        with self.assertRaisesRegex(RuntimeError, "not-pinned"):
            deploy.report_source_hash("c" * 40)

    def test_report_delta_is_only_skip_environment_and_preserves_input(self):
        original = schedule("vevo-daily-report-email", "vevo", "vevo-reporting-daily")
        payload = {"taskRoleArn": "synthetic-existing-role", "containerOverrides": [{"name": "reporting",
                   "command": ["python", "daily_report_runner.py", "--project", "vevo"],
                   "environment": [{"name": "KEEP", "value": "synthetic-existing-value"}]}]}
        original["Target"]["Input"] = json.dumps(payload)
        snapshot = copy.deepcopy(original)
        actual = deploy.report_override(original)
        changed_input = json.loads(actual["Target"].pop("Input"))
        unchanged = copy.deepcopy(original)
        unchanged["Target"].pop("Input")
        self.assertEqual(unchanged, actual)
        changed_input["containerOverrides"][0]["environment"].pop()
        self.assertEqual(payload, changed_input)
        self.assertEqual(snapshot, original)

    def test_report_duplicate_skip_or_container_is_rejected(self):
        for containers in ([{"name": "reporting"}, {"name": "reporting"}],
                           [{"name": "reporting", "environment": [{"name": SKIP}, {"name": SKIP}]}]):
            original = schedule("roy-daily-report-email")
            original["Target"]["Input"] = json.dumps({"containerOverrides": containers})
            with self.assertRaises(RuntimeError):
                deploy.report_override(original)

    def test_guard_has_immutable_image_only_required_api_and_native_secrets_and_correct_storage(self):
        source = source_definition()
        prior = copy.deepcopy(source)
        actual = deploy.guard_definition(source, "roy", IMAGE, "new-scoped-role", "synthetic-bucket", "daily-reports/roy")
        self.assertEqual(prior, source)
        self.assertEqual("roy-creditnote-storno-guard", actual["family"])
        container = actual["containerDefinitions"][0]
        self.assertEqual(["python", "creditnote_storno_runner.py", "--project", "roy"], container["command"])
        self.assertEqual(prior["containerDefinitions"][0]["secrets"][:-1], container["secrets"])
        self.assertNotIn("REPORT_INVOICE_DRY_RUN", {row["name"] for row in container["environment"]})
        for bad_image in ("latest", IMAGE.replace("@sha256:", ":sha256:")):
            with self.assertRaisesRegex(RuntimeError, "immutable"):
                deploy.guard_definition(source, "roy", bad_image, "role", "bucket", "prefix")
        source["containerDefinitions"][0]["entryPoint"] = ["synthetic-unreviewed-entrypoint"]
        with self.assertRaisesRegex(RuntimeError, "entrypoint"):
            deploy.guard_definition(source, "roy", IMAGE, "role", "bucket", "prefix")

    def test_guard_rejects_missing_duplicate_and_empty_required_references(self):
        for name in deploy.GUARD_SECRET_NAMES:
            for defect in ("missing", "duplicate", "empty", "whitespace", "not-text"):
                with self.subTest(name=name, defect=defect):
                    source = source_definition()
                    secrets = source["containerDefinitions"][0]["secrets"]
                    target = next(row for row in secrets if row["name"] == name)
                    if defect == "missing":
                        secrets.remove(target)
                    elif defect == "duplicate":
                        secrets.append(copy.deepcopy(target))
                    else:
                        target["valueFrom"] = {"empty": "", "whitespace": " ", "not-text": None}[defect]
                    with self.assertRaisesRegex(RuntimeError, "guard-.*secret"):
                        deploy.guard_definition(source, "roy", IMAGE, "role", "bucket", "prefix")

    def test_guard_rejects_cross_project_account_key_and_version_bindings(self):
        original = source_definition()["containerDefinitions"][0]["secrets"][0]["valueFrom"]
        for reference in (original.replace("roy/", "vevo/"), original.replace(ACCOUNT, "999999999999"),
                          original.replace("eu-central-1", "us-east-1"), original.replace("ABC123", "DEF456"),
                          original.replace(":BIZNISWEB_API_TOKEN:", ":BIZNISWEB_PASSWORD:"),
                          original[:-2] + ":AWSPREVIOUS:", original[:-2] + ":AWSCURRENT:synthetic-version"):
            with self.subTest(reference=reference):
                source = source_definition()
                source["containerDefinitions"][0]["secrets"][0]["valueFrom"] = reference
                with self.assertRaisesRegex(RuntimeError, "guard-secret-.*binding"):
                    deploy.guard_definition(source, "roy", IMAGE, "role", "bucket", "prefix")

    def test_guard_preserves_existing_common_version_selector_exactly(self):
        for selector in (":AWSCURRENT:", "::synthetic-version"):
            source = source_definition("vevo")
            for row in source["containerDefinitions"][0]["secrets"][:-1]:
                row["valueFrom"] = row["valueFrom"][:-2] + selector
            result = deploy.guard_definition(source, "vevo", IMAGE, "role", "bucket", "prefix")
            self.assertEqual(source["containerDefinitions"][0]["secrets"][:-1], result["containerDefinitions"][0]["secrets"])

    def test_task_secret_contract_reaches_real_creditnote_login_and_complete_scan(self):
        from creditnote_export import fetch_project_creditnotes
        from creditnote_storno_runner import _validate_runtime

        for project in deploy.PROJECTS:
            with self.subTest(project=project):
                definition = deploy.guard_definition(source_definition(project), project, IMAGE, "role", "bucket", "prefix")
                container = definition["containerDefinitions"][0]
                secret_values = {"BIZNISWEB_API_URL": f"https://{project}.flox.sk/api/graphql",
                                 "BIZNISWEB_API_TOKEN": "synthetic-api-token",
                                 "BIZNISWEB_USERNAME": "synthetic-admin-user", "BIZNISWEB_PASSWORD": "synthetic-admin-password"}
                environment = {row["name"]: row["value"] for row in container["environment"]}
                environment.update({row["name"]: secret_values[row["valueFrom"].split(":")[7]] for row in container["secrets"]})
                session = Mock()
                session.get.return_value.text = "?arf=syntheticToken"
                login, listing = Mock(), Mock()
                login.json.return_value = {"arf": "syntheticToken"}
                listing.text = '{"total":1,"rows":[{"creditnote_id":"synthetic-document"}]}'
                session.post.side_effect = [login, listing]
                with patch.dict(os.environ, environment, clear=True), \
                        patch("creditnote_export.build_retry_session", return_value=session):
                    _validate_runtime(project, {})
                    rows, total = fetch_project_creditnotes(project)
                self.assertEqual(1, total)
                self.assertEqual([{"creditnote_id": "synthetic-document"}], rows)
                self.assertEqual(f"https://{project}.flox.sk/admin/login/authenticate/", session.post.call_args_list[0].args[0])
                self.assertEqual("synthetic-admin-user", session.post.call_args_list[0].kwargs["data"]["username"])
                self.assertEqual("synthetic-admin-password", session.post.call_args_list[0].kwargs["data"]["password"])
                self.assertEqual(f"https://{project}.flox.sk/erp/orders/creditnotes/getListJson", session.post.call_args_list[1].args[0])
                self.assertEqual(2, session.post.call_count)

    def test_report_launch_accepts_only_reviewed_default_path_and_plain_project_binding(self):
        for project in deploy.PROJECTS:
            original = schedule(f"{project}-daily-report-email", project, f"{project}-reporting-daily")
            container = {"environment": [{"name": "REPORT_PROJECT", "value": project}]}
            for command in (None, ["python", "daily_report_runner.py"]):
                deploy.verify_report_launch(original, {**container, "command": command}, project)
            for command in ([], ["sh", "-c", "python daily_report_runner.py"], ["python", "creditnote_storno_runner.py"],
                            ["python", "daily_report_runner.py", "--project", "other"]):
                with self.subTest(command=command), self.assertRaisesRegex(RuntimeError, "command-drift"):
                    deploy.verify_report_launch(original, {**container, "command": command}, project)
            for environment in ([], [{"name": "REPORT_PROJECT", "value": "other"}], container["environment"] * 2):
                with self.subTest(environment=environment), self.assertRaisesRegex(RuntimeError, "project-binding"):
                    deploy.verify_report_launch(original, {"environment": environment}, project)
            for name in ("REPORT_PROJECT", SKIP):
                with self.assertRaisesRegex(RuntimeError, "project-binding"):
                    deploy.verify_report_launch(original, {**container, "secrets": [{"name": name, "valueFrom": "unreviewed"}]}, project)
            for payload in ({}, {"containerOverrides": [{"name": "reporting", "command": ["python", "-c", "unreviewed wrapper"]}]}):
                with self.subTest(payload=payload), self.assertRaisesRegex(RuntimeError, "input-not-reviewed"):
                    deploy.verify_report_launch({**original, "Target": {**original["Target"], "Input": json.dumps(payload)}}, container, project)

    def test_inspection_rejects_report_command_override_before_pause(self):
        value = controller()
        value.no_capture, value.pause, value.ecs = Mock(), Mock(), Mock()
        for name, service in deploy.PROTECTED.items():
            value.scheduler.values[name]["Target"]["EcsParameters"]["TaskDefinitionArn"] = (
                f"arn:aws:ecs:eu-central-1:{ACCOUNT}:task-definition/{service}:8")
        project = deploy.PROJECTS[0]
        report_name = f"{project}-daily-report-email"
        report = value.scheduler.values[report_name]
        revision, digest, _ = deploy.REPORT_PINS[project]
        report["Target"]["EcsParameters"]["TaskDefinitionArn"] = (
            f"arn:aws:ecs:eu-central-1:{ACCOUNT}:task-definition/{project}-reporting-daily:{revision}")
        report["Target"]["Input"] = json.dumps({"containerOverrides": [{"name": "reporting", "command": ["python", "-c", "unreviewed"]}]})
        value.ecs.describe_task_definition.return_value = {"taskDefinition": {"containerDefinitions": [{
            "name": "reporting", "image": IMAGE.split("@", 1)[0] + "@sha256:" + digest,
            "environment": [{"name": "REPORT_PROJECT", "value": project}]}]}}
        with patch.object(deploy, "require_main"), self.assertRaisesRegex(RuntimeError, "input-not-reviewed"):
            value.apply()
        value.pause.assert_not_called()
        value.save.assert_not_called()
        self.assertEqual([], value.scheduler.writes)

    def test_guard_policy_only_allows_own_two_state_objects_and_live_metrics(self):
        policy = deploy.state_policy(ACCOUNT, "synthetic-bucket", "roy", "daily-reports/roy")
        encoded = json.dumps(policy)
        self.assertNotIn("vevo", encoded)
        self.assertNotIn("s3:Delete", encoded)
        self.assertNotIn("sns:", encoded)
        self.assertEqual(["arn:aws:s3:::synthetic-bucket/data/roy/order-automation/state.json",
                          "arn:aws:s3:::synthetic-bucket/daily-reports/roy/state/creditnote_status_change_audit.json"],
                         policy["Statement"][0]["Resource"])


class CreditnoteRollbackTests(unittest.TestCase):
    def test_schedule_intent_saved_before_ambiguous_write_and_original_restored(self):
        value = controller()
        name = next(iter(deploy.PROTECTED))
        request = {**value.originals[name], "State": "DISABLED"}
        value.scheduler.fail_call, value.scheduler.ambiguous = 1, True
        with self.assertRaises(TimeoutError):
            value.update(name, request)
        self.assertEqual(request, value.evidence["expected_schedules"][name])
        value.save.assert_called_with("before-schedule-write")
        value.restore(live_attempted=False)
        self.assertEqual(value.originals, value.scheduler.values)

    def test_failure_before_live_restores_all_seven_schedules(self):
        value = controller()
        value.drain = Mock()
        value.pause()
        self.assertTrue(all(row["State"] == "DISABLED" for row in value.scheduler.values.values()))
        value.restore(live_attempted=False)
        self.assertEqual(value.originals, value.scheduler.values)
        self.assertFalse(value.evidence["reports_retained_paused"])

    def test_after_live_attempt_reports_stay_disabled_and_five_others_restore(self):
        value = controller()
        value.drain = Mock()
        value.pause()
        guard = schedule("roy-creditnote-storno-guard")
        value.update(guard["Name"], guard)
        value.restore(live_attempted=True)
        for name in deploy.PROTECTED:
            self.assertEqual(value.originals[name], value.scheduler.values[name])
        for project in deploy.PROJECTS:
            self.assertEqual("DISABLED", value.scheduler.values[f"{project}-daily-report-email"]["State"])
        self.assertEqual("DISABLED", value.scheduler.values[guard["Name"]]["State"])
        self.assertTrue(value.evidence["reports_retained_paused"])

    def test_independent_drift_is_not_overwritten_during_rollback(self):
        value = controller()
        value.drain = Mock()
        value.pause()
        name = next(iter(deploy.PROTECTED))
        value.scheduler.values[name]["Description"] = "changed independently"
        with self.assertRaisesRegex(RuntimeError, "rollback-incomplete"):
            value.restore(live_attempted=False)
        self.assertEqual("changed independently", value.scheduler.values[name]["Description"])
        self.assertEqual([name], value.evidence["rollback_failed_schedules"])

    def test_drain_requires_continuous_quiet_and_never_stops_natural_tasks(self):
        value = controller()
        value.expected = {name: {**row, "State": "DISABLED"} for name, row in value.originals.items()}
        value.scheduler.values = copy.deepcopy(value.expected)
        now = [0]
        value.clock = lambda: now[0]
        value.sleep = lambda seconds: now.__setitem__(0, now[0] + seconds)
        value.ecs = Mock()
        value.tasks = Mock(side_effect=lambda families: [{"lastStatus": "RUNNING"}] if now[0] == 10 else [])
        value.drain({"synthetic-family"}, timeout=100, quiet=20)
        self.assertEqual(40, now[0])
        value.ecs.stop_task.assert_not_called()


class CreditnoteTaskTests(unittest.TestCase):
    def task_controller(self, *, stopped=True):
        value = controller()
        value.ecs = Mock()
        value.session = Mock()
        value.check_schedules = Mock()
        value.cleanup_probe = Mock()
        definition = source_definition()
        definition["family"] = "roy-creditnote-storno-guard"
        definition["containerDefinitions"][0]["logConfiguration"] = {"options": {
            "awslogs-group": "/ecs/roy-creditnote-storno-guard", "awslogs-stream-prefix": "ecs"}}
        value.ecs.describe_task_definition.return_value = {"taskDefinition": definition}
        arn = f"arn:aws:ecs:eu-central-1:{ACCOUNT}:task-definition/roy-creditnote-storno-guard:1"
        own = f"arn:aws:ecs:eu-central-1:{ACCOUNT}:task/synthetic-own"
        value.ecs.run_task.return_value = {"tasks": [{"taskArn": own}]}
        task = {"taskArn": own, "taskDefinitionArn": arn, "clusterArn": CLUSTER,
                "startedBy": "creditnote-automation-migration", "lastStatus": "STOPPED" if stopped else "RUNNING",
                "launchType": "FARGATE", "containers": [{"name": "reporting", "exitCode": 0,
                    "imageDigest": DIGEST, "networkInterfaces": [{"privateIpv4Address": "192.0.2.10"}]}]}
        value.ecs.describe_tasks.return_value = {"tasks": [task]}
        value.clock = Mock(side_effect=[0, 1, 1801])
        value.sleep = Mock()
        value.read_logs = Mock(return_value={deploy.REPORT_MARKER: [], deploy.GUARD_MARKER: [],
                                          "CREDITNOTE_STANDALONE_SUMMARY": [summary()]})
        return value, arn

    def test_live_task_requires_real_completed_summary_and_exact_image(self):
        value, arn = self.task_controller()
        result = value.task_run("roy", arn, value.originals["roy-daily-invoice-generation"], DIGEST,
                                ["python", "creditnote_storno_runner.py", "--project", "roy"], kind="guard-live")
        self.assertEqual("guard-live", result["kind"])
        self.assertEqual("192.0.2.10", result["private_ip"])
        self.assertFalse(result["summary"]["dry_run"])
        self.assertEqual(1, value.ecs.run_task.call_count)
        self.assertTrue(value.ecs.run_task.call_args.kwargs["clientToken"])
        value.cleanup_probe.assert_not_called()
        value.ecs.stop_task.assert_not_called()

    def test_ambiguous_launch_is_recorded_and_never_replayed(self):
        value, arn = self.task_controller()
        value.ecs.run_task.side_effect = TimeoutError("private response")
        with self.assertRaises(TimeoutError):
            value.task_run("roy", arn, value.originals["roy-daily-invoice-generation"], DIGEST,
                           ["python", "creditnote_storno_runner.py"], kind="guard-live")
        self.assertEqual(1, value.ecs.run_task.call_count)
        self.assertEqual(value.ecs.run_task.call_args.kwargs["clientToken"], value.evidence["task_attempts"][0]["client_token"])
        value.save.assert_called_with("before-guard-live")

    def test_unfinished_live_task_is_never_killed_or_reported_completed(self):
        value, arn = self.task_controller(stopped=False)
        with self.assertRaisesRegex(RuntimeError, "timeout"):
            value.task_run("roy", arn, value.originals["roy-daily-invoice-generation"], DIGEST,
                           ["python", "creditnote_storno_runner.py"], kind="guard-live")
        self.assertEqual([], value.evidence["hosts"])
        value.ecs.stop_task.assert_not_called()
        value.cleanup_probe.assert_not_called()

    def test_probe_timeout_uses_owned_cleanup(self):
        value, arn = self.task_controller(stopped=False)
        command = ["python", "scripts/creditnote_automation_host_gate.py", "--project", "roy"]
        with self.assertRaisesRegex(RuntimeError, "timeout"):
            value.task_run("roy", arn, value.originals["roy-daily-invoice-generation"], DIGEST, command, kind="guard-probe")
        self.assertEqual(command, value.cleanup_probe.call_args.args[-1])

    def test_wrong_image_or_dry_run_summary_cannot_pass_live_completion(self):
        for wrong_image in (True, False):
            value, arn = self.task_controller()
            if wrong_image:
                value.ecs.describe_tasks.return_value["tasks"][0]["containers"][0]["imageDigest"] = "wrong-digest"
            else:
                value.read_logs.return_value["CREDITNOTE_STANDALONE_SUMMARY"] = [summary(dry_run=True)]
            with self.assertRaises(RuntimeError):
                value.task_run("roy", arn, value.originals["roy-daily-invoice-generation"], DIGEST,
                               ["python", "creditnote_storno_runner.py"], kind="guard-live")
            self.assertEqual([], value.evidence["hosts"])

    def test_cleanup_refuses_wrong_owner_or_command_and_checks_stopped_independently(self):
        for fault in ("owner", "command", None):
            value = controller()
            value.ecs = Mock()
            task = {"taskArn": "own", "taskDefinitionArn": "definition", "clusterArn": CLUSTER,
                    "startedBy": "owner", "lastStatus": "RUNNING", "overrides": {"containerOverrides": [
                        {"name": "reporting", "command": ["verified", "probe"]}]}}
            if fault == "owner":
                task["startedBy"] = "other"
            if fault == "command":
                task["overrides"]["containerOverrides"][0]["command"] = ["business-runner"]
            value.ecs.describe_tasks.side_effect = [{"tasks": [task]}, {"tasks": [{**task, "lastStatus": "STOPPED"}]}]
            if fault:
                with self.assertRaises(RuntimeError):
                    value.cleanup_probe("own", "definition", "owner", ["verified", "probe"])
                value.ecs.stop_task.assert_not_called()
            else:
                value.cleanup_probe("own", "definition", "owner", ["verified", "probe"])
                self.assertEqual(2, value.ecs.describe_tasks.call_count)
                self.assertEqual("own", value.ecs.stop_task.call_args.kwargs["task"])


class CreditnotePromotionTests(unittest.TestCase):
    def ready(self):
        value = controller()
        value.drain = Mock()
        value.no_capture = Mock()
        value.pause()
        guards = {}
        for project in deploy.PROJECTS:
            name = deploy.family(project)
            guards[name] = {**schedule(name, project), "State": "DISABLED"}
            value.update(name, guards[name])
            value.evidence["hosts"].append({"service": name, "kind": "guard-live"})
        return value, guards

    def test_promotion_requires_both_live_completions_before_any_write(self):
        value, guards = self.ready()
        value.evidence["hosts"].pop()
        before = len(value.scheduler.writes)
        with self.assertRaisesRegex(RuntimeError, "live-guard-completion-required"):
            value.promote(guards)
        self.assertEqual(before, len(value.scheduler.writes))

    def test_promotion_preserves_five_schedules_and_only_adds_report_skip(self):
        value, guards = self.ready()
        with patch.object(deploy, "require_main"):
            value.promote(guards)
        for name in deploy.PROTECTED:
            self.assertEqual(value.originals[name], value.scheduler.values[name])
        for project in deploy.PROJECTS:
            name = f"{project}-daily-report-email"
            self.assertEqual(deploy.report_override(value.originals[name]), value.scheduler.values[name])
            self.assertEqual("ENABLED", value.scheduler.values[deploy.family(project)]["State"])
        value.save.assert_called_with("promotion-readback-verified")

    def test_drift_or_main_change_prevents_promotion(self):
        for drift in (True, False):
            value, guards = self.ready()
            before = len(value.scheduler.writes)
            if drift:
                value.scheduler.values[next(iter(deploy.PROTECTED))]["Description"] = "independently changed"
            with patch.object(deploy, "require_main", side_effect=None if drift else RuntimeError("main moved")):
                with self.assertRaises(RuntimeError):
                    value.promote(guards)
            self.assertEqual(before, len(value.scheduler.writes))

    def test_private_receipt_uses_cas_encryption_owner_and_exact_readback(self):
        value = object.__new__(deploy.CreditnoteDeployment)
        value.evidence, value.bucket, value.key, value.account, value.etag = {}, "bucket", "key", ACCOUNT, None
        value.s3 = Mock()
        body = json.dumps({"phase": "verified"}, sort_keys=True, default=str).encode()
        value.s3.get_object.return_value = {"Body": io.BytesIO(body), "ServerSideEncryption": "AES256", "ETag": "first-etag"}
        value.save("verified")
        request = value.s3.put_object.call_args.kwargs
        self.assertEqual("*", request["IfNoneMatch"])
        self.assertEqual(ACCOUNT, request["ExpectedBucketOwner"])
        self.assertEqual("AES256", request["ServerSideEncryption"])
        value.s3.get_object.return_value["Body"] = io.BytesIO(body)
        value.save("verified")
        self.assertEqual("first-etag", value.s3.put_object.call_args.kwargs["IfMatch"])
        value.s3.get_object.return_value["Body"] = io.BytesIO(b"altered")
        with self.assertRaisesRegex(RuntimeError, "readback"):
            value.save("verified")


class CreditnoteMigrationSequenceTests(unittest.TestCase):
    def prepared(self, fail_kind=None):
        value = controller()
        value.inspect, value.drain, value.no_capture = Mock(), Mock(), Mock()
        value.report_sources = {project: "a" * 64 for project in deploy.PROJECTS}
        value.session = Mock()
        value.session.client.return_value.describe_images.return_value = {"imageDetails": [{"imageDigest": DIGEST}]}
        value.provision = Mock(side_effect=lambda project, image: ("synthetic-candidate-" + project, source_definition(project)))
        events = []
        def task_run(project, arn, snapshot, digest, command, *, kind):
            events.append((project, kind))
            self.assertTrue(all(value.scheduler.values[name]["State"] == "DISABLED" for name in value.originals))
            if kind == fail_kind:
                raise TimeoutError("synthetic private response")
            service = f"{project}-reporting-daily" if kind == "report-probe" else deploy.family(project)
            value.evidence["hosts"].append({"service": service, "kind": kind})
        value.task_run = Mock(side_effect=task_run)
        def provision_schedule(project, arn, definition):
            name = deploy.family(project)
            request = {**schedule(name, project), "State": "DISABLED"}
            value.update(name, request)
            return request
        value.monitor_and_schedule = Mock(side_effect=provision_schedule)
        return value, events

    def test_apply_completes_both_real_live_runs_before_enabling_any_schedule(self):
        value, events = self.prepared()
        with patch.object(deploy, "require_main"), patch.object(deploy, "established_alarm_route", return_value="existing-route"):
            result = value.apply()
        self.assertEqual([("roy", "report-probe"), ("vevo", "report-probe"), ("roy", "guard-probe"),
                          ("vevo", "guard-probe"), ("roy", "guard-live"), ("vevo", "guard-live")], events)
        self.assertEqual("monitored-not-enforced", result["future_completion_dependency"])
        self.assertEqual(2, result["live_completions"])
        self.assertTrue(all(row["State"] == "ENABLED" for row in value.scheduler.values.values()))

    def test_main_movement_during_preflight_does_not_pause_or_restore_schedules(self):
        value, events = self.prepared()
        value.pause, value.restore = Mock(), Mock()
        with patch.object(deploy, "require_main", side_effect=[None, RuntimeError("main-moved-during-preflight")]) as gate, \
                patch.object(deploy, "established_alarm_route", return_value="existing-route"):
            with self.assertRaisesRegex(RuntimeError, "main-moved-during-preflight"):
                value.apply()
        self.assertEqual(2, gate.call_count)
        value.inspect.assert_called_once()
        value.pause.assert_not_called()
        value.restore.assert_not_called()
        value.provision.assert_not_called()
        value.task_run.assert_not_called()
        self.assertEqual([], events)
        self.assertEqual([], value.scheduler.writes)
        self.assertEqual(value.originals, value.scheduler.values)

    def test_apply_failure_before_live_restores_reports_and_never_starts_live(self):
        value, events = self.prepared(fail_kind="guard-probe")
        with patch.object(deploy, "require_main"), patch.object(deploy, "established_alarm_route", return_value="existing-route"):
            with self.assertRaisesRegex(RuntimeError, "review-private-evidence"):
                value.apply()
        self.assertFalse(any(kind == "guard-live" for _, kind in events))
        for name, row in value.originals.items():
            self.assertEqual(row, value.scheduler.values[name])
        self.assertFalse(value.evidence["reports_retained_paused"])

    def test_apply_uncertain_live_failure_stops_sequence_and_retains_reports_paused(self):
        value, events = self.prepared(fail_kind="guard-live")
        with patch.object(deploy, "require_main"), patch.object(deploy, "established_alarm_route", return_value="existing-route"):
            with self.assertRaisesRegex(RuntimeError, "review-private-evidence"):
                value.apply()
        self.assertEqual(1, sum(kind == "guard-live" for _, kind in events))
        for name in deploy.PROTECTED:
            self.assertEqual(value.originals[name], value.scheduler.values[name])
        for project in deploy.PROJECTS:
            self.assertEqual("DISABLED", value.scheduler.values[f"{project}-daily-report-email"]["State"])
            self.assertEqual("DISABLED", value.scheduler.values[deploy.family(project)]["State"])
        self.assertTrue(value.evidence["reports_retained_paused"])

    def test_capture_inventory_uses_active_queries_and_preserves_running_collector(self):
        value = controller()
        collector_cluster = CLUSTER.rsplit("/", 1)[0] + "/vevo-growthbook-collector-production"
        value.clusters = Mock(return_value=[collector_cluster, CLUSTER])
        value.tasks = Mock(side_effect=lambda families, *, cluster: [] if cluster == CLUSTER else [{
            "taskDefinitionArn": "arn/vevo-growthbook-collector-production:1", "lastStatus": "RUNNING",
            "group": "service:vevo-growthbook-collector-production", "startedBy": "ecs-svc/synthetic-deployment"}])
        with patch.object(deploy.subprocess, "check_output", return_value=json.dumps({"workflow_runs": []}).encode()) as command:
            value.no_capture()
        self.assertEqual(5, command.call_count)
        self.assertTrue(all("?status=" in call.args[0][-1] for call in command.call_args_list))
        value.tasks.side_effect = None
        value.tasks.return_value = [{"taskDefinitionArn": "arn/vevo-growthbook-reconcile-production:1", "lastStatus": "RUNNING"}]
        with patch.object(deploy.subprocess, "check_output", return_value=json.dumps({"workflow_runs": []}).encode()):
            with self.assertRaisesRegex(RuntimeError, "active-growthbook"):
                value.no_capture()

    def test_active_source_capture_prevents_any_ecs_probe(self):
        value = controller()
        value.tasks = Mock()
        raw = json.dumps({"workflow_runs": [{"path": ".github/workflows/growthbook-aa-source.yml", "status": "in_progress"}]}).encode()
        with patch.object(deploy.subprocess, "check_output", return_value=raw):
            with self.assertRaisesRegex(RuntimeError, "active-aa"):
                value.no_capture()
        value.tasks.assert_not_called()

    def test_guard_live_launch_rechecks_main_after_probes_and_after_first_live_task(self):
        for rejected_call, expected_live_runs in ((3, 0), (4, 1)):
            value, events = self.prepared()
            calls = [0]
            def gate(commit):
                calls[0] += 1
                if calls[0] == rejected_call:
                    raise RuntimeError("main moved after read-only probes")
            with patch.object(deploy, "require_main", side_effect=gate), \
                    patch.object(deploy, "established_alarm_route", return_value="existing-route"):
                with self.assertRaisesRegex(RuntimeError, "review-private-evidence"):
                    value.apply()
            self.assertEqual(expected_live_runs, sum(kind == "guard-live" for _, kind in events))
            for project in deploy.PROJECTS:
                self.assertEqual("DISABLED" if expected_live_runs else "ENABLED",
                                 value.scheduler.values[f"{project}-daily-report-email"]["State"])

    def test_workflow_timeout_covers_all_task_bounds_drain_cleanup_and_rollback(self):
        import yaml
        path = Path(__file__).resolve().parents[1] / ".github/workflows/deploy-creditnote-automations.yml"
        workflow = yaml.safe_load(path.read_text())
        maximum_task_minutes = 6 * 30
        drain_minutes = 30
        probe_cleanup_minutes = 4 * 2
        rollback_and_api_minutes = 30
        timeout = workflow["jobs"]["deploy"]["timeout-minutes"]
        self.assertEqual(300, timeout)
        self.assertGreater(timeout, maximum_task_minutes + drain_minutes + probe_cleanup_minutes + rollback_and_api_minutes)

    def test_new_schedule_is_disabled_daily_92_minutes_before_report_with_scoped_monitoring(self):
        for project, cron in (("roy", "cron(58 23 * * ? *)"), ("vevo", "cron(28 23 * * ? *)")):
            value = controller()
            service = deploy.family(project)
            value.session = Mock()
            sqs, cloudwatch = Mock(), Mock()
            value.session.client.side_effect = lambda name: {"sqs": sqs, "cloudwatch": cloudwatch}[name]
            role = f"arn:aws:iam::{ACCOUNT}:role/new-scoped-schedule-role"
            value.create_role = Mock(return_value=role)
            value.alarm_route = "verified-existing-operator-route"
            queue_arn = f"arn:aws:sqs:eu-central-1:{ACCOUNT}:{service}-dlq"
            sqs.create_queue.return_value = {"QueueUrl": "synthetic-url"}
            queue_policy = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"AWS": role},
                "Action": "sqs:SendMessage", "Resource": queue_arn}]}
            sqs.get_queue_attributes.side_effect = [
                {"Attributes": {"QueueArn": queue_arn, "MessageRetentionPeriod": "1209600", "SqsManagedSseEnabled": "true"}},
                {"Attributes": {"Policy": json.dumps(queue_policy)}}]
            cloudwatch.describe_alarms.side_effect = lambda **kwargs: {"MetricAlarms": [call.kwargs for call in cloudwatch.put_metric_alarm.call_args_list]}
            arn = f"arn:aws:ecs:eu-central-1:{ACCOUNT}:task-definition/{service}:1"
            result = value.monitor_and_schedule(project, arn, source_definition(project))
            self.assertEqual(cron, result["ScheduleExpression"])
            self.assertEqual("DISABLED", result["State"])
            self.assertEqual("NONE", result["ActionAfterCompletion"])
            self.assertEqual("Europe/Bratislava", result["ScheduleExpressionTimezone"])
            self.assertEqual(4, cloudwatch.put_metric_alarm.call_count)
            for call in cloudwatch.put_metric_alarm.call_args_list:
                self.assertEqual([value.alarm_route], call.kwargs["AlarmActions"])
                if call.kwargs["Namespace"] == "BizniswebReporting":
                    self.assertIn({"Name": "RunMode", "Value": "live"}, call.kwargs["Dimensions"])
            policy = value.create_role.call_args.args[2]
            self.assertEqual(arn, policy["Statement"][0]["Resource"])
            self.assertEqual({"ArnEquals": {"ecs:cluster": CLUSTER}}, policy["Statement"][0]["Condition"])
            for name in deploy.PROTECTED:
                self.assertEqual(value.originals[name], value.scheduler.values[name])


class CreditnoteCaptureClusterTests(unittest.TestCase):
    def test_cluster_inventory_collects_all_pages_and_rejects_cycle_or_foreign_identity(self):
        value = controller()
        value.ecs = Mock()
        other = CLUSTER.rsplit("/", 1)[0] + "/vevo-growthbook-collector-production"
        value.ecs.list_clusters.side_effect = [{"clusterArns": [CLUSTER], "nextToken": "second"}, {"clusterArns": [other]}]
        self.assertEqual(sorted([CLUSTER, other]), value.clusters())
        self.assertEqual("second", value.ecs.list_clusters.call_args.kwargs["nextToken"])
        value.ecs.list_clusters.side_effect = None
        value.ecs.list_clusters.return_value = {"clusterArns": [CLUSTER], "nextToken": "cycle"}
        with self.assertRaisesRegex(RuntimeError, "pagination-cycle"):
            value.clusters()
        value.ecs.list_clusters.return_value = {"clusterArns": [CLUSTER.replace(ACCOUNT, "999999999999")]}
        with self.assertRaisesRegex(RuntimeError, "identity-drift"):
            value.clusters()

    def test_task_inventory_uses_requested_cluster_for_both_statuses_and_readback(self):
        value = controller()
        value.ecs = Mock()
        other = CLUSTER.rsplit("/", 1)[0] + "/vevo-growthbook-collector-production"
        value.ecs.list_tasks.return_value = {"taskArns": ["synthetic-task"]}
        value.ecs.describe_tasks.return_value = {"tasks": [{"taskArn": "synthetic-task", "clusterArn": other,
            "taskDefinitionArn": "arn/vevo-growthbook-reconcile-production:1", "lastStatus": "STOPPING"}]}
        actual = value.tasks(None, cluster=other)
        self.assertEqual("STOPPING", actual[0]["lastStatus"])
        self.assertEqual(["RUNNING", "STOPPED"], [call.kwargs["desiredStatus"] for call in value.ecs.list_tasks.call_args_list])
        self.assertTrue(all(call.kwargs["cluster"] == other for call in value.ecs.list_tasks.call_args_list))
        self.assertEqual(other, value.ecs.describe_tasks.call_args.kwargs["cluster"])
        value.ecs.describe_tasks.return_value["tasks"][0]["clusterArn"] = CLUSTER
        with self.assertRaisesRegex(RuntimeError, "task-identity-drift"):
            value.tasks(None, cluster=other)

    def test_nonservice_collector_and_reconciler_in_other_cluster_stop_capture_gate(self):
        for task in (
            {"taskDefinitionArn": "arn/vevo-growthbook-reconcile-production:1", "lastStatus": "RUNNING"},
            {"taskDefinitionArn": "arn/vevo-growthbook-reconcile-production:1", "lastStatus": "STOPPING"},
            {"taskDefinitionArn": "arn/vevo-growthbook-collector-production:1", "lastStatus": "RUNNING", "group": "family:synthetic-host-probe"},
            {"taskDefinitionArn": "arn/vevo-growthbook-collector-production:1", "lastStatus": "RUNNING",
             "group": "service:vevo-growthbook-collector-production", "startedBy": "ecs-svc/synthetic",
             "overrides": {"containerOverrides": [{"name": "collector", "command": ["synthetic-source-probe"]}]}},
        ):
            with self.subTest(task=task):
                value = controller()
                other = CLUSTER.rsplit("/", 1)[0] + "/vevo-growthbook-collector-production"
                value.clusters = Mock(return_value=[CLUSTER, other])
                value.tasks = Mock(side_effect=lambda families, *, cluster: [] if cluster == CLUSTER else [task])
                with patch.object(deploy.subprocess, "check_output", return_value=b'{"workflow_runs": []}'):
                    with self.assertRaisesRegex(RuntimeError, "active-growthbook-capture-task"):
                        value.no_capture()
                self.assertEqual([CLUSTER, other], [call.kwargs["cluster"] for call in value.tasks.call_args_list])

    def test_stable_complete_cluster_inventory_is_recorded_and_new_cluster_drift_rejected(self):
        value = controller()
        other = CLUSTER.rsplit("/", 1)[0] + "/vevo-growthbook-collector-production"
        value.tasks = Mock(return_value=[])
        value.clusters = Mock(return_value=[CLUSTER, other])
        with patch.object(deploy.subprocess, "check_output", return_value=b'{"workflow_runs": []}'):
            value.no_capture()
        self.assertEqual([CLUSTER, other], value.evidence["capture_inventory"]["clusters"])
        self.assertEqual(0, value.evidence["capture_inventory"]["active_nonservice_growthbook_tasks"])
        value.clusters.side_effect = [[CLUSTER], [CLUSTER, other]]
        with patch.object(deploy.subprocess, "check_output", return_value=b'{"workflow_runs": []}'):
            with self.assertRaisesRegex(RuntimeError, "inventory-changed"):
                value.no_capture()

    def test_missing_reporting_cluster_or_incomplete_task_readback_fails_closed(self):
        value = controller()
        value.clusters, value.tasks = Mock(return_value=[]), Mock()
        with patch.object(deploy.subprocess, "check_output", return_value=b'{"workflow_runs": []}'):
            with self.assertRaisesRegex(RuntimeError, "not-in-inventory"):
                value.no_capture()
        value.tasks.assert_not_called()
        value = controller()
        value.ecs = Mock()
        value.ecs.list_tasks.return_value = {"taskArns": ["synthetic-task"]}
        value.ecs.describe_tasks.return_value = {"tasks": [], "failures": [{"reason": "synthetic-missing"}]}
        with self.assertRaisesRegex(RuntimeError, "readback-incomplete"):
            value.tasks(None)


class IndependentGuardProvisionEnvironmentTests(unittest.TestCase):
    def provision_controller(self, project, mutate=None):
        value = controller()
        value.locations = {project: ("synthetic-private-bucket", f"daily-reports/{project}")}
        value.create_role = Mock(return_value=f"arn:aws:iam::{ACCOUNT}:role/synthetic-guard-{project}")
        logs = Mock()
        value.session = Mock()
        value.session.client.side_effect = lambda name: logs if name == "logs" else self.fail("unexpected client")
        source = source_definition(project)
        source["volumes"] = [{"name": "one"}, {"name": "two"}]
        source["containerDefinitions"][0]["mountPoints"] = [
            {"sourceVolume": "one", "containerPath": "/one"},
            {"sourceVolume": "two", "containerPath": "/two"},
        ]
        original_source = copy.deepcopy(source)
        source_arn = value.originals[f"{project}-daily-invoice-generation"]["Target"]["EcsParameters"]["TaskDefinitionArn"]
        candidate_arn = f"arn:aws:ecs:eu-central-1:{ACCOUNT}:task-definition/{deploy.family(project)}:1"
        registered = {}
        observed = {}
        def register(**request):
            registered.update(copy.deepcopy(request))
            observed.update(copy.deepcopy(request), taskDefinitionArn=candidate_arn)
            observed["containerDefinitions"][0]["environment"].reverse()
            if mutate:
                mutate(observed)
            return {"taskDefinition": copy.deepcopy(observed)}
        def describe(**request):
            arn = request["taskDefinition"]
            if arn == source_arn:
                return {"taskDefinition": copy.deepcopy(source)}
            self.assertEqual(candidate_arn, arn)
            return {"taskDefinition": copy.deepcopy(observed)}
        value.ecs = Mock()
        value.ecs.register_task_definition.side_effect = register
        value.ecs.describe_task_definition.side_effect = describe
        return value, source, original_source, registered, observed, candidate_arn

    def test_actual_provision_accepts_reordered_readback_and_preserves_raw_candidate_evidence(self):
        for project in deploy.PROJECTS:
            value, source, original_source, registered, observed, expected_arn = self.provision_controller(project)
            with self.subTest(project=project):
                arn, definition = value.provision(project, IMAGE)
                self.assertEqual(expected_arn, arn)
                self.assertEqual(registered, definition)
                self.assertEqual({"arn": arn, "definition": registered}, value.evidence["candidate_definitions"][project])
                self.assertNotEqual(registered["containerDefinitions"][0]["environment"],
                                    observed["containerDefinitions"][0]["environment"])
                self.assertEqual(original_source, source)
                value.save.assert_called_once_with("guard-candidate-registered")
                value.ecs.register_task_definition.assert_called_once()
                self.assertEqual(2, value.ecs.describe_task_definition.call_count)
                value.ecs.run_task.assert_not_called()

    def test_actual_provision_rejects_financially_relevant_or_order_sensitive_drift_before_publication(self):
        def change(kind):
            def mutate(definition):
                container = definition["containerDefinitions"][0]
                if kind == "duplicate":
                    container["environment"].append(copy.deepcopy(container["environment"][0]))
                elif kind == "missing":
                    container["environment"].pop()
                elif kind == "missing-environment":
                    container.pop("environment")
                elif kind == "changed":
                    container["environment"][0]["value"] += "-changed"
                elif kind == "malformed":
                    container["environment"][0]["value"] = False
                elif kind in ("command", "secrets", "mountPoints"):
                    container[kind].reverse()
                elif kind == "volumes":
                    definition["volumes"].reverse()
                elif kind == "extra-container":
                    definition["containerDefinitions"].append({"name": "unexpected", "image": IMAGE})
                elif kind == "image":
                    container["image"] = IMAGE[:-1] + "f"
                else:
                    definition["executionRoleArn"] = "foreign-role"
            return mutate
        for project in deploy.PROJECTS:
            for kind in ("duplicate", "missing", "missing-environment", "changed", "malformed", "command",
                         "secrets", "mountPoints", "volumes", "extra-container", "image", "execution-role"):
                value, source, original_source, registered, _, _ = self.provision_controller(project, change(kind))
                with self.subTest(project=project, kind=kind), self.assertRaises(RuntimeError):
                    value.provision(project, IMAGE)
                self.assertNotIn("candidate_definitions", value.evidence)
                value.save.assert_not_called()
                value.ecs.register_task_definition.assert_called_once()
                value.ecs.run_task.assert_not_called()
                self.assertEqual(original_source, source)
                self.assertEqual(registered, value.ecs.register_task_definition.call_args.kwargs)

    def test_comparison_retains_absent_environment_and_container_order(self):
        absent = {"containerDefinitions": [{"name": "one"}, {"name": "two"}]}
        explicit = copy.deepcopy(absent)
        explicit["containerDefinitions"][0]["environment"] = []
        self.assertNotEqual(deploy.comparable_definition(absent), deploy.comparable_definition(explicit))
        reordered = copy.deepcopy(absent)
        reordered["containerDefinitions"].reverse()
        self.assertNotEqual(deploy.comparable_definition(absent), deploy.comparable_definition(reordered))


if __name__ == "__main__":
    unittest.main()
