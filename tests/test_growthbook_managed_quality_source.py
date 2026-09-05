from __future__ import annotations

import copy
import hashlib
import io
import json
import os
import sys
import unittest
import zipfile
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

from scripts import collect_growthbook_aa_quality_source as source
from scripts.build_growthbook_aa_quality_source import build_quality_source, canonical_source_bytes
from scripts.validate_growthbook_aa_quality_capture import validate_capture, validate_capture_bytes
from tests.test_growthbook_aa_evidence_gate_recorder import load, resolved_snapshot
from tests.test_growthbook_aa_quality_source import CONFIG
from tests.test_growthbook_pipeline import event
from tests.test_growthbook_quality_source_io import MemoryS3, source_order
from reporting_core.experiment_quality_source_io import read_receipted_order_source, read_stable_retained_raw_source

ROOT = Path(__file__).resolve().parents[1]
ENV = {"GITHUB_ACTIONS": "true", "RUNNER_ENVIRONMENT": "github-hosted", "GITHUB_REPOSITORY": source.REPO,
       "GITHUB_REF": "refs/heads/main", "GITHUB_EVENT_NAME": "workflow_dispatch", "GITHUB_RUN_ATTEMPT": "1",
       "GITHUB_WORKFLOW_REF": f"{source.REPO}/{source.WORKFLOW}@refs/heads/main", "CONFIRM_SOURCE": "true",
       "GITHUB_RUN_ID": "123456789", "GITHUB_SHA": "c" * 40, "HEALTH_RUN_ID": "987654321", "HEALTH_JSON_SHA256": "d" * 64}
TOKEN_REF = f"arn:aws:ssm:{source.REGION}:{source.ACCOUNT}:parameter/vevo/test-token"


def inputs():
    workspace = load("growthbook_workspace.json")
    activation = load("growthbook_production_aa_activation.json")
    # Isolate the running phase so future reviewed stop transitions cannot make
    # synthetic pre-source tests inherit live post-source state.
    activation["status"] = "production_aa_running_activation_verified"
    activation["growthbook"]["allocation_percent"] = 100
    for row in workspace["experiments"]:
        if row["tracking_key"] == "vevo-sk-product-cta-color-001":
            row.update(status="unstarted_draft", production_allocation_percent=0)
    return [canonical_source_bytes(resolved_snapshot()), workspace, activation,
            load("growthbook_aa_acceptance.json"), load("growthbook_production_reconciliation_deploy_evidence.json"), dict(ENV)]


def plan():
    return source.make_plan(*inputs())


def fake_runtime():
    args = inputs()
    current = source.make_plan(*args)
    activation, reconciliation = args[2], args[4]
    collector_arn = f"arn:aws:ecs:{source.REGION}:{source.ACCOUNT}:task-definition/{activation['collector']['task_definition']}"
    task_arn = f"arn:aws:ecs:{source.REGION}:{source.ACCOUNT}:task/test/" + "1" * 32
    target_arn = reconciliation["reconciliation"]["task_definition"]
    source_arn = reconciliation["source_runtime"]["task_definition"]
    outputs = {"CollectorServiceName": source.SERVICE, "ExperimentDatabaseName": "vevo_growthbook_production",
               "ReportingWorkGroupName": "vevo-growthbook-reporting-production", "CollectorClusterArn": "collector-cluster",
               "EventBucketName": "vevo-test-events", "CollectorTaskDefinitionArn": collector_arn,
               "CollectorContainerLogGroup": "test-collector-logs"}
    stack = {"StackStatus": "UPDATE_COMPLETE", "Outputs": [{"OutputKey": k, "OutputValue": v} for k, v in outputs.items()]}
    service = {"status": "ACTIVE", "desiredCount": 1, "runningCount": 1, "pendingCount": 0,
               "taskDefinition": collector_arn, "enableExecuteCommand": False}
    task = {"taskArn": task_arn, "taskDefinitionArn": collector_arn, "launchType": "FARGATE", "lastStatus": "RUNNING",
            "containers": [{"imageDigest": activation["collector"]["image_digest"]}],
            "attachments": [{"details": [{"name": "privateIPv4Address", "value": "172.31.1.2"}]}]}
    container = {"image": "synthetic@" + reconciliation["reconciliation"]["image_digest"],
                 "command": ["/bin/bash", "-lc", "cd /app && python scripts/run_scheduled_growthbook_reconciliation.py"],
                 "secrets": [{"name": "BIZNISWEB_API_TOKEN", "valueFrom": TOKEN_REF}]}
    definition = {"taskDefinitionArn": target_arn, "networkMode": "awsvpc",
                  "taskRoleArn": f"arn:aws:iam::{source.ACCOUNT}:role/BiznisWebReportingTaskRole-vevo",
                  "containerDefinitions": [container]}
    original_definition = {"containerDefinitions": [copy.deepcopy(container)]}
    schedule = {"Name": source.RECONCILER, "State": "ENABLED", "ScheduleExpression": "cron(45 3 * * ? *)",
                "ScheduleExpressionTimezone": "Europe/Bratislava", "Target": {"Arn": "reporting-cluster", "EcsParameters": {"TaskDefinitionArn": target_arn}}}
    original_schedule = {"Name": source.SOURCE_SCHEDULE, "State": "ENABLED",
                         "Target": {"Arn": "reporting-cluster", "EcsParameters": {"TaskDefinitionArn": source_arn}}}
    rules = [{"ID": name, "Status": "Enabled", "Prefix": prefix, "Expiration": {"Days": days}}
             for name, prefix, days in (("ExpireRawExperimentEvents", "experiment-events/raw/", 180),
                                        ("ExpireCuratedExperimentFacts", "experiment-events/curated/", 400),
                                        ("ExpireAthenaQueryResults", "athena-results/", 30))]
    rules.append({"ID": "AbortIncompleteMultipartUploads", "Status": "Enabled", "Prefix": "",
                  "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}})
    clients = {name: Mock() for name in ("sts", "cloudformation", "s3", "ecs", "scheduler")}
    clients["sts"].get_caller_identity.return_value = {"Account": source.ACCOUNT}
    clients["cloudformation"].describe_stacks.return_value = {"Stacks": [stack]}
    clients["cloudformation"].describe_stack_resource.return_value = {"StackResourceDetail": {
        "PhysicalResourceId": "vevo-test-events", "ResourceType": "AWS::S3::Bucket"}}
    clients["s3"].get_bucket_lifecycle_configuration.return_value = {"Rules": rules}
    clients["ecs"].describe_services.return_value = {"services": [service]}
    clients["ecs"].list_tasks.return_value = {"taskArns": [task_arn]}
    clients["ecs"].describe_tasks.return_value = {"tasks": [task]}
    clients["ecs"].describe_task_definition.side_effect = lambda taskDefinition: {
        "taskDefinition": definition if taskDefinition == target_arn else original_definition}
    clients["scheduler"].get_schedule.side_effect = lambda Name: schedule if Name == source.RECONCILER else original_schedule
    return current, activation, reconciliation, clients, {
        "stack": stack, "service": service, "task": task, "definition": definition,
        "source_definition": original_definition, "schedule": schedule, "source_schedule": original_schedule, "rules": rules}


def capture_fixture():
    current = replace(plan(), eligible=1)
    row = event(received_at=current.window.from_utc)
    raw = read_stable_retained_raw_source(MemoryS3([row]), bucket="vevo-test",
        context_from_utc=current.window.context_from_utc, through_utc=current.window.through_utc)
    orders = read_receipted_order_source(Mock(), completion_receipts={})
    now = datetime.now(UTC).replace(microsecond=0)
    quality = build_quality_source(raw.rows, [], config=CONFIG, window=current.window, generated_at=now,
        expected_eligible_devices=1, snapshot_manifest_sha256=current.snapshot_sha256,
        checkpoint_evidence_sha256=current.checkpoint_sha256, workflow_run_id=current.run_id, main_commit=current.main_commit)
    capture = {"schema_version": 1, "evidence_type": "vevo_growthbook_aa_quality_capture", "source": quality,
        "acquisition": {"started_at_utc": source.stamp(now), "completed_at_utc": source.stamp(now),
            "foundation_evidence_sha256": current.foundation_sha256, "context_floor_source": "verified_empty_production_foundation_utc_day",
            "retention_policy_days": 180, "raw_input": raw.sanitized_proof, "order_input": orders.sanitized_proof,
            "receipt_parity": {"context_receipt_summary_sha256": "e" * 64, "accepted_write_count_parity_verified": True},
            "control_before_sha256": "a" * 64, "control_after_sha256": "a" * 64, "managed_token_reference_sha256": "b" * 64,
            "health": {"workflow_run_id": current.health_run_id, "main_commit": current.main_commit, "sha256": current.health_sha256},
            "collector_live_identity_verified": True, "reconciler_immutable_localhost_gate_inherited": True, "source_schedule_unchanged": True},
        "safety": {"read_only": True, "contains_identities": False, "contains_credentials": False, "contains_raw_aws_payloads": False,
                   "ordinary_publish_allowed": False, "preview_woken": False, "experiment_mutations": False, "winner_calls": False}}
    return current, capture


class ManagedSourceTests(unittest.TestCase):
    def test_progress_accepts_only_fixed_names_and_defaults_to_silent(self):
        stream = io.StringIO()
        progress = source.CaptureProgress(stream)
        self.assertEqual("not-started", progress.phase)
        for phase in source.CAPTURE_PHASES:
            progress(phase)
        self.assertEqual(
            [f"VEVO_AA_QUALITY_SOURCE_PROGRESS:phase={phase}:raw=false" for phase in source.CAPTURE_PHASES],
            stream.getvalue().splitlines(),
        )
        before = stream.getvalue()
        for invalid in ("private-device-or-token", "retained-raw-source\nprivate", None, 42):
            with self.assertRaises(source.SourceCollectionError):
                progress(invalid)
            self.assertEqual(before, stream.getvalue())
            self.assertEqual(source.CAPTURE_PHASES[-1], progress.phase)
        with patch.object(source, "print", create=True) as printer:
            source.CaptureProgress()("runtime-preflight")
            printer.assert_not_called()

    def test_failure_codes_never_format_exception_messages_or_sdk_payloads(self):
        class UnsafeError(RuntimeError):
            def __str__(self):
                raise AssertionError("exception formatting is forbidden")

        for message, code in source.SAFE_FAILURE_CODES.items():
            self.assertEqual(code, source.safe_failure_code(source.SourceCollectionError(message)))
        for error, expected in (
            (UnsafeError("private-device-or-token"), "unclassified-error"),
            (RuntimeError({"Error": {"Message": "private-device-or-token"}}), "unclassified-error"),
            (source.SourceCollectionError("private-device-or-token"), "local-contract-check"),
            (source.QualityInputError("private-device-or-token"), "input-read-or-validation"),
        ):
            self.assertEqual(expected, source.safe_failure_code(error))

    def test_cli_reports_safe_failure_phase_while_suppressing_raw_output(self):
        current = plan()
        output, errors = io.StringIO(), io.StringIO()
        sdk = MagicMock()

        def fail_capture(*args, progress, **kwargs):
            print("private-device-or-token")
            print("private-sdk-payload", file=sys.stderr)
            progress("reporting-runtime")
            raise source.SourceCollectionError("unexpected runner API environment")

        with patch.object(sys, "argv", ["source"]), patch.object(sys, "stdout", output), \
             patch.object(sys, "stderr", errors), patch.dict(sys.modules, {"boto3": sdk}), \
             patch.dict(os.environ, {"GITHUB_WORKSPACE": str(ROOT)}), \
             patch.object(source, "load_inputs", return_value=(current, {}, {})), \
             patch.object(source, "verify_checkout"), patch.object(source, "reject_previous_capture"), \
             patch.object(source, "download_health"), patch.object(source.logging, "disable"), \
             patch.object(source, "collect", side_effect=fail_capture), patch.object(Path, "mkdir") as mkdir:
            self.assertEqual(2, source.main())
            mkdir.assert_not_called()
        sdk.Session.return_value.client.assert_not_called()
        self.assertEqual("", output.getvalue())
        self.assertEqual([
            "VEVO_AA_QUALITY_SOURCE_PROGRESS:phase=reporting-runtime:raw=false",
            "VEVO_AA_QUALITY_SOURCE_STOPPED:stage=source-capture:phase=reporting-runtime:code=api-environment-drift:raw=false",
        ], errors.getvalue().splitlines())

    def setUp(self):
        clock = patch.object(source, "now_utc", return_value=datetime(2026, 9, 5, 8, tzinfo=UTC))
        clock.start()
        self.addCleanup(clock.stop)

    def test_context_is_derived_from_the_verified_empty_foundation_not_window_start(self):
        args = inputs()
        current = source.make_plan(*args)
        foundation = args[1]["athena"]["production"]["successful_foundation_deployment"]
        self.assertEqual(source.utc(foundation["verified_at_utc"]).replace(hour=0, minute=0, second=0), current.window.context_from_utc)
        self.assertLess(current.window.context_from_utc, current.window.from_utc)
        self.assertEqual(hashlib.sha256(args[0]).hexdigest(), current.snapshot_sha256)
        self.assertEqual(1000, current.eligible)

    def test_each_managed_gate_is_required(self):
        for key in ENV:
            with self.subTest(key=key):
                args = inputs()
                args[-1][key] = "wrong"
                with self.assertRaises(ValueError):
                    source.make_plan(*args)

    def test_closed_or_changed_lifecycle_rejects(self):
        for target in ("source", "snapshot", "aa", "foundation", "cta"):
            args = inputs()
            snapshot = json.loads(args[0])
            if target == "source":
                snapshot["automated_evidence"]["producer_allowed"] = True
            elif target == "snapshot":
                snapshot["snapshot_build_allowed"] = True
            elif target == "aa":
                args[2]["status"] = "stopped"
            elif target == "foundation":
                args[1]["athena"]["production"]["successful_foundation_deployment"]["deployment"]["event_bucket_empty"] = False
            else:
                next(row for row in args[1]["experiments"] if row["tracking_key"] == "vevo-sk-product-cta-color-001")["production_allocation_percent"] = 100
            args[0] = canonical_source_bytes(snapshot)
            with self.subTest(target=target), self.assertRaises(ValueError):
                source.make_plan(*args)

    def test_runtime_preflight_reads_only_approved_metadata(self):
        current, activation, reconciliation, clients, state = fake_runtime()
        result = source.runtime_preflight(clients.__getitem__, current, activation, reconciliation)
        self.assertEqual("vevo-test-events", result["bucket"])
        self.assertEqual(TOKEN_REF, result["token_reference"])
        self.assertRegex(result["control_sha256"], r"^[a-f0-9]{64}$")
        allowed = {"get_caller_identity", "describe_stacks", "describe_stack_resource", "get_bucket_lifecycle_configuration",
                   "describe_services", "list_tasks", "describe_tasks", "describe_task_definition", "get_schedule"}
        self.assertTrue(all(call[0] in allowed for client in clients.values() for call in client.mock_calls))
        self.assertNotIn("172.31.1.2", json.dumps(result))

    def test_runtime_identity_and_retention_drift_reject_before_token_or_event_read(self):
        mutations = [lambda s: s["stack"].update(StackStatus="UPDATE_IN_PROGRESS"),
                     lambda s: s["service"].update(runningCount=0),
                     lambda s: s["task"]["containers"][0].update(imageDigest="wrong"),
                     lambda s: s["task"]["attachments"][0]["details"][0].update(value="8.8.8.8"),
                     lambda s: s["definition"].update(taskRoleArn="unrelated"),
                     lambda s: s["definition"]["containerDefinitions"][0].update(command=["other"]),
                     lambda s: s["source_definition"]["containerDefinitions"][0]["secrets"][0].update(valueFrom="other"),
                     lambda s: s["schedule"].update(State="DISABLED"),
                     lambda s: s["source_schedule"]["Target"].update(Arn="other-cluster"),
                     lambda s: s["rules"][0].update(Expiration={"Days": 1}),
                     lambda s: s["rules"][1].update(Prefix="experiment-events/raw/"),
                     lambda s: s["rules"][3].update(Expiration={"Days": 1})]
        for mutate in mutations:
            current, activation, reconciliation, clients, state = fake_runtime()
            mutate(state)
            with self.subTest(mutate=mutate), self.assertRaises(ValueError):
                source.runtime_preflight(clients.__getitem__, current, activation, reconciliation)
            clients["s3"].get_object.assert_not_called()

    def test_control_hash_detects_schedule_change_without_outputting_raw_control(self):
        current, activation, reconciliation, clients, state = fake_runtime()
        first = source.runtime_preflight(clients.__getitem__, current, activation, reconciliation)
        state["source_schedule"]["Description"] = "changed"
        second = source.runtime_preflight(clients.__getitem__, current, activation, reconciliation)
        self.assertNotEqual(first["control_sha256"], second["control_sha256"])

    def test_only_exact_inherited_secure_parameter_is_read(self):
        client = Mock()
        client.get_parameter.return_value = {"Parameter": {"ARN": TOKEN_REF, "Type": "SecureString", "Value": "x" * 32}}
        factory = Mock(return_value=client)
        self.assertEqual("x" * 32, source.read_managed_token(factory, TOKEN_REF))
        factory.assert_called_once_with("ssm")
        client.get_parameter.assert_called_once_with(Name=TOKEN_REF, WithDecryption=True)

    def test_only_exact_json_key_from_same_account_secret_is_selected(self):
        arn = f"arn:aws:secretsmanager:{source.REGION}:{source.ACCOUNT}:secret:vevo/test-abc123"
        client = Mock()
        client.get_secret_value.return_value = {"ARN": arn, "SecretString": json.dumps({"BIZNISWEB_API_TOKEN": "x" * 32, "unused": "never-exported"})}
        self.assertEqual("x" * 32, source.read_managed_token(lambda name: client, arn + ":BIZNISWEB_API_TOKEN::"))
        client.get_secret_value.assert_called_once_with(SecretId=arn)
        for reference in (arn.replace(source.ACCOUNT, "000000000000"), arn + ":OTHER::", arn + ":BIZNISWEB_API_TOKEN:stage:version"):
            with self.assertRaises(ValueError):
                source.read_managed_token(lambda name: client, reference)

    def test_fixed_transport_blocks_redirects_and_partial_or_large_responses(self):
        session = MagicMock()
        response = session.post.return_value.__enter__.return_value = Mock()
        session.post.return_value.__exit__ = Mock(return_value=False)
        response.status_code, response.url = 200, source.API_URL
        response.headers = {"Content-Type": "application/json; charset=utf-8"}
        response.iter_content.return_value = [json.dumps({"data": {"getOrder": None}}).encode()]
        pace = Mock()
        execute = source.fixed_order_transport(session, "x" * 32, pace)
        self.assertEqual({"getOrder": None}, execute(source.RECEIPTED_ORDER_QUERY, variable_values={"order_num": "123"}))
        self.assertFalse(session.post.call_args.kwargs["allow_redirects"])
        pace.assert_called_once()
        for mutate in (lambda: setattr(response, "status_code", 302),
                       lambda: setattr(response, "url", "https://www.roy.sk/api/graphql"),
                       lambda: setattr(response.iter_content, "return_value", [b'{"data":{"getOrder":null},"errors":["x"]}']),
                       lambda: setattr(response.iter_content, "return_value", [b'{"data":{"getOrder":{},"getOrder":null}}']),
                       lambda: setattr(response.iter_content, "return_value", [b'{"data":{"getOrder":NaN}}']),
                       lambda: setattr(response.iter_content, "return_value", [b"x" * (128 * 1024 + 1)])):
            response.status_code, response.url = 200, source.API_URL
            response.iter_content.return_value = [json.dumps({"data": {"getOrder": None}}).encode()]
            mutate()
            with self.assertRaises(ValueError):
                execute(source.RECEIPTED_ORDER_QUERY, variable_values={"order_num": "123"})
        with self.assertRaises(ValueError):
            execute("mutation {}", variable_values={"order_num": "123"})

    def test_control_digest_ignores_only_top_level_sdk_response_metadata(self):
        current, activation, reconciliation, clients, objects = fake_runtime()
        check = lambda: source.runtime_preflight(clients.__getitem__, current, activation, reconciliation)
        original = check()
        for value in ('first-request', 'second-request'):
            for name in ('schedule', 'source_schedule'):
                objects[name]['ResponseMetadata'] = {'RequestId': value, 'HTTPHeaders': {'date': value}, 'RetryAttempts': 1}
            saved = copy.deepcopy(objects)
            self.assertEqual(original, check())
            self.assertEqual(saved, objects)
        # Changing a real source configuration field is still detected even
        # when it is not part of the minimal individual invariant checks.
        objects['source_schedule']['Target']['Input'] = 'synthetic-changed-input'
        self.assertNotEqual(original['control_sha256'], check()['control_sha256'])
        del objects['source_schedule']['Target']['Input']
        objects['schedule']['Description'] = 'synthetic-configuration-change'
        self.assertNotEqual(original['control_sha256'], check()['control_sha256'])

    def test_existing_success_or_active_source_is_not_recaptured(self):
        for status, conclusion in (("in_progress", None), ("queued", None), ("completed", "success")):
            with patch.object(source, "gh_json", return_value={"workflow_runs": [{"id": 555555555, "status": status, "conclusion": conclusion}]}):
                with self.assertRaises(ValueError):
                    source.reject_previous_capture(ENV["GITHUB_RUN_ID"])
        history = {"workflow_runs": [{"id": int(ENV["GITHUB_RUN_ID"]), "status": "in_progress"},
                                     {"id": 555555555, "status": "completed", "conclusion": "failure"}]}
        with patch.object(source, "gh_json", side_effect=[history, {"total_count": 0}]):
            source.reject_previous_capture(ENV["GITHUB_RUN_ID"])
        with patch.object(source, "gh_json", side_effect=[history, {"total_count": 1}]):
            with self.assertRaises(ValueError):
                source.reject_previous_capture(ENV["GITHUB_RUN_ID"])

    def test_receipt_counts_must_cover_retained_context_writes(self):
        current = plan()
        logs = Mock()
        logs.describe_log_groups.return_value = {"logGroups": [{"logGroupName": "test", "retentionInDays": 180}]}
        message = json.dumps({"schema_version": 1, "marker": "VEVO_GROWTHBOOK_COLLECTOR_RECEIPT", "accepted": True, "duplicate": False})
        logs.filter_log_events.return_value = {"events": [{"eventId": "synthetic-log", "timestamp": int(current.window.from_utc.timestamp() * 1000), "message": message}]}
        rows = [event(received_at=current.window.from_utc)]
        proof = source.receipt_parity(logs, "test", current, rows)
        self.assertTrue(proof["accepted_write_count_parity_verified"])
        self.assertNotIn("synthetic-log", json.dumps(proof))
        with self.assertRaises(ValueError):
            source.receipt_parity(logs, "test", current, [])
        logs.filter_log_events.return_value["nextToken"] = "repeated"
        with self.assertRaises(ValueError):
            source.receipt_parity(logs, "test", current, rows)

    def test_capture_canonical_provenance_and_no_overclaim(self):
        current, capture = capture_fixture()
        validate_capture(capture, current)
        raw = canonical_source_bytes(capture)
        self.assertEqual(capture, validate_capture_bytes(raw, current, expected_sha256=hashlib.sha256(raw).hexdigest()))
        for field in ("main_commit", "snapshot_sha256", "checkpoint_sha256", "foundation_sha256", "health_sha256"):
            with self.subTest(field=field), self.assertRaises(ValueError):
                validate_capture(capture, replace(current, **{field: "f" * (40 if field == "main_commit" else 64)}))
        for section, field, value in (("raw_input", "historical_retention_proven", True),
                                      ("raw_input", "context_floor_proven", True),
                                      ("order_input", "atomic_historical_snapshot_proven", True),
                                      ("receipt_parity", "accepted_write_count_parity_verified", False),
                                      ("order_input", "query_sha256", "f" * 64)):
            changed = copy.deepcopy(capture)
            changed["acquisition"][section][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                validate_capture(changed, current)
        with self.assertRaises(ValueError):
            validate_capture_bytes(raw + b" ", current, expected_sha256=hashlib.sha256(raw + b" ").hexdigest())

    def test_calculator_mode_has_no_constructor_clients_dirs_or_environment_mutation(self):
        with patch("dotenv.load_dotenv", return_value=False), patch.dict(os.environ, {"REPORT_PROJECT": "vevo"}):
            import export_orders as reporting
            saved_env = dict(os.environ)
            with patch.object(reporting, "RequestsHTTPTransport") as transport, patch.object(reporting, "Client") as client, \
                 patch.object(reporting, "FacebookAdsClient") as facebook, patch.object(reporting, "GoogleAdsClient") as google, \
                 patch.object(reporting, "WeatherClient") as weather, patch.object(Path, "mkdir") as mkdir:
                calculator = reporting.BizniWebExporter(source.API_URL, "", project_name="vevo", order_facts_only=True)
                for mocked in (transport, client, facebook, google, weather, mkdir):
                    mocked.assert_not_called()
                self.assertIsNone(calculator.client)
                self.assertFalse(calculator.enable_period_bundle)
            self.assertEqual(saved_env, dict(os.environ))

    def test_default_exporter_still_constructs_its_existing_clients_and_directories(self):
        with patch("dotenv.load_dotenv", return_value=False), patch.dict(os.environ, {"REPORT_PROJECT": "vevo"}):
            import export_orders as reporting
            with patch.object(reporting, "RequestsHTTPTransport") as transport, patch.object(reporting, "Client") as client, \
                 patch.object(reporting, "FacebookAdsClient") as facebook, patch.object(reporting, "GoogleAdsClient") as google, \
                 patch.object(reporting, "WeatherClient") as weather, patch.object(Path, "mkdir") as mkdir, \
                 patch.object(reporting, "WEATHER_SETTINGS", {"enabled": True, "locations": ["synthetic"]}):
                exporter = reporting.BizniWebExporter(source.API_URL, "", project_name="vevo")
                for mocked in (transport, client, facebook, google, weather):
                    mocked.assert_called_once()
                self.assertEqual(3, mkdir.call_count)
                self.assertIs(exporter.client, client.return_value)
                self.assertTrue(exporter.enable_period_bundle)
                self.assertEqual("vevo", os.environ["REPORT_PROJECT"])
                self.assertEqual(str((Path("data") / "vevo").resolve()), os.environ["REPORT_DATA_DIR"])
                for invalid in (None, 0, 1, "true"):
                    with self.assertRaisesRegex(ValueError, "must be boolean"):
                        reporting.BizniWebExporter(source.API_URL, "", order_facts_only=invalid)

    def test_pii_free_projection_preserves_shared_financial_and_lifecycle_facts(self):
        from reporting_core.experiment_orders import build_biznisweb_authoritative_orders
        with patch("dotenv.load_dotenv", return_value=False), patch.dict(os.environ, {"REPORT_PROJECT": "vevo"}):
            import export_orders as reporting
            with patch.object(reporting, "RequestsHTTPTransport"), patch.object(reporting, "Client"), \
                 patch.object(reporting, "FacebookAdsClient"), patch.object(reporting, "GoogleAdsClient"), \
                 patch.object(reporting, "WeatherClient"), patch.object(Path, "mkdir"):
                ordinary = reporting.BizniWebExporter(source.API_URL, "", project_name="vevo")
                calculator = reporting.BizniWebExporter(source.API_URL, "", project_name="vevo", order_facts_only=True)
            for status in ("Platba online - zaplatené", "Nová", "Stornovaná"):
                projected = source_order("123")
                projected["status"]["name"] = status
                original = copy.deepcopy(projected)
                original.update(customer={"name": "Synthetic", "email": "synthetic@example.invalid"},
                                invoice_address={"city": "Synthetic"}, delivery_address={"city": "Synthetic"})
                options = dict(completion_receipts={"123": source.now_utc() - timedelta(days=22)},
                               generated_at=source.now_utc(), maturity_checkpoint_days=21,
                               packaging_cost_eur=0.50, shipping_net_cost_eur=1.00)
                expected = build_biznisweb_authoritative_orders(ordinary, [original], **options)
                actual = build_biznisweb_authoritative_orders(calculator, [projected], **options)
                with self.subTest(status=status):
                    self.assertEqual(expected, actual)
                    self.assertNotIn("synthetic@example.invalid", json.dumps(actual))
                    if status == "Platba online - zaplatené":
                        self.assertEqual("realized", actual[0]["lifecycle_state"])
                        self.assertEqual(20.0, actual[0]["net_revenue_eur"])
                    else:
                        self.assertEqual(0.0, actual[0]["net_revenue_eur"])

    def test_workflow_gates_before_credentials_and_uploads_one_file(self):
        workflow = (ROOT / source.WORKFLOW).read_text(encoding="utf-8")
        self.assertLess(workflow.index("--gate-only"), workflow.index("configure-aws-credentials"))
        self.assertIn("github.ref == 'refs/heads/main'", workflow)
        self.assertIn("test \"$CONFIRM_SOURCE\" = 'true'", workflow)
        self.assertIn("cancel-in-progress: false", workflow)
        self.assertEqual(1, workflow.count("actions/upload-artifact@"))
        self.assertIn("retention-days: 90", workflow)
        self.assertIn("vevo-aa-quality-${{ github.run_id }}/vevo-growthbook-aa-quality-source.json", workflow)
        for operation in ("run-task", "update-service", "put-object", "register-task-definition", "workflow_run:"):
            self.assertNotIn(operation, workflow)

    def test_prepared_source_cannot_read_until_consumer_migration_is_reviewed(self):
        with patch("scripts.record_growthbook_aa_evidence_gates.EXACT_WINDOW_SOURCE_CAPTURE_SUPPORTED", False), \
             patch.object(source, "make_plan") as make_plan:
            with self.assertRaisesRegex(ValueError, "consumer migration"):
                source.load_inputs()
            make_plan.assert_not_called()

    def test_head_and_dirty_checkout_are_rejected(self):
        for outputs in ((b"f" * 40, b""), (ENV["GITHUB_SHA"].encode(), b" M source.py")):
            with patch.object(source.subprocess, "run", side_effect=[Mock(returncode=0, stdout=value) for value in outputs]):
                with self.assertRaises(ValueError):
                    source.verify_checkout(ENV["GITHUB_SHA"])

    def test_independent_health_run_zip_and_json_are_all_bound(self):
        from tests.test_growthbook_aa_infra_health_evidence import health_evidence
        value = health_evidence(post_run=True)
        value["observed_at_utc"] = source.stamp(source.now_utc())
        value["phase"]["checked_due_local"] = "2026-09-05T03:45:00+02:00"
        value["provenance"].update(workflow_run_id=ENV["HEALTH_RUN_ID"], main_commit=ENV["GITHUB_SHA"])
        raw = source.canonical_health_bytes(value)
        sha = hashlib.sha256(raw).hexdigest()
        archive = io.BytesIO()
        with zipfile.ZipFile(archive, "w") as zipped:
            zipped.writestr(source.HEALTH_ARTIFACT + ".json", raw)
        blob = archive.getvalue()
        run = {"id": int(ENV["HEALTH_RUN_ID"]), "head_sha": ENV["GITHUB_SHA"], "head_branch": "main",
               "path": source.HEALTH_WORKFLOW, "status": "completed", "conclusion": "success"}
        listing = {"total_count": 1, "artifacts": [{"id": 123456, "name": source.HEALTH_ARTIFACT, "expired": False,
                                                   "digest": "sha256:" + hashlib.sha256(blob).hexdigest()}]}
        with patch.object(source, "gh_json", side_effect=[run, listing]), \
             patch.object(source.subprocess, "run", return_value=Mock(returncode=0, stdout=blob)):
            self.assertEqual(value, source.download_health(ENV["HEALTH_RUN_ID"], ENV["GITHUB_SHA"], sha))
        for key, invalid in (("head_sha", "f" * 40), ("head_branch", "other"), ("path", "other.yml"), ("conclusion", "failure")):
            with patch.object(source, "gh_json", return_value={**run, key: invalid}), \
                 patch.object(source.subprocess, "run") as fetch:
                with self.assertRaises(ValueError):
                    source.download_health(ENV["HEALTH_RUN_ID"], ENV["GITHUB_SHA"], sha)
                fetch.assert_not_called()
        for broken_listing, expected_hash, payload in (({**listing, "total_count": 2}, sha, blob),
            ({"total_count": 1, "artifacts": [{**listing["artifacts"][0], "digest": "sha256:" + "f" * 64}]}, sha, blob),
            (listing, "f" * 64, blob)):
            with patch.object(source, "gh_json", side_effect=[run, broken_listing]), \
                 patch.object(source.subprocess, "run", return_value=Mock(returncode=0, stdout=payload)):
                with self.assertRaises(ValueError):
                    source.download_health(ENV["HEALTH_RUN_ID"], ENV["GITHUB_SHA"], expected_hash)

    def test_full_synthetic_capture_uses_real_adapters_without_network_or_publishing(self):
        current, activation, reconciliation, clients, state = fake_runtime()
        current = replace(current, eligible=1)
        first = event(received_at=current.window.from_utc)
        purchase = event("order_completed", received_at=current.window.from_utc + timedelta(minutes=1),
                         device_id=first["device_id"], transaction_id="123")
        memory = MemoryS3([first, purchase])
        clients["s3"].list_objects_v2.side_effect = memory.list_objects_v2
        clients["s3"].get_object.side_effect = memory.get_object
        clients["ssm"] = Mock()
        clients["ssm"].get_parameter.return_value = {"Parameter": {"ARN": TOKEN_REF, "Type": "SecureString", "Value": "x" * 32}}
        clients["logs"] = Mock()
        clients["logs"].describe_log_groups.return_value = {"logGroups": [{"logGroupName": "test-collector-logs", "retentionInDays": 180}]}
        clients["logs"].filter_log_events.return_value = {"events": [{"eventId": f"log-{i}",
            "timestamp": int(source.utc(row["received_at"]).timestamp() * 1000),
            "message": json.dumps({"schema_version": 1, "marker": "VEVO_GROWTHBOOK_COLLECTOR_RECEIPT", "accepted": True, "duplicate": False})}
            for i, row in enumerate([first, purchase])]}
        session = MagicMock()
        session.__enter__.return_value = session
        response = session.post.return_value.__enter__.return_value
        response.status_code, response.url = 200, source.API_URL
        response.headers = {"Content-Type": "application/json"}
        response.iter_content.return_value = [json.dumps({"data": {"getOrder": source_order("123")}}).encode()]
        phases = []
        with patch("dotenv.load_dotenv", return_value=False), patch("requests.Session", return_value=session), \
             patch.object(source.time, "sleep"), patch.object(Path, "mkdir") as mkdir, \
             patch.dict(os.environ, {"REPORT_PROJECT": "vevo", "BIZNISWEB_API_TOKEN": "", "VEVO_BIZNISWEB_API_TOKEN": ""}):
            result = source.collect(current, clients.__getitem__, activation, reconciliation,
                                    {"workflow_run_id": current.health_run_id, "main_commit": current.main_commit, "sha256": current.health_sha256},
                                    progress=phases.append)
            mkdir.assert_not_called()
        self.assertEqual(list(source.CAPTURE_PHASES), phases)
        validate_capture(result, current)
        self.assertEqual(1, result["source"]["quality"]["eligible_device_count"])
        self.assertEqual(1, result["source"]["quality"]["exact_joined_transaction_count"])
        self.assertEqual(2, session.post.call_count)
        self.assertFalse(session.trust_env)
        clients["s3"].put_object.assert_not_called()
        clients["ecs"].run_task.assert_not_called()
        serialized = canonical_source_bytes(result).decode()
        for private in (first["device_id"], first["event_id"], purchase["event_id"], TOKEN_REF, "x" * 32):
            self.assertNotIn(private, serialized)


if __name__ == "__main__":
    unittest.main()
