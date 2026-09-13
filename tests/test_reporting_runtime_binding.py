from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
import textwrap
import unittest
from unittest.mock import Mock, patch

from scripts import reporting_runtime_binding as b
from scripts import bootstrap_reporting_runtime as bootstrap
from scripts.validate_growthbook_aa_infra_health_evidence import InfraHealthEvidenceError, validate_health_evidence
from tests.test_growthbook_aa_infra_health_evidence import DEPLOY, DEPLOY_BYTES, health_evidence

NOW = datetime(2026, 9, 13, 10, tzinfo=timezone.utc)
RELEASE = "a" * 32


class StoreError(Exception):
    def __init__(self, code):
        self.response = {"Error": {"Code": code}}


class MemoryS3:
    def __init__(self):
        self.objects, self.reads, self.writes, self.bodies = {}, [], [], []
        self.policy_error = None
        self.number = 0
        self.before_get = None
        self.before_put = None

    def head_bucket(self, **kwargs):
        assert kwargs == {"Bucket": b.BUCKET, "ExpectedBucketOwner": b.ACCOUNT}
        return {}

    def get_bucket_location(self, **kwargs):
        return {"LocationConstraint": b.REGION}

    def get_public_access_block(self, **kwargs):
        return {"PublicAccessBlockConfiguration": {k: True for k in (
            "BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets")}}

    def get_bucket_policy_status(self, **kwargs):
        if self.policy_error:
            raise StoreError(self.policy_error)
        return {"PolicyStatus": {"IsPublic": False}}

    def get_bucket_acl(self, **kwargs):
        return {"Owner": {"ID": "owner"}, "Grants": [{"Grantee": {"Type": "CanonicalUser", "ID": "owner"}, "Permission": "FULL_CONTROL"}]}

    def get_object(self, **kwargs):
        assert kwargs["ExpectedBucketOwner"] == b.ACCOUNT and kwargs["Bucket"] == b.BUCKET
        key = kwargs["Key"]
        self.reads.append(key)
        if self.before_get:
            self.before_get(key)
        if key not in self.objects:
            raise StoreError("NoSuchKey")
        raw, etag, encryption = self.objects[key]
        body = io.BytesIO(raw)
        self.bodies.append(body)
        return {"Body": body, "ContentLength": len(raw), "ETag": etag, "ServerSideEncryption": encryption}

    def put_object(self, **kwargs):
        key = kwargs["Key"]
        assert kwargs["ServerSideEncryption"] == "AES256" and kwargs["ExpectedBucketOwner"] == b.ACCOUNT
        assert kwargs["Bucket"] == b.BUCKET
        if self.before_put:
            self.before_put(key)
        current = self.objects.get(key)
        if "IfMatch" in kwargs:
            if not current or kwargs["IfMatch"] != current[1]:
                raise StoreError("PreconditionFailed")
        elif kwargs.get("IfNoneMatch") != "*" or current:
            raise StoreError("PreconditionFailed")
        self.number += 1
        etag = f'"etag-{self.number}"'
        self.objects[key] = (kwargs["Body"], etag, "AES256")
        self.writes.append(key)
        return {"ETag": etag}


def baseline():
    definition = {
        "taskDefinitionArn": b.POLICY["baseline_definition"], "family": b.POLICY["family"], "networkMode": "awsvpc",
        "taskRoleArn": f"arn:aws:iam::{b.ACCOUNT}:role/BiznisWebReportingTaskRole-vevo",
        "executionRoleArn": f"arn:aws:iam::{b.ACCOUNT}:role/ecsTaskExecutionRole",
        "cpu": "1024", "memory": "4096", "requiresCompatibilities": ["FARGATE"],
        "containerDefinitions": [{"name": "reporting", "image": f"{b.ACCOUNT}.dkr.ecr.{b.REGION}.amazonaws.com/vevo-reporting@{b.POLICY['baseline_image_digest']}",
            "environment": [{"name": "REPORT_PROJECT", "value": "vevo"}],
            "secrets": [{"name": name, "valueFrom": f"arn:aws:secretsmanager:{b.REGION}:{b.ACCOUNT}:secret:vevo/reporting/runtime-env-abc123:{name}::"}
                        for name in ("BIZNISWEB_API_URL", "BIZNISWEB_API_TOKEN")]}],
    }
    schedule = {"Name": b.POLICY["service"], "GroupName": "default", "State": "ENABLED", "ScheduleExpression": "cron(0 1 * * ? *)",
                "ScheduleExpressionTimezone": "Europe/Bratislava", "FlexibleTimeWindow": {"Mode": "OFF"},
                "Target": {"Arn": b.POLICY["cluster"], "RoleArn": f"arn:aws:iam::{b.ACCOUNT}:role/vevo-report-scheduler",
                           "EcsParameters": {"TaskDefinitionArn": definition["taskDefinitionArn"], "LaunchType": "FARGATE", "TaskCount": 1}}}
    proof = {"task_arn": f"arn:aws:ecs:{b.REGION}:{b.ACCOUNT}:task/vevo-reporting-cluster/" + "b" * 32,
             "private_ip": "172.31.1.2", "definition_arn": definition["taskDefinitionArn"], "image_digest": b.POLICY["baseline_image_digest"],
             "instance_id": "N/A:Fargate", "service": b.POLICY["service"], "path": "/app", "localhost_marker_sha256": "c" * 64,
             "command": ["python", "-c", "verified_identity_probe"], "exit_code": 0, "stopped": True}
    return b.build_baseline_record(schedule, definition, release_id="baseline-" + RELEASE, workflow_run_id="34700000001",
                                   protected_sha256="d" * 64, candidate_proof=proof, verified_at=NOW.isoformat())


def promotion(previous):
    source = previous["record"]
    definition = copy.deepcopy(source["task_definition"])
    definition["taskDefinitionArn"] = definition["taskDefinitionArn"].rsplit(":", 1)[0] + ":101"
    image = f"{b.ACCOUNT}.dkr.ecr.{b.REGION}.amazonaws.com/vevo-reporting@sha256:" + "e" * 64
    definition["containerDefinitions"][0]["image"] = image
    definition["containerDefinitions"][0]["workingDirectory"] = "/app"
    definition["containerDefinitions"][0]["environment"].extend([
        {"name": "REPORT_SKIP_INVOICES", "value": "true"}, {"name": "REPORT_SKIP_CREDITNOTE_STORNO_GUARD", "value": "true"}])
    candidate = copy.deepcopy(definition)
    candidate["taskDefinitionArn"] = candidate["taskDefinitionArn"].rsplit(":", 1)[0] + ":100"
    candidate["taskRoleArn"] = f"arn:aws:iam::{b.ACCOUNT}:role/VevoReportProbe-{RELEASE}"
    command = ["python", "scripts/reporting_migration_host_gate.py", "--release-id", RELEASE,
               "--source-commit", "f" * 40, "--image-digest", "sha256:" + "e" * 64, "--gate-sha256", "1" * 64]
    candidate["containerDefinitions"][0]["command"] = command
    candidate["containerDefinitions"][0]["environment"].append({"name": "REPORT_SKIP_EMAIL", "value": "true"})
    schedule = copy.deepcopy(source["schedule"])
    schedule["Target"]["EcsParameters"]["TaskDefinitionArn"] = definition["taskDefinitionArn"]
    proof = {**source["candidate_proof"], "definition_arn": candidate["taskDefinitionArn"], "image_digest": "sha256:" + "e" * 64,
             "command": command, "output_manifest_key": b.PREFIX + f"probes/{RELEASE}/artifacts/report.json", "output_manifest_sha256": "2" * 64,
             "provider_writes": False, "email_sent": False, "live_outputs_changed": False, "skip_invoices": True, "skip_inline_guard": True}
    return b.build_promotion_record(previous, schedule, definition, release_id=RELEASE, source_commit="f" * 40, image=image,
                                    workflow_run_id="34700000002", build_run_id="34700000003", protected_sha256="d" * 64,
                                    candidate_proof=proof, candidate_task_definition=candidate, verified_at=NOW.isoformat())


def loaded(record):
    digest = b.sha256(record)
    return {"record": record, "record_key": b.PREFIX + f"releases/{digest}.json", "record_sha256": digest, "pointer_etag": '"initial"'}


class RuntimeBindingTests(unittest.TestCase):
    def setUp(self):
        self.s3 = MemoryS3()
        self.record = baseline()

    def establish(self):
        lease = b.MigrationLease(self.s3, owner=self.record["release_id"], now=lambda: NOW).acquire()
        result = b.publish_verified_binding(self.s3, self.record, expected_pointer_etag=None, lease=lease)
        lease.release()
        return result

    def test_baseline_then_isolated_candidate_promotion_exact_readback(self):
        previous = self.establish()
        lease = b.MigrationLease(self.s3, owner=RELEASE, now=lambda: NOW).acquire()
        record = promotion(previous)
        result = b.publish_verified_binding(self.s3, record, expected_pointer_etag=previous["pointer_etag"], lease=lease)
        with self.assertRaisesRegex(b.BindingError, "migration-active"):
            b.load_current_binding(self.s3)
        lease.release()
        self.assertEqual(b.load_current_binding(self.s3), result)
        b.validate_runtime(result, record["schedule"], record["task_definition"])
        self.assertTrue(all(body.closed for body in self.s3.bodies))
        self.assertTrue(all(key.startswith(b.PREFIX) for key in self.s3.writes))

    def test_missing_current_pointer_never_falls_back_to_old_revision(self):
        with self.assertRaises(StoreError):
            b.load_current_binding(self.s3)
        self.assertEqual(self.s3.writes, [])

    def test_only_explicit_missing_policy_is_accepted(self):
        self.s3.policy_error = "NoSuchBucketPolicy"
        b.require_private_bucket(self.s3)
        self.s3.policy_error = "AccessDenied"
        with self.assertRaises(StoreError):
            b.require_private_bucket(self.s3)

    def test_public_bucket_is_blocked_before_writes(self):
        self.s3.get_public_access_block = Mock(return_value={"PublicAccessBlockConfiguration": {}})
        with self.assertRaises(b.BindingError):
            b.MigrationLease(self.s3, owner=RELEASE).acquire()
        self.assertEqual(self.s3.writes, [])

    def test_bad_encryption_and_duplicate_json_close_body(self):
        for raw, encryption in ((b.canonical_bytes({"x": 1}), "aws:kms"), (b'{"x":1,"x":2}\n', "AES256")):
            self.s3.objects[b.CURRENT_KEY] = (raw, '"e"', encryption)
            with self.assertRaises(b.BindingError):
                b.read_object(self.s3, b.CURRENT_KEY)
            self.assertTrue(self.s3.bodies[-1].closed)

    def test_status_image_or_schedule_drift_cannot_pass_by_family(self):
        value = loaded(self.record)
        for field in ("schedule", "task_definition"):
            other = copy.deepcopy(self.record[field])
            if field == "schedule":
                other["Target"]["EcsParameters"]["TaskDefinitionArn"] += "9"
            else:
                other["containerDefinitions"][0]["image"] = "latest"
            with self.assertRaises(b.BindingError):
                b.validate_runtime(value, other if field == "schedule" else self.record["schedule"],
                                   other if field == "task_definition" else self.record["task_definition"])

    def test_promotion_requires_real_report_and_safe_distinct_candidate(self):
        record = promotion(loaded(self.record))
        mutations = [
            lambda r: r["candidate_proof"].update(email_sent=True),
            lambda r: r["candidate_proof"].update(live_outputs_changed=True),
            lambda r: r["candidate_proof"].update(exit_code=True),
            lambda r: r["candidate_proof"].update(output_manifest_key="daily-reports/vevo/latest/generation.json"),
            lambda r: r["candidate_task_definition"].update(taskRoleArn=r["task_definition"]["taskRoleArn"]),
            lambda r: r["candidate_task_definition"]["containerDefinitions"][0]["secrets"][0].update(valueFrom="foreign"),
            lambda r: r["candidate_task_definition"]["containerDefinitions"][0]["environment"].append({"name": "REPORT_S3_PREFIX", "value": "other"}),
            lambda r: r["candidate_task_definition"].update(memory="9999"),
            lambda r: r["candidate_proof"].update(command=["bash", "-c", "run anything"]),
        ]
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                changed = copy.deepcopy(record)
                mutate(changed)
                with self.assertRaises((b.BindingError, ValueError)):
                    b.validate_record(changed)

    def test_input_overrides_are_preserved_but_no_arbitrary_command_accepted(self):
        record = promotion(loaded(self.record))
        record["schedule"]["Target"]["Input"] = json.dumps({"containerOverrides": [{"name": "reporting", "environment": [
            {"name": "REPORT_SKIP_CREDITNOTE_STORNO_GUARD", "value": "true"}]}]})
        b.validate_record(record)
        record["schedule"]["Target"]["Input"] = '{"containerOverrides":[{"name":"reporting","command":["anything"]}]}'
        with self.assertRaises(b.BindingError):
            b.validate_record(record)

    def test_cross_project_secret_or_control_secret_rejected(self):
        for name, reference in (("BIZNISWEB_API_URL", "roy/reporting/token"), ("REPORT_SKIP_EMAIL", "vevo/reporting/token")):
            record = copy.deepcopy(self.record)
            for field in ("task_definition", "candidate_task_definition"):
                record[field]["containerDefinitions"][0]["secrets"].append({"name": name, "valueFrom": f"arn:aws:secretsmanager:{b.REGION}:{b.ACCOUNT}:secret:{reference}"})
            with self.assertRaises(b.BindingError):
                b.validate_record(record)

    def test_lease_expiry_is_not_permission_to_steal(self):
        lease = b.MigrationLease(self.s3, owner=RELEASE, now=lambda: NOW).acquire()
        with self.assertRaises(b.BindingError):
            b.MigrationLease(self.s3, owner="b" * 32, now=lambda: NOW + timedelta(days=2)).acquire()
        lease.now = lambda: NOW + timedelta(days=2)
        with self.assertRaises(b.BindingError):
            lease.renew()
        lease.retain_uncertain()
        with self.assertRaises(b.BindingError):
            b.check_migration(self.s3)

    def test_lease_renewal_and_ownership_prevent_foreign_release(self):
        lease = b.MigrationLease(self.s3, owner=RELEASE, now=lambda: NOW).acquire()
        old = lease.etag
        lease.renew()
        self.assertNotEqual(old, lease.etag)
        impostor = b.MigrationLease(self.s3, owner="b" * 32, now=lambda: NOW)
        impostor.etag = lease.etag
        with self.assertRaises(b.BindingError):
            impostor.release()
        lease.release()
        b.check_migration(self.s3)

    def test_pointer_cas_failure_preserves_old_authority_and_blocks_reader(self):
        previous = self.establish()
        lease = b.MigrationLease(self.s3, owner=RELEASE, now=lambda: NOW).acquire()
        def fail(key):
            if key == b.CURRENT_KEY:
                raise StoreError("PreconditionFailed")
        self.s3.before_put = fail
        with self.assertRaises(StoreError):
            b.publish_verified_binding(self.s3, promotion(previous), expected_pointer_etag=previous["pointer_etag"], lease=lease)
        self.s3.before_put = None
        self.assertEqual(b.load_current_binding(self.s3, lease=lease), previous)
        with self.assertRaises(b.BindingError):
            b.load_current_binding(self.s3)

    def test_wrong_predecessor_stops_before_any_release_write(self):
        previous = self.establish()
        lease = b.MigrationLease(self.s3, owner=RELEASE, now=lambda: NOW).acquire()
        record = promotion(previous)
        record["predecessor_sha256"] = "9" * 64
        writes = len(self.s3.writes)
        with self.assertRaises(b.BindingError):
            b.publish_verified_binding(self.s3, record, expected_pointer_etag=previous["pointer_etag"], lease=lease)
        self.assertEqual(len(self.s3.writes), writes)

    def test_rollback_restores_successful_pointer_and_keeps_append_only_receipt(self):
        previous = self.establish()
        lease = b.MigrationLease(self.s3, owner=RELEASE, now=lambda: NOW).acquire()
        current = b.publish_verified_binding(self.s3, promotion(previous), expected_pointer_etag=previous["pointer_etag"], lease=lease)
        proof = {"workflow_run_id": "34700000002", "observed_at": NOW.isoformat(),
                 "restored_schedule_sha256": b.sha256(previous["record"]["schedule"]),
                 "restored_definition_sha256": b.sha256(previous["record"]["task_definition"]),
                 "failed_record_sha256": current["record_sha256"]}
        result = b.restore_previous_binding(self.s3, previous, expected_pointer_etag=current["pointer_etag"], lease=lease, rollback_proof=proof)
        self.assertEqual(result["record"], previous["record"])
        self.assertEqual(result["record_sha256"], previous["record_sha256"])
        self.assertNotEqual(result["pointer_etag"], previous["pointer_etag"])
        self.assertEqual(len([key for key in self.s3.objects if "/rollbacks/" in key]), 1)
        self.assertIn(current["record_key"], self.s3.objects)
        lease.release()
        self.assertEqual(b.load_current_binding(self.s3), result)

    def test_rollback_cannot_overwrite_unrelated_pointer_or_wrong_readback(self):
        previous = self.establish()
        lease = b.MigrationLease(self.s3, owner=RELEASE, now=lambda: NOW).acquire()
        current = b.publish_verified_binding(self.s3, promotion(previous), expected_pointer_etag=previous["pointer_etag"], lease=lease)
        proof = {"workflow_run_id": "34700000002", "observed_at": NOW.isoformat(),
                 "restored_schedule_sha256": "0" * 64, "restored_definition_sha256": b.sha256(previous["record"]["task_definition"]),
                 "failed_record_sha256": current["record_sha256"]}
        writes = len(self.s3.writes)
        with self.assertRaises(b.BindingError):
            b.restore_previous_binding(self.s3, previous, expected_pointer_etag=current["pointer_etag"], lease=lease, rollback_proof=proof)
        proof["restored_schedule_sha256"] = b.sha256(previous["record"]["schedule"])
        with self.assertRaises(b.BindingError):
            b.restore_previous_binding(self.s3, previous, expected_pointer_etag='"foreign"', lease=lease, rollback_proof=proof)
        self.assertEqual(len(self.s3.writes), writes)
        self.assertEqual(b.load_current_binding(self.s3, lease=lease), current)

    def test_pointer_changes_mid_read_never_return_old_success(self):
        self.establish()
        count = 0
        def change(key):
            nonlocal count
            if key == b.CURRENT_KEY:
                count += 1
                if count == 2:
                    raw, _, enc = self.s3.objects[key]
                    self.s3.objects[key] = (raw, '"changed"', enc)
        self.s3.before_get = change
        with self.assertRaises(b.BindingError):
            b.load_current_binding(self.s3)

    def test_record_future_or_unverified_phase_is_not_promotion(self):
        record = promotion(loaded(self.record))
        record["phase"] = "candidate-passed"
        with self.assertRaises(b.BindingError):
            b.validate_record(record)

    def test_independent_run_and_build_provenance(self):
        record = promotion(loaded(self.record))
        run = {"id": 34700000002, "head_branch": "main", "head_sha": record["source_commit"], "path": ".github/workflows/deploy-vevo-report.yml",
               "event": "workflow_dispatch", "status": "in_progress", "conclusion": None, "repository": {"full_name": "vzeman/biznisweb"}}
        build = {**run, "id": 34700000003, "path": ".github/workflows/build-and-push-ecr.yml", "status": "completed", "conclusion": "success"}
        def fetch(run_id):
            return run if run_id == record["workflow_run_id"] else build
        b.verify_managed_provenance(loaded(record), fetch_run=fetch)
        for status, conclusion in (("completed", "failure"), ("completed", "cancelled"), ("queued", None)):
            run.update(status=status, conclusion=conclusion)
            with self.assertRaises(b.BindingError):
                b.verify_managed_provenance(loaded(record), fetch_run=fetch)
        run.update(status="completed", conclusion="success")
        build["head_sha"] = "0" * 40
        with self.assertRaises(b.BindingError):
            b.verify_managed_provenance(loaded(record), fetch_run=fetch)

    def test_schema3_binds_current_without_rewriting_old_schema(self):
        record = promotion(loaded(self.record))
        binding = loaded(record)
        old = health_evidence(post_run=True)
        raw = copy.deepcopy(old)
        validate_health_evidence(old, DEPLOY, deploy_evidence_bytes=DEPLOY_BYTES)
        new = copy.deepcopy(old)
        new["schema_version"] = 3
        new["observed_at_utc"] = "2026-09-13T10:00:01Z"
        new["provenance"]["current_reporting_binding_sha256"] = binding["record_sha256"]
        new["control"]["source_task_definition"] = record["task_definition"]["taskDefinitionArn"].rsplit("/", 1)[1]
        validate_health_evidence(new, DEPLOY, deploy_evidence_bytes=DEPLOY_BYTES, current_binding=binding)
        self.assertEqual(old, raw)
        with self.assertRaises(InfraHealthEvidenceError):
            validate_health_evidence(new, DEPLOY, deploy_evidence_bytes=DEPLOY_BYTES)
        new["control"]["source_task_definition"] = "vevo-reporting-daily:33"
        with self.assertRaises(InfraHealthEvidenceError):
            validate_health_evidence(new, DEPLOY, deploy_evidence_bytes=DEPLOY_BYTES, current_binding=binding)
        old["control"]["source_task_definition"] = "vevo-reporting-daily:101"
        with self.assertRaises(InfraHealthEvidenceError):
            validate_health_evidence(old, DEPLOY, deploy_evidence_bytes=DEPLOY_BYTES, current_binding=binding)

    def test_bootstrap_preview_never_acquires_lease_or_writes(self):
        scheduler = Mock(get_schedule=Mock(return_value=self.record["schedule"]))
        ecs = Mock(describe_task_definition=Mock(return_value={"taskDefinition": self.record["task_definition"]}))
        with patch.object(bootstrap, "verify_host"), patch.object(b, "now_utc", return_value=NOW):
            result = bootstrap.bootstrap(self.s3, scheduler, ecs, self.record, apply=False)
        self.assertFalse(result["applied"])
        self.assertEqual(self.s3.writes, [])

    def test_bootstrap_cannot_install_promotion_or_stale_approved_proof(self):
        for record in (promotion(loaded(self.record)), {**self.record, "verified_at": (NOW - timedelta(days=1)).isoformat()}):
            with patch.object(b, "now_utc", return_value=NOW), self.assertRaises(b.BindingError):
                bootstrap.bootstrap(self.s3, Mock(), Mock(), record, apply=False)
        self.assertEqual(self.s3.writes, [])

    def test_bootstrap_apply_requires_managed_source_and_rechecks_current_before_publish(self):
        scheduler = Mock(get_schedule=Mock(return_value=self.record["schedule"]))
        ecs = Mock(describe_task_definition=Mock(return_value={"taskDefinition": self.record["task_definition"]}))
        with patch.object(bootstrap, "verify_host"), patch.object(bootstrap, "require_managed_source") as source, patch.object(b, "now_utc", return_value=NOW):
            result = bootstrap.bootstrap(self.s3, scheduler, ecs, self.record, apply=True)
        self.assertTrue(result["applied"])
        self.assertEqual(source.call_count, 2)
        self.assertEqual(scheduler.get_schedule.call_count, 3)
        self.assertEqual(b.load_current_binding(self.s3)["record"], self.record)

    def test_bootstrap_apply_drift_keeps_blocking_lease_without_current_pointer(self):
        changed = copy.deepcopy(self.record["schedule"])
        changed["State"] = "DISABLED"
        scheduler = Mock(get_schedule=Mock(side_effect=[self.record["schedule"], changed]))
        ecs = Mock(describe_task_definition=Mock(return_value={"taskDefinition": self.record["task_definition"]}))
        with patch.object(bootstrap, "verify_host"), patch.object(bootstrap, "require_managed_source"), patch.object(b, "now_utc", return_value=NOW), self.assertRaises(b.BindingError):
            bootstrap.bootstrap(self.s3, scheduler, ecs, self.record, apply=True)
        self.assertNotIn(b.CURRENT_KEY, self.s3.objects)
        self.assertEqual(b.read_object(self.s3, b.LOCK_KEY)[0]["state"], "uncertain")


class RuntimeWorkflowTests(unittest.TestCase):
    def test_all_five_workflows_have_independent_entry_exit_gates_and_compile(self):
        names = ("monitor-vevo-growthbook-production-aa-infra", "check-vevo-growthbook-production-aa-window",
                 "check-vevo-growthbook-production-cta-window", "check-vevo-growthbook-production-cta-safety",
                 "build-vevo-growthbook-production-cta-final-snapshot")
        for name in names:
            with self.subTest(workflow=name):
                source = (b.ROOT / ".github/workflows" / (name + ".yml")).read_text()
                self.assertNotIn("vevo-reporting-daily:33", source)
                self.assertIn("validate_runtime(current_binding, source, current_binding['record']['task_definition'])", source)
                self.assertLess(source.index("Configure AWS credentials"), source.index("Verify independent current reporting runtime binding"))
                self.assertLess(source.index("Recheck unchanged current reporting authority before publication"), source.index("- name: Upload only"))
                self.assertIn('Path(name).unlink(missing_ok=True)', source)
                lines = source.splitlines()
                for i, line in enumerate(lines):
                    if "python - <<'PY'" in line:
                        end = next(j for j in range(i + 1, len(lines)) if lines[j].strip() == "PY")
                        compile(textwrap.dedent("\n".join(lines[i + 1:end])), name, "exec")

    def test_historical_exact_deployment_bytes_remain_bound(self):
        self.assertEqual(hashlib.sha256(DEPLOY_BYTES).hexdigest(), b.POLICY["historical_evidence_sha256"])
        source = (b.ROOT / "scripts/collect_growthbook_aa_quality_source.py").read_text()
        self.assertIn('source_definition_arn == reconciliation["source_runtime"]["task_definition"]', source)
        self.assertNotIn("reporting_runtime_binding", source)


if __name__ == "__main__":
    unittest.main()
