"""Independent offline failure probes; no real AWS/provider client is used."""
from copy import deepcopy
from fnmatch import fnmatchcase
import hashlib
from io import BytesIO
import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from scripts import deploy_vevo_report as deploy
from scripts import reporting_migration_host_gate as host
from scripts import reporting_readiness as readiness


class IamError(Exception):
    def __init__(self, code):
        self.response = {"Error": {"Code": code}}


class EventuallyConsistentIam:
    def __init__(self, visibility):
        self.visibility = list(visibility)
        self.role = None
        self.deleted = False
        self.create_role = Mock(side_effect=self.create)
        self.delete_role = Mock(side_effect=self.delete)
        self.list_role_policies = Mock(return_value={"PolicyNames": []})
        self.list_attached_role_policies = Mock(return_value={"AttachedPolicies": []})

    def create(self, **request):
        self.role = {"Arn": f"arn:aws:iam::{deploy.ACCOUNT}:role/" + request["RoleName"],
                     "Tags": request["Tags"], "AssumeRolePolicyDocument": json.loads(request["AssumeRolePolicyDocument"])}
        raise TimeoutError("synthetic committed create lost acknowledgement")

    def get_role(self, **_request):
        if self.role is None or self.deleted:
            raise IamError("NoSuchEntity")
        state = self.visibility.pop(0) if self.visibility else "owned"
        if state == "missing":
            raise IamError("NoSuchEntity")
        if state == "denied":
            raise IamError("AccessDenied")
        role = deepcopy(self.role)
        if state == "foreign":
            role["Tags"] = [{"Key": "ManagedReportProbe", "Value": "foreign-owner"}]
        return {"Role": role}

    def delete(self, **request):
        assert self.role["Arn"].endswith("/" + request["RoleName"])
        self.deleted = True


class IndependentRoleReviewTests(unittest.TestCase):
    def deployment(self, visibility):
        obj = object.__new__(deploy.Deployment)
        obj.release_id = "d" * 32
        obj.role_created = obj.role_attempted = False
        obj.cleanup_task = Mock()
        obj.sleep = Mock()
        obj.iam = EventuallyConsistentIam(visibility)
        return obj

    def test_lost_create_ack_and_initial_absence_find_and_delete_only_owned_role(self):
        obj = self.deployment(["missing", "owned"])
        with self.assertRaises(TimeoutError):
            obj.create_role()
        self.assertTrue(obj.role_created)
        obj.cleanup_role()
        obj.cleanup_task.assert_called_once()
        obj.iam.create_role.assert_called_once()
        obj.iam.delete_role.assert_called_once()
        self.assertTrue(obj.iam.deleted)
        self.assertFalse(obj.role_created or obj.role_attempted)

    def test_absence_after_brief_visibility_does_not_masquerade_as_deletion(self):
        obj = self.deployment(["missing", "owned", "missing", "owned"])
        with self.assertRaises(TimeoutError):
            obj.create_role()
        self.assertTrue(obj.role_created)
        obj.cleanup_role()
        obj.iam.delete_role.assert_called_once()
        self.assertTrue(obj.iam.deleted)
        obj.iam.create_role.assert_called_once()

    def test_persistent_absence_retains_unknown_create_and_never_replays(self):
        obj = self.deployment(["missing"] * 100)
        with self.assertRaisesRegex(RuntimeError, "create-unconfirmed"):
            obj.create_role()
        self.assertTrue(obj.role_attempted)
        with self.assertRaisesRegex(RuntimeError, "create-unconfirmed"):
            obj.cleanup_role()
        self.assertTrue(obj.role_attempted)
        obj.iam.create_role.assert_called_once()
        obj.iam.delete_role.assert_not_called()
        self.assertEqual(60, obj.sleep.call_count)

    def test_foreign_role_cannot_be_deleted_or_clear_unknown_create(self):
        obj = self.deployment(["foreign", "foreign"])
        for action in (obj.create_role, obj.cleanup_role):
            with self.assertRaisesRegex(RuntimeError, "ownership-invalid"):
                action()
            self.assertTrue(obj.role_attempted)
        obj.iam.delete_role.assert_not_called()
        obj.iam.create_role.assert_called_once()

    def test_unknown_read_error_retains_attempt_and_later_cleanup_is_bounded(self):
        obj = self.deployment(["denied", "missing", "owned"])
        with self.assertRaisesRegex(RuntimeError, "read-uncertain"):
            obj.create_role()
        self.assertTrue(obj.role_attempted)
        obj.cleanup_role()
        obj.iam.delete_role.assert_called_once()
        obj.iam.create_role.assert_called_once()


class IndependentHostReviewTests(unittest.TestCase):
    required = {"REPORT_PROJECT": "vevo", "REPORT_SKIP_INVOICES": "true",
                "REPORT_SKIP_CREDITNOTE_STORNO_GUARD": "true", "REPORT_SKIP_EMAIL": "true",
                "REPORT_S3_BUCKET": host.BUCKET, "REPORT_S3_PREFIX": "daily-reports/vevo"}

    def test_swallowed_forbidden_native_attempt_cannot_publish_success(self):
        import daily_report_runner as runner
        import requests

        def ordinary_path():
            for method, path in (("POST", "/erp/orders/invoices/pay"),
                                 ("GET", "/erp/orders/invoices/finalize/123"),
                                 ("POST", "/erp/orders/creditnotes/storno")):
                try:
                    requests.Session().send(requests.Request(method, "https://vevo.flox.sk" + path).prepare())
                except RuntimeError:
                    pass
            runner.s3_upload_outputs("vevo", {})

        with patch.dict(host.os.environ, self.required), patch.object(runner, "main", ordinary_path), \
             patch.object(host, "isolate_outputs", return_value={"key": "fixture", "sha256": "fixture"}), \
             patch.object(requests.Session, "send") as network:
            with self.assertRaisesRegex(RuntimeError, "report-not-verified"):
                host.report_probe(object(), host.PREFIX + "a" * 32 + "/", "a" * 32)
        network.assert_not_called()

    def test_only_supported_native_read_paths_reach_transport(self):
        import daily_report_runner as runner
        import requests
        routes = (("GET", "/erp/main/login"), ("POST", "/admin/login/authenticate/"),
                  ("POST", "/erp/orders/creditnotes/getListJson"), ("GET", "/erp/main/"))

        def ordinary_path():
            for method, path in routes:
                requests.Session().send(requests.Request(method, "https://vevo.flox.sk" + path).prepare())
            runner.s3_upload_outputs("vevo", {})

        with patch.dict(host.os.environ, self.required), patch.object(runner, "main", ordinary_path), \
             patch.object(host, "isolate_outputs", return_value={"key": "fixture", "sha256": "fixture"}), \
             patch.object(requests.Session, "send", return_value=SimpleNamespace(status_code=200)) as network:
            self.assertEqual("fixture", host.report_probe(object(), host.PREFIX + "a" * 32 + "/", "a" * 32)["key"])
        self.assertEqual(len(routes), network.call_count)

    def test_diagnostic_role_cannot_write_live_state_authorization_or_another_probe(self):
        release_id = "a" * 32
        policy = deploy.diagnostic_policy(release_id)
        statements = policy["Statement"]
        self.assertEqual({"s3:GetObject", "s3:PutObject"}, {action for row in statements for action in row["Action"]})
        put = [row for row in statements if "s3:PutObject" in row["Action"]]
        self.assertEqual(1, len(put))
        self.assertEqual({"StringEquals": {"s3:x-amz-server-side-encryption": "AES256"}}, put[0]["Condition"])
        def allowed(key):
            return any(fnmatchcase(f"arn:aws:s3:::{host.BUCKET}/" + key, pattern) for pattern in put[0]["Resource"])
        for suffix in ("markers/complete.json", "artifacts/output-manifest.json"):
            self.assertTrue(allowed(host.PREFIX + release_id + "/" + suffix))
        for key in ("daily-reports/vevo/report_latest.html", "data/vevo/order-automation/state.json",
                    "data/vevo/reporting/runtime/current.json", host.PREFIX + release_id + "/authorize.json",
                    host.PREFIX + "b" * 32 + "/markers/ready.json"):
            self.assertFalse(allowed(key), key)


class IndependentReadinessReferenceTests(unittest.TestCase):
    def fixture(self):
        commit = "a" * 40
        proof = {"schema": 1, "phase": "promotion-readback-verified", "commit": commit,
                 "created_at": "2026-09-13T10:00:01+00:00"}
        run = {"id": 12345, "head_sha": commit, "head_branch": "main", "event": "workflow_dispatch",
               "repository": {"full_name": "vzeman/biznisweb"},
               "path": ".github/workflows/deploy-order-automations.yml", "status": "completed", "conclusion": "success",
               "run_started_at": "2026-09-13T10:00:00+00:00", "updated_at": "2026-09-13T11:00:00+00:00"}
        reference = {"key": f"data/roy/order-automation/deployments/{commit}/{'b' * 32}.json",
                     "workflow_run_id": "12345"}
        return proof, run, reference

    def read(self, proof, run, reference):
        raw = json.dumps(proof).encode()
        body = BytesIO(raw)
        ref = {**reference, "sha256": hashlib.sha256(raw).hexdigest()}
        s3 = Mock()
        s3.get_object.return_value = {"ServerSideEncryption": "AES256", "Body": body}
        try:
            return readiness.read_proof(s3, ref, "primary_release", Mock(return_value=run))
        finally:
            self.assertTrue(body.closed)
            s3.get_object.assert_called_once_with(Bucket=readiness.BUCKET, Key=reference["key"],
                                                  ExpectedBucketOwner=readiness.ACCOUNT)

    def test_correct_hash_does_not_make_failed_or_unpromoted_receipt_authoritative(self):
        proof, run, reference = self.fixture()
        self.assertEqual(proof, self.read(proof, run, reference))
        for changes in ({"phase": "ready-to-promote"}, {"phase": "deployment-failed-check-rollback"},
                        {"rollback": "restored"}, {"reports_retained_paused": True}, {"schema": True}):
            with self.subTest(changes=changes), self.assertRaises(readiness.binding.BindingError):
                self.read({**proof, **changes}, run, reference)

    def test_managed_run_failure_wrong_workflow_or_wrong_source_rejects_same_valid_bytes(self):
        proof, run, reference = self.fixture()
        for changes in ({"status": "in_progress", "conclusion": None}, {"conclusion": "failure"},
                        {"head_sha": "f" * 40}, {"path": ".github/workflows/deploy-vevo-report.yml"},
                        {"event": "push"}, {"run_started_at": "2026-09-13T10:00:02+00:00"}):
            with self.subTest(changes=changes), self.assertRaises(readiness.binding.BindingError):
                self.read(proof, {**run, **changes}, reference)


class IndependentFullReadinessTests(unittest.TestCase):
    def fixture(self):
        from tests.test_reporting_readiness import ReadinessFixture
        return ReadinessFixture()

    def reject(self, fixture):
        fixture.reseal()
        with self.assertRaises(readiness.binding.BindingError):
            fixture.validate()
        self.assertTrue(all(body.closed for body in fixture.bodies))

    def test_positive_full_chain_uses_real_validator_and_closes_all_private_bodies(self):
        fixture = self.fixture()
        self.assertIs(fixture.receipt, fixture.validate())
        self.assertTrue(all(body.closed for body in fixture.bodies))

    def test_contradictory_independent_promotion_or_embedded_managed_record_rejects(self):
        for project in ("primary_audit", "guard_audit"):
            for contradiction in ("promotion-false", "different-managed-latest"):
                fixture = self.fixture()
                audit = getattr(fixture, project)
                if contradiction == "promotion-false":
                    audit["release_promoted"] = False
                else:
                    audit["managed_latest"]["phase"] = "deployment-failed-check-rollback"
                with self.subTest(project=project, contradiction=contradiction):
                    self.reject(fixture)

    def test_guard_live_failure_cannot_be_resealed_into_success_even_when_audit_agrees(self):
        for changes in ({"dry_run": True}, {"creditnote_scan_complete": False}, {"skipped_locked": True},
                        {"failed_orders": 1}, {"review_required_orders": 1}, {"audit_error_orders": True},
                        {"updated_orders": True}):
            fixture = self.fixture()
            live = next(row for row in fixture.guards["hosts"] if row["kind"] == "guard-live")
            live["summary"].update(changes)
            audited = fixture.guard_audit["final"]["hosts"][live["task"]]
            audited["managed_host"] = deepcopy(live)
            audited["summaries"] = [deepcopy(live["summary"])]
            fixture.guard_audit["managed_latest"] = deepcopy(fixture.guards)
            with self.subTest(changes=changes):
                self.reject(fixture)

    def test_primary_incomplete_drain_or_nonfull_invoice_probe_cannot_be_resealed(self):
        for scenario in ("short-drain", "unfinished-drain", "nonfull-invoice-probe"):
            fixture = self.fixture()
            if scenario == "nonfull-invoice-probe":
                host_row = next(row for row in fixture.primary["hosts"] if row["service"] == "vevo-invoice-daily")
                host_row["marker"].pop("full_backlog")
                audited = fixture.primary_audit["final"]["hosts"][host_row["task"]]
                audited["managed_host"] = deepcopy(host_row)
                audited["markers"] = [deepcopy(host_row["marker"])]
            else:
                fixture.primary["drain"].update({"quiet_seconds": 119} if scenario == "short-drain" else {"unfinished_tasks": 1})
                fixture.primary_audit["final"]["drain"] = deepcopy(fixture.primary["drain"])
            fixture.primary_audit["managed_latest"] = deepcopy(fixture.primary)
            with self.subTest(scenario=scenario):
                self.reject(fixture)

    def test_fresh_actual_host_disagreement_cannot_be_hidden_by_valid_historical_hashes(self):
        for scenario in ("not-stopped", "wrong-task", "wrong-image", "false-zero", "wrong-ip"):
            fixture = self.fixture()
            task = fixture.tasks[fixture.primary["hosts"][0]["task"]]
            if scenario == "not-stopped":
                task["lastStatus"] = "RUNNING"
            elif scenario == "wrong-task":
                task["taskArn"] = task["taskArn"][:-1] + "f"
            elif scenario == "wrong-image":
                task["containers"][0]["imageDigest"] = "sha256:" + "f" * 64
            elif scenario == "false-zero":
                task["containers"][0]["exitCode"] = False
            else:
                task["containers"][0]["networkInterfaces"][0]["privateIpv4Address"] = "172.31.9.9"
            with self.subTest(scenario=scenario):
                self.reject(fixture)
