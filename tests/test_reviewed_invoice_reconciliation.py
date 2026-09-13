from copy import deepcopy
from datetime import datetime, timezone
from io import BytesIO
import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from invoice_automation_state import AutomationLeaseBusy, AutomationStateError, S3AutomationStateStore
from scripts.reconcile_reviewed_invoice_outcomes import (
    ACCOUNT, BUCKET, CLUSTER, EVIDENCE_SHA, RECONCILIATION, REVIEWED_RELEASE_COMMIT, REVIEWED_RELEASE_DIGEST, SCHEDULES,
    ReconciliationBlocked, command_for, load_cases, private_json, reconcile,
    require_source, response_fingerprint, validate_manifest, verify_release,
)
from tests.test_invoice_automation_state import MemoryS3


def order(number, final=False):
    return {"id": number, "order_num": number, "blocked": False, "status": {"id": "4", "name": "Shipped"},
            "sum": {"value": "25.00", "currency": {"code": "EUR"}}, "preinvoices": [{"id": number}],
            "invoices": [{"id": number, "invoice_num": f"TEST-{number}"}] if final else []}


def evidence():
    rows = []
    for idx in range(1, 7):
        number = str(1000 + idx)
        email = idx in {2, 3}
        prior = {"order_num": number, "phase": "email" if email else "create_ambiguous",
                 "attempted_at": "2026-09-10T08:00:00+00:00", "updated_at": "2026-09-10T11:22:00.500000+00:00"}
        if email:
            prior.update(email_state="ambiguous", invoice_id=number, invoice_num=f"TEST-{number}")
        if idx == 1:
            prior.update(email_policy="hold", source="complete_historical_backlog_audit")
        history = {"source": "https://roy.flox.sk/erp/main/orders", "timezone": "Europe/Bratislava",
                   "all_displayed_history_reviewed": True, "dialog_canceled_without_save": True,
                   "final_invoice_creation_events": int(email),
                   "invoice_email_events": ["2026-09-10T13:00:00", "2026-09-10T13:22:00"] if email else []}
        rows.append({"case": f"UNCERTAIN-{idx}", "order_num": number, "journal": prior,
                     "fresh": order(number, email), "admin_history_readback": history})
    return {"read_only": True, "cases": rows}


class Generator:
    def __init__(self, orders):
        self.orders = orders
        self.base_url = "https://example.test"
        self.arf_token = "private-fixture-token"
        self.client = SimpleNamespace(transport=SimpleNamespace(close=Mock()))
        self.web_session = None
        self.operation_journal = None
        self.send_invoice_email_enabled = False
        self.read_calls = []
        self.read_hook = None
        self.validate_session = Mock(return_value=True)
        self.resolve_eligible_status_ids = Mock(return_value={4})
        self.create_invoice = Mock(side_effect=AssertionError("Creation/preparation path is forbidden"))
        self._prepare_invoice = Mock(side_effect=AssertionError("Preparation path is forbidden"))
        self.send_invoice_email = Mock(side_effect=AssertionError("Email is forbidden"))

    def fetch_order_for_invoice(self, number):
        self.read_calls.append(number)
        if self.read_hook:
            self.read_hook(number, len(self.read_calls))
        return deepcopy(self.orders[number])

    def _status_is_eligible(self, current):
        return current["status"]["id"] == "4"


class Web:
    def __init__(self, generator):
        self.generator = generator
        self.calls = []
        self.close = Mock()
        self.outcome = "success"
        self.before_send = None
        self.retry_total = 0
        self.response_body = b'{success:true}'

    def get_adapter(self, prefix):
        return SimpleNamespace(max_retries=SimpleNamespace(total=self.retry_total))

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if self.before_send:
            self.before_send()
        number = url.rsplit("/", 1)[-1]
        if self.outcome in {"success", "lost_response"}:
            self.generator.orders[number]["invoices"] = [{"id": number, "invoice_num": f"TEST-{number}"}]
        if self.outcome in {"timeout", "lost_response"}:
            raise TimeoutError("PRIVATE fixture response should never escape")
        return SimpleNamespace(status_code=403 if self.outcome == "rejected" else 200, content=self.response_body)


class ReviewedReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.evidence = evidence()
        self.cases = validate_manifest(self.evidence)
        self.s3 = MemoryS3()
        self.store = S3AutomationStateStore(self.s3, "private", "data/roy/order-automation/state.json", "roy",
            now=lambda: datetime(2026, 9, 13, tzinfo=timezone.utc))
        state = self.store.empty_state()
        state["orders"] = {row["order_num"]: deepcopy(row["journal"]) for row in self.evidence["cases"]}
        self.store.put(state, "")
        self.s3.writes.clear()
        self.generator = Generator({row["order_num"]: deepcopy(row["fresh"]) for row in self.evidence["cases"]})
        self.web = Web(self.generator)
        self.factories = []
        self.gate = Mock()

    def factory(self, *, web_login):
        self.factories.append(web_login)
        self.generator.web_session = self.web if web_login else None
        if web_login:
            self.assertTrue(self.store.read()[0]["lease"])
        return self.generator

    def run_reconciliation(self, action="finalize-one", apply=True, **overrides):
        kwargs = dict(store=self.store, cases=self.cases, action=action, generator_factory=self.factory,
                      apply=apply, source_commit="a" * 40, release_gate=self.gate)
        kwargs.update(overrides)
        return reconcile(**kwargs)

    def record(self, number="1001"):
        return self.store.read()[0]["orders"][number]

    def change_record(self, change, number="1001"):
        state, etag = self.store.read()
        change(state["orders"][number])
        self.store.put(state, etag)
        self.s3.writes.clear()

    def assert_no_business_calls(self):
        self.assertEqual([], self.web.calls)
        self.generator.create_invoice.assert_not_called()
        self.generator._prepare_invoice.assert_not_called()
        self.generator.send_invoice_email.assert_not_called()

    def test_manifest_is_fixed_six_cases_two_sent_four_unfinished(self):
        self.assertEqual(2, sum(c["kind"] == "email" for c in self.cases))
        self.assertEqual(4, sum(c["kind"] == "finalize" for c in self.cases))
        for mutate in (
            lambda e: e["cases"].pop(),
            lambda e: e["cases"][1]["admin_history_readback"].update(invoice_email_events=[]),
            lambda e: e["cases"][1]["admin_history_readback"].update(invoice_email_events=["2026-09-10T13:00:00", "2026-09-10T15:00:00"]),
            lambda e: e["cases"][0]["fresh"].update(preinvoices=[]),
            lambda e: e["cases"][0].update(order_num="other"),
        ):
            data = deepcopy(self.evidence)
            mutate(data)
            with self.assertRaises(ReconciliationBlocked):
                validate_manifest(data)

    def test_preview_reads_fixed_scope_without_lease_web_or_any_write(self):
        result = self.run_reconciliation(apply=False)
        self.assertTrue(result["ok"])
        self.assertEqual(4, result["reviewed_cases"])
        self.assertEqual([False], self.factories)
        self.assertEqual([], self.s3.writes)
        self.gate.assert_not_called()
        self.assert_no_business_calls()
        self.assertNotIn("1001", json.dumps(result))

    def test_preview_reports_consumed_intent_and_never_suggests_replay(self):
        self.web.outcome = "timeout"
        self.run_reconciliation()
        writes = len(self.s3.writes)
        result = self.run_reconciliation(apply=False)
        self.assertFalse(result["ok"])
        self.assertEqual("previous_intent_consumed", result["blocked_reason"])
        self.assertEqual(writes, len(self.s3.writes))

    def test_preview_validates_status_operation_original_record_and_eligibility(self):
        for mutate in (
            lambda: self.change_record(lambda row: row.update(attempted_at="2026-01-01T00:00:00+00:00")),
            lambda: self.change_record(lambda row: row.update(status_mutation={"state": "uncertain"})),
            lambda: self.generator.orders["1001"].update(blocked=True),
        ):
            with self.subTest(mutate=mutate):
                self.setUp()
                mutate()
                with self.assertRaises(ReconciliationBlocked):
                    self.run_reconciliation(apply=False)
                self.assert_no_business_calls()

    def test_preview_requires_stable_journal_etag(self):
        def on_read(number, count):
            if count == 1:
                self.change_record(lambda row: row.update(status_observed_at="later"))
        self.generator.read_hook = on_read
        with self.assertRaisesRegex(ReconciliationBlocked, "journal-changed"):
            self.run_reconciliation(apply=False)

    def test_unresolved_status_write_blocks_apply(self):
        self.change_record(lambda row: row.update(status_mutation={"state": "attempted"}))
        with self.assertRaisesRegex(ReconciliationBlocked, "unresolved-status"):
            self.run_reconciliation()
        self.assert_no_business_calls()

    def test_wrong_journal_project_blocks_preview(self):
        with patch.object(self.store, "read", return_value=({"project": "vevo", "schema_version": 1, "orders": {}, "last_scan": {}}, "etag")):
            with self.assertRaisesRegex(ReconciliationBlocked, "journal-identity"):
                self.run_reconciliation(apply=False)

    def test_email_confirmation_writes_two_verified_records_without_web_login(self):
        result = self.run_reconciliation(action="confirm-emails")
        self.assertEqual(2, result["confirmed_emails"])
        self.assertEqual([False], self.factories)
        self.assert_no_business_calls()
        for number in ("1002", "1003"):
            row = self.record(number)
            self.assertEqual(("complete", "sent"), (row["phase"], row["email_state"]))
            self.assertEqual("provider_order_history", row[RECONCILIATION]["confirmed_by"])
            self.assertFalse(row[RECONCILIATION]["recipient_delivery_confirmed"])
            self.assertEqual(2, len(row[RECONCILIATION]["provider_send_events"]))
            self.assertEqual("ambiguous", row[RECONCILIATION]["original_record"]["email_state"])
        repeat = self.run_reconciliation(action="confirm-emails")
        self.assertEqual(0, repeat["confirmed_emails"])
        self.assert_no_business_calls()

    def test_both_email_documents_validated_before_first_outcome_write(self):
        self.generator.orders["1003"]["invoices"][0]["invoice_num"] = "DIFFERENT"
        with self.assertRaises(ReconciliationBlocked):
            self.run_reconciliation(action="confirm-emails")
        self.assertEqual("email", self.record("1002")["phase"])
        self.assertNotIn(RECONCILIATION, self.record("1002"))
        self.assert_no_business_calls()

    def test_email_binding_is_refreshed_again_immediately_before_journal_update(self):
        def on_read(number, count):
            if count == 3:
                self.generator.orders[number]["invoices"][0]["invoice_num"] = "LATE-DRIFT"
        self.generator.read_hook = on_read
        with self.assertRaises(ReconciliationBlocked):
            self.run_reconciliation(action="confirm-emails")
        self.assertNotIn(RECONCILIATION, self.record("1002"))
        self.assert_no_business_calls()

    def test_changed_original_operation_blocks_before_financial_request(self):
        self.change_record(lambda row: row.update(attempted_at="2026-09-12T09:00:00+00:00"))
        with self.assertRaises(ReconciliationBlocked):
            self.run_reconciliation()
        self.assert_no_business_calls()

    def test_one_finalization_binds_preinvoice_holds_email_and_preserves_old_attempt(self):
        before = deepcopy(self.record())
        seen = []
        self.web.before_send = lambda: seen.append(deepcopy(self.record()))
        result = self.run_reconciliation()
        self.assertTrue(result["ok"])
        self.assertEqual(1, result["finalization_requests"])
        self.assertEqual(1, len(self.web.calls))
        url, opts = self.web.calls[0]
        self.assertTrue(url.endswith("/finalize/1001"))
        self.assertFalse(opts["allow_redirects"])
        self.assertEqual((10, 30), opts["timeout"])
        self.assertEqual("create_ambiguous", seen[0]["phase"])
        self.assertEqual("hold", seen[0]["email_policy"])
        self.assertEqual("attempt_started", seen[0][RECONCILIATION]["state"])
        self.assertEqual(before["attempted_at"], self.record()["attempted_at"])
        self.assertEqual(before, self.record()[RECONCILIATION]["original_record"])
        self.assertEqual("unconfirmed", self.record()[RECONCILIATION]["original_outcome"])
        self.assertEqual(("complete", "held"), (self.record()["phase"], self.record()["email_state"]))
        self.assertEqual("object_literal_like", self.record()[RECONCILIATION]["response"]["format"])
        self.assertEqual("create_ambiguous", self.record("1004")["phase"])
        self.generator.create_invoice.assert_not_called()
        self.generator._prepare_invoice.assert_not_called()
        self.generator.send_invoice_email.assert_not_called()
        self.assertNotIn("private-fixture-token", json.dumps(self.store.read()[0]))
        self.assertNotIn("1001", json.dumps(result))

    def test_next_explicit_apply_selects_next_unfinished_fixed_case(self):
        self.run_reconciliation()
        self.run_reconciliation()
        self.assertEqual(2, len(self.web.calls))
        self.assertTrue(self.web.calls[1][0].endswith("/1004"))
        self.assertEqual("hold", self.record("1004")["email_policy"])
        self.assertIsNone(self.record("1004")[RECONCILIATION]["email_policy_before"])

    def test_lost_response_with_positive_final_readback_completes_without_replay(self):
        self.web.outcome = "lost_response"
        result = self.run_reconciliation()
        self.assertTrue(result["ok"])
        self.assertEqual(1, len(self.web.calls))
        self.assertEqual("TimeoutError", self.record()[RECONCILIATION]["response"]["exception_type"])
        self.assertNotIn("PRIVATE", json.dumps(self.store.read()[0]))

    def test_timeout_or_rejection_consumes_intent_and_blocks_later_cases(self):
        for outcome in ("timeout", "rejected"):
            with self.subTest(outcome=outcome):
                self.setUp()
                self.web.outcome = outcome
                result = self.run_reconciliation()
                self.assertFalse(result["ok"])
                self.assertEqual("create_ambiguous", self.record()["phase"])
                self.assertEqual("uncertain", self.record()[RECONCILIATION]["state"])
                with self.assertRaisesRegex(ReconciliationBlocked, "previous-intent-consumed"):
                    self.run_reconciliation()
                self.assertEqual(1, len(self.web.calls))
                self.assertNotIn(RECONCILIATION, self.record("1004"))

    def test_final_invoice_appearing_before_request_is_readback_only(self):
        self.generator.orders["1001"]["invoices"] = [{"id": "1001", "invoice_num": "EXTERNAL"}]
        result = self.run_reconciliation()
        self.assertEqual(0, result["finalization_requests"])
        self.assertEqual("EXTERNAL", self.record()["invoice_num"])
        self.assert_no_business_calls()

    def test_final_invoice_appearing_after_intent_is_readback_only(self):
        def on_read(number, count):
            if count == 3:
                self.generator.orders[number]["invoices"] = [{"id": number, "invoice_num": "LATE"}]
        self.generator.read_hook = on_read
        result = self.run_reconciliation()
        self.assertEqual(0, result["finalization_requests"])
        self.assertEqual("LATE", self.record()["invoice_num"])
        self.assert_no_business_calls()

    def test_disappearing_preinvoice_never_enters_preparation(self):
        self.generator.orders["1001"]["preinvoices"] = []
        with self.assertRaises(ReconciliationBlocked):
            self.run_reconciliation()
        self.assert_no_business_calls()
        self.assertNotIn(RECONCILIATION, self.record())

    def test_identity_status_and_price_drift_after_intent_consume_without_request(self):
        for mutate in (
            lambda row: row.update(id="999"),
            lambda row: row.update(status={"id": "67"}),
            lambda row: row["sum"].update(value="-1"),
            lambda row: row["sum"].update(value="35"),
        ):
            with self.subTest(mutate=mutate):
                self.setUp()
                self.generator.read_hook = lambda number, count: mutate(self.generator.orders[number]) if count == 3 else None
                with self.assertRaises(ReconciliationBlocked):
                    self.run_reconciliation()
                self.assertEqual("attempt_started", self.record()[RECONCILIATION]["state"])
                self.assert_no_business_calls()

    def test_missing_final_number_or_wrong_document_identity_never_reports_success(self):
        for invoice in ({"id": "1001", "invoice_num": ""}, {"id": "9999", "invoice_num": "WRONG"}):
            with self.subTest(invoice=invoice):
                self.setUp()
                self.generator.read_hook = lambda number, count: self.generator.orders[number].update(invoices=[invoice]) if count == 4 else None
                with self.assertRaises(ReconciliationBlocked):
                    self.run_reconciliation()
                self.assertEqual("uncertain", self.record()[RECONCILIATION]["state"])
                self.assertEqual("create_ambiguous", self.record()["phase"])
                self.assertEqual(1, len(self.web.calls))

    def test_cas_failure_before_intent_prevents_request(self):
        self.generator.read_hook = lambda number, count: setattr(self.s3, "fail_write", True) if count == 2 else None
        with self.assertRaises(AutomationStateError):
            self.run_reconciliation()
        self.assert_no_business_calls()

    def test_storage_failure_after_transmission_preserves_intent_and_recovers_without_replay(self):
        self.web.before_send = lambda: setattr(self.s3, "fail_write", True)
        with self.assertRaises(AutomationStateError):
            self.run_reconciliation()
        self.assertEqual(1, len(self.web.calls))
        self.assertEqual("attempt_started", self.record()[RECONCILIATION]["state"])
        self.assertEqual("hold", self.record()["email_policy"])
        self.s3.fail_write = False
        with self.assertRaises(AutomationLeaseBusy):
            self.run_reconciliation()
        self.assertEqual(1, len(self.web.calls))
        self.store.now = lambda: datetime(2026, 9, 13, 0, 6, tzinfo=timezone.utc)
        result = self.run_reconciliation()
        self.assertTrue(result["ok"])
        self.assertEqual(0, result["finalization_requests"])
        self.assertEqual(1, len(self.web.calls))

    def test_lost_lease_after_intent_never_sends_or_clears_replacement_owner(self):
        def on_read(number, count):
            if count == 3:
                state, etag = self.store.read()
                state["lease"]["token"] = "replacement-owner"
                self.store.put(state, etag)
        self.generator.read_hook = on_read
        with self.assertRaises(AutomationStateError):
            self.run_reconciliation()
        self.assertEqual("attempt_started", self.record()[RECONCILIATION]["state"])
        self.assertEqual("replacement-owner", self.store.read()[0]["lease"]["token"])
        self.assert_no_business_calls()

    def test_release_gate_failure_prevents_login_and_mutation(self):
        self.gate.side_effect = ReconciliationBlocked("not-promoted")
        with self.assertRaises(ReconciliationBlocked):
            self.run_reconciliation()
        self.assertEqual([], self.factories)
        self.assertEqual([], self.s3.writes)
        self.assert_no_business_calls()

    def test_release_drift_during_review_prevents_intent(self):
        self.gate.side_effect = [None, None, ReconciliationBlocked("drift")]
        with self.assertRaises(ReconciliationBlocked):
            self.run_reconciliation()
        self.assertNotIn(RECONCILIATION, self.record())
        self.assert_no_business_calls()

    def test_native_session_with_retries_is_rejected(self):
        self.web.retry_total = 1
        with self.assertRaisesRegex(ReconciliationBlocked, "retries-enabled"):
            self.run_reconciliation()
        self.assert_no_business_calls()

    def test_response_fingerprints_do_not_retain_body_token_or_url(self):
        for body, expected in ((b'{"success":true}', "json"), (b'\xef\xbb\xbf{"success":true}', "bom_json"),
                               (b'{success:true,token:"PRIVATE"}', "object_literal_like"), (b'<html>PRIVATE</html>', "other")):
            value = response_fingerprint(SimpleNamespace(status_code=200, content=body))
            self.assertEqual(expected, value["format"])
            self.assertNotIn("PRIVATE", json.dumps(value))


class ReleaseAndEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.digest, self.commit = REVIEWED_RELEASE_DIGEST, REVIEWED_RELEASE_COMMIT
        self.key = f"data/roy/order-automation/deployments/{self.commit}/{'b' * 32}.json"
        self.image = f"{ACCOUNT}.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting@{self.digest}"
        self.clients = {name: Mock() for name in ("s3", "sts", "ecr", "ecs", "scheduler")}
        self.session = Mock(client=lambda name: self.clients[name])
        self.clients["sts"].get_caller_identity.return_value = {"Account": ACCOUNT}
        self.clients["ecr"].describe_images.return_value = {"imageDetails": [{"imageDigest": self.digest}]}
        self.schedules = {name: {"Name": name, "State": "ENABLED", "Target": {
            "Arn": f"arn:aws:ecs:eu-central-1:{ACCOUNT}:cluster/{CLUSTER}",
            "EcsParameters": {"TaskDefinitionArn": f"arn:aws:ecs:eu-central-1:{ACCOUNT}:task-definition/{family}:9"}}}
            for name, family in SCHEDULES.items()}
        hosts = []
        for idx, family in enumerate(sorted(set(SCHEDULES.values())), 1):
            marker = {"marker": "ORDER_AUTOMATION_HOST_OK", "project": family.split("-")[0],
                      "kind": "invoice" if family.endswith("invoice-daily") else "cancellation", "path": "/app", "dry_run": True}
            if marker["kind"] == "invoice":
                marker["full_backlog"] = True
            hosts.append({"service": family, "image_digest": self.digest, "path": "/app", "exit_code": 0, "marker": marker,
                          "private_ip": f"172.31.0.{idx}", "instance_id": "N/A:FARGATE",
                          "task": f"arn:aws:ecs:eu-central-1:{ACCOUNT}:task/{CLUSTER}/{idx:032x}",
                          "task_definition": f"arn:aws:ecs:eu-central-1:{ACCOUNT}:task-definition/{family}:9"})
        self.proof = {"schema": 1, "commit": self.commit, "image_digest": self.digest,
                      "phase": "promotion-readback-verified", "desired_schedules": deepcopy(self.schedules),
                      "candidate_drain": {"quiet_seconds": 120, "unfinished_tasks": 0, "verified_at": "2026-09-13T01:00:00+00:00"},
                      "drain": {"quiet_seconds": 120, "unfinished_tasks": 0, "verified_at": "2026-09-13T02:00:00+00:00"}, "hosts": hosts}
        self.clients["s3"].get_object.side_effect = lambda **_: {"ServerSideEncryption": "AES256", "Body": BytesIO(json.dumps(self.proof).encode())}
        self.clients["scheduler"].get_schedule.side_effect = lambda Name: deepcopy(self.schedules[Name])
        self.clients["ecs"].describe_task_definition.side_effect = lambda taskDefinition: {"taskDefinition": {
            "family": taskDefinition.split("/")[-1].split(":")[0], "containerDefinitions": [{"name": "reporting",
            "workingDirectory": "/app", "image": self.image,
            "command": command_for(taskDefinition.split("/")[-1].split(":")[0])}]}}
        self.clients["ecs"].list_tasks.return_value = {"taskArns": []}

    def verify(self):
        return verify_release(self.session, digest=self.digest, commit=self.commit, evidence_key=self.key)

    def test_release_requires_verified_proof_all_five_exact_targets_and_source_digest(self):
        self.verify()
        self.assertEqual(5, self.clients["scheduler"].get_schedule.call_count)
        self.assertEqual(6, self.clients["ecs"].list_tasks.call_count)

    def test_even_successful_old_release_is_not_accepted(self):
        with self.assertRaisesRegex(ReconciliationBlocked, "outside-reviewed"):
            verify_release(self.session, digest="sha256:" + "f" * 64, commit="f" * 40,
                           evidence_key=f"data/roy/order-automation/deployments/{'f' * 40}/{'b' * 32}.json")

    def test_exact_host_scope_markers_and_both_drains_are_required(self):
        for mutate in (
            lambda: self.proof["hosts"][0].update(marker="truthy"),
            lambda: self.proof["hosts"][0]["marker"].pop("full_backlog"),
            lambda: self.proof["hosts"][1].update(service=self.proof["hosts"][0]["service"]),
            lambda: self.proof["candidate_drain"].update(quiet_seconds=119),
            lambda: self.proof["drain"].update(unfinished_tasks=1),
        ):
            with self.subTest(mutate=mutate):
                self.setUp()
                mutate()
                with self.assertRaises(ReconciliationBlocked):
                    self.verify()

    def test_release_failures_block_unpromoted_wrong_source_or_changed_schedule(self):
        for change in (
            lambda: self.proof.update(phase="before-candidates"),
            lambda: self.proof.update(commit="f" * 40),
            lambda: self.clients["ecr"].describe_images.configure_mock(return_value={"imageDetails": [{"imageDigest": "wrong"}]}),
            lambda: self.schedules["roy-daily-invoice-generation"].update(State="DISABLED"),
        ):
            with self.subTest(change=change):
                self.setUp()
                change()
                with self.assertRaises(ReconciliationBlocked):
                    self.verify()

    def test_stopping_old_writer_is_rejected_even_when_desired_status_is_stopped(self):
        self.clients["ecs"].list_tasks.side_effect = lambda **kw: {"taskArns": ["old-task"] if kw["desiredStatus"] == "STOPPED" else []}
        self.clients["ecs"].describe_tasks.return_value = {"tasks": [{"taskArn": "old-task", "lastStatus": "STOPPING",
            "taskDefinitionArn": f"arn:aws:ecs:eu-central-1:{ACCOUNT}:task-definition/roy-invoice-daily:8",
            "clusterArn": f"arn:aws:ecs:eu-central-1:{ACCOUNT}:cluster/{CLUSTER}",
            "containers": [{"image": "old", "imageDigest": "old"}]}]}
        with self.assertRaisesRegex(ReconciliationBlocked, "old-or-unverified"):
            self.verify()

    def test_new_same_image_active_writer_is_allowed_subject_to_shared_lease(self):
        self.clients["ecs"].list_tasks.side_effect = lambda **kw: {"taskArns": [kw["family"]]}
        self.clients["ecs"].describe_tasks.side_effect = lambda **kw: {"tasks": [{"taskArn": kw["tasks"][0], "lastStatus": "RUNNING",
            "taskDefinitionArn": f"arn:aws:ecs:eu-central-1:{ACCOUNT}:task-definition/{kw['tasks'][0]}:9",
            "clusterArn": f"arn:aws:ecs:eu-central-1:{ACCOUNT}:cluster/{CLUSTER}",
            "containers": [{"image": self.image, "imageDigest": self.digest}]}]}
        self.verify()

    def test_task_readback_must_match_exact_requested_identity(self):
        self.clients["ecs"].list_tasks.return_value = {"taskArns": ["expected"]}
        self.clients["ecs"].describe_tasks.return_value = {"tasks": [{"taskArn": "other", "lastStatus": "STOPPED"}]}
        with self.assertRaisesRegex(ReconciliationBlocked, "inspection-incomplete"):
            self.verify()

    def test_encrypted_private_evidence_must_match_fixed_hash(self):
        s3 = Mock()
        s3.get_object.return_value = {"ServerSideEncryption": "AES256", "Body": BytesIO(b'{}')}
        with self.assertRaisesRegex(ReconciliationBlocked, "hash-mismatch"):
            private_json(s3, "private-evidence", expected_sha=EVIDENCE_SHA)
        s3.get_object.assert_called_once_with(Bucket=BUCKET, Key="private-evidence", ExpectedBucketOwner=ACCOUNT)

    def test_loader_never_accepts_an_arbitrary_evidence_key_or_hash(self):
        with patch("scripts.reconcile_reviewed_invoice_outcomes.private_json", return_value=evidence()) as reader:
            with self.assertRaises(ReconciliationBlocked):
                load_cases(Mock())
            self.assertEqual(EVIDENCE_SHA, reader.call_args_list[0].kwargs["expected_sha"])

    def test_source_rejects_main_even_if_clean(self):
        with patch("scripts.reconcile_reviewed_invoice_outcomes.subprocess.run"), patch(
            "scripts.reconcile_reviewed_invoice_outcomes.subprocess.check_output", return_value="main\n"
        ), self.assertRaisesRegex(ReconciliationBlocked, "codex-source"):
            require_source("fixture-root")


if __name__ == "__main__":
    unittest.main()
