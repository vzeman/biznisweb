from copy import deepcopy
from datetime import datetime, timedelta, timezone
from io import BytesIO
import json
import unittest
from reporting_core.storage import resolve_report_s3_location

from invoice_automation_state import (
    AutomationLeaseBusy, AutomationStateError, S3AutomationStateStore,
    resolve_automation_state_location,
)


class S3Failure(Exception):
    def __init__(self, code):
        self.response = {"Error": {"Code": code}}


class MemoryS3:
    """An atomic conditional S3 fake; no filesystem or AWS access."""
    def __init__(self):
        self.objects = {}
        self.writes = []
        self.revision = 0
        self.fail_write = False

    def get_object(self, *, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise S3Failure("NoSuchKey")
        body, etag = self.objects[Bucket, Key]
        return {"Body": BytesIO(body), "ETag": etag}

    def put_object(self, **kwargs):
        if self.fail_write:
            raise S3Failure("AccessDenied")
        key = (kwargs["Bucket"], kwargs["Key"])
        existing = self.objects.get(key)
        if (kwargs.get("IfNoneMatch") == "*" and existing) or (
            "IfMatch" in kwargs and (not existing or existing[1] != kwargs["IfMatch"])
        ):
            raise S3Failure("PreconditionFailed")
        self.revision += 1
        etag = f'"revision-{self.revision}"'
        self.objects[key] = (kwargs["Body"], etag)
        self.writes.append(deepcopy(kwargs))
        return {"ETag": etag}


class InvoiceAutomationStateTests(unittest.TestCase):
    def setUp(self):
        self.clock = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.s3 = MemoryS3()
        self.store = S3AutomationStateStore(self.s3, "private-bucket", "data/shop/order-automation/state.json", "shop",
                                           now=lambda: self.clock)

    def test_resolver_is_project_scoped_and_requires_bucket(self):
        self.assertEqual(("private", "data/shop/order-automation/state.json"),
                         resolve_automation_state_location("shop", {}, {"REPORT_S3_BUCKET": "private"}))
        with self.assertRaises(AutomationStateError):
            resolve_automation_state_location("shop", {}, {})
        with self.assertRaises(AutomationStateError):
            resolve_automation_state_location("shop", {}, {"REPORT_S3_BUCKET": "private", "ORDER_AUTOMATION_STATE_PREFIX": "data/another/order-automation"})

    def test_blank_runtime_bucket_uses_only_explicit_project_destination(self):
        settings = {"live_dashboard_artifacts": {"s3_bucket": "verified-existing-bucket", "s3_prefix": "daily-reports/shop"}}
        env = {"REPORT_S3_BUCKET": "  ", "REPORT_S3_PREFIX": ""}
        self.assertEqual(("verified-existing-bucket", "daily-reports/shop"),
                         resolve_report_s3_location("shop", settings, env, required=True))
        self.assertEqual(("verified-existing-bucket", "data/shop/order-automation/state.json"),
                         resolve_automation_state_location("shop", settings, env))
        with self.assertRaises(ValueError):
            resolve_report_s3_location("another", {}, env, required=True)

    def test_nonempty_runtime_destination_is_preserved_for_deployment_validation(self):
        settings = {"live_dashboard_artifacts": {"s3_bucket": "configured-bucket"}}
        env = {"REPORT_S3_BUCKET": "runtime-bucket", "OTHER_REPORT_S3_BUCKET": "other-bucket"}
        self.assertEqual("runtime-bucket", resolve_report_s3_location("shop", settings, env, required=True)[0])
        self.assertEqual("configured-bucket", resolve_report_s3_location("shop", settings, {"OTHER_REPORT_S3_BUCKET": "other-bucket"})[0])

    def test_project_runtime_override_and_invalid_storage_are_explicit(self):
        self.assertEqual(("project-bucket", "reports/shop"), resolve_report_s3_location("shop", {}, {
            "SHOP_REPORT_S3_BUCKET": "project-bucket", "REPORT_S3_BUCKET": "generic-bucket", "REPORT_S3_PREFIX_SHOP": "reports/shop"}, required=True))
        for env in ({"REPORT_S3_BUCKET": "s3://not-a-bucket"}, {"REPORT_S3_PREFIX": "reports/../another"}):
            with self.assertRaises(ValueError):
                resolve_report_s3_location("shop", {}, env)

    def test_read_missing_state_does_not_write(self):
        state, etag = self.store.read()
        self.assertEqual("shop", state["project"])
        self.assertEqual("", etag)
        self.assertEqual([], self.s3.writes)

    def test_lease_prevents_overlap_and_releases_only_own_lease(self):
        with self.store.lease("first") as journal:
            journal.update_order("fixture-1", phase="pending")
            with self.assertRaises(AutomationLeaseBusy):
                with self.store.lease("second"):
                    self.fail("overlapping lease granted")
        self.assertIsNone(self.store.read()[0]["lease"])
        self.assertEqual("pending", self.store.read()[0]["orders"]["fixture-1"]["phase"])

    def test_expired_owner_cannot_resume_or_clear_replacement(self):
        with self.store.lease("first") as first:
            self.clock += timedelta(seconds=301)
            with self.assertRaises(AutomationStateError):
                first.assert_owned()
            with self.store.lease("replacement") as replacement:
                first.release()
                replacement.assert_owned()
                self.assertEqual(replacement.token, self.store.read()[0]["lease"]["token"])

    def test_lost_cas_prevents_write(self):
        state, etag = self.store.read()
        self.store.put(state, etag)
        with self.assertRaises(AutomationStateError):
            self.store.put(state, etag)

    def test_wrong_project_or_corrupt_state_is_not_empty(self):
        for body in (b"not json", json.dumps({**self.store.empty_state(), "project": "another"}).encode()):
            self.s3.objects[self.store.bucket, self.store.key] = (body, '"one"')
            with self.assertRaises(AutomationStateError):
                self.store.read()

    def test_s3_read_error_is_not_treated_as_empty(self):
        class Denied:
            def get_object(self, **kwargs):
                raise S3Failure("AccessDenied")
        with self.assertRaises(AutomationStateError):
            S3AutomationStateStore(Denied(), "private", "key", "shop").read()

    def test_enqueue_preserves_uncertain_attempt_and_email_outbox(self):
        with self.store.lease("invoice") as journal:
            journal.update_order("fixture-1", phase="create_ambiguous")
            journal.update_order("fixture-2", phase="email", email_state="failed", invoice_id="fixture-invoice")
            journal.enqueue_orders([{"order_num": "fixture-1"}, {"order_num": "fixture-2"}, {"order_num": "fixture-3"}])
            self.assertEqual("create_ambiguous", journal.get_order("fixture-1")["phase"])
            self.assertEqual("failed", journal.get_order("fixture-2")["email_state"])
            self.assertEqual("pending", journal.get_order("fixture-3")["phase"])
        with self.store.lease("next-run") as journal:
            self.assertEqual(3, len(journal.pending_orders()))

    def test_only_actual_shipped_observation_records_fulfillment(self):
        with self.store.lease("invoice") as journal:
            journal.remember_shipped_orders([
                {"order_num": "shipped", "blocked": False, "status": {"id": 4}},
                {"order_num": "blocked", "blocked": True, "status": {"id": 4}},
                {"order_num": "invoice-only", "blocked": False, "status": {"id": 9}, "invoices": [{"id": 1}]},
            ], {4})
            self.assertEqual("Odoslaná", journal.get_order("shipped")["verified_fulfillment_status"])
            self.assertEqual({}, journal.get_order("blocked"))
            self.assertEqual({}, journal.get_order("invoice-only"))

    def test_new_scan_cannot_reset_uncertain_preinvoice_attempts(self):
        for phase in ("preparing", "prepare_ambiguous"):
            with self.subTest(phase=phase):
                with self.store.lease("preinvoice-attempt") as journal:
                    journal.update_order("fixture-preinvoice", phase=phase, email_policy="hold")
                with self.store.lease("next-scan") as journal:
                    journal.enqueue_orders([{"order_num": "fixture-preinvoice"}])
                    record = journal.get_order("fixture-preinvoice")
                    self.assertEqual(phase, record["phase"])
                    self.assertEqual("hold", record["email_policy"])
                    self.assertEqual([record], journal.pending_orders())

    def test_all_mutations_use_encryption_and_conditional_put(self):
        with self.store.lease("invoice") as journal:
            journal.record_scan(changed_watermark=self.clock.isoformat())
        for write in self.s3.writes:
            self.assertEqual("AES256", write["ServerSideEncryption"])
            self.assertEqual(1, int("IfMatch" in write) + int("IfNoneMatch" in write))


if __name__ == "__main__":
    unittest.main()
