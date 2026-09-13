from copy import deepcopy
import unittest
from unittest.mock import patch

from generate_invoices import reconcile_existing_invoice_statuses
from tests.test_manual_settlement import confirmed, current_order, store_for
from tests.test_unpaid_order_cancellation import FakeBizniswebClient
from unpaid_order_cancellation import run_unpaid_order_cancellation


class RegressionClient(FakeBizniswebClient):
    def execute(self, query, variable_values=None):
        result = super().execute(query, variable_values)
        if variable_values and "status_id" in variable_values:
            self.pages[0][0]["last_change"] = (
                f"2026-05-{len(self.mutations) * 2:02d} 10:00:00"
            )
        return result


class ManualSettlementRecoveryTests(unittest.TestCase):
    def setUp(self):
        sleep = patch("unpaid_order_cancellation.time.sleep")
        sleep.start()
        self.addCleanup(sleep.stop)
        self.value = current_order()
        self.value.update(
            pur_date="2026-01-01 10:00:00",
            price_elements=[
                {"type": "payment", "title": "Bankovým prevodom", "reference_id": "6"}
            ],
        )
        self.client = RegressionClient([[self.value]])
        self.record = {
            "order_num": self.value["order_num"],
            "phase": "complete",
            "manual_settlement": confirmed(self.value),
        }
        self.store = store_for({self.value["order_num"]: self.record})

    def invoice(self, *, dry_run=False, creditnotes=None, read_order=None):
        settings = {
            "enabled": True,
            "source_statuses": ["Stripe - expired"],
            "target_status_name": "Platba online - zaplatené",
        }
        if dry_run:
            return reconcile_existing_invoice_statuses(
                self.client,
                [deepcopy(self.value)],
                settings,
                project="vevo",
                dry_run=True,
                records=self.store.read()[0]["orders"],
                creditnote_order_numbers=set() if creditnotes is None else creditnotes,
                read_order=read_order,
            )
        with self.store.lease("test-invoice") as journal:
            return reconcile_existing_invoice_statuses(
                self.client,
                [deepcopy(self.value)],
                settings,
                project="vevo",
                dry_run=False,
                journal=journal,
                creditnote_order_numbers=set() if creditnotes is None else creditnotes,
                read_order=read_order,
            )

    def cancellation(self, *, dry_run=False, recovery=True, creditnotes=None):
        settings = {
            "unpaid_order_cancellation": {
                "enabled": True,
                "recovery_enabled": recovery,
                "target_status_id": 74,
                "candidate_statuses": ["Stripe - expired"],
                "payment_reference_ids": ["6"],
                "recovery_source_statuses": ["Stripe - expired"],
            }
        }
        return run_unpaid_order_cancellation(
            "vevo",
            reference_date="2026-05-27",
            client=self.client,
            project_settings=settings,
            automation_state_store=self.store,
            dry_run=dry_run,
            creditnote_context={} if creditnotes is None else creditnotes,
        )

    def expire_again(self, stamp):
        self.value["status"] = {"id": 69, "name": "Stripe - expired"}
        self.value["last_change"] = stamp

    def test_both_writers_recover_again_only_for_later_proven_regression_and_archive_intent(
        self,
    ):
        for writer in ("invoice", "cancellation"):
            with self.subTest(writer=writer):
                self.setUp()
                getattr(self, writer)()
                self.assertEqual([("900001", 4)], self.client.mutations)
                self.expire_again("2026-05-03 10:00:00")
                getattr(self, writer)()
                self.assertEqual([("900001", 4), ("900001", 4)], self.client.mutations)
                record = self.store.read()[0]["orders"]["900001"]
                self.assertEqual(1, len(record["status_mutation_history"]))
                self.assertEqual(
                    "verified", record["status_mutation_history"][0]["state"]
                )
                self.assertEqual(
                    "2026-05-04 10:00:00",
                    record["status_mutation"]["verified_last_change"],
                )
                self.expire_again("2026-05-04 10:00:00")
                getattr(self, writer)()
                self.assertEqual(2, len(self.client.mutations))

    def test_paid_can_advance_to_newly_proven_shipped_but_never_regress_to_paid(self):
        for writer in ("invoice", "cancellation"):
            with self.subTest(writer=writer):
                self.setUp()
                self.record["manual_settlement"] = confirmed(
                    self.value, target={"id": "55", "name": "Platba online - zaplatené"}
                )
                self.store = store_for({"900001": self.record})
                getattr(self, writer)()
                self.assertEqual([("900001", 55)], self.client.mutations)
                self.expire_again("2026-05-03 10:00:00")
                self.value["shipments"] = [
                    {"status": "delivered", "shipment_number": "fixture"}
                ]
                getattr(self, writer)()
                self.assertEqual([("900001", 55), ("900001", 4)], self.client.mutations)
                record = self.store.read()[0]["orders"]["900001"]
                self.assertEqual(
                    "Platba online - zaplatené",
                    record["status_mutation_history"][0]["target_status_name"],
                )
                self.assertEqual(
                    "Odoslaná", record["status_mutation"]["target_status_name"]
                )
                self.expire_again("2026-05-05 10:00:00")
                self.value["shipments"] = []
                getattr(self, writer)()
                self.assertEqual(2, len(self.client.mutations))

    def test_ambiguous_or_pending_financial_email_status_intent_cannot_replay(self):
        for writer in ("invoice", "cancellation"):
            for field, value in (
                ("phase", "create_ambiguous"),
                ("email_state", "ambiguous"),
                ("status_mutation", {"state": "pending"}),
                ("status_mutation", {"state": "uncertain"}),
            ):
                with self.subTest(writer=writer, field=field, value=value):
                    self.setUp()
                    with self.store.lease("fixture") as journal:
                        journal.update_order("900001", **{field: value})
                    getattr(self, writer)()
                    getattr(self, writer)()
                    self.assertEqual([], self.client.mutations)
                    self.assertEqual(
                        value, self.store.read()[0]["orders"]["900001"][field]
                    )

    def test_unpaid_downgrade_blocked_without_invoice_and_with_recovery_disabled(self):
        self.value["invoices"] = []
        result = self.cancellation(recovery=False)
        self.assertEqual(0, result.updated_orders)
        self.assertEqual({"payment_present": 1}, result.skipped_by_reason)
        self.assertEqual([], self.client.mutations)

    def test_explicit_manual_record_extends_recovery_to_order_without_invoice(self):
        for writer in ("invoice", "cancellation"):
            with self.subTest(writer=writer):
                self.setUp()
                self.value["invoices"] = []
                getattr(self, writer)()
                self.assertEqual([("900001", 4)], self.client.mutations)

    def test_complete_creditnote_presence_prevents_both_writers_restoring(self):
        self.invoice(creditnotes={"900001"})
        self.cancellation(
            creditnotes={"900001": [{"fixture": "partial or full creditnote"}]}
        )
        self.assertEqual([], self.client.mutations)

    def test_new_total_or_missing_detail_during_final_recheck_prevents_invoice_write(
        self,
    ):
        reads = []

        def read(number):
            value = deepcopy(self.value)
            reads.append(number)
            if len(reads) == 2:
                value["sum"]["value"] = 101
            return value

        result = self.invoice(read_order=read)
        self.assertEqual(1, result["failed"])
        self.assertEqual(2, len(reads))
        self.assertEqual([], self.client.mutations)
        self.assertNotIn("status_mutation", self.store.read()[0]["orders"]["900001"])

    def test_fresh_cancellation_detail_rechecks_manual_binding_before_any_status_write(
        self,
    ):
        calls = []
        original = self.client.execute

        def read(query, variable_values=None):
            result = original(query, variable_values)
            if (
                variable_values
                and "order_num" in variable_values
                and "status_id" not in variable_values
            ):
                calls.append(1)
                result = deepcopy(result)
                if len(calls) == 2:
                    result["getOrder"]["sum"]["value"] = 101
            return result

        self.client.execute = read
        result = self.cancellation()
        self.assertEqual(1, result.review_required_orders)
        self.assertEqual([], self.client.mutations)

    def test_dry_runs_use_manual_snapshot_and_have_no_journal_or_provider_writes(self):
        before = len(self.store.client.writes)
        self.invoice(dry_run=True)
        self.cancellation(dry_run=True)
        self.assertEqual(before, len(self.store.client.writes))
        self.assertEqual([], self.client.mutations)


if __name__ == "__main__":
    unittest.main()
