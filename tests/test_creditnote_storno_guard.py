import json
import copy
import unittest
from pathlib import Path
from unittest.mock import patch

from creditnote_storno_guard import resolve_creditnote_storno_settings, run_creditnote_storno_guard
from tests.test_order_status_safety import MemoryAutomationStore, money


ROOT_DIR = Path(__file__).resolve().parents[1]


def creditnote_row(number: str, order_num: str) -> dict:
    return {
        "number": number,
        "creditnote_id": number,
        "inv_id": "invoice-" + order_num,
        "created": "2026-05-12 10:00:00",
        "order_num": order_num,
        "price": "10,00 €",
        "taxed_price": "12,30 €",
    }


def order_row(order_num: str, status_name: str) -> dict:
    return {
        "id": order_num,
        "order_num": order_num,
        "pur_date": "2026-05-10 10:00:00",
        "status": {"id": 1, "name": status_name},
        "price_elements": [],
        "blocked": False, "sum": money(12.30),
        "invoices": [{"id": "invoice-" + order_num}],
    }


class FakeClient:
    def __init__(self, orders):
        self.orders = {order["order_num"]: order for order in orders}
        self.mutations = []
        self.mutation_variables = []
        self.detail_calls = 0
        self.before_detail = None
        self.mutation_result = "success"

    def execute(self, query, variable_values=None):  # noqa: ANN001 - mimics gql.Client
        variables = variable_values or {}
        if "lang_code" in variables:
            return {"listOrderStatuses": [{"id": 99, "name": "Storno"}]}
        if "order_num" in variables and "status_id" in variables:
            self.mutations.append((variables["order_num"], variables["status_id"]))
            self.mutation_variables.append(variables)
            if self.mutation_result == "uncertain":
                raise TimeoutError("unverified response")
            self.orders[variables["order_num"]]["status"] = {"id": variables["status_id"], "name": "Storno"}
            return {
                "changeOrderStatus": {
                    "order_num": variables["order_num"],
                    "status": {"id": variables["status_id"], "name": "Storno"},
                }
            }
        if "order_num" in variables:
            self.detail_calls += 1
            if self.before_detail:
                self.before_detail(self.detail_calls, self.orders[variables["order_num"]])
            return {"getOrder": self.orders.get(variables["order_num"])}
        raise AssertionError(f"Unexpected GraphQL variables: {variables}")


class FakeExporter:
    def __init__(self, orders):
        self.client = FakeClient(orders)

    @staticmethod
    def _is_price_elements_error(exc):  # noqa: ANN001 - compatibility with real exporter
        return "price_elements" in str(exc or "")

    @staticmethod
    def _fetch_order_payment_metadata(order):  # noqa: ANN001 - compatibility with real exporter
        return False

    @staticmethod
    def _realized_revenue_decision(order):
        status = ((order or {}).get("status") or {}).get("name")
        return status == "Odoslana", "test_status"


class CreditnoteStornoGuardTests(unittest.TestCase):
    def settings(self) -> dict:
        return {
            "creditnote_storno_guard": {
                "enabled": True,
                "target_status_name": "Storno",
                "target_status_id": 99,
                "only_if_in_realized_revenue": True,
                "final_statuses": ["Storno", "Vratene", "Dobropis"],
            }
        }

    def test_dry_run_flags_creditnoted_revenue_order_without_mutation(self) -> None:
        exporter = FakeExporter(
            [
                order_row("R-1", "Odoslana"),
                order_row("R-2", "Storno"),
                order_row("R-3", "Vratene"),
            ]
        )

        summary = run_creditnote_storno_guard(
            "roy",
            date_from="2026-05-01",
            date_to="2026-05-31",
            dry_run=True,
            exporter=exporter,
            raw_creditnote_rows=[
                creditnote_row("D-1", "R-1"),
                creditnote_row("D-2", "R-2"),
                creditnote_row("D-3", "R-3"),
            ],
            project_settings=self.settings(),
        )

        self.assertEqual(3, summary.creditnoted_orders)
        self.assertEqual(1, summary.eligible_orders)
        self.assertEqual(["R-1"], summary.eligible_order_nums)
        self.assertEqual("Odoslana", summary.eligible_order_statuses[0]["previous_status"])
        self.assertTrue(summary.eligible_order_statuses[0]["sent_before_cancel"])
        self.assertEqual({"already_final_status": 1, "already_target_status": 1}, summary.skipped_by_reason)
        self.assertEqual([], exporter.client.mutations)

    def test_real_run_updates_only_eligible_creditnoted_revenue_orders(self) -> None:
        exporter = FakeExporter(
            [
                order_row("R-1", "Odoslana"),
                order_row("R-2", "Storno"),
                order_row("R-3", "Vratene"),
            ]
        )

        saved_audits = []
        with patch("creditnote_storno_guard.load_creditnote_status_change_audit", return_value={"project": "roy", "orders": []}), patch(
            "creditnote_storno_guard.save_creditnote_status_change_audit",
            side_effect=lambda project, audit, settings, **kwargs: saved_audits.append(copy.deepcopy(audit)) or Path("audit.json"),
        ):
            summary = run_creditnote_storno_guard(
                "roy",
                date_from="2026-05-01",
                date_to="2026-05-31",
                dry_run=False,
                exporter=exporter,
                raw_creditnote_rows=[
                    creditnote_row("D-1", "R-1"),
                    creditnote_row("D-2", "R-2"),
                    creditnote_row("D-3", "R-3"),
                ],
                project_settings=self.settings(),
                automation_state_store=MemoryAutomationStore(),
            )

        self.assertEqual(1, summary.updated_orders)
        self.assertEqual(["R-1"], summary.updated_order_nums)
        self.assertEqual("Odoslana", summary.updated_order_statuses[0]["previous_status"])
        self.assertTrue(summary.updated_order_statuses[0]["sent_before_cancel"])
        self.assertEqual("audit.json", summary.status_audit_path)
        self.assertEqual("R-1", saved_audits[-1]["orders"][0]["order_num"])
        self.assertEqual("Odoslana", saved_audits[-1]["orders"][0]["previous_status"])
        self.assertEqual([("R-1", 99)], exporter.client.mutations)
        self.assertEqual("pending", saved_audits[0]["orders"][0]["change_result"])
        self.assertEqual([{"type": "EMAIL_CUSTOMER", "if": ["NONE"]}], exporter.client.mutation_variables[0]["send_notification"])

    def run_case(self, exporter, *, rows=None, store=None, dry_run=False):
        return run_creditnote_storno_guard(
            "roy", date_from="2026-05-01", date_to="2026-05-31", dry_run=dry_run,
            exporter=exporter, raw_creditnote_rows=rows if rows is not None else [creditnote_row("D-1", "R-1")],
            project_settings=self.settings(), automation_state_store=store or MemoryAutomationStore(),
        )

    def test_partial_creditnote_does_not_cancel_whole_order_or_fail_valid_refund(self):
        exporter = FakeExporter([order_row("R-1", "Odoslana")])
        row = creditnote_row("D-1", "R-1")
        row["taxed_price"] = "1,23 €"
        summary = self.run_case(exporter, rows=[row])
        self.assertEqual(1, summary.partial_creditnote_orders)
        self.assertEqual(0, summary.failed_orders)
        self.assertEqual([], exporter.client.mutations)

    def test_malformed_or_duplicate_creditnote_scan_stops_before_any_write(self):
        for rows in ([creditnote_row("D-1", "R-1")] * 2, [{**creditnote_row("D-1", "R-1"), "creditnote_id": None}]):
            exporter = FakeExporter([order_row("R-1", "Odoslana")])
            with self.assertRaises(RuntimeError):
                self.run_case(exporter, rows=rows)
            self.assertEqual([], exporter.client.mutations)

    def test_blocked_order_and_unrelated_invoice_require_review(self):
        for field, value in (("blocked", True), ("invoices", [{"id": "unrelated"}])):
            order = order_row("R-1", "Odoslana")
            order[field] = value
            exporter = FakeExporter([order])
            summary = self.run_case(exporter, dry_run=True)
            self.assertEqual(1, summary.review_required_orders)
            self.assertEqual([], exporter.client.mutations)

    def test_fresh_order_change_blocks_previously_planned_mutation(self):
        exporter = FakeExporter([order_row("R-1", "Odoslana")])
        def block_on_recheck(call, order):
            if call == 2:
                order["blocked"] = True
        exporter.client.before_detail = block_on_recheck
        with patch("creditnote_storno_guard.load_creditnote_status_change_audit", return_value={"project": "roy", "orders": []}):
            summary = self.run_case(exporter)
        self.assertEqual(1, summary.eligible_orders)
        self.assertEqual(1, summary.failed_orders)
        self.assertEqual([], exporter.client.mutations)

    def test_durable_prechange_audit_failure_prevents_status_mutation(self):
        exporter = FakeExporter([order_row("R-1", "Odoslana")])
        with patch("creditnote_storno_guard.load_creditnote_status_change_audit", return_value={"project": "roy", "orders": []}), patch(
            "creditnote_storno_guard.save_creditnote_status_change_audit", side_effect=RuntimeError("storage unavailable")
        ):
            with self.assertRaisesRegex(RuntimeError, "storage unavailable"):
                self.run_case(exporter)
        self.assertEqual([], exporter.client.mutations)

    def test_uncertain_write_is_durable_and_never_replayed_on_next_run(self):
        exporter = FakeExporter([order_row("R-1", "Odoslana")])
        exporter.client.mutation_result = "uncertain"
        store = MemoryAutomationStore()
        with patch("creditnote_storno_guard.load_creditnote_status_change_audit", return_value={"project": "roy", "orders": []}), patch(
            "creditnote_storno_guard.save_creditnote_status_change_audit", return_value=Path("audit.json")
        ):
            first = self.run_case(exporter, store=store)
            second = self.run_case(exporter, store=store)
        self.assertEqual(1, first.failed_orders)
        self.assertEqual(1, second.failed_orders)
        self.assertEqual("uncertain", store.orders["R-1"]["status_mutation"]["state"])
        self.assertEqual([("R-1", 99)], exporter.client.mutations)

    def test_verified_shipment_recovery_does_not_block_later_full_creditnote(self):
        exporter = FakeExporter([order_row("R-1", "Odoslana")])
        store = MemoryAutomationStore({"R-1": {"status_mutation": {"state": "verified", "target_status_name": "Odoslaná"}}})
        with patch("creditnote_storno_guard.load_creditnote_status_change_audit", return_value={"project": "roy", "orders": []}), patch(
            "creditnote_storno_guard.save_creditnote_status_change_audit", return_value=Path("audit.json")
        ):
            summary = self.run_case(exporter, store=store)
        self.assertEqual(1, summary.updated_orders)
        self.assertEqual("full_creditnote", store.orders["R-1"]["status_mutation"]["reason"])

    def test_final_audit_failure_reports_failure_after_verified_status_change(self):
        exporter = FakeExporter([order_row("R-1", "Odoslana")])
        store = MemoryAutomationStore()
        with patch("creditnote_storno_guard.load_creditnote_status_change_audit", return_value={"project": "roy", "orders": []}), patch(
            "creditnote_storno_guard.save_creditnote_status_change_audit", side_effect=[Path("audit.json"), RuntimeError("write failed")]
        ):
            summary = self.run_case(exporter, store=store)
        self.assertEqual(1, summary.updated_orders)
        self.assertEqual(1, summary.failed_orders)
        self.assertEqual("verified", store.orders["R-1"]["status_mutation"]["state"])

    def test_project_settings_enable_guard_for_both_shops(self) -> None:
        for project in ("roy", "vevo"):
            with self.subTest(project=project):
                settings = json.loads((ROOT_DIR / "projects" / project / "settings.json").read_text(encoding="utf-8"))
                guard = resolve_creditnote_storno_settings(settings)
                self.assertTrue(guard.enabled)
                self.assertEqual("Storno", guard.target_status_name)
                self.assertTrue(guard.only_if_in_realized_revenue)
                self.assertTrue(settings["creditnote_fulfillment_costs"]["enabled"])


if __name__ == "__main__":
    unittest.main()
