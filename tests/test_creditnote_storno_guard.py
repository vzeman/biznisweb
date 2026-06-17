import json
import unittest
from pathlib import Path

from creditnote_storno_guard import resolve_creditnote_storno_settings, run_creditnote_storno_guard


ROOT_DIR = Path(__file__).resolve().parents[1]


def creditnote_row(number: str, order_num: str) -> dict:
    return {
        "number": number,
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
    }


class FakeClient:
    def __init__(self, orders):
        self.orders = {order["order_num"]: order for order in orders}
        self.mutations = []

    def execute(self, query, variable_values=None):  # noqa: ANN001 - mimics gql.Client
        variables = variable_values or {}
        if "lang_code" in variables:
            return {"listOrderStatuses": [{"id": 99, "name": "Storno"}]}
        if "order_num" in variables and "status_id" in variables:
            self.mutations.append((variables["order_num"], variables["status_id"]))
            return {
                "changeOrderStatus": {
                    "order_num": variables["order_num"],
                    "status": {"id": variables["status_id"], "name": "Storno"},
                }
            }
        if "order_num" in variables:
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
        )

        self.assertEqual(1, summary.updated_orders)
        self.assertEqual(["R-1"], summary.updated_order_nums)
        self.assertEqual([("R-1", 99)], exporter.client.mutations)

    def test_project_settings_enable_guard_for_both_shops(self) -> None:
        for project in ("roy", "vevo"):
            with self.subTest(project=project):
                settings = json.loads((ROOT_DIR / "projects" / project / "settings.json").read_text(encoding="utf-8"))
                guard = resolve_creditnote_storno_settings(settings)
                self.assertTrue(guard.enabled)
                self.assertEqual("Storno", guard.target_status_name)
                self.assertTrue(guard.only_if_in_realized_revenue)


if __name__ == "__main__":
    unittest.main()
