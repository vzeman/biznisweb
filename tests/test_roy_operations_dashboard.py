import json
import unittest
from pathlib import Path

from live_dashboard_server import build_roy_operations_dashboard_html
from roy_operations_dashboard import (
    build_executive_kpi_snapshot,
    build_roy_orders_snapshot,
    resolve_roy_operations_settings,
)


ROOT_DIR = Path(__file__).resolve().parents[1]


def make_settings() -> dict:
    return resolve_roy_operations_settings(
        {
            "operations_dashboard": {
                "enabled": True,
                "paid_statuses": ["Platba online - zaplatené"],
                "cod_statuses": ["Čaká na vybavenie"],
                "cod_payment_patterns": ["dobierka", "dobírka"],
                "cod_payment_ids": ["7", "10"],
                "personal_pickup_shipping_names": ["Osobný odber na sklade"],
                "personal_pickup_shipping_ids": ["11"],
                "pickup_action_statuses": ["Čaká na vybavenie", "Platba online - zaplatené"],
                "shipped_status_name": "Odoslaná",
                "shipped_status_id": 4,
            }
        }
    )


def price_element(kind: str, title: str, reference_id: str = "") -> dict:
    return {
        "type": kind,
        "title": title,
        "value": "",
        "reference_id": reference_id,
        "price": {"value": 0, "formatted": "0,00 €"},
    }


def make_order(
    order_num: str,
    status_name: str,
    payment: dict,
    shipping: dict,
    pur_date: str = "2026-05-26 10:00:00",
) -> dict:
    return {
        "id": order_num,
        "order_num": order_num,
        "pur_date": pur_date,
        "last_change": pur_date,
        "status": {"id": 1, "name": status_name, "color": "#fff"},
        "price_elements": [shipping, payment],
        "sum": {"value": 120.0, "formatted": "120,00 €"},
        "items": [
            {
                "item_label": "Wachman HC800",
                "ean": "123",
                "import_code": "WA-HC800",
                "warehouse_number": "W1",
                "quantity": 2,
            }
        ],
    }


class RoyOperationsDashboardTests(unittest.TestCase):
    def test_roy_settings_enable_operations_dashboard_and_lead_times(self) -> None:
        project_settings = json.loads((ROOT_DIR / "projects" / "roy" / "settings.json").read_text(encoding="utf-8"))
        operations = resolve_roy_operations_settings(project_settings)
        inventory = project_settings["inventory_model"]

        self.assertTrue(operations["enabled"])
        self.assertEqual(90, operations["auto_refresh_seconds"])
        self.assertEqual(4, operations["shipped_status_id"])
        self.assertIn("Čaká na vybavenie", operations["cod_statuses"])
        self.assertEqual(30, inventory["lead_time_working_days_by_brand"]["wachman"])
        self.assertEqual(30, inventory["lead_time_working_days_by_brand"]["roy"])
        self.assertEqual(12, inventory["lead_time_working_days_by_brand"]["maco_stop"])
        self.assertEqual(3, inventory["lead_time_working_days_by_family"]["memory_storage"])

    def test_snapshot_includes_paid_and_cod_orders_only_for_fulfillment(self) -> None:
        settings = make_settings()
        pickup_shipping = price_element("shipping", "Osobný odber na sklade", "11")
        packeta_shipping = price_element("shipping", "Packeta - výdajné miesto", "9")
        orders = [
            make_order("R-1", "Platba online - zaplatené", price_element("payment", "Okamžitá platba online", "18"), packeta_shipping),
            make_order("R-2", "Čaká na vybavenie", price_element("payment", "Dobierkou", "7"), pickup_shipping),
            make_order("R-3", "Čaká na vybavenie", price_element("payment", "Bankovým prevodom", "6"), packeta_shipping),
            make_order("R-4", "Odoslaná", price_element("payment", "Dobierkou", "7"), pickup_shipping),
        ]

        snapshot = build_roy_orders_snapshot(project="roy", orders=orders, settings=settings)

        self.assertEqual(2, snapshot["summary"]["fulfillable_orders"])
        self.assertEqual(1, snapshot["summary"]["paid_online_orders"])
        self.assertEqual(1, snapshot["summary"]["cod_waiting_orders"])
        self.assertEqual(["R-1", "R-2"], [row["order_num"] for row in snapshot["orders"]])
        self.assertEqual(["R-2"], [row["order_num"] for row in snapshot["personal_pickups"]])
        self.assertTrue(snapshot["personal_pickups"][0]["pickup_action_allowed"])

    def test_executive_kpis_build_calendar_months_from_series(self) -> None:
        payload = {
            "generated_at": "2026-05-26T10:00:00Z",
            "date_from": "2026-04-01",
            "date_to": "2026-05-02",
            "dashboard": {
                "kpis": {
                    "metric_defs": [{"key": "revenue", "label_en": "Revenue"}],
                    "windows": {"monthly": {"metrics": {"revenue": 300}}},
                    "comparisons": {},
                },
                "series": {
                    "dates": ["2026-04-30", "2026-05-01", "2026-05-02"],
                    "revenue": [100, 50, 70],
                    "orders": [2, 1, 1],
                    "total_ads": [10, 0, 10],
                    "product_cost": [40, 20, 30],
                    "packaging": [1, 1, 1],
                    "shipping": [0, 0, 0],
                    "profit_without_fixed": [49, 29, 29],
                    "profit_with_fixed": [40, 20, 20],
                },
            },
        }

        snapshot = build_executive_kpi_snapshot(payload)

        self.assertEqual(["2026-04", "2026-05"], [row["key"] for row in snapshot["months"]])
        may = snapshot["months"][1]
        self.assertEqual(120, may["metrics"]["revenue"])
        self.assertEqual(2, may["metrics"]["orders"])
        self.assertEqual(60, may["metrics"]["aov"])
        self.assertIsNotNone(may["comparisons"]["revenue"]["vs_previous_month"])

    def test_html_contains_roy_operations_markers(self) -> None:
        html = build_roy_operations_dashboard_html("roy")

        self.assertIn('data-marker="roy-operations-dashboard"', html)
        self.assertIn("/api/operations/", html)
        self.assertIn("Executive KPI deck", html)
        self.assertIn("Osobné odbery", html)
        self.assertIn("visibleInventoryLimit = 100", html)


if __name__ == "__main__":
    unittest.main()
