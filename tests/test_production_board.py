import json
import unittest
from pathlib import Path

from production_board import build_production_board_snapshot, resolve_production_board_settings


ROOT_DIR = Path(__file__).resolve().parents[1]


def make_settings() -> dict:
    return resolve_production_board_settings(
        {
            "production_board": {
                "enabled": True,
                "active_order_statuses": [
                    "\u010cak\u00e1 na vybavenie",
                    "Platba online - zaplaten\u00e9",
                ],
                "manufactured_product_terms": ["vevo"],
                "excluded_product_labels": ["Vevo Ylang Absolute prac\u00ed g\u00e9l 1L"],
                "excluded_product_label_patterns": [],
            }
        }
    )


def make_order(order_num: str, status_name: str, items: list, pur_date: str = "2026-05-21 08:00:00") -> dict:
    return {
        "id": order_num,
        "order_num": order_num,
        "pur_date": pur_date,
        "last_change": pur_date,
        "status": {"id": 1, "name": status_name, "color": "#fff"},
        "customer": {"name": "Test", "surname": "Customer"},
        "items": items,
        "sum": {"formatted": "10,00 \u20ac"},
    }


def make_item(label: str, quantity: float, ean: str = "", import_code: str = "", warehouse_number: str = "") -> dict:
    return {
        "item_label": label,
        "quantity": quantity,
        "ean": ean,
        "import_code": import_code,
        "warehouse_number": warehouse_number,
    }


class ProductionBoardTests(unittest.TestCase):
    def test_vevo_settings_enable_board_with_current_exclusion(self) -> None:
        project_settings = json.loads((ROOT_DIR / "projects" / "vevo" / "settings.json").read_text(encoding="utf-8"))
        settings = resolve_production_board_settings(project_settings)

        self.assertTrue(settings["enabled"])
        self.assertEqual(
            ["\u010cak\u00e1 na vybavenie", "Platba online - zaplaten\u00e9"],
            settings["active_order_statuses"],
        )
        self.assertIn("Vevo Ylang Absolute prac\u00ed g\u00e9l 1L", settings["excluded_product_labels"])
        self.assertIn("vevo", settings["manufactured_product_terms"])

    def test_snapshot_filters_active_statuses_and_manufactured_products(self) -> None:
        settings = make_settings()
        orders = [
            make_order(
                "A-1",
                "\u010cak\u00e1 na vybavenie",
                [
                    make_item("Vevo Lavender prac\u00ed parfum 500ml", 2, ean="8580001"),
                    make_item("Other Brand product", 3, ean="9990001"),
                    make_item("Vevo Ylang Absolute prac\u00ed g\u00e9l 1L", 4, ean="8589999"),
                ],
                "2026-05-20 10:00:00",
            ),
            make_order(
                "A-2",
                "Platba online - zaplaten\u00e9",
                [make_item("Vevo Lavender prac\u00ed parfum 500ml", 1, ean="8580001")],
                "2026-05-21 09:00:00",
            ),
            make_order(
                "A-3",
                "Odoslan\u00e1",
                [make_item("Vevo Lavender prac\u00ed parfum 500ml", 9, ean="8580001")],
                "2026-05-21 10:00:00",
            ),
        ]

        snapshot = build_production_board_snapshot(project="vevo", orders=orders, settings=settings)

        self.assertEqual(2, snapshot["summary"]["active_orders"])
        self.assertEqual(2, snapshot["summary"]["manufacturing_orders"])
        self.assertEqual(1, snapshot["summary"]["manufacturing_products"])
        self.assertEqual(3.0, snapshot["summary"]["units_to_make"])
        self.assertEqual(["A-1", "A-2"], [order["order_num"] for order in snapshot["orders"]])

        product = snapshot["products"][0]
        self.assertEqual("Vevo Lavender prac\u00ed parfum 500ml", product["label"])
        self.assertEqual("8580001", product["identifier"])
        self.assertEqual(3.0, product["quantity_required"])
        self.assertEqual(2, product["orders_count"])

        ignored_reasons = {item["reason"] for item in snapshot["ignored_items"]}
        self.assertEqual({"excluded_product", "not_manufactured_brand"}, ignored_reasons)
        self.assertEqual(7.0, snapshot["summary"]["ignored_units"])

    def test_status_matching_handles_missing_diacritics(self) -> None:
        settings = make_settings()
        snapshot = build_production_board_snapshot(
            project="vevo",
            orders=[
                make_order(
                    "A-1",
                    "Caka na vybavenie",
                    [make_item("VEVO citrus 200ml", 2, import_code="V-CITRUS")],
                )
            ],
            settings=settings,
        )

        self.assertEqual(1, snapshot["summary"]["active_orders"])
        self.assertEqual(2.0, snapshot["summary"]["units_to_make"])
        self.assertEqual("V-CITRUS", snapshot["products"][0]["identifier"])


if __name__ == "__main__":
    unittest.main()
