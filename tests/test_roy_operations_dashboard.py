import json
import unittest
from pathlib import Path

from live_dashboard_server import build_roy_operations_dashboard_html
from roy_operations_dashboard import (
    build_executive_kpi_snapshot,
    build_commercial_snapshot,
    build_inventory_snapshot,
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
        self.assertEqual(10, operations["wholesale_detection"]["discount_threshold_pct"])
        self.assertTrue(operations["wholesale_detection"]["require_company_customer"])
        self.assertIn("Čaká na vybavenie", operations["cod_statuses"])
        self.assertEqual(30, inventory["lead_time_working_days_by_brand"]["wachman"])
        self.assertEqual(30, inventory["lead_time_working_days_by_brand"]["roy"])
        self.assertEqual(12, inventory["lead_time_working_days_by_brand"]["maco_stop"])
        self.assertEqual(3, inventory["lead_time_working_days_by_family"]["memory_storage"])
        self.assertEqual(5, inventory["default_lead_time_working_days"])
        self.assertEqual(3, inventory["historical_restock_min_orders"])
        self.assertTrue(project_settings["product_identity"]["prefer_import_code"])
        maco_bundle_rule = inventory["bundle_component_rules"][0]
        self.assertEqual(3, len(maco_bundle_rule["components"]))
        self.assertTrue(maco_bundle_rule["exclude_bundle_from_alerts"])

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

    def test_snapshot_exposes_notes_addresses_and_wholesale_signal_for_pdf(self) -> None:
        settings = make_settings()
        paid_payment = price_element("payment", "Okamžitá platba online", "18")
        packeta_shipping = price_element("shipping", "Packeta - výdajné miesto", "9")
        order = make_order("R-VO", settings["paid_statuses"][0], paid_payment, packeta_shipping)
        order["note"] = "Prosime pribalit darcek."
        order["customer"] = {
            "__typename": "Company",
            "company_name": "B2B Partner s.r.o.",
            "company_id": "12345678",
            "vat_id": "SK1234567890",
            "phone": "+421900000000",
            "email": "b2b@example.com",
        }
        order["invoice_address"] = {
            "company_name": "B2B Partner s.r.o.",
            "street": "Hlavna",
            "descriptive_number": "12",
            "city": "Bratislava",
            "zip": "81101",
            "country": "Slovensko",
        }
        order["delivery_address"] = {
            "company_name": "Sklad B2B",
            "street": "Skladova",
            "descriptive_number": "5",
            "city": "Trnava",
            "zip": "91701",
            "country": "Slovensko",
        }
        order["items"][0].update(
            {
                "tax_rate": 23,
                "price": {"raw_value": 80.0, "value": 80.0, "formatted": "80,00 €", "is_net_price": True},
                "product": {
                    "final_price": {
                        "raw_value": 123.0,
                        "value": 123.0,
                        "formatted": "123,00 €",
                        "is_net_price": False,
                    }
                },
            }
        )

        snapshot = build_roy_orders_snapshot(project="roy", orders=[order], settings=settings)
        row = snapshot["orders"][0]

        self.assertEqual("Prosime pribalit darcek.", row["customer_note"])
        self.assertEqual("B2B Partner s.r.o.", row["customer"]["display_name"])
        self.assertIn("B2B Partner s.r.o.", row["invoice_address"]["lines"])
        self.assertIn("Sklad B2B", row["delivery_address"]["lines"])
        self.assertTrue(row["wholesale_pricing"]["is_wholesale"])
        self.assertEqual(20.0, row["wholesale_pricing"]["max_discount_pct"])

    def test_snapshot_expands_bundle_order_items_to_pickable_components(self) -> None:
        project_settings = json.loads((ROOT_DIR / "projects" / "roy" / "settings.json").read_text(encoding="utf-8"))
        settings = resolve_roy_operations_settings(project_settings)
        rules = {rule["key"]: rule for rule in project_settings["product_component_expansion_rules"]}
        packeta_shipping = price_element("shipping", "Packeta - vĂ˝dajnĂ© miesto", "9")
        paid_payment = price_element("payment", "OkamĹľitĂˇ platba online", "18")
        maco_rule = rules["maco_stop_large_set"]
        rio_rule = rules["wachman_rio_solar_4g"]
        paid_status = settings["paid_statuses"][0]
        maco_order = make_order("R-MACO", paid_status, paid_payment, packeta_shipping)
        maco_order["items"] = [
            {
                "item_label": maco_rule["bundle_patterns"][0],
                "ean": "",
                "import_code": "",
                "warehouse_number": "",
                "quantity": 2,
            }
        ]
        rio_order = make_order("R-RIO", paid_status, paid_payment, packeta_shipping)
        rio_order["items"] = [
            {
                "item_label": rio_rule["bundle_patterns"][0],
                "ean": "",
                "import_code": "",
                "warehouse_number": "",
                "quantity": 1,
            }
        ]

        snapshot = build_roy_orders_snapshot(project="roy", orders=[maco_order, rio_order], settings=settings)
        rows = {row["order_num"]: row for row in snapshot["orders"]}
        maco_items = rows["R-MACO"]["items"]
        rio_items = rows["R-RIO"]["items"]

        self.assertNotIn(maco_rule["bundle_patterns"][0], [item["label"] for item in maco_items])
        self.assertEqual({component["item_label"] for component in maco_rule["components"]}, {item["label"] for item in maco_items})
        self.assertEqual(
            {component["item_import_code"] for component in maco_rule["components"]},
            {item["import_code"] for item in maco_items},
        )
        self.assertEqual({2.0}, {item["quantity"] for item in maco_items})
        self.assertTrue(all(item["bundle_component"] for item in maco_items))

        self.assertNotIn(rio_rule["bundle_patterns"][0], [item["label"] for item in rio_items])
        self.assertEqual({component["item_label"] for component in rio_rule["components"]}, {item["label"] for item in rio_items})
        self.assertEqual(
            {component["item_import_code"] for component in rio_rule["components"]},
            {item["import_code"] for item in rio_items},
        )
        self.assertEqual({1.0}, {item["quantity"] for item in rio_items})
        self.assertTrue(all(item["bundle_component"] for item in rio_items))

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
        self.assertIn("visibleInventoryAlertLimit = 100", html)
        self.assertIn("visibleInventoryLimit = 100", html)
        self.assertIn("Top zna", html)
        self.assertIn("Krajiny", html)
        self.assertIn("countryPerformanceBody", html)
        self.assertIn("Produkty v strate", html)
        self.assertIn("Hrubý zisk/strata", html)
        self.assertNotIn("Zisk s fixom", html)
        self.assertIn("data-save-inbound", html)
        self.assertIn("soundToggleBtn", html)
        self.assertIn("playNewOrderSound", html)
        self.assertIn("notifyAboutNewFulfillableOrders", html)
        self.assertIn("Vysklad. PDF", html)
        self.assertIn("/api/operations/roy/picking-lists.pdf", html)
        self.assertIn("seenFulfillableOrderKeys", html)
        self.assertIn("nová objednávka na odoslanie", html)
        self.assertIn("replace(/[\"\\\\]/g, '\\\\$&')", html)
        self.assertNotIn('replace(/["\\]/g', html)

    def test_inbound_order_units_suppress_covered_stock_alert_until_restock(self) -> None:
        payload = {
            "dashboard": {
                "roy_product_demand": {
                    "summary": {
                        "alert_delivery_count": 1,
                        "stock_risk_30d_count": 1,
                    },
                    "alert_rows": [
                        {
                            "sku": "LOW-1",
                            "product": "Low stock product",
                            "available_quantity": 0,
                            "available_quantity_raw": 0,
                            "alert_30d_units": 3,
                            "lead_time_working_days": 5,
                            "suggested_reorder_units": 8,
                            "stock_risk_level": "Out of stock",
                            "reorder_action_label": "Order now",
                            "alert_30d_revenue": 90,
                            "alert_30d_profit_estimate": 30,
                        }
                    ],
                    "stock_risk_rows": [
                        {
                            "sku": "LOW-1",
                            "product": "Low stock product",
                            "available_quantity": 0,
                            "available_quantity_raw": 0,
                            "alert_30d_units": 3,
                            "lead_time_working_days": 5,
                            "suggested_reorder_units": 8,
                            "stock_risk_level": "Out of stock",
                            "reorder_action_label": "Order now",
                        }
                    ],
                    "restock_priority_rows": [
                        {
                            "sku": "LOW-1",
                            "product": "Low stock product",
                            "available_quantity": 0,
                            "available_quantity_raw": 0,
                            "alert_30d_units": 3,
                            "lead_time_working_days": 5,
                            "suggested_reorder_units": 8,
                            "stock_risk_level": "Out of stock",
                            "reorder_action_label": "Order now",
                            "restock_priority_score": 90,
                        }
                    ],
                    "inventory_rows": [
                        {
                            "sku": "LOW-1",
                            "product": "Low stock product",
                            "available_quantity": 0,
                            "available_quantity_raw": 0,
                            "alert_30d_units": 3,
                            "lead_time_working_days": 5,
                            "suggested_reorder_units": 8,
                            "stock_risk_level": "Out of stock",
                            "reorder_action_label": "Order now",
                        }
                    ],
                }
            }
        }
        state = {
            "version": 1,
            "loss_acknowledgements": {},
            "inbound_orders": {
                "LOW-1": {
                    "sku": "LOW-1",
                    "product": "Low stock product",
                    "ordered_units": 20,
                    "expected_arrival_date": "2026-06-05",
                    "baseline_available_quantity": 0,
                    "created_at": "2026-05-27T10:00:00Z",
                    "updated_at": "2026-05-27T10:00:00Z",
                }
            },
            "auto_cleared_inbound_orders": [],
        }

        inventory, state_changed = build_inventory_snapshot(
            payload,
            state=state,
            project_settings={"inventory_model": {"critical_days_of_cover": 14, "warning_days_of_cover": 30, "watch_days_of_cover": 45, "reorder_cover_days": 30}},
        )

        self.assertFalse(state_changed)
        self.assertEqual([], inventory["alert_rows"])
        self.assertEqual([], inventory["restock_priority_rows"])
        self.assertEqual(1, inventory["summary"]["inbound_order_count"])
        self.assertEqual(20, inventory["inbound_order_rows"][0]["ordered_units"])
        self.assertEqual("Inbound ordered", inventory["inventory_rows"][0]["reorder_action_label"])

    def test_inbound_order_marker_auto_clears_after_stock_increase(self) -> None:
        payload = {
            "dashboard": {
                "roy_product_demand": {
                    "summary": {},
                    "inventory_rows": [{"sku": "LOW-1", "product": "Low stock product", "available_quantity": 5}],
                }
            }
        }
        state = {
            "version": 1,
            "loss_acknowledgements": {},
            "inbound_orders": {
                "LOW-1": {
                    "sku": "LOW-1",
                    "product": "Low stock product",
                    "ordered_units": 5,
                    "expected_arrival_date": "2026-06-05",
                    "baseline_available_quantity": 0,
                }
            },
            "auto_cleared_inbound_orders": [],
        }

        inventory, state_changed = build_inventory_snapshot(
            payload,
            state=state,
            project_settings={"inventory_model": {}},
        )

        self.assertTrue(state_changed)
        self.assertEqual({}, state["inbound_orders"])
        self.assertEqual([], inventory["inbound_order_rows"])
        self.assertEqual(1, len(state["auto_cleared_inbound_orders"]))

    def test_commercial_snapshot_filters_acknowledged_loss_products(self) -> None:
        payload = {
            "dashboard": {
                "roy_product_demand": {
                    "brand_revenue_rows": [{"brand_key": "a"}, {"brand_key": "b"}, {"brand_key": "c"}, {"brand_key": "d"}],
                    "brand_profit_rows": [{"brand_key": "p1"}, {"brand_key": "p2"}, {"brand_key": "p3"}, {"brand_key": "p4"}],
                    "product_revenue_rows": [{"sku": f"R-{index}"} for index in range(12)],
                    "product_profit_rows": [{"sku": f"P-{index}"} for index in range(12)],
                    "country_rows": [{"country": "sk"}, {"country": "cz"}],
                    "loss_product_rows": [
                        {"sku": "LOSS-1", "gross_profit": -7, "revenue": 50},
                        {"sku": "LOSS-2", "gross_profit": -5, "revenue": 40},
                    ],
                },
                "geo_rows": [{"country": "sk", "paid_ads_spend": 25, "contribution_profit_with_fixed": 75}],
            }
        }

        snapshot = build_commercial_snapshot(
            payload,
            {"loss_acknowledgements": {"LOSS-1": {"sku": "LOSS-1"}}},
        )

        self.assertEqual(3, len(snapshot["brand_revenue_rows"]))
        self.assertEqual(10, len(snapshot["product_revenue_rows"]))
        self.assertEqual(["sk", "cz"], [row["country"] for row in snapshot["country_rows"]])
        self.assertEqual(25, snapshot["country_rows"][0]["spend"])
        self.assertEqual(75, snapshot["country_rows"][0]["profit_with_fixed"])
        self.assertEqual(["LOSS-2"], [row["sku"] for row in snapshot["loss_product_rows"]])
        self.assertEqual(1, snapshot["acknowledged_loss_product_count"])

    def test_commercial_snapshot_uses_gross_profit_for_product_rankings(self) -> None:
        payload = {
            "dashboard": {
                "roy_product_demand": {
                    "product_revenue_rows": [
                        {
                            "sku": "16689",
                            "product": "Wachman HC800",
                            "revenue": 1000,
                            "gross_profit": 440,
                            "profit_with_fixed": -120,
                            "gross_margin_pct": 44,
                        },
                        {
                            "sku": "P-GROSS-WIN",
                            "product": "Gross winner",
                            "revenue": 900,
                            "gross_profit": 500,
                            "profit_with_fixed": 50,
                            "gross_margin_pct": 55.6,
                        },
                    ],
                    "product_profit_rows": [
                        {"sku": "P-GROSS-WIN", "product": "Gross winner", "revenue": 900, "profit_with_fixed": 50},
                        {"sku": "16689", "product": "Wachman HC800", "revenue": 1000, "profit_with_fixed": -120},
                    ],
                    "loss_product_rows": [
                        {"sku": "16689", "product": "Wachman HC800", "revenue": 1000, "gross_profit": 440, "profit_with_fixed": -120},
                    ],
                },
            },
        }

        snapshot = build_commercial_snapshot(payload, {})

        self.assertEqual("16689", snapshot["product_revenue_rows"][0]["sku"])
        self.assertEqual("P-GROSS-WIN", snapshot["product_profit_rows"][0]["sku"])
        self.assertEqual(440, snapshot["product_profit_rows"][1]["gross_profit"])
        self.assertEqual([], snapshot["loss_product_rows"])

    def test_commercial_snapshot_uses_only_gross_loss_products(self) -> None:
        payload = {
            "dashboard": {
                "roy_product_demand": {
                    "loss_product_rows": [
                        {"sku": "GROSS-LOSS", "gross_profit": -3, "profit_with_fixed": 10, "revenue": 30},
                        {"sku": "CM1-LOSS", "cm1_profit": -2, "profit_with_fixed": -12, "revenue": 20},
                        {"sku": "FIXED-ONLY-LOSS", "profit_with_fixed": -25, "revenue": 100},
                        {"sku": "GROSS-PROFIT", "gross_profit": 5, "profit_with_fixed": -10, "revenue": 20},
                    ],
                },
            },
        }

        snapshot = build_commercial_snapshot(payload, {})

        self.assertEqual(["GROSS-LOSS", "CM1-LOSS"], [row["sku"] for row in snapshot["loss_product_rows"]])
        self.assertEqual(-3, snapshot["loss_product_rows"][0]["gross_profit"])
        self.assertEqual(-10.0, snapshot["loss_product_rows"][0]["gross_margin_pct"])


if __name__ == "__main__":
    unittest.main()
