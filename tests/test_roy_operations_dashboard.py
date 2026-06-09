import json
import shutil
import unittest
from pathlib import Path
from unittest.mock import patch

from dashboard_modern import DASHBOARD_PAYLOAD_SCRIPT_ID
from export_orders import BizniWebExporter
from live_dashboard_server import build_roy_operations_dashboard_html
from roy_operations_dashboard import (
    _select_current_stock_for_target,
    build_executive_kpi_snapshot,
    build_commercial_snapshot,
    build_inventory_snapshot,
    build_roy_orders_snapshot,
    filter_unprinted_picking_orders,
    mark_picking_orders_printed,
    mark_personal_pickup_ready,
    mark_personal_pickup_shipped,
    resolve_roy_operations_settings,
)


ROOT_DIR = Path(__file__).resolve().parents[1]


def make_project_settings() -> dict:
    return {
        "operations_dashboard": {
            "enabled": True,
            "paid_statuses": ["Platba online - zaplatené"],
            "cod_statuses": ["Čaká na vybavenie"],
            "cod_payment_patterns": [
                "dobierka",
                "dobírka",
                "utanvetes",
                "utanvet",
                "cash on delivery",
                "platnosc przy odbiorze",
                "nachnahme",
                "paiement a la livraison",
                "contra reembolso",
                "ramburs",
            ],
            "cod_payment_ids": ["7", "10", "16"],
            "personal_pickup_shipping_names": ["Osobný odber na sklade"],
            "personal_pickup_shipping_ids": ["11"],
            "pickup_ready_status_name": "Pripravené k odberu",
            "pickup_ready_status_id": 23,
            "pickup_ready_action_statuses": ["Platba online - zaplatené"],
            "pickup_ship_action_statuses": ["Pripravené k odberu"],
            "shipped_status_name": "Odoslaná",
            "shipped_status_id": 4,
        }
    }


def make_settings() -> dict:
    return resolve_roy_operations_settings(make_project_settings())


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
                "price": {"raw_value": 13.7, "value": 13.7, "formatted": "13,70 EUR", "is_net_price": True},
            }
        ],
    }


class FakePickupActionClient:
    def __init__(self, order: dict) -> None:
        self.order = order
        self.changed_status_ids: list[int] = []

    def execute(self, query: object, variable_values: dict | None = None) -> dict:
        values = variable_values or {}
        if "status_id" in values:
            self.changed_status_ids.append(values["status_id"])
            return {
                "changeOrderStatus": {
                    "order_num": values["order_num"],
                    "status": {"id": values["status_id"], "name": "target"},
                }
            }
        return {"getOrder": self.order}


class RoyOperationsDashboardTests(unittest.TestCase):
    def test_roy_settings_enable_operations_dashboard_and_lead_times(self) -> None:
        project_settings = json.loads((ROOT_DIR / "projects" / "roy" / "settings.json").read_text(encoding="utf-8"))
        operations = resolve_roy_operations_settings(project_settings)
        inventory = project_settings["inventory_model"]

        self.assertTrue(operations["enabled"])
        self.assertEqual(90, operations["auto_refresh_seconds"])
        self.assertEqual(23, operations["pickup_ready_status_id"])
        self.assertEqual("Pripravené k odberu", operations["pickup_ready_status_name"])
        self.assertEqual(4, operations["shipped_status_id"])
        self.assertEqual(10, operations["wholesale_detection"]["discount_threshold_pct"])
        self.assertTrue(operations["wholesale_detection"]["require_company_customer"])
        self.assertIn("Čaká na vybavenie", operations["cod_statuses"])
        self.assertIn("16", operations["cod_payment_ids"])
        self.assertIn("utanvetes", operations["cod_payment_patterns"])
        self.assertIn("cash on delivery", operations["cod_payment_patterns"])
        self.assertIn("platnosc przy odbiorze", operations["cod_payment_patterns"])
        self.assertIn("nachnahme", operations["cod_payment_patterns"])
        self.assertIn("paiement a la livraison", operations["cod_payment_patterns"])
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
            make_order("R-5", "Platba online - zaplatené", price_element("payment", "Okamžitá platba online", "18"), pickup_shipping),
            make_order("R-6", "Nezaplatená - zrušená objednávka", price_element("payment", "Okamžitá platba online", "18"), pickup_shipping),
            make_order("R-7", "Pripravené k odberu", price_element("payment", "Okamžitá platba online", "18"), pickup_shipping),
            make_order("R-8", "Čaká na vybavenie", price_element("payment", "Utánvétes fizetés", "16"), packeta_shipping),
        ]

        snapshot = build_roy_orders_snapshot(project="roy", orders=orders, settings=settings)

        self.assertEqual(4, snapshot["summary"]["fulfillable_orders"])
        self.assertEqual(2, snapshot["summary"]["paid_online_orders"])
        self.assertEqual(2, snapshot["summary"]["cod_waiting_orders"])
        self.assertEqual(["R-1", "R-2", "R-5", "R-8"], [row["order_num"] for row in snapshot["orders"]])
        self.assertEqual("13,70 EUR", snapshot["orders"][0]["items"][0]["unit_price_formatted"])
        self.assertEqual(["R-5", "R-7"], [row["order_num"] for row in snapshot["personal_pickups"]])
        self.assertEqual(1, snapshot["summary"]["pickup_ready_actions_available"])
        self.assertEqual(1, snapshot["summary"]["pickup_ship_actions_available"])
        self.assertEqual(2, snapshot["summary"]["pickup_actions_available"])
        cod_pickup = next(row for row in snapshot["orders"] if row["order_num"] == "R-2")
        self.assertFalse(cod_pickup["paid_personal_pickup"])
        self.assertFalse(cod_pickup["pickup_action_allowed"])
        paid_pickup = snapshot["personal_pickups"][0]
        ready_pickup = snapshot["personal_pickups"][1]
        self.assertTrue(paid_pickup["pickup_ready_action_allowed"])
        self.assertFalse(paid_pickup["pickup_ready"])
        self.assertFalse(paid_pickup["pickup_ship_action_allowed"])
        self.assertTrue(paid_pickup["paid_personal_pickup"])
        self.assertFalse(ready_pickup["pickup_ready_action_allowed"])
        self.assertTrue(ready_pickup["pickup_ready"])
        self.assertTrue(ready_pickup["pickup_ship_action_allowed"])
        self.assertTrue(ready_pickup["pickup_action_allowed"])
        self.assertTrue(ready_pickup["paid_personal_pickup"])

    def test_multilingual_cod_titles_are_fulfillable_without_known_payment_id(self) -> None:
        settings = make_settings()
        packeta_shipping = price_element("shipping", "Packeta - výdajné miesto", "9")
        payment_titles = [
            "Cash on delivery",
            "Płatność przy odbiorze",
            "Zahlung per Nachnahme",
            "Paiement à la livraison",
            "Pago contra reembolso",
            "Plata ramburs",
        ]
        orders = [
            make_order(
                f"FOREIGN-COD-{index}",
                "Čaká na vybavenie",
                price_element("payment", payment_title, str(900 + index)),
                packeta_shipping,
            )
            for index, payment_title in enumerate(payment_titles, start=1)
        ]

        snapshot = build_roy_orders_snapshot(project="roy", orders=orders, settings=settings)

        self.assertEqual(len(payment_titles), snapshot["summary"]["fulfillable_orders"])
        self.assertEqual(len(payment_titles), snapshot["summary"]["cod_waiting_orders"])
        self.assertTrue(all(row["fulfillment_reason"] == "cod_waiting" for row in snapshot["orders"]))

    def test_pickup_ready_and_ship_actions_use_separate_target_statuses(self) -> None:
        pickup_shipping = price_element("shipping", "Osobný odber na sklade", "11")
        paid_payment = price_element("payment", "Okamžitá platba online", "18")
        project_settings = make_project_settings()
        ready_client = FakePickupActionClient(
            make_order("R-READY", "Platba online - zaplatené", paid_payment, pickup_shipping)
        )
        ship_client = FakePickupActionClient(
            make_order("R-SHIP", "Pripravené k odberu", paid_payment, pickup_shipping)
        )

        with patch("roy_operations_dashboard.load_project_env"), patch(
            "roy_operations_dashboard.load_project_settings",
            return_value=project_settings,
        ), patch("roy_operations_dashboard._build_client", return_value=ready_client):
            ready_result = mark_personal_pickup_ready("roy", "R-READY")

        with patch("roy_operations_dashboard.load_project_env"), patch(
            "roy_operations_dashboard.load_project_settings",
            return_value=project_settings,
        ), patch("roy_operations_dashboard._build_client", return_value=ship_client):
            ship_result = mark_personal_pickup_shipped("roy", "R-SHIP")

        self.assertEqual([23], ready_client.changed_status_ids)
        self.assertEqual("ready", ready_result["action"])
        self.assertEqual("Pripravené k odberu", ready_result["target_status_name"])
        self.assertEqual([4], ship_client.changed_status_ids)
        self.assertEqual("ship", ship_result["action"])
        self.assertEqual("Odoslaná", ship_result["target_status_name"])

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

    def test_discounted_person_order_is_not_wholesale_when_company_signal_is_required(self) -> None:
        settings = make_settings()
        settings["wholesale_detection"]["require_company_customer"] = True
        paid_payment = price_element("payment", "Okamžitá platba online", "18")
        packeta_shipping = price_element("shipping", "Packeta - výdajné miesto", "9")
        order = make_order("R-DISCOUNT", settings["paid_statuses"][0], paid_payment, packeta_shipping)
        order["customer"] = {
            "__typename": "Person",
            "name": "Retail",
            "surname": "Customer",
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
        wholesale = snapshot["orders"][0]["wholesale_pricing"]

        self.assertFalse(wholesale["is_wholesale"])
        self.assertFalse(wholesale["customer_is_company"])
        self.assertEqual(20.0, wholesale["max_discount_pct"])
        self.assertEqual("1/1 discounted line(s), but customer is not Company", wholesale["reason"])

    def test_full_price_company_order_is_not_wholesale_from_vat_mismatch(self) -> None:
        settings = make_settings()
        paid_payment = price_element("payment", "Okamžitá platba online", "18")
        packeta_shipping = price_element("shipping", "Packeta - výdajné miesto", "9")
        order = make_order("R-FULL", settings["paid_statuses"][0], paid_payment, packeta_shipping)
        order["customer"] = {
            "__typename": "Company",
            "company_name": "Company Buyer s.r.o.",
            "company_id": "12345678",
        }
        order["items"][0].update(
            {
                "tax_rate": 23,
                "quantity": 1,
                "price": {"raw_value": 40.642276422764, "value": 40.64, "formatted": "40,64 €", "is_net_price": False},
                "sum": {"raw_value": 40.642276422764, "value": 40.64, "formatted": "40,64 €", "is_net_price": False},
                "sum_with_tax": {"raw_value": 49.99, "value": 49.99, "formatted": "49,99 €", "is_net_price": True},
                "product": {
                    "final_price": {
                        "raw_value": 49.99,
                        "value": 49.99,
                        "formatted": "49,99 €",
                        "is_net_price": False,
                    }
                },
            }
        )

        snapshot = build_roy_orders_snapshot(project="roy", orders=[order], settings=settings)
        wholesale = snapshot["orders"][0]["wholesale_pricing"]

        self.assertFalse(wholesale["is_wholesale"])
        self.assertTrue(wholesale["customer_is_company"])
        self.assertEqual(0.0, wholesale["max_discount_pct"])
        self.assertEqual("company customer, no wholesale price discount detected", wholesale["reason"])

    def test_discount_code_prevents_wholesale_flag(self) -> None:
        settings = make_settings()
        paid_payment = price_element("payment", "Okamžitá platba online", "18")
        packeta_shipping = price_element("shipping", "Packeta - výdajné miesto", "9")
        discount = {
            "type": "percent_discount",
            "title": "5% kod (5%)",
            "value": "5",
            "reference_id": "5",
            "price": {"value": -5, "formatted": "-5,00 €"},
        }
        order = make_order("R-CODE", settings["paid_statuses"][0], paid_payment, packeta_shipping)
        order["price_elements"].append(discount)
        order["customer"] = {
            "__typename": "Company",
            "company_name": "Code Buyer s.r.o.",
            "company_id": "12345678",
        }
        order["items"][0].update(
            {
                "tax_rate": 23,
                "quantity": 1,
                "sum_with_tax": {"raw_value": 80.0, "value": 80.0, "formatted": "80,00 €", "is_net_price": True},
                "product": {
                    "final_price": {
                        "raw_value": 100.0,
                        "value": 100.0,
                        "formatted": "100,00 €",
                        "is_net_price": False,
                    }
                },
            }
        )

        snapshot = build_roy_orders_snapshot(project="roy", orders=[order], settings=settings)
        wholesale = snapshot["orders"][0]["wholesale_pricing"]

        self.assertFalse(wholesale["is_wholesale"])
        self.assertTrue(wholesale["customer_is_company"])
        self.assertTrue(wholesale["discount_code_used"])
        self.assertEqual(20.0, wholesale["max_discount_pct"])
        self.assertEqual("discount code used", wholesale["reason"])

    def test_picking_pdf_orders_are_marked_printed_once(self) -> None:
        orders = [
            {"order_num": "R-1", "status": "Platba online - zaplatené", "purchase_at": "2026-05-28 09:00:00", "sum": "10,00 €"},
            {"order_num": "R-2", "status": "Čaká na vybavenie", "purchase_at": "2026-05-28 09:05:00", "sum": "20,00 €"},
        ]
        state = {"version": 1, "printed_picking_orders": {}}

        first_batch = filter_unprinted_picking_orders(orders, state)
        first_mark = mark_picking_orders_printed(state, first_batch, printed_at="2026-05-28T10:00:00Z")
        second_batch = filter_unprinted_picking_orders(orders, state)
        second_mark = mark_picking_orders_printed(state, second_batch, printed_at="2026-05-28T10:05:00Z")

        self.assertEqual(["R-1", "R-2"], [row["order_num"] for row in first_batch])
        self.assertEqual(["R-1", "R-2"], first_mark["order_nums"])
        self.assertEqual([], second_batch)
        self.assertEqual([], second_mark["order_nums"])
        self.assertEqual({"R-1", "R-2"}, set(state["printed_picking_orders"]))
        self.assertEqual("2026-05-28T10:00:00Z", state["printed_picking_orders"]["R-1"]["printed_at"])
        self.assertEqual(1, len(state["picking_print_batches"]))

    def test_picking_pdf_filter_keeps_new_orders_after_previous_print(self) -> None:
        orders = [{"order_num": "R-1"}, {"order_num": "R-2"}, {"order_num": "R-3"}]
        state = {
            "version": 1,
            "printed_picking_orders": {
                "R-1": {"order_num": "R-1", "printed_at": "2026-05-28T10:00:00Z"},
                "R-2": {"order_num": "R-2", "printed_at": "2026-05-28T10:00:00Z"},
            },
        }

        unprinted = filter_unprinted_picking_orders(orders, state)

        self.assertEqual(["R-3"], [row["order_num"] for row in unprinted])

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
        self.assertIn("loud-two-tone-v2", html)
        self.assertIn("playOrderAlertBurst", html)
        self.assertIn("notifyAboutNewFulfillableOrders", html)
        self.assertIn("data-ready-pickup", html)
        self.assertIn("/ready", html)
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

    def test_inventory_snapshot_prefers_operations_inventory_payload(self) -> None:
        payload = {
            "dashboard": {
                "roy_product_demand": {
                    "summary": {
                        "alert_delivery_count": 0,
                        "inventory_products_total": 0,
                    },
                    "alert_rows": [],
                    "inventory_rows": [],
                },
                "roy_operations_inventory": {
                    "summary": {
                        "alert_delivery_count": 1,
                        "inventory_products_total": 1,
                        "operations_inventory_source_period": "monthly",
                    },
                    "alert_rows": [
                        {
                            "sku": "F_206",
                            "product": "Micro SD KARTA 32GB s adapterom",
                            "available_quantity": 21,
                            "available_quantity_raw": 21,
                            "alert_30d_units": 26,
                            "lead_time_working_days": 3,
                            "suggested_reorder_units": 10,
                            "stock_risk_level": "Low",
                            "reorder_action_label": "30d alert",
                            "alert_30d_revenue": 260,
                            "alert_30d_profit_estimate": 80,
                        }
                    ],
                    "inventory_rows": [
                        {
                            "sku": "F_206",
                            "product": "Micro SD KARTA 32GB s adapterom",
                            "available_quantity": 21,
                            "available_quantity_raw": 21,
                            "alert_30d_units": 26,
                            "lead_time_working_days": 3,
                            "suggested_reorder_units": 10,
                            "stock_risk_level": "Low",
                            "reorder_action_label": "30d alert",
                        }
                    ],
                },
            }
        }

        inventory, state_changed = build_inventory_snapshot(
            payload,
            project_settings={
                "inventory_model": {
                    "critical_days_of_cover": 14,
                    "warning_days_of_cover": 30,
                    "watch_days_of_cover": 45,
                    "reorder_cover_days": 30,
                }
            },
        )

        self.assertFalse(state_changed)
        self.assertEqual(["F_206"], [row["sku"] for row in inventory["inventory_rows"]])
        self.assertEqual(["F_206"], [row["sku"] for row in inventory["alert_rows"]])
        self.assertEqual("monthly", inventory["summary"]["operations_inventory_source_period"])
        self.assertEqual(1, inventory["summary"]["alert_delivery_count"])

    def test_export_sidecar_embeds_monthly_operations_inventory_payload(self) -> None:
        exporter = BizniWebExporter(
            "https://example.invalid/graphql",
            "test-token",
            project_name="roy",
            artifact_subdir="_unit_operations_inventory",
            enable_period_bundle=False,
        )
        try:
            monthly_inventory = {
                "summary": {
                    "alert_delivery_count": 1,
                    "inventory_products_total": 1,
                },
                "alert_rows": [{"sku": "12468", "stock_risk_level": "Out of stock"}],
                "inventory_rows": [{"sku": "12468", "available_quantity": 0}],
            }
            monthly_report_path = exporter.output_path("monthly_report.html")
            monthly_report_path.write_text(
                (
                    f'<script id="{DASHBOARD_PAYLOAD_SCRIPT_ID}" type="application/json">'
                    + json.dumps({"roy_product_demand": monthly_inventory})
                    + "</script>"
                ),
                encoding="utf-8",
            )
            dashboard_payload = {
                "roy_product_demand": {
                    "summary": {
                        "alert_delivery_count": 0,
                        "inventory_products_total": 0,
                    },
                    "alert_rows": [],
                    "inventory_rows": [],
                }
            }

            exporter._enrich_roy_operations_inventory_payload(
                dashboard_payload,
                {
                    "current_key": "full",
                    "_embedded_specs": [
                        {"key": "monthly", "report_path": str(monthly_report_path)},
                    ],
                },
            )

            operations_inventory = dashboard_payload["roy_operations_inventory"]
            self.assertEqual(["12468"], [row["sku"] for row in operations_inventory["inventory_rows"]])
            self.assertEqual("monthly", operations_inventory["summary"]["operations_inventory_source_period"])
            self.assertEqual(1, operations_inventory["summary"]["alert_delivery_count"])
        finally:
            shutil.rmtree(exporter.data_dir, ignore_errors=True)

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

    def test_current_stock_overlay_clears_stale_out_of_stock_alert(self) -> None:
        payload = {
            "dashboard": {
                "roy_product_demand": {
                    "summary": {"alert_delivery_count": 1},
                    "alert_rows": [
                        {
                            "sku": "MICRO64",
                            "product": "Micro SD KARTA 64GB s adapterom",
                            "available_quantity": 0,
                            "available_quantity_raw": 0,
                            "alert_30d_units": 6,
                            "lead_time_working_days": 3,
                            "suggested_reorder_units": 7,
                            "stock_risk_level": "Out of stock",
                            "reorder_action_label": "Order now",
                        }
                    ],
                    "restock_priority_rows": [
                        {
                            "sku": "MICRO64",
                            "product": "Micro SD KARTA 64GB s adapterom",
                            "available_quantity": 0,
                            "available_quantity_raw": 0,
                            "alert_30d_units": 6,
                            "lead_time_working_days": 3,
                            "suggested_reorder_units": 7,
                            "stock_risk_level": "Out of stock",
                            "reorder_action_label": "Order now",
                            "restock_priority_score": 100,
                        }
                    ],
                    "inventory_rows": [
                        {
                            "sku": "MICRO64",
                            "product": "Micro SD KARTA 64GB s adapterom",
                            "available_quantity": 0,
                            "available_quantity_raw": 0,
                            "alert_30d_units": 6,
                            "lead_time_working_days": 3,
                            "suggested_reorder_units": 7,
                            "stock_risk_level": "Out of stock",
                            "reorder_action_label": "Order now",
                        }
                    ],
                }
            }
        }
        state = {"version": 1, "loss_acknowledgements": {}, "inbound_orders": {}, "auto_cleared_inbound_orders": []}

        inventory, state_changed = build_inventory_snapshot(
            payload,
            state=state,
            project_settings={"inventory_model": {"critical_days_of_cover": 14, "warning_days_of_cover": 30, "watch_days_of_cover": 45, "reorder_cover_days": 30}},
            current_stock_by_sku={
                "MICRO64": {
                    "available_quantity": 20,
                    "available_quantity_raw": 20,
                    "quantity": 20,
                    "quantity_raw": 20,
                    "active": True,
                    "matched_product_id": "4808",
                    "matched_product_title": "Micro SD KARTA 64GB s adapterom",
                }
            },
            live_stock_diagnostics={"matched_count": 1, "error_count": 0},
        )

        self.assertFalse(state_changed)
        self.assertEqual([], inventory["alert_rows"])
        self.assertEqual([], inventory["restock_priority_rows"])
        self.assertEqual(0, inventory["summary"]["alert_delivery_count"])
        self.assertEqual(1, inventory["summary"]["live_stock_overlay_matched_products"])
        self.assertEqual(20, inventory["inventory_rows"][0]["available_quantity"])
        self.assertEqual("Healthy", inventory["inventory_rows"][0]["stock_risk_level"])

    def test_current_stock_match_prefers_title_when_historical_ean_points_elsewhere(self) -> None:
        target = {"sku": "23942440833", "product": "Micro SD KARTA 64GB s adapterom"}
        search_results = {
            "micro sd karta 64gb s adapterom": [
                {
                    "id": "4808",
                    "title": "Micro SD KARTA 64GB s adapterom",
                    "active": True,
                    "ean": None,
                    "import_code": None,
                    "warehouse_items": [{"quantity": 20, "available_quantity": 20}],
                }
            ],
            "23942440833": [
                {
                    "id": "2873",
                    "title": "Micro SD KARTA 32GB s adapterom",
                    "active": True,
                    "ean": "23942440833",
                    "import_code": "F_206",
                    "warehouse_items": [{"quantity": 34, "available_quantity": 31}],
                }
            ],
        }

        stock = _select_current_stock_for_target(target, search_results)

        self.assertIsNotNone(stock)
        self.assertEqual("4808", stock["matched_product_id"])
        self.assertEqual(20, stock["available_quantity"])

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
