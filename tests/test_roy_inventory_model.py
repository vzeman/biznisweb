import json
import unittest
from pathlib import Path
from unittest.mock import Mock

import pandas as pd

from export_orders import BizniWebExporter


class RoyInventoryModelExporter(BizniWebExporter):
    def __init__(self, inventory_snapshot: pd.DataFrame):
        super().__init__(
            api_url="https://example.test/graphql",
            api_token="test-token",
            project_name="roy",
            output_tag="inventory-model-test",
        )
        self.inventory_snapshot = inventory_snapshot

    def fetch_product_inventory_snapshot(self, lang_code: str = "SK", page_limit: int = 30) -> pd.DataFrame:
        return self.inventory_snapshot.copy()


def item_row(
    order_num: str,
    product_sku: str,
    item_label: str,
    purchase_datetime: str,
    quantity: float,
    revenue: float,
    item_import_code: str = "",
    item_ean: str = "",
    cm2_profit: float | None = None,
    cm3_profit: float | None = None,
    cm1_profit: float | None = None,
    total_expense: float | None = None,
    allocated_paid_spend: float = 0.0,
    country: str = "SK",
) -> dict:
    row = {
        "order_num": order_num,
        "product_sku": product_sku,
        "item_label": item_label,
        "item_import_code": item_import_code,
        "item_ean": item_ean,
        "purchase_datetime": purchase_datetime,
        "item_quantity": quantity,
        "item_total_without_tax": revenue,
        "cm1_profit": revenue * 0.5 if cm1_profit is None else cm1_profit,
        "cm2_profit": revenue * 0.4 if cm2_profit is None else cm2_profit,
        "cm3_profit": revenue * 0.3 if cm3_profit is None else cm3_profit,
        "allocated_paid_spend": allocated_paid_spend,
        "geo_country": country,
    }
    if total_expense is not None:
        row["total_expense"] = total_expense
    return row


def inventory_row(
    sku: str,
    product: str,
    available_quantity: float = 0.0,
    inventory_cost_value: float = 0.0,
    inventory_retail_value: float = 0.0,
) -> dict:
    return {
        "reporting_sku": sku,
        "reporting_product": product,
        "active": True,
        "available_quantity": available_quantity,
        "available_quantity_raw": available_quantity,
        "quantity": available_quantity,
        "quantity_raw": available_quantity,
        "mapped_available_quantity": available_quantity if inventory_cost_value else 0.0,
        "inventory_cost_value": inventory_cost_value,
        "inventory_retail_value": inventory_retail_value,
        "mapped_inventory_retail_value": inventory_retail_value if inventory_cost_value else 0.0,
    }


class RoyInventoryModelTests(unittest.TestCase):
    @staticmethod
    def _one_off_spike_rows() -> pd.DataFrame:
        return pd.DataFrame(
            [
                item_row("R-OLD-1", "SPIKE-SKU", "Slow Product", "2025-07-10", 1, 10),
                item_row("R-OLD-2", "SPIKE-SKU", "Slow Product", "2025-10-10", 1, 10),
                item_row("R-OLD-3", "SPIKE-SKU", "Slow Product", "2026-02-10", 1, 10),
                item_row("R-SPIKE", "SPIKE-SKU", "Slow Product", "2026-07-01", 40, 400),
            ]
        )

    def test_one_off_large_order_does_not_create_false_restock_alert(self) -> None:
        exporter = RoyInventoryModelExporter(
            inventory_snapshot=pd.DataFrame(
                [inventory_row("SPIKE-SKU", "Slow Product", 20, 100, 300)]
            )
        )

        result = exporter.analyze_roy_product_demand_analytics(
            df=pd.DataFrame(),
            orders_df=pd.DataFrame(),
            item_df=self._one_off_spike_rows(),
        )

        row = result["inventory_rows"].loc[
            result["inventory_rows"]["sku"] == "SPIKE-SKU"
        ].iloc[0]
        self.assertEqual(40.0, float(row["raw_recent_30d_units"]))
        self.assertGreaterEqual(float(row["raw_alert_30d_units"]), 40.0)
        self.assertLess(float(row["alert_30d_units"]), 5.0)
        self.assertEqual("Healthy", row["stock_risk_level"])
        self.assertTrue(bool(row["unusual_large_order_flag"]))
        self.assertEqual("one_off_large_order", row["demand_signal_code"])
        self.assertNotIn("SPIKE-SKU", set(result["alert_rows"].get("sku", [])))
        self.assertIn("SPIKE-SKU", set(result["demand_anomaly_rows"]["sku"]))
        self.assertEqual("order-aware-tsb-v1", result["summary"]["demand_model_version"])

    def test_old_sales_outside_forecast_lookback_still_prevent_false_alert(self) -> None:
        exporter = RoyInventoryModelExporter(
            inventory_snapshot=pd.DataFrame(
                [inventory_row("OLD-SPIKE-SKU", "Very Slow Product", 20, 100, 300)]
            )
        )
        item_df = pd.DataFrame(
            [
                item_row("R-OLD-1", "OLD-SPIKE-SKU", "Very Slow Product", "2023-01-10", 1, 10),
                item_row("R-OLD-2", "OLD-SPIKE-SKU", "Very Slow Product", "2023-10-10", 1, 10),
                item_row("R-OLD-3", "OLD-SPIKE-SKU", "Very Slow Product", "2024-07-10", 1, 10),
                item_row("R-SPIKE", "OLD-SPIKE-SKU", "Very Slow Product", "2026-07-01", 40, 400),
            ]
        )

        result = exporter.analyze_roy_product_demand_analytics(
            df=pd.DataFrame(),
            orders_df=pd.DataFrame(),
            item_df=item_df,
        )

        row = result["inventory_rows"].loc[
            result["inventory_rows"]["sku"] == "OLD-SPIKE-SKU"
        ].iloc[0]
        self.assertTrue(bool(row["smart_demand_model_active"]))
        self.assertTrue(bool(row["unusual_large_order_flag"]))
        self.assertEqual(40.0, float(row["raw_recent_30d_units"]))
        self.assertLess(float(row["alert_30d_units"]), 5.0)
        self.assertEqual("Healthy", row["stock_risk_level"])
        self.assertNotIn("OLD-SPIKE-SKU", set(result["alert_rows"].get("sku", [])))

    def test_zero_stock_after_one_off_order_remains_visible_as_state_alert(self) -> None:
        exporter = RoyInventoryModelExporter(inventory_snapshot=pd.DataFrame())

        result = exporter.analyze_roy_product_demand_analytics(
            df=pd.DataFrame(),
            orders_df=pd.DataFrame(),
            item_df=self._one_off_spike_rows(),
        )

        row = result["alert_rows"].loc[result["alert_rows"]["sku"] == "SPIKE-SKU"].iloc[0]
        self.assertEqual("Out of stock", row["stock_risk_level"])
        self.assertEqual("low_stock_after_large_order", row["alert_reason_code"])
        self.assertLess(float(row["alert_30d_units"]), 5.0)

    def test_short_history_keeps_legacy_demand_floor(self) -> None:
        exporter = RoyInventoryModelExporter(
            inventory_snapshot=pd.DataFrame(
                [inventory_row("NEW-SKU", "New Product", 20, 100, 300)]
            )
        )
        item_df = pd.DataFrame(
            [
                item_row("R-1", "NEW-SKU", "New Product", "2026-06-10", 1, 10),
                item_row("R-2", "NEW-SKU", "New Product", "2026-06-15", 1, 10),
                item_row("R-3", "NEW-SKU", "New Product", "2026-06-20", 1, 10),
                item_row("R-4", "NEW-SKU", "New Product", "2026-07-01", 40, 400),
            ]
        )

        result = exporter.analyze_roy_product_demand_analytics(
            df=pd.DataFrame(),
            orders_df=pd.DataFrame(),
            item_df=item_df,
        )
        row = result["inventory_rows"].loc[
            result["inventory_rows"]["sku"] == "NEW-SKU"
        ].iloc[0]

        self.assertFalse(bool(row["smart_demand_model_active"]))
        self.assertGreaterEqual(float(row["alert_30d_units"]), 40.0)

    def test_live_inventory_uses_canonical_64gb_identity_cost_and_keeps_32gb_separate(self) -> None:
        exporter = BizniWebExporter(
            api_url="https://example.test/graphql",
            api_token="test-token",
            project_name="roy",
            output_tag="inventory-identity-test",
        )
        exporter.product_expenses_exact.update(
            {
                "MICRO-SD-64GB": 3.3,
                "F_206": 1.8,
            }
        )
        exporter.client.execute = Mock(
            return_value={
                "getProductList": {
                    "data": [
                        {
                            "id": "64",
                            "title": "Micro SD CARD 64GB s adaptérem",
                            "active": True,
                            "ean": "",
                            "import_code": "",
                            "price": {"value": 10.0, "currency": {"code": "EUR"}},
                            "final_price": {"value": 10.0, "currency": {"code": "EUR"}},
                            "warehouse_items": [
                                {
                                    "id": "W64",
                                    "warehouse_number": "",
                                    "quantity": 20,
                                    "available_quantity": 20,
                                    "status": {"id": "1", "name": "Skladom"},
                                    "price": {"value": 10.0, "currency": {"code": "EUR"}},
                                    "final_price": {"value": 10.0, "currency": {"code": "EUR"}},
                                }
                            ],
                        },
                        {
                            "id": "32",
                            "title": "Micro SD CARD 32GB s adaptérem",
                            "active": True,
                            "ean": "23942440833",
                            "import_code": "F_206",
                            "price": {"value": 8.0, "currency": {"code": "EUR"}},
                            "final_price": {"value": 8.0, "currency": {"code": "EUR"}},
                            "warehouse_items": [
                                {
                                    "id": "W32",
                                    "warehouse_number": "",
                                    "quantity": 10,
                                    "available_quantity": 10,
                                    "status": {"id": "1", "name": "Skladom"},
                                    "price": {"value": 8.0, "currency": {"code": "EUR"}},
                                    "final_price": {"value": 8.0, "currency": {"code": "EUR"}},
                                }
                            ],
                        },
                    ],
                    "pageInfo": {"hasNextPage": False, "nextCursor": None},
                }
            }
        )

        snapshot = exporter.fetch_product_inventory_snapshot(lang_code="SK")
        card_64gb = snapshot[snapshot["reporting_sku"] == "MICRO-SD-64GB"].iloc[0]
        card_32gb = snapshot[snapshot["reporting_sku"] == "F_206"].iloc[0]

        self.assertEqual("H-69235D5B", card_64gb["raw_product_sku"])
        self.assertEqual("Micro SD KARTA 64GB s adaptérom", card_64gb["reporting_product"])
        self.assertEqual(3.3, card_64gb["cost_per_unit"])
        self.assertEqual(66.0, card_64gb["inventory_cost_value"])
        self.assertEqual("F_206", card_32gb["reporting_sku"])
        self.assertEqual(1.8, card_32gb["cost_per_unit"])

    def test_roy_reporting_product_identity_prefers_import_code_across_languages(self) -> None:
        exporter = RoyInventoryModelExporter(inventory_snapshot=pd.DataFrame())
        item_df = pd.DataFrame(
            [
                item_row(
                    "R-HU",
                    "H-HU",
                    "Micro SD CARD 32GB adapterrel",
                    "2026-05-01",
                    1,
                    12,
                    item_import_code="12474",
                ),
                item_row(
                    "R-CZ",
                    "H-CZ",
                    "Micro SD CARD 32GB s adaptérem",
                    "2026-05-02",
                    1,
                    12,
                    item_import_code="12474",
                ),
            ]
        )

        canonical_df = exporter.add_reporting_product_identity_columns(item_df)

        self.assertEqual(["12474"], canonical_df["product_sku"].drop_duplicates().tolist())
        self.assertEqual(["H-HU", "H-CZ"], canonical_df["raw_product_sku"].tolist())
        self.assertEqual(["12474", "12474"], canonical_df["raw_item_import_code"].tolist())

    def test_product_expense_mapping_falls_back_to_legacy_title_hash(self) -> None:
        exporter = RoyInventoryModelExporter(inventory_snapshot=pd.DataFrame())
        legacy_title_hash = exporter.get_product_sku("", "Micro SD CARD 32GB adapterrel")
        exporter.product_expenses_exact = {legacy_title_hash: 4.5}
        exporter.product_expenses_normalized = {
            exporter._normalize_match_text(legacy_title_hash): 4.5,
        }

        cost, source = exporter._resolve_product_expense(
            product_sku="12474",
            item_label="Micro SD CARD 32GB adapterrel",
            import_code="12474",
            ean="8590000000000",
        )

        self.assertEqual(4.5, cost)
        self.assertEqual("mapped_legacy_title_hash", source)

    def test_maco_stop_large_set_cost_aliases_resolve_to_configured_cost(self) -> None:
        expense_path = Path(__file__).resolve().parents[1] / "projects" / "roy" / "product_expenses.json"
        cost_map = json.loads(expense_path.read_text(encoding="utf-8"))
        expected_cost = 26.58

        self.assertEqual(expected_cost, cost_map["133652"])
        self.assertEqual(expected_cost, cost_map["H-226DA29F"])
        self.assertEqual(expected_cost, cost_map["H-CF33B34C"])

        exporter = RoyInventoryModelExporter(inventory_snapshot=pd.DataFrame())
        exporter.product_expenses_exact = cost_map
        exporter.product_expenses_normalized = {
            exporter._normalize_match_text(key): float(value)
            for key, value in cost_map.items()
            if exporter._normalize_match_text(key)
        }

        scenarios = [
            (
                {
                    "product_sku": "H-226DA29F",
                    "item_label": "Set MACO STOP VEĽKÝ",
                    "import_code": "",
                    "warehouse_number": "",
                    "ean": "",
                },
                "mapped_product_sku",
            ),
            (
                {
                    "product_sku": "H-CF33B34C",
                    "item_label": "Set proti medveďom VEĽKÝ",
                    "import_code": "133652",
                    "warehouse_number": "",
                    "ean": "",
                },
                "mapped_product_identifier",
            ),
            (
                {
                    "product_sku": "",
                    "item_label": "Set proti medveďom VEĽKÝ",
                    "import_code": "",
                    "warehouse_number": "133652",
                    "ean": "",
                },
                "mapped_product_identifier",
            ),
        ]

        for kwargs, expected_source in scenarios:
            with self.subTest(kwargs=kwargs):
                cost, source = exporter._resolve_product_expense(**kwargs)
                self.assertEqual(expected_cost, cost)
                self.assertEqual(expected_source, source)

    def test_wachman_hc800_import_code_resolves_to_configured_cost(self) -> None:
        expense_path = Path(__file__).resolve().parents[1] / "projects" / "roy" / "product_expenses.json"
        cost_map = json.loads(expense_path.read_text(encoding="utf-8"))
        expected_cost = 13.7

        self.assertEqual(expected_cost, cost_map["16689"])
        self.assertEqual(expected_cost, cost_map["H-F15A179C"])
        self.assertEqual(expected_cost, cost_map["H-799006D2"])
        self.assertEqual(expected_cost, cost_map["H-8865AA6B"])

        exporter = RoyInventoryModelExporter(inventory_snapshot=pd.DataFrame())
        exporter.product_expenses_exact = cost_map
        exporter.product_expenses_normalized = {
            exporter._normalize_match_text(key): float(value)
            for key, value in cost_map.items()
            if exporter._normalize_match_text(key)
        }

        cost, source = exporter._resolve_product_expense(
            product_sku="16689",
            item_label="Wachman HC800",
            import_code="16689",
            warehouse_number="",
            ean="",
        )

        self.assertEqual(expected_cost, cost)
        self.assertEqual("mapped_product_identifier", source)

    def test_restock_alerts_include_relevant_historical_products_without_inventory_rows(self) -> None:
        exporter = RoyInventoryModelExporter(inventory_snapshot=pd.DataFrame())
        item_df = pd.DataFrame(
            [
                item_row("R-1", "GEN-RELEVANT", "Generic Charger", "2026-05-01", 2, 120),
                item_row("R-2", "GEN-RELEVANT", "Generic Charger", "2026-05-10", 2, 120),
                item_row("R-3", "GEN-RELEVANT", "Generic Charger", "2026-05-20", 2, 120),
                item_row("R-4", "GEN-NOISE", "One-off Accessory", "2026-05-11", 1, 40),
                item_row("R-5", "GEN-NOISE", "One-off Accessory", "2026-05-21", 1, 40),
            ]
        )

        result = exporter.analyze_roy_product_demand_analytics(
            df=pd.DataFrame(),
            orders_df=pd.DataFrame(),
            item_df=item_df,
        )

        alert_rows = result["alert_rows"]
        restock_rows = result["restock_priority_rows"]
        self.assertIn("GEN-RELEVANT", set(alert_rows["sku"]))
        self.assertIn("GEN-RELEVANT", set(restock_rows["sku"]))
        self.assertNotIn("GEN-NOISE", set(alert_rows["sku"]))
        self.assertNotIn("GEN-NOISE", set(restock_rows["sku"]))

        alert_row = alert_rows.loc[alert_rows["sku"] == "GEN-RELEVANT"].iloc[0]
        self.assertEqual("Out of stock", alert_row["stock_risk_level"])
        self.assertEqual(5, int(alert_row["lead_time_working_days"]))
        self.assertTrue(bool(alert_row["history_only_inventory_flag"]))
        self.assertTrue(pd.isna(alert_row["suggested_reorder_units"]))
        self.assertGreater(float(alert_row["suggested_reorder_units_estimate"]), 0)
        self.assertFalse(bool(alert_row["recommendation_ready"]))
        self.assertEqual("Review risk", alert_row["reorder_action_label"])

        summary = result["summary"]
        self.assertEqual(1, summary["history_only_inventory_products"])
        self.assertEqual(1, summary["historical_restock_relevant_products"])
        self.assertEqual("warning_only", summary["inventory_recommendation_status"])
        self.assertIn("inbound purchase orders are not modeled", summary["inventory_recommendation_blockers"])

    def test_service_work_rows_are_excluded_from_restock_alerts(self) -> None:
        exporter = RoyInventoryModelExporter(inventory_snapshot=pd.DataFrame())
        item_df = pd.DataFrame(
            [
                item_row("R-1", "SERVICE-1", "Diagnostika / praca / testovanie", "2026-05-01", 1, 100),
                item_row("R-2", "SERVICE-1", "Diagnostika / praca / testovanie", "2026-05-10", 1, 100),
                item_row("R-3", "SERVICE-1", "Diagnostika / praca / testovanie", "2026-05-20", 1, 100),
            ]
        )

        result = exporter.analyze_roy_product_demand_analytics(
            df=pd.DataFrame(),
            orders_df=pd.DataFrame(),
            item_df=item_df,
        )

        self.assertNotIn("SERVICE-1", set(result["alert_rows"].get("sku", [])))
        self.assertNotIn("SERVICE-1", set(result["restock_priority_rows"].get("sku", [])))
        excluded = result["inventory_rows"]
        if not excluded.empty and "sku" in excluded.columns:
            service_rows = excluded.loc[excluded["sku"] == "SERVICE-1"]
            if not service_rows.empty:
                self.assertTrue(bool(service_rows.iloc[0]["alert_excluded_flag"]))

    def test_maco_stop_large_set_demand_is_shifted_to_components(self) -> None:
        inventory_snapshot = pd.DataFrame(
            [
                inventory_row(
                    "MACO-EXTREME-300",
                    "Najsilnejší sprej na medvede MACO STOP Extreme 300ml hmla",
                ),
                inventory_row(
                    "MACO-EXTREME-300-GEL",
                    "Najsilnejší sprej na medvede MACO STOP Extreme 300ml gel",
                ),
                inventory_row("MACO-HOLSTER-300", "Puzdro MACO STOP na sprej 300ml"),
                inventory_row("BEAR-BELL", "Zvonček na medvede, plašič medveďov"),
            ]
        )
        exporter = RoyInventoryModelExporter(inventory_snapshot=inventory_snapshot)
        item_df = pd.DataFrame(
            [
                item_row("R-1", "SET-MACO-LARGE", "Set MACO STOP VEĽKÝ", "2026-05-01", 1, 120),
                item_row("R-2", "SET-MACO-LARGE", "Set MACO STOP VEĽKÝ", "2026-05-10", 1, 120),
                item_row("R-3", "SET-MACO-LARGE", "Set MACO STOP VEĽKÝ", "2026-05-20", 1, 120),
            ]
        )

        result = exporter.analyze_roy_product_demand_analytics(
            df=pd.DataFrame(),
            orders_df=pd.DataFrame(),
            item_df=item_df,
        )

        alert_rows = result["alert_rows"]
        restock_rows = result["restock_priority_rows"]
        expected_component_skus = {"MACO-EXTREME-300", "MACO-HOLSTER-300", "BEAR-BELL"}

        self.assertTrue(expected_component_skus.issubset(set(alert_rows["sku"])))
        self.assertTrue(expected_component_skus.issubset(set(restock_rows["sku"])))
        self.assertNotIn("SET-MACO-LARGE", set(alert_rows["sku"]))
        self.assertNotIn("SET-MACO-LARGE", set(restock_rows["sku"]))
        self.assertNotIn("MACO-EXTREME-300-GEL", set(alert_rows["sku"]))
        self.assertNotIn("MACO-EXTREME-300-GEL", set(restock_rows["sku"]))

        for component_sku in expected_component_skus:
            row = alert_rows.loc[alert_rows["sku"] == component_sku].iloc[0]
            self.assertEqual("Out of stock", row["stock_risk_level"])
            self.assertEqual(3.0, float(row["bundle_component_recent_30d_units"]))
            self.assertEqual(3.0, float(row["alert_30d_units"]))
            self.assertTrue(bool(row["historical_restock_relevant_flag"]))

        summary = result["summary"]
        self.assertEqual(1, summary["bundle_component_rule_count"])
        self.assertEqual(9.0, float(summary["bundle_component_adjustment_30d_units"]))
        self.assertGreaterEqual(summary["historical_restock_relevant_products"], 3)

    def test_roy_demand_outputs_top_products_and_loss_products(self) -> None:
        exporter = RoyInventoryModelExporter(inventory_snapshot=pd.DataFrame())
        item_df = pd.DataFrame(
            [
                item_row("R-1", "P-WIN", "Wachman Profit Product", "2026-05-01", 2, 300, cm1_profit=260, cm2_profit=120, cm3_profit=90, total_expense=40),
                item_row("R-2", "P-LOSS", "Loss Product", "2026-05-02", 2, 120, cm1_profit=-10, cm2_profit=-15, cm3_profit=-25, total_expense=130),
                item_row("R-3", "P-REV", "Revenue Product", "2026-05-03", 1, 500, cm2_profit=40, cm3_profit=30),
                item_row("R-4", "P-FIXED-LOSS", "Fixed Loss Product", "2026-05-04", 1, 100, cm1_profit=50, cm2_profit=20, cm3_profit=-10, total_expense=50),
            ]
        )

        result = exporter.analyze_roy_product_demand_analytics(
            df=pd.DataFrame(),
            orders_df=pd.DataFrame(),
            item_df=item_df,
        )

        self.assertEqual("P-REV", result["product_revenue_rows"].iloc[0]["sku"])
        self.assertEqual("P-WIN", result["product_profit_rows"].iloc[0]["sku"])
        self.assertEqual(["P-LOSS"], result["loss_product_rows"]["sku"].tolist())
        self.assertEqual(-10, float(result["loss_product_rows"].iloc[0]["gross_profit"]))
        self.assertLess(float(result["loss_product_rows"].iloc[0]["gross_profit"]), 0)
        self.assertNotIn("P-FIXED-LOSS", result["loss_product_rows"]["sku"].tolist())

    def test_wachman_hc800_fixed_loss_is_not_product_loss(self) -> None:
        exporter = RoyInventoryModelExporter(inventory_snapshot=pd.DataFrame())
        item_df = pd.DataFrame(
            [
                item_row(
                    "R-HC800",
                    "16689",
                    "Wachman HC800",
                    "2026-05-01",
                    3,
                    73.14,
                    item_import_code="16689",
                    cm1_profit=32.04,
                    cm2_profit=24.00,
                    cm3_profit=-6.00,
                    total_expense=41.10,
                ),
            ]
        )

        result = exporter.analyze_roy_product_demand_analytics(
            df=pd.DataFrame(),
            orders_df=pd.DataFrame(),
            item_df=item_df,
        )

        revenue_row = result["product_revenue_rows"].iloc[0]
        self.assertEqual("16689", revenue_row["sku"])
        self.assertEqual("Wachman HC800", revenue_row["product"])
        self.assertEqual(32.04, float(revenue_row["gross_profit"]))
        self.assertEqual(-6.00, float(revenue_row["profit_with_fixed"]))
        self.assertTrue(result["loss_product_rows"].empty)

    def test_roy_demand_outputs_country_performance_with_top_products(self) -> None:
        exporter = RoyInventoryModelExporter(inventory_snapshot=pd.DataFrame())
        item_df = pd.DataFrame(
            [
                item_row(
                    "R-SK-1",
                    "P-SK-A",
                    "SK hero product",
                    "2026-05-01",
                    2,
                    300,
                    cm1_profit=180,
                    cm2_profit=150,
                    cm3_profit=120,
                    allocated_paid_spend=30,
                    country="Slovensko",
                ),
                item_row(
                    "R-SK-2",
                    "P-SK-B",
                    "SK secondary product",
                    "2026-05-02",
                    1,
                    120,
                    cm1_profit=70,
                    cm2_profit=55,
                    cm3_profit=40,
                    allocated_paid_spend=15,
                    country="SK",
                ),
                item_row(
                    "R-CZ-1",
                    "P-CZ-A",
                    "CZ hero product",
                    "2026-05-03",
                    1,
                    220,
                    cm1_profit=130,
                    cm2_profit=100,
                    cm3_profit=75,
                    allocated_paid_spend=30,
                    country="Czech Republic",
                ),
            ]
        )

        result = exporter.analyze_roy_product_demand_analytics(
            df=pd.DataFrame(),
            orders_df=pd.DataFrame(),
            item_df=item_df,
        )

        country_rows = result["country_rows"]
        sk_row = country_rows.loc[country_rows["country"] == "sk"].iloc[0]
        cz_row = country_rows.loc[country_rows["country"] == "cz"].iloc[0]

        self.assertEqual("SK", sk_row["country_label"])
        self.assertEqual(2, int(sk_row["orders"]))
        self.assertEqual(420, float(sk_row["revenue"]))
        self.assertEqual(250, float(sk_row["gross_profit"]))
        self.assertEqual(45, float(sk_row["spend"]))
        self.assertEqual(160, float(sk_row["profit_with_fixed"]))
        self.assertEqual("P-SK-A", sk_row["top_products"][0]["sku"])
        self.assertEqual("CZ", cz_row["country_label"])
        self.assertEqual("P-CZ-A", cz_row["top_products"][0]["sku"])

    def test_inventory_cost_history_merges_previous_rows_by_snapshot_date(self) -> None:
        current_point = BizniWebExporter._build_roy_inventory_cost_history_point(
            {
                "inventory_status": "ok",
                "inventory_snapshot_date": "2026-06-18",
                "inventory_cost_value": 150.456,
                "inventory_retail_value": 300.123,
                "inventory_available_units": 12.4,
                "inventory_products_with_stock": 4,
                "inventory_products_total": 5,
                "inventory_cost_coverage_units_pct": 88.8,
                "inventory_cost_coverage_retail_pct": 91.2,
                "dead_stock_cost_value": 25.5,
                "dead_stock_count": 1,
                "stock_risk_critical_count": 2,
                "stock_risk_30d_count": 3,
                "stock_risk_45d_count": 4,
                "negative_stock_count": 0,
                "revenue_at_risk_30d": 42.42,
                "profit_at_risk_30d": 12.34,
            }
        )

        history_rows = BizniWebExporter._merge_roy_inventory_cost_history_rows(
            [
                {"date": "2026-06-17", "inventory_cost_value": 100.0, "inventory_retail_value": 180.0},
                {"date": "2026-06-18", "inventory_cost_value": 120.0, "inventory_retail_value": 220.0},
            ],
            current_point,
        )

        self.assertEqual(["2026-06-17", "2026-06-18"], [row["date"] for row in history_rows])
        self.assertEqual(150.46, history_rows[-1]["inventory_cost_value"])
        self.assertEqual(300.12, history_rows[-1]["inventory_retail_value"])
        self.assertEqual(25.5, history_rows[-1]["dead_stock_cost_value"])
        self.assertEqual(3, history_rows[-1]["stock_risk_30d_count"])

    def test_inventory_cost_history_seeds_from_previous_dashboard_summary(self) -> None:
        rows = BizniWebExporter._extract_roy_inventory_cost_history_rows_from_snapshot(
            {
                "dashboard": {
                    "roy_product_demand": {
                        "summary": {
                            "inventory_status": "ok",
                            "inventory_snapshot_date": "2026-06-17",
                            "inventory_cost_value": 100.0,
                            "inventory_retail_value": 180.0,
                            "dead_stock_cost_value": 15.0,
                        }
                    }
                }
            }
        )

        self.assertEqual(1, len(rows))
        self.assertEqual("2026-06-17", rows[0]["date"])
        self.assertEqual(100.0, rows[0]["inventory_cost_value"])
        self.assertEqual(15.0, rows[0]["dead_stock_cost_value"])


if __name__ == "__main__":
    unittest.main()
