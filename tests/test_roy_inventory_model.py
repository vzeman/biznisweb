import unittest

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
    allocated_paid_spend: float = 0.0,
    country: str = "SK",
) -> dict:
    return {
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
        self.assertGreater(float(alert_row["suggested_reorder_units"]), 0)

        summary = result["summary"]
        self.assertEqual(1, summary["history_only_inventory_products"])
        self.assertEqual(1, summary["historical_restock_relevant_products"])

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
                item_row("R-1", "P-WIN", "Wachman Profit Product", "2026-05-01", 2, 300, cm2_profit=120, cm3_profit=90),
                item_row("R-2", "P-LOSS", "Loss Product", "2026-05-02", 2, 120, cm2_profit=-15, cm3_profit=-25),
                item_row("R-3", "P-REV", "Revenue Product", "2026-05-03", 1, 500, cm2_profit=40, cm3_profit=30),
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
        self.assertLess(float(result["loss_product_rows"].iloc[0]["profit_without_fixed"]), 0)

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


if __name__ == "__main__":
    unittest.main()
