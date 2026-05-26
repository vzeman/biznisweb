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
) -> dict:
    return {
        "order_num": order_num,
        "product_sku": product_sku,
        "item_label": item_label,
        "purchase_datetime": purchase_datetime,
        "item_quantity": quantity,
        "item_total_without_tax": revenue,
        "cm2_profit": revenue * 0.4,
        "cm3_profit": revenue * 0.3,
    }


class RoyInventoryModelTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
