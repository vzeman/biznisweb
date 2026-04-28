import unittest
from datetime import datetime

import pandas as pd

from export_orders import BizniWebExporter
from reporting_core.cfo_kpis import build_order_records_from_export_df


def make_exporter() -> BizniWebExporter:
    return BizniWebExporter(
        api_url="https://example.com/api/graphql",
        api_token="token",
        project_name="vevo",
        output_tag="unit",
        enable_period_bundle=False,
    )


class ReportingCalculationFixTests(unittest.TestCase):
    def test_unknown_currency_is_not_treated_as_eur(self) -> None:
        exporter = make_exporter()
        with self.assertRaises(ValueError):
            exporter.convert_to_eur(10.0, "BTC")

    def test_missing_product_cost_uses_zero_margin_fallback(self) -> None:
        exporter = make_exporter()
        rows = exporter.flatten_order(
            {
                "id": "1",
                "order_num": "A-1",
                "pur_date": "2026-04-20 10:00:00",
                "sum": {"value": 120.0, "currency": {"code": "EUR"}},
                "customer": {"email": "a@example.com"},
                "items": [
                    {
                        "item_label": "Unknown unit-test product",
                        "ean": "",
                        "quantity": 2,
                        "tax_rate": 20,
                        "price": {"value": 50.0, "currency": {"code": "EUR"}},
                        "sum": {"value": 100.0, "currency": {"code": "EUR"}},
                        "sum_with_tax": {"value": 120.0, "currency": {"code": "EUR"}},
                    }
                ],
            }
        )

        self.assertEqual("missing_cost_zero_margin_fallback", rows[0]["expense_source"])
        self.assertEqual(50.0, rows[0]["expense_per_item"])
        self.assertEqual(100.0, rows[0]["total_expense"])
        self.assertEqual(0.0, rows[0]["profit_before_ads"])

    def test_zero_revenue_order_allocates_item_level_overhead(self) -> None:
        exporter = make_exporter()
        df = pd.DataFrame(
            [
                {
                    "order_num": "Z-1",
                    "customer_email": "z@example.com",
                    "purchase_date": "2026-04-20 10:00:00",
                    "order_revenue_net": 0.0,
                    "total_items_in_order": 2,
                    "fb_ads_daily_spend": 10.0,
                    "google_ads_daily_spend": 5.0,
                    "product_sku": "SKU-A",
                    "item_label": "Zero A",
                    "item_quantity": 1,
                    "item_total_without_tax": 0.0,
                    "item_total_with_tax": 0.0,
                    "item_unit_price": 0.0,
                    "item_line_sum_original": 0.0,
                    "item_line_sum_with_tax_original": 0.0,
                    "item_unit_price_original": 0.0,
                    "total_expense": 30.0,
                },
                {
                    "order_num": "Z-1",
                    "customer_email": "z@example.com",
                    "purchase_date": "2026-04-20 10:00:00",
                    "order_revenue_net": 0.0,
                    "total_items_in_order": 2,
                    "fb_ads_daily_spend": 10.0,
                    "google_ads_daily_spend": 5.0,
                    "product_sku": "SKU-B",
                    "item_label": "Zero B",
                    "item_quantity": 1,
                    "item_total_without_tax": 0.0,
                    "item_total_with_tax": 0.0,
                    "item_unit_price": 0.0,
                    "item_line_sum_original": 0.0,
                    "item_line_sum_with_tax_original": 0.0,
                    "item_unit_price_original": 0.0,
                    "total_expense": 10.0,
                },
            ]
        )

        orders_df, item_df, _ = exporter._build_growth_order_item_frames(df)

        self.assertAlmostEqual(1.0, float(item_df["item_rev_share"].sum()), places=6)
        self.assertAlmostEqual(
            float(orders_df["allocated_paid_spend"].sum()),
            float(item_df["allocated_paid_spend"].sum()),
            places=6,
        )
        self.assertAlmostEqual(
            float(orders_df["allocated_fixed_overhead"].sum()),
            float(item_df["allocated_fixed_overhead"].sum()),
            places=6,
        )

    def test_period_customer_history_marks_prior_customer_returning(self) -> None:
        exporter = make_exporter()
        full_orders = [
            {
                "order_num": "A-1",
                "pur_date": "2026-03-01 08:00:00",
                "customer": {"email": "a@example.com"},
            },
            {
                "order_num": "A-2",
                "pur_date": "2026-04-20 08:00:00",
                "customer": {"email": "a@example.com"},
            },
        ]
        period_df = pd.DataFrame(
            [
                {
                    "order_num": "A-2",
                    "customer_email": "a@example.com",
                    "purchase_date": "2026-04-20 08:00:00",
                }
            ]
        )

        first_map = exporter._build_customer_first_purchase_map(full_orders)
        enriched = exporter._add_customer_history_columns(period_df, first_map)
        orders_df = enriched.copy()
        orders_df["purchase_datetime"] = pd.to_datetime(orders_df["purchase_date"])
        orders_df = exporter._attach_customer_history_flags(orders_df)

        self.assertEqual(datetime(2026, 3, 1, 8, 0, 0), first_map["a@example.com"])
        self.assertTrue(bool(orders_df.iloc[0]["is_returning"]))
        self.assertFalse(bool(orders_df.iloc[0]["is_customer_first_order"]))

        cfo_records = build_order_records_from_export_df(enriched)
        self.assertEqual(datetime(2026, 3, 1).date(), cfo_records[0]["first_date"])

    def test_geo_profitability_includes_google_ads(self) -> None:
        exporter = make_exporter()
        df = pd.DataFrame(
            [
                {
                    "order_num": "G-1",
                    "customer_email": "g@example.com",
                    "purchase_date": "2026-04-20 10:00:00",
                    "order_revenue_net": 100.0,
                    "total_items_in_order": 1,
                    "fb_ads_daily_spend": 0.0,
                    "google_ads_daily_spend": 9.0,
                    "product_sku": "SKU-G",
                    "item_label": "Google geo",
                    "item_quantity": 1,
                    "item_total_without_tax": 100.0,
                    "item_total_with_tax": 120.0,
                    "item_unit_price": 100.0,
                    "item_line_sum_original": 100.0,
                    "item_line_sum_with_tax_original": 120.0,
                    "item_unit_price_original": 100.0,
                    "total_expense": 40.0,
                    "delivery_country": "sk",
                    "invoice_country": "sk",
                    "delivery_city": "Bratislava",
                    "invoice_city": "Bratislava",
                }
            ]
        )

        result = exporter.analyze_geo_profitability(df, fb_campaigns=[])
        geo = result["table"]

        self.assertEqual(9.0, float(geo.loc[0, "google_ads_spend"]))
        self.assertEqual(9.0, float(geo.loc[0, "paid_ads_spend"]))


if __name__ == "__main__":
    unittest.main()
