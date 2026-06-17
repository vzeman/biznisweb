import unittest
import csv
import json
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from export_orders import BizniWebExporter
from reporting_core.cfo_kpis import build_order_records_from_export_df
from scripts.apply_missing_product_costs import apply_missing_cost_rows, parse_purchase_cost, resolve_expense_key


def make_exporter(project_name: str = "vevo") -> BizniWebExporter:
    return BizniWebExporter(
        api_url="https://example.com/api/graphql",
        api_token="token",
        project_name=project_name,
        output_tag="unit",
        enable_period_bundle=False,
    )


def write_order_cache(exporter: BizniWebExporter, order_date: datetime, cached_at: datetime, orders=None) -> Path:
    cache_file = exporter.get_cache_filename(order_date)
    cache_file.write_text(
        json.dumps(
            {
                "date": order_date.strftime("%Y-%m-%d"),
                "cached_at": cached_at.isoformat(),
                "orders": list(orders or []),
            }
        ),
        encoding="utf-8",
    )
    return cache_file


def price_element(element_type: str, title: str, reference_id: str) -> dict:
    return {
        "type": element_type,
        "title": title,
        "reference_id": reference_id,
        "value": "",
        "price": {"value": 0, "formatted": "0 €", "is_net_price": False},
    }


def reporting_order(order_num: str, status_name: str, payment_title: str, payment_ref: str) -> dict:
    return {
        "id": order_num,
        "order_num": order_num,
        "pur_date": "2026-06-01 10:00:00",
        "status": {"name": status_name},
        "price_elements": [price_element("payment", payment_title, payment_ref)],
    }


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

    def test_product_expense_coverage_writes_missing_cost_fill_in_csv(self) -> None:
        exporter = make_exporter()
        with tempfile.TemporaryDirectory() as tmp_dir:
            exporter.data_dir = Path(tmp_dir)
            qa = exporter._build_product_expense_coverage_qa(
                pd.DataFrame(
                    [
                        {
                            "order_num": "A-1",
                            "item_label": "Unknown unit-test product",
                            "product_sku": "SKU-FALLBACK",
                            "item_ean": "8581234567890",
                            "item_import_code": "IMP-1",
                            "item_warehouse_number": "WH-1",
                            "item_quantity": 2,
                            "item_total_without_tax": 100.0,
                            "profit_before_ads": 0.0,
                            "expense_per_item": 50.0,
                            "expense_source": "missing_cost_zero_margin_fallback",
                        }
                    ]
                ),
                date_from=datetime(2026, 6, 1),
                date_to=datetime(2026, 6, 1),
            )

            csv_path = Path(qa["missing_product_costs_path"])
            self.assertTrue(csv_path.exists())
            self.assertEqual(1, qa["missing_product_cost_rows"])
            with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
                csv_rows = list(csv.DictReader(handle))

            self.assertEqual(1, len(csv_rows))
            self.assertEqual(
                "Unknown unit-test product||IMP-1",
                csv_rows[0]["suggested_expense_key"],
            )
            self.assertEqual("", csv_rows[0]["purchase_cost_net"])

    def test_apply_missing_product_cost_rows_is_conflict_safe(self) -> None:
        rows = [
            {
                "suggested_expense_key": "Product A||IMP-1",
                "purchase_cost_net": "12,50",
            },
            {
                "suggested_expense_key": "Product B||IMP-2",
                "purchase_cost_net": "",
            },
            {
                "item_label": "Product C",
                "item_import_code": "IMP-3",
                "purchase_cost_net": "7.25",
            },
        ]

        updated, summary = apply_missing_cost_rows(
            {"Product A||IMP-1": 11.0},
            rows,
            allow_overwrite=False,
        )

        self.assertEqual(1, summary["applied"])
        self.assertEqual(1, summary["skipped_empty_cost"])
        self.assertEqual(1, summary["skipped_existing_conflict"])
        self.assertEqual(11.0, updated["Product A||IMP-1"])
        self.assertEqual(7.25, updated["Product C||IMP-3"])
        self.assertEqual(12.5, parse_purchase_cost("12,50"))
        self.assertEqual(1234.5, parse_purchase_cost("1 234,50 €"))
        self.assertEqual("Product C||IMP-3", resolve_expense_key(rows[2]))

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

    def test_cache_policy_refreshes_recent_orders_even_when_cache_exists(self) -> None:
        exporter = make_exporter()
        today = datetime(2026, 5, 20)
        with tempfile.TemporaryDirectory() as tmp_dir:
            exporter.cache_dir = Path(tmp_dir)
            order_date = today - timedelta(days=11)
            write_order_cache(
                exporter,
                order_date,
                today,
                [{"order_num": "A-1", "status": {"name": "Caka na uhradu"}}],
            )

            self.assertFalse(exporter.should_use_cache(order_date, today=today))

    def test_cache_policy_revalidates_late_payment_windows(self) -> None:
        exporter = make_exporter()
        today = datetime(2026, 5, 20)
        with tempfile.TemporaryDirectory() as tmp_dir:
            exporter.cache_dir = Path(tmp_dir)
            order_date = today - timedelta(days=30)
            write_order_cache(exporter, order_date, today - timedelta(days=6))
            self.assertTrue(exporter.should_use_cache(order_date, today=today))

            write_order_cache(exporter, order_date, today - timedelta(days=7))
            self.assertFalse(exporter.should_use_cache(order_date, today=today))

    def test_cache_policy_revalidates_monthly_and_older_history(self) -> None:
        exporter = make_exporter()
        today = datetime(2026, 5, 20)
        with tempfile.TemporaryDirectory() as tmp_dir:
            exporter.cache_dir = Path(tmp_dir)

            monthly_date = today - timedelta(days=180)
            write_order_cache(exporter, monthly_date, today - timedelta(days=29))
            self.assertTrue(exporter.should_use_cache(monthly_date, today=today))

            write_order_cache(exporter, monthly_date, today - timedelta(days=30))
            self.assertFalse(exporter.should_use_cache(monthly_date, today=today))

            old_date = today - timedelta(days=500)
            write_order_cache(exporter, old_date, today - timedelta(days=89))
            self.assertTrue(exporter.should_use_cache(old_date, today=today))

            write_order_cache(exporter, old_date, today - timedelta(days=90))
            self.assertFalse(exporter.should_use_cache(old_date, today=today))

    def test_realized_revenue_filter_counts_paid_cod_and_shipped_prepaid_orders(self) -> None:
        exporter = make_exporter()
        orders = [
            reporting_order("COD-WAIT", "Čaká na vybavenie", "Dobierkou", "7"),
            reporting_order("COD-SHIPPED", "Odoslaná", "Dobírka", "10"),
            reporting_order("PAID-CARD", "Platba online - zaplatené", "Okamžitá platba online", "18"),
            reporting_order("PAID-BANK", "Platba online - zaplatené", "Bankovým prevodom", "6"),
            reporting_order("BANK-WAIT", "Čaká na vybavenie", "Bankovým prevodom", "6"),
            reporting_order("CARD-WAIT", "Čaká na vybavenie", "Okamžitá platba online", "18"),
            reporting_order("BANK-SHIPPED", "Odoslaná", "Bankovým prevodom", "6"),
            reporting_order("CANCELLED", "Nezaplatená - zrušená objednávka", "Bankovým prevodom", "6"),
        ]

        filtered = exporter._filter_by_status(orders, track_excluded=False)

        self.assertEqual(
            ["COD-WAIT", "COD-SHIPPED", "PAID-CARD", "PAID-BANK", "BANK-SHIPPED"],
            [order["order_num"] for order in filtered],
        )

    def test_shipped_online_card_counts_as_prepaid_fulfilled_revenue(self) -> None:
        exporter = make_exporter()
        order = reporting_order("CARD-SHIPPED", "Odoslana", "Okamzita platba online", "18")

        self.assertEqual(
            (True, "prepaid_fulfilled_status"),
            exporter._realized_revenue_decision(order),
        )

    def test_roy_realized_revenue_counts_hungarian_cod(self) -> None:
        exporter = make_exporter("roy")
        order = reporting_order("HU-COD", "Čaká na vybavenie", "Utánvétes fizetés", "16")

        self.assertEqual(
            (True, "cod_status_and_payment"),
            exporter._realized_revenue_decision(order),
        )

    def test_roy_realized_revenue_counts_multilingual_cod_titles_without_known_id(self) -> None:
        exporter = make_exporter("roy")
        payment_titles = [
            "Cash on delivery",
            "Płatność przy odbiorze",
            "Zahlung per Nachnahme",
            "Paiement à la livraison",
            "Pago contra reembolso",
            "Plata ramburs",
        ]

        for index, payment_title in enumerate(payment_titles, start=1):
            with self.subTest(payment_title=payment_title):
                order = reporting_order(
                    f"FOREIGN-COD-{index}",
                    "Čaká na vybavenie",
                    payment_title,
                    str(900 + index),
                )
                self.assertEqual(
                    (True, "cod_status_and_payment"),
                    exporter._realized_revenue_decision(order),
                )

    def test_cod_status_without_payment_metadata_is_not_counted(self) -> None:
        exporter = make_exporter()

        missing_metadata = {
            "order_num": "COD-MISSING",
            "status": {"name": "Odoslana"},
        }
        paid_without_metadata = {
            "order_num": "PAID-MISSING",
            "status": {"name": "Platba online - zaplatene"},
        }

        self.assertEqual(
            (False, "fulfilled_status_missing_payment_metadata"),
            exporter._realized_revenue_decision(missing_metadata),
        )
        self.assertEqual(
            (True, "paid_status"),
            exporter._realized_revenue_decision(paid_without_metadata),
        )

    def test_price_elements_page_failure_falls_back_and_enriches_cod_orders(self) -> None:
        exporter = make_exporter()

        class FallbackClient:
            def __init__(self) -> None:
                self.calls = []

            def execute(self, query, variable_values=None):
                self.calls.append((query, variable_values))
                if len(self.calls) == 1:
                    raise Exception("{'path': ['getOrderList', 'data', 5, 'price_elements']}")
                if len(self.calls) == 2:
                    return {
                        "getOrderList": {
                            "data": [
                                {
                                    "id": "COD-FALLBACK",
                                    "order_num": "COD-FALLBACK",
                                    "pur_date": "2026-06-01 09:00:00",
                                    "status": {"name": "Odoslana"},
                                }
                            ],
                            "pageInfo": {
                                "hasNextPage": False,
                                "nextCursor": None,
                            },
                        }
                    }
                return {
                    "getOrder": {
                        "order_num": "COD-FALLBACK",
                        "price_elements": [price_element("payment", "Dobierkou", "7")],
                    }
                }

        exporter.client = FallbackClient()

        orders, next_cursor = exporter.fetch_all_orders_bulk(max_orders=30)
        filtered = exporter._filter_by_status(orders, track_excluded=False)

        self.assertIsNone(next_cursor)
        self.assertEqual(["COD-FALLBACK"], [order["order_num"] for order in filtered])
        self.assertEqual("Dobierkou", exporter._price_element_info(orders[0], "payment")["title"])
        self.assertEqual(3, len(exporter.client.calls))

    def test_flatten_order_exports_payment_audit_fields(self) -> None:
        exporter = make_exporter()
        order = {
            "id": "PAID-BANK",
            "order_num": "PAID-BANK",
            "pur_date": "2026-06-01 10:00:00",
            "status": {"name": "Platba online - zaplatené"},
            "price_elements": [price_element("payment", "Bankovým prevodom", "6")],
            "sum": {"value": 123.0, "currency": {"code": "EUR"}},
            "customer": {"email": "a@example.com"},
            "items": [
                {
                    "item_label": "Unknown unit-test product",
                    "ean": "",
                    "quantity": 1,
                    "tax_rate": 23,
                    "price": {"value": 100.0, "currency": {"code": "EUR"}},
                    "sum": {"value": 100.0, "currency": {"code": "EUR"}},
                    "sum_with_tax": {"value": 123.0, "currency": {"code": "EUR"}},
                }
            ],
        }

        rows = exporter.flatten_order(order)

        self.assertEqual("Bankovým prevodom", rows[0]["payment_title"])
        self.assertEqual("6", rows[0]["payment_reference_id"])
        self.assertTrue(rows[0]["realized_revenue"])
        self.assertEqual("paid_status", rows[0]["realized_revenue_reason"])

    def test_old_order_cache_without_current_schema_is_revalidated(self) -> None:
        exporter = make_exporter()
        today = datetime(2026, 5, 20)
        with tempfile.TemporaryDirectory() as tmp_dir:
            exporter.cache_dir = Path(tmp_dir)
            order_date = today - timedelta(days=30)
            write_order_cache(
                exporter,
                order_date,
                today,
                [reporting_order("LEGACY", "Odoslaná", "Dobierkou", "7")],
            )

            self.assertIsNone(exporter.load_from_cache(order_date))


if __name__ == "__main__":
    unittest.main()
