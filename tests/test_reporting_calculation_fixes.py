import unittest
import json
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from export_orders import ORDER_CACHE_SCHEMA_VERSION, BizniWebExporter
from html_report_generator import generate_html_report
from reporting_core.cfo_kpis import build_order_records_from_export_df
from reporting_core.runtime import load_project_runtime


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


def analytics_item_row(
    order_num: str,
    customer_email: str,
    purchase_date: str,
    *,
    revenue: float = 100.0,
    fb_spend: float = 0.0,
    google_spend: float = 0.0,
) -> dict:
    return {
        "order_num": order_num,
        "customer_email": customer_email,
        "purchase_date": purchase_date,
        "customer_first_purchase_date": purchase_date,
        "order_total": revenue,
        "total_items_in_order": 1,
        "fb_ads_daily_spend": fb_spend,
        "google_ads_daily_spend": google_spend,
        "total_expense": 40.0,
        "product_sku": "TEST-SKU",
        "item_label": "Test product",
        "item_quantity": 1,
        "item_total_without_tax": revenue,
        "item_total_with_tax": revenue,
        "item_unit_price": revenue,
        "item_line_sum_original": revenue,
        "item_line_sum_with_tax_original": revenue,
        "item_unit_price_original": revenue,
    }


class ReportingCalculationFixTests(unittest.TestCase):
    def test_unknown_currency_is_not_treated_as_eur(self) -> None:
        exporter = make_exporter()
        with self.assertRaises(ValueError):
            exporter.convert_to_eur(10.0, "BTC")

    def test_vevo_missing_product_cost_uses_configured_35_percent_margin(self) -> None:
        exporter = make_exporter()
        settings_path = Path(__file__).resolve().parents[1] / "projects" / "vevo" / "settings.json"
        settings = json.loads(settings_path.read_text(encoding="utf-8"))
        runtime = load_project_runtime(
            "vevo",
            settings=settings,
            default_packaging_cost_per_order=0.0,
            default_shipping_subsidy_per_order=0.0,
            default_fixed_monthly_cost=0.0,
            default_fixed_daily_cost=0.0,
        )
        self.assertEqual(35.0, runtime.missing_cost_margin_pct)

        with patch("export_orders.MISSING_COST_MARGIN_PCT", runtime.missing_cost_margin_pct):
            rows = exporter.flatten_order({
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
            })

        self.assertEqual("missing_cost_margin_35_fallback", rows[0]["expense_source"])
        self.assertEqual(32.5, rows[0]["expense_per_item"])
        self.assertEqual(65.0, rows[0]["total_expense"])
        self.assertEqual(35.0, rows[0]["profit_before_ads"])

    def test_roy_missing_product_cost_uses_configured_35_percent_margin(self) -> None:
        exporter = make_exporter(project_name="roy")

        with patch("export_orders.MISSING_COST_MARGIN_PCT", 35.0):
            rows = exporter.flatten_order(
                {
                    "id": "1",
                    "order_num": "R-MISSING",
                    "pur_date": "2026-07-14 10:00:00",
                    "sum": {"value": 123.0, "currency": {"code": "EUR"}},
                    "customer": {"email": "a@example.com"},
                    "items": [
                        {
                            "item_label": "Unknown ROY product",
                            "ean": "",
                            "quantity": 2,
                            "tax_rate": 23,
                            "price": {"value": 50.0, "currency": {"code": "EUR"}},
                            "sum": {"value": 100.0, "currency": {"code": "EUR"}},
                            "sum_with_tax": {"value": 123.0, "currency": {"code": "EUR"}},
                        }
                    ],
                }
            )

        self.assertEqual("missing_cost_margin_35_fallback", rows[0]["expense_source"])
        self.assertEqual(32.5, rows[0]["expense_per_item"])
        self.assertEqual(65.0, rows[0]["total_expense"])
        self.assertEqual(35.0, rows[0]["profit_before_ads"])

    def test_product_expense_qa_describes_configured_missing_cost_margin(self) -> None:
        exporter = make_exporter(project_name="roy")
        frame = pd.DataFrame(
            {
                "order_num": ["R-MISSING"],
                "item_label": ["Unknown ROY product"],
                "product_sku": ["UNKNOWN"],
                "item_quantity": [1],
                "item_total_without_tax": [100.0],
                "profit_before_ads": [35.0],
                "expense_per_item": [65.0],
                "expense_source": ["missing_cost_margin_35_fallback"],
            }
        )

        with patch("export_orders.MISSING_COST_MARGIN_PCT", 35.0):
            qa = exporter._build_product_expense_coverage_qa(
                frame,
                report_date_from="2026-06-01",
                report_date_to="2026-07-14",
            )

        self.assertEqual(
            "missing costs use a configured 35% margin estimate (65% of net item revenue is treated as expense)",
            qa["fallback_policy"],
        )

    def test_product_expense_qa_keeps_complete_missing_cost_product_list(self) -> None:
        exporter = make_exporter(project_name="vevo")
        frame = pd.DataFrame(
            [
                {
                    "order_num": f"V-{index}",
                    "item_label": f"Missing product {index}",
                    "product_sku": f"MISSING-{index}",
                    "item_quantity": 1,
                    "item_total_without_tax": float(10 * index),
                    "profit_before_ads": float(3.5 * index),
                    "expense_per_item": float(6.5 * index),
                    "expense_source": "missing_cost_margin_35_fallback",
                }
                for index in range(1, 7)
            ]
        )

        with patch("export_orders.MISSING_COST_MARGIN_PCT", 35.0):
            qa = exporter._build_product_expense_coverage_qa(frame)

        self.assertEqual(6, qa["fallback_product_count"])
        self.assertEqual(6, len(qa["fallback_items"]))
        self.assertEqual(5, len(qa["top_fallback_items"]))
        self.assertEqual("MISSING-6", qa["fallback_items"][0]["product_sku"])
        self.assertAlmostEqual(28.5714, qa["fallback_items"][0]["total_revenue_share_pct"], places=4)

    def test_product_expense_qa_lists_unmapped_zero_revenue_roy_gifts_separately(self) -> None:
        exporter = make_exporter(project_name="roy")
        frame = pd.DataFrame(
            [
                {
                    "order_num": "R-GIFT-MISSING",
                    "purchase_date": "2026-07-14",
                    "item_label": "Unmapped free knife",
                    "product_sku": "GIFT-MISSING",
                    "item_quantity": 2,
                    "item_total_without_tax": 0.0,
                    "profit_before_ads": 0.0,
                    "expense_per_item": 0.0,
                    "expense_source": "zero_revenue_gift_missing_cost",
                }
            ]
        )

        qa = exporter._build_product_expense_coverage_qa(
            frame,
            report_date_to="2026-07-14",
        )

        self.assertEqual(0, qa["fallback_rows"])
        self.assertEqual(1, qa["missing_cost_product_entry_count"])
        self.assertEqual(1, qa["zero_revenue_gift_missing_cost_rows"])
        self.assertEqual(2.0, qa["zero_revenue_gift_missing_cost_units"])
        self.assertEqual(1, qa["zero_revenue_gift_missing_cost_product_count"])
        self.assertEqual("zero_revenue_gift", qa["missing_cost_items"][0]["category"])
        self.assertEqual(0.0, qa["missing_cost_items"][0]["revenue"])
        self.assertEqual(0.0, qa["missing_cost_items"][0]["profit_before_ads"])

    def test_product_expense_qa_flags_recent_missing_cost_revenue_concentration(self) -> None:
        exporter = make_exporter(project_name="vevo")
        frame = pd.DataFrame(
            [
                {
                    "order_num": "V-MISSING",
                    "purchase_date": "2026-07-14",
                    "item_label": "Missing recent product",
                    "product_sku": "MISSING-RECENT",
                    "item_quantity": 1,
                    "item_total_without_tax": 4.0,
                    "profit_before_ads": 1.4,
                    "expense_per_item": 2.6,
                    "expense_source": "missing_cost_margin_35_fallback",
                },
                {
                    "order_num": "V-MAPPED",
                    "purchase_date": "2026-07-14",
                    "item_label": "Mapped recent product",
                    "product_sku": "MAPPED-RECENT",
                    "item_quantity": 1,
                    "item_total_without_tax": 96.0,
                    "profit_before_ads": 40.0,
                    "expense_per_item": 56.0,
                    "expense_source": "mapped_product_identifier",
                },
            ]
        )

        with patch("export_orders.MISSING_COST_MARGIN_PCT", 35.0):
            qa = exporter._build_product_expense_coverage_qa(
                frame,
                report_date_from="2026-06-01",
                report_date_to="2026-07-14",
            )

        self.assertEqual("2026-06-15", qa["fallback_recent_30d_date_from"])
        self.assertEqual("2026-07-14", qa["fallback_recent_30d_date_to"])
        self.assertEqual(4.0, qa["fallback_recent_30d_revenue"])
        self.assertEqual(4.0, qa["fallback_recent_30d_revenue_share_pct"])
        self.assertTrue(any("Recent 30-day missing-cost rows" in warning for warning in qa["warnings"]))

    def test_product_expense_qa_recent_window_uses_report_end_not_last_sale(self) -> None:
        exporter = make_exporter(project_name="vevo")
        frame = pd.DataFrame(
            [
                {
                    "order_num": "V-OLD-MISSING",
                    "purchase_date": "2026-06-01",
                    "item_label": "Old missing-cost product",
                    "product_sku": "MISSING-OLD",
                    "item_quantity": 1,
                    "item_total_without_tax": 100.0,
                    "profit_before_ads": 35.0,
                    "expense_per_item": 65.0,
                    "expense_source": "missing_cost_margin_35_fallback",
                }
            ]
        )

        qa = exporter._build_product_expense_coverage_qa(
            frame,
            report_date_to="2026-07-14",
        )

        self.assertEqual("2026-06-15", qa["fallback_recent_30d_date_from"])
        self.assertEqual("2026-07-14", qa["fallback_recent_30d_date_to"])
        self.assertEqual(0.0, qa["fallback_recent_30d_total_revenue"])
        self.assertEqual(0.0, qa["fallback_recent_30d_revenue"])
        self.assertEqual(0.0, qa["fallback_recent_30d_revenue_share_pct"])

    def test_roy_mapped_cost_keeps_real_loss_despite_missing_cost_margin(self) -> None:
        exporter = make_exporter(project_name="roy")
        exporter.product_expenses_exact["LOSS-SKU"] = 80.0

        with patch("export_orders.MISSING_COST_MARGIN_PCT", 35.0), patch(
            "export_orders.MARGIN_OVERRIDE_SKUS", {}
        ), patch("export_orders.MARGIN_OVERRIDE_BRANDS", {}), patch(
            "export_orders.MARGIN_OVERRIDE_LABEL_PATTERNS", {}
        ):
            rows = exporter.flatten_order(
                {
                    "id": "1",
                    "order_num": "R-LOSS",
                    "pur_date": "2026-07-14 10:00:00",
                    "sum": {"value": 61.5, "currency": {"code": "EUR"}},
                    "customer": {"email": "a@example.com"},
                    "items": [
                        {
                            "item_label": "Intentional clearance loss",
                            "ean": "LOSS-SKU",
                            "quantity": 1,
                            "tax_rate": 23,
                            "price": {"value": 50.0, "currency": {"code": "EUR"}},
                            "sum": {"value": 50.0, "currency": {"code": "EUR"}},
                            "sum_with_tax": {"value": 61.5, "currency": {"code": "EUR"}},
                        }
                    ],
                }
            )

        self.assertEqual("mapped_product_identifier", rows[0]["expense_source"])
        self.assertEqual(80.0, rows[0]["expense_per_item"])
        self.assertEqual(80.0, rows[0]["total_expense"])
        self.assertEqual(-30.0, rows[0]["profit_before_ads"])

    def test_margin_override_brand_forces_configured_product_margin(self) -> None:
        exporter = make_exporter(project_name="roy")

        with patch("export_orders.MARGIN_OVERRIDE_SKUS", {}), patch(
            "export_orders.MARGIN_OVERRIDE_BRANDS", {"Ganzo": 35.0}
        ), patch(
            "export_orders.MARGIN_OVERRIDE_LABEL_PATTERNS", {}
        ):
            rows = exporter.flatten_order(
                {
                    "id": "1",
                    "order_num": "R-1",
                    "pur_date": "2026-04-20 10:00:00",
                    "sum": {"value": 123.0, "currency": {"code": "EUR"}},
                    "customer": {"email": "a@example.com"},
                    "items": [
                        {
                            "item_label": "Noz Ganzo G7211-BK",
                            "ean": "",
                            "quantity": 1,
                            "tax_rate": 23,
                            "price": {"value": 100.0, "currency": {"code": "EUR"}},
                            "sum": {"value": 100.0, "currency": {"code": "EUR"}},
                            "sum_with_tax": {"value": 123.0, "currency": {"code": "EUR"}},
                        }
                    ],
                }
            )

        self.assertEqual("margin_35_override", rows[0]["expense_source"])
        self.assertEqual(65.0, rows[0]["expense_per_item"])
        self.assertEqual(65.0, rows[0]["total_expense"])
        self.assertEqual(35.0, rows[0]["profit_before_ads"])

    def test_margin_override_sku_uses_net_line_total(self) -> None:
        exporter = make_exporter(project_name="roy")

        with patch("export_orders.MARGIN_OVERRIDE_SKUS", {"12837": 35.0}), patch(
            "export_orders.MARGIN_OVERRIDE_BRANDS", {}
        ), patch("export_orders.MARGIN_OVERRIDE_LABEL_PATTERNS", {}):
            rows = exporter.flatten_order(
                {
                    "id": "1",
                    "order_num": "R-1",
                    "pur_date": "2026-04-20 10:00:00",
                    "sum": {"value": 123.0, "currency": {"code": "EUR"}},
                    "customer": {"email": "a@example.com"},
                    "items": [
                        {
                            "item_label": "Puzdro na spreje 300ml univerzal",
                            "ean": "",
                            "import_code": "12837",
                            "quantity": 1,
                            "tax_rate": 23,
                            "price": {"value": 100.0, "currency": {"code": "EUR"}},
                            "sum": {"value": 100.0, "currency": {"code": "EUR"}},
                            "sum_with_tax": {"value": 123.0, "currency": {"code": "EUR"}},
                        }
                    ],
                }
            )

        self.assertEqual("margin_35_override", rows[0]["expense_source"])
        self.assertEqual(65.0, rows[0]["expense_per_item"])
        self.assertEqual(65.0, rows[0]["total_expense"])
        self.assertEqual(35.0, rows[0]["profit_before_ads"])

    def test_mapped_purchase_cost_wins_over_legacy_overrides(self) -> None:
        exporter = make_exporter(project_name="roy")
        exporter.product_expenses_exact.update(
            {
                "ZERO-SKU": 18.0,
                "MARGIN-SKU": 42.0,
                "MARGIN15-SKU": 73.0,
            }
        )
        base_item = {
            "quantity": 1,
            "tax_rate": 23,
            "price": {"value": 100.0, "currency": {"code": "EUR"}},
            "sum": {"value": 100.0, "currency": {"code": "EUR"}},
            "sum_with_tax": {"value": 123.0, "currency": {"code": "EUR"}},
        }

        with patch("export_orders.ZERO_COST_LABEL_PATTERNS", ["Zero mapped"]), patch(
            "export_orders.MARGIN_OVERRIDE_SKUS", {"MARGIN-SKU": 35.0}
        ), patch("export_orders.MARGIN_15_LABEL_PATTERNS", ["Margin 15 mapped"]):
            rows = exporter.flatten_order(
                {
                    "id": "1",
                    "order_num": "R-MAPPED-PRECEDENCE",
                    "pur_date": "2026-07-14 10:00:00",
                    "sum": {"value": 369.0, "currency": {"code": "EUR"}},
                    "customer": {"email": "a@example.com"},
                    "items": [
                        {**base_item, "item_label": "Zero mapped", "ean": "ZERO-SKU"},
                        {**base_item, "item_label": "Margin mapped", "ean": "MARGIN-SKU"},
                        {**base_item, "item_label": "Margin 15 mapped", "ean": "MARGIN15-SKU"},
                    ],
                }
            )

        self.assertEqual([18.0, 42.0, 73.0], [row["expense_per_item"] for row in rows])
        self.assertTrue(all(row["expense_source"] == "mapped_product_identifier" for row in rows))

    def test_roy_64gb_sd_card_alias_uses_canonical_cost_without_merging_32gb(self) -> None:
        exporter = make_exporter(project_name="roy")
        exporter.product_expenses_exact.update(
            {
                "MICRO-SD-64GB": 3.3,
                "F_206": 1.8,
            }
        )

        rows = exporter.flatten_order(
            {
                "id": "1",
                "order_num": "R-SD-CARD-IDENTITY",
                "pur_date": "2026-07-14 10:00:00",
                "sum": {"value": 34.44, "currency": {"code": "EUR"}},
                "customer": {"email": "a@example.com"},
                "items": [
                    {
                        "item_label": "Micro SD CARD 64GB s adaptérem",
                        "ean": "",
                        "quantity": 2,
                        "tax_rate": 23,
                        "price": {"value": 10.0, "currency": {"code": "EUR"}},
                        "sum": {"value": 20.0, "currency": {"code": "EUR"}},
                        "sum_with_tax": {"value": 24.6, "currency": {"code": "EUR"}},
                    },
                    {
                        "item_label": "Micro SD CARD 32GB s adaptérem",
                        "ean": "23942440833",
                        "import_code": "F_206",
                        "quantity": 1,
                        "tax_rate": 23,
                        "price": {"value": 8.0, "currency": {"code": "EUR"}},
                        "sum": {"value": 8.0, "currency": {"code": "EUR"}},
                        "sum_with_tax": {"value": 9.84, "currency": {"code": "EUR"}},
                    },
                ],
            }
        )

        self.assertEqual(["MICRO-SD-64GB", "F_206"], [row["product_sku"] for row in rows])
        self.assertEqual(["H-69235D5B", "F_206"], [row["raw_product_sku"] for row in rows])
        self.assertEqual([3.3, 1.8], [row["expense_per_item"] for row in rows])
        self.assertEqual(
            ["mapped_product_sku", "mapped_product_identifier"],
            [row["expense_source"] for row in rows],
        )
        self.assertEqual([6.6, 1.8], [row["total_expense"] for row in rows])

    def test_zero_revenue_gift_is_the_only_mapped_cost_exception(self) -> None:
        exporter = make_exporter(project_name="roy")
        exporter.product_expenses_exact["GIFT-KNIFE"] = 80.0

        rows = exporter.flatten_order(
            {
                "id": "1",
                "order_num": "R-ZERO-EUR-GIFT",
                "pur_date": "2026-07-14 10:00:00",
                "sum": {"value": 0.0, "currency": {"code": "EUR"}},
                "customer": {"email": "a@example.com"},
                "items": [
                    {
                        "item_label": "Sada nozov Roy 3-dielna Lux - darcek",
                        "ean": "GIFT-KNIFE",
                        "quantity": 1,
                        "tax_rate": 23,
                        "price": {"value": 0.0, "currency": {"code": "EUR"}},
                        "sum": {"value": 0.0, "currency": {"code": "EUR"}},
                        "sum_with_tax": {"value": 0.0, "currency": {"code": "EUR"}},
                    }
                ],
            }
        )

        self.assertEqual("zero_revenue_gift_mapped_cost", rows[0]["expense_source"])
        self.assertEqual(0.0, rows[0]["total_expense"])
        self.assertEqual(0.0, rows[0]["profit_before_ads"])
        self.assertEqual(80.0, rows[0]["purchase_cost_reference_per_item"])
        self.assertEqual("mapped_product_identifier", rows[0]["purchase_cost_reference_source"])

    def test_vevo_zero_revenue_rows_keep_known_purchase_costs(self) -> None:
        exporter = make_exporter(project_name="vevo")
        exporter.product_expenses_exact.update({"GIFT-A": 4.29, "GIFT-B": 0.31})

        rows = exporter.flatten_order(
            {
                "id": "1",
                "order_num": "V-ZERO-EUR-KNOWN-COSTS",
                "pur_date": "2026-07-14 10:00:00",
                "sum": {"value": 0.0, "currency": {"code": "EUR"}},
                "customer": {"email": "a@example.com"},
                "items": [
                    {
                        "item_label": "VEVO sample A",
                        "ean": "GIFT-A",
                        "quantity": 1,
                        "tax_rate": 20,
                        "price": {"value": 0.0, "currency": {"code": "EUR"}},
                        "sum": {"value": 0.0, "currency": {"code": "EUR"}},
                        "sum_with_tax": {"value": 0.0, "currency": {"code": "EUR"}},
                    },
                    {
                        "item_label": "VEVO sample B",
                        "ean": "GIFT-B",
                        "quantity": 1,
                        "tax_rate": 20,
                        "price": {"value": 0.0, "currency": {"code": "EUR"}},
                        "sum": {"value": 0.0, "currency": {"code": "EUR"}},
                        "sum_with_tax": {"value": 0.0, "currency": {"code": "EUR"}},
                    },
                ],
            }
        )

        self.assertEqual(2, len(rows))
        self.assertEqual(
            ["mapped_product_identifier", "mapped_product_identifier"],
            [row["expense_source"] for row in rows],
        )
        self.assertAlmostEqual(4.60, sum(row["total_expense"] for row in rows), places=2)
        self.assertAlmostEqual(-4.60, sum(row["profit_before_ads"] for row in rows), places=2)

    def test_roy_scissors_do_not_match_zero_revenue_knife_gift_exception(self) -> None:
        exporter = make_exporter(project_name="roy")
        exporter.product_expenses_exact["FREE-SCISSORS"] = 12.0

        rows = exporter.flatten_order(
            {
                "id": "1",
                "order_num": "R-ZERO-EUR-NON-KNIFE",
                "pur_date": "2026-07-14 10:00:00",
                "sum": {"value": 0.0, "currency": {"code": "EUR"}},
                "customer": {"email": "a@example.com"},
                "items": [
                    {
                        "item_label": "ROY nožnice 20 cm zdarma",
                        "ean": "FREE-SCISSORS",
                        "quantity": 1,
                        "tax_rate": 23,
                        "price": {"value": 0.0, "currency": {"code": "EUR"}},
                        "sum": {"value": 0.0, "currency": {"code": "EUR"}},
                        "sum_with_tax": {"value": 0.0, "currency": {"code": "EUR"}},
                    }
                ],
            }
        )

        self.assertEqual("mapped_product_identifier", rows[0]["expense_source"])
        self.assertEqual(12.0, rows[0]["total_expense"])
        self.assertEqual(-12.0, rows[0]["profit_before_ads"])

    def test_low_confidence_incrementality_requires_experiment(self) -> None:
        exporter = make_exporter(project_name="roy")

        decision_ready, blockers = exporter._incrementality_decision_gate(
            active_days=289,
            control_days=5,
            effective_pair_days=5,
            confidence="low",
        )
        verdict, reason, tone = exporter._build_incrementality_verdict(
            incremental_profit_without_fixed_per_day=50.0,
            incremental_profit_with_fixed_per_day=20.0,
            incremental_cac=10.0,
            break_even_cac=20.0,
            confidence="low",
            effective_pair_days=5,
            decision_ready=decision_ready,
            decision_blockers=blockers,
        )

        self.assertFalse(decision_ready)
        self.assertIn("control days 5/14", blockers)
        self.assertEqual("Experiment required", verdict)
        self.assertIn("Do not scale or cut", reason)
        self.assertEqual("warning", tone)

    def test_financial_paid_cac_uses_meta_and_google_spend(self) -> None:
        exporter = make_exporter(project_name="roy")
        item_df = pd.DataFrame(
            [
                {
                    "order_num": "R-1",
                    "customer_email": "a@example.com",
                    "purchase_date": "2026-07-01",
                    "order_total": 100.0,
                },
                {
                    "order_num": "R-2",
                    "customer_email": "b@example.com",
                    "purchase_date": "2026-07-02",
                    "order_total": 100.0,
                },
            ]
        )
        date_agg = pd.DataFrame(
            [
                {
                    "total_revenue": 200.0,
                    "unique_orders": 2,
                    "fb_ads_spend": 10.0,
                    "google_ads_spend": 20.0,
                    "product_expense": 100.0,
                    "packaging_cost": 0.0,
                    "shipping_net_cost": 0.0,
                    "fixed_daily_cost": 0.0,
                    "total_cost": 130.0,
                    "contribution_cost": 130.0,
                    "contribution_profit": 70.0,
                    "net_profit": 70.0,
                }
            ]
        )
        acquisition = pd.DataFrame([{"new_customers": 2, "avg_return_time_days": 30.0}])

        metrics = exporter.calculate_financial_metrics(item_df, date_agg, acquisition)

        self.assertEqual(5.0, metrics["current_fb_cac"])
        self.assertEqual(15.0, metrics["paid_cac"])
        self.assertEqual(15.0, metrics["blended_cac"])
        self.assertAlmostEqual(3.33, metrics["contribution_ltv_cac"], places=2)
        self.assertEqual(0.3, metrics["payback_orders"])

        qa = exporter._build_data_assertions_qa(
            financial_metrics=metrics,
            consistency_checks={},
            refunds_analysis={},
            day_of_week_analysis=pd.DataFrame(),
            advanced_dtc_metrics={},
            country_analysis=pd.DataFrame(),
            geo_profitability={},
            cost_per_order={},
        )
        self.assertEqual(0, qa["shell_parity_failures"])

    def test_weekly_cac_includes_blended_spend_on_days_without_orders(self) -> None:
        exporter = make_exporter(project_name="vevo")
        frame = pd.DataFrame(
            [
                analytics_item_row(
                    "V-1",
                    "new@example.com",
                    "2026-07-06 10:00:00",
                    fb_spend=3.0,
                    google_spend=2.0,
                )
            ]
        )
        fb_calendar = {
            "2026-07-06": 4.0,
            "2026-07-07": 6.0,  # no order
            "2026-07-13": 10.0,  # spend-only week
        }
        google_calendar = {
            "2026-07-06": 1.0,
            "2026-07-07": 2.0,  # no order
            "2026-07-13": 2.0,  # spend-only week
        }

        with tempfile.TemporaryDirectory() as tmp:
            exporter.data_dir = Path(tmp)
            weekly = exporter.calculate_clv_and_return_time(
                frame,
                fb_daily_spend=fb_calendar,
                google_ads_daily_spend=google_calendar,
            )

        self.assertEqual(2, len(weekly))
        self.assertEqual([10.0, 10.0], weekly["fb_ads_spend"].tolist())
        self.assertEqual([3.0, 2.0], weekly["google_ads_spend"].tolist())
        self.assertEqual([13.0, 12.0], weekly["paid_ads_spend"].tolist())
        self.assertEqual(13.0, weekly.iloc[0]["cac"])
        self.assertTrue(pd.isna(weekly.iloc[1]["cac"]))
        self.assertTrue(pd.isna(weekly.iloc[1]["ltv_cac_ratio"]))
        self.assertEqual(25.0, weekly.iloc[-1]["cumulative_avg_cac"])

        date_agg = pd.DataFrame(
            [
                {
                    "total_revenue": 100.0,
                    "fb_ads_spend": 20.0,
                    "google_ads_spend": 5.0,
                    "net_profit": 40.0,
                    "unique_orders": 1,
                }
            ]
        )
        checks = exporter.validate_metric_consistency(
            date_agg,
            {
                "roas": 4.0,
                "company_profit_margin_pct": 40.0,
                "paid_cac": 25.0,
            },
            weekly,
        )
        self.assertTrue(checks["cac_ok"])
        self.assertEqual("paid_ads_spend", checks["cac_spend_source"])
        self.assertEqual("paid_ads_spend / new_customers", checks["cac_formula"])

        metrics = exporter.calculate_financial_metrics(frame, date_agg, weekly)
        self.assertEqual(2, len(metrics["payback_weekly_orders"]))
        self.assertAlmostEqual(0.22, metrics["payback_weekly_orders"][0], places=2)
        self.assertIsNone(metrics["payback_weekly_orders"][1])

    def test_cumulative_cac_is_undefined_until_the_first_acquired_customer(self) -> None:
        exporter = make_exporter(project_name="vevo")
        frame = pd.DataFrame(
            [
                analytics_item_row(
                    "V-LATER",
                    "later@example.com",
                    "2026-07-13 10:00:00",
                    fb_spend=1.0,
                    google_spend=1.0,
                )
            ]
        )

        with tempfile.TemporaryDirectory() as tmp:
            exporter.data_dir = Path(tmp)
            weekly = exporter.calculate_clv_and_return_time(
                frame,
                fb_daily_spend={"2026-07-06": 10.0, "2026-07-13": 3.0},
                google_ads_daily_spend={"2026-07-06": 2.0, "2026-07-13": 1.0},
            )

        self.assertEqual(2, len(weekly))
        self.assertTrue(pd.isna(weekly.iloc[0]["cac"]))
        self.assertTrue(pd.isna(weekly.iloc[0]["cumulative_avg_cac"]))
        self.assertEqual(16.0, weekly.iloc[1]["cumulative_avg_cac"])

    def test_advanced_cohort_cac_uses_full_fb_google_calendars(self) -> None:
        exporter = make_exporter(project_name="vevo")
        frame = pd.DataFrame(
            [
                analytics_item_row(
                    "V-1",
                    "first@example.com",
                    "2026-07-01 10:00:00",
                    fb_spend=1.0,
                    google_spend=1.0,
                ),
                analytics_item_row(
                    "V-2",
                    "second@example.com",
                    "2026-07-10 10:00:00",
                    fb_spend=1.0,
                    google_spend=1.0,
                ),
            ]
        )
        fallback_calendar = exporter._build_daily_paid_spend_calendar(frame)
        self.assertEqual(2.0, fallback_calendar["fb_ads_daily_spend"].sum())
        self.assertEqual(2.0, fallback_calendar["google_ads_daily_spend"].sum())

        metrics = exporter.analyze_advanced_dtc_metrics(
            frame,
            fb_daily_spend={"2026-07-01": 10.0, "2026-07-05": 20.0},
            google_ads_daily_spend={"2026-07-02": 5.0, "2026-07-10": 5.0},
        )
        cohort = metrics["cohort_payback"].iloc[0]

        self.assertEqual(15.0, metrics["summary"]["paid_cac_fb"])
        self.assertEqual(20.0, metrics["summary"]["paid_cac"])
        self.assertEqual(30.0, cohort["cohort_fb_spend"])
        self.assertEqual(10.0, cohort["cohort_google_spend"])
        self.assertEqual(40.0, cohort["cohort_paid_spend"])
        self.assertEqual(20.0, cohort["cohort_cac"])

    def test_advanced_cac_is_undefined_when_paid_spend_has_no_new_customer(self) -> None:
        exporter = make_exporter(project_name="vevo")
        frame = pd.DataFrame(
            [
                analytics_item_row(
                    "V-RETURNING",
                    "returning@example.com",
                    "2026-07-15 10:00:00",
                    fb_spend=5.0,
                    google_spend=2.0,
                )
            ]
        )
        frame["customer_first_purchase_date"] = "2026-01-01 10:00:00"

        with tempfile.TemporaryDirectory() as tmp:
            exporter.data_dir = Path(tmp)
            metrics = exporter.analyze_advanced_dtc_metrics(
                frame,
                fb_daily_spend={"2026-07-15": 5.0},
                google_ads_daily_spend={"2026-07-15": 2.0},
            )

        self.assertIsNone(metrics["summary"]["paid_cac"])
        self.assertIsNone(metrics["summary"]["contribution_ltv_cac"])

    def test_legacy_report_renders_undefined_cac_as_na_and_null_chart_values(self) -> None:
        date_agg = pd.DataFrame(
            [
                {
                    "date": "2026-07-15",
                    "total_revenue": 0.0,
                    "product_expense": 0.0,
                    "fb_ads_spend": 10.0,
                    "google_ads_spend": 5.0,
                    "net_profit": -15.0,
                    "roi_percent": -100.0,
                    "unique_orders": 0,
                    "total_items": 0,
                    "total_cost": 15.0,
                    "packaging_cost": 0.0,
                    "shipping_net_cost": 0.0,
                    "fixed_daily_cost": 0.0,
                }
            ]
        )
        items_agg = pd.DataFrame(columns=["item_label", "total_revenue", "total_quantity", "profit"])
        clv_analysis = pd.DataFrame(
            [
                {
                    "week": "2026-W29",
                    "week_start": pd.Timestamp("2026-07-13"),
                    "unique_customers": 1,
                    "new_customers": 0,
                    "returning_customers": 1,
                    "avg_clv": 42.0,
                    "cumulative_avg_clv": 42.0,
                    "cac": float("nan"),
                    "avg_return_time_days": float("nan"),
                    "total_revenue": 42.0,
                    "paid_ads_spend": 15.0,
                }
            ]
        )

        html = generate_html_report(
            date_agg,
            pd.DataFrame(),
            items_agg,
            datetime(2026, 7, 15),
            datetime(2026, 7, 15),
            clv_return_time_analysis=clv_analysis,
            financial_metrics={
                "break_even_cac": 20.0,
                "paid_cac": None,
                "blended_cac": None,
                "contribution_ltv_cac": None,
            },
            consistency_checks={
                "roas_delta": 0.0,
                "company_margin_delta_pct": 0.0,
                "cac_expected": None,
                "cac_delta": None,
                "cac_if_orders_denominator": 0.0,
            },
            advanced_dtc_metrics={"summary": {"contribution_ltv_cac": None}},
            dashboard_variant="legacy",
        )

        self.assertIn("Contribution LTV/CAC", html)
        self.assertIn("data: [null, null, 20.00]", html)
        self.assertIn("N/A", html)
        cac_card = html[html.index("Customer Acq. Cost (FB + Google)"):][:250]
        revenue_ltv_cac_card = html[html.index("Revenue LTV/CAC"):][:250]
        self.assertIn("N/A", cac_card)
        self.assertNotIn("&#8364;0.00", cac_card)
        self.assertIn("N/A", revenue_ltv_cac_card)
        self.assertNotIn("0.00x", revenue_ltv_cac_card)

        modern_html = generate_html_report(
            date_agg,
            pd.DataFrame(),
            items_agg,
            datetime(2026, 7, 15),
            datetime(2026, 7, 15),
            financial_metrics={"paid_cac": None, "contribution_ltv_cac": None},
            consistency_checks={
                "roas_delta": 0.0,
                "company_margin_delta_pct": 0.0,
                "cac_delta": None,
            },
        )
        self.assertIn("CAC check delta</span></small><strong>N/A</strong>", modern_html)
        self.assertIn("value: nullableNumber(DATA.consistency.cac_delta)", modern_html)
        self.assertNotIn("value: Number(DATA.consistency.cac_delta || 0)", modern_html)

        exporter = make_exporter(project_name="vevo")
        with patch("builtins.print") as print_mock:
            exporter.display_clv_return_time_analysis(clv_analysis)
        total_lines = [
            str(call.args[0])
            for call in print_mock.call_args_list
            if call.args and "TOTAL" in str(call.args[0])
        ]
        self.assertTrue(total_lines)
        self.assertIn("N/A", total_lines[-1])

    def test_customer_concentration_includes_profit_shares(self) -> None:
        exporter = make_exporter(project_name="roy")

        def row(order_num: str, email: str, revenue: float, cost: float) -> dict:
            return {
                "order_num": order_num,
                "customer_email": email,
                "purchase_date": "2026-07-01",
                "order_total": revenue,
                "total_items_in_order": 1,
                "fb_ads_daily_spend": 0.0,
                "google_ads_daily_spend": 0.0,
                "total_expense": cost,
                "product_sku": order_num,
                "item_label": order_num,
                "item_quantity": 1,
                "item_total_without_tax": revenue,
                "item_total_with_tax": revenue,
                "item_unit_price": revenue,
                "item_line_sum_original": revenue,
                "item_line_sum_with_tax_original": revenue,
                "item_unit_price_original": revenue,
            }

        frame = pd.DataFrame(
            [
                row("R-1", "a@example.com", 100.0, 20.0),
                row("R-2", "b@example.com", 50.0, 20.0),
                row("R-3", "c@example.com", 25.0, 20.0),
            ]
        )
        with patch("export_orders.PACKAGING_COST_PER_ORDER", 0.0), patch(
            "export_orders.SHIPPING_NET_PER_ORDER", 0.0
        ), patch("export_orders.FIXED_MONTHLY_COST", 0.0), patch(
            "export_orders.FIXED_DAILY_COST", 0.0
        ):
            concentration = exporter.analyze_customer_concentration(frame)

        self.assertEqual(69.6, concentration["top_10_pct_profit_share"])
        self.assertEqual(69.6, concentration["top_20_pct_profit_share"])
        self.assertEqual(69.6, concentration["top_10_pct_contribution_share"])

    def test_customer_segments_use_realized_marker_not_one_mojibaked_status(self) -> None:
        exporter = make_exporter(project_name="roy")
        frame = pd.DataFrame(
            [
                {
                    "order_num": "R-1",
                    "customer_email": "a@example.com",
                    "customer_name": "A",
                    "purchase_date": "2026-06-01",
                    "status_name": "Elkuldve",
                    "realized_revenue": True,
                    "order_total": 100.0,
                    "item_label": "Test product",
                    "invoice_city": "Budapest",
                    "invoice_country": "HU",
                }
            ]
        )

        segments = exporter.analyze_customer_email_segments(frame)

        self.assertEqual(1, segments["one_time_buyers_30_days"]["count"])
        description = segments["high_value_one_time"]["description_en"].lower()
        self.assertNotIn("nan", description)
        self.assertIn("100.00", description)

    def test_roy_knife_brand_margin_overrides_cover_requested_brands(self) -> None:
        expected_brands = [
            "Opinel",
            "Morakniv",
            "Walther",
            "Kizlyar",
            "Higonokami",
            "Ganzo",
            "Ruike",
            "Helle",
            "Cold Steel",
            "Civivi",
            "Victorinox",
            "Bestech",
            "Mikov",
            "Boker",
            "Joker",
            "Kanetsune",
            "Muela",
            "Marttiini",
            "Benchmade",
            "Spyderco",
        ]
        settings_path = Path(__file__).resolve().parents[1] / "projects" / "roy" / "settings.json"
        settings = json.loads(settings_path.read_text(encoding="utf-8"))
        runtime = load_project_runtime(
            "roy",
            settings=settings,
            default_packaging_cost_per_order=0.0,
            default_shipping_subsidy_per_order=0.0,
            default_fixed_monthly_cost=0.0,
            default_fixed_daily_cost=0.0,
        )

        for brand in expected_brands:
            with self.subTest(brand=brand):
                self.assertEqual(35.0, runtime.margin_override_brands[brand])

    def test_roy_zero_margin_sku_overrides_cover_recent_fallback_products(self) -> None:
        expected_skus = [
            "12837",
            "H-1DADF217",
            "BC_AP2X",
            "7310-2G-4G",
            "11898",
            "H-CF3B7CAD",
            "690",
            "11439",
            "MCS30943",
            "H-E4CC29CC",
            "H-69235D5B",
            "F_393",
            "27068",
            "H-45306D08",
            "F_359",
            "F_1509",
            "H-93405DC3",
            "14949",
            "H-177FC644",
            "14002701",
            "39179",
            "840086",
            "F_438",
            "12039",
            "H-791A744A",
            "165016",
            "456890",
            "F_476",
            "F_392",
            "CONF-5643",
            "F_261",
            "F_1562",
            "78607-3",
            "H-AF2FD84B",
            "CONF-16806",
            "41001",
            "31853L",
            "ZSK001",
            "0-45",
            "H-9D2E0A2C",
            "1157",
            "F_403",
            "H-52688CE6",
            "TK-02S",
            "NTR002",
            "452045-1",
            "636",
            "CONF-7200",
            "15305",
            "CONF-15794",
            "14955",
            "16394",
            "33385",
            "780701",
            "4507700",
            "780700",
            "11442",
            "0-61",
            "406780",
            "TKO-02H",
            "870578",
            "TK-02H",
            "AOF-S+",
            "AOF-S+-G",
            "12252",
            "12258",
        ]
        settings_path = Path(__file__).resolve().parents[1] / "projects" / "roy" / "settings.json"
        settings = json.loads(settings_path.read_text(encoding="utf-8"))
        runtime = load_project_runtime(
            "roy",
            settings=settings,
            default_packaging_cost_per_order=0.0,
            default_shipping_subsidy_per_order=0.0,
            default_fixed_monthly_cost=0.0,
            default_fixed_daily_cost=0.0,
        )

        for sku in expected_skus:
            with self.subTest(sku=sku):
                self.assertEqual(35.0, runtime.margin_override_skus[sku])

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

    def test_creditnoted_excluded_order_keeps_fulfillment_cost_without_revenue(self) -> None:
        exporter = make_exporter()
        exporter.project_settings["creditnote_fulfillment_costs"] = {
            "enabled": True,
            "shipping_cost_per_order": 0.2,
        }
        exporter._creditnote_order_nums_cache = {"RETURN-1"}
        exporter._creditnote_status_change_audit_cache = {
            "project": "vevo",
            "orders": [{"order_num": "RETURN-1", "previous_status": "Odoslaná"}],
        }
        exporter.excluded_status_orders = [
            {
                "order_num": "RETURN-1",
                "pur_date": "2026-04-20 11:00:00",
                "status": {"name": "Storno"},
            }
        ]
        df = pd.DataFrame(
            [
                {
                    "order_num": "OK-1",
                    "customer_email": "ok@example.com",
                    "purchase_date": "2026-04-20 10:00:00",
                    "purchase_date_only": "2026-04-20",
                    "order_revenue_net": 100.0,
                    "total_items_in_order": 1,
                    "fb_ads_daily_spend": 0.0,
                    "google_ads_daily_spend": 0.0,
                    "product_sku": "SKU-OK",
                    "item_label": "Revenue item",
                    "item_quantity": 1,
                    "item_total_without_tax": 100.0,
                    "item_total_with_tax": 123.0,
                    "item_unit_price": 100.0,
                    "item_line_sum_original": 100.0,
                    "item_line_sum_with_tax_original": 123.0,
                    "item_unit_price_original": 100.0,
                    "total_expense": 40.0,
                    "profit_before_ads": 60.0,
                }
            ]
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            exporter.data_dir = Path(tmp_dir)
            _, date_agg, _, month_agg, _ = exporter.create_aggregated_reports(
                df,
                datetime(2026, 4, 20),
                datetime(2026, 4, 20),
                fb_daily_spend={},
                google_ads_daily_spend={},
            )

        day = date_agg.iloc[0]
        self.assertEqual(1, int(day["unique_orders"]))
        self.assertEqual(1, int(day["creditnote_fulfillment_orders"]))
        self.assertEqual(100.0, float(day["total_revenue"]))
        self.assertEqual(40.0, float(day["product_expense"]))
        self.assertEqual(0.3, float(day["creditnote_packaging_cost"]))
        self.assertEqual(0.2, float(day["creditnote_shipping_net_cost"]))
        self.assertEqual(0.5, float(day["creditnote_fulfillment_cost"]))
        self.assertEqual(0.6, float(day["packaging_cost"]))
        self.assertEqual(0.4, float(day["shipping_net_cost"]))
        self.assertEqual(59.0, float(day["net_profit"]))
        self.assertEqual(1, int(month_agg.iloc[0]["creditnote_fulfillment_orders"]))

    def test_creditnoted_excluded_order_without_sent_audit_does_not_keep_fulfillment_cost(self) -> None:
        exporter = make_exporter()
        exporter.project_settings["creditnote_fulfillment_costs"] = {
            "enabled": True,
            "shipping_cost_per_order": 0.2,
        }
        exporter._creditnote_order_nums_cache = {"RETURN-1"}
        exporter._creditnote_status_change_audit_cache = {"project": "vevo", "orders": []}
        exporter.excluded_status_orders = [
            {
                "order_num": "RETURN-1",
                "pur_date": "2026-04-20 11:00:00",
                "status": {"name": "Storno"},
            }
        ]
        df = pd.DataFrame(
            [
                {
                    "order_num": "OK-1",
                    "customer_email": "ok@example.com",
                    "purchase_date": "2026-04-20 10:00:00",
                    "purchase_date_only": "2026-04-20",
                    "order_revenue_net": 100.0,
                    "total_items_in_order": 1,
                    "fb_ads_daily_spend": 0.0,
                    "google_ads_daily_spend": 0.0,
                    "product_sku": "SKU-OK",
                    "item_label": "Revenue item",
                    "item_quantity": 1,
                    "item_total_without_tax": 100.0,
                    "item_total_with_tax": 123.0,
                    "item_unit_price": 100.0,
                    "item_line_sum_original": 100.0,
                    "item_line_sum_with_tax_original": 123.0,
                    "item_unit_price_original": 100.0,
                    "total_expense": 40.0,
                    "profit_before_ads": 60.0,
                }
            ]
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            exporter.data_dir = Path(tmp_dir)
            _, date_agg, _, month_agg, _ = exporter.create_aggregated_reports(
                df,
                datetime(2026, 4, 20),
                datetime(2026, 4, 20),
                fb_daily_spend={},
                google_ads_daily_spend={},
            )

        day = date_agg.iloc[0]
        self.assertEqual(0, int(day["creditnote_fulfillment_orders"]))
        self.assertEqual(0.0, float(day["creditnote_fulfillment_cost"]))
        self.assertEqual(1, int(day["unique_orders"]))
        self.assertEqual(59.5, float(day["net_profit"]))
        self.assertEqual(0, int(month_agg.iloc[0]["creditnote_fulfillment_orders"]))

    @patch("creditnote_export.fetch_project_creditnotes")
    def test_creditnote_reporting_metrics_use_sent_orders_as_carrier_denominator(self, fetch_mock) -> None:
        exporter = make_exporter()
        exporter.project_settings["currency_rates_to_eur"] = {"EUR": 1.0, "CZK": 0.04}
        exporter._creditnote_status_change_audit_cache = {
            "project": "vevo",
            "orders": [{"order_num": "RET-1", "previous_status": "Odoslaná"}],
        }
        packeta = price_element("shipping", "Packeta - vydajne miesto", "9")
        sps = price_element("shipping", "SPS Balikovo", "14")
        orders = [
            {
                "order_num": "OK-1",
                "pur_date": "2026-06-01 10:00:00",
                "status": {"name": "Odoslaná"},
                "price_elements": [packeta, price_element("payment", "Dobierka", "7")],
            },
            {
                "order_num": "OK-2",
                "pur_date": "2026-06-01 11:00:00",
                "status": {"name": "Odoslaná"},
                "price_elements": [sps, price_element("payment", "Dobierka", "7")],
            },
        ]
        exporter.excluded_status_orders = [
            {
                "order_num": "RET-1",
                "pur_date": "2026-06-01 12:00:00",
                "status": {"name": "Storno"},
                "price_elements": [packeta, price_element("payment", "Dobierka", "7")],
            }
        ]
        fetch_mock.return_value = (
            [
                {
                    "number": "D-1",
                    "creditnote_id": "1",
                    "created": "2026-06-02 08:00:00",
                    "order_num": "OK-1",
                    "price": "100 €",
                    "taxed_price": "123 €",
                },
                {
                    "number": "D-2",
                    "creditnote_id": "2",
                    "created": "2026-06-02 09:00:00",
                    "order_num": "RET-1",
                    "price": "200 Kč",
                    "taxed_price": "250 Kč",
                },
            ],
            2,
        )
        date_agg = pd.DataFrame(
            [
                {
                    "date": datetime(2026, 6, 1).date(),
                    "unique_orders": 2,
                    "creditnote_fulfillment_orders": 1,
                    "creditnote_packaging_cost": 0.3,
                    "creditnote_shipping_net_cost": 0.2,
                    "creditnote_fulfillment_cost": 0.5,
                }
            ]
        )

        metrics = exporter.analyze_creditnote_reporting_metrics(
            orders,
            datetime(2026, 6, 1),
            datetime(2026, 6, 30),
            date_agg,
        )

        summary = metrics["summary"]
        self.assertEqual(2, summary["creditnotes"])
        self.assertEqual(2, summary["creditnoted_orders"])
        self.assertEqual(2, summary["all_creditnoted_orders"])
        self.assertEqual(2, summary["sent_creditnoted_orders"])
        self.assertEqual(133.0, summary["credited_gross_eur"])
        self.assertEqual(108.0, summary["credited_net_eur"])
        self.assertEqual(1, summary["fulfillment_orders"])
        self.assertEqual(0.5, summary["fulfillment_cost_eur"])
        packeta_row = next(row for row in metrics["carrier_rows"] if row["carrier"] == "Packeta")
        self.assertEqual(2, packeta_row["realized_orders"])
        self.assertEqual(2, packeta_row["creditnoted_orders"])
        self.assertEqual(100.0, packeta_row["creditnote_rate_pct"])

    @patch("creditnote_export.fetch_project_creditnotes")
    def test_creditnote_reporting_metrics_use_creditnote_count_for_rate(self, fetch_mock) -> None:
        exporter = make_exporter()
        exporter.project_settings["currency_rates_to_eur"] = {"EUR": 1.0}
        exporter._creditnote_status_change_audit_cache = {"project": "vevo", "orders": []}
        packeta = price_element("shipping", "Packeta - vydajne miesto", "9")
        orders = [
            {
                "order_num": "OK-1",
                "pur_date": "2026-06-01 10:00:00",
                "status": {"name": "Odoslaná"},
                "price_elements": [packeta, price_element("payment", "Dobierka", "7")],
            },
        ]
        exporter.excluded_status_orders = [
            {
                "order_num": "RET-1",
                "pur_date": "2026-06-01 12:00:00",
                "status": {"name": "Storno"},
                "price_elements": [packeta, price_element("payment", "Dobierka", "7")],
            }
        ]
        fetch_mock.return_value = (
            [
                {
                    "number": "D-1",
                    "creditnote_id": "1",
                    "created": "2026-06-02 08:00:00",
                    "order_num": "OK-1",
                    "price": "100 €",
                    "taxed_price": "123 €",
                },
                {
                    "number": "D-2",
                    "creditnote_id": "2",
                    "created": "2026-06-02 09:00:00",
                    "order_num": "RET-1",
                    "price": "40 €",
                    "taxed_price": "49.2 €",
                },
            ],
            2,
        )
        date_agg = pd.DataFrame(
            [
                {
                    "date": datetime(2026, 6, 1).date(),
                    "unique_orders": 1,
                    "creditnote_fulfillment_orders": 0,
                    "creditnote_packaging_cost": 0.0,
                    "creditnote_shipping_net_cost": 0.0,
                    "creditnote_fulfillment_cost": 0.0,
                }
            ]
        )

        metrics = exporter.analyze_creditnote_reporting_metrics(
            orders,
            datetime(2026, 6, 1),
            datetime(2026, 6, 30),
            date_agg,
        )

        summary = metrics["summary"]
        self.assertEqual(2, summary["creditnotes"])
        self.assertEqual(2, summary["all_creditnoted_orders"])
        self.assertEqual(1, summary["creditnoted_orders"])
        self.assertEqual(1, summary["sent_creditnoted_orders"])
        self.assertEqual(200.0, summary["creditnote_rate_pct"])
        packeta_row = next(row for row in metrics["carrier_rows"] if row["carrier"] == "Packeta")
        self.assertEqual(1, packeta_row["realized_orders"])
        self.assertEqual(1, packeta_row["creditnoted_orders"])
        self.assertEqual(2, packeta_row["creditnotes"])
        self.assertEqual(200.0, packeta_row["creditnote_rate_pct"])

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

    def test_cache_save_preserves_raw_excluded_status_orders(self) -> None:
        exporter = make_exporter()
        order_date = datetime(2026, 6, 1)
        orders = [
            reporting_order("OK-1", "Odoslana", "Dobierkou", "7"),
            reporting_order("STORNO-1", "Storno", "Dobierkou", "7"),
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            exporter.cache_dir = Path(tmp_dir)
            exporter.save_to_cache_simple(order_date, orders)
            payload = json.loads(exporter.get_cache_filename(order_date).read_text(encoding="utf-8"))
            loaded = exporter.load_from_cache(order_date)

        self.assertEqual(ORDER_CACHE_SCHEMA_VERSION, payload["schema_version"])
        self.assertEqual(["OK-1", "STORNO-1"], [order["order_num"] for order in loaded])

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
