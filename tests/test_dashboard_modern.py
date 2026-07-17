import unittest
from datetime import datetime

import pandas as pd

from dashboard_modern import (
    _build_fb_daily_payload,
    _format_attributed_cpa,
    _format_attributed_order_estimate,
    _frame_rows,
    extract_embedded_dashboard_payload,
    generate_modern_dashboard,
)


class DashboardModernTests(unittest.TestCase):
    def test_attributed_campaign_values_use_adaptive_precision(self) -> None:
        self.assertEqual("0.232", _format_attributed_order_estimate(0.232, "estimated"))
        self.assertEqual("&lt;0.0001", _format_attributed_order_estimate(0, "insufficient_sample"))
        self.assertEqual("&euro;1.21", _format_attributed_cpa(1.21, "estimated"))
        self.assertEqual("N/A", _format_attributed_cpa(None, "insufficient_sample"))

    def test_fb_daily_payload_zero_fills_missing_report_dates(self) -> None:
        payload = _build_fb_daily_payload(
            datetime(2026, 5, 1),
            datetime(2026, 5, 4),
            {
                "2026-05-01": {"spend": 10.24, "clicks": 80, "impressions": 3924},
                "2026-05-03": {"spend": 10.09, "clicks": 120, "impressions": 5630},
            },
        )

        self.assertEqual(
            ["2026-05-01", "2026-05-02", "2026-05-03", "2026-05-04"],
            payload["dates"],
        )
        self.assertEqual([10.24, 0.0, 10.09, 0.0], payload["spend"])
        self.assertEqual([80.0, 0.0, 120.0, 0.0], payload["clicks"])
        self.assertEqual([3924.0, 0.0, 5630.0, 0.0], payload["impressions"])

    def test_frame_rows_serializes_nested_country_top_products(self) -> None:
        rows = _frame_rows(
            pd.DataFrame(
                [
                    {
                        "country": "sk",
                        "top_products": [
                            {"sku": "12474", "revenue": 100.5, "stockout_date": pd.NaT},
                            {"sku": "sd32", "revenue": 20, "note": None},
                        ],
                    }
                ]
            ),
            ["country", "top_products"],
        )

        self.assertEqual("sk", rows[0]["country"])
        self.assertEqual("12474", rows[0]["top_products"][0]["sku"])
        self.assertIsNone(rows[0]["top_products"][0]["stockout_date"])

    def test_roy_loss_product_payload_keeps_gross_loss_fields(self) -> None:
        date_agg = pd.DataFrame(
            [
                {
                    "date": pd.Timestamp("2026-05-01"),
                    "total_revenue": 100.0,
                    "net_profit": 20.0,
                    "contribution_profit": 25.0,
                    "unique_orders": 1,
                    "fb_ads_spend": 0.0,
                    "google_ads_spend": 0.0,
                    "total_items": 1,
                    "product_expense": 80.0,
                    "total_cost": 80.0,
                    "pre_ad_contribution_profit": 20.0,
                }
            ]
        )
        items_agg = pd.DataFrame([{"item_label": "Test", "total_quantity": 1, "total_revenue": 100.0}])
        loss_product_rows = pd.DataFrame(
            [
                {
                    "sku": "LOSS-1",
                    "product": "Loss product",
                    "orders": 3,
                    "units": 3,
                    "revenue": 30.0,
                    "gross_profit": -3.0,
                    "profit_without_fixed": 5.0,
                    "profit_with_fixed": 10.0,
                    "gross_margin_pct": -10.0,
                    "margin_without_fixed_pct": 16.7,
                    "margin_with_fixed_pct": 33.3,
                    "first_sale": "2026-05-01",
                    "last_sale": "2026-05-02",
                }
            ]
        )

        html = generate_modern_dashboard(
            date_agg,
            items_agg,
            datetime(2026, 5, 1),
            datetime(2026, 5, 1),
            report_title="ROY test",
            advanced_dtc_metrics={
                "roy_product_demand": {
                    "summary": {"inventory_status": "ok"},
                    "loss_product_rows": loss_product_rows,
                }
            },
            source_health={"project": "roy"},
        )

        payload = extract_embedded_dashboard_payload(html)
        row = payload["roy_product_demand"]["loss_product_rows"][0]

        self.assertEqual(-3.0, row["gross_profit"])
        self.assertEqual(-10.0, row["gross_margin_pct"])

    def test_roy_inventory_cost_history_payload_and_chart_are_visible(self) -> None:
        date_agg = pd.DataFrame(
            [
                {
                    "date": pd.Timestamp("2026-06-18"),
                    "total_revenue": 100.0,
                    "net_profit": 20.0,
                    "contribution_profit": 25.0,
                    "unique_orders": 1,
                    "fb_ads_spend": 0.0,
                    "google_ads_spend": 0.0,
                    "total_items": 1,
                    "product_expense": 60.0,
                    "total_cost": 80.0,
                    "pre_ad_contribution_profit": 35.0,
                }
            ]
        )
        items_agg = pd.DataFrame([{"item_label": "Test", "total_quantity": 1, "total_revenue": 100.0}])
        history_rows = pd.DataFrame(
            [
                {
                    "date": "2026-06-17",
                    "inventory_cost_value": 100.0,
                    "inventory_retail_value": 180.0,
                    "dead_stock_cost_value": 15.0,
                },
                {
                    "date": "2026-06-18",
                    "inventory_cost_value": 150.0,
                    "inventory_retail_value": 260.0,
                    "dead_stock_cost_value": 20.0,
                },
            ]
        )

        html = generate_modern_dashboard(
            date_agg,
            items_agg,
            datetime(2026, 6, 18),
            datetime(2026, 6, 18),
            report_title="ROY inventory test",
            advanced_dtc_metrics={
                "roy_product_demand": {
                    "summary": {
                        "inventory_status": "ok",
                        "inventory_snapshot_date": "2026-06-18",
                        "inventory_cost_value": 150.0,
                        "inventory_retail_value": 260.0,
                    },
                    "inventory_cost_history_rows": history_rows,
                }
            },
            source_health={"project": "roy"},
        )

        self.assertIn("royInventoryCostValueChart", html)
        self.assertIn("Inventory cost value trend", html)
        payload = extract_embedded_dashboard_payload(html)
        self.assertEqual(2, len(payload["roy_product_demand"]["inventory_cost_history_rows"]))
        self.assertEqual(
            150.0,
            payload["roy_product_demand"]["inventory_cost_history_rows"][-1]["inventory_cost_value"],
        )

    def test_smart_inventory_fields_and_persistence_survive_payload_roundtrip(self) -> None:
        date_agg = pd.DataFrame(
            [
                {
                    "date": pd.Timestamp("2026-07-17"),
                    "total_revenue": 100.0,
                    "net_profit": 20.0,
                    "contribution_profit": 25.0,
                    "unique_orders": 1,
                    "fb_ads_spend": 0.0,
                    "google_ads_spend": 0.0,
                    "total_items": 1,
                    "product_expense": 60.0,
                    "total_cost": 80.0,
                    "pre_ad_contribution_profit": 35.0,
                }
            ]
        )
        items_agg = pd.DataFrame(
            [{"item_label": "Test", "total_quantity": 1, "total_revenue": 100.0}]
        )
        smart_row = {
            "sku": "SPIKE-1",
            "product": "Slow product",
            "available_quantity": 20,
            "stock_risk_level": "Healthy",
            "raw_recent_30d_units": 40,
            "raw_alert_30d_units": 42.8,
            "alert_30d_units": 0.8,
            "demand_adjustment_units_30d": 42.0,
            "unusual_large_order_flag": True,
            "unusual_large_order_adjustment_units_30d": 35,
            "largest_order_units_30d": 40,
            "demand_model": "tsb_intermittent",
            "demand_model_version": "order-aware-tsb-v1",
            "demand_confidence": "Low",
            "demand_signal_code": "one_off_large_order",
            "demand_signal_label_sk": "Jednorazová veľká objednávka",
            "lead_time_stockout_probability": 0.01,
            "alert_reason_code": "unusual_large_order",
            "alert_reason_label_sk": "Neobvykle veľká objednávka bez potvrdenia trendu",
        }

        html = generate_modern_dashboard(
            date_agg,
            items_agg,
            datetime(2026, 7, 17),
            datetime(2026, 7, 17),
            advanced_dtc_metrics={
                "roy_product_demand": {
                    "summary": {
                        "inventory_status": "ok",
                        "demand_model_version": "order-aware-tsb-v1",
                    },
                    "inventory_rows": pd.DataFrame([smart_row]),
                    "demand_anomaly_rows": pd.DataFrame([smart_row]),
                    "demand_signal_history_rows": pd.DataFrame(
                        [
                            {
                                "sku": "SPIKE-1",
                                "last_check_date": "2026-07-17",
                                "trend_candidate_checks": "010",
                                "trend_confirmation_count": 1,
                                "trend_confirmed_flag": False,
                                "trend_persistence_window": 3,
                                "trend_persistence_required": 2,
                            }
                        ]
                    ),
                }
            },
            source_health={"project": "roy"},
        )

        payload = extract_embedded_dashboard_payload(html)["roy_product_demand"]
        self.assertEqual(40, payload["inventory_rows"][0]["raw_recent_30d_units"])
        self.assertEqual(
            "one_off_large_order",
            payload["demand_anomaly_rows"][0]["demand_signal_code"],
        )
        self.assertEqual(
            "010",
            payload["demand_signal_history_rows"][0]["trend_candidate_checks"],
        )

    def test_creditnote_metrics_are_visible_in_modern_dashboard(self) -> None:
        date_agg = pd.DataFrame(
            [
                {
                    "date": pd.Timestamp("2026-06-01"),
                    "total_revenue": 100.0,
                    "net_profit": 20.0,
                    "contribution_profit": 25.0,
                    "unique_orders": 2,
                    "fb_ads_spend": 0.0,
                    "google_ads_spend": 0.0,
                    "total_items": 2,
                    "product_expense": 60.0,
                    "total_cost": 80.0,
                    "pre_ad_contribution_profit": 35.0,
                }
            ]
        )
        items_agg = pd.DataFrame([{"item_label": "Test", "total_quantity": 2, "total_revenue": 100.0}])

        html = generate_modern_dashboard(
            date_agg,
            items_agg,
            datetime(2026, 6, 1),
            datetime(2026, 6, 1),
            advanced_dtc_metrics={
                "creditnotes": {
                    "summary": {
                        "available": True,
                        "creditnotes": 2,
                        "creditnoted_orders": 2,
                        "realized_orders": 10,
                        "creditnote_rate_pct": 20.0,
                        "credited_gross_eur": 49.2,
                        "credited_net_eur": 40.0,
                        "revenue_excluded_orders": 2,
                        "revenue_included_orders": 0,
                        "fulfillment_cost_eur": 1.0,
                        "fulfillment_orders": 2,
                        "outlier_carrier_count": 1,
                    },
                    "carrier_rows": [
                        {
                            "carrier": "Packeta",
                            "realized_orders": 10,
                            "creditnoted_orders": 2,
                            "creditnotes": 2,
                            "creditnote_rate_pct": 20.0,
                            "rate_index": 2.0,
                            "credited_gross_eur": 49.2,
                            "outlier": True,
                        }
                    ],
                    "currency_rows": [
                        {
                            "currency": "EUR",
                            "creditnotes": 2,
                            "credited_gross": 49.2,
                            "credited_gross_eur": 49.2,
                            "credited_net_eur": 40.0,
                        }
                    ],
                }
            },
        )

        self.assertIn("Creditnotes and carrier return rate", html)
        self.assertIn("creditnote-carrier-row outlier", html)
        self.assertIn("Packeta", html)
        payload = extract_embedded_dashboard_payload(html)
        self.assertEqual(49.2, payload["creditnotes"]["summary"]["credited_gross_eur"])
        self.assertEqual("Packeta", payload["creditnotes"]["carrier_rows"][0]["carrier"])

    def test_decision_safety_fixes_are_visible_in_roy_dashboard(self) -> None:
        date_agg = pd.DataFrame(
            [
                {
                    "date": pd.Timestamp("2026-07-14"),
                    "total_revenue": 100.0,
                    "net_profit": 20.0,
                    "contribution_profit": 25.0,
                    "unique_orders": 1,
                    "fb_ads_spend": 0.0,
                    "google_ads_spend": 0.0,
                    "total_items": 1,
                    "product_expense": 60.0,
                    "total_cost": 80.0,
                    "pre_ad_contribution_profit": 35.0,
                }
            ]
        )
        items_agg = pd.DataFrame([{"item_label": "Test", "total_quantity": 1, "total_revenue": 100.0}])
        alert_rows = pd.DataFrame(
            [
                {
                    "sku": "W-1",
                    "product": "Wachman Discovery",
                    "strategic_stock_flag": True,
                    "stock_risk_level": "Critical",
                    "available_quantity": 1,
                    "alert_30d_units": 134,
                    "days_of_cover": 2,
                    "lead_time_working_days": 30,
                    "reorder_by_date": None,
                    "reorder_by_date_estimate": "2026-07-01",
                    "suggested_reorder_units": None,
                    "suggested_reorder_units_estimate": 134,
                    "reorder_action_label": "Review risk",
                    "alert_30d_revenue": 1000,
                    "recommendation_ready": False,
                    "recommendation_confidence": "low",
                    "recommendation_note": "Estimate only",
                }
            ]
        )

        html = generate_modern_dashboard(
            date_agg,
            items_agg,
            datetime(2026, 7, 14),
            datetime(2026, 7, 14),
            advanced_dtc_metrics={
                "roy_product_demand": {
                    "summary": {
                        "inventory_status": "ok",
                        "inventory_recommendation_ready": False,
                        "inventory_recommendation_confidence": "low",
                        "inventory_recommendation_blockers": ["inbound purchase orders are not modeled"],
                        "alert_attention_count": 1,
                    },
                    "alert_rows": alert_rows,
                }
            },
            same_item_repurchase={
                "customer_item_frequency": pd.DataFrame(
                    [{"purchase_frequency": "2x", "customer_count": 3, "percentage": 60.0}]
                )
            },
            source_health={
                "project": "roy",
                "qa": {
                    "product_expense_coverage": {
                        "fallback_policy": "missing costs use a configured 35% margin estimate"
                    }
                },
            },
        )

        self.assertIn("<tr><td>2x</td><td>3</td><td>60.0%</td></tr>", html)
        self.assertIn("WARNING ONLY / LOW", html)
        self.assertIn("<td>Blocked</td>", html)
        self.assertIn("configured 35% margin estimate", html)
        self.assertNotIn("conservative zero-margin fallback", html)
        self.assertNotIn("@media (max-width: 1280px)", html)
        self.assertIn("@media (max-width: 900px)", html)
        self.assertIn("grid-template-columns: 240px minmax(0, 1fr)", html)
        self.assertIn('<nav class="sidebar-nav" aria-label="Reporting sections">', html)
        self.assertIn(".sidebar-nav { display:flex", html)
        self.assertIn("CEO decision cockpit", html)
        payload = extract_embedded_dashboard_payload(html)
        self.assertEqual(5, len(payload["ceo_cockpit"]["actions"]))
        self.assertEqual("not_configured", payload["ceo_cockpit"]["company_profit_plan_status"])

    def test_roy_ceo_cockpit_explains_30_day_profit_change(self) -> None:
        rows = []
        for day in range(60):
            current = day >= 30
            rows.append(
                {
                    "date": pd.Timestamp("2026-05-01") + pd.Timedelta(days=day),
                    "total_revenue": 120.0 if current else 100.0,
                    "net_profit": 20.0 if current else 10.0,
                    "contribution_profit": 25.0 if current else 15.0,
                    "unique_orders": 1,
                    "fb_ads_spend": 10.0,
                    "google_ads_spend": 0.0,
                    "total_items": 1,
                    "product_expense": 60.0 if current else 50.0,
                    "packaging_cost": 5.0,
                    "shipping_net_cost": 0.0,
                    "fixed_daily_cost": 25.0,
                    "total_cost": 100.0 if current else 90.0,
                    "pre_ad_contribution_profit": 55.0 if current else 45.0,
                }
            )
        date_agg = pd.DataFrame(rows)
        items_agg = pd.DataFrame([{"item_label": "Test", "total_quantity": 60, "total_revenue": 6600.0}])

        html = generate_modern_dashboard(
            date_agg,
            items_agg,
            datetime(2026, 5, 1),
            datetime(2026, 6, 29),
            report_title="ROY CEO test",
            advanced_dtc_metrics={"roy_product_demand": {"summary": {"inventory_status": "ok"}}},
            source_health={"project": "roy"},
        )

        payload = extract_embedded_dashboard_payload(html)["ceo_cockpit"]
        self.assertEqual(5, len(payload["actions"]))
        self.assertEqual(6, len(payload["waterfall_rows"]))
        self.assertEqual("Company profit change", payload["waterfall_rows"][-1]["label_en"])
        self.assertEqual(300.0, payload["waterfall_rows"][-1]["profit_effect"])

    def test_vevo_ceo_cockpit_surfaces_recent_profit_cost_risk_and_returns(self) -> None:
        rows = []
        for day in range(60):
            current = day >= 30
            rows.append(
                {
                    "date": pd.Timestamp("2026-05-01") + pd.Timedelta(days=day),
                    "total_revenue": 120.0 if current else 100.0,
                    "net_profit": 4.0 if current else 10.0,
                    "contribution_profit": 74.0 if current else 80.0,
                    "unique_orders": 1,
                    "fb_ads_spend": 10.0,
                    "google_ads_spend": 2.0,
                    "total_items": 1,
                    "product_expense": 30.0,
                    "packaging_cost": 0.3,
                    "shipping_net_cost": 0.2,
                    "fixed_daily_cost": 70.0,
                    "total_cost": 116.0 if current else 90.0,
                    "pre_ad_contribution_profit": 89.5 if current else 69.5,
                }
            )
        missing_items = [
            {
                "product_sku": f"MISSING-{index}",
                "item_label": f"Missing product {index}",
                "rows": index,
                "units": index,
                "revenue": float(index * 10),
                "total_revenue_share_pct": float(index) / 10,
                "profit_before_ads": float(index * 3.5),
            }
            for index in range(1, 7)
        ]

        html = generate_modern_dashboard(
            pd.DataFrame(rows),
            pd.DataFrame([{"item_label": "Test", "total_quantity": 60, "total_revenue": 6600.0}]),
            datetime(2026, 5, 1),
            datetime(2026, 6, 29),
            report_title="VEVO CEO test",
            advanced_dtc_metrics={
                "creditnotes": {
                    "summary": {
                        "available": True,
                        "all_creditnoted_orders": 12,
                        "credited_gross_eur": 240.0,
                        "credited_net_eur": 195.12,
                    }
                }
            },
            refunds_analysis={
                "summary": {"refund_orders": 1, "refund_rate_pct": 1.0, "refund_amount": 10.0}
            },
            source_health={
                "project": "vevo",
                "reporting_assumptions": {
                    "fixed_cost": {
                        "mode": "manual_daily_estimate",
                        "actuals_configured": False,
                    }
                },
                "qa": {
                    "product_expense_coverage": {
                        "total_profit_before_ads": 1000.0,
                        "fallback_profit_before_ads": 73.5,
                        "fallback_items": missing_items,
                        "fallback_policy": "missing costs use a configured 35% margin estimate",
                    }
                },
            },
        )

        payload = extract_embedded_dashboard_payload(html)["ceo_cockpit"]
        self.assertEqual(5, len(payload["actions"]))
        self.assertEqual(120.0, payload["window_metrics"]["30"]["profit"])
        self.assertEqual(12, payload["returns_and_creditnotes"]["creditnoted_orders"])
        self.assertEqual("manual_daily_estimate", payload["fixed_cost_assumption"]["mode"])
        self.assertIn("Replace overhead estimate with actuals", html)
        self.assertIn("Complete missing purchase-cost list", html)
        self.assertIn("MISSING-6", html)
        self.assertIn("Order-status refund proxy", html)


if __name__ == "__main__":
    unittest.main()
