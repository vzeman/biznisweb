#!/usr/bin/env python3
from __future__ import annotations

import math
import pathlib
import sys
from datetime import timedelta

import pandas as pd


ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from export_orders import BizniWebExporter
from reporting_core.cfo_kpis import build_cfo_kpi_payload


def make_exporter() -> BizniWebExporter:
    return BizniWebExporter(
        api_url="https://example.com/graphql",
        api_token="dummy",
        project_name="vevo",
        output_tag="qa_smoke",
        enable_period_bundle=False,
    )


def base_financial_metrics() -> dict:
    return {
        "pre_ad_contribution_per_order": 10.0,
        "break_even_cac": 12.5,
        "payback_orders": 0.5,
        "contribution_ltv_cac": 2.5,
        "cm1_profit": 1000.0,
        "cm1_profit_per_order": 10.0,
        "cm1_profit_per_customer": 12.5,
        "cm2_profit": 800.0,
        "cm3_profit": 700.0,
        "current_fb_cac": 5.0,
        "total_orders": 100,
        "refund_orders": 4,
        "refund_rate_pct": 4.0,
        "refund_amount": 21.11,
    }


def base_cost_per_order() -> dict:
    return {
        "campaign_attribution": [
            {
                "spend": 100.0,
                "platform_conversions": 10.0,
                "cost_per_platform_conversion": 10.0,
                "attributed_orders_est": 5.0,
                "cost_per_attributed_order": 20.0,
            }
        ],
        "campaign_attribution_summary": {
            "estimated_orders_total": 5.0,
            "total_orders": 100.0,
        },
    }


def assert_data_assertions_ok(exporter: BizniWebExporter) -> None:
    qa = exporter._build_data_assertions_qa(
        financial_metrics=base_financial_metrics(),
        consistency_checks={"roas_ok": True, "company_margin_ok": True, "cac_ok": True},
        refunds_analysis={"summary": {"refund_orders": 4, "refund_rate_pct": 4.0, "refund_amount": 21.11}},
        day_of_week_analysis=pd.DataFrame({"day_name": ["Mon", "Tue"], "orders": [10, 12]}),
        advanced_dtc_metrics={
            "attach_rate": pd.DataFrame(
                {"anchor_item": ["Sample"], "attached_item": ["Bottle"], "anchor_orders": [25]}
            )
        },
        country_analysis=pd.DataFrame({"country": ["sk", "cz"], "orders": [80, 20]}),
        geo_profitability={
            "table": pd.DataFrame(
                {
                    "country": ["sk", "cz"],
                    "orders": [80, 20],
                    "revenue": [1000.0, 200.0],
                    "confidence_status": ["ready", "ready"],
                }
            )
        },
        cost_per_order=base_cost_per_order(),
    )
    assert qa["status"] == "ok", qa
    assert qa["failure_count"] == 0, qa
    assert qa["warning_count"] == 0, qa


def assert_refund_registry_failure(exporter: BizniWebExporter) -> None:
    broken_metrics = base_financial_metrics()
    broken_metrics.pop("refund_rate_pct")
    qa = exporter._build_data_assertions_qa(
        financial_metrics=broken_metrics,
        consistency_checks={"roas_ok": True, "company_margin_ok": True, "cac_ok": True},
        refunds_analysis={"summary": {"refund_orders": 4, "refund_rate_pct": 4.0, "refund_amount": 21.11}},
        day_of_week_analysis=pd.DataFrame({"day_name": ["Mon"], "orders": [10]}),
        advanced_dtc_metrics={
            "attach_rate": pd.DataFrame(
                {"anchor_item": ["Sample"], "attached_item": ["Bottle"], "anchor_orders": [25]}
            )
        },
        country_analysis=pd.DataFrame({"country": ["sk"], "orders": [100]}),
        geo_profitability={
            "table": pd.DataFrame(
                {
                    "country": ["sk"],
                    "orders": [100],
                    "revenue": [1200.0],
                    "confidence_status": ["ready"],
                }
            )
        },
        cost_per_order=base_cost_per_order(),
    )
    assert qa["status"] == "critical", qa
    assert any("Refund summary metrics are missing" in msg for msg in qa["failures"]), qa


def assert_geo_warning(exporter: BizniWebExporter) -> None:
    qa = exporter._build_geo_qa(
        country_analysis=pd.DataFrame({"country": ["sk", "unknown"], "orders": [9, 1]}),
        geo_profitability={
            "table": pd.DataFrame(
                {
                    "country": ["sk", "hu", "cz"],
                    "orders": [50, 2, 8],
                    "revenue": [1000.0, 50.0, 120.0],
                    "confidence_status": ["ready", "ignore", "observe"],
                }
            )
        },
    )
    assert qa["status"] == "warning", qa
    assert qa["ignore_count"] == 1, qa
    assert qa["observe_count"] == 1, qa
    assert qa["unknown_country_rate"] == 10.0, qa


def assert_margin_stability(exporter: BizniWebExporter) -> None:
    stable = exporter._build_margin_stability_qa(
        pd.DataFrame(
            {
                "date": pd.date_range("2026-03-01", periods=8, freq="D"),
                "total_revenue": [100.0] * 8,
                "pre_ad_contribution_profit": [70.0] * 8,
                "fixed_daily_cost": [10.0] * 8,
                "net_profit": [45.0] * 8,
            }
        )
    )
    assert stable["status"] == "ok", stable

    unstable = exporter._build_margin_stability_qa(
        pd.DataFrame(
            {
                "date": pd.date_range("2026-03-01", periods=8, freq="D"),
                "total_revenue": [10.0] * 8,
                "pre_ad_contribution_profit": [0.0] * 8,
                "fixed_daily_cost": [50.0] * 8,
                "net_profit": [-60.0] * 8,
            }
        )
    )
    assert unstable["status"] == "warning", unstable
    assert unstable["smoothed_extreme_days"] > 0, unstable


def assert_product_expense_coverage(exporter: BizniWebExporter) -> None:
    healthy = exporter._build_product_expense_coverage_qa(
        pd.DataFrame(
            {
                "order_num": ["1", "2"],
                "item_label": ["Mapped SKU", "Mapped title"],
                "product_sku": ["SKU-1", "SKU-2"],
                "item_quantity": [1, 2],
                "item_total_without_tax": [10.0, 18.0],
                "profit_before_ads": [4.0, 7.0],
                "expense_per_item": [6.0, 5.5],
                "expense_source": ["mapped_product_sku", "mapped_item_label"],
            }
        )
    )
    assert healthy["status"] == "ok", healthy

    risky = exporter._build_product_expense_coverage_qa(
        pd.DataFrame(
            {
                "order_num": ["1", "2", "3"],
                "item_label": ["Mapped SKU", "Fallback sample", "Fallback sample"],
                "product_sku": ["SKU-1", "SKU-FALLBACK", "SKU-FALLBACK"],
                "item_quantity": [1, 1, 1],
                "item_total_without_tax": [10.0, 8.0, 7.0],
                "profit_before_ads": [4.0, 7.0, 6.0],
                "expense_per_item": [6.0, 1.0, 1.0],
                "expense_source": ["mapped_product_sku", "fallback_default", "fallback_default"],
            }
        )
    )
    assert risky["status"] == "critical", risky
    assert risky["fallback_rows"] == 2, risky
    assert risky["fallback_revenue_share_pct"] == 60.0, risky
    assert risky["top_fallback_items"][0]["product_sku"] == "SKU-FALLBACK", risky


def assert_cfo_kpi_layer_invariants() -> None:
    date_agg = pd.DataFrame(
        {
            "date": pd.date_range("2026-03-01", periods=3, freq="D"),
            "total_quantity": [3, 4, 2],
            "total_revenue": [100.0, 200.0, 50.0],
            "product_expense": [30.0, 60.0, 20.0],
            "unique_orders": [2, 4, 1],
            "fb_ads_spend": [5.0, 10.0, 2.0],
            "google_ads_spend": [5.0, 10.0, 3.0],
            "packaging_cost": [2.0, 4.0, 1.0],
            "shipping_net_cost": [3.0, 6.0, 1.0],
            "pre_ad_contribution_profit": [65.0, 130.0, 28.0],
            "pre_ad_contribution_margin_pct": [65.0, 65.0, 56.0],
            "contribution_profit": [55.0, 110.0, 23.0],
            "net_profit": [45.0, 90.0, 18.0],
            "fixed_daily_cost": [10.0, 20.0, 5.0],
        }
    )

    payload = build_cfo_kpi_payload(date_agg=date_agg, export_df=None, fixed_daily_cost_eur=999.0)
    assert payload["default_window"] == "monthly", payload

    date_agg = date_agg.copy()
    date_agg["date"] = pd.to_datetime(date_agg["date"]).dt.date
    last_date = date_agg["date"].max()
    first_date = date_agg["date"].min()
    all_time_days = (last_date - first_date).days + 1

    def expected(days: int) -> dict:
        start_date = last_date - timedelta(days=days - 1)
        window = date_agg[(date_agg["date"] >= start_date) & (date_agg["date"] <= last_date)]
        revenue = float(window["total_revenue"].sum())
        profit_without_fixed = float(window["contribution_profit"].sum())
        profit_with_fixed = float(window["net_profit"].sum())
        orders = float(window["unique_orders"].sum())
        return {
            "revenue": revenue,
            "profit": profit_without_fixed,
            "company_profit": profit_with_fixed,
            "orders": orders,
            "post_margin": (profit_without_fixed / revenue * 100) if revenue > 0 else 0.0,
            "company_margin": (profit_with_fixed / revenue * 100) if revenue > 0 else 0.0,
        }

    def assert_close(actual: float, exp: float, label: str) -> None:
        assert math.isclose(actual, exp, rel_tol=1e-9, abs_tol=1e-9), f"{label}: {actual} != {exp}"

    for window_key, days in {"daily": 1, "weekly": 7, "monthly": 30, "all_time": all_time_days}.items():
        current = payload["windows"][window_key]
        metrics = current["metrics"]
        secondary = current["secondary_metrics"]
        exp = expected(days)

        assert_close(float(metrics["revenue"]), exp["revenue"], f"{window_key} revenue")
        assert_close(float(metrics["profit"]), exp["profit"], f"{window_key} profit_without_fixed")
        assert_close(float(secondary["company_margin_with_fixed"]), exp["company_profit"], f"{window_key} company_profit_with_fixed")
        assert_close(float(metrics["orders"]), exp["orders"], f"{window_key} orders")
        assert_close(float(metrics["post_ad_margin"]), exp["post_margin"], f"{window_key} post_ad_margin")
        assert_close(
            float(metrics["company_margin_with_fixed"]),
            exp["company_margin"],
            f"{window_key} company_margin_with_fixed",
        )

    assert not math.isclose(
        float(payload["windows"]["monthly"]["metrics"]["profit"]),
        float(payload["windows"]["monthly"]["secondary_metrics"]["company_margin_with_fixed"]),
        rel_tol=1e-9,
        abs_tol=1e-9,
    ), payload["windows"]["monthly"]


def main() -> int:
    exporter = make_exporter()
    assert_data_assertions_ok(exporter)
    assert_refund_registry_failure(exporter)
    assert_geo_warning(exporter)
    assert_margin_stability(exporter)
    assert_product_expense_coverage(exporter)
    assert_cfo_kpi_layer_invariants()
    print("reporting_qa_smoke.py: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
