#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import sys

import pandas as pd


ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from export_orders import BizniWebExporter


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


def main() -> int:
    exporter = make_exporter()
    assert_data_assertions_ok(exporter)
    assert_refund_registry_failure(exporter)
    assert_geo_warning(exporter)
    assert_margin_stability(exporter)
    print("reporting_qa_smoke.py: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
