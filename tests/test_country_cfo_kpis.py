import copy
from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import pandas as pd

from reporting_core.cfo_kpis import build_cfo_kpi_payload
from reporting_core.country_cfo_kpis import build_country_cfo_payload


def source(provider, daily, date_to="2026-10-03"):
    daily = {**{day.strftime("%Y-%m-%d"): {} for day in pd.date_range("2026-10-01", date_to)}, **daily}
    countries = {}
    for values in daily.values():
        for key, value in values.items():
            countries[key] = countries.get(key, Decimal(0)) + Decimal(str(value))
    return {
        "status": "ok", "currency": "EUR", "date_from": "2026-10-01", "date_to": date_to,
        "basis": "meta_country_breakdown_daily" if provider == "facebook_ads" else "google_user_location_daily",
        "daily_spend_by_country": daily,
        "spend_by_country": {key: float(value) for key, value in countries.items()},
        "total_spend": float(sum(countries.values(), Decimal(0))),
    }


def inputs():
    orders = pd.DataFrame([
        dict(order_num="SK-A", purchase_date="2026-10-01", country="sk", customer_email="same@example.test",
             order_revenue_net=100, product_cost=30, packaging_cost=1, shipping_net_cost=2,
             allocated_fixed_overhead=35, order_credit_net_adjustment=0, total_items_in_order=2),
        dict(order_num="CZ-B", purchase_date="2026-10-01", country="cz", customer_email="new@example.test",
             order_revenue_net=80, product_cost=20, packaging_cost=1, shipping_net_cost=2,
             allocated_fixed_overhead=35, order_credit_net_adjustment=-20, total_items_in_order=1),
        dict(order_num="CZ-C", purchase_date="2026-10-02", country="cz", customer_email="same@example.test",
             order_revenue_net=50, product_cost=10, packaging_cost=1, shipping_net_cost=2,
             allocated_fixed_overhead=70, order_credit_net_adjustment=0, total_items_in_order=1),
    ])
    daily = pd.DataFrame([
        dict(date="2026-10-01", total_revenue=180, product_expense=50, packaging_cost=2,
             shipping_net_cost=4, unique_orders=2, fixed_daily_cost=70, fb_ads_spend=16,
             google_ads_spend=4, contribution_profit=104, pre_ad_contribution_profit=124,
             revenue_credit_adjustment=-20),
        dict(date="2026-10-02", total_revenue=50, product_expense=10, packaging_cost=1,
             shipping_net_cost=2, unique_orders=1, fixed_daily_cost=70, fb_ads_spend=15,
             google_ads_spend=1, contribution_profit=21, pre_ad_contribution_profit=37,
             revenue_credit_adjustment=0),
        dict(date="2026-10-03", total_revenue=0, product_expense=0, packaging_cost=0,
             shipping_net_cost=0, unique_orders=0, fixed_daily_cost=70, fb_ads_spend=11,
             google_ads_spend=1, contribution_profit=-12, pre_ad_contribution_profit=0,
             revenue_credit_adjustment=0),
    ])
    ads = {
        "facebook_ads": source("facebook_ads", {
            "2026-10-01": {"sk": 10, "cz": 5, "unknown": 1},
            "2026-10-02": {"sk": 2, "cz": 10, "hu": 3},
            "2026-10-03": {"cz": 4, "hu": 7},
        }),
        "google_ads": source("google_ads", {
            "2026-10-01": {"sk": 1, "cz": 2, "unallocated": 1},
            "2026-10-02": {"sk": 1}, "2026-10-03": {"hu": 1},
        }),
    }
    return daily, orders, ads


class CountryCfoKpiTests(unittest.TestCase):
    def test_canonical_orders_credit_once_and_shared_fixed_costs(self):
        daily, orders, ads = inputs()
        result = build_country_cfo_payload(daily, orders, ads)
        self.assertEqual(["sk", "cz", "hu"], result["country_order"])
        for key in result["country_order"]:
            self.assertEqual({"daily", "weekly", "monthly", "all_time"}, set(result["countries"][key]["windows"]))
            self.assertEqual(9, len(result["countries"][key]["metric_defs"]))
        for period in ("weekly", "monthly", "all_time"):
            sk = result["countries"]["sk"]["windows"][period]
            cz = result["countries"]["cz"]["windows"][period]
            hu = result["countries"]["hu"]["windows"][period]
            self.assertEqual(1, sk["metrics"]["orders"])
            self.assertEqual(2, cz["metrics"]["orders"])
            self.assertEqual(130, cz["metrics"]["revenue"])
            self.assertEqual(-20, cz["secondary_metrics"]["revenue_credit_adjustment"])
            self.assertEqual(53, sk["metrics"]["profit"])
            self.assertEqual(73, cz["metrics"]["profit"])
            self.assertEqual(18, sk["secondary_metrics"]["company_margin_with_fixed"])
            self.assertEqual(-32, cz["secondary_metrics"]["company_margin_with_fixed"])
            self.assertEqual(-11, hu["secondary_metrics"]["company_margin_with_fixed"])
        residual = result["country_metadata"]["company_minus_displayed_countries"]
        self.assertEqual(70, residual["fixed_daily_cost"])
        self.assertEqual(1, residual["fb_ads_spend"])
        self.assertEqual(1, residual["google_ads_spend"])
        self.assertFalse(result["country_metadata"]["residual_reallocated"])

    def test_last_day_and_trend_keep_orderless_market_ad_spend(self):
        daily, orders, ads = inputs()
        hu = build_country_cfo_payload(daily, orders, ads)["countries"]["hu"]
        self.assertEqual(-8, hu["windows"]["daily"]["metrics"]["profit"])
        self.assertEqual(0, hu["windows"]["daily"]["metrics"]["orders"])
        self.assertEqual(["2026-10-01", "2026-10-02", "2026-10-03"], hu["windows"]["daily"]["trend"]["dates"])
        self.assertEqual([0, -3, -8], hu["windows"]["daily"]["trend"]["metrics"]["profit"])
        self.assertAlmostEqual(-166.6666666667, hu["comparisons"]["daily"]["profit"]["vs_prev_day"])

    def test_cac_uses_first_customer_date_across_markets(self):
        daily, orders, ads = inputs()
        two_day_ads = {}
        for key, value in ads.items():
            valid = source(key, {day: row for day, row in value["daily_spend_by_country"].items() if day <= "2026-10-02"}, date_to="2026-10-02")
            two_day_ads[key] = valid
        cz = build_country_cfo_payload(daily.iloc[:2], orders, two_day_ads)["countries"]["cz"]
        self.assertIsNone(cz["windows"]["daily"]["metrics"]["cac"])
        self.assertEqual(17, cz["windows"]["weekly"]["metrics"]["cac"])

    def test_original_global_first_purchase_date_survives_country_filter(self):
        daily, orders, ads = inputs()
        orders["customer_first_purchase_date"] = "2025-01-01"
        result = build_country_cfo_payload(daily, orders, ads)
        self.assertIsNone(result["countries"]["sk"]["windows"]["all_time"]["metrics"]["cac"])
        self.assertIsNone(result["countries"]["cz"]["windows"]["all_time"]["metrics"]["cac"])

    def test_fulfillment_cost_for_excluded_credited_order_has_country_but_no_sale(self):
        daily, orders, ads = inputs()
        fulfillment = pd.DataFrame([dict(date="2026-10-02", country="hu", creditnote_packaging_cost=1, creditnote_shipping_net_cost=2)])
        daily.loc[1, "packaging_cost"] += 1
        daily.loc[1, "shipping_net_cost"] += 2
        result = build_country_cfo_payload(daily, orders, ads, fulfillment_rows=fulfillment)
        hu = result["countries"]["hu"]["windows"]["all_time"]
        self.assertEqual(0, hu["metrics"]["orders"])
        self.assertEqual(0, hu["metrics"]["revenue"])
        self.assertEqual(-14, hu["metrics"]["profit"])
        residual = result["country_metadata"]["company_minus_displayed_countries"]
        self.assertEqual(0, residual["packaging_cost"])
        self.assertEqual(0, residual["shipping_net_cost"])
        self.assertTrue(result["countries"]["hu"]["metadata"]["creditnote_fulfillment_included"])

    def test_unprovided_fulfillment_source_remains_an_explicit_company_residual(self):
        daily, orders, ads = inputs()
        daily.loc[1, "packaging_cost"] += 1
        daily.loc[1, "shipping_net_cost"] += 2
        result = build_country_cfo_payload(daily, orders, ads)
        self.assertEqual(1, result["country_metadata"]["company_minus_displayed_countries"]["packaging_cost"])
        self.assertEqual(2, result["country_metadata"]["company_minus_displayed_countries"]["shipping_net_cost"])
        self.assertFalse(result["countries"]["hu"]["metadata"]["creditnote_fulfillment_included"])

    def test_unavailable_or_invalid_source_masks_only_ad_dependent_values(self):
        daily, orders, good = inputs()
        bad_sources = [
            {"status": "unavailable"},
            {**good["facebook_ads"], "currency": "USD"},
            {**good["facebook_ads"], "date_to": "2026-10-04"},
            {**good["facebook_ads"], "total_spend": 0},
            {**good["facebook_ads"], "daily_spend_by_country": {"2026-10-04": {"sk": 1}}},
            {**good["facebook_ads"], "daily_spend_by_country": {"2026-10-01": {"sk": float("nan")}}},
        ]
        for invalid in bad_sources:
            with self.subTest(source=invalid):
                ads = {**good, "facebook_ads": invalid}
                result = build_country_cfo_payload(daily, orders, ads)
                sk = result["countries"]["sk"]
                self.assertEqual("unavailable", sk["status"])
                self.assertEqual(100, sk["windows"]["all_time"]["metrics"]["revenue"])
                self.assertEqual(1, sk["windows"]["all_time"]["metrics"]["orders"])
                self.assertEqual(100, sk["windows"]["all_time"]["metrics"]["aov"])
                self.assertEqual(67, sk["windows"]["all_time"]["metrics"]["pre_ad_contribution_margin"])
                for window in sk["windows"].values():
                    for name in ("profit", "cac", "roas", "post_ad_margin", "company_margin_with_fixed"):
                        self.assertIsNone(window["metrics"][name])
                        self.assertTrue(all(value is None for value in window["trend"]["metrics"][name]))
                    self.assertIsNone(window["secondary_metrics"]["company_margin_with_fixed"])
                self.assertTrue(all(value is None for value in sk["comparisons"]["daily"]["profit"].values()))

    def test_successful_zero_spend_is_distinct_from_missing_source(self):
        daily, orders, _ads = inputs()
        ads = {key: source(key, {}) for key in ("facebook_ads", "google_ads")}
        result = build_country_cfo_payload(daily, orders, ads)
        sk = result["countries"]["sk"]
        self.assertEqual("ok", sk["status"])
        self.assertEqual(67, sk["windows"]["all_time"]["metrics"]["profit"])
        self.assertIsNone(sk["windows"]["all_time"]["metrics"]["cac"])

    def test_missing_explicit_zero_day_is_not_accepted_as_complete_source(self):
        daily, orders, _ads = inputs()
        ads = {key: source(key, {}) for key in ("facebook_ads", "google_ads")}
        del ads["facebook_ads"]["daily_spend_by_country"]["2026-10-02"]
        sk = build_country_cfo_payload(daily, orders, ads)["countries"]["sk"]
        self.assertEqual("unavailable", sk["status"])
        self.assertIsNone(sk["windows"]["all_time"]["metrics"]["profit"])
        self.assertEqual(100, sk["windows"]["all_time"]["metrics"]["revenue"])

    def test_sparse_company_calendar_is_expanded_to_the_same_global_endpoints(self):
        daily, orders, ads = inputs()
        orders = orders[orders["purchase_date"] != "2026-10-02"]
        result = build_country_cfo_payload(daily.iloc[[0, 2]], orders, ads)
        hu = result["countries"]["hu"]
        self.assertEqual("ok", hu["status"])
        self.assertEqual(["2026-10-01", "2026-10-02", "2026-10-03"], hu["windows"]["daily"]["trend"]["dates"])
        self.assertEqual(3, hu["metadata"]["calendar_days"])
        self.assertTrue(hu["metadata"]["calendar_complete"])
        self.assertEqual(-11, hu["windows"]["all_time"]["metrics"]["profit"])

    def test_unknown_and_unallocated_coverage_stay_explicit(self):
        daily, orders, ads = inputs()
        result = build_country_cfo_payload(daily, orders, ads)
        coverage = result["country_metadata"]["provider_country_coverage"]
        self.assertEqual("partial", coverage["facebook_ads"]["status"])
        self.assertEqual(1, coverage["facebook_ads"]["unknown_spend_eur"])
        self.assertEqual("partial", coverage["google_ads"]["status"])
        self.assertEqual(1, coverage["google_ads"]["unallocated_spend_eur"])
        self.assertEqual(coverage, result["countries"]["cz"]["metadata"]["provider_country_coverage"])

    def test_daily_period_difference_stays_visible_and_does_not_rescale(self):
        daily, orders, ads = inputs()
        old = {"facebook_ads": {"status": "ok", "spend_by_country": {"sk": 13, "cz": 19, "hu": 10}, "total_spend": 42}}
        result = build_country_cfo_payload(daily, orders, ads, period_country_ads=old)
        delta = result["country_metadata"]["daily_vs_period"]["facebook_ads"]
        self.assertEqual(-1, delta["daily_minus_period_by_country_eur"]["sk"])
        self.assertEqual(1, delta["daily_minus_period_by_country_eur"]["unknown"])
        self.assertEqual(53, result["countries"]["sk"]["windows"]["all_time"]["metrics"]["profit"])

    def test_global_payload_inputs_and_other_country_residual_are_unchanged(self):
        daily, orders, ads = inputs()
        daily.loc[0, "unique_orders"] += 1
        before_daily, before_orders, before_ads = daily.copy(deep=True), orders.copy(deep=True), copy.deepcopy(ads)
        before_global = build_cfo_kpi_payload(daily, orders, fixed_daily_cost_eur=70)
        extra = orders.iloc[[0]].assign(order_num="OTHER", country="si", allocated_fixed_overhead=0,
                                       order_revenue_net=0, product_cost=0, packaging_cost=0, shipping_net_cost=0)
        result = build_country_cfo_payload(daily, pd.concat([orders, extra], ignore_index=True), ads)
        self.assertEqual(["sk", "cz", "hu"], list(result["countries"]))
        self.assertEqual(1, result["country_metadata"]["company_minus_displayed_countries"]["unique_orders"])
        pd.testing.assert_frame_equal(daily, before_daily)
        pd.testing.assert_frame_equal(orders, before_orders)
        self.assertEqual(ads, before_ads)
        self.assertEqual(before_global, build_cfo_kpi_payload(daily, orders, fixed_daily_cost_eur=70))

    def test_conflicting_order_rows_and_out_of_scope_orders_are_rejected(self):
        daily, orders, ads = inputs()
        for extra in (orders.iloc[[0]].assign(order_revenue_net=999), orders.iloc[[0]].assign(order_num="LATE", purchase_date="2026-11-01")):
            with self.assertRaises(ValueError):
                build_country_cfo_payload(daily, pd.concat([orders, extra], ignore_index=True), ads)


class CountryCfoExporterAdapterTests(unittest.TestCase):
    def test_adapter_fetches_each_daily_source_once_and_preserves_canonical_credit(self):
        from export_orders import BizniWebExporter

        daily, orders, ads = inputs()
        raw = pd.DataFrame([dict(order_num="CZ-B", order_credit_net_adjustment=-20)] * 2)
        canonical = orders[orders["order_num"] == "CZ-B"].rename(columns={"country": "geo_country"})
        adapter = SimpleNamespace(
            fb_client=SimpleNamespace(get_country_daily_spend=Mock(return_value=ads["facebook_ads"])),
            google_ads_client=SimpleNamespace(get_country_daily_spend=Mock(return_value=ads["google_ads"])),
            _build_growth_order_item_frames=Mock(return_value=(canonical, pd.DataFrame(), "order_revenue_net")),
            _build_order_geo_frame=Mock(return_value=canonical),
            _normalize_country_key_label=lambda value: (str(value).lower(), str(value)),
        )
        result = BizniWebExporter._build_country_cfo_decks(adapter, raw, daily, datetime(2026, 10, 1), datetime(2026, 10, 3), None)
        cz = result["countries"]["cz"]["windows"]["all_time"]
        self.assertEqual(80, cz["metrics"]["revenue"])
        self.assertEqual(-20, cz["secondary_metrics"]["revenue_credit_adjustment"])
        self.assertEqual(1, cz["metrics"]["orders"])
        adapter.fb_client.get_country_daily_spend.assert_called_once()
        adapter.google_ads_client.get_country_daily_spend.assert_called_once()
        adapter._build_growth_order_item_frames.assert_called_once_with(raw, require_customer_email=False)

    def test_selected_fulfillment_events_are_retained_without_changing_company_rows(self):
        from export_orders import BizniWebExporter

        adapter = SimpleNamespace(
            excluded_status_orders=[dict(order_num="CREDIT", delivery_address={"country": ""}, invoice_address={"country": "CZ"})],
            _creditnote_fulfillment_cost_settings=lambda: {"enabled": True},
            _order_purchase_datetime=lambda order: datetime(2026, 10, 2),
            _fetch_creditnote_order_nums=lambda: {"CREDIT"},
            _order_was_sent_before_creditnote=lambda order, order_num: True,
            _creditnote_shipping_cost_per_order=lambda: 2,
            _normalize_country_key_label=lambda value: (str(value).lower(), str(value)),
        )
        with patch("export_orders.PACKAGING_COST_PER_ORDER", 1):
            frame = BizniWebExporter._build_creditnote_fulfillment_costs_by_date(adapter, datetime(2026, 10, 1), datetime(2026, 10, 3))
        self.assertEqual(3, frame["creditnote_fulfillment_cost"].sum())
        self.assertNotIn("country", frame)
        event = adapter._country_creditnote_fulfillment["rows"][0]
        self.assertEqual("cz", event["country"])
        self.assertEqual(3, event["creditnote_fulfillment_cost"])


if __name__ == "__main__":
    unittest.main()
