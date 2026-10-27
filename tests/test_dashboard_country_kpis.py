"""Exercise the generated executive deck with real JavaScript rendering."""

from datetime import datetime
from html.parser import HTMLParser
import json
import shutil
import subprocess
import unittest

import pandas as pd

from dashboard_modern import (
    _kpis,
    extract_embedded_dashboard_payload,
    generate_modern_dashboard,
)


METRICS = (
    "revenue", "profit", "orders", "aov", "cac", "roas",
    "pre_ad_contribution_margin", "post_ad_margin", "company_margin_with_fixed",
)
WINDOWS = ("daily", "weekly", "monthly", "all_time")
NODE = shutil.which("node")


def cfo_payload(multiplier=1):
    windows = {}
    comparisons = {}
    for index, window in enumerate(WINDOWS, 1):
        revenue = 100 * index * multiplier
        values = dict(zip(METRICS, (revenue, 20, 10, revenue / 10, 5, 2, 60, 20, 10)))
        windows[window] = {
            "metrics": values,
            "secondary_metrics": {"company_margin_with_fixed": 10 * index * multiplier},
            "trend": {
                "label_en": "14d trend" if window == "daily" else window + " trend",
                "label_sk": "Trend",
                "dates": ["2026-10-25", "2026-10-26"],
                "metrics": {key: [value / 2, value] for key, value in values.items()},
            },
        }
        comparison_key = {
            "daily": "vs_prev_day", "weekly": "vs_prev_7d",
            "monthly": "vs_prev_30d", "all_time": "vs_prev_span",
        }[window]
        comparisons[window] = {key: {comparison_key: 10.0, "vs_year": None} for key in METRICS}
    return {
        "default_window": "daily",
        "metric_defs": [{"key": key, "direction": "down" if key == "cac" else "up"} for key in METRICS],
        "windows": windows,
        "comparisons": comparisons,
    }


def report_html(payload):
    daily = pd.DataFrame([{
        "date": pd.Timestamp("2026-10-26"), "total_revenue": 100.0,
        "net_profit": 10.0, "contribution_profit": 20.0, "unique_orders": 10,
        "fb_ads_spend": 25.0, "google_ads_spend": 0.0,
        "total_items": 10, "product_expense": 50.0, "total_cost": 90.0,
        "pre_ad_contribution_profit": 45.0,
    }])
    return generate_modern_dashboard(
        daily, pd.DataFrame([{"item_label": "Test", "total_quantity": 10, "total_revenue": 100.0}]),
        datetime(2026, 10, 26), datetime(2026, 10, 26),
        report_title="VEVO country KPI test", cfo_kpi_payload=payload,
        source_health={"project": "vevo"},
    )


class DeckParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.buttons = []
        self.country_sections = []

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        if tag == "button" and "window-btn" in attributes.get("class", "").split():
            self.buttons.append(attributes.get("data-window"))
        if tag == "section" and "country-kpi-deck" in attributes.get("class", "").split():
            self.country_sections.append(attributes.get("data-kpi-country"))


def rendered_windows(html):
    """Run exactly the emitted formatter/deck functions, without a browser/server."""
    formats = html[html.index("const KPI_TYPES ="):html.index("function loadStoredJson(")]
    renderer = html[html.index("function compClass("):html.index("function gradient(")]
    data = extract_embedded_dashboard_payload(html)
    harness = r"""
const fs = require('fs');
const vm = require('vm');
const input = JSON.parse(fs.readFileSync(0, 'utf8'));
const grids = {kpiGrid:{innerHTML:''}};
for (const country of input.data.kpis.country_order || []) grids['countryKpiGrid-' + country] = {innerHTML:''};
const buttons = ['daily','weekly','monthly','all_time'].map(window => ({dataset:{window},active:false,classList:{toggle(_name,value){ this.active=value; }}}));
const context = vm.createContext({DATA:input.data,Intl,language:'en',
  document:{getElementById:id => grids[id] || null, querySelectorAll:() => buttons},
});
vm.runInContext('function lang(){return language;}' + input.formats + input.renderer, context);
const result = {};
for (const window of ['daily','weekly','monthly','all_time']) {
  vm.runInContext('renderKpis(' + JSON.stringify(window) + ')', context);
  result[window] = Object.fromEntries(Object.entries(grids).map(([id,grid]) => [id,grid.innerHTML]));
}
result.gapSparkline = vm.runInContext('sparklineSvg([10,null,20], "up")', context);
result.noTrend = vm.runInContext('sparklineSvg([null,null], "up")', context);
process.stdout.write(JSON.stringify(result));
"""
    completed = subprocess.run(
        [NODE, "-e", harness], input=json.dumps({"data": data, "formats": formats, "renderer": renderer}),
        text=True, encoding="utf-8", capture_output=True, timeout=20, check=True,
    )
    return json.loads(completed.stdout)


class DashboardCountryKpiTests(unittest.TestCase):
    def setUp(self):
        self.source = cfo_payload(10)
        self.source["countries"] = {country: cfo_payload(index) for index, country in enumerate(("sk", "cz", "hu"), 1)}
        self.source["country_order"] = ["sk", "cz", "hu"]

    def test_serializer_is_bounded_and_preserves_the_original_summary(self):
        original = _kpis(cfo_payload(10))
        self.source["countries"]["sk"]["countries"] = self.source["countries"]
        self.source["countries"]["sk"]["private_token"] = "must-not-be-serialized"
        self.source["countries"]["unknown"] = cfo_payload(999)
        self.source["country_order"] = ["sk", "sk", "unknown", "cz", "hu"]
        serialized = _kpis(self.source)
        self.assertEqual(original, {key: value for key, value in serialized.items() if key not in {"countries", "country_order"}})
        self.assertEqual(["sk", "cz", "hu"], serialized["country_order"])
        self.assertNotIn("countries", serialized["countries"]["sk"])
        self.assertNotIn("private_token", serialized["countries"]["sk"])
        self.assertEqual("Česko", serialized["countries"]["cz"]["label_sk"])

    def test_country_sections_immediately_follow_summary_with_one_shared_control(self):
        html = report_html(self.source)
        parser = DeckParser()
        parser.feed(html)
        self.assertEqual(list(WINDOWS), parser.buttons)
        self.assertEqual(["sk", "cz", "hu"], parser.country_sections)
        self.assertLess(html.index('id="kpiGrid"'), html.index('id="countryKpiHeading-sk"'))
        self.assertLess(html.index('id="countryKpiGrid-hu"'), html.index('id="sales"'))
        for country in ("sk", "cz", "hu"):
            self.assertIn(f'aria-labelledby="countryKpiHeading-{country}"', html)
        self.assertIn("Slovensko", html)
        self.assertIn("Maďarsko", html)

    def test_legacy_payload_does_not_add_country_sections(self):
        html = report_html(cfo_payload())
        parser = DeckParser()
        parser.feed(html)
        self.assertEqual([], parser.country_sections)
        self.assertNotIn("countries", extract_embedded_dashboard_payload(html)["kpis"])

    def test_country_labels_and_embedded_json_are_escaped(self):
        dangerous = '</script><img src=x onerror="alert(1)">'
        self.source["countries"]["sk"]["label_en"] = dangerous
        html = report_html(self.source)
        self.assertNotIn(dangerous, html)
        self.assertIn("&lt;/script&gt;&lt;img", html)
        self.assertEqual(dangerous, extract_embedded_dashboard_payload(html)["kpis"]["countries"]["sk"]["label_en"])

    def test_country_provenance_retains_native_residuals_without_reallocation(self):
        self.source["country_metadata"] = {
            "basis": "canonical_order_financials_and_measured_daily_country_ads",
            "date_from": "2025-05-03", "date_to": "2026-10-26",
            "calendar_complete": True, "calendar_days": 542,
            "provider_status": {"facebook_ads": "ok", "google_ads": "unavailable"},
            "provider_country_coverage": {
                "facebook_ads": {"status": "partial", "unknown_spend_eur": 1.2, "unallocated_spend_eur": 0.0},
                "google_ads": {"status": "unavailable", "unknown_spend_eur": None, "unallocated_spend_eur": None},
            },
            "provider_daily_totals": {"facebook_ads": {"sk": 100.0, "cz": 10.0, "hu": 20.0, "other": 0.3, "unknown": 1.2}},
            "daily_vs_period": {
                "facebook_ads": {"daily_minus_period_by_country_eur": {"sk": 0.120205, "cz": -0.044675, "hu": -0.075525, "unknown": -0.000002}, "daily_minus_period_total_eur": 0.000003},
                "google_ads": None,
            },
            "company_minus_displayed_countries": {"total_revenue": 5.0, "fb_ads_spend": -0.000003, "google_ads_spend": None, "fixed_daily_cost": 70.0},
            "residual_basis": "other_or_unknown_markets_unallocated_ads_and_orderless_day_fixed_costs",
            "residual_reallocated": False,
        }
        self.source["countries"]["hu"].update(status="unavailable", metadata={
            "date_from": "2025-05-03", "date_to": "2026-10-26", "calendar_complete": True,
            "calendar_days": 542, "market_basis": "delivery_country_with_invoice_fallback",
            "fixed_cost_basis": "existing_daily_order_share", "customer_basis": "global_first_purchase_date",
            "advertising_reallocated": False, "unavailable_ad_sources": ["google_ads"],
            "creditnote_fulfillment_included": True,
            "creditnote_fulfillment_source": "same_selected_company_events",
        })
        serialized = _kpis(self.source)
        self.assertEqual(self.source["country_metadata"], serialized["country_metadata"])
        self.assertEqual("unavailable", serialized["countries"]["hu"]["status"])
        self.assertEqual(self.source["countries"]["hu"]["metadata"], serialized["countries"]["hu"]["metadata"])
        self.assertFalse(serialized["country_metadata"]["residual_reallocated"])

    def test_country_metadata_rejects_unexpected_keys_credentials_and_nonfinite_values(self):
        self.source["country_metadata"] = {
            "secret_token": "private-value", "basis": "private-value", "date_from": "private-value",
            "date_to": "2026-10-26", "calendar_complete": "private-value", "calendar_days": True,
            "provider_status": {"facebook_ads": "ok", "google_ads": "private-value", "private-value": "ok"},
            "provider_country_coverage": {"facebook_ads": {
                "status": "partial", "unknown_spend_eur": float("nan"), "unallocated_spend_eur": 0,
                "error": "private-value", "access_token": "private-value",
            }},
            "provider_daily_totals": {"facebook_ads": {
                "sk": 1.0, "cz": float("inf"), "hu": float("nan"), "ro": None, "si": True,
                "hr": "private-value", "other": 0.1, "unallocated": 0, "secret_token": 100,
            }, "private-value": {"sk": 1.0}},
            "daily_vs_period": {"facebook_ads": {
                "daily_minus_period_total_eur": float("inf"), "error": "private-value",
                "daily_minus_period_by_country_eur": {"sk": -0.1, "cz": float("nan"), "private-value": 1},
            }},
            "company_minus_displayed_countries": {
                "total_revenue": 0.1, "product_expense": None, "google_ads_spend": float("inf"),
                "fb_ads_spend": True, "secret_token": 1,
            },
        }
        self.source["countries"]["sk"].update(status="private-value", metadata={
            "error": "private-value", "date_to": "2026-10-26",
            "unavailable_ad_sources": ["google_ads", "private-value", {"secret_token": "private-value"}],
        })
        serialized = _kpis(self.source)
        encoded = json.dumps(serialized, allow_nan=False)
        self.assertNotIn("private-value", encoded)
        self.assertNotIn("secret_token", encoded)
        self.assertNotIn("access_token", encoded)
        self.assertNotIn("status", serialized["countries"]["sk"])
        self.assertEqual({"sk": 1.0, "other": 0.1, "unallocated": 0}, serialized["country_metadata"]["provider_daily_totals"]["facebook_ads"])
        self.assertEqual({"total_revenue": 0.1, "product_expense": None}, serialized["country_metadata"]["company_minus_displayed_countries"])
        self.assertEqual({"sk": -0.1}, serialized["country_metadata"]["daily_vs_period"]["facebook_ads"]["daily_minus_period_by_country_eur"])

    def test_unavailable_country_has_one_honest_bilingual_notice_and_one_shared_footnote(self):
        self.source["countries"]["sk"]["status"] = "ok"
        self.source["countries"]["cz"]["status"] = "ok"
        self.source["countries"]["hu"]["status"] = "unavailable"
        html = report_html(self.source)
        self.assertEqual(1, html.count('<p class="country-kpi-note country-kpi-unavailable" role="status">'))
        country_section = html.split('data-kpi-country="hu"', 1)[1].split('</section>', 1)[0]
        self.assertIn("Advertising data is unavailable.", country_section)
        self.assertIn("Reklamné údaje nie sú dostupné.", country_section)
        self.assertEqual(1, html.count('<p class="country-kpi-note country-kpi-footnote">'))
        self.assertIn("Company totals may also include other countries and unallocated amounts.", html)
        legacy = report_html(cfo_payload())
        self.assertNotIn('<p class="country-kpi-note country-kpi-unavailable"', legacy)
        self.assertNotIn('<p class="country-kpi-note country-kpi-footnote"', legacy)

    def test_invalid_country_order_cannot_break_an_otherwise_valid_report(self):
        self.source["country_order"] = 7
        self.assertEqual(["sk", "cz", "hu"], _kpis(self.source)["country_order"])

    @unittest.skipUnless(NODE, "Node.js is required to execute the emitted browser functions")
    def test_shared_renderer_switches_all_twenty_seven_country_cards_in_all_four_windows(self):
        rendered = rendered_windows(report_html(self.source))
        for index, window in enumerate(WINDOWS, 1):
            for multiplier, country in enumerate(("sk", "cz", "hu"), 1):
                html = rendered[window][f"countryKpiGrid-{country}"]
                self.assertEqual(9, html.count('<article class="kpi-card">'))
                self.assertIn(f"€{100 * index * multiplier:,.2f}", html)
                self.assertIn(f"€{10 * index * multiplier:,.2f}", html)
                self.assertIn("kpi-trend", html)
                self.assertIn("compare-row", html)
            self.assertEqual(9, rendered[window]["kpiGrid"].count('<article class="kpi-card">'))
            self.assertIn(f"€{1000 * index:,.2f}", rendered[window]["kpiGrid"])

    @unittest.skipUnless(NODE, "Node.js is required to execute the emitted browser functions")
    def test_unavailable_economics_display_na_and_do_not_create_zero_trend_points(self):
        hu = self.source["countries"]["hu"]
        for window in WINDOWS:
            for key in ("profit", "cac", "roas", "post_ad_margin", "company_margin_with_fixed"):
                hu["windows"][window]["metrics"][key] = None
                hu["windows"][window]["trend"]["metrics"][key] = [None, None]
                for comparison in hu["comparisons"][window][key]:
                    hu["comparisons"][window][key][comparison] = None
            hu["windows"][window]["secondary_metrics"]["company_margin_with_fixed"] = None
        rendered = rendered_windows(report_html(self.source))
        html = rendered["daily"]["countryKpiGrid-hu"]
        self.assertEqual(5, html.count('<div class="kpi-value">N/A</div>'))
        self.assertNotIn('<div class="kpi-secondary">', html)
        self.assertEqual("", rendered["noTrend"])
        # The missing middle day keeps its X position and breaks the path.
        line = rendered["gapSparkline"].split('<path d="')[2].split('"')[0]
        self.assertEqual(2, line.count("M"))
        self.assertNotIn("L", line)
        self.assertIn("240.00", line)

    @unittest.skipUnless(NODE, "Node.js is required to execute the emitted browser functions")
    def test_country_trend_label_cannot_insert_html_during_client_rendering(self):
        self.source["countries"]["sk"]["windows"]["daily"]["trend"]["label_en"] = '<img src=x onerror="boom">'
        html = rendered_windows(report_html(self.source))["daily"]["countryKpiGrid-sk"]
        self.assertNotIn("<img", html)
        self.assertIn("&lt;img", html)


if __name__ == "__main__":
    unittest.main()
