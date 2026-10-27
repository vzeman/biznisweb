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
