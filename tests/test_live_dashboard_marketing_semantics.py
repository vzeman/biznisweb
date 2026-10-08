"""Synthetic browser-surface regression; no server, provider or private data."""
import json
import shutil
import subprocess
import unittest
from unittest.mock import patch

from live_dashboard_server import build_live_dashboard_html, build_roy_operations_dashboard_html


def live_html():
    with patch("live_dashboard_server.remote_dashboard_origin", return_value=None):
        return build_live_dashboard_html(["vevo", "roy"], "vevo", "7d")


def run_js(test, source):
    result = subprocess.run([shutil.which("node"), "-"], input=source, text=True,
                            encoding="utf-8", capture_output=True, timeout=20, check=False)
    test.assertEqual(0, result.returncode, result.stderr)
    test.assertIn("SEMANTICS_OK", result.stdout)


class LiveDashboardMarketingSemanticsTests(unittest.TestCase):
    def test_built_in_labels_distinguish_shop_ratio_from_attribution(self):
        html = live_html()
        self.assertIn("Observed advertising comparisons", html)
        self.assertIn("including organic and repeat sales", html)
        self.assertIn("New-customer CAC", html)
        for unsupported in ("label:'Blended ROAS'", "label:'ROAS'", "label:'Incremental ROAS'",
                            "<th>Inc ROAS</th>", "Primary ad-scaling profit view", "Net revenue lift per day"):
            self.assertNotIn(unsupported, html)

    @unittest.skipUnless(shutil.which("node"), "Node.js required for embedded JavaScript regression")
    def test_old_causal_payload_is_rendered_as_observation_without_changing_money(self):
        script = live_html().split("    <script>\n", 1)[1].split("    </script>", 1)[0]
        prefix = script[:script.index("      function renderError")]
        regression = r'''
const assert = require('node:assert/strict');
const nodes = {};
const document = {getElementById(id) {
  return nodes[id] ||= {textContent: id === 'live-dashboard-bootstrap' ? '{"projects":["vevo"],"project":"vevo"}' : '', innerHTML:'', hidden:false};
}};
__CODE__
const primary = {key:'meta', label_en:'Selected comparison', verdict:'Scale', verdict_tone:'scale',
 verdict_reason_en:'Increase the budget now', confidence:'high', confidence_note_en:'Causal proof', decision_ready:true,
 incremental_total_ad_spend_per_day:10, incremental_revenue_per_day:50,
 incremental_profit_without_fixed_per_day:20, incremental_profit_with_fixed_per_day:18,
 incremental_roas:5, incremental_cac:4, break_even_cac:9};
const snapshot = {dashboard:{incrementality_primary:primary, incrementality_rows:[primary],
 series:{revenue:[100,200],orders:[1,2],fb_ads:[10,20],google_ads:[5,15],total_ads:[15,35],
 profit_without_fixed:[30,50],profit_with_fixed:[20,40],product_cost:[40,80],packaging:[1,2],shipping:[1,2],fixed:[10,10]},
 kpis:{default_window:'weekly',windows:{weekly:{metrics:{revenue:300,roas:6,cac:5},secondary_metrics:{}}}}}};
const before = JSON.stringify(snapshot);
renderSummary(snapshot); renderContext(snapshot); renderIncrementality(snapshot);
assert.equal(buildTotals(snapshot).blendedRoas,6);
assert.equal(buildTotals(snapshot).profitWithFixed,60);
assert.equal(JSON.stringify(snapshot),before);
const rendered = Object.values(nodes).map(n=>n.innerHTML+' '+n.textContent).join(' ');
assert.ok(rendered.includes('Net MER'));
assert.ok(rendered.includes('Observation only'));
assert.ok(rendered.includes('do not identify a causal advertising effect'));
for (const forbidden of ['Increase the budget now','Causal proof','>Scale<','badge scale','Incremental ROAS']) assert.ok(!rendered.includes(forbidden),forbidden);
assert.equal(observationalComparison(primary).incremental_roas,5);
assert.equal(observationalComparison(primary).decision_ready,false);
assert.equal(primary.verdict,'Scale');
const zeroSpend = {dashboard:{series:{revenue:[100],total_ads:[0]}}};
assert.equal(buildTotals(zeroSpend).blendedRoas,null);
renderSummary(zeroSpend); assert.ok(nodes.summaryGrid.innerHTML.includes('N/A'));
console.log('SEMANTICS_OK');
'''.replace("__CODE__", prefix)
        run_js(self, regression)

    @unittest.skipUnless(shutil.which("node"), "Node.js required for embedded JavaScript regression")
    def test_operations_old_metric_definition_cannot_restore_roas_label(self):
        html = build_roy_operations_dashboard_html("roy")
        self.assertIn("{ key:'roas', label_en:'Net MER' }", html)
        function = html[html.index("    function renderKpis() {"):html.index("    function renderAlerts(data)")]
        regression = r'''
const assert = require('node:assert/strict');
const nodes = {kpiMeta:{},kpiGrid:{}};
const el = id => nodes[id]; const safe = String; const text = String;
const renderKpiControls = () => {}; const comparisonText = () => 'N/A';
const sparkline = () => ''; const fmtMoney = String; const formatKpiValue = (_key,value) => String(value);
const kpiScope = 'monthly'; const metricDefsFallback = [];
const latestData = {executive_kpis:{metric_defs:[{key:'roas',label_en:'ROAS'},{key:'profit',label_en:'Post-ad profit'}]}};
const selectedKpiWindow = () => ({metrics:{roas:4,profit:25},label_en:'Month'});
__CODE__
renderKpis();
assert.ok(nodes.kpiGrid.innerHTML.includes('>Net MER<'));
assert.ok(nodes.kpiGrid.innerHTML.includes('>4<'));
assert.ok(nodes.kpiGrid.innerHTML.includes('>25<'));
assert.ok(nodes.kpiGrid.innerHTML.includes('not platform-attributed ROAS'));
assert.ok(!nodes.kpiGrid.innerHTML.includes('>ROAS<'));
console.log('SEMANTICS_OK');
'''.replace("__CODE__", function)
        run_js(self, regression)

    @unittest.skipUnless(shutil.which("node"), "Node.js required for embedded JavaScript regression")
    def test_complete_embedded_scripts_remain_valid_javascript(self):
        scripts = []
        for html in (live_html(), build_roy_operations_dashboard_html("roy")):
            script = html.split("<script>\n", 1)[1].split("</script>", 1)[0]
            scripts.append(script)
        run_js(self, "for (const source of " + json.dumps(scripts) + ") new Function(source); console.log('SEMANTICS_OK');")


if __name__ == "__main__":
    unittest.main()
