"""Synthetic regression coverage for descriptive marketing semantics."""
from datetime import datetime
import unittest

import pandas as pd

from dashboard_modern import _creditnote_carrier_row_html, extract_embedded_dashboard_payload, generate_modern_dashboard
from export_orders import BizniWebExporter
from html_report_generator import generate_html_report


class MarketingSemanticsTests(unittest.TestCase):
    def test_creditnote_carrier_surface_uses_same_cohort_denominator(self):
        html = _creditnote_carrier_row_html(dict(carrier='Synthetic carrier', cohort_orders=120,
                                               cohort_creditnoted_orders=3, realized_orders=999,
                                               creditnoted_orders=77, creditnote_rate_pct=2.5))
        self.assertTrue('>120</td>' in html)
        self.assertTrue('>3</td>' in html)
        self.assertFalse('999' in html)
        self.assertFalse('>77</td>' in html)

    def test_large_positive_observational_sample_cannot_authorize_scaling(self):
        exporter = BizniWebExporter.__new__(BizniWebExporter)
        exporter.project_settings = {}
        rows = []
        for index, date in enumerate(pd.date_range('2026-01-01', periods=56)):
            paid = index >= 28
            rows.append(dict(date=date, day_of_week=date.dayofweek, orders=2 if paid else 1,
                             revenue=200 if paid else 100, aov=100, pre_ad_contribution=150 if paid else 70,
                             profit_without_fixed=130 if paid else 70, profit_with_fixed=100 if paid else 40,
                             new_customers=2 if paid else 1, returning_customers=0, new_orders=2 if paid else 1,
                             returning_orders=0, new_revenue=200 if paid else 100, returning_revenue=0,
                             fb_spend=20 if paid else 0, google_spend=0, total_ad_spend=20 if paid else 0))
        daily = pd.DataFrame(rows)
        for method in ['raw', 'matched_weekday']:
            with self.subTest(method=method):
                result = exporter._build_incrementality_comparison(
                    daily, key='synthetic', label_en='Synthetic', label_sk='Synthetic', method=method,
                    active_mask=daily.fb_spend.gt(0), control_mask=daily.fb_spend.eq(0),
                    overlap_rate=0, break_even_cac=100)
                self.assertEqual('high', result['confidence'])
                self.assertTrue(result['sample_filter_passed'])
                self.assertEqual(100, result['incremental_revenue_per_day'])
                self.assertEqual(5, result['incremental_roas'])
                self.assertEqual(20, result['incremental_cac'])
                self.assertFalse(result['decision_ready'])
                self.assertFalse(result['budget_recommendation_available'])
                self.assertEqual('Observation only', result['verdict'])
                self.assertIn('observational comparison is not causal evidence', result['decision_blockers'])

    @staticmethod
    def data():
        daily = pd.DataFrame([dict(date='2026-09-01', total_revenue=1000., product_expense=400.,
                                  fb_ads_spend=100., google_ads_spend=100., unique_orders=10, total_items=10,
                                  net_profit=330., contribution_profit=400., pre_ad_contribution_profit=600.,
                                  roi_percent=49.25, total_cost=670., packaging_cost=0., shipping_net_cost=0.,
                                  fixed_daily_cost=70., revenue_credit_adjustment=-10.)])
        items = pd.DataFrame(columns=['item_label', 'total_revenue', 'total_quantity', 'profit'])
        return daily, items

    def test_both_surfaces_identify_mer_modeled_campaigns_and_separate_credit(self):
        daily, items = self.data()
        financial = dict(roas=5., mer=5., roas_fb=10., roas_google=10., revenue_credit_adjustment=-10.)
        campaign = dict(campaign_name='Synthetic campaign', spend=100., clicks=10, impressions=100,
                        ctr=10., cpc=10., estimated_orders=10., attributed_orders_est=10.,
                        cost_per_attributed_order=10., estimated_cpo=10., estimated_revenue=1000.,
                        estimated_roas=10., attribution_sample_status='estimated', click_share_pct=100., spend_share_pct=100.)
        cost = dict(total_orders=10, total_fb_spend=100., total_revenue=1000.,
                    campaign_attribution=[campaign], daily_cpo=[dict(date='2026-09-01', orders=10, fb_spend=100., revenue=1000., cpo=10., roas=10.)])
        for variant in ['modern', 'legacy']:
            with self.subTest(variant=variant):
                html = generate_html_report(daily, pd.DataFrame(), items, datetime(2026,9,1), datetime(2026,9,1),
                                            financial_metrics=financial, cost_per_order=cost, dashboard_variant=variant)
                self.assertTrue('Net MER' in html)
                self.assertTrue('not platform-attributed ROAS' in html)
                self.assertTrue('Modeled revenue / spend' in html or 'Modeled net revenue / spend' in html)
                self.assertTrue('Order credit adjustment (included)' in html)
                self.assertTrue('no COGS reversal' in html)
                self.assertTrue('Product figures are before unallocated order credit adjustments' in html)
                for bad in ['ROAS (All Ads)', 'ROAS (all ads)', 'Blended ROAS', 'Above 3x is usually healthy',
                            'Green = profitable (', 'against attributed ROAS']:
                    self.assertFalse(bad in html, bad)
                if variant == 'modern':
                    payload = extract_embedded_dashboard_payload(html)
                    self.assertEqual([1000.], payload['series']['revenue'])
                    self.assertEqual([330.], payload['series']['profit'])
                    self.assertEqual([5.], payload['series']['roas'])

    def test_old_observational_payload_cannot_render_prescriptive_verdict(self):
        daily, items = self.data()
        old = dict(primary=dict(verdict='Scale', decision_ready=True, verdict_reason_en='Ads cause profit',
                                incremental_roas=5., incremental_profit_with_fixed_per_day=60.), comparisons=[])
        html = generate_modern_dashboard(daily, items, datetime(2026,9,1), datetime(2026,9,1),
                                         ads_effectiveness={'incrementality': old})
        payload = extract_embedded_dashboard_payload(html)
        self.assertFalse(payload['incrementality_primary']['decision_ready'])
        self.assertEqual('Observation only', payload['incrementality_primary']['verdict'])
        self.assertEqual(5., payload['incrementality_primary']['incremental_roas'])
        self.assertTrue(old['primary']['decision_ready'])
        self.assertFalse('Ads cause profit' in html)


if __name__ == '__main__':
    unittest.main()
