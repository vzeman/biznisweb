"""Synthetic provider/geo regressions: exact definitions, conservation and honest gaps."""
import json
from copy import deepcopy
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pandas as pd

from dashboard_modern import generate_modern_dashboard
from export_orders import BizniWebExporter
from facebook_ads import ADS_MEASUREMENT_SCHEMA_VERSION, FacebookAdsClient, PURCHASE_ACTION
from google_ads import GoogleAdsClient
from html_report_generator import generate_html_report


START = datetime(2026, 9, 1)
END = datetime(2026, 9, 30)


def provider(basis, countries, residual=0):
    total = sum(countries.values())
    result = dict(status='ok', basis=basis, currency='EUR', date_from='2026-09-01',
                  date_to='2026-09-30', spend_by_country=dict(countries), total_spend=total)
    if basis == 'google_user_location':
        account = total + residual
        unknown = countries.get('unknown', 0)
        result.update(customer_id='123456', total_spend=account, account_total_spend=account,
                      reported_geo_total_spend=total, reported_spend_by_country=dict(countries),
                      reported_unknown_spend=unknown, unallocated_spend=residual,
                      country_coverage_ratio=(total-unknown)/account if account else 1.0,
                      country_coverage_status='partial' if residual or unknown else 'complete',
                      residual_basis='account_total_minus_user_location')
        if residual:
            result['spend_by_country']['unallocated'] = residual
    return result


def evidence():
    return dict(date_from='2026-09-01', date_to='2026-09-30',
                expected_totals={'facebook_ads': 33.0, 'google_ads': 3.0},
                expected_account_ids={'google_ads': '123456'},
                facebook_ads=provider('meta_country_breakdown', {'sk': 20.0, 'hu': 10.0, 'unknown': 3.0}),
                google_ads=provider('google_user_location', {'sk': 2.0, 'hu': 1.0}))


def exporter():
    instance = BizniWebExporter.__new__(BizniWebExporter)
    instance.project_settings = {}
    orders = pd.DataFrame([dict(order_num='synthetic', order_revenue_net=100.0, product_cost=40.0,
                                packaging_cost=.3, shipping_net_cost=.2, allocated_google_spend=3.0,
                                allocated_fixed_overhead=70.0)])
    instance._build_growth_order_item_frames = Mock(return_value=(orders, pd.DataFrame(), 'order_revenue_net'))
    return instance


def frame():
    return pd.DataFrame([dict(order_num='synthetic', delivery_country='sk', invoice_country='sk')])


class MetaMeasurementTests(unittest.TestCase):
    def client(self):
        client = FacebookAdsClient.__new__(FacebookAdsClient)
        client.is_configured = True
        client.base_url = 'https://graph.facebook.com/v21.0'
        client.ad_account_id = 'act_synthetic'
        client._get_json = Mock()
        return client

    def test_exact_event_and_window_ignores_aliases_and_cart(self):
        actions = [dict(action_type=PURCHASE_ACTION, value='19', **{'7d_click': '16', '1d_view': '6'}),
                   dict(action_type='purchase', value='19', **{'7d_click': '16'}),
                   dict(action_type='omni_purchase', value='19', **{'7d_click': '16'}),
                   dict(action_type='offsite_conversion.fb_pixel_add_to_cart', value='100', **{'7d_click': '90'})]
        self.assertEqual(FacebookAdsClient._action_count(actions, PURCHASE_ACTION), 16)
        self.assertEqual(FacebookAdsClient._action_count([], PURCHASE_ACTION), 0)

    def test_invalid_or_missing_window_never_zero_fills(self):
        for actions in ([dict(action_type=PURCHASE_ACTION, value='9')],
                        [dict(action_type=PURCHASE_ACTION, **{'7d_click': 'nan'})],
                        [dict(action_type=PURCHASE_ACTION, **{'7d_click': '-1'})],
                        [dict(action_type=PURCHASE_ACTION, **{'7d_click': '1'})] * 2):
            with self.subTest(actions=actions), self.assertRaises(ValueError):
                FacebookAdsClient._action_count(actions, PURCHASE_ACTION)
        self.assertEqual(FacebookAdsClient._action_count(
            [dict(action_type=PURCHASE_ACTION, **{'7d_click': '2.5'})], PURCHASE_ACTION), 2.5)

    def test_campaign_request_definition_and_zero_purchase_cpa(self):
        client = self.client()
        client._get_json.side_effect = [
            {'data': [{'id': 'one', 'name': 'Synthetic'}]},
            {'data': [{'spend': '50', 'clicks': '10', 'actions': []}]},
        ]
        row = client.get_campaign_spend(START, END)[0]
        params = client._get_json.call_args_list[1].args[1]
        self.assertEqual(json.loads(params['action_attribution_windows']), ['7d_click'])
        self.assertEqual(params['action_report_time'], 'impression')
        self.assertEqual(row['platform_conversions'], 0)
        self.assertIsNone(row['cost_per_platform_conversion'])
        self.assertEqual(client.campaign_measurement_status['status'], 'ok')

    def test_campaign_measurement_failure_has_critical_qa(self):
        client = self.client()
        client._get_json.side_effect = [
            {'data': [{'id': 'one', 'name': 'Synthetic'}]},
            {'data': [{'spend': '50', 'actions': [dict(action_type=PURCHASE_ACTION, value='9')]}]},
        ]
        self.assertEqual(client.get_campaign_spend(START, END), [])
        qa = BizniWebExporter._build_attribution_qa(
            cost_per_order={}, fb_campaigns=[], total_orders=1,
            measurement_status=client.campaign_measurement_status)
        self.assertGreater(qa['failure_count'], 0)

    def test_country_pagination_and_unknown_spend(self):
        client = self.client()
        client._get_json.side_effect = [
            {'currency': 'EUR'},
            {'data': [{'country': 'SK', 'spend': '10'}],
             'paging': {'next': 'not-followed', 'cursors': {'after': 'cursor'}}},
            {'data': [{'country': 'unknown', 'spend': '2'}]},
        ]
        result = client.get_country_spend(START, END)
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['spend_by_country'], {'sk': 10, 'unknown': 2})
        self.assertEqual(client._get_json.call_args_list[-1].args[1]['after'], 'cursor')

    def test_missing_campaign_data_is_unavailable(self):
        client = self.client()
        client._get_json.side_effect = [{'data': [{'id': 'one', 'name': 'Synthetic'}]}, {}]
        self.assertEqual(client.get_campaign_spend(START, END), [])
        self.assertEqual(client.campaign_measurement_status['status'], 'unavailable')

    def test_repeated_cursor_and_wrong_currency_are_unavailable(self):
        client = self.client()
        repeated = {'data': [], 'paging': {'next': 'never-follow', 'cursors': {'after': 'same'}}}
        client._get_json.side_effect = [{'currency': 'EUR'}, repeated, repeated]
        self.assertEqual(client.get_country_spend(START, END)['status'], 'unavailable')
        client._get_json.side_effect = [{'currency': 'USD'}]
        self.assertIsNone(client.get_country_spend(START, END)['total_spend'])

    def test_legacy_cache_is_invalidated(self):
        client = self.client()
        with tempfile.TemporaryDirectory() as directory:
            client.cache_dir = Path(directory)
            path = client.get_cache_filename(START, END)
            payload = dict(cached_at=datetime.now().isoformat(), daily_spend={'2026-09-01': 10})
            path.write_text(json.dumps(payload), encoding='utf-8')
            self.assertIsNone(client.load_from_cache(START, END))
            payload['measurement_schema_version'] = ADS_MEASUREMENT_SCHEMA_VERSION
            path.write_text(json.dumps(payload), encoding='utf-8')
            self.assertEqual(client.load_from_cache(START, END), {'2026-09-01': 10})


class GoogleCountryTests(unittest.TestCase):
    @staticmethod
    def client(rows, total=3_500_000, codes=None):
        client = GoogleAdsClient.__new__(GoogleAdsClient)
        client.is_configured = True
        client.customer_id = '123-456'
        service = Mock()
        service.search_stream.side_effect = [
            [NS(results=rows)],
            [NS(results=[NS(customer=NS(id=123456, currency_code='EUR'), metrics=NS(cost_micros=total))])],
            [NS(results=[NS(geo_target_constant=NS(id=key, country_code=value))
                         for key, value in (codes or {1: 'HU'}).items()])],
        ]
        client.client = Mock()
        client.client.get_service.return_value = service
        return client, service

    @staticmethod
    def row(country=1, amount=2_000_000, targeted=True, customer=123456):
        return NS(customer=NS(id=customer), metrics=NS(cost_micros=amount),
                  user_location_view=NS(country_criterion_id=country, targeting_location=targeted))

    def test_physical_country_sums_both_targeting_flags_and_unknown(self):
        rows = [self.row(), self.row(amount=1_000_000, targeted=False), self.row(country=0, amount=500_000)]
        client, service = self.client(rows)
        result = client.get_country_spend(START, END)
        self.assertEqual(result['spend_by_country'], {'hu': 3.0, 'unknown': .5})
        self.assertEqual(result['total_spend'], 3.5)
        self.assertEqual(result['reported_unknown_spend'], .5)
        self.assertEqual(result['unallocated_spend'], 0)
        self.assertEqual(result['country_coverage_status'], 'partial')
        self.assertIn('FROM user_location_view', service.search_stream.call_args_list[0].kwargs['query'])
        for call in service.search_stream.call_args_list[:2]:
            self.assertIn("BETWEEN '2026-09-01' AND '2026-09-30'", call.kwargs['query'])
            self.assertEqual(call.kwargs['customer_id'], '123456')

    def test_positive_residual_preserves_countries_and_is_not_unknown(self):
        client, _ = self.client([self.row(), self.row(country=0, amount=500_000)])
        result = client.get_country_spend(START, END)
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['spend_by_country'], {'hu': 2, 'unknown': .5, 'unallocated': 1})
        self.assertEqual(result['reported_spend_by_country'], {'hu': 2, 'unknown': .5})
        self.assertEqual(result['country_coverage_ratio'], 2/3.5)
        self.assertEqual(sum(result['spend_by_country'].values()), result['total_spend'])

    def test_micro_residual_is_kept_without_rounding_into_a_country(self):
        client, _ = self.client([self.row()], total=2_000_001)
        result = client.get_country_spend(START, END)
        self.assertEqual(result['unallocated_spend'], .000001)
        self.assertEqual(result['spend_by_country']['hu'], 2)

    def test_account_failure_after_geo_success_never_becomes_unallocated(self):
        client, service = self.client([self.row()])
        service.search_stream.side_effect = [[NS(results=[self.row()])], RuntimeError('synthetic account query failure')]
        result = client.get_country_spend(START, END)
        self.assertEqual(result['status'], 'unavailable')
        self.assertIsNone(result['total_spend'])
        self.assertEqual(result['spend_by_country'], {})

    def test_missing_account_and_mapping_fail_closed(self):
        for failed_query in (1, 2):
            client, service = self.client([self.row()])
            calls = [[NS(results=[self.row()])],
                     [NS(results=[NS(customer=NS(id=123456, currency_code='EUR'), metrics=NS(cost_micros=2_000_000))])],
                     [NS(results=[NS(geo_target_constant=NS(id=1, country_code='HU'))])]]
            calls[failed_query] = [NS(results=[])]
            service.search_stream.side_effect = calls
            with self.subTest(failed_query=failed_query):
                self.assertEqual(client.get_country_spend(START, END)['status'], 'unavailable')

    def test_invalid_excess_duplicate_and_wrong_account_fail_closed(self):
        cases = [([self.row()], 1_999_999), ([self.row(amount=-1)], 1),
                 ([self.row(amount=float('nan'))], 1), ([self.row(customer=999)], 2_000_000),
                 ([self.row(), self.row()], 4_000_000)]
        for rows, total in cases:
            with self.subTest(total=total):
                client, _ = self.client(rows, total=total)
                self.assertEqual(client.get_country_spend(START, END)['status'], 'unavailable')
        client, service = self.client([self.row()])
        service.search_stream.side_effect = [[NS(results=[self.row()])],
            [NS(results=[NS(customer=NS(id=999, currency_code='EUR'), metrics=NS(cost_micros=2_000_000))])]]
        self.assertEqual(client.get_country_spend(START, END)['status'], 'unavailable')

    def test_verified_zero_account_with_no_geo_rows_is_complete(self):
        client, _ = self.client([], total=0)
        result = client.get_country_spend(START, END)
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['spend_by_country'], {})
        self.assertEqual(result['country_coverage_ratio'], 1)


class CountryEconomicsTests(unittest.TestCase):
    def test_residual_conservation_and_warning_do_not_allocate_to_orders(self):
        values = evidence()
        values['google_ads'] = provider('google_user_location', {'sk': 2, 'hu': 1, 'ro': 3, 'unknown': .5}, residual=4)
        values['expected_totals']['google_ads'] = 10.5
        instance = exporter()
        result = instance.analyze_geo_profitability(frame(), country_ads=values)
        geo = result['table'].set_index('country')
        self.assertEqual(set(geo.index), {'sk', 'cz', 'hu', 'other', 'unknown', 'unallocated'})
        self.assertEqual(geo['google_ads_spend'].sum(), 10.5)
        self.assertEqual(geo.loc['sk', 'google_ads_spend'], 2)
        self.assertEqual(geo.loc['other', 'google_ads_spend'], 3)
        self.assertEqual(geo.loc['unknown', 'google_ads_spend'], .5)
        self.assertEqual(geo.loc['unallocated', 'google_ads_spend'], 4)
        self.assertEqual(geo.loc['unallocated', 'orders'], 0)
        self.assertEqual(geo.loc['unallocated', 'contribution_profit_without_fixed'], -4)
        self.assertTrue(pd.isna(geo.loc['unknown', 'net_mer']))
        self.assertTrue(pd.isna(geo.loc['unallocated', 'net_mer']))
        self.assertEqual(geo.loc['sk', 'spend_attribution_status'], 'measured_partial_coverage')
        self.assertAlmostEqual(geo.loc['sk', 'net_mer'], 100/22)
        qa = instance._build_geo_qa(None, result)
        self.assertEqual(qa['failure_count'], 0)
        self.assertEqual(qa['status'], 'warning')
        self.assertTrue(any('physical-country coverage' in s for s in qa['warnings']))

    def test_unproven_or_forged_residual_is_critical(self):
        baseline = evidence()
        baseline['google_ads'] = provider('google_user_location', {'sk': 2}, residual=1)
        for field, value in [('customer_id', '999'), ('unallocated_spend', -1),
                             ('country_coverage_ratio', 1), ('account_total_spend', 4),
                             ('reported_spend_by_country', None), ('country_coverage_status', 'complete'),
                             ('spend_by_country', {'sk': 3}), ('date_from', '2026-08-01')]:
            values = deepcopy(baseline)
            values['google_ads'][field] = value
            with self.subTest(field=field):
                result = exporter().analyze_geo_profitability(frame(), country_ads=values)
                self.assertGreater(result['spend_attribution']['failure_count'], 0)
                self.assertTrue(result['table']['net_mer'].isna().all())

    def test_measured_spend_and_mer_include_zero_order_and_unknown_countries(self):
        instance = exporter()
        result = instance.analyze_geo_profitability(frame(), country_ads=evidence())
        geo = result['table'].set_index('country')
        self.assertEqual(set(geo.index), {'sk', 'cz', 'hu', 'unknown'})
        self.assertEqual(geo['fb_ads_spend'].sum(), 33)
        self.assertEqual(geo['google_ads_spend'].sum(), 3)
        self.assertEqual(geo.loc['hu', 'orders'], 0)
        self.assertEqual(geo.loc['hu', 'contribution_profit_without_fixed'], -11)
        self.assertTrue(pd.isna(geo.loc['hu', 'fb_cpo']))
        self.assertEqual(geo.loc['hu', 'net_mer'], 0)
        self.assertTrue(pd.isna(geo.loc['cz', 'net_mer']))
        self.assertAlmostEqual(geo.loc['sk', 'net_mer'], 100/22)

    def test_missing_source_wrong_dates_and_mismatch_fail_closed(self):
        for alteration in ('missing', 'dates', 'difference'):
            values = evidence()
            if alteration == 'missing':
                values['facebook_ads']['status'] = 'unavailable'
            elif alteration == 'dates':
                values['facebook_ads']['date_from'] = '2026-08-01'
            else:
                values['expected_totals']['facebook_ads'] = 50
            with self.subTest(alteration=alteration):
                instance = exporter()
                result = instance.analyze_geo_profitability(frame(), country_ads=values)
                self.assertTrue(result['table']['contribution_profit_without_fixed'].isna().all())
                self.assertTrue((result['table']['confidence_status'] == 'unavailable').all())
                qa = instance._build_geo_qa(None, result)
                self.assertEqual(qa['status'], 'error')
                self.assertGreater(qa['failure_count'], 0)

    def test_empty_order_frame_still_reports_spend(self):
        result = exporter().analyze_geo_profitability(pd.DataFrame(), country_ads=evidence())
        self.assertEqual(result['table']['paid_ads_spend'].sum(), 36)
        self.assertEqual(result['table']['contribution_profit_without_fixed'].sum(), -36)

    def test_project_flag_avoids_roy_provider_calls(self):
        instance = exporter()
        instance.fb_client = Mock()
        instance.google_ads_client = Mock()
        self.assertIsNone(instance._fetch_country_ads(START, END, {}, {}))
        instance.fb_client.get_country_spend.assert_not_called()
        instance.google_ads_client.get_country_spend.assert_not_called()
        instance.project_settings['measured_country_ads_enabled'] = True
        instance.fb_client.get_country_spend.return_value = evidence()['facebook_ads']
        instance.google_ads_client.get_country_spend.return_value = evidence()['google_ads']
        instance._fetch_country_ads(START, END, {}, {})
        instance.fb_client.get_country_spend.assert_called_once_with(START, END)

    def test_legacy_geo_retains_estimated_values_but_declares_basis(self):
        result = exporter().analyze_geo_profitability(frame(), fb_campaigns=[{'campaign_name': 'SK', 'spend': 10}])
        self.assertEqual(result['table'].iloc[0]['google_ads_spend'], 3)
        self.assertEqual(result['spend_attribution']['mode'], 'estimated')

    def test_rendering_preserves_null_cpa_geo_basis_and_mer(self):
        geo = exporter().analyze_geo_profitability(frame(), country_ads=evidence())
        daily = pd.DataFrame([dict(date='2026-09-01', total_revenue=100., product_expense=40.,
                                  fb_ads_spend=33., google_ads_spend=3., unique_orders=1, total_items=1,
                                  net_profit=-46.5, roi_percent=-31.7, total_cost=146.5,
                                  packaging_cost=.3, shipping_net_cost=.2, fixed_daily_cost=70.)])
        items = pd.DataFrame(columns=['item_label', 'total_revenue', 'total_quantity', 'profit'])
        campaigns = [dict(campaign_name='Synthetic', spend=33., clicks=10, platform_conversions=0.,
                          conversions=0., cost_per_conversion=None, cost_per_platform_conversion=None)]
        html = generate_modern_dashboard(daily, items, START, END, geo_profitability=geo, fb_campaigns=campaigns)
        self.assertIn('Net MER (all shop sales / ads)', html)
        self.assertIn('not platform-attributed ROAS', html)
        self.assertIn('2026-09-01 - 2026-09-30', html)
        self.assertIn('meta_country_breakdown', html)
        self.assertIn('Google country coverage: 100.00%', html)
        self.assertIn('Pokrytie krajiny Google nákladov: 100.00 %', html)
        self.assertIn('cost_per_platform_conversion == null ? null', html)
        self.assertEqual(html.count('<th>Meta spend</th><th>Google spend</th>'), 1)
        fractional = dict(campaigns[0], platform_conversions=2.5, conversions=2.5)
        fractional_html = generate_modern_dashboard(daily, items, START, END, fb_campaigns=[fractional])
        self.assertIn('<td>2.50</td>', fractional_html)
        legacy = generate_html_report(daily, pd.DataFrame(), items, START, END,
                                      geo_profitability=geo, fb_campaigns=campaigns, dashboard_variant='legacy')
        self.assertIn('N/A', legacy)
        self.assertIn('campaignCostPerConversions = [null]', legacy)
        self.assertNotIn('Best performers are listed first', legacy)
        values = evidence()
        values['google_ads'] = provider('google_user_location', {'sk': 2, 'hu': 1}, residual=1)
        values['expected_totals']['google_ads'] = 4
        partial_geo = exporter().analyze_geo_profitability(frame(), country_ads=values)
        partial_html = generate_modern_dashboard(daily, items, START, END, geo_profitability=partial_geo)
        self.assertIn('Google country coverage: 75.00%', partial_html)
        self.assertIn('Nepriradené (UNALLOCATED): 1.00 EUR', partial_html)
        self.assertIn('SK/CZ/HU costs are not estimated', partial_html)
        self.assertIn('UNKNOWN and UNALLOCATED have no MER', partial_html)
        partial_legacy = generate_html_report(daily, pd.DataFrame(), items, START, END,
                                             geo_profitability=partial_geo, dashboard_variant='legacy')
        self.assertIn('Google country coverage: 75.00%', partial_legacy)
        self.assertIn('Pokrytie krajiny Google nákladov: 75.00 %', partial_legacy)
        self.assertIn('<th class="number">Google Spend</th>', partial_legacy)
        self.assertIn('Net MER (all shop sales / ads)', partial_legacy)


if __name__ == '__main__':
    unittest.main()
