"""Daily measured country spend: provider failures never become zero allocations."""
from copy import deepcopy
from datetime import datetime
from decimal import Decimal
import json
from types import SimpleNamespace as NS
import unittest
from unittest.mock import Mock, patch

with patch('dotenv.load_dotenv'):
    from facebook_ads import FacebookAdsClient
    from google_ads import GoogleAdsClient


START = datetime(2026, 9, 1)
END = datetime(2026, 9, 3)
DAYS = ['2026-09-01', '2026-09-02', '2026-09-03']


def meta_row(day=DAYS[0], country='SK', spend='1.25'):
    return dict(date_start=day, date_stop=day, country=country, spend=spend)


class MetaCountryDailyTests(unittest.TestCase):
    def client(self, pages, currency='EUR'):
        client = FacebookAdsClient.__new__(FacebookAdsClient)
        client.is_configured = True
        client.base_url = 'https://graph.facebook.com/v21.0'
        client.ad_account_id = 'act_synthetic'
        client._get_json = Mock(side_effect=[{'currency': currency}, *pages])
        return client

    def assert_unavailable(self, result):
        self.assertEqual(result['status'], 'unavailable')
        self.assertIsNone(result['total_spend'])
        self.assertEqual(result['daily_spend_by_country'], {})
        self.assertEqual(result['spend_by_country'], {})

    def test_paginated_daily_country_query_preserves_native_countries_and_precision(self):
        client = self.client([
            {'data': [meta_row(spend='0.1'), meta_row(country='PL', spend='2.123456')],
             'paging': {'next': 'https://never-follow.invalid/?access_token=private',
                        'cursors': {'after': 'next-page'}}},
            {'data': [meta_row(DAYS[1], 'SK', '0.2'), meta_row(DAYS[1], 'unknown', '0.000001')]},
        ])
        result = client.get_country_daily_spend(START, END)
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['basis'], 'meta_country_breakdown_daily')
        self.assertEqual(result['currency'], 'EUR')
        self.assertEqual(result['spend_by_country'], {'sk': .3, 'pl': 2.123456, 'unknown': .000001})
        self.assertEqual(result['daily_spend_by_country'][DAYS[2]], {})
        self.assertEqual(result['daily_total_spend'][DAYS[2]], 0)
        self.assertEqual(result['total_spend'], 2.423457)
        calls = client._get_json.call_args_list
        self.assertEqual(calls[0].args[1], {'fields': 'currency'})
        for call in calls[1:]:
            self.assertEqual(call.args[0], client.base_url + '/act_synthetic/insights')
            query = call.args[1]
            self.assertEqual(query['time_increment'], 1)
            self.assertEqual(query['breakdowns'], 'country')
            self.assertEqual(query['fields'], 'spend')
            self.assertEqual(query['level'], 'account')
            self.assertEqual(json.loads(query['time_range']), {'since': DAYS[0], 'until': DAYS[2]})
        self.assertEqual(calls[-1].args[1]['after'], 'next-page')

    def test_complete_empty_source_is_zero_but_disabled_is_not(self):
        client = self.client([{'data': []}])
        result = client.get_country_daily_spend(START, END)
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['daily_spend_by_country'], dict.fromkeys(DAYS, {}))
        self.assertEqual(result['total_spend'], 0)
        client.is_configured = False
        client._get_json.reset_mock()
        result = client.get_country_daily_spend(START, END)
        self.assertEqual(result['status'], 'disabled')
        self.assertIsNone(result['total_spend'])
        client._get_json.assert_not_called()

    def test_zero_country_row_is_retained(self):
        result = self.client([{'data': [meta_row(country='HR', spend='0')]}]).get_country_daily_spend(START, END)
        self.assertEqual(result['daily_spend_by_country'][DAYS[0]], {'hr': 0})

    def test_repeated_or_missing_pagination_cursor_is_unavailable(self):
        repeat = {'data': [], 'paging': {'next': 'ignored', 'cursors': {'after': 'same'}}}
        for pages in ([repeat, repeat], [{'data': [], 'paging': {'next': 'ignored'}}]):
            with self.subTest(pages=pages):
                self.assert_unavailable(self.client(pages).get_country_daily_spend(START, END))

    def test_pagination_limit_is_bounded(self):
        pages = [{'data': [], 'paging': {'next': 'ignored', 'cursors': {'after': str(n)}}} for n in range(100)]
        client = self.client(pages)
        self.assert_unavailable(client.get_country_daily_spend(START, END))
        self.assertEqual(client._get_json.call_count, 101)

    def test_duplicate_day_country_across_pages_is_unavailable(self):
        client = self.client([{'data': [meta_row()], 'paging': {'next': 'ignored', 'cursors': {'after': 'two'}}},
                              {'data': [meta_row(country='sk')]}])
        self.assert_unavailable(client.get_country_daily_spend(START, END))

    def test_malformed_missing_negative_and_nonfinite_spend_are_unavailable(self):
        rows = [meta_row(spend=value) for value in ('-0.01', 'NaN', 'Infinity', True, None, {}, '1e999')]
        for field in ('date_start', 'date_stop', 'country', 'spend'):
            row = meta_row()
            row.pop(field)
            rows.append(row)
        for row in rows:
            with self.subTest(row=row):
                self.assert_unavailable(self.client([{'data': [row]}]).get_country_daily_spend(START, END))

    def test_invalid_dates_outside_range_and_multiday_rows_are_unavailable(self):
        rows = [meta_row(day) for day in ('2026-08-31', '2026-09-04', '2026-02-30', '2026-9-01')]
        rows.append({**meta_row(), 'date_stop': DAYS[1]})
        for row in rows:
            with self.subTest(row=row):
                self.assert_unavailable(self.client([{'data': [row]}]).get_country_daily_spend(START, END))
        client = self.client([])
        self.assert_unavailable(client.get_country_daily_spend(END, START))
        client._get_json.assert_not_called()

    def test_wrong_currency_or_provider_failure_is_not_zero_and_does_not_log_secrets(self):
        self.assert_unavailable(self.client([], currency='USD').get_country_daily_spend(START, END))
        client = self.client([RuntimeError('access_token=do-not-log')])
        with patch('facebook_ads.logger.warning') as warning:
            self.assert_unavailable(client.get_country_daily_spend(START, END))
        self.assertNotIn('do-not-log', str(warning.call_args))
        for body in ({}, {'data': None}, {'error': {'message': 'private'}}):
            self.assert_unavailable(self.client([body]).get_country_daily_spend(START, END))

    def test_malformed_country_and_error_currency_response_are_unavailable(self):
        for country in ([], {}, True, 123):
            client = self.client([{'data': [meta_row(country=country)]}])
            self.assert_unavailable(client.get_country_daily_spend(START, END))
        client = self.client([])
        client._get_json.side_effect = [{'currency': 'EUR', 'error': {'message': 'private'}}]
        self.assert_unavailable(client.get_country_daily_spend(START, END))


def google_row(day=DAYS[0], country=1, amount=2_000_000, targeted=True, customer=123456):
    return NS(customer=NS(id=customer), segments=NS(date=day), metrics=NS(cost_micros=amount),
              user_location_view=NS(country_criterion_id=country, targeting_location=targeted))


def account_row(day=DAYS[0], amount=3_000_000, currency='EUR', customer=123456):
    return NS(customer=NS(id=customer, currency_code=currency), segments=NS(date=day),
              metrics=NS(cost_micros=amount))


class GoogleCountryDailyTests(unittest.TestCase):
    def client(self, geo=None, daily=None, total=3_000_000, currency='EUR', codes=None):
        client = GoogleAdsClient.__new__(GoogleAdsClient)
        client.is_configured = True
        client.customer_id = '123-456'
        service = Mock()
        geo = [google_row()] if geo is None else geo
        daily = [account_row()] if daily is None else daily
        codes = {1: 'HU'} if codes is None else codes
        service.search_stream.side_effect = [
            [NS(results=geo)], [NS(results=[account_row(amount=total, currency=currency)])],
            [NS(results=daily)],
            [NS(results=[NS(geo_target_constant=NS(id=key, country_code=value)) for key, value in codes.items()])],
        ]
        client.client = Mock()
        client.client.get_service.return_value = service
        return client, service

    def assert_unavailable(self, result):
        self.assertEqual(result['status'], 'unavailable')
        self.assertIsNone(result['total_spend'])
        self.assertEqual(result['daily_spend_by_country'], {})
        self.assertEqual(result['spend_by_country'], {})

    def test_exact_daily_queries_and_integer_residual_conservation(self):
        rows = [google_row(amount=1_000_000), google_row(amount=500_000, targeted=False),
                google_row(country=0, amount=100_000), google_row(DAYS[1], country=2, amount=700_000)]
        client, service = self.client(rows, [account_row(amount=1_700_001), account_row(DAYS[1], 800_000)],
                                      total=2_500_001, codes={1: 'HU', 2: 'PL'})
        result = client.get_country_daily_spend(START, END)
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['basis'], 'google_user_location_daily')
        self.assertEqual(result['customer_id'], '123456')
        self.assertEqual(result['spend_by_country'], {'hu': 1.5, 'unknown': .1, 'pl': .7, 'unallocated': .200001})
        self.assertEqual(result['daily_unallocated_spend'], {DAYS[0]: .100001, DAYS[1]: .1, DAYS[2]: 0})
        self.assertEqual(result['daily_reported_unknown_spend'][DAYS[0]], .1)
        self.assertEqual(result['reported_geo_total_spend'], 2.3)
        self.assertEqual(result['country_coverage_status'], 'partial')
        self.assertEqual(result['total_spend'], 2.500001)
        for day, countries in result['daily_spend_by_country'].items():
            self.assertEqual(sum(map(lambda n: Decimal(str(n)), countries.values()), Decimal(0)),
                             Decimal(str(result['daily_account_spend'][day])))
        queries = [call.kwargs['query'] for call in service.search_stream.call_args_list]
        self.assertIn('segments.date, user_location_view.country_criterion_id', queries[0])
        self.assertIn('FROM user_location_view', queries[0])
        self.assertIn('customer.currency_code, segments.date, metrics.cost_micros', queries[2])
        for call in service.search_stream.call_args_list:
            self.assertEqual(call.kwargs['customer_id'], '123456')
        for query in queries[:3]:
            self.assertIn("BETWEEN '2026-09-01' AND '2026-09-03'", query)

    def test_stream_batches_combine_distinct_days_not_duplicate_rows(self):
        client, service = self.client(daily=[account_row(amount=2_000_000), account_row(DAYS[1], 1_000_000)])
        calls = list(service.search_stream.side_effect)
        calls[0] = [NS(results=[google_row()]), NS(results=[google_row(DAYS[1], amount=1_000_000)])]
        service.search_stream.side_effect = calls
        result = client.get_country_daily_spend(START, END)
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['spend_by_country'], {'hu': 3})

    def test_verified_empty_zero_account_is_complete(self):
        client, service = self.client(geo=[], daily=[], total=0)
        result = client.get_country_daily_spend(START, END)
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['daily_account_spend'], dict.fromkeys(DAYS, 0))
        self.assertEqual(result['daily_spend_by_country'], dict.fromkeys(DAYS, {}))
        self.assertEqual(result['country_coverage_status'], 'complete')
        self.assertEqual(result['total_spend'], 0)
        self.assertEqual(service.search_stream.call_count, 3)

    def test_no_geo_data_retains_each_days_residual_without_assigning_named_country(self):
        client, _ = self.client(geo=[])
        result = client.get_country_daily_spend(START, END)
        self.assertEqual(result['spend_by_country'], {'unallocated': 3})
        self.assertEqual(result['daily_spend_by_country'][DAYS[0]], {'unallocated': 3})
        self.assertEqual(result['country_coverage_ratio'], 0)

    def test_disabled_never_calls_provider(self):
        client, service = self.client()
        client.is_configured = False
        result = client.get_country_daily_spend(START, END)
        self.assertEqual(result['status'], 'disabled')
        self.assertIsNone(result['total_spend'])
        service.search_stream.assert_not_called()

    def test_missing_account_or_mapping_is_unavailable(self):
        for index in (1, 3):
            client, service = self.client()
            calls = list(service.search_stream.side_effect)
            calls[index] = [NS(results=[])]
            service.search_stream.side_effect = calls
            with self.subTest(query=index):
                self.assert_unavailable(client.get_country_daily_spend(START, END))

    def test_missing_daily_account_cost_and_incorrect_total_are_unavailable(self):
        for daily, total in (([], 3_000_000), ([account_row()], 3_000_001)):
            client, _ = self.client(daily=daily, total=total)
            self.assert_unavailable(client.get_country_daily_spend(START, END))

    def test_daily_excess_cannot_hide_inside_positive_whole_period_residual(self):
        client, _ = self.client(geo=[google_row(amount=2_000_001)],
                               daily=[account_row(amount=2_000_000), account_row(DAYS[1], 1_000_000)])
        self.assert_unavailable(client.get_country_daily_spend(START, END))

    def test_invalid_duplicate_geographic_identity_and_amounts_are_unavailable(self):
        cases = [[google_row(), google_row()], [google_row(amount=-1)], [google_row(amount=True)],
                 [google_row(amount=1.5)], [google_row(amount=float('nan'))], [google_row(customer=999)],
                 [google_row(country=-1)], [google_row(country=True)], [google_row(targeted='false')]]
        for rows in cases:
            with self.subTest(rows=rows):
                client, _ = self.client(geo=rows)
                self.assert_unavailable(client.get_country_daily_spend(START, END))

    def test_missing_metric_or_date_is_unavailable(self):
        for key in ('metrics', 'segments', 'customer', 'user_location_view'):
            row = google_row()
            delattr(row, key)
            client, _ = self.client(geo=[row])
            self.assert_unavailable(client.get_country_daily_spend(START, END))

    def test_invalid_outside_or_duplicate_account_dates_are_unavailable(self):
        cases = [[account_row(day='2026-09-04')], [account_row(day='2026-02-30')],
                 [account_row(day='2026-9-01')], [account_row(), account_row()]]
        for rows in cases:
            client, _ = self.client(daily=rows)
            self.assert_unavailable(client.get_country_daily_spend(START, END))
        client, service = self.client()
        self.assert_unavailable(client.get_country_daily_spend(END, START))
        service.search_stream.assert_not_called()

    def test_wrong_currency_or_daily_account_identity_is_unavailable(self):
        client, _ = self.client(currency='USD')
        self.assert_unavailable(client.get_country_daily_spend(START, END))
        for row in (account_row(currency='USD'), account_row(customer=999), account_row(amount=-1)):
            client, _ = self.client(daily=[row])
            self.assert_unavailable(client.get_country_daily_spend(START, END))

    def test_duplicate_or_extra_mapping_does_not_silently_overwrite(self):
        client, service = self.client()
        calls = list(service.search_stream.side_effect)
        mapping = NS(geo_target_constant=NS(id=1, country_code='HU'))
        calls[3] = [NS(results=[mapping, deepcopy(mapping)])]
        service.search_stream.side_effect = calls
        self.assert_unavailable(client.get_country_daily_spend(START, END))
        client, _ = self.client(codes={1: 'HU', 2: 'SK'})
        self.assert_unavailable(client.get_country_daily_spend(START, END))
        for identity, country in ((True, 'HU'), (1.0, 'HU'), (1, None), (1, 'HUN')):
            client, service = self.client()
            calls = list(service.search_stream.side_effect)
            calls[3] = [NS(results=[NS(geo_target_constant=NS(id=identity, country_code=country))])]
            service.search_stream.side_effect = calls
            self.assert_unavailable(client.get_country_daily_spend(START, END))

    def test_late_stream_failure_is_unavailable_and_private_exception_not_logged(self):
        def broken_stream():
            yield NS(results=[google_row()])
            raise RuntimeError('private-refresh-token')
        client, service = self.client()
        service.search_stream.side_effect = [broken_stream()]
        with patch('google_ads.logger.warning') as warning:
            self.assert_unavailable(client.get_country_daily_spend(START, END))
        self.assertNotIn('private-refresh-token', str(warning.call_args))


if __name__ == '__main__':
    unittest.main()
