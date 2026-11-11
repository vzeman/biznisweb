"""Daily country source receipts are additive to original country/daily proof."""
from datetime import date, timedelta
import json
import unittest
from types import SimpleNamespace
from urllib.parse import parse_qsl, urlencode, urlparse

from tests import test_reporting_meta_source_snapshot as fixtures
from scripts import reporting_meta_source_snapshot as snapshot


class CountryDailySnapshotTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.MetaSourceSnapshotTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.audit = self.fixture.audit

    def observe_daily_country(self, start, end, rows, **query):
        req = fixtures.request(fixtures.params('country', start, end, time_increment=1, **query))
        body = {'data': rows}
        self.audit.observe(req, fixtures.response(req, body))

    def test_additive_daily_country_keeps_original_proof_and_exact_native_bytes(self):
        self.fixture.fill()
        for start, end in self.audit.periods.values():
            self.observe_daily_country(start, end, [
                fixtures.row('country', start=start, end=start, spend='1.234567', country='SK'),
                fixtures.row('country', start=start, end=start, spend='0.000001', country='HU'),
                fixtures.row('country', start=end, end=end, spend='0', country='SK'),
            ])
        reference = self.audit.finalize()
        manifest = json.loads(self.fixture.stored[reference['key']])
        kinds = [group['kind'] for group in manifest['query_groups'].values()]
        self.assertEqual(4, kinds.count('country'))
        self.assertEqual(4, kinds.count('country_daily'))
        self.assertEqual(1, kinds.count('daily'))
        for record in manifest['native_responses']:
            if record['kind'] == 'country_daily':
                raw = self.fixture.stored[record['key']]
                self.assertIn(b'1.234567', raw)
                self.assertEqual(snapshot.sha(raw), record['original_response_sha256'])
                self.assertFalse(record['credential_redaction'])

    def test_partial_daily_country_ranges_cannot_finalize(self):
        self.fixture.fill()
        self.observe_daily_country(fixtures.START, fixtures.END, [])
        with self.assertRaisesRegex(RuntimeError, 'country-daily-periods-incomplete'):
            self.audit.finalize()

    def test_legacy_manifest_still_completes_without_any_daily_country_read(self):
        self.fixture.fill()
        reference = self.audit.finalize()
        manifest = json.loads(self.fixture.stored[reference['key']])
        self.assertEqual(5, len(manifest['query_groups']))
        self.assertTrue(manifest['complete'])

    def test_country_daily_requires_native_currency_first(self):
        with self.assertRaisesRegex(RuntimeError, 'country-currency-missing'):
            self.observe_daily_country(fixtures.START, fixtures.END, [])

    def test_duplicate_country_day_across_pages_is_rejected(self):
        self.fixture.currency()
        query = fixtures.params('country', time_increment=1)
        req = fixtures.request(query)
        next_url = fixtures.request({**query, 'after': 'next'}).url
        native = fixtures.row('country', start=fixtures.START, end=fixtures.START)
        self.audit.observe(req, fixtures.response(req, {'data': [native],
                           'paging': {'cursors': {'after': 'next'}, 'next': next_url}}))
        with self.assertRaisesRegex(RuntimeError, 'duplicate-native-row'):
            self.observe_daily_country(fixtures.START, fixtures.END, [native], after='next')

    def test_invalid_daily_country_date_or_market_is_rejected(self):
        invalid_rows = [
            fixtures.row('country', start='2025-12-31', end='2025-12-31'),
            fixtures.row('country', start=fixtures.START, end='2026-01-02'),
            fixtures.row('country', start='2026-02-30', end='2026-02-30'),
            fixtures.row('country', start=fixtures.START, end=fixtures.START, country='Slovakia'),
        ]
        for native in invalid_rows:
            with self.subTest(native=native):
                self.fixture = fixtures.MetaSourceSnapshotTests()
                self.fixture.setUp()
                self.addCleanup(self.fixture.doCleanups)
                self.audit = self.fixture.audit
                self.fixture.currency()
                with self.assertRaisesRegex(RuntimeError, 'country-daily-row'):
                    self.observe_daily_country(fixtures.START, fixtures.END, [native])

    def test_other_time_increment_and_extra_fields_are_rejected(self):
        self.fixture.currency()
        for change in ({'time_increment': 7}, {'fields': 'spend,reach'}):
            with self.subTest(change=change):
                req = fixtures.request(fixtures.params('country', time_increment=1) | change)
                with self.assertRaisesRegex(RuntimeError, 'country-query'):
                    self.audit.observe(req, fixtures.response(req, {'data': []}))


class NativePagingVersionTests(unittest.TestCase):
    """Run the same scope/cursor/byte contract for each admitted native version."""

    paging_version = 'v24.0'

    def setUp(self):
        self.fixture = fixtures.MetaSourceSnapshotTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.audit = self.fixture.audit

    @classmethod
    def paging_url(cls, query, cursor='next', *, version=None, direction='next'):
        public = {key: value for key, value in query.items() if key != 'after'}
        public['after' if direction == 'next' else 'before'] = cursor
        version = version or cls.paging_version
        return f'{snapshot.ORIGIN}/{version}/{snapshot.ACCOUNT}/insights?' + urlencode(public)

    def fresh(self):
        self.fixture.stored.clear()
        self.audit = self.fixture.new()
        self.fixture.currency(self.audit)

    def observe_page(self, query, *, url=None, cursor='next', rows=None, previous=None):
        req = fixtures.request(query)
        paging = {'cursors': {'after': cursor}, 'next': url or self.paging_url(query, cursor)}
        if previous:
            paging['cursors']['before'] = 'prior'
            paging['previous'] = previous
        body = {'data': rows or [fixtures.row('country', end=fixtures.START)], 'paging': paging}
        res = fixtures.response(req, body)
        self.audit.observe(req, res)
        return req, res

    def test_minimal_captured_native_shape_is_admitted_without_changing_request(self):
        # Public endpoint/query shape observed on 2026-10-27 and 2026-11-11;
        # the monetary row is a synthetic fixture, not new provider evidence.
        end = '2026-11-10' if self.paging_version == 'v25.0' else '2026-10-26'
        native_url = (f'https://graph.facebook.com/{self.paging_version}/act_1131531703632101/insights?'
                      'fields=spend&level=account&breakdowns=country&time_increment=1&'
                      'time_range=%7B%22since%22%3A+%222025-05-03%22%2C+%22until%22%3A+'
                      f'%22{end}%22%7D&limit=500&after=NDk5')
        self.audit = snapshot.MetaSourceSnapshot(
            fixtures.IDENTITY, snapshot.sha(snapshot.canonical(fixtures.IDENTITY)),
            '2025-05-03', end, fixtures.PREFIX, self.fixture.put,
            source_paths=(self.fixture.source,))
        self.fixture.currency(self.audit)
        query = fixtures.params('country', '2025-05-03', end, time_increment=1)
        req, res = self.observe_page(query, url=native_url, cursor='NDk5', rows=[
            fixtures.row('country', start='2025-05-03', end='2025-05-03', spend='34.99')])
        self.assertEqual('/v21.0/' + snapshot.ACCOUNT + '/insights', urlparse(req.url).path)
        saved = json.loads(self.fixture.stored[self.audit.pages[-1]['key']])
        self.assertEqual(native_url, saved['paging']['next'])
        self.assertEqual(res.json()['data'], saved['data'])
        self.assertEqual([], self.audit.errors)

    def test_500_row_first_page_native_previous_and_v21_cursor_requests_complete_all_ranges(self):
        self.fixture.fill()
        query = fixtures.params('country', time_increment=1)
        rows = [fixtures.row('country', start=day, end=day, country=country, spend='1.234567890123456789')
                for offset in range(250)
                for day in [(date.fromisoformat(fixtures.START) + timedelta(days=offset)).isoformat()]
                for country in ('SK', 'CZ')]
        original_requests, original_responses = [], []
        client = SimpleNamespace()

        def get_json(url, params, _context):
            req = fixtures.request(params, url=url)
            original_requests.append(req)
            if len(original_requests) == 1:
                body = {'data': rows, 'paging': {'cursors': {'before': 'first', 'after': 'next'},
                        'next': self.paging_url(params)}}
            else:
                body = {'data': [fixtures.row('country', end=fixtures.START, country='HU')],
                        'paging': {'cursors': {'before': 'prior', 'after': 'last'},
                                   'previous': self.paging_url(params, 'prior', direction='previous')}}
            res = fixtures.response(req, body)
            original_responses.append(res.content)
            self.audit.observe(req, res)
            return res.json()

        client._get_json = get_json
        received = fixtures.core_method('_get_all_rows')(client, fixtures.INSIGHTS, query, 'native paging')
        self.assertEqual(501, len(received))
        self.assertEqual([None, 'next'], [dict(parse_qsl(urlparse(req.url).query)).get('after')
                                        for req in original_requests])
        self.assertTrue(all(urlparse(req.url).path == f'/v21.0/{snapshot.ACCOUNT}/insights'
                            for req in original_requests))
        for start, end in list(self.audit.periods.values())[1:]:
            req = fixtures.request(fixtures.params('country', start, end, time_increment=1))
            self.audit.observe(req, fixtures.response(req, {'data': []}))
        reference = self.audit.finalize()
        manifest = json.loads(self.fixture.stored[reference['key']])
        self.assertEqual(4, sum(group['kind'] == 'country_daily' for group in manifest['query_groups'].values()))
        self.assertTrue(all(group['complete'] for group in manifest['query_groups'].values()))
        self.assertTrue(all(record['path'].startswith('/v21.0/') for record in manifest['native_responses']))
        full_pages = [record for record in manifest['native_responses'] if record['kind'] == 'country_daily'][:2]
        for record, native in zip(full_pages, original_responses):
            self.assertEqual(snapshot.sha(native), record['original_response_sha256'])
            self.assertEqual(json.loads(native)['data'], json.loads(self.fixture.stored[record['key']])['data'])
            self.assertFalse(record['credential_redaction'])
        self.assertIn(b'1.234567890123456789', b''.join(self.fixture.stored.values()))

    def test_request_and_response_urls_still_reject_native_paging_version(self):
        query = fixtures.params('country', time_increment=1)
        for request_version, response_version in ((self.paging_version, self.paging_version), ('v21.0', self.paging_version)):
            self.fresh()
            req = fixtures.request(query, url=fixtures.INSIGHTS.replace('/v21.0/', f'/{request_version}/'))
            response_url = req.url.replace(f'/{request_version}/', f'/{response_version}/')
            before = dict(self.fixture.stored)
            with self.subTest(request_version=request_version, response_version=response_version), \
                    self.assertRaisesRegex(RuntimeError, 'account-version-path'):
                self.audit.observe(req, fixtures.response(req, {'data': []}, url=response_url))
            self.assertEqual(before, self.fixture.stored)

    def test_host_account_path_version_and_duplicate_query_rejected_before_page_write(self):
        query = fixtures.params('country', time_increment=1)
        native = self.paging_url(query)
        cases = [native.replace('https:', 'http:'),
                 native.replace('graph.facebook.com', 'graph.facebook.com.evil.invalid'),
                 native.replace('graph.facebook.com', 'graph.facebook.com:443'),
                 native.replace('graph.facebook.com', 'user@graph.facebook.com'),
                 native.replace(snapshot.ACCOUNT, 'act_9'),
                 native.replace('/insights?', '?'), native.replace('/insights?', '/insights/edge?'),
                 native.replace('/insights?', '/insights;extra?'),
                 native.replace(f'/{self.paging_version}/', '/v26.0/'),
                 native.replace(f'/{self.paging_version}/', '/v20.0/'),
                 native + '#fragment', native + '&after=again', native + '&unknown=value']
        for url in cases:
            self.fresh()
            before = dict(self.fixture.stored)
            with self.subTest(url=url), self.assertRaises(RuntimeError):
                self.observe_page(query, url=url)
            self.assertEqual(before, self.fixture.stored)
            self.assertTrue(self.audit.errors)

    def test_native_query_scope_changes_cannot_be_hidden_by_version_admission(self):
        query = fixtures.params('country', time_increment=1)
        changes = [{'fields': 'spend,reach'}, {'level': 'campaign'}, {'limit': 499},
                   {'breakdowns': 'region'}, {'time_increment': 7},
                   {'time_range': json.dumps({'since': '2026-01-02', 'until': fixtures.END})},
                   {'time_range': '{"since":"2026-01-01","since":"2026-01-01","until":"2026-10-13"}'},
                   {'fields': 'spend,spend'}]
        for change in changes:
            self.fresh()
            before = dict(self.fixture.stored)
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                self.observe_page(query, url=self.paging_url(query | change))
            self.assertEqual(before, self.fixture.stored)

    def test_equivalent_json_and_field_order_are_admitted(self):
        query = fixtures.params()
        equivalent = query | {'fields': 'ctr,cpm,cpc,clicks,impressions,spend',
                              'time_range': json.dumps({'until': fixtures.END, 'since': fixtures.START}, separators=(',', ':'))}
        self.observe_page(query, url=self.paging_url(equivalent), rows=[fixtures.row()])
        self.assertEqual([], self.audit.errors)

    def test_missing_changed_or_repeated_cursor_stays_fatal_for_native_version(self):
        query = fixtures.params('country', time_increment=1)
        for direction in ('next', 'previous'):
            self.fresh()
            before = dict(self.fixture.stored)
            with self.subTest(direction=direction), self.assertRaisesRegex(RuntimeError, 'server-cursor-mismatch'):
                self.observe_page(query, url=self.paging_url(query, 'wrong') if direction == 'next' else None,
                                  previous=self.paging_url(query, 'wrong', direction='previous') if direction == 'previous' else None)
            self.assertEqual(before, self.fixture.stored)
        self.fresh()
        self.observe_page(query)
        with self.assertRaisesRegex(RuntimeError, 'nonadvancing-cursor'):
            self.observe_page(query | {'after': 'next'}, rows=[
                fixtures.row('country', start='2026-01-02', end='2026-01-02')])

    def test_paging_credentials_are_removed_without_rewriting_native_money(self):
        token, proof = 'test-private-token/value', 'test-private-proof'
        self.audit = self.fixture.new(credentials=(token, proof))
        self.fixture.currency(self.audit)
        query = fixtures.params('country', time_increment=1, access_token=token, appsecret_proof=proof)
        req = fixtures.request(query)
        native = (b'{"data":[{"date_start":"2026-01-01","date_stop":"2026-01-01",'
                  b'"country":"SK","spend":1.234567890123456789}],"paging":{"cursors":{"after":"next","before":"prior"},"next":'
                  + json.dumps(self.paging_url(query)).encode() + b',"previous":'
                  + json.dumps(self.paging_url(query, 'prior', direction='previous')).encode() + b'}}')
        res = fixtures.response(req, {}, raw=native)
        self.audit.observe(req, res)
        self.assertEqual(native, res.content)
        record = self.audit.pages[-1]
        saved = self.fixture.stored[record['key']]
        self.assertIn(b'1.234567890123456789', saved)
        for direction in ('next', 'previous'):
            public = dict(parse_qsl(urlparse(json.loads(saved)['paging'][direction]).query))
            self.assertNotIn('access_token', public)
            self.assertNotIn('appsecret_proof', public)
        self.assertTrue(record['credential_redaction'])
        self.assertEqual(snapshot.sha(native), record['original_response_sha256'])
        self.assertNotIn(token.encode(), b''.join(self.fixture.stored.values()))
        self.assertNotIn(proof.encode(), b''.join(self.fixture.stored.values()))


class NativeV25PagingVersionTests(NativePagingVersionTests):
    paging_version = 'v25.0'


if __name__ == '__main__':
    unittest.main()
