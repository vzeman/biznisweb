"""Daily country source receipts are additive to original country/daily proof."""
import json
import unittest

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


if __name__ == '__main__':
    unittest.main()
