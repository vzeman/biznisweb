import ast
import copy
from datetime import datetime
from decimal import Decimal
from fnmatch import fnmatchcase
import json
import logging
import math
from pathlib import Path
import re
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from urllib.parse import parse_qsl, quote, urlencode, urlparse

import requests

from scripts import reporting_meta_source_snapshot as snapshot
from scripts import reporting_migration_host_gate as host
from scripts.reporting_image_release_policy import PROJECTS


ROOT = Path(__file__).resolve().parents[1]
START, END = '2026-01-01', '2026-10-13'
RELEASE = 'a' * 32
PREFIX = f'data/vevo/reporting/runtime/probes/{RELEASE}/'
INSIGHTS = f'{snapshot.ORIGIN}/{snapshot.VERSION}/{snapshot.ACCOUNT}/insights'
IDENTITY = {
    'marker': 'VEVO_REPORT_PROBE_HOST_OK', 'release_id': RELEASE,
    'source_commit': 'b' * 40, 'image_digest': 'sha256:' + 'c' * 64,
    'task_arn': 'arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/' + 'd' * 32,
    'task_definition': 'arn:aws:ecs:eu-central-1:919341186960:task-definition/vevo-reporting-daily:51',
    'private_ip': '172.31.36.140', 'instance_id': 'N/A:Fargate',
    'service': 'vevo-daily-report-email', 'path': '/app', 'project': 'vevo',
    'gate_sha256': 'e' * 64,
}


def params(kind='daily', start=START, end=END, **extra):
    value = {'fields': 'spend' if kind == 'country' else 'spend,impressions,clicks,cpc,cpm,ctr', 'level': 'account',
             'time_range': json.dumps({'since': start, 'until': end}), 'limit': 500}
    value['breakdowns' if kind == 'country' else 'time_increment'] = 'country' if kind == 'country' else 1
    return {**value, **extra}


def request(parameters, *, url=INSIGHTS, method='GET', headers=None):
    return requests.Request(method, url, params=parameters, headers=headers or {}).prepare()


def response(req, body, *, status=200, raw=None, url=None):
    result = requests.Response()
    result.status_code = status
    result.url = req.url if url is None else url
    result._content = json.dumps(body).encode() if raw is None else raw
    result.request = req
    return result


def row(kind='daily', *, start=START, end=END, spend='1.234567', country='SK'):
    value = {'date_start': start, 'date_stop': start if kind == 'daily' else end, 'spend': spend}
    if kind == 'country':
        value['country'] = country
    return value


def core_method(name):
    """Use the production method body without importing dotenv or creating a client."""
    tree = ast.parse((ROOT / 'facebook_ads.py').read_bytes())
    node = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == name)
    namespace = {'Dict': dict, 'Any': object, 'List': list, 'datetime': datetime,
                 'math': math, 're': re, 'json': json, 'logger': logging.getLogger(__name__),
                 'requests': requests}
    exec(compile(ast.Module(body=[node], type_ignores=[]), '<actual-facebook-method>', 'exec'), namespace)
    return namespace[name]


class MetaSourceSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.folder = TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.source = Path(self.folder.name) / 'source.py'
        self.source.write_bytes(b'# immutable synthetic source\n')
        self.stored = {}
        self.audit = self.new()

    def put(self, key, raw):
        self.assertTrue(key.startswith(PREFIX + 'artifacts/source-inputs/meta/'))
        self.assertNotIn(key, self.stored)
        self.assertIsInstance(raw, bytes)
        self.stored[key] = raw

    def new(self, *, identity=None, ready=None, prefix=PREFIX, put=None, credentials=()):
        identity = copy.deepcopy(IDENTITY if identity is None else identity)
        return snapshot.MetaSourceSnapshot(identity, ready or snapshot.sha(snapshot.canonical(identity)),
            START, END, prefix, put or self.put, credentials=credentials, source_paths=(self.source,))

    def observe(self, kind='daily', *, audit=None, body=None, query=None, **options):
        audit = audit or self.audit
        req = request(params(kind) if query is None else query)
        body = {'data': [row(kind)]} if body is None else body
        res = response(req, body, **options)
        audit.observe(req, res)
        return req, res

    def currency(self, audit=None, *, currency='EUR', account=snapshot.ACCOUNT):
        audit = audit or self.audit
        req = request({'fields': 'currency'}, url=INSIGHTS.removesuffix('/insights'))
        audit.observe(req, response(req, {'currency': currency, 'id': account}))

    def fill(self, audit=None):
        audit = audit or self.audit
        self.currency(audit)
        self.observe(audit=audit)
        for start, end in audit.periods.values():
            self.observe('country', audit=audit, query=params('country', start, end),
                         body={'data': [row('country', start=start, end=end)]})

    def assert_incomplete(self, audit=None):
        with self.assertRaises(RuntimeError):
            (audit or self.audit).finalize()
        self.assertFalse(any(key.endswith('source-manifest.json') for key in self.stored))

    def test_complete_manifest_binds_native_bytes_groups_host_and_no_physical_retry_claim(self):
        self.fill()
        reference = self.audit.finalize()
        manifest_raw = self.stored[reference['key']]
        self.assertEqual(snapshot.sha(manifest_raw), reference['sha256'])
        self.assertEqual(len(manifest_raw), reference['size'])
        manifest = json.loads(manifest_raw)
        self.assertEqual(IDENTITY, manifest['host_identity'])
        self.assertTrue(manifest['complete'])
        self.assertIsNone(manifest['physical_http_attempt_count'])
        self.assertEqual(6, manifest['observed_session_send_responses'])
        self.assertEqual(5, len(manifest['query_groups']))
        from scripts.deploy_vevo_report import diagnostic_policy
        for policy in (PROJECTS['vevo'].probe_policy(RELEASE), diagnostic_policy(RELEASE)):
            writes = [statement for statement in policy['Statement']
                      if statement['Effect'] == 'Allow' and 's3:PutObject' in statement['Action']]
            for key in self.stored:
                self.assertTrue(any(fnmatchcase(f'arn:aws:s3:::{host.BUCKET}/{key}', resource)
                                    for statement in writes for resource in statement['Resource']))
            self.assertFalse(any(fnmatchcase(f'arn:aws:s3:::{host.BUCKET}/{PREFIX}review/independent-source.json', resource)
                                 for statement in writes for resource in statement['Resource']))
        for group in manifest['query_groups'].values():
            self.assertTrue(group['complete'])
            if group['kind'] == 'country':
                self.assertEqual(1, group['currency_metadata_page'])
        for record in manifest['native_responses']:
            self.assertEqual(record['sha256'], snapshot.sha(self.stored[record['key']]))
            if record['kind'] != 'identity':
                self.assertEqual('1.234567', json.loads(self.stored[record['key']])['data'][0]['spend'])
        with self.assertRaises(RuntimeError):
            self.audit.finalize()
        self.stored.clear()
        audit = self.new()
        native = b' \n{"data": [{"date_start":"2026-01-01", "date_stop":"2026-01-01", "spend":1.234567890123456789}]}\n '
        req = request(params())
        res = response(req, {}, raw=native)
        audit.observe(req, res)
        self.assertEqual(native, res.content)
        native_records = [raw for key, raw in self.stored.items() if key.endswith('-response.json')]
        self.assertEqual([native], native_records)

    def test_foreign_host_marker_task_service_and_project_rejected_before_write(self):
        for key, value in [('project', 'roy'), ('marker', 'OTHER'), ('path', '/other'),
                           ('service', 'roy-daily-report-email'), ('source_commit', 'bad'),
                           ('task_arn', IDENTITY['task_arn'].replace('919341186960', '111111111111')),
                           ('task_definition', IDENTITY['task_definition'].replace('vevo-', 'roy-'))]:
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                self.new(identity={**IDENTITY, key: value})
        self.assertEqual({}, self.stored)

    def test_ready_digest_prefix_and_calendar_boundaries_are_required(self):
        for options in ({'ready': '0' * 64}, {'prefix': PREFIX.replace('/vevo/', '/roy/')}):
            with self.subTest(options=options), self.assertRaises(RuntimeError):
                self.new(**options)
        for start, end in [('2026-02-30', END), (END, START)]:
            with self.subTest(start=start), self.assertRaises((RuntimeError, ValueError)):
                snapshot.MetaSourceSnapshot(IDENTITY, snapshot.sha(snapshot.canonical(IDENTITY)),
                    start, end, PREFIX, self.put, source_paths=(self.source,))

    def test_source_or_host_identity_change_cannot_produce_manifest(self):
        self.fill()
        self.source.write_bytes(b'# changed\n')
        self.assert_incomplete()
        self.source.write_bytes(b'# immutable synthetic source\n')
        self.audit.identity['private_ip'] = '172.31.1.2'
        self.assert_incomplete()

    def test_country_requires_preceding_correct_native_currency_identity(self):
        with self.assertRaisesRegex(RuntimeError, 'currency-missing'):
            self.observe('country')
        self.assert_incomplete()
        for currency, account in [('HUF', snapshot.ACCOUNT), ('EUR', 'act_1')]:
            audit = self.new()
            with self.subTest(currency=currency, account=account), self.assertRaises(RuntimeError):
                self.currency(audit, currency=currency, account=account)

    def test_missing_country_window_or_full_daily_source_stays_incomplete(self):
        self.currency()
        self.observe()
        self.observe('country')
        self.assert_incomplete()
        self.stored.clear()
        audit = self.new()
        self.currency(audit)
        for start, end in audit.periods.values():
            self.observe('country', audit=audit, query=params('country', start, end),
                         body={'data': [row('country', start=start, end=end)]})
        self.assert_incomplete(audit)
        self.observe(audit=audit, query=params(fields='spend,impressions,clicks,cpc,cpm,ctr,reach,frequency,unique_clicks,cost_per_unique_click'))
        self.assert_incomplete(audit)  # Optional detailed metrics cannot stand in for actual daily-spend input.

    def test_paging_auth_redacted_but_raw_response_unchanged_and_cursor_chain_preserved(self):
        token = 'secret-token/with+symbols'
        audit = self.new(credentials=(token,))
        p = params(access_token=token)
        req = request(p)
        next_url = INSIGHTS + '?' + urlencode({**p, 'after': 'cursor-2'})
        res = response(req, {'data': [row()], 'paging': {'cursors': {'after': 'cursor-2'}, 'next': next_url}})
        original = res.content
        audit.observe(req, res)
        self.assertEqual(original, res.content)
        self.assertEqual(token, dict(parse_qsl(urlparse(req.url).query))['access_token'])
        saved = json.loads(self.stored[PREFIX + 'artifacts/source-inputs/meta/0001-response.json'])
        public_next = dict(parse_qsl(urlparse(saved['paging']['next']).query))
        self.assertNotIn('access_token', public_next)
        self.assertEqual('cursor-2', public_next['after'])
        audit.observe(request({**p, 'after': 'cursor-2'}), response(request({**p, 'after': 'cursor-2'}),
            {'data': [row(start='2026-01-02')]}))
        self.assertTrue(next(iter(audit.groups.values()))['complete'])
        self.assertNotIn(token.encode(), b''.join(self.stored.values()))
        self.assertNotIn(quote(token, safe='').encode(), b''.join(self.stored.values()))

    def test_missing_next_page_prevents_completion(self):
        self.fill()
        req = request(params(fields='spend,clicks'))
        next_url = INSIGHTS + '?' + urlencode({**params(fields='spend,clicks'), 'after': 'unfinished'})
        self.audit.observe(req, response(req, {'data': [row()],
            'paging': {'cursors': {'after': 'unfinished'}, 'next': next_url}}))
        self.assert_incomplete()

    def test_outgoing_or_incoming_wrong_cursor_is_sticky(self):
        for mismatch in ('outgoing', 'incoming'):
            self.stored.clear()
            audit = self.new()
            req = request(params())
            url = INSIGHTS + '?' + urlencode({**params(), 'after': 'bad' if mismatch == 'outgoing' else 'next'})
            first = response(req, {'data': [row()], 'paging': {'cursors': {'after': 'next'}, 'next': url}})
            with self.subTest(mismatch=mismatch):
                if mismatch == 'outgoing':
                    with self.assertRaises(RuntimeError):
                        audit.observe(req, first)
                else:
                    audit.observe(req, first)
                    with self.assertRaises(RuntimeError):
                        self.observe(audit=audit, query=params(after='wrong'), body={'data': [row(start='2026-01-02')]})
                self.assert_incomplete(audit)

    def test_duplicate_native_country_or_date_across_pages_is_rejected(self):
        for kind in ('daily', 'country'):
            self.stored.clear()
            audit = self.new()
            self.currency(audit)
            p = params(kind)
            req = request(p)
            next_url = INSIGHTS + '?' + urlencode({**p, 'after': 'next'})
            audit.observe(req, response(req, {'data': [row(kind)],
                'paging': {'cursors': {'after': 'next'}, 'next': next_url}}))
            with self.subTest(kind=kind), self.assertRaisesRegex(RuntimeError, 'duplicate-native-row'):
                self.observe(kind, audit=audit, query={**p, 'after': 'next'})
            self.assert_incomplete(audit)

    def test_repeated_finished_query_does_not_overwrite_original_evidence(self):
        self.observe()
        original = dict(self.stored)
        with self.assertRaisesRegex(RuntimeError, 'repeated-or-wrong-page'):
            self.observe()
        self.assertEqual(original, self.stored)
        self.assert_incomplete()

    def test_invalid_calendar_and_out_of_period_rows_are_rejected(self):
        for day in ('2026-02-30', '2025-12-31', '2026-10-14'):
            with self.subTest(day=day), self.assertRaises((RuntimeError, ValueError)):
                self.observe(audit=self.new(), body={'data': [row(start=day)]})

    def test_invalid_money_values_never_become_zero(self):
        for value in ('NaN', 'Infinity', '-0.01', True, None, 'not-money'):
            with self.subTest(value=value), self.assertRaises(RuntimeError):
                self.observe(audit=self.new(), body={'data': [row(spend=value)]})
        self.assertEqual({}, self.stored)

    def test_http_empty_oversize_and_duplicate_json_rejections_are_sticky(self):
        cases = [{'status': 503}, {'raw': b''}, {'raw': b'{"data":[],"data":[]}'}, {'raw': b'x' * 81}]
        for options in cases:
            audit = self.new()
            with self.subTest(options=options), patch.object(snapshot, 'MAX_BODY', 80), self.assertRaises((RuntimeError, ValueError)):
                self.observe(audit=audit, **options)
            self.assert_incomplete(audit)

    def test_unknown_response_fields_and_graphql_like_error_not_silenced(self):
        for body in ({'data': [row()], 'error': {'message': 'failed'}},
                     {'data': [{**row(), 'unrequested_private_field': 'value'}]}):
            with self.subTest(body=body), self.assertRaises(RuntimeError):
                self.observe(audit=self.new(), body=body)

    def test_nonpaging_credentials_literal_json_escaped_or_url_encoded_are_fatal(self):
        secret = 'private-"token/+with spaces'
        for reflected in (secret, json.dumps(secret)[1:-1], quote(secret, safe=''),
                          quote(secret, safe='').replace('%2F', '%2f')):
            audit = self.new(credentials=(secret,))
            with self.subTest(reflected=reflected), self.assertRaisesRegex(RuntimeError, 'credential-reflection'):
                self.observe(audit=audit, query=params(fields='spend,clicks'), body={'data': [{**row(), 'clicks': reflected}]})
            self.assert_incomplete(audit)
        self.assertEqual({}, self.stored)

    def test_credential_in_key_or_public_cursor_is_rejected(self):
        secret = 'test-secret-long-token'
        for in_query in (True, False):
            audit = self.new(credentials=(secret,))
            with self.subTest(in_query=in_query), self.assertRaisesRegex(RuntimeError, 'credential-reflection'):
                self.observe(audit=audit, query=params(after=secret) if in_query else params(),
                             body={'data': [row()]} if in_query else {'data': [{**row(), secret: 'value'}]})
        self.assertEqual({}, self.stored)

    def test_bearer_and_query_credentials_are_never_persisted(self):
        token, proof = 'bearer-private-value', 'appsecret-private-value'
        req = request(params(access_token=token, appsecret_proof=proof), headers={'Authorization': 'Bearer ' + token})
        self.audit.observe(req, response(req, {'data': [row()]}))
        stored = b''.join(self.stored.values())
        self.assertNotIn(token.encode(), stored)
        self.assertNotIn(proof.encode(), stored)
        receipt = json.loads(self.stored[PREFIX + 'artifacts/source-inputs/meta/0001-receipt.json'])
        self.assertNotIn('access_token', receipt['public_params'])
        self.assertNotIn('appsecret_proof', receipt['public_params'])

    def test_selected_wrong_account_version_origin_or_method_rejected(self):
        cases = [(INSIGHTS.replace(snapshot.ACCOUNT, 'act_9'), 'GET', None),
                 (INSIGHTS.replace('/v21.0/', '/v20.0/'), 'GET', None),
                 (INSIGHTS, 'POST', None), (INSIGHTS, 'GET', 'https://evil.invalid/result')]
        for url, method, target in cases:
            audit = self.new()
            req = request(params(), url=url, method=method)
            with self.subTest(url=url, method=method), self.assertRaises(RuntimeError):
                audit.observe(req, response(req, {'data': [row()]}, url=target))
            self.assert_incomplete(audit)
        duplicate = request(params())
        duplicate.url += '&limit=500'
        with self.assertRaisesRegex(RuntimeError, 'duplicate-query-key'):
            self.audit.observe(duplicate, response(duplicate, {'data': [row()]}))

    def test_actual_core_ad_performance_and_hourly_queries_opt_out_before_strict_params(self):
        observed = []
        client = SimpleNamespace(is_configured=True, base_url=snapshot.ORIGIN + '/v21.0', ad_account_id=snapshot.ACCOUNT)
        def get_json(url, query, context):
            req = request(query, url=url)
            observed.append(query)
            self.assertFalse(self.audit.observe(req, response(req, {'data': []})))
            self.audit.observe_failure(req)
            return {'data': []}
        client._get_json = get_json
        client._get_all_rows = lambda *args: core_method('_get_all_rows')(client, *args)
        for name in ('get_ads_performance', 'get_hourly_stats'):
            self.assertEqual([], core_method(name)(client, datetime(2026, 1, 1), datetime(2026, 10, 13)))
        for _ in range(2):
            self.assertEqual({}, core_method('get_daily_metrics')(client, datetime(2026, 1, 1), datetime(2026, 10, 13)))
        self.assertEqual('spend_descending', observed[0]['sort'])
        self.assertEqual('hourly_stats_aggregated_by_advertiser_time_zone', observed[1]['breakdowns'])
        self.assertEqual(observed[2], observed[3])
        self.assertEqual('spend,impressions,clicks,cpc,cpm,ctr,reach,frequency,unique_clicks,cost_per_unique_click',
                         observed[2]['fields'])
        self.assertEqual([], self.audit.errors)
        self.assertEqual({}, self.stored)

    def test_selected_transport_failure_caught_by_caller_still_blocks_finalize(self):
        self.fill()
        self.audit.observe_failure(request(params()))
        self.assert_incomplete()

    def test_private_body_or_receipt_callback_failure_is_sticky(self):
        for fail_at in (1, 2):
            self.stored.clear()
            calls = []
            def fail(key, raw):
                calls.append(key)
                if len(calls) == fail_at:
                    raise OSError('synthetic private store failure')
                self.put(key, raw)
            audit = self.new(put=fail)
            with self.subTest(fail_at=fail_at), self.assertRaises(OSError):
                self.observe(audit=audit)
            self.assert_incomplete(audit)

    def test_existing_private_writer_full_readback_failure_prevents_receipt(self):
        from io import BytesIO
        class CorruptS3:
            def put_object(inner, **kwargs):
                self.assertEqual('*', kwargs['IfNoneMatch'])
                self.assertEqual('AES256', kwargs['ServerSideEncryption'])
                self.assertEqual(host.ACCOUNT, kwargs['ExpectedBucketOwner'])
            def get_object(inner, **kwargs):
                return {'Body': BytesIO(b'corrupted'), 'ServerSideEncryption': 'AES256'}
        audit = self.new(put=lambda key, raw: host.put_private(CorruptS3(), key, raw))
        with self.assertRaises(RuntimeError):
            self.observe(audit=audit)
        self.assert_incomplete(audit)

    def test_manifest_callback_failure_cannot_return_complete_reference(self):
        def fail_manifest(key, raw):
            if key.endswith('source-manifest.json'):
                raise OSError('manifest write rejected')
            self.put(key, raw)
        audit = self.new(put=fail_manifest)
        self.fill(audit)
        with self.assertRaises(OSError):
            audit.finalize()
        self.assertFalse(audit.finished)
        self.assertFalse(any(key.endswith('source-manifest.json') for key in self.stored))
        audit.put = self.put
        self.assert_incomplete(audit)

    def test_actual_core_country_method_uses_same_native_rows_queries_and_amounts(self):
        client = SimpleNamespace(is_configured=True, base_url=snapshot.ORIGIN + '/v21.0', ad_account_id=snapshot.ACCOUNT)
        original_bodies = []
        def get_json(url, query, context):
            req = request(query, url=url)
            body = {'currency': 'EUR', 'id': snapshot.ACCOUNT} if not url.endswith('/insights') else {
                'data': [row('country', spend='1.234567'), row('country', spend='2.345678', country='CZ')]}
            res = response(req, body)
            original_bodies.append(res.content)
            self.audit.observe(req, res)
            self.assertEqual(original_bodies[-1], res.content)
            return res.json()
        client._get_json = get_json
        client._get_all_rows = lambda *args: core_method('_get_all_rows')(client, *args)
        result = core_method('get_country_spend')(client, datetime(2026, 1, 1), datetime(2026, 10, 13))
        self.assertEqual('ok', result['status'])
        self.assertEqual({'sk': 1.234567, 'cz': 2.345678}, result['spend_by_country'])
        self.assertEqual(Decimal('3.580245'), Decimal(str(result['total_spend'])))
        self.assertEqual(2, len(original_bodies))

    def test_host_probe_returns_original_response_and_roy_keeps_snapshot_disabled(self):
        import daily_report_runner as runner
        request_value = request(params())
        original_response = response(request_value, {'data': [row()]})
        marker = {'key': PREFIX + 'artifacts/source-inputs/meta/source-manifest.json', 'sha256': 'f' * 64, 'size': 99}
        fake = SimpleNamespace(observe=lambda req, res: self.assertIs(original_response, res),
                               observe_failure=lambda req: self.fail('unexpected transport failure'),
                               finalize=lambda: marker)
        env = {'REPORT_PROJECT': 'vevo', 'REPORT_SKIP_INVOICES': 'true', 'REPORT_SKIP_CREDITNOTE_STORNO_GUARD': 'true',
               'REPORT_SKIP_EMAIL': 'true', 'REPORT_S3_BUCKET': host.BUCKET, 'REPORT_S3_PREFIX': 'daily-reports/vevo'}
        def ordinary():
            self.assertIs(original_response, requests.Session().send(request_value))
            runner.s3_upload_outputs('vevo', {})
        with patch.dict(host.os.environ, env), patch.object(requests.Session, 'send', return_value=original_response), \
             patch.object(runner, 'main', ordinary), patch.object(host, 'isolate_outputs', return_value={'key': 'private'}):
            result = host.report_probe(None, PREFIX, RELEASE, meta_snapshot=fake)
        self.assertEqual(marker, result['meta_source_manifest'])
        roy_identity = {'release_id': RELEASE}
        artifact = SimpleNamespace(as_dict=lambda: {})
        with patch.dict(host.os.environ, {'REPORT_FROM_DATE': START, 'REPORT_TO_DATE': END}), \
             patch.object(runner, 'build_artifact_set', return_value=artifact), \
             patch.object(snapshot, 'MetaSourceSnapshot') as constructor, patch.object(host, 'report_probe', return_value={}) as probe:
            host.run_authorized_probe(None, PROJECTS['roy'].probe_prefix + RELEASE + '/', roy_identity, 'not-used', policy=PROJECTS['roy'])
        constructor.assert_not_called()
        self.assertIsNone(probe.call_args.kwargs['meta_snapshot'])


if __name__ == '__main__':
    unittest.main()
