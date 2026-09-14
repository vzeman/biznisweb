import copy
from io import BytesIO
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts import reporting_migration_host_gate as host


class S3:
    def __init__(self):
        self.objects, self.puts, self.bodies = {}, [], []

    def put_object(self, **kwargs):
        assert kwargs['IfNoneMatch'] == '*'
        assert kwargs['ServerSideEncryption'] == 'AES256'
        assert kwargs['ExpectedBucketOwner'] == host.ACCOUNT
        assert kwargs['Key'] not in self.objects
        self.objects[kwargs['Key']] = kwargs['Body']
        self.puts.append(kwargs['Key'])

    def get_object(self, **kwargs):
        body = BytesIO(self.objects[kwargs['Key']])
        self.bodies.append(body)
        return {'Body': body, 'ServerSideEncryption': 'AES256'}


QUALITY = {'is_partial': False, 'qa_status': 'ok', 'qa_failure_count': 0, 'qa_errors': []}


class HostGateTests(unittest.TestCase):
    def test_inventory_errors_require_same_page_recovery_and_caps_stay_blocked(self):
        import daily_report_runner as runner
        import export_orders
        from gql import Client, gql
        required = {'REPORT_PROJECT': 'vevo', 'REPORT_SKIP_INVOICES': 'true', 'REPORT_SKIP_CREDITNOTE_STORNO_GUARD': 'true',
                    'REPORT_SKIP_EMAIL': 'true', 'REPORT_S3_BUCKET': host.BUCKET, 'REPORT_S3_PREFIX': 'daily-reports/vevo'}
        query = gql('query Orders { getOrderList { data { id } pageInfo { hasNextPage nextCursor } } }')
        reduced = gql('query Orders { getOrderList { data { id } pageInfo { hasNextPage nextCursor } } }')
        page = {'getOrderList': {'data': [{'id': 1}], 'pageInfo': {'hasNextPage': True, 'nextCursor': 'boundary'}}}
        for scenario in ('same-page-recovered', 'different-page', 'cap', 'open-empty'):
            def execute(*_args, **_kwargs):
                execute.calls += 1
                if execute.calls == 1:
                    raise RuntimeError('transient or optional resolver')
                return page if scenario != 'open-empty' else {'getOrderList': {'data': [], 'pageInfo': {'hasNextPage': True, 'nextCursor': 'x'}}}
            execute.calls = 0
            def ordinary_path():
                client = object()
                try:
                    Client.execute(client, query, variable_values={'params': {'cursor': None}})
                except RuntimeError:
                    pass
                try:
                    Client.execute(client, reduced, variable_values={'params': {'cursor': 'other' if scenario == 'different-page' else None}})
                except RuntimeError:
                    pass
                if scenario == 'cap':
                    export_orders.logger.warning('Stopped after max_batches=1')
                # A proven requested-date boundary may end while API has older history.
                runner.s3_upload_outputs('vevo', {})
            with self.subTest(scenario=scenario), patch.dict(host.os.environ, required), \
                 patch.object(Client, 'execute', execute), patch.object(runner, 'main', ordinary_path), \
                 patch.object(host, 'isolate_outputs', return_value={'key': 'private', 'sha256': 'hash'}):
                if scenario == 'same-page-recovered':
                    self.assertEqual('private', host.report_probe(S3(), host.PREFIX + 'a' * 32 + '/', 'a' * 32)['key'])
                else:
                    with self.assertRaisesRegex(RuntimeError, 'incomplete'):
                        host.report_probe(S3(), host.PREFIX + 'a' * 32 + '/', 'a' * 32)

    def test_real_runner_boundaries_block_financial_email_and_graphql_mutation(self):
        import daily_report_runner as runner
        from gql import Client, gql
        import requests
        required = {'REPORT_PROJECT': 'vevo', 'REPORT_SKIP_INVOICES': 'true', 'REPORT_SKIP_CREDITNOTE_STORNO_GUARD': 'true',
                    'REPORT_SKIP_EMAIL': 'true', 'REPORT_S3_BUCKET': host.BUCKET, 'REPORT_S3_PREFIX': 'daily-reports/vevo'}
        actions = [lambda: runner.maybe_run_invoice_automation(), lambda: runner.send_email_ses(),
                   lambda: Client.execute(object(), gql('mutation { preinvoiceOrder(order_num: "synthetic") { id } }')),
                   lambda: requests.Session().send(requests.Request('GET', 'https://vevo.flox.sk/erp/orders/invoices/finalize/12').prepare())]
        for action in actions:
            with self.subTest(action=action), patch.dict(host.os.environ, required), patch.object(runner, 'main', action):
                with self.assertRaisesRegex(RuntimeError, 'probe-forbidden'):
                    host.report_probe(S3(), host.PREFIX + 'a' * 32 + '/', 'a' * 32)

    def test_guarded_runner_keeps_production_input_prefix_and_suppresses_metrics(self):
        import daily_report_runner as runner
        import export_orders
        required = {'REPORT_PROJECT': 'vevo', 'REPORT_SKIP_INVOICES': 'true', 'REPORT_SKIP_CREDITNOTE_STORNO_GUARD': 'true',
                    'REPORT_SKIP_EMAIL': 'true', 'REPORT_S3_BUCKET': host.BUCKET, 'REPORT_S3_PREFIX': 'daily-reports/vevo'}
        def ordinary_path():
            flags = runner.parse_args()
            self.assertTrue(flags.skip_email and flags.skip_invoices and flags.skip_creditnote_storno_guard)
            runner.run_export(project='vevo', from_date='2026-09-01', to_date='2026-09-02', clear_cache=False,
                              no_cache=False, output_tag='migration_' + 'a' * 32)
            runner.put_metric('ReportRunSucceeded', 1, 'vevo', {})
            runner.s3_upload_outputs('vevo', {})
        with patch.dict(host.os.environ, required), patch.object(runner, 'main', ordinary_path), \
             patch.object(export_orders, 'main') as export, patch.object(host, 'isolate_outputs', return_value={'key': 'private', 'sha256': 'hash'}):
            self.assertEqual('private', host.report_probe(S3(), host.PREFIX + 'a' * 32 + '/', 'a' * 32)['key'])
            export.assert_called_once()
            self.assertEqual('daily-reports/vevo', host.os.environ['REPORT_S3_PREFIX'])

    def test_partial_missing_critical_and_malformed_quality_reject(self):
        host.verify_quality(QUALITY)
        for value in (None, {}, {**QUALITY, 'is_partial': True}, {**QUALITY, 'qa_failure_count': True},
                      {**QUALITY, 'qa_status': 'critical'}, {**QUALITY, 'qa_errors': ['failed']}):
            with self.subTest(value=value), self.assertRaises(RuntimeError):
                host.verify_quality(value)

    def test_localhost_identity_precedes_signal_acceptance(self):
        s3 = S3()
        identity = {'release_id': 'a' * 32, 'task_arn': 'task'}
        prefix = host.PREFIX + 'a' * 32 + '/'
        s3.objects[prefix + 'authorize.json'] = host.canonical({
            'phase': 'host-authorized', **identity, 'ready_sha256': host.sha(host.canonical(identity)),
        })
        events = []
        with patch('scripts.order_automation_host_gate.localhost_marker', side_effect=lambda _x: events.append('curl')):
            digest = host.await_authorization(s3, prefix, identity)
        self.assertEqual(['curl'], events)
        self.assertEqual(host.sha(host.canonical(identity)), digest)
        self.assertEqual([prefix + 'markers/ready.json'], s3.puts)
        self.assertTrue(all(body.closed for body in s3.bodies))

    def test_other_task_or_changed_marker_cannot_authorize(self):
        s3, prefix = S3(), host.PREFIX + 'b' * 32 + '/'
        s3.objects[prefix + 'authorize.json'] = b'{}'
        with patch('scripts.order_automation_host_gate.localhost_marker'), self.assertRaisesRegex(RuntimeError, 'binding'):
            host.await_authorization(s3, prefix, {'release_id': 'b' * 32, 'task_arn': 'owned'})

    def test_missing_encryption_closes_body(self):
        body = BytesIO(b'private')
        with patch.object(S3, 'get_object', return_value={'Body': body}):
            with self.assertRaises(RuntimeError):
                host.read_private(S3(), 'anything')
        self.assertTrue(body.closed)

    def test_canonical_outputs_are_isolated_without_live_aliases(self):
        with TemporaryDirectory() as folder:
            root = Path(folder).resolve()
            data = root / 'data' / 'vevo'
            data.mkdir(parents=True)
            html, payload, quality = data / 'tagged.html', data / 'tagged.json', data / 'qa.json'
            html.write_text('<html>synthetic</html>')
            payload.write_text(json.dumps({'project': 'vevo', 'source_health': QUALITY}))
            quality.write_text(json.dumps(QUALITY))
            runner = SimpleNamespace(STABLE_LIVE_ARTIFACT_NAMES={'report_latest.html'},
                PERIOD_LIVE_ARTIFACT_NAMES={'7d': {'payload': 'dashboard_payload_7d.json'}},
                _canonical_live_artifact_paths=lambda *_: {'report_latest.html': html, 'dashboard_payload_7d.json': payload},
                load_data_quality=lambda _: QUALITY)
            s3 = S3()
            prefix = host.PREFIX + 'a' * 32 + '/'
            with patch.object(host, 'ROOT', root):
                manifest = host.isolate_outputs(s3, prefix, runner, {'data_quality_json': quality})
            self.assertEqual(prefix + 'artifacts/output-manifest.json', manifest['key'])
            self.assertTrue(all(key.startswith(prefix) and '/latest/' not in key for key in s3.puts))
            self.assertEqual(4, len(s3.puts))
            self.assertTrue(all(body.closed for body in s3.bodies))

    def test_missing_period_fails_before_any_upload(self):
        runner = SimpleNamespace(STABLE_LIVE_ARTIFACT_NAMES={'report_latest.html'},
            PERIOD_LIVE_ARTIFACT_NAMES={'7d': {'payload': 'dashboard_payload_7d.json'}},
            _canonical_live_artifact_paths=lambda *_: {})
        s3 = S3()
        with self.assertRaisesRegex(RuntimeError, 'artifacts-missing'):
            host.isolate_outputs(s3, host.PREFIX + 'a' * 32 + '/', runner, {})
        self.assertEqual([], s3.puts)

    def test_metadata_binding_rejects_foreign_host_and_image(self):
        metadata = {'Family': 'vevo-reporting-daily', 'LaunchType': 'FARGATE', 'Revision': '88',
                    'TaskARN': f'arn:aws:ecs:{host.REGION}:{host.ACCOUNT}:task/vevo-reporting-cluster/' + 'a' * 32,
                    'Containers': [{'Name': 'reporting', 'ImageID': 'sha256:' + 'c' * 64,
                                    'Networks': [{'IPv4Addresses': ['172.31.1.2']}]}]}
        options = dict(release_id='a' * 32, source_commit='b' * 40, image_digest='sha256:' + 'c' * 64,
                       gate_sha256=host.sha(Path(host.__file__).read_bytes()))
        with patch.object(host.os, 'getcwd', return_value='/app'):
            self.assertEqual('172.31.1.2', host.host_identity(metadata, **options)['private_ip'])
            for field, value in (('Family', 'roy-reporting-daily'), ('LaunchType', 'EC2'), ('TaskARN', 'foreign')):
                changed = copy.deepcopy(metadata)
                changed[field] = value
                with self.subTest(field=field), self.assertRaises(RuntimeError):
                    host.host_identity(changed, **options)


if __name__ == '__main__':
    unittest.main()
