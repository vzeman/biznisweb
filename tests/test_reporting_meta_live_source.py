"""Live native input receipts must precede publication; all transports are fake."""
from contextlib import ExitStack
import copy
from io import BytesIO
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import sys
import unittest
from unittest.mock import Mock, patch
from urllib.parse import parse_qsl, urlparse

import requests

from scripts import reporting_meta_source_snapshot as source
from scripts import vevo_report_image_host as live
from scripts.reporting_image_release_policy import PROJECTS
from tests.test_reporting_meta_source_snapshot import IDENTITY, params, request, response, row

START, END = '2026-01-01', '2026-10-13'
LIVE_IDENTITY = {**IDENTITY, 'marker':'VEVO_REPORT_IMAGE_LIVE_HOST_OK', 'report_to_date':END,
                 'skip_email':True,'skip_invoices':True,'skip_inline_guard':True}
BASE = 'daily-reports/vevo/source-inputs/'+IDENTITY['release_id']+'/'


def rejected_publication():
    health = {'is_partial':False,'qa_status':'critical','qa_failure_count':1,'qa_errors':['Geo confidence'],
        'qa':{'geo':{'status':'error','failure_count':1,'failures':['private row must not be copied'],
            'spend_attribution':{'facebook_ads':{'status':'unavailable','total_spend':38003.41,
                'validation_error':'country spend does not reconcile to daily/account spend'}}}},
        'sources':{'facebook_ads':{'status':'ok','healthy':True,'message':'secret@example.test'}}}
    return {'schema_version':1,'project':'vevo','report_from_date':START,'report_to_date':END,
        'periods':{key:{'read_status':'available','date_from':START,'date_to':END,
            'source_health':copy.deepcopy(health),'invalid_fields':['source_health.qa_status','source_health.qa_failure_count']}
            for key in ('latest','7d','30d','90d')}}


class LiveSourceTests(unittest.TestCase):
    def setUp(self):
        self.folder = TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.fixture = Path(self.folder.name)/'source.py'
        self.fixture.write_text('# synthetic immutable source\n')
        self.objects = {}

    def put(self, key, raw):
        self.assertTrue(key.startswith(BASE+'meta/'))
        self.assertNotIn(key,self.objects)
        self.objects[key] = raw

    def capture(self, identity=None, **kwargs):
        identity = copy.deepcopy(identity or LIVE_IDENTITY)
        return source.MetaSourceSnapshot(identity,source.sha(source.canonical(identity)),START,END,BASE,self.put,
            capture_mode='live',source_paths=(self.fixture,),**kwargs)

    def fill(self, capture):
        req = request({'fields':'currency'},url=f'{source.ORIGIN}/{source.VERSION}/{source.ACCOUNT}')
        capture.observe(req,response(req,{'currency':'EUR','id':source.ACCOUNT}))
        req = request(params())
        capture.observe(req,response(req,{'data':[row()]}))
        for start,end in capture.periods.values():
            req = request(params('country',start,end))
            capture.observe(req,response(req,{'data':[row('country',start=start,end=end)]}))

    def test_true_live_identity_mode_sink_prefix_and_native_manifest(self):
        capture = self.capture()
        self.fill(capture)
        reference = capture.finalize()
        self.assertEqual(BASE+'meta/source-manifest.json',reference['key'])
        manifest = json.loads(self.objects[reference['key']])
        self.assertEqual('live',manifest['capture_mode'])
        self.assertEqual(LIVE_IDENTITY,manifest['host_identity'])
        self.assertIsNone(manifest['physical_http_attempt_count'])
        self.assertFalse(manifest['provider_queries_changed'])
        self.assertFalse(manifest['financial_calculation_changed'])

    def test_live_identity_requires_exact_true_report_only_flags_and_date(self):
        for key,value in [('skip_email',False),('skip_invoices',1),('skip_inline_guard',False),
                          ('report_to_date','2026-10-12'),('marker','VEVO_REPORT_PROBE_HOST_OK')]:
            with self.subTest(key=key),self.assertRaises(RuntimeError):
                self.capture({**LIVE_IDENTITY,key:value})
        missing = dict(LIVE_IDENTITY)
        missing.pop('skip_email')
        with self.assertRaises(RuntimeError):
            self.capture(missing)
        self.assertEqual({},self.objects)

    def test_probe_live_and_foreign_scope_are_not_interchangeable(self):
        for mode,prefix,identity in [('probe',BASE,LIVE_IDENTITY),
                                    ('live',BASE.replace('/vevo/','/roy/'),LIVE_IDENTITY),
                                    ('live','data/vevo/reporting/runtime/probes/'+IDENTITY['release_id']+'/',LIVE_IDENTITY)]:
            with self.subTest(mode=mode,prefix=prefix),self.assertRaises(RuntimeError):
                source.MetaSourceSnapshot(identity,source.sha(source.canonical(identity)),START,END,prefix,self.put,
                    capture_mode=mode,source_paths=(self.fixture,))

    def test_default_live_source_hashes_include_both_host_entries_and_client(self):
        capture = source.MetaSourceSnapshot(LIVE_IDENTITY,source.sha(source.canonical(LIVE_IDENTITY)),
            START,END,BASE,self.put,capture_mode='live')
        self.assertEqual({'reporting_meta_source_snapshot.py','reporting_migration_host_gate.py',
                          'vevo_report_image_host.py','facebook_ads.py'},{path.name for path in capture.sources})

    def test_live_token_stays_in_memory_and_original_paging_response_is_unchanged(self):
        token = 'synthetic-live-token/with+symbols'
        capture = self.capture(credentials=(token,))
        query = params(access_token=token)
        req = request(query,headers={'Authorization':'Bearer '+token})
        body = {'data':[row()], 'paging':{'cursors':{'after':'next'},
            'next':req.url+'&after=next'}}
        reply = response(req,body)
        original = reply.content
        capture.observe(req,reply)
        self.assertEqual(original,reply.content)
        self.assertEqual(token,dict(parse_qsl(urlparse(req.url).query))['access_token'])
        saved = json.loads(self.objects[BASE+'meta/0001-response.json'])
        self.assertNotIn('access_token',dict(parse_qsl(urlparse(saved['paging']['next']).query)))
        self.assertNotIn(token.encode(),b''.join(self.objects.values()))
        with self.assertRaises(RuntimeError):
            capture.finalize()

    def test_caught_selected_failure_still_prevents_live_source_completion(self):
        capture = self.capture()
        req = request(params())
        with self.assertRaises(RuntimeError):
            capture.observe(req,response(req,{'data':[row(spend='not-money')]}))
        self.fill(capture)
        with self.assertRaises(RuntimeError):
            capture.finalize()
        self.assertFalse(any(key.endswith('source-manifest.json') for key in self.objects))


class LiveRunnerTests(unittest.TestCase):
    def setUp(self):
        self.events = []
        self.flags = SimpleNamespace(project='vevo',from_date=START,to_date=END,timezone='Europe/Bratislava',
            no_cache=True,clear_cache=True,skip_email=True,skip_invoices=True,
            skip_creditnote_storno_guard=True,skip_export=False,output_tag='')
        self.upload = Mock(side_effect=lambda *_a,**_k:self.events.append('upload') or {'generation_id':'synthetic'})
        self.runner = SimpleNamespace(parse_args=Mock(return_value=self.flags),normalize_date=lambda v:v,
            resolve_to_date=lambda v,_tz:v,run_export=Mock(),s3_upload_outputs=self.upload)
        self.manifest = {'key':BASE+'meta/source-manifest.json','sha256':'f'*64,'size':17}
        self.fake = SimpleNamespace(observe=Mock(),observe_failure=Mock(),
            finalize=Mock(side_effect=lambda:self.events.append('finalize') or self.manifest))
        self.runner.main = self.ordinary
        self.original_reply = response(request(params()),{'data':[row()]})
        self.private = Mock(side_effect=self.private_write)
        self.transport_error = None

    def ordinary(self):
        self.runner.run_export(project='vevo',from_date=START,to_date=END,clear_cache=True,no_cache=True,output_tag='')
        return self.runner.s3_upload_outputs('vevo',{'synthetic':'paths'})

    def private_write(self,*args,**kwargs):
        self.assertEqual(BASE,args[1])
        self.assertEqual(BASE+'source-complete.json',args[2])
        raw = next(value for value in args if isinstance(value,bytes))
        marker = json.loads(raw)
        self.assertEqual('live-native-inputs-complete',marker['phase'])
        self.assertEqual('live',marker['capture_mode'])
        self.assertEqual(LIVE_IDENTITY,marker['host_identity'])
        self.assertEqual(source.sha(source.canonical(LIVE_IDENTITY)),marker['host_identity_sha256'])
        self.assertEqual(self.manifest,marker['meta_source_manifest'])
        self.assertFalse(marker['publication_verified'])
        self.events.append('source-marker')

    def execute(self):
        import export_orders
        def exporter():
            self.events.append('export')
            self.assertEqual(['export_orders.py','--project','vevo','--from-date',START,'--to-date',END,
                              '--clear-cache','--no-cache'],sys.argv)
            self.assertIs(self.original_reply,requests.Session().send(request(params())))
        with ExitStack() as stack:
            constructor = stack.enter_context(patch.object(source,'MetaSourceSnapshot',return_value=self.fake))
            stack.enter_context(patch('boto3.client',return_value=object()))
            stack.enter_context(patch.object(live,'put_live_source',self.private))
            stack.enter_context(patch.object(requests.Session,'send',return_value=self.original_reply,side_effect=self.transport_error))
            stack.enter_context(patch.object(export_orders,'main',side_effect=exporter))
            result = live.run_live_report(self.runner,copy.deepcopy(LIVE_IDENTITY),live.report_arguments(END))
        return result,constructor

    def test_exact_export_cli_original_response_and_source_before_uploader(self):
        result,constructor = self.execute()
        self.assertEqual({'generation_id':'synthetic'},result)
        self.assertEqual(['export','finalize','source-marker','upload'],self.events)
        self.assertEqual('live',constructor.call_args.kwargs['capture_mode'])
        self.assertEqual(BASE,constructor.call_args.args[4])
        self.fake.observe.assert_called_once()
        self.assertIs(self.original_reply,self.fake.observe.call_args.args[1])

    def test_every_unsafe_runner_flag_rejects_before_snapshot_or_upload(self):
        for key,value in [('project','roy'),('no_cache',False),('clear_cache',False),('skip_email',False),
                          ('skip_invoices',False),('skip_creditnote_storno_guard',False),
                          ('skip_export',True),('output_tag','other'),('to_date','2026-10-12')]:
            original = getattr(self.flags,key)
            setattr(self.flags,key,value)
            with self.subTest(key=key),self.assertRaises(RuntimeError):
                self.execute()
            setattr(self.flags,key,original)
        self.upload.assert_not_called()

    def test_missing_publication_is_not_a_success(self):
        self.runner.main = lambda:None
        with self.assertRaisesRegex(RuntimeError,'not-published'):
            self.execute()
        self.upload.assert_not_called()

    def test_exact_export_arguments_are_enforced(self):
        self.runner.main = lambda:self.runner.run_export(project='vevo',from_date=START,to_date=END,
            clear_cache=False,no_cache=True,output_tag='')
        with self.assertRaisesRegex(RuntimeError,'export-context'):
            self.execute()
        self.upload.assert_not_called()

    def test_finalization_failure_prevents_private_marker_and_upload(self):
        self.fake.finalize.side_effect = RuntimeError('synthetic rejected input')
        with self.assertRaisesRegex(RuntimeError,'rejected input'):
            self.execute()
        self.private.assert_not_called()
        self.upload.assert_not_called()

    def test_selected_transport_failure_is_reported_and_never_uploads(self):
        self.transport_error = RuntimeError('synthetic transport failure')
        with self.assertRaisesRegex(RuntimeError,'transport failure'):
            self.execute()
        self.fake.observe_failure.assert_called_once()
        self.fake.finalize.assert_not_called()
        self.upload.assert_not_called()

    def test_private_marker_failure_prevents_live_upload(self):
        self.private.side_effect = RuntimeError('synthetic private readback failure')
        with self.assertRaisesRegex(RuntimeError,'private readback'):
            self.execute()
        self.upload.assert_not_called()

    def test_duplicate_uploader_is_rejected_before_second_finalization(self):
        def twice():
            self.ordinary()
            self.runner.s3_upload_outputs('vevo',{})
        self.runner.main = twice
        with self.assertRaisesRegex(RuntimeError,'source-publication'):
            self.execute()
        self.assertEqual(1,self.upload.call_count)
        self.assertEqual(1,self.fake.finalize.call_count)

    def test_wrong_project_upload_is_rejected_before_financial_publication(self):
        self.runner.main = lambda:self.runner.s3_upload_outputs('roy',{})
        with self.assertRaisesRegex(RuntimeError,'source-publication'):
            self.execute()
        self.fake.finalize.assert_not_called()
        self.upload.assert_not_called()

    def test_prepublication_quality_rejection_retains_four_periods_and_original_exception(self):
        failure = RuntimeError('private original error must not be copied')
        failure.publication_diagnostics = rejected_publication()
        self.runner.main = Mock(side_effect=failure)
        captured = {}
        self.private.side_effect = lambda _s3,_prefix,key,raw:captured.setdefault(key,raw)
        with self.assertRaises(RuntimeError) as caught:
            self.execute()
        self.assertIs(failure,caught.exception)
        self.assertEqual({BASE+'diagnostics/publication.json',BASE+'diagnostics/failed.json'},set(captured))
        saved = json.loads(captured[BASE+'diagnostics/publication.json'])
        self.assertEqual({'latest','7d','30d','90d'},set(saved['periods']))
        marker = json.loads(captured[BASE+'diagnostics/failed.json'])
        self.assertFalse(marker['publication_attempted'])
        self.assertEqual('live-report-rejected-before-publication',marker['phase'])
        self.assertEqual(source.sha(source.canonical(LIVE_IDENTITY)),marker['localhost_marker_sha256'])
        self.assertEqual(source.sha(source.canonical(failure.publication_diagnostics)),marker['diagnostic']['original_diagnostics_sha256'])
        self.upload.assert_not_called()
        self.fake.finalize.assert_not_called()
        self.assertNotIn(b'private original error',b''.join(captured.values()))
        self.assertNotIn(b'secret@example.test',b''.join(captured.values()))

    def test_diagnostic_write_failure_keeps_original_exception_and_logs_only_fixed_message(self):
        failure = RuntimeError('original private rejection')
        self.runner.main = Mock(side_effect=failure)
        with patch.object(live,'retain_live_failure',side_effect=RuntimeError('private S3 failure')), \
             patch('builtins.print') as output, self.assertRaises(RuntimeError) as caught:
            self.execute()
        self.assertIs(failure,caught.exception)
        output.assert_called_once_with('REPORT_LIVE_FAILURE_DIAGNOSTICS_UNAVAILABLE',flush=True)
        self.upload.assert_not_called()

    def test_ambiguous_uploader_failure_does_not_claim_prepublication_rejection(self):
        self.upload.side_effect = RuntimeError('upload failed after possible writes')
        with patch.object(live,'retain_live_failure') as retain, self.assertRaisesRegex(RuntimeError,'upload failed'):
            self.execute()
        retain.assert_not_called()
        self.assertEqual(1,self.upload.call_count)

    def test_roy_uses_original_cli_without_meta_factory_or_writer(self):
        self.runner.main = Mock(return_value='roy-result')
        with patch.object(source,'MetaSourceSnapshot') as constructor:
            result = live.run_live_report(self.runner,{},live.report_arguments(END,policy=PROJECTS['roy']),policy=PROJECTS['roy'])
        self.assertEqual('roy-result',result)
        constructor.assert_not_called()
        self.assertEqual('roy',live.report_arguments(END,policy=PROJECTS['roy'])[2])


class LivePrivateWriterTests(unittest.TestCase):
    class S3:
        def __init__(self, *, corrupt=False, encrypted=True):
            self.calls,self.objects,self.bodies = [],{},[]
            self.corrupt,self.encrypted = corrupt,encrypted

        def put_object(self,**kwargs):
            self.calls.append(('put',kwargs))
            if kwargs['Key'] in self.objects:
                raise RuntimeError('synthetic conditional conflict')
            self.objects[kwargs['Key']] = kwargs['Body']

        def get_object(self,**kwargs):
            self.calls.append(('get',kwargs))
            raw = b'corrupted' if self.corrupt else self.objects[kwargs['Key']]
            body = BytesIO(raw)
            self.bodies.append(body)
            return {'Body':body,'ServerSideEncryption':'AES256' if self.encrypted else None}

    def test_existing_sink_scope_conditional_aes_and_full_readback(self):
        s3 = self.S3()
        for key in (BASE+'meta/0001-response.json',BASE+'source-complete.json',
                    BASE+'diagnostics/publication.json',BASE+'diagnostics/failed.json'):
            live.put_live_source(s3,BASE,key,b'{"synthetic":"native"}')
        self.assertEqual(['put','get']*4,[name for name,_ in s3.calls])
        for method,kwargs in s3.calls:
            self.assertEqual(live.probe.ACCOUNT,kwargs['ExpectedBucketOwner'])
            self.assertEqual(live.probe.BUCKET,kwargs['Bucket'])
            if method == 'put':
                self.assertEqual('*',kwargs['IfNoneMatch'])
                self.assertEqual('AES256',kwargs['ServerSideEncryption'])
        self.assertTrue(all(body.closed for body in s3.bodies))

    def test_foreign_scope_and_live_aliases_reject_before_put(self):
        s3 = self.S3()
        for prefix,key in [(BASE.replace('/vevo/','/roy/'),BASE+'meta/1.json'),
                           (BASE,BASE+'../live.json'),(BASE,'daily-reports/vevo/latest/generation.json'),
                           (BASE,BASE.replace('a'*32,'b'*32)+'meta/1.json'),
                           (BASE,BASE+'diagnostics/arbitrary.json')]:
            with self.subTest(key=key),self.assertRaises(RuntimeError):
                live.put_live_source(s3,prefix,key,b'{}')
        self.assertEqual([],s3.calls)

    def test_corrupt_or_unencrypted_readback_and_existing_key_reject(self):
        for options in ({'corrupt':True},{'encrypted':False}):
            s3 = self.S3(**options)
            with self.subTest(options=options),self.assertRaises(RuntimeError):
                live.put_live_source(s3,BASE,BASE+'meta/1.json',b'{}')
            self.assertTrue(all(body.closed for body in s3.bodies))
        s3 = self.S3()
        live.put_live_source(s3,BASE,BASE+'meta/1.json',b'{}')
        with self.assertRaisesRegex(RuntimeError,'conditional conflict'):
            live.put_live_source(s3,BASE,BASE+'meta/1.json',b'{}')


class LiveFailureDiagnosticTests(unittest.TestCase):
    def retain(self, value, *, identity=None):
        s3 = LivePrivateWriterTests.S3()
        failure = RuntimeError('original private exception')
        if value is not None:
            failure.publication_diagnostics = value
        live.retain_live_failure(s3,BASE,identity or LIVE_IDENTITY,START,END,failure)
        return s3,json.loads(s3.objects[BASE+'diagnostics/failed.json'])

    def test_only_allowlisted_aggregate_qa_is_retained(self):
        value = rejected_publication()
        value['periods']['latest']['customer_rows'] = [{'email':'private@example.test'}]
        value['periods']['latest']['source_health']['qa']['geo']['failures'] = [{'order_num':'private-order','email':'private@example.test'}]
        s3,marker = self.retain(value)
        self.assertEqual('available',marker['diagnostic']['status'])
        self.assertEqual(source.sha(s3.objects[marker['diagnostic']['key']]),marker['diagnostic']['sha256'])
        content = json.loads(s3.objects[marker['diagnostic']['key']])
        check = content['periods']['latest']['source_health']['qa']['geo']
        self.assertEqual(1,check['failure_count'])
        self.assertEqual(38003.41,check['spend_attribution']['facebook_ads']['total_spend'])
        self.assertEqual('country spend does not reconcile to daily/account spend',
                         check['spend_attribution']['facebook_ads']['validation_error'])
        all_bytes = b''.join(s3.objects.values())
        for private in (b'private@example.test',b'private-order',b'secret@example.test',b'original private exception'):
            self.assertNotIn(private,all_bytes)
        self.assertFalse(any(key.endswith('source-complete.json') for key in s3.objects))

    def test_missing_oversize_or_unbound_payload_retains_only_safe_failure_marker(self):
        cases = [('missing',None),('too-large',{**rejected_publication(),'extra':'secret'*1000}),
                 ('invalid-json',{**rejected_publication(),'project':'roy'}),
                 ('invalid-json',{**rejected_publication(),'report_to_date':'2026-10-12'}),
                 ('invalid-json',{**rejected_publication(),'schema_version':True})]
        for status,value in cases:
            with self.subTest(status=status),patch.object(live.probe,'DIAGNOSTIC_LIMIT',2048 if status=='too-large' else 8*1024*1024):
                s3,marker = self.retain(value)
                self.assertEqual(status,marker['diagnostic']['status'])
                self.assertEqual([BASE+'diagnostics/failed.json'],list(s3.objects))
                self.assertNotIn(b'secret',b''.join(s3.objects.values()))

    def test_malformed_nested_values_cannot_drop_the_failure_marker_or_leak_strings(self):
        for malformed in ('private-string',[],{'status':[],'validation_error':{},'failure_count':10**400}):
            value = rejected_publication()
            value['periods']['latest']['source_health']['qa']['geo'] = malformed
            value['periods']['latest']['source_health']['qa_status'] = []
            value['periods']['latest']['read_status'] = {}
            with self.subTest(type=type(malformed).__name__):
                s3,marker = self.retain(value)
                self.assertIn(BASE+'diagnostics/failed.json',s3.objects)
                self.assertEqual('available',marker['diagnostic']['status'])
                self.assertNotIn(b'private-string',b''.join(s3.objects.values()))

    def test_unknown_identity_fields_and_oversize_direct_write_reject_before_s3(self):
        s3 = LivePrivateWriterTests.S3()
        with self.assertRaisesRegex(RuntimeError,'diagnostic-identity'):
            live.retain_live_failure(s3,BASE,{**LIVE_IDENTITY,'secret':'do-not-copy'},START,END,RuntimeError())
        with patch.object(live.probe,'DIAGNOSTIC_LIMIT',3),self.assertRaisesRegex(RuntimeError,'diagnostic-size'):
            live.put_live_source(s3,BASE,BASE+'diagnostics/publication.json',b'1234')
        self.assertEqual([],s3.calls)


class LiveHostOrderingTests(unittest.TestCase):
    def setUp(self):
        self.argv = ['vevo_report_image_host.py','--release-id',IDENTITY['release_id'],
            '--source-commit',IDENTITY['source_commit'],'--image-digest',IDENTITY['image_digest'],
            '--gate-sha256',IDENTITY['gate_sha256'],'--to-date',END]

    def test_host_identity_and_localhost_marker_precede_live_runner(self):
        import daily_report_runner as runner
        events = []
        identity = copy.deepcopy(IDENTITY)
        flags = SimpleNamespace(skip_email=True,skip_invoices=True,skip_creditnote_storno_guard=True,
                                skip_export=False,output_tag='')
        metadata = SimpleNamespace(raise_for_status=lambda:None,json=lambda:{'synthetic':'metadata'})
        def bind(*_args,**_kwargs):
            events.append('host-identity')
            return identity
        def local(value):
            events.append('localhost')
            self.assertEqual(LIVE_IDENTITY,value)
        with patch.object(sys,'argv',self.argv),patch.dict(live.os.environ,{'ECS_CONTAINER_METADATA_URI_V4':'http://169.254.170.2/v4/synthetic'}), \
             patch.object(requests,'get',return_value=metadata),patch.object(live.probe,'host_identity',side_effect=bind), \
             patch.object(live,'localhost_marker',side_effect=local),patch.object(runner,'parse_args',return_value=flags), \
             patch.object(live,'run_live_report',side_effect=lambda *_a,**_k:events.append('live-report')),patch('builtins.print'):
            live.main()
        self.assertEqual(['host-identity','localhost','live-report'],events)

    def test_failed_localhost_marker_never_enters_live_runner(self):
        metadata = SimpleNamespace(raise_for_status=lambda:None,json=lambda:{})
        with patch.object(sys,'argv',self.argv),patch.dict(live.os.environ,{'ECS_CONTAINER_METADATA_URI_V4':'http://169.254.170.2/v4/synthetic'}), \
             patch.object(requests,'get',return_value=metadata),patch.object(live.probe,'host_identity',return_value=copy.deepcopy(IDENTITY)), \
             patch.object(live,'localhost_marker',side_effect=RuntimeError('synthetic localhost mismatch')), \
             patch.object(live,'run_live_report') as report:
            with self.assertRaisesRegex(RuntimeError,'localhost mismatch'):
                live.main()
        report.assert_not_called()


if __name__ == '__main__':
    unittest.main()
