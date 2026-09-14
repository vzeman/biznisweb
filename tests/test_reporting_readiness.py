import copy
from datetime import datetime, timezone, timedelta
from io import BytesIO
from types import SimpleNamespace
import unittest
import sys
from unittest.mock import patch

from scripts import reporting_readiness as ready
from scripts import reporting_runtime_binding as binding

NOW = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)


class ReadinessSdkTests(unittest.TestCase):
    def test_old_response_model_rejects_before_any_aws_session(self):
        argv = ['reporting_readiness.py', '--primary-key', 'p', '--primary-sha256', 'p',
                '--guards-key', 'g', '--guards-sha256', 'g']
        with patch.object(sys, 'argv', argv), patch.object(ready, 'version', return_value='1.42.63'), \
                patch('boto3.Session') as session:
            with self.assertRaisesRegex(binding.BindingError, 'aws-sdk-version-mismatch'):
                ready.main()
        session.assert_not_called()

    def test_botocore_model_must_match_even_with_correct_boto3(self):
        with patch.object(ready, 'version', side_effect=lambda p: '1.43.93' if p == 'boto3' else '1.42.63'):
            with self.assertRaisesRegex(binding.BindingError, 'aws-sdk-version-mismatch'):
                ready.require_sdk_versions()

    def test_verified_model_pair_matches_repository_pins(self):
        with patch.object(ready, 'version', return_value='1.43.93'):
            ready.require_sdk_versions()


class ReadinessFixture:
    """Synthetic independent + managed receipts, separate AWS readbacks and hashes."""
    def __init__(self):
        self.objects, self.bodies, self.tasks, self.definitions, self.runs, self.puts = {}, [], {}, {}, {}, []
        self.s3 = self.ecs = self.ecr = self
        self.primary_image, self.guard_image = 'sha256:' + 'c' * 64, 'sha256:' + 'd' * 64
        self.primary_source, self.guard_source = 'a' * 40, 'b' * 40
        self.protected = {'schedules': {}, 'definitions': {}}
        self.old_reports = {}
        self.primary = {'schema': 1, 'phase': 'promotion-readback-verified', 'commit': self.primary_source,
                        'created_at': NOW.isoformat(), 'image_digest': self.primary_image, 'candidate_task_definitions': {},
                        'desired_schedules': {}, 'hosts': [], 'drain': {'quiet_seconds': 120, 'unfinished_tasks': 0,
                        'verified_at': NOW.isoformat()}}
        self.primary['candidate_drain'] = copy.deepcopy(self.primary['drain'])
        for family, (project, kind, _) in ready.SERVICES.items():
            td = self.definition(family, 10, self.primary_image)
            self.primary['candidate_task_definitions'][family] = {k: v for k, v in td.items() if k != 'taskDefinitionArn'}
            marker = {'marker': 'ORDER_AUTOMATION_HOST_OK', 'project': project, 'kind': kind, 'path': '/app', 'dry_run': True}
            if kind == 'invoice':
                marker['full_backlog'] = True
            self.primary['hosts'].append(self.host(family, td, marker=marker))
        for name, family in ready.SCHEDULES.items():
            schedule = self.schedule(name, self.td_arn(family, 10))
            self.primary['desired_schedules'][name] = copy.deepcopy(schedule)
            self.protected['schedules'][name] = schedule
        self.guards = {'schema': 1, 'phase': 'promotion-readback-verified', 'commit': self.guard_source,
                       'created_at': NOW.isoformat(), 'candidate_definitions': {}, 'expected_schedules': {},
                       'original_schedules': copy.deepcopy(self.primary['desired_schedules']), 'hosts': []}
        for project in ('roy', 'vevo'):
            name = project + '-daily-report-email'
            revision, image = ready.REPORT_PINS[project]
            td = self.definition(project + '-reporting-daily', revision, image)
            original = self.schedule(name, td['taskDefinitionArn'])
            self.old_reports[name] = original
            self.guards['original_schedules'][name] = copy.deepcopy(original)
            self.guards['expected_schedules'][name] = ready.report_skip_only(original)
            host = self.host(project + '-reporting-daily', td, kind='report-probe')
            self.guards['hosts'].append(host)
            family = project + '-creditnote-storno-guard'
            td = self.definition(family, 1, self.guard_image)
            self.guards['candidate_definitions'][project] = {'arn': td['taskDefinitionArn'],
                'definition': {k: v for k, v in td.items() if k != 'taskDefinitionArn'}}
            self.guards['expected_schedules'][family] = self.schedule(family, td['taskDefinitionArn'])
            for kind in ('guard-probe', 'guard-live'):
                summary = {'ok': True, 'project': project, 'enabled': True, 'dry_run': kind == 'guard-probe',
                           'creditnote_scan_complete': True, 'skipped_locked': False, 'failed_orders': 0,
                           'review_required_orders': 0, 'audit_error_orders': 0, 'updated_orders': 0}
                self.guards['hosts'].append(self.host(family, td, kind=kind, summary=summary))
        self.guards['expected_schedules'].update(copy.deepcopy(self.primary['desired_schedules']))
        self.protected['schedules'].update({k: copy.deepcopy(v) for k, v in self.guards['expected_schedules'].items()
                                           if k != 'vevo-daily-report-email'})
        self.vevo_schedule = copy.deepcopy(self.guards['expected_schedules']['vevo-daily-report-email'])
        self.primary_audit = self.audit('primary_release', self.primary, '101', self.primary_image)
        self.primary_audit['final'].update(candidate_drain=copy.deepcopy(self.primary['candidate_drain']),
            drain=copy.deepcopy(self.primary['drain']), report_schedules=copy.deepcopy(self.old_reports))
        self.guard_audit = self.audit('standalone_guards', self.guards, '102', self.guard_image)
        self.guard_audit['final']['report_task_definitions'] = {name: copy.deepcopy(self.definitions[schedule['Target']['EcsParameters']['TaskDefinitionArn']])
                                                             for name, schedule in self.old_reports.items()}
        self.primary_audit['final']['report_task_definitions'] = copy.deepcopy(self.guard_audit['final']['report_task_definitions'])
        self.receipt = {'schema_version': 1, 'phase': ready.PHASE, 'account': ready.ACCOUNT, 'region': ready.REGION,
                        'verified_at': NOW.isoformat(), 'protected': copy.deepcopy(self.protected)}
        self.reseal()

    @staticmethod
    def td_arn(family, revision):
        return f'arn:aws:ecs:{ready.REGION}:{ready.ACCOUNT}:task-definition/{family}:{revision}'

    def definition(self, family, revision, digest):
        arn = self.td_arn(family, revision)
        td = {'taskDefinitionArn': arn, 'family': family, 'containerDefinitions': [{'name': 'reporting',
              'image': f'{ready.ACCOUNT}.dkr.ecr.{ready.REGION}.amazonaws.com/vevo-reporting@{digest}'}]}
        self.definitions[arn] = copy.deepcopy(td)
        self.protected['definitions'][arn] = copy.deepcopy(td)
        return td

    @staticmethod
    def schedule(name, arn):
        return {'Name': name, 'State': 'ENABLED', 'ScheduleExpression': 'cron(0 1 * * ? *)',
                'Target': {'Arn': ready.CLUSTER, 'EcsParameters': {'TaskDefinitionArn': arn}}}

    def host(self, service, td, **extra):
        arn = ready.CLUSTER.replace(':cluster/', ':task/') + '/' + format(len(self.tasks) + 1, '032x')
        digest = td['containerDefinitions'][0]['image'].rsplit('@', 1)[1]
        host = {'task': arn, 'task_definition': td['taskDefinitionArn'], 'service': service, 'private_ip': '172.31.1.2',
                'image_digest': digest, 'instance_id': 'N/A:FARGATE', 'path': '/app', 'exit_code': 0, **extra}
        self.tasks[arn] = {'taskArn': arn, 'taskDefinitionArn': td['taskDefinitionArn'], 'clusterArn': ready.CLUSTER,
            'launchType': 'FARGATE', 'lastStatus': 'STOPPED',
            'startedBy': 'creditnote-automation-migration' if 'kind' in extra else 'order-automation-host-gate',
            'containers': [{'name': 'reporting', 'exitCode': 0, 'imageDigest': digest,
                            'networkInterfaces': [{'privateIpv4Address': host['private_ip']}]}]}
        return host

    def audit(self, kind, managed, run_id, image):
        final_hosts = {}
        for host in managed['hosts']:
            item = {'managed_host': copy.deepcopy(host), 'direct_terminal_task': copy.deepcopy(self.tasks[host['task']])}
            if kind == 'primary_release':
                item['markers'] = [copy.deepcopy(host['marker'])]
            else:
                item['markers'] = {'REPORTING_GUARD_IDENTITY_OK': [], 'CREDITNOTE_AUTOMATION_HOST_OK': []}
                item['summaries'] = [copy.deepcopy(host['summary'])] if host.get('summary') else []
                project = host['service'].split('-')[0]
                if host['kind'] == 'guard-probe':
                    item['markers']['CREDITNOTE_AUTOMATION_HOST_OK'] = [{'marker': 'CREDITNOTE_AUTOMATION_HOST_OK',
                        'project': project, 'path': '/app', 'dry_run': True}]
                elif host['kind'] == 'report-probe':
                    item['markers']['REPORTING_GUARD_IDENTITY_OK'] = [{'marker': 'REPORTING_GUARD_IDENTITY_OK',
                        'project': project, 'task': host['task'], 'private_ip': host['private_ip'], 'path': '/app',
                        'service': host['service'], 'revision': str(ready.REPORT_PINS[project][0]),
                        'instance_id': 'N/A:FARGATE', 'source_sha256': ready.REPORT_RUNNER_SHA256[project],
                        'inline_guard_skipped': True, 'business_runner_started': False}]
            final_hosts[host['task']] = item
        prefix, workflow = ready.REFERENCES[kind]
        self.runs[run_id] = {'id': int(run_id), 'head_sha': managed['commit'], 'head_branch': 'main',
            'repository': {'full_name': 'vzeman/biznisweb'}, 'path': workflow, 'event': 'workflow_dispatch',
            'status': 'completed', 'conclusion': 'success', 'run_started_at': (NOW - timedelta(minutes=5)).isoformat(),
            'updated_at': (NOW + timedelta(minutes=5)).isoformat()}
        families = ready.SERVICES if kind == 'primary_release' else {p + '-creditnote-storno-guard' for p in ('roy', 'vevo')}
        definitions = {family: copy.deepcopy(next(td for td in self.definitions.values() if td['family'] == family)) for family in families}
        return {'schema_version': 1, 'evidence_type': 'independent_order_automation_release' if kind == 'primary_release' else 'independent_creditnote_automation_release',
            'verdict': 'promotion_verified', 'release_promoted': True, 'account': ready.ACCOUNT, 'region': ready.REGION, 'bucket': ready.BUCKET,
            'commit': managed['commit'], 'expected_image_digest': image, 'run_id': run_id,
            'managed_key': f"data/roy/order-automation/{prefix}/{managed['commit']}/" + ('1' if kind == 'primary_release' else '2') * 32 + '.json',
            'managed_sha256': None, 'managed_latest': copy.deepcopy(managed),
            'final': {'workflow': {'status': 'completed', 'conclusion': 'success', 'headSha': managed['commit']},
                      'schedules': copy.deepcopy(managed['desired_schedules' if kind == 'primary_release' else 'expected_schedules']),
                      'hosts': final_hosts, 'task_definitions': definitions}}

    def store(self, key, value):
        raw = binding.canonical_bytes(value)
        self.objects[key] = raw
        return ready.hashlib.sha256(raw).hexdigest()

    def reseal(self):
        for kind, managed, audit in (('primary_release', self.primary, self.primary_audit), ('standalone_guards', self.guards, self.guard_audit)):
            audit['managed_sha256'] = self.store(audit['managed_key'], managed)
            key = 'data/roy/order-automation/audits/2026-09-13/' + kind + '.json'
            self.receipt[kind] = {'key': key, 'sha256': self.store(key, audit)}

    def get_object(self, **kwargs):
        assert kwargs['ExpectedBucketOwner'] == ready.ACCOUNT
        body = BytesIO(self.objects[kwargs['Key']])
        self.bodies.append(body)
        raw = self.objects[kwargs['Key']]
        return {'Body': body, 'ServerSideEncryption': 'AES256', 'ContentLength': len(raw), 'ETag': ready.hashlib.sha256(raw).hexdigest()}

    def put_object(self, **kwargs):
        assert kwargs['ExpectedBucketOwner'] == ready.ACCOUNT and kwargs['ServerSideEncryption'] == 'AES256'
        assert kwargs['IfNoneMatch'] == '*' and kwargs['Key'] not in self.objects
        self.objects[kwargs['Key']] = kwargs['Body']
        self.puts.append(kwargs['Key'])

    def describe_tasks(self, **kwargs):
        return {'tasks': [copy.deepcopy(self.tasks[key]) for key in kwargs['tasks']]}

    def describe_task_definition(self, **kwargs):
        return {'taskDefinition': copy.deepcopy(self.definitions[kwargs['taskDefinition']])}

    def describe_images(self, **kwargs):
        return {'imageDetails': [{'imageDigest': self.primary_image if kwargs['imageIds'][0]['imageTag'] == 'git-' + self.primary_source else self.guard_image}]}

    def fetch_run(self, path):
        return copy.deepcopy(self.runs[path.rsplit('/', 1)[1]])

    def validate(self):
        with patch.object(binding, 'require_private_bucket'):
            return ready.validate_readiness(self.receipt, s3=self, ecs=self, ecr=self, protected=self.protected,
                vevo_schedule=self.vevo_schedule, fetch_run=self.fetch_run, now=NOW)


class ReadinessTests(unittest.TestCase):
    def test_producer_preview_and_single_encrypted_publication_use_real_semantic_validation(self):
        from scripts import deploy_vevo_report
        for publish in (False, True):
            fixture = ReadinessFixture()
            deployment = SimpleNamespace(s3=fixture, ecs=fixture, schedule=lambda: fixture.vevo_schedule,
                definition=lambda arn: fixture.definitions[arn], snapshot_protected=lambda: copy.deepcopy(fixture.protected))
            session = SimpleNamespace(client=lambda name: SimpleNamespace(get_caller_identity=lambda: {'Account': ready.ACCOUNT})
                                      if name == 'sts' else fixture)
            validate = ready.validate_readiness
            def checked(receipt, **kwargs):
                return validate(receipt, **kwargs, fetch_run=fixture.fetch_run, now=NOW)
            with self.subTest(publish=publish), patch.object(ready, 'require_source'), \
                 patch.object(binding, 'require_private_bucket'), patch.object(binding, 'check_migration'), \
                 patch.object(deploy_vevo_report, 'Deployment', return_value=deployment), \
                 patch.object(ready, 'datetime') as clock, patch.object(ready, 'validate_readiness', side_effect=checked):
                clock.now.return_value = NOW
                result = ready.prepare(session, fixture.receipt['primary_release'], fixture.receipt['standalone_guards'], publish=publish)
            self.assertEqual(int(publish), len(fixture.puts))
            self.assertEqual(publish, result['published'])
            self.assertEqual(0, result['runtime_writes'])
            self.assertTrue(all(body.closed for body in fixture.bodies))
            if publish:
                self.assertTrue(result['key'].startswith(binding.PREFIX + 'readiness/'))
                self.assertEqual(result['sha256'], ready.hashlib.sha256(fixture.objects[result['key']]).hexdigest())

    def test_bound_independent_and_managed_releases_with_skip_only_transition_pass(self):
        fixture = ReadinessFixture()
        self.assertIs(fixture.receipt, fixture.validate())
        self.assertTrue(all(body.closed for body in fixture.bodies))

    def test_hash_correct_arbitrary_independent_document_cannot_authorize(self):
        fixture = ReadinessFixture()
        fixture.primary_audit['verdict'] = 'candidate_only'
        fixture.reseal()
        with self.assertRaisesRegex(binding.BindingError, 'independent-verdict'):
            fixture.validate()

    def test_report_transition_rejects_any_other_schedule_change(self):
        fixture = ReadinessFixture()
        name = 'vevo-daily-report-email'
        fixture.vevo_schedule['ScheduleExpression'] = 'cron(1 1 * * ? *)'
        fixture.guards['expected_schedules'][name] = copy.deepcopy(fixture.vevo_schedule)
        fixture.guard_audit['managed_latest'] = copy.deepcopy(fixture.guards)
        fixture.guard_audit['final']['schedules'][name] = copy.deepcopy(fixture.vevo_schedule)
        fixture.reseal()
        with self.assertRaisesRegex(binding.BindingError, 'delta-not-only'):
            fixture.validate()


if __name__ == '__main__':
    unittest.main()
