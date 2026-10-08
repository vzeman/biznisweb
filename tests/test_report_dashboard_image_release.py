import copy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from scripts import dashboard_routing_host_gate as host
from scripts import deploy_picking_batch_dashboard as deploy


DATE = '2026-10-07'
DIGEST = 'sha256:' + '1' * 64
SHA = 'a' * 40


def http_fixture(project):
    responses = {'/health': b'{"ok":true}'}
    for period in host.PERIODS:
        responses[f'/report/{project}?period={period}'] = (
            '<html><head></head><body>Net MER; not platform-attributed ROAS; '
            f'<script id="report-dashboard-json" type="application/json">{{}}</script>'
            f'/report/{project}?period={period}' + ' ' * 1001 + '</body></html>').encode()
        responses[f'/api/{project}/latest?period={period}'] = json.dumps({
            'project': project, 'date_to': DATE, 'period_switcher': {'current_key': period},
            'source_health': {'is_partial': False, 'qa_status': 'warning', 'qa_failure_count': 0, 'qa_errors': []},
        }).encode()
        responses[f'/dashboard/{project}?period={period}'] = b'Net MER; Observation only; not platform-attributed ROAS'
    return responses


def proof_fixture(project='roy'):
    receipt = {'project': project, 'mode': 'reporting', 'report_to_date': DATE,
               'task_arn': 'task-a', 'definition': 'definition-a', 'started_by': 'owner-a', 'digest': DIGEST}
    task = {'taskArn': 'task-a', 'taskDefinitionArn': 'definition-a', 'startedBy': 'owner-a',
            'lastStatus': 'STOPPED', 'containers': [{'exitCode': 0, 'imageDigest': DIGEST}],
            'attachments': [{'details': [{'name': 'privateIPv4Address', 'value': '172.31.1.1'}]}]}
    proof = {'identity': {'task_arn': 'task-a', 'service': deploy.SERVICES[project][0], 'project': project,
                         'path': '/app', 'private_ips': ['172.31.1.1'], 'command': ['scripts/dashboard_routing_host_gate.py']},
             'marker': 'dashboard-project-routing-v1', 'mode': 'reporting', 'read_only_routes': True,
             'business_state_writes': False, 'authentication_required': True, 'foreign_redirects': True,
             'reports': host.verify_report_http(project, http_fixture(project).__getitem__, expected_to_date=DATE)}
    return task, proof, receipt


class ReportHostTests(unittest.TestCase):
    def test_four_periods_use_only_health_and_report_gets(self):
        for project in ('roy', 'vevo'):
            with self.subTest(project=project):
                reader = Mock(side_effect=http_fixture(project).__getitem__)
                result = host.verify_report_http(project, reader, expected_to_date=DATE)
                paths = [call.args[0] for call in reader.call_args_list]
                self.assertEqual(len(paths), 13)
                self.assertEqual(set(paths), set(http_fixture(project)))
                self.assertTrue(all(host.report_path_allowed(path) for path in paths))
                self.assertEqual([row['period'] for row in result], list(host.PERIODS))

    def test_operations_routes_are_rejected_including_encoded_and_refresh_variants(self):
        for path in ('/api/operations/roy/live?refresh=0', '/api/production/vevo/live',
                     '/api/operations/roy/picking-lists.pdf?preview=1', '/production/roy',
                     '/report/roy/../production', '/%61pi/operations/roy/live'):
            with self.subTest(path=path):
                self.assertFalse(host.report_path_allowed(path))

    def test_source_health_dates_project_period_and_semantics_fail_closed(self):
        changes = [
            lambda data: data.update(project='vevo'),
            lambda data: data.update(date_to='2026-10-06'),
            lambda data: data['period_switcher'].update(current_key='full'),
            lambda data: data['source_health'].update(qa_failure_count=1),
            lambda data: data['source_health'].update(is_partial=True),
            lambda data: data['source_health'].update(qa_errors=['invalid']),
        ]
        for change in changes:
            with self.subTest(change=change):
                responses = http_fixture('roy')
                key = '/api/roy/latest?period=7d'
                data = json.loads(responses[key])
                change(data)
                responses[key] = json.dumps(data).encode()
                with self.assertRaises((AssertionError, RuntimeError)):
                    host.verify_report_http('roy', responses.__getitem__, expected_to_date=DATE)
        for path, old in (('/report/roy?period=7d', b'Net MER'),
                          ('/report/roy?period=7d', b'not platform-attributed ROAS'),
                          ('/dashboard/roy?period=7d', b'Observation only')):
            responses = http_fixture('roy')
            responses[path] = responses[path].replace(old, b'old label')
            with self.assertRaises(AssertionError):
                host.verify_report_http('roy', responses.__getitem__, expected_to_date=DATE)

    def test_candidate_read_boundary_blocks_business_helpers_and_aws_writes(self):
        import boto3
        client = Mock()
        with patch('boto3.client', return_value=client):
            with host.report_read_only_boundary():
                self.assertIs(boto3.client('s3').get_object(Key='report'), client.get_object.return_value)
                for action in (lambda: boto3.client('s3').put_object(Key='state'),
                               lambda: boto3.client('ssm'),
                               lambda: host.dashboard.get_cached_roy_operations_snapshot('roy'),
                               lambda: host.dashboard.get_cached_production_board_snapshot('vevo')):
                    with self.assertRaises(RuntimeError):
                        action()
        client.put_object.assert_not_called()


class ReportReleaseProofTests(unittest.TestCase):
    def test_reporting_task_proof_is_identity_and_mode_bound(self):
        task, proof, receipt = proof_fixture()
        deploy.validate_proof(task, proof, receipt)
        mutations = [
            lambda t, p, r: t.update(startedBy='foreign'),
            lambda t, p, r: t.update(taskDefinitionArn='other'),
            lambda t, p, r: t.update(lastStatus='RUNNING'),
            lambda t, p, r: t['containers'][0].update(exitCode=1),
            lambda t, p, r: t['containers'][0].update(imageDigest='old'),
            lambda t, p, r: p['identity'].update(private_ips=['172.31.9.9']),
            lambda t, p, r: p['identity'].update(command=['other.py']),
            lambda t, p, r: p['identity'].update(service=deploy.SERVICES['vevo'][0]),
            lambda t, p, r: p.update(business_state_writes=True),
            lambda t, p, r: p.update(authentication_required=False),
            lambda t, p, r: p.update(mode='picking'),
            lambda t, p, r: p['reports'][0].update(date_to='2026-10-06'),
            lambda t, p, r: p['reports'][0].update(payload_sha256='bad'),
            lambda t, p, r: p.update(reports=p['reports'][:-1]),
            lambda t, p, r: r.update(mode='picking'),
        ]
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                t, p, r = copy.deepcopy((task, proof, receipt))
                mutate(t, p, r)
                with self.assertRaises(AssertionError):
                    deploy.validate_proof(t, p, r)

    def test_existing_picking_receipt_still_requires_original_picking_proof(self):
        task, proof, receipt = proof_fixture()
        receipt.pop('mode')
        receipt.pop('report_to_date')
        picking = {'identity': proof['identity'], 'marker': 'pdf-batch-v1', 'batch_races_verified': True,
                   'synthetic_batch_tests': 5, 'preview_pdf_bytes': 1001,
                   'refresh_policy': 'independent-orders-v1', 'independent_refresh_tests': 9}
        deploy.validate_proof(task, picking, receipt)
        picking['batch_races_verified'] = False
        with self.assertRaises(AssertionError):
            deploy.validate_proof(task, picking, receipt)

    def test_report_candidate_has_no_provider_secret_and_default_picking_is_unchanged(self):
        environment = {'RuntimeEnvironmentVariables': {'REPORT_PROJECT': 'roy', 'BIZNISWEB_API_TOKEN': 'synthetic',
                       'LIVE_DASHBOARD_AUTH_PASSWORD': 'synthetic', 'PRESERVED': 'yes'}}
        original = {'secrets': [{'name': 'BIZNISWEB_API_TOKEN', 'valueFrom': 'reference'}],
                    'logConfiguration': {'logDriver': 'awslogs'}}
        original_inputs = copy.deepcopy((environment, original))
        for reporting in (False, True):
            container = deploy.candidate_container('roy', 'immutable', environment, original,
                                                    report_only=reporting, report_to_date=DATE if reporting else None)
            env = {row['name']: row['value'] for row in container['environment']}
            self.assertNotIn('LIVE_DASHBOARD_AUTH_PASSWORD', env)
            self.assertEqual(env['PRESERVED'], 'yes')
            if reporting:
                self.assertEqual(container['secrets'], [])
                self.assertNotIn('BIZNISWEB_API_TOKEN', env)
                self.assertEqual(env['EXPECTED_REPORT_TO_DATE'], DATE)
                self.assertEqual(container['command'], ['python', 'scripts/dashboard_routing_host_gate.py'])
            else:
                self.assertEqual(container['secrets'], original['secrets'])
                self.assertEqual(container['command'], ['python', 'scripts/picking_batch_host_gate.py'])
        self.assertEqual((environment, original), original_inputs)

    def test_protected_boundary_snapshots_every_schedule_and_rejects_running_or_uncertain(self):
        clients = {key: Mock() for key in ('scheduler', 'ecs', 's3')}
        names = ['roy-daily-report-email', 'vevo-daily-report-email', 'unrelated-monthly']
        clients['scheduler'].get_paginator.return_value.paginate.return_value = [{'Schedules': [{'Name': name} for name in names]}]
        clients['scheduler'].get_schedule.side_effect = lambda **kw: {'Name': kw['Name'], 'State': 'ENABLED', 'Target': {'Input': 'preserve'}}
        clients['ecs'].list_tasks.return_value = {'taskArns': []}
        with patch('scripts.reporting_image_release_lease.read_lease', return_value=({'state': 'released'}, 'etag')) as read:
            baseline = deploy.reporting_boundary(clients, 'cluster')
            self.assertEqual(set(baseline['schedules']), set(names))
            self.assertEqual(baseline['leases']['roy'], [{'state': 'released'}, 'etag'])
            self.assertEqual(baseline, json.loads(json.dumps(baseline)))
            for state in ('active', 'uncertain'):
                read.return_value = ({'state': state}, 'etag')
                with self.assertRaises(AssertionError):
                    deploy.reporting_boundary(clients, 'cluster')
            read.return_value = None
            clients['ecs'].list_tasks.return_value = {'taskArns': ['pending-task']}
            with self.assertRaises(AssertionError):
                deploy.reporting_boundary(clients, 'cluster')


class ReportReleaseLifecycleTests(unittest.TestCase):
    def test_invalid_or_misplaced_report_date_rejects_before_aws(self):
        base = ['deploy', 'probe', '--project', 'roy', '--source-sha', SHA,
                '--expected-current-digest', DIGEST, '--receipt', 'unused.json']
        for flags in (['--report-only'], ['--report-only', '--report-to-date', '2026-02-30'],
                      ['--report-only', '--report-to-date', '2026-10-7'], ['--report-to-date', DATE]):
            with self.subTest(flags=flags), patch.object(deploy.sys, 'argv', base + flags), patch('boto3.Session') as session:
                with self.assertRaises((AssertionError, ValueError)):
                    deploy.main()
                session.assert_not_called()

    def test_failed_dispatch_with_definite_no_task_deactivates_only_its_definition(self):
        ecs, save = Mock(), Mock()
        receipt = {'cluster': 'cluster', 'started_by': 'owner', 'definition': 'owned-definition'}
        ecs.describe_task_definition.return_value = {'taskDefinition': {'status': 'INACTIVE'}}
        self.assertTrue(deploy.recover_report_dispatch(ecs, receipt, save, known_tasks=[]))
        ecs.run_task.assert_not_called()
        ecs.stop_task.assert_not_called()
        ecs.deregister_task_definition.assert_called_once_with(taskDefinition='owned-definition')
        self.assertEqual(receipt['phase'], 'dispatch-failed-cleaned')

    def test_unknown_dispatch_is_not_retried_or_claimed_clean_when_task_is_not_visible(self):
        ecs, save = Mock(), Mock()
        ecs.list_tasks.return_value = {'taskArns': []}
        receipt = {'cluster': 'cluster', 'started_by': 'owner', 'definition': 'owned-definition'}
        with patch.object(deploy.time, 'sleep') as sleep:
            self.assertFalse(deploy.recover_report_dispatch(ecs, receipt, save, attempts=2))
        self.assertEqual(receipt['phase'], 'dispatch-unresolved')
        self.assertEqual(ecs.list_tasks.call_count, 2)
        sleep.assert_called_once_with(5)
        ecs.run_task.assert_not_called()
        ecs.stop_task.assert_not_called()
        ecs.deregister_task_definition.assert_not_called()

    def test_unknown_dispatch_recovery_stops_exact_owned_task_and_survives_receipt_failure(self):
        ecs, save = Mock(), Mock(side_effect=[OSError('receipt unavailable'), None])
        task, _, receipt = proof_fixture()
        receipt['cluster'] = 'cluster'
        task['lastStatus'] = 'RUNNING'
        task['containers'][0]['image'] = deploy.REPOSITORY + '@' + DIGEST
        stopped = copy.deepcopy(task)
        stopped['lastStatus'] = 'STOPPED'
        ecs.list_tasks.return_value = {'taskArns': [task['taskArn']]}
        ecs.describe_tasks.side_effect = [{'tasks': [task]}, {'tasks': [stopped]}]
        ecs.describe_task_definition.return_value = {'taskDefinition': {'status': 'INACTIVE'}}
        self.assertTrue(deploy.recover_report_dispatch(ecs, receipt, save))
        self.assertEqual(ecs.stop_task.call_args.kwargs['task'], task['taskArn'])
        self.assertEqual(receipt['recovered_tasks'], [stopped])
        self.assertTrue(receipt['cleanup_receipt_write_failed'])
        ecs.run_task.assert_not_called()

    def test_dispatch_cleanup_rejects_foreign_owner_definition_or_image(self):
        for field in ('owner', 'definition', 'image'):
            ecs = Mock()
            task, _, receipt = proof_fixture()
            receipt['cluster'] = 'cluster'
            task['lastStatus'] = 'RUNNING'
            task['containers'][0]['image'] = deploy.REPOSITORY + '@' + DIGEST
            if field == 'owner':
                task['startedBy'] = 'foreign'
            elif field == 'definition':
                task['taskDefinitionArn'] = 'foreign'
            else:
                task['containers'][0]['image'] = 'foreign'
            ecs.describe_tasks.return_value = {'tasks': [task]}
            with self.subTest(field=field), self.assertRaises(AssertionError):
                deploy.recover_report_dispatch(ecs, receipt, Mock(), known_tasks=[task['taskArn']])
            ecs.stop_task.assert_not_called()
            ecs.deregister_task_definition.assert_not_called()

    def test_probe_and_promotion_are_mode_bound_image_only_and_get_only(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt_path = Path(directory) / 'receipt.json'
            clients = {name: Mock() for name in ('sts', 's3', 'apprunner', 'ecs', 'ecr', 'scheduler', 'logs', 'ssm')}
            clients['sts'].get_caller_identity.return_value = {'Account': deploy.ACCOUNT}
            clients['s3'].get_public_access_block.return_value = {'PublicAccessBlockConfiguration': {'BlockPublicAcls': True}}
            objects = {}
            clients['s3'].put_object.side_effect = lambda **kw: objects.setdefault(kw['Key'], kw['Body'])
            clients['s3'].get_object.side_effect = lambda **kw: {'ServerSideEncryption': 'AES256', 'Body': io.BytesIO(objects[kw['Key']])}
            old_digest = 'sha256:' + '2' * 64
            service = {'ServiceArn': 'service', 'ServiceName': deploy.SERVICES['roy'][0], 'ServiceId': deploy.SERVICES['roy'][1],
                       'Status': 'RUNNING', 'ServiceUrl': 'report.invalid', 'InstanceConfiguration': {'Cpu': '256', 'Memory': '512'},
                       'SourceConfiguration': {'AutoDeploymentsEnabled': False, 'ImageRepository': {
                           'ImageIdentifier': deploy.REPOSITORY + '@' + old_digest,
                           'ImageConfiguration': {'Port': '8080', 'StartCommand': 'python live_dashboard_server.py --host 0.0.0.0 --port 8080',
                               'RuntimeEnvironmentVariables': {'REPORT_PROJECT': 'roy', 'LIVE_DASHBOARD_AUTH_USER': 'test'},
                               'RuntimeEnvironmentSecrets': {'LIVE_DASHBOARD_AUTH_PASSWORD': 'parameter-reference'}}}}}
            original_service = copy.deepcopy(service)
            clients['apprunner'].describe_service.side_effect = lambda **kw: {'Service': copy.deepcopy(service)}
            clients['ecr'].describe_images.return_value = {'imageDetails': [{'imageDigest': DIGEST}]}
            schedule = {'Target': {'Arn': 'cluster', 'EcsParameters': {'TaskDefinitionArn': 'original-definition',
                        'NetworkConfiguration': {'awsvpcConfiguration': {'Subnets': ['subnet']}}}}}
            clients['scheduler'].get_schedule.return_value = schedule
            source = {'family': 'roy-reporting-daily', 'executionRoleArn': 'execution', 'taskRoleArn': 'role',
                      'containerDefinitions': [{'secrets': [], 'logConfiguration': {'options': {'awslogs-group': 'group', 'awslogs-stream-prefix': 'prefix'}}}]}
            clients['ecs'].describe_task_definition.side_effect = lambda **kw: {'taskDefinition': source if kw['taskDefinition'] == 'original-definition' else {'status': 'INACTIVE'}}
            clients['ecs'].register_task_definition.return_value = {'taskDefinition': {'taskDefinitionArn': 'definition-a'}}
            clients['ecs'].list_tasks.return_value = {'taskArns': []}
            task, proof, _ = proof_fixture()
            task['taskArn'] = 'task/task-a'
            proof['identity']['task_arn'] = task['taskArn']
            def run_task(**kw):
                task['startedBy'] = kw['startedBy']
                return {'tasks': [copy.deepcopy(task)]}
            clients['ecs'].run_task.side_effect = run_task
            clients['ecs'].describe_tasks.side_effect = lambda **kw: {'tasks': [copy.deepcopy(task)]}
            clients['logs'].get_log_events.side_effect = lambda **kw: {'events': [] if 'nextToken' in kw else [
                {'message': 'DASHBOARD_ROUTING_HOST_OK ' + json.dumps(proof)}, {'message': 'DASHBOARD_ROUTING_LOCAL_SERVER_CLOSED'}], 'nextForwardToken': 'done'}
            def update(**kw):
                service['SourceConfiguration'] = kw['SourceConfiguration']
                return {'OperationId': 'operation'}
            clients['apprunner'].update_service.side_effect = update
            clients['apprunner'].list_operations.return_value = {'OperationSummaryList': [{'Id': 'operation', 'Status': 'SUCCEEDED'}]}
            clients['ssm'].get_parameter.return_value = {'Parameter': {'Value': 'synthetic-password'}}
            session = Mock()
            session.client.side_effect = lambda name, **kw: clients[name]
            requests = []
            def open_request(request, **kwargs):
                url = request.full_url
                path = url.removeprefix('https://report.invalid')
                requests.append(path)
                response = Mock()
                response.__enter__ = Mock(return_value=response)
                response.__exit__ = Mock(return_value=False)
                response.geturl.return_value = url
                response.read.return_value = http_fixture('roy')[path]
                return response
            opener = Mock()
            opener.open.side_effect = open_request
            argv = ['deploy', 'probe', '--project', 'roy', '--source-sha', SHA, '--expected-current-digest', old_digest,
                    '--receipt', str(receipt_path), '--report-only', '--report-to-date', DATE]
            def subprocess_output(command):
                if command[:2] == ['gh', 'api']:
                    return json.dumps({'object': {'sha': SHA}}).encode()
                return SHA.encode() if command[1] == 'rev-parse' else b''
            with patch('boto3.Session', return_value=session), patch.object(deploy.subprocess, 'check_output', side_effect=subprocess_output), \
                 patch.object(deploy, 'reporting_boundary', return_value={'all-schedules': 'unchanged'}), \
                 patch.object(deploy, 'build_opener', return_value=opener), patch.object(deploy.sys, 'argv', argv):
                deploy.main()
                receipt = json.loads(receipt_path.read_text())
                self.assertEqual(receipt['phase'], 'verified')
                self.assertEqual(receipt['mode'], 'reporting')
                clients['apprunner'].update_service.assert_not_called()
                clients['ecs'].deregister_task_definition.assert_called_once_with(taskDefinition='definition-a')
                clients['ecs'].stop_task.assert_not_called()
                argv[1] = 'promote'
                argv[-1] = '2026-10-06'
                with self.assertRaisesRegex(AssertionError, 'mode or reporting window'):
                    deploy.main()
                clients['apprunner'].update_service.assert_not_called()
                argv[-1] = DATE
                deploy.main()
            self.assertEqual(set(requests), set(http_fixture('roy')))
            self.assertTrue(all(host.report_path_allowed(path) for path in requests))
            update_call = clients['apprunner'].update_service.call_args.kwargs
            self.assertEqual(set(update_call), {'ServiceArn', 'SourceConfiguration'})
            expected_source = copy.deepcopy(original_service['SourceConfiguration'])
            expected_source['ImageRepository']['ImageIdentifier'] = deploy.REPOSITORY + '@' + DIGEST
            self.assertEqual(update_call['SourceConfiguration'], expected_source)
            self.assertEqual(json.loads(receipt_path.read_text())['phase'], 'deployed')
            for call in clients['s3'].put_object.call_args_list:
                self.assertEqual(call.kwargs['IfNoneMatch'], '*')
                self.assertEqual(call.kwargs['ServerSideEncryption'], 'AES256')
                self.assertIn('/report-dashboard-deployments/', call.kwargs['Key'])
            self.assertEqual(len(objects), clients['s3'].put_object.call_count)
            clients['scheduler'].update_schedule.assert_not_called()


if __name__ == '__main__':
    unittest.main()
