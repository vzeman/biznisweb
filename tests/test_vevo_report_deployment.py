import copy
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from scripts import deploy_vevo_report as deploy


def definition():
    return {'family': 'vevo-reporting-daily', 'networkMode': 'awsvpc', 'cpu': '1024', 'memory': '2048',
            'taskRoleArn': 'original-role', 'executionRoleArn': 'original-execution',
            'containerDefinitions': [{'name': 'reporting', 'image': 'old-image',
                'environment': [{'name': 'REPORT_PROJECT', 'value': 'vevo'}, {'name': 'REPORT_S3_PREFIX', 'value': 'daily-reports/vevo'}],
                'secrets': [{'name': 'BIZNISWEB_API_TOKEN', 'valueFrom': 'secret-reference'}]}]}


IMAGE = f'{deploy.ACCOUNT}.dkr.ecr.{deploy.REGION}.amazonaws.com/vevo-reporting@sha256:' + 'a' * 64


class DeploymentTests(unittest.TestCase):
    def test_candidate_preserves_input_credentials_resources_and_production_email(self):
        source = definition()
        saved = copy.deepcopy(source)
        prod = deploy.candidate_definition(source, IMAGE)
        probe = deploy.candidate_definition(source, IMAGE, 'b' * 32, source_commit='c' * 40, gate_sha256='d' * 64)
        self.assertEqual(saved, source)
        self.assertEqual(source['taskRoleArn'], prod['taskRoleArn'])
        self.assertNotEqual(prod['taskRoleArn'], probe['taskRoleArn'])
        self.assertNotIn('command', prod['containerDefinitions'][0])
        self.assertEqual(['python', 'scripts/reporting_migration_host_gate.py'], probe['containerDefinitions'][0]['command'][:2])
        for value in (prod, probe):
            self.assertEqual(source['executionRoleArn'], value['executionRoleArn'])
            self.assertEqual(source['containerDefinitions'][0]['secrets'], value['containerDefinitions'][0]['secrets'])
            env = {r['name']: r['value'] for r in value['containerDefinitions'][0]['environment']}
            self.assertEqual('daily-reports/vevo', env['REPORT_S3_PREFIX'])
            self.assertEqual('true', env['REPORT_SKIP_INVOICES'])
            self.assertEqual('true', env['REPORT_SKIP_CREDITNOTE_STORNO_GUARD'])

    def test_unknown_command_secret_override_or_mutable_image_rejects(self):
        for kind in ('command', 'secret', 'image'):
            source = definition()
            image = IMAGE
            if kind == 'command':
                source['containerDefinitions'][0]['command'] = ['python', 'generate_invoices.py']
            elif kind == 'secret':
                source['containerDefinitions'][0]['secrets'].append({'name': 'REPORT_SKIP_EMAIL', 'valueFrom': 'ref'})
            else:
                image = IMAGE.split('@')[0] + ':latest'
            with self.subTest(kind=kind), self.assertRaises(RuntimeError):
                deploy.candidate_definition(source, image)

    def test_diagnostic_writer_cannot_write_signal_live_journal_metrics_or_email(self):
        policy = deploy.diagnostic_policy('b' * 32)
        writers = [s for s in policy['Statement'] if 's3:PutObject' in s['Action']]
        self.assertEqual(1, len(writers))
        resources = writers[0]['Resource']
        self.assertEqual(2, len(resources))
        self.assertTrue(all('/probes/' + 'b' * 32 + '/' in r for r in resources))
        self.assertTrue(all(r.endswith(('markers/*', 'artifacts/*')) for r in resources))
        actions = {a for s in policy['Statement'] for a in s['Action']}
        self.assertEqual({'s3:GetObject', 's3:PutObject'}, actions)

    def transaction(self, fail=None):
        obj = object.__new__(deploy.Deployment)
        state = {'Name': deploy.SERVICE, 'State': 'ENABLED', 'Target': {'safe': True}}
        obj.known_schedule = copy.deepcopy(state)
        obj.binding = SimpleNamespace(schedule_snapshot=lambda v: copy.deepcopy(v))
        obj.schedule = lambda: copy.deepcopy(state)
        def write(**request):
            if fail == 'before':
                raise RuntimeError('lost before write')
            state.clear()
            state.update(request)
            if fail == 'after':
                raise RuntimeError('lost after write')
            if fail == 'drift':
                state['Target'] = {'foreign': True}
        obj.scheduler = SimpleNamespace(update_schedule=write)
        return obj, state

    def test_silent_pause_preserves_all_other_fields(self):
        obj, state = self.transaction()
        original = copy.deepcopy(state)
        obj.update({**original, 'State': 'DISABLED'})
        self.assertEqual({**original, 'State': 'DISABLED'}, state)
        self.assertEqual(state, obj.known_schedule)

    def test_ambiguous_update_records_only_observed_owned_state(self):
        for failure in ('before', 'after'):
            obj, state = self.transaction(failure)
            with self.subTest(failure=failure), self.assertRaises(RuntimeError):
                obj.update({**state, 'State': 'DISABLED'})
            self.assertEqual(state, obj.known_schedule)

    def test_schedule_drift_blocks_rollback_authority(self):
        obj, state = self.transaction('drift')
        with self.assertRaisesRegex(RuntimeError, 'uncertain'):
            obj.update({**state, 'State': 'DISABLED'})
        self.assertNotEqual(state, obj.known_schedule)

    def test_task_cleanup_rejects_an_unowned_task_before_stop(self):
        obj = object.__new__(deploy.Deployment)
        obj.owned_task = 'owned'
        obj.task = Mock(side_effect=RuntimeError('ownership invalid'))
        obj.ecs = Mock()
        with self.assertRaises(RuntimeError):
            obj.cleanup_task()
        obj.ecs.stop_task.assert_not_called()


if __name__ == '__main__':
    unittest.main()
