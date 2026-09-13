import copy
import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from scripts import deploy_vevo_report as deploy


def definition():
    return {'family': 'vevo-reporting-daily', 'networkMode': 'awsvpc', 'cpu': '1024', 'memory': '2048',
            'taskRoleArn': 'original-role', 'executionRoleArn': 'original-execution',
            'containerDefinitions': [{'name': 'reporting', 'image': 'old-image',
                'environment': [{'name': 'REPORT_PROJECT', 'value': 'vevo'}, {'name': 'REPORT_S3_PREFIX', 'value': 'daily-reports/vevo'}],
                'secrets': [{'name': 'BIZNISWEB_API_TOKEN', 'valueFrom': 'secret-reference'}]}]}


IMAGE = f'{deploy.ACCOUNT}.dkr.ecr.{deploy.REGION}.amazonaws.com/vevo-reporting@sha256:' + 'a' * 64


class DeploymentTests(unittest.TestCase):
    def test_role_create_lost_acknowledgement_is_reconciled_and_cleaned_without_replay(self):
        class Missing(Exception):
            response = {'Error': {'Code': 'NoSuchEntity'}}
        for acknowledgement in ('lost-before', 'lost-after'):
            obj = object.__new__(deploy.Deployment)
            obj.release_id = 'b' * 32
            obj.role_created = obj.role_attempted = False
            obj.cleanup_task = Mock()
            state = {}
            def get(**_):
                if not state:
                    raise Missing()
                return {'Role': copy.deepcopy(state)}
            def create(**request):
                if acknowledgement == 'lost-after':
                    state.update(Arn=f'arn:aws:iam::{deploy.ACCOUNT}:role/' + request['RoleName'],
                                 Tags=request['Tags'], AssumeRolePolicyDocument=json.loads(request['AssumeRolePolicyDocument']))
                raise RuntimeError('response lost')
            obj.iam = SimpleNamespace(get_role=get, create_role=Mock(side_effect=create),
                list_role_policies=lambda **_: {'PolicyNames': []}, list_attached_role_policies=lambda **_: {'AttachedPolicies': []},
                delete_role=Mock(side_effect=lambda **_: state.clear()))
            with self.subTest(acknowledgement=acknowledgement), self.assertRaisesRegex(RuntimeError, 'response lost'):
                obj.create_role()
            self.assertEqual(acknowledgement == 'lost-after', obj.role_created)
            obj.cleanup_role()
            self.assertEqual({}, state)
            obj.iam.create_role.assert_called_once()
            self.assertEqual(acknowledgement == 'lost-after', obj.iam.delete_role.called)

    def test_terminal_release_readback_requires_the_exact_new_migration_generation(self):
        obj = object.__new__(deploy.Deployment)
        obj.s3 = object()
        obj.release_attempted = False
        obj.lease = Mock(etag='new-generation')
        obj.binding = SimpleNamespace(load_current_binding=Mock(return_value={'migration_etag': 'foreign-generation'}))
        with self.assertRaisesRegex(RuntimeError, 'terminal-authority-drift'):
            obj.release_verified({'migration_etag': 'prior-generation'})
        self.assertTrue(obj.release_attempted)

    def managed_transaction(self, failure=None):
        """Exercise real run/update/rollback with independent scheduler/pointer stores."""
        obj = object.__new__(deploy.Deployment)
        obj.commit, obj.release_id = 'c' * 40, 'b' * 32
        obj.s3 = object()
        obj.owned_task = obj.probe_definition = obj.production_definition = None
        obj.known_schedule = obj.original = obj.previous = obj.protected = None
        obj.role_created = obj.role_attempted = obj.release_attempted = obj.start_uncertain = False
        source = {**definition(), 'taskDefinitionArn': 'old-definition'}
        state = {'Name': deploy.SERVICE, 'State': 'ENABLED', 'Target': {'EcsParameters': {'TaskDefinitionArn': 'old-definition'}}}
        original = copy.deepcopy(state)
        pointer = {'record': {'release_id': 'prior', 'task_definition': source}, 'record_sha256': 'old',
                   'record_key': 'old-key', 'pointer_etag': 'old-etag'}
        definitions, writes, phases = {'old-definition': source}, [], []
        obj.lease = Mock(etag='active-generation')
        obj.lease.release.side_effect = lambda: setattr(obj.lease, 'etag', 'released-generation')
        obj.schedule = lambda: copy.deepcopy(state)
        obj.definition = lambda arn: copy.deepcopy(definitions[arn])
        def update(**request):
            writes.append(copy.deepcopy(request))
            state.clear()
            state.update(copy.deepcopy(request))
        obj.scheduler = SimpleNamespace(update_schedule=update)
        def register(**request):
            arn = 'definition-' + str(len(definitions))
            definitions[arn] = {**copy.deepcopy(request), 'taskDefinitionArn': arn}
            return {'taskDefinition': copy.deepcopy(definitions[arn])}
        obj.ecs = SimpleNamespace(register_task_definition=register)
        obj.session = SimpleNamespace(client=lambda name: SimpleNamespace(get_caller_identity=lambda: {'Account': deploy.ACCOUNT}))
        def load(*_args, **_kwargs):
            return {**copy.deepcopy(pointer), 'migration_etag': obj.lease.etag}
        def publish(_s3, record, **_kwargs):
            if failure == 'pointer-before':
                raise RuntimeError('pointer rejected')
            pointer.update(record=copy.deepcopy(record), record_sha256='new', record_key='new-key', pointer_etag='new-etag')
            if failure == 'pointer-after':
                raise RuntimeError('pointer acknowledgement lost')
            return load()
        def restore(_s3, previous, **_kwargs):
            pointer.clear()
            pointer.update({k: copy.deepcopy(v) for k, v in previous.items() if k != 'migration_etag'})
            pointer['pointer_etag'] = 'restored-etag'
            return load()
        obj.binding = SimpleNamespace(require_private_bucket=Mock(), load_current_binding=load,
            verify_managed_provenance=Mock(), validate_runtime=Mock(), schedule_snapshot=copy.deepcopy,
            definition_snapshot=copy.deepcopy, sha256=lambda value: deploy.sha(deploy.canonical(value)),
            build_promotion_record=lambda _old, _schedule, td, **_kw: {'release_id': obj.release_id, 'task_definition': td},
            publish_verified_binding=publish, restore_previous_binding=Mock(side_effect=restore))
        obj.readiness = lambda: setattr(obj, 'protected', {'verified': True})
        obj.checkpoint = Mock()
        obj.existing_host = Mock()
        obj.exact_image = Mock(return_value=(IMAGE, 'successful-build'))
        obj.cleanup_task = Mock()
        obj.create_role = Mock()
        obj.cleanup_role = Mock()
        obj.drain = Mock()
        obj.probe = Mock(return_value={'verified': True})
        def event(phase, **_kwargs):
            phases.append(phase)
            if failure == 'after-enable' and phase == 'promotion-readback-verified':
                raise RuntimeError('final durable event failed')
            if failure == 'foreign-after-probe' and phase == 'candidate-verified':
                state['Target'] = {'foreign': True}
        obj.event = event
        if failure == 'candidate':
            obj.probe.side_effect = RuntimeError('candidate incomplete')
        if failure == 'image-drift':
            obj.exact_image.side_effect = [(IMAGE, 'successful-build'), (IMAGE[:-1] + '0', 'successful-build')]
        if failure == 'release-readback':
            def release_then_fail():
                obj.lease.etag = 'released-generation'
                raise RuntimeError('release response unavailable')
            obj.lease.release.side_effect = release_then_fail
        return obj, state, original, pointer, writes, phases

    def test_transaction_success_enables_only_after_candidate_and_releases_verified_generation(self):
        obj, state, original, pointer, writes, phases = self.managed_transaction()
        with patch.object(deploy, 'current_main') as main, patch.dict(deploy.os.environ, {'GITHUB_RUN_ID': 'new-run'}):
            obj.run()
        self.assertEqual(['DISABLED', 'DISABLED', 'ENABLED'], [row['State'] for row in writes])
        self.assertEqual(original['Target'], writes[0]['Target'])
        self.assertNotEqual(original['Target'], state['Target'])
        self.assertEqual(obj.release_id, pointer['record']['release_id'])
        self.assertLess(phases.index('candidate-verified'), phases.index('promotion-readback-verified'))
        self.assertGreaterEqual(main.call_count, 4)
        obj.lease.release.assert_called_once()
        obj.lease.retain_uncertain.assert_not_called()
        obj.binding.verify_managed_provenance.assert_any_call(obj.previous, require_completed=True)

    def test_known_failures_restore_original_runtime_and_previous_pointer(self):
        for failure in ('candidate', 'image-drift', 'pointer-before', 'pointer-after', 'after-enable'):
            obj, state, original, pointer, writes, _ = self.managed_transaction(failure)
            with self.subTest(failure=failure), patch.object(deploy, 'current_main'), \
                 patch.dict(deploy.os.environ, {'GITHUB_RUN_ID': 'new-run'}), self.assertRaises(RuntimeError):
                obj.run()
            self.assertEqual(original, state)
            self.assertEqual('old', pointer['record_sha256'])
            self.assertTrue(all(row['Name'] == deploy.SERVICE for row in writes))
            obj.lease.retain_uncertain.assert_not_called()
            obj.cleanup_task.assert_called()
            if failure in ('pointer-after', 'after-enable'):
                obj.binding.restore_previous_binding.assert_called_once()

    def test_foreign_schedule_never_overwritten_and_release_uncertainty_pauses_owned_target(self):
        for failure in ('foreign-after-probe', 'release-readback'):
            obj, state, _, _, writes, _ = self.managed_transaction(failure)
            with self.subTest(failure=failure), patch.object(deploy, 'current_main'), \
                 patch.dict(deploy.os.environ, {'GITHUB_RUN_ID': 'new-run'}), self.assertRaises(RuntimeError):
                obj.run()
            obj.lease.retain_uncertain.assert_called_once()
            if failure == 'foreign-after-probe':
                self.assertEqual({'foreign': True}, state['Target'])
                self.assertEqual(1, len(writes))
            else:
                self.assertEqual('DISABLED', state['State'])

    def test_fresh_main_failure_after_final_drain_never_promotes(self):
        obj, state, original, _, writes, _ = self.managed_transaction()
        with patch.object(deploy, 'current_main', side_effect=[None, None, None, RuntimeError('main moved')]), \
             patch.dict(deploy.os.environ, {'GITHUB_RUN_ID': 'new-run'}), self.assertRaisesRegex(RuntimeError, 'main moved'):
            obj.run()
        self.assertEqual(original, state)
        self.assertTrue(all(row['Target'] == original['Target'] for row in writes))

    def test_quiet_drain_restarts_for_a_late_old_task_and_accepts_clock_zero(self):
        obj = object.__new__(deploy.Deployment)
        clock = [0]
        obj.clock = lambda: clock[0]
        obj.sleep = lambda seconds: clock.__setitem__(0, clock[0] + seconds)
        obj.checkpoint = Mock()
        obj.tasks = lambda: [{'lastStatus': 'RUNNING'}] if clock[0] == 110 else []
        obj.drain()
        self.assertEqual(240, clock[0])

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
