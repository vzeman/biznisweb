import copy
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from scripts import deploy_vevo_report as deploy
from scripts import recover_vevo_baseline_preflight as recovery


class BootstrapRecoveryTests(unittest.TestCase):
    def test_competing_workflow_rejects_before_lease_acquisition(self):
        obj = object.__new__(deploy.Deployment)
        obj.commit, obj.s3 = 'a' * 40, Mock()
        obj.session = Mock()
        obj.session.client.return_value.get_caller_identity.return_value = {'Account': deploy.ACCOUNT}
        obj.binding = Mock()
        obj.lease = Mock()
        obj.exclusion = Mock(side_effect=RuntimeError('report-competing-managed-run'))
        with patch.object(deploy, 'current_main'), self.assertRaisesRegex(RuntimeError, 'competing'):
            obj.bootstrap()
        obj.lease.acquire.assert_not_called()

    def test_read_only_recovery_does_not_write(self):
        obj = Mock()
        with patch.object(recovery, 'inspect', return_value={}):
            result = recovery.recover(obj)
        self.assertFalse(result['applied'])
        obj.s3.put_object.assert_not_called()

    def test_failed_inspection_never_writes(self):
        obj = Mock()
        with patch.object(recovery, 'inspect', side_effect=RuntimeError('recovery-launch-attempt')):
            with self.assertRaisesRegex(RuntimeError, 'launch-attempt'):
                recovery.recover(obj, apply=True)
        obj.s3.put_object.assert_not_called()

    def test_release_preserves_incident_owner_and_uses_exact_cas(self):
        before = {'lock': {'owner': recovery.OWNER, 'generation': recovery.GENERATION, 'state': 'uncertain'},
                  'etag': 'original', 'schedule': {}, 'protected': {}}
        storage = {}
        obj = SimpleNamespace(s3=Mock(), schedule=lambda: {}, snapshot_protected=lambda: {})
        def put(**request):
            import json
            storage[request['Key']] = json.loads(request['Body'])
            self.assertEqual(recovery.binding.canonical_bytes(storage[request['Key']]), request['Body'])
        obj.s3.put_object.side_effect = put
        def read(_s3, key, **_):
            if key == recovery.binding.CURRENT_KEY:
                return None
            return storage[key], 'new'
        def write(_s3, key, value, *, etag):
            self.assertEqual('original', etag)
            self.assertEqual(recovery.OWNER, value['owner'])
            self.assertEqual('released', value['state'])
            storage[key] = value
            return 'new'
        with patch.object(recovery, 'inspect', return_value=copy.deepcopy(before)), \
                patch.object(recovery.binding, 'read_object', side_effect=read), \
                patch.object(recovery.binding, 'write_object', side_effect=write) as cas, \
                patch.object(recovery.binding, 'schedule_snapshot', side_effect=lambda x: x):
            result = recovery.recover(obj, apply=True)
        self.assertTrue(result['applied'])
        cas.assert_called_once()


if __name__ == '__main__':
    unittest.main()
