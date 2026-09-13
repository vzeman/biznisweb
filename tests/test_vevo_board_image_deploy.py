import copy
import unittest

from scripts import deploy_vevo_board_image as deploy


class VevoBoardImageDeployTests(unittest.TestCase):
    def test_promotion_payload_changes_only_image_and_preserves_input(self):
        service = {'SourceConfiguration': {
            'AutoDeploymentsEnabled': False,
            'AuthenticationConfiguration': {'AccessRoleArn': 'existing-role'},
            'ImageRepository': {'ImageIdentifier': 'old-image', 'ImageRepositoryType': 'ECR',
                                'ImageConfiguration': {'Port': '8080', 'RuntimeEnvironmentSecrets': {'TOKEN': 'reference'}}},
        }, 'InstanceConfiguration': {'Cpu': '256'}}
        baseline = copy.deepcopy(service)
        result = deploy.image_update(service, 'new-image')
        self.assertEqual(set(result), {'ServiceArn', 'SourceConfiguration'})
        self.assertEqual(result['ServiceArn'], deploy.SERVICE_ARN)
        self.assertEqual(service, baseline)
        result['SourceConfiguration']['ImageRepository']['ImageIdentifier'] = 'old-image'
        self.assertEqual(result['SourceConfiguration'], baseline['SourceConfiguration'])

    def test_host_proof_rejects_wrong_task_image_exit_ip_and_status(self):
        receipt = {'task_arn': 'task-a', 'definition': 'definition-a', 'started_by': 'probe-a', 'digest': 'digest-a'}
        task = {'taskArn': 'task-a', 'taskDefinitionArn': 'definition-a', 'startedBy': 'probe-a',
                'lastStatus': 'STOPPED', 'containers': [{'exitCode': 0, 'imageDigest': 'digest-a'}],
                'attachments': [{'details': [{'name': 'privateIPv4Address', 'value': '172.31.1.1'}]}]}
        proof = {'identity': {'task_arn': 'task-a', 'service': deploy.SERVICE, 'path': '/app', 'private_ips': ['172.31.1.1']},
                 'statuses': deploy.STATUSES, 'summary': {'active_orders': 1, 'units_to_make': 2}}
        deploy.validate_proof(task, proof, receipt)
        mutations = [
            lambda t, p: t.update(taskArn='another-task'),
            lambda t, p: t.update(startedBy='another-owner'),
            lambda t, p: t.update(lastStatus='RUNNING'),
            lambda t, p: t['containers'][0].update(exitCode=1),
            lambda t, p: t['containers'][0].update(imageDigest='old-image'),
            lambda t, p: p['identity'].update(private_ips=['172.31.2.2']),
            lambda t, p: p['identity'].update(path='/wrong'),
            lambda t, p: p.update(statuses=['old-status']),
            lambda t, p: p['summary'].update(units_to_make=0),
        ]
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                changed_task, changed_proof = copy.deepcopy(task), copy.deepcopy(proof)
                mutate(changed_task, changed_proof)
                with self.assertRaises(AssertionError):
                    deploy.validate_proof(changed_task, changed_proof, receipt)

    def test_host_command_is_valid_python_and_blocks_redirects(self):
        compile(deploy.HOST_PROBE, '<host-probe>', 'exec')
        self.assertIsNone(deploy.NoRedirect().redirect_request(None, None, 302, '', {}, 'https://untrusted.invalid'))


if __name__ == '__main__':
    unittest.main()
