#!/usr/bin/env python3
"""Probe a pinned VEVO image on Fargate, then promote only its App Runner image.

Run from an authenticated AWS shell and a verified Git checkout. No IAM,
scheduler, report artifact or provider-order mutations are performed.
"""
import argparse
import base64
import copy
import hashlib
import json
from pathlib import Path
import re
import time
from urllib.request import HTTPRedirectHandler, Request, build_opener

ACCOUNT = "919341186960"
REGION = "eu-central-1"
SERVICE = "biznisweb-vevo-production-board"
SERVICE_ARN = f"arn:aws:apprunner:{REGION}:{ACCOUNT}:service/{SERVICE}/2711a253ae014a8aaf1a37929997496d"
ORIGIN = "https://2mhmsmgq3m.eu-central-1.awsapprunner.com"
REPOSITORY = f"{ACCOUNT}.dkr.ecr.{REGION}.amazonaws.com/vevo-reporting"
STATUSES = ["New order", "Payment online - paid"]

# Sent as the command of the isolated task; versioned here rather than installed
# into or copied over application files inside the candidate image.
HOST_PROBE = r'''
import base64, json, os, secrets, subprocess, sys, threading
from pathlib import Path
from http.server import ThreadingHTTPServer
from urllib.request import urlopen
import live_dashboard_server as dashboard
from reporting_core import load_project_settings
with urlopen(os.environ['ECS_CONTAINER_METADATA_URI_V4'] + '/task', timeout=5) as response:
    metadata = json.load(response)
identity = {
    'instance_id': 'N/A (ECS/Fargate)', 'task_arn': metadata['TaskARN'],
    'private_ips': sorted({ip for c in metadata['Containers'] for n in c.get('Networks', []) for ip in n.get('IPv4Addresses', [])}),
    'service': 'biznisweb-vevo-production-board', 'path': str(Path.cwd()),
    'pid': os.getpid(), 'parent_pid': os.getppid(), 'executable': sys.executable,
    'images': [{'image': c['Image'], 'image_id': c.get('ImageID')} for c in metadata['Containers']],
}
assert identity['path'] == '/app' and identity['private_ips']
assert load_project_settings('vevo')['production_board']['active_order_statuses'] == ['New order', 'Payment online - paid']
os.environ['LIVE_DASHBOARD_AUTH_USER'] = 'host-probe'
os.environ['LIVE_DASHBOARD_AUTH_PASSWORD'] = secrets.token_urlsafe(32)
token = base64.b64encode(('host-probe:' + os.environ['LIVE_DASHBOARD_AUTH_PASSWORD']).encode()).decode()
class Handler(dashboard.LiveDashboardHandler):
    def do_GET(self):
        if self.path == '/__vevo_board_marker':
            self._send_json({'marker': 'VEVO_BOARD_HOST_IDENTITY', **identity})
        else:
            super().do_GET()
server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
port = server.server_port
worker = threading.Thread(target=server.serve_forever, daemon=True)
worker.start()
def curl(path):
    config = f'url = "http://127.0.0.1:{port}{path}"\nheader = "Authorization: Basic {token}"\n'
    return subprocess.run(['curl', '--fail', '--silent', '--show-error', '--max-time', '240', '--config', '-'], input=config.encode(), capture_output=True, check=True).stdout
try:
    assert json.loads(curl('/health'))['ok'] is True
    assert json.loads(curl('/__vevo_board_marker'))['task_arn'] == identity['task_arn']
    print('VEVO_BOARD_HOST_IDENTITY ' + json.dumps({**identity, 'port': port}), flush=True)
    assert b'vevo-production-board' in curl('/production/vevo')
    data = json.loads(curl('/api/production/vevo/live?refresh=1'))
    assert data['project'] == 'vevo' and data['active_order_statuses'] == ['New order', 'Payment online - paid']
    assert data['summary']['active_orders'] > 0 and data['summary']['units_to_make'] > 0
    assert {o['status'] for o in data['orders']} <= set(data['active_order_statuses'])
    assert sum(p['quantity_required'] for p in data['products']) == data['summary']['units_to_make']
    print('VEVO_BOARD_HOST_OK ' + json.dumps({'identity': identity, 'summary': data['summary'], 'scan': data['scan'], 'statuses': data['active_order_statuses']}), flush=True)
finally:
    server.shutdown()
    server.server_close()
    worker.join(timeout=5)
    assert not worker.is_alive()
    import socket
    with socket.socket() as check:
        assert check.connect_ex(('127.0.0.1', port)) != 0
    print('VEVO_BOARD_HOST_CLOSED', flush=True)
'''


def check_service(service, expected_image):
    assert service['ServiceArn'] == SERVICE_ARN and service['ServiceName'] == SERVICE
    assert service['Status'] == 'RUNNING' and 'https://' + service['ServiceUrl'] == ORIGIN
    source = service['SourceConfiguration']
    assert source['ImageRepository']['ImageIdentifier'] == expected_image, 'Current image drift'
    assert source['AutoDeploymentsEnabled'] is False
    config = source['ImageRepository']['ImageConfiguration']
    assert config['Port'] == '8080'
    assert config['StartCommand'] == 'python live_dashboard_server.py --host 0.0.0.0 --port 8080'
    assert config['RuntimeEnvironmentVariables']['REPORT_PROJECT'] == 'vevo'


def service_boundary(service):
    return {key: service.get(key) for key in (
        'ServiceArn', 'ServiceName', 'ServiceUrl', 'SourceConfiguration',
        'InstanceConfiguration', 'HealthCheckConfiguration',
        'AutoScalingConfigurationSummary', 'NetworkConfiguration',
        'EncryptionConfiguration', 'ObservabilityConfiguration',
    )}


def image_update(service, image):
    source = copy.deepcopy(service['SourceConfiguration'])
    source['ImageRepository']['ImageIdentifier'] = image
    return {'ServiceArn': SERVICE_ARN, 'SourceConfiguration': source}


def validate_proof(task, proof, receipt):
    assert task['taskArn'] == receipt['task_arn']
    assert task['taskDefinitionArn'] == receipt['definition']
    assert task['startedBy'] == receipt['started_by']
    assert task['lastStatus'] == 'STOPPED'
    assert len(task['containers']) == 1
    container = task['containers'][0]
    assert container.get('exitCode') == 0 and container['imageDigest'] == receipt['digest']
    assert proof['identity']['task_arn'] == task['taskArn']
    assert proof['identity']['service'] == SERVICE and proof['identity']['path'] == '/app'
    ips = {x['value'] for a in task.get('attachments', []) for x in a.get('details', []) if x['name'] == 'privateIPv4Address'}
    assert ips and set(proof['identity']['private_ips']) == ips
    assert proof['statuses'] == STATUSES
    assert proof['summary']['active_orders'] > 0 and proof['summary']['units_to_make'] > 0


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase', choices=['probe', 'promote'])
    parser.add_argument('--source-sha', required=True)
    parser.add_argument('--expected-current-digest', required=True)
    parser.add_argument('--receipt', type=Path, required=True)
    args = parser.parse_args()
    assert re.fullmatch(r'[0-9a-f]{40}', args.source_sha)
    assert re.fullmatch(r'sha256:[0-9a-f]{64}', args.expected_current_digest)
    import boto3
    from botocore.config import Config
    session = boto3.Session(region_name=REGION)
    clients = {name: session.client(name, config=Config(retries={'max_attempts': 0}, connect_timeout=10, read_timeout=30)) for name in ['sts', 'apprunner', 'ecs', 'ecr', 'scheduler', 'logs', 'ssm']}
    assert clients['sts'].get_caller_identity()['Account'] == ACCOUNT
    app, ecs = clients['apprunner'], clients['ecs']
    digest = clients['ecr'].describe_images(repositoryName='vevo-reporting', imageIds=[{'imageTag': 'git-' + args.source_sha}])['imageDetails'][0]['imageDigest']
    image, previous = REPOSITORY + '@' + digest, REPOSITORY + '@' + args.expected_current_digest
    current = app.describe_service(ServiceArn=SERVICE_ARN)['Service']
    check_service(current, previous)
    print(json.dumps({'marker': 'VEVO_BOARD_DEPLOY_PREFLIGHT', 'instance_id': 'N/A (App Runner managed)', 'host_ip': 'N/A (managed; service URL is authoritative)', 'service': SERVICE_ARN, 'path': '/app', 'previous': previous, 'candidate': image}), flush=True)

    def save(receipt):
        args.receipt.parent.mkdir(parents=True, exist_ok=True)
        args.receipt.write_text(json.dumps(receipt, indent=2, default=str) + '\n', encoding='utf-8')

    def read_task(receipt):
        response = ecs.describe_tasks(cluster=receipt['cluster'], tasks=[receipt['task_arn']])
        assert not response.get('failures') and len(response['tasks']) == 1
        return response['tasks'][0]

    def read_proof(receipt):
        messages, token = [], None
        while True:
            kwargs = {'logGroupName': receipt['log_group'], 'logStreamName': receipt['log_stream'], 'startFromHead': True}
            if token:
                kwargs['nextToken'] = token
            response = clients['logs'].get_log_events(**kwargs)
            messages.extend(e['message'] for e in response['events'])
            next_token = response['nextForwardToken']
            if next_token == token:
                break
            token = next_token
            assert len(messages) < 1000, 'Unexpected log volume'
        proofs = [json.loads(m.split(' ', 1)[1]) for m in messages if m.startswith('VEVO_BOARD_HOST_OK ')]
        assert len(proofs) == 1 and messages.count('VEVO_BOARD_HOST_CLOSED') == 1, 'Host proof absent or ambiguous'
        return proofs[0]

    if args.phase == 'probe':
        assert not args.receipt.exists(), 'Receipt already exists; reconcile it before retrying'
        schedule = clients['scheduler'].get_schedule(Name='vevo-daily-report-email')
        target = schedule['Target']
        source = ecs.describe_task_definition(taskDefinition=target['EcsParameters']['TaskDefinitionArn'])['taskDefinition']
        assert source['family'] == 'vevo-reporting-daily' and len(source['containerDefinitions']) == 1
        original = source['containerDefinitions'][0]
        api_secrets = [s for s in original['secrets'] if s['name'] == 'BIZNISWEB_API_TOKEN']
        assert len(api_secrets) == 1
        container = {
            'name': 'vevo-board-probe', 'image': image, 'essential': True,
            'command': ['python', '-c', HOST_PROBE], 'workingDirectory': '/app',
            'secrets': api_secrets, 'logConfiguration': original['logConfiguration'],
            'environment': [{'name': k, 'value': v} for k, v in {
                'REPORT_PROJECT': 'vevo', 'REPORT_SKIP_PROJECT_ENV': 'true',
                'AWS_REGION': REGION, 'BIZNISWEB_API_TIMEOUT_SEC': '30',
            }.items()],
        }
        assert container['logConfiguration']['logDriver'] == 'awslogs'
        receipt = {'source_sha': args.source_sha, 'digest': digest, 'previous': previous,
                   'baseline': service_boundary(current), 'started_by': 'vevo-board-' + args.source_sha[:12],
                   'cluster': target['Arn'], 'phase': 'registering', 'host_code_sha256': hashlib.sha256(HOST_PROBE.encode()).hexdigest()}
        save(receipt)
        registered = ecs.register_task_definition(
            family='vevo-board-probe-' + args.source_sha[:12], executionRoleArn=source['executionRoleArn'],
            networkMode='awsvpc', requiresCompatibilities=['FARGATE'],
            cpu=source['cpu'], memory=source['memory'], containerDefinitions=[container])
        receipt['definition'] = registered['taskDefinition']['taskDefinitionArn']
        save(receipt)
        raw = target['EcsParameters']['NetworkConfiguration']['awsvpcConfiguration']
        network = {'awsvpcConfiguration': {k[0].lower() + k[1:]: v for k, v in raw.items()}}
        response = ecs.run_task(cluster=receipt['cluster'], taskDefinition=receipt['definition'],
                                launchType='FARGATE', networkConfiguration=network,
                                startedBy=receipt['started_by'], count=1, clientToken=receipt['started_by'])
        assert not response.get('failures') and len(response['tasks']) == 1, 'Task start requires reconciliation'
        receipt['task_arn'] = response['tasks'][0]['taskArn']
        options = container['logConfiguration']['options']
        receipt['log_group'] = options['awslogs-group']
        receipt['log_stream'] = options['awslogs-stream-prefix'] + '/vevo-board-probe/' + receipt['task_arn'].rsplit('/', 1)[1]
        receipt['phase'] = 'probing'
        save(receipt)
        print('VEVO_BOARD_TASK ' + receipt['task_arn'], flush=True)
        last = None
        for _ in range(120):
            task = read_task(receipt)
            if task['lastStatus'] != last:
                last = task['lastStatus']
                print('VEVO_BOARD_TASK_STATUS ' + last, flush=True)
            if last == 'STOPPED':
                break
            time.sleep(5)
        if task['lastStatus'] != 'STOPPED':
            assert task['startedBy'] == receipt['started_by'] and task['taskDefinitionArn'] == receipt['definition']
            ecs.stop_task(cluster=receipt['cluster'], task=receipt['task_arn'], reason='Bounded VEVO board probe timeout')
            ecs.get_waiter('tasks_stopped').wait(cluster=receipt['cluster'], tasks=[receipt['task_arn']], WaiterConfig={'Delay': 5, 'MaxAttempts': 24})
            task = read_task(receipt)
        assert task['lastStatus'] == 'STOPPED'
        ecs.deregister_task_definition(taskDefinition=receipt['definition'])
        assert ecs.describe_task_definition(taskDefinition=receipt['definition'])['taskDefinition']['status'] == 'INACTIVE'
        proof = read_proof(receipt)
        validate_proof(task, proof, receipt)
        receipt.update({'phase': 'verified', 'proof': proof, 'task': task, 'definition_inactive': True, 'verified_at': time.time()})
        save(receipt)
        print('VEVO_BOARD_PROBE_VERIFIED ' + json.dumps(proof), flush=True)
        return

    receipt = json.loads(args.receipt.read_text(encoding='utf-8'))
    assert receipt['phase'] == 'verified' and 0 <= time.time() - receipt['verified_at'] < 1800
    assert receipt['source_sha'] == args.source_sha and receipt['digest'] == digest and receipt['previous'] == previous
    assert service_boundary(current) == receipt['baseline'], 'Service configuration drift'
    proof = read_proof(receipt)
    assert proof == receipt['proof']
    validate_proof(read_task(receipt), proof, receipt)
    update = image_update(current, image)
    receipt['phase'] = 'promoting'
    save(receipt)
    response = app.update_service(**update)
    receipt['operation_id'] = response['OperationId']
    save(receipt)
    print('VEVO_BOARD_OPERATION ' + receipt['operation_id'], flush=True)
    for _ in range(120):
        operations = app.list_operations(ServiceArn=SERVICE_ARN)['OperationSummaryList']
        matches = [o for o in operations if o['Id'] == receipt['operation_id']]
        assert len(matches) == 1
        status = matches[0]['Status']
        if status not in {'PENDING', 'IN_PROGRESS', 'ROLLBACK_IN_PROGRESS'}:
            break
        time.sleep(5)
    assert status == 'SUCCEEDED', 'Inspect recorded App Runner operation; do not repeat promotion'
    deployed = app.describe_service(ServiceArn=SERVICE_ARN)['Service']
    check_service(deployed, image)
    expected = copy.deepcopy(receipt['baseline'])
    expected['SourceConfiguration'] = update['SourceConfiguration']
    assert service_boundary(deployed) == expected, 'Non-image service configuration changed'
    config = deployed['SourceConfiguration']['ImageRepository']['ImageConfiguration']
    password = clients['ssm'].get_parameter(Name=config['RuntimeEnvironmentSecrets']['LIVE_DASHBOARD_AUTH_PASSWORD'], WithDecryption=True)['Parameter']['Value']
    auth = base64.b64encode((config['RuntimeEnvironmentVariables']['LIVE_DASHBOARD_AUTH_USER'] + ':' + password).encode()).decode()
    opener = build_opener(NoRedirect())
    def live(path):
        with opener.open(Request(ORIGIN + path, headers={'Authorization': 'Basic ' + auth}), timeout=240) as response:
            assert response.geturl() == ORIGIN + path, 'Unexpected redirect'
            return response.read()
    assert json.loads(live('/health'))['ok'] is True
    assert b'vevo-production-board' in live('/production/vevo')
    data = json.loads(live('/api/production/vevo/live?refresh=1'))
    assert data['project'] == 'vevo' and data['active_order_statuses'] == STATUSES
    assert data['summary']['active_orders'] > 0 and data['summary']['units_to_make'] > 0
    receipt.update({'phase': 'deployed', 'live_summary': data['summary'], 'live_scan': data['scan']})
    save(receipt)
    print('VEVO_BOARD_DEPLOYED ' + json.dumps({'image': image, 'operation_id': receipt['operation_id'], 'summary': data['summary'], 'scan': data['scan']}), flush=True)


if __name__ == '__main__':
    main()
