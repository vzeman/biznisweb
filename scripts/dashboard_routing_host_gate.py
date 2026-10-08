#!/usr/bin/env python3
"""Read-only Fargate check of real HTML reports and cross-project navigation."""
import base64
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import threading
import socket
from http.server import ThreadingHTTPServer
from urllib.request import urlopen
from urllib.parse import urlparse
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import live_dashboard_server as dashboard


PERIODS = ('7d', '30d', '90d', 'full')


def report_path_allowed(path):
    path = urlparse(path).path
    return path in {'/health', '/__routing_host_marker'} or path in {
        f'/{route}/{project}' for route in ('report', 'dashboard') for project in ('roy', 'vevo')
    } or path in {f'/api/{project}/latest' for project in ('roy', 'vevo')}


@contextmanager
def report_read_only_boundary():
    """Candidate application boundary: only artifact reads, never business state."""
    import boto3
    original_client = boto3.client

    class ReadOnlyS3:
        def __init__(self, client):
            self.client = client

        def __getattr__(self, name):
            if name not in {'get_object', 'head_object'}:
                raise RuntimeError('report-host-aws-write-or-unsupported-read-blocked')
            return getattr(self.client, name)

    def client(name, *args, **kwargs):
        if name != 's3':
            raise RuntimeError('report-host-non-artifact-client-blocked')
        return ReadOnlyS3(original_client(name, *args, **kwargs))

    with patch('boto3.client', side_effect=client), \
         patch.object(dashboard, 'get_cached_roy_operations_snapshot', side_effect=RuntimeError('report-host-operations-blocked')), \
         patch.object(dashboard, 'get_cached_production_board_snapshot', side_effect=RuntimeError('report-host-production-blocked')):
        yield


def verify_report_http(project, read, *, expected_to_date):
    """Shared host/public GET checks. Returned hashes bind the promoted data."""
    from daily_report_runner import validate_publish_quality
    assert project in {'roy', 'vevo'}
    assert json.loads(read('/health'))['ok'] is True
    results = []
    for period in PERIODS:
        html = read(f'/report/{project}?period={period}')
        assert len(html) > 1000 and b'<html' in html.lower()
        assert b'report-dashboard-json' in html and b'Net MER' in html
        assert b'not platform-attributed ROAS' in html
        assert f'/report/{project}?period={period}'.encode() in html
        raw = read(f'/api/{project}/latest?period={period}')
        data = json.loads(raw)
        assert data['project'] == project and data['period_switcher']['current_key'] == period
        assert data['date_to'] == expected_to_date
        validate_publish_quality(data.get('source_health'), f'report-host-{period}')
        shell = read(f'/dashboard/{project}?period={period}')
        assert b'Net MER' in shell and b'Observation only' in shell and b'not platform-attributed ROAS' in shell
        results.append({'project': project, 'period': period, 'date_to': data['date_to'],
                        'html_bytes': len(html), 'sha256': hashlib.sha256(html).hexdigest(),
                        'payload_sha256': hashlib.sha256(raw).hexdigest(),
                        'shell_sha256': hashlib.sha256(shell).hexdigest()})
    return results


def main():
    project = os.environ['REPORT_PROJECT']
    assert project in {'roy', 'vevo'}
    metadata_uri = os.environ['ECS_CONTAINER_METADATA_URI_V4']
    with urlopen(metadata_uri + '/task', timeout=5) as response:
        task = json.load(response)
    identity = {'instance_id': 'N/A (ECS/Fargate)', 'task_arn': task['TaskARN'],
                'private_ips': sorted({ip for c in task['Containers'] for n in c.get('Networks', []) for ip in n.get('IPv4Addresses', [])}),
                'service': os.environ['EXPECTED_APP_RUNNER_SERVICE'], 'project': project,
                'path': str(Path.cwd()), 'pid': os.getpid(), 'parent_pid': os.getppid(),
                'executable': sys.executable, 'command': sys.argv,
                'images': [{'image': c['Image'], 'image_id': c.get('ImageID')} for c in task['Containers']]}
    assert identity['path'] == '/app' and identity['private_ips']
    token = base64.b64encode(('host-check:' + secrets.token_urlsafe(24)).encode()).decode()
    username, password = base64.b64decode(token).decode().split(':', 1)
    os.environ['LIVE_DASHBOARD_AUTH_USER'] = username
    os.environ['LIVE_DASHBOARD_AUTH_PASSWORD'] = password

    class MarkerHandler(dashboard.LiveDashboardHandler):
        def do_GET(self):
            if not report_path_allowed(self.path):
                self._send_json({'error': 'report-only host route'}, status=405)
                return
            if self.path == '/__routing_host_marker':
                self._send_json({'marker': 'dashboard-project-routing-v1', **identity})
            else:
                super().do_GET()

        def do_POST(self):
            self._send_json({'error': 'report-only host route'}, status=405)

    httpd = ThreadingHTTPServer(('127.0.0.1', 0), MarkerHandler)
    port = httpd.server_port
    worker = threading.Thread(target=httpd.serve_forever, daemon=True)
    worker.start()
    results = []
    def curl(path, authenticated=True):
        config = f'url = "http://127.0.0.1:{port}{path}"\n'
        if authenticated:
            config += f'header = "Authorization: Basic {token}"\n'
        output = subprocess.run(['curl', '--silent', '--show-error', '--max-time', '60', '--include', '--config', '-'],
                                input=config.encode(), capture_output=True, check=True).stdout
        headers, body = output.split(b'\r\n\r\n', 1)
        status = int(headers.splitlines()[0].split()[1])
        return status, headers.decode(), body
    try:
        code, _, marker = curl('/__routing_host_marker', authenticated=False)
        assert code == 200 and json.loads(marker)['marker'] == 'dashboard-project-routing-v1'
        print('DASHBOARD_ROUTING_IDENTITY ' + json.dumps({**identity, 'port': port}), flush=True)
        def read(path):
            assert report_path_allowed(path)
            code, _, body = curl(path)
            assert code == 200
            return body

        with report_read_only_boundary():
            results = verify_report_http(project, read, expected_to_date=os.environ['EXPECTED_REPORT_TO_DATE'])
        foreign = 'vevo' if project == 'roy' else 'roy'
        expected = dashboard.remote_dashboard_origin(foreign)
        assert expected
        for route in ['report', 'dashboard']:
            code, headers, body = curl(f'/{route}/{foreign}?period=30d')
            assert code == 302 and f'Location: {expected}/{route}/{foreign}?period=30d' in headers and not body
        code, _, _ = curl(f'/api/{foreign}/latest?period=full')
        assert code == 409
        code, _, _ = curl(f'/report/{project}?period=full', authenticated=False)
        assert code == 401
        print('DASHBOARD_ROUTING_HOST_OK ' + json.dumps({'marker': 'dashboard-project-routing-v1', 'mode': 'reporting',
              'identity': identity, 'reports': results, 'foreign_redirects': True, 'authentication_required': True,
              'read_only_routes': True, 'business_state_writes': False}), flush=True)
    finally:
        httpd.shutdown()
        httpd.server_close()
        worker.join(timeout=5)
        assert not worker.is_alive()
        with socket.socket() as check:
            assert check.connect_ex(('127.0.0.1', port)) != 0
        print('DASHBOARD_ROUTING_LOCAL_SERVER_CLOSED', flush=True)


if __name__ == '__main__':
    main()
