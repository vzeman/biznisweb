#!/usr/bin/env python3
"""Read-only Fargate check of real HTML reports and cross-project navigation."""
import base64
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import threading
from http.server import ThreadingHTTPServer
from urllib.request import urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import live_dashboard_server as dashboard


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
            if self.path == '/__routing_host_marker':
                self._send_json({'marker': 'dashboard-project-routing-v1', **identity})
            else:
                super().do_GET()

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
        for period in ['7d', '30d', '90d', 'full']:
            code, _, body = curl(f'/report/{project}?period={period}')
            assert code == 200 and len(body) > 1000 and b'<html' in body.lower()
            assert f'/report/{project}?period={period}'.encode() in body
            code, _, payload = curl(f'/api/{project}/latest?period={period}')
            data = json.loads(payload)
            assert code == 200 and data['project'] == project
            assert data['period_switcher']['current_key'] == period
            results.append({'project': project, 'period': period, 'html_bytes': len(body), 'sha256': hashlib.sha256(body).hexdigest()})
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
        print('DASHBOARD_ROUTING_HOST_OK ' + json.dumps({'identity': identity, 'reports': results, 'foreign_redirects': True, 'authentication_required': True}), flush=True)
    finally:
        httpd.shutdown()
        httpd.server_close()
        worker.join(timeout=5)
        assert not worker.is_alive()
        print('DASHBOARD_ROUTING_LOCAL_SERVER_CLOSED', flush=True)


if __name__ == '__main__':
    main()
