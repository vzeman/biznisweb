"""Stdlib-only identity probe injected into the unchanged baseline image."""
import argparse
import hashlib
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
import subprocess
import threading
from urllib.request import urlopen

MARKER = "VEVO_REPORT_BASELINE_HOST_OK"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--runner-sha256", required=True)
    parser.add_argument("--image-digest", required=True)
    args = parser.parse_args()
    if os.getcwd() != "/app" or os.environ.get("REPORT_PROJECT") != "vevo":
        raise RuntimeError("baseline-host-path-or-project")
    if hashlib.sha256(Path("daily_report_runner.py").read_bytes()).hexdigest() != args.runner_sha256:
        raise RuntimeError("baseline-source-mismatch")
    uri = os.environ["ECS_CONTAINER_METADATA_URI_V4"]
    if not uri.startswith("http://169.254.170.2/v4/"):
        raise RuntimeError("baseline-metadata-endpoint")
    with urlopen(uri + "/task", timeout=10) as response:
        task = json.load(response)
    containers = [row for row in task.get("Containers", []) if row.get("Name") == "reporting"]
    if task.get("Family") != "vevo-reporting-daily" or str(task.get("Revision")) != "33" or len(containers) != 1:
        raise RuntimeError("baseline-host-definition")
    ips = [ip for row in containers[0].get("Networks", []) for ip in row.get("IPv4Addresses", [])]
    if len(ips) != 1 or containers[0].get("ImageID") != args.image_digest:
        raise RuntimeError("baseline-host-network-or-image")
    payload = {"marker": MARKER, "task_arn": task["TaskARN"], "private_ip": ips[0],
               "image_digest": args.image_digest, "path": "/app", "service": "vevo-daily-report-email",
               "instance_id": "N/A:Fargate", "runner_sha256": args.runner_sha256, "provider_reads": 0}
    raw = json.dumps(payload, sort_keys=True).encode()
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/marker.json":
                self.send_error(404)
                return
            self.send_response(200)
            self.end_headers()
            self.wfile.write(raw)
        def log_message(self, *_):
            pass
    server = HTTPServer(("127.0.0.1", 0), Handler)
    worker = threading.Thread(target=server.serve_forever)
    worker.start()
    try:
        result = subprocess.run(["curl", "--fail", "--silent", "--show-error", "--max-time", "10",
                                 f"http://127.0.0.1:{server.server_port}/marker.json"], capture_output=True, check=True, timeout=15)
        if result.stdout != raw:
            raise RuntimeError("baseline-localhost-mismatch")
    finally:
        server.shutdown()
        worker.join(timeout=10)
        server.server_close()
    if worker.is_alive():
        raise RuntimeError("baseline-localhost-still-running")
    print(MARKER + " " + raw.decode(), flush=True)


if __name__ == "__main__":
    main()
