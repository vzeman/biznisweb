"""Finite Fargate check: real read-only dashboard and isolated batch HTTP races."""
import base64
import io
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import threading
import unittest
from http.server import ThreadingHTTPServer
from urllib.request import urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import live_dashboard_server as dashboard


def main():
    project = os.environ["REPORT_PROJECT"]
    assert project in {"roy", "vevo"}
    with urlopen(os.environ["ECS_CONTAINER_METADATA_URI_V4"] + "/task", timeout=5) as response:
        task = json.load(response)
    identity = {
        "task_arn": task["TaskARN"], "instance_id": "N/A (ECS/Fargate)",
        "private_ips": sorted({ip for c in task["Containers"] for n in c.get("Networks", []) for ip in n.get("IPv4Addresses", [])}),
        "service": os.environ["EXPECTED_APP_RUNNER_SERVICE"], "project": project,
        "path": str(Path.cwd()), "pid": os.getpid(), "parent_pid": os.getppid(),
        "executable": sys.executable, "command": sys.argv,
    }
    assert identity["path"] == "/app" and identity["private_ips"]
    print("PICKING_BATCH_HOST_IDENTITY " + json.dumps(identity), flush=True)
    # Run isolated writes before any live read can start background refreshes.
    # All storage and upstream calls in these tests use synthetic dictionaries.
    from tests.test_picking_print_batches import PickingBatchHttpTests, PickingBatchTests
    suite = unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromTestCase(cls)
                               for cls in (PickingBatchHttpTests, PickingBatchTests))
    output = io.StringIO()
    result = unittest.TextTestRunner(stream=output).run(suite)
    if not result.wasSuccessful():
        raise RuntimeError("Isolated batch host tests failed: " + output.getvalue())
    os.environ["LIVE_DASHBOARD_AUTH_USER"] = "host-check"
    os.environ["LIVE_DASHBOARD_AUTH_PASSWORD"] = secrets.token_urlsafe(32)
    token = base64.b64encode(("host-check:" + os.environ["LIVE_DASHBOARD_AUTH_PASSWORD"]).encode()).decode()
    proof = {"marker": "pdf-batch-v1", "identity": identity}

    class Handler(dashboard.LiveDashboardHandler):
        def do_GET(self):
            if self.path == "/__picking_batch_marker":
                self._send_json(proof)
            else:
                super().do_GET()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    worker = threading.Thread(target=server.serve_forever)
    worker.start()
    port = server.server_port

    def curl(path):
        config = f'url = "http://127.0.0.1:{port}{path}"\nheader = "Authorization: Basic {token}"\n'
        return subprocess.run(["curl", "--fail", "--silent", "--show-error", "--max-time", "240", "--config", "-"],
                              input=config.encode(), capture_output=True, check=True, timeout=250).stdout

    try:
        assert json.loads(curl("/health"))["ok"] is True
        html = curl(f"/production/{project}")
        assert b'data-picking-policy="pdf-batch-v1"' in html and b"batch_id: batch.batchId" in html
        live = json.loads(curl(f"/api/operations/{project}/live?refresh=0"))
        assert live["project"] == project and live["marker"] == f"{project}-operations-dashboard"
        pdf = curl(f"/api/operations/{project}/picking-lists.pdf?preview=1&refresh=0")
        assert pdf.startswith(b"%PDF-") and len(pdf) > 1000
        proof.update({"synthetic_batch_tests": result.testsRun, "batch_races_verified": True,
                      "live_order_count": len(live["orders"]["orders"]), "preview_pdf_bytes": len(pdf)})
        assert json.loads(curl("/__picking_batch_marker")) == proof
        print("PICKING_BATCH_HOST_OK " + json.dumps(proof), flush=True)
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=5)
        assert not worker.is_alive()
        with socket.socket() as check:
            assert check.connect_ex(("127.0.0.1", port)) != 0
        print("PICKING_BATCH_HOST_CLOSED", flush=True)


if __name__ == "__main__":
    main()
