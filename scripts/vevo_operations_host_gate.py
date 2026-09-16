#!/usr/bin/env python3
"""Finite candidate-host checks; no provider mutation, report run or email."""
import base64
import json
import os
from pathlib import Path
import resource
import secrets
import socket
import subprocess
import sys
import threading
from http.server import ThreadingHTTPServer
from urllib.request import urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import live_dashboard_server as dashboard


def main():
    assert os.environ["REPORT_PROJECT"] == "vevo"
    with urlopen(os.environ["ECS_CONTAINER_METADATA_URI_V4"] + "/task", timeout=5) as response:
        metadata = json.load(response)
    identity = {
        "instance_id": "N/A (ECS/Fargate)", "task_arn": metadata["TaskARN"],
        "private_ips": sorted({ip for c in metadata["Containers"] for n in c.get("Networks", []) for ip in n.get("IPv4Addresses", [])}),
        "service": "biznisweb-vevo-production-board", "path": str(Path.cwd()),
        "pid": os.getpid(), "parent_pid": os.getppid(), "executable": sys.executable,
        "images": [{"image": c["Image"], "image_id": c.get("ImageID")} for c in metadata["Containers"]],
    }
    assert identity["path"] == "/app" and identity["private_ips"]
    os.environ["LIVE_DASHBOARD_AUTH_USER"] = "host-probe"
    os.environ["LIVE_DASHBOARD_AUTH_PASSWORD"] = secrets.token_urlsafe(32)
    token = base64.b64encode(("host-probe:" + os.environ["LIVE_DASHBOARD_AUTH_PASSWORD"]).encode()).decode()

    class Handler(dashboard.LiveDashboardHandler):
        def do_GET(self):
            if self.path == "/__vevo_operations_marker":
                self._send_json({"marker": "VEVO_OPERATIONS_HOST_IDENTITY", **identity})
            else:
                super().do_GET()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_port
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()

    def curl(path, *, authenticate=True, post=False):
        config = f'url = "http://127.0.0.1:{port}{path}"\n'
        if authenticate:
            config += f'header = "Authorization: Basic {token}"\n'
        if post:
            config += 'request = "POST"\nheader = "Content-Type: application/json"\nheader = "Sec-Fetch-Site: cross-site"\n'
        output = subprocess.run(["curl", "--silent", "--show-error", "--max-time", "240", "--include", "--config", "-"],
                                input=config.encode(), capture_output=True, check=True).stdout
        headers, body = output.split(b"\r\n\r\n", 1)
        return int(headers.splitlines()[0].split()[1]), body

    try:
        code, body = curl("/__vevo_operations_marker")
        assert code == 200 and json.loads(body)["task_arn"] == identity["task_arn"]
        print("VEVO_BOARD_HOST_IDENTITY " + json.dumps({**identity, "port": port}), flush=True)
        code, html = curl("/production/vevo")
        assert code == 200 and b"VEVO operations dashboard" in html and b"vevo-operations-dashboard" in html
        assert b"/api/operations/roy/" not in html
        assert curl("/production/vevo", authenticate=False)[0] == 401
        assert curl("/api/operations/roy/live")[0] == 409
        assert curl("/api/operations/vevo/inbound/host-probe-rejected", post=True)[0] == 403
        assert curl("/api/operations/roy/inbound/host-probe-rejected", post=True)[0] == 409
        code, body = curl("/api/operations/vevo/maintenance")
        maintenance = json.loads(body)
        assert code == 200 and maintenance["project"] == "vevo" and not maintenance.get("status_error") and not maintenance["active"]
        code, body = curl("/api/operations/vevo/live?refresh=1")
        data = json.loads(body)
        assert code == 200, data.get("error")
        assert data["marker"] == "vevo-operations-dashboard" and data["project"] == "vevo"
        assert data["orders"]["summary"]["fulfillable_orders"] > 0
        for order in data["orders"]["orders"]:
            assert order["status_id"] in {"1", "31", "70"}
            if order["status_id"] == "1":
                # Reviewed SK, CZ and HU COD methods; do not accept online payments.
                assert order["payment"]["reference_id"] in {"7", "10", "16"}
        summary = data["inventory"]["summary"]
        assert summary["inventory_status"] == "ok" and summary["inventory_products_total"] > 0
        assert summary["inventory_available_units"] > 0 and summary["inventory_cost_value"] > 0
        assert data["executive_kpis"]["windows"] and data["executive_kpis"]["months"]
        assert summary["live_stock_overlay"]["error_count"] == 0
        code, pdf = curl("/api/operations/vevo/picking-lists.pdf?preview=1&include_printed=1&refresh=0")
        assert code == 200 and pdf.startswith(b"%PDF-") and len(pdf) > 1000
        code, html = curl("/manufacturing/vevo")
        assert code == 200 and b"vevo-production-board" in html
        code, body = curl("/api/production/vevo/live?refresh=1")
        manufacturing = json.loads(body)
        assert code == 200 and manufacturing["eligibility_policy"] == "paid_or_cod"
        for order in manufacturing["orders"]:
            if order["eligibility_reason"] == "paid_online":
                assert order["status_id"] in {"31", "70"}
            elif order["eligibility_reason"] == "cod_waiting":
                assert order["status_id"] == "1" and order["payment_id"] in {"7", "10", "16"}
            else:
                raise AssertionError("Manufacturing contains an order not ready for fulfillment")
        assert manufacturing["summary"]["active_orders"] > 0
        print("VEVO_BOARD_HOST_OK " + json.dumps({"identity": identity, "mode": "operations",
              "manufacturing_summary": manufacturing["summary"], "manufacturing_paid_or_cod_verified": True,
              "summary": data["orders"]["summary"], "inventory": summary,
              "scan": data["orders"]["scan"], "statuses": sorted({o["status"] for o in data["orders"]["orders"]}),
              "pdf_bytes": len(pdf), "max_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}), flush=True)
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=5)
        assert not worker.is_alive()
        with socket.socket() as check:
            assert check.connect_ex(("127.0.0.1", port)) != 0
        print("VEVO_BOARD_HOST_CLOSED", flush=True)


if __name__ == "__main__":
    main()
