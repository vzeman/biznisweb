import copy
from datetime import datetime, timedelta, timezone
import hashlib
import http.client
import json
from pathlib import Path
import shutil
import socket
import subprocess
import threading
import unittest
from http.server import ThreadingHTTPServer
from unittest.mock import patch

from live_dashboard_server import LiveDashboardHandler, build_roy_operations_dashboard_html
from picking_print_batches import acknowledge_picking_pdf_batch, register_picking_pdf_batch
from roy_operations_dashboard import _normalize_operations_state
from scripts.deploy_picking_batch_dashboard import SERVICES, validate_proof


class PickingBatchTests(unittest.TestCase):
    def test_deployment_proof_is_bound_to_exact_task_project_ip_and_image(self):
        for project in SERVICES:
            receipt = {"project": project, "task_arn": "task-1", "definition": "definition-1", "started_by": "probe-1", "digest": "sha256:exact"}
            task = {"taskArn": "task-1", "taskDefinitionArn": "definition-1", "startedBy": "probe-1", "lastStatus": "STOPPED",
                    "containers": [{"exitCode": 0, "imageDigest": "sha256:exact"}],
                    "attachments": [{"details": [{"name": "privateIPv4Address", "value": "172.31.1.2"}]}]}
            proof = {"identity": {"task_arn": "task-1", "service": SERVICES[project][0], "project": project,
                                  "path": "/app", "private_ips": ["172.31.1.2"]},
                     "marker": "pdf-batch-v1", "batch_races_verified": True, "synthetic_batch_tests": 5, "preview_pdf_bytes": 1500}
            validate_proof(task, proof, receipt)
            for field, value in (("task_arn", "another-task"), ("path", "/wrong"),
                                 ("private_ips", ["172.31.1.3"]), ("project", "foreign")):
                changed = copy.deepcopy(proof)
                changed["identity"][field] = value
                with self.assertRaises(AssertionError):
                    validate_proof(task, changed, receipt)
            for field, value in (("exitCode", 1), ("imageDigest", "sha256:other")):
                changed = copy.deepcopy(task)
                changed["containers"][0][field] = value
                with self.assertRaises(AssertionError):
                    validate_proof(changed, proof, receipt)

    def test_snapshot_survives_order_changes_and_repeat_ack_is_idempotent(self):
        state = {}
        orders = [{"order_num": "A", "status": "paid"}, {"order_num": "B"}]
        batch = register_picking_pdf_batch(state, "roy", orders, b"%PDF-A-B", "orders.pdf")
        orders[0]["order_num"] = "NEW"
        orders.append({"order_num": "C"})
        state = _normalize_operations_state(state)
        first = acknowledge_picking_pdf_batch(state, "roy", batch["batch_id"])
        second = acknowledge_picking_pdf_batch(state, "roy", batch["batch_id"])
        self.assertEqual(first, second)
        self.assertEqual({"A", "B"}, set(state["printed_picking_orders"]))
        self.assertEqual(hashlib.sha256(b"%PDF-A-B").hexdigest(), first["pdf_sha256"])
        self.assertEqual(1, len(state["picking_print_batches"]))

    def test_two_windows_keep_independent_batches_and_preserve_first_print_record(self):
        state = {}
        first = register_picking_pdf_batch(state, "vevo", [{"order_num": "A"}], b"%PDF-A", "a.pdf")
        second = register_picking_pdf_batch(state, "vevo", [{"order_num": "A"}, {"order_num": "B"}], b"%PDF-A-B", "b.pdf")
        acknowledge_picking_pdf_batch(state, "vevo", first["batch_id"])
        record = copy.deepcopy(state["printed_picking_orders"]["A"])
        result = acknowledge_picking_pdf_batch(state, "vevo", second["batch_id"])
        self.assertEqual(["B"], result["order_nums"])
        self.assertEqual(record, state["printed_picking_orders"]["A"])
        self.assertEqual(2, result["download_order_count"])

    def test_missing_wrong_shop_and_expired_batches_cannot_mark_orders(self):
        now = datetime(2026, 10, 1, tzinfo=timezone.utc)
        state = {}
        batch = register_picking_pdf_batch(state, "roy", [{"order_num": "A"}], b"%PDF-A", "a.pdf", now=now)
        for project, batch_id, when in (("roy", None, now), ("roy", "f" * 32, now),
                                        ("vevo", batch["batch_id"], now),
                                        ("roy", batch["batch_id"], now + timedelta(days=8))):
            with self.subTest(project=project, when=when), self.assertRaises(ValueError):
                acknowledge_picking_pdf_batch(state, project, batch_id, now=when)
        self.assertNotIn("printed_picking_orders", state)

    def test_empty_or_invalid_pdf_does_not_register_batch(self):
        for orders, pdf in (([], b"%PDF-"), ([{"order_num": "A"}], b"error")):
            state = {}
            with self.assertRaises(ValueError):
                register_picking_pdf_batch(state, "roy", orders, pdf, "a.pdf")
            self.assertEqual({}, state)


class PickingBatchHttpTests(unittest.TestCase):
    def test_download_ack_race_replay_and_failure_boundaries_for_both_shops(self):
        for project in ("roy", "vevo"):
            with self.subTest(project=project):
                self.exercise_project(project)

    def exercise_project(self, project):
        state = {}
        payload = {"orders": {"orders": [{"order_num": "A"}, {"order_num": "B"}]}}

        def save(_project, value, _settings):
            nonlocal state
            state = _normalize_operations_state(copy.deepcopy(value))
            return {"storage": "test"}

        class Handler(LiveDashboardHandler):
            # Consume even rejected requests so Windows does not reset a socket
            # with unread request bytes before the client receives its 403.
            def do_POST(self):
                self._read_json_body()
                super().do_POST()

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        worker = threading.Thread(target=server.serve_forever)
        worker.start()
        host, port = server.server_address

        def request(action, body, *, trusted=True):
            connection = http.client.HTTPConnection(host, port, timeout=5)
            try:
                connection.request("POST", f"/api/operations/{project}/picking-lists/{action}",
                                   body=json.dumps(body), headers={"Content-Type": "application/json",
                                   "X-Operations-Action": "dashboard-action",
                                   "Sec-Fetch-Site": "same-origin" if trusted else "cross-site"})
                response = connection.getresponse()
                return response.status, dict(response.getheaders()), response.read()
            finally:
                connection.close()

        try:
            with (
                patch.dict("os.environ", {"REPORT_PROJECT": project}),
                patch("live_dashboard_server.available_projects", return_value=[project]),
                patch("live_dashboard_server.live_dashboard_auth_credentials", return_value=None),
                patch("live_dashboard_server.get_live_dashboard_maintenance_status", return_value={"active": False}),
                patch("live_dashboard_server.load_project_settings", return_value={}),
                patch("live_dashboard_server.resolve_roy_operations_settings", return_value={"enabled": True}),
                patch("live_dashboard_server.read_latest_dashboard_payload", return_value={}),
                patch("live_dashboard_server.get_cached_roy_operations_snapshot", side_effect=lambda *a, **k: payload) as snapshot,
                patch("live_dashboard_server.load_roy_operations_state", side_effect=lambda *a, **k: copy.deepcopy(state)),
                patch("live_dashboard_server.save_roy_operations_state", side_effect=save) as store,
                patch("live_dashboard_server.build_roy_picking_lists_pdf", return_value=b"%PDF-A-B") as render,
            ):
                self.assertEqual(403, request("download", {"order_nums": ["A"]}, trusted=False)[0])
                self.assertEqual(400, request("printed", {"order_nums": ["A"]})[0])
                store.assert_not_called()
                status, headers, pdf = request("download", {"order_nums": ["A", "B"]})
                self.assertEqual(200, status)
                self.assertEqual(b"%PDF-A-B", pdf)
                batch_id = headers["X-Picking-Batch-Id"]
                self.assertEqual("2", headers["X-Picking-Order-Count"])
                self.assertFalse(state["printed_picking_orders"])
                self.assertEqual(hashlib.sha256(pdf).hexdigest(), state["picking_pdf_batches"][batch_id]["pdf_sha256"])
                payload["orders"]["orders"] = [{"order_num": "B"}, {"order_num": "C"}]
                snapshot.reset_mock()
                status, _, result = request("printed", {"batch_id": batch_id})
                self.assertEqual(200, status)
                self.assertEqual(["A", "B"], json.loads(result)["batch"]["order_nums"])
                snapshot.assert_not_called()
                self.assertEqual({"A", "B"}, set(state["printed_picking_orders"]))
                self.assertEqual(result, request("printed", {"batch_id": batch_id})[2])
                self.assertEqual(400, request("printed", {"batch_id": batch_id, "order_nums": ["C"]})[0])
                render.side_effect = RuntimeError("render failed")
                self.assertEqual(400, request("download", {"order_nums": ["C"]})[0])
                self.assertEqual(1, len(state["picking_pdf_batches"]))
                render.side_effect = None
                store.side_effect = RuntimeError("state changed concurrently")
                status, headers, _ = request("download", {"order_nums": ["C"]})
                self.assertEqual(400, status)
                self.assertNotIn("X-Picking-Batch-Id", headers)
                self.assertEqual(1, len(state["picking_pdf_batches"]))
                self.assertEqual({"A", "B"}, set(state["printed_picking_orders"]))
        finally:
            server.shutdown()
            server.server_close()
            worker.join(timeout=5)
        self.assertFalse(worker.is_alive())
        with socket.socket() as check:
            self.assertNotEqual(0, check.connect_ex((host, port)))


class PickingBatchBrowserTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which("node"), "Node.js required")
    def test_browser_keeps_last_successful_download_across_refreshes_and_tabs(self):
        html = build_roy_operations_dashboard_html("roy")
        script = html.split("  <script>\n", 1)[1].split("  </script>", 1)[0]
        helpers = script[script.index("    const project ="):script.index("    let refreshTimer")]
        controls = script[script.index("    function currentUnprintedPickingOrderNums"):script.index("    function pickingPrintBadge")]
        actions = script[script.index("    function rememberPickingBatch"):script.index("    async function acknowledgeLossProduct")]
        source = (Path(__file__).parent / "picking_print_batches.cjs").read_text(encoding="utf-8")
        result = subprocess.run([shutil.which("node"), "-"],
                                input=source.replace("__DASHBOARD_CODE__", json.dumps(helpers + controls + actions)),
                                encoding="utf-8", text=True, capture_output=True, timeout=20)
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("PICKING_BATCH_BROWSER_OK", result.stdout)
