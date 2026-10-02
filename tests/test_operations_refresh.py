import copy
from contextlib import ExitStack
import json
from pathlib import Path
import shutil
import subprocess
import threading
import time
import unittest
from unittest.mock import patch

import roy_operations_dashboard as rod
from live_dashboard_server import build_roy_operations_dashboard_html
from tests.test_roy_operations_dashboard import make_project_settings


class IndependentRefreshTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.threads = []
        self.release = threading.Event()
        real_thread = threading.Thread

        def tracked_thread(*args, **kwargs):
            thread = real_thread(*args, **kwargs)
            self.threads.append(thread)
            return thread

        self.stack.enter_context(patch("roy_operations_dashboard.threading.Thread", side_effect=tracked_thread))
        for name in ("_CACHE", "_CACHE_TOKENS", "_BACKGROUND_REFRESH", "_INVENTORY_CACHE", "_INVENTORY_REFRESH"):
            self.stack.enter_context(patch.dict(getattr(rod, name), {}, clear=True))
        self.settings = make_project_settings()
        self.settings["operations_dashboard"].update(cache_ttl_seconds=1, auto_refresh_seconds=30)
        self.state = rod._empty_operations_state()
        self.stack.enter_context(patch("roy_operations_dashboard.load_project_env"))
        self.stack.enter_context(patch("roy_operations_dashboard.load_project_settings", return_value=self.settings))
        self.stack.enter_context(patch("roy_operations_dashboard.load_roy_operations_state", side_effect=lambda *a, **k: copy.deepcopy(self.state)))
        self.shared = self.stack.enter_context(patch("roy_operations_dashboard._save_shared_operations_snapshot"))
        self.stack.enter_context(patch("roy_operations_dashboard._load_shared_operations_snapshot", return_value=None))
        self.stack.enter_context(patch("roy_operations_dashboard._delete_shared_operations_snapshot"))
        self.fetch = self.stack.enter_context(patch("roy_operations_dashboard.fetch_open_orders_for_roy_operations", return_value=([], {})))
        self.inventory = self.stack.enter_context(patch("roy_operations_dashboard._generate_inventory_component", side_effect=lambda *a: self.component()))

    def component(self):
        return {"inventory": {"summary": {"available": 12}}, "executive_kpis": {}, "performance": {},
                "inventory_generated_at": rod._state_now_iso(), "inventory_duration_seconds": 1,
                "operations_state_revision": rod._operations_display_revision(self.state)}

    def join_workers(self):
        for thread in self.threads:
            thread.join(timeout=3)
            self.assertFalse(thread.is_alive(), thread.name)

    def tearDown(self):
        self.release.set()
        try:
            self.join_workers()
        finally:
            self.stack.close()

    def test_orders_publish_and_refresh_while_inventory_is_blocked(self):
        entered = threading.Event()

        def slow_inventory(*args):
            entered.set()
            self.release.wait(timeout=3)
            return self.component()

        self.inventory.side_effect = slow_inventory
        first = rod.get_cached_roy_operations_snapshot("roy")
        self.assertTrue(entered.wait(timeout=1))
        self.assertEqual(1, self.fetch.call_count)
        self.assertTrue(first["inventory_refresh"]["in_progress"])
        self.assertEqual({}, first["inventory"])
        rod._start_background_operations_refresh("roy", None)
        # Only join the order worker; inventory must still be waiting independently.
        order_worker = next(t for t in self.threads if "operations-refresh" in t.name)
        order_worker.join(timeout=2)
        self.assertFalse(order_worker.is_alive())
        self.assertEqual(2, self.fetch.call_count)
        self.assertFalse(self.release.is_set())
        self.assertEqual(1, self.inventory.call_count)
        self.assertEqual("independent-orders-v1", rod._CACHE["roy"][1]["refresh_policy"])

    def test_inventory_completion_preserves_newer_orders_and_their_cache_age(self):
        result = rod.get_cached_roy_operations_snapshot("roy")
        self.join_workers()
        result["orders"]["orders"] = [{"order_num": "synthetic-new-order"}]
        stamp = time.monotonic()
        rod._CACHE["roy"] = (stamp, result)
        current = rod.get_cached_roy_operations_snapshot("roy")
        self.assertEqual([{"order_num": "synthetic-new-order"}], current["orders"]["orders"])
        self.assertEqual(12, current["inventory"]["summary"]["available"])
        self.assertEqual(stamp, rod._CACHE["roy"][0])
        self.assertEqual(1, self.fetch.call_count)
        self.assertEqual(1, self.inventory.call_count)

    def test_failed_inventory_does_not_block_orders_or_retry_on_every_poll(self):
        self.inventory.side_effect = RuntimeError("synthetic inventory outage")
        rod.get_cached_roy_operations_snapshot("roy")
        self.join_workers()
        for _ in range(3):
            result = rod.get_cached_roy_operations_snapshot("roy")
            self.assertIn("outage", result["inventory_refresh"]["last_error"])
            self.assertFalse(result["inventory_refresh"]["in_progress"])
        self.assertEqual(1, self.inventory.call_count)
        self.assertEqual(1, self.fetch.call_count)

    def test_old_order_worker_cannot_overwrite_action_readback(self):
        entered = threading.Event()

        def slow_orders(*args):
            entered.set()
            self.release.wait(timeout=3)
            return [], {}

        self.fetch.side_effect = slow_orders
        with patch("roy_operations_dashboard._start_inventory_refresh"):
            rod._start_background_operations_refresh("roy", None)
            self.assertTrue(entered.wait(timeout=1))
            rod._clear_operations_cache("roy")
            rod._cache_payload("roy", {"generated_at": "newer-action-readback"})
            self.release.set()
            self.join_workers()
        self.assertEqual("newer-action-readback", rod._CACHE["roy"][1]["generated_at"])
        self.shared.assert_not_called()

    def test_inventory_result_after_local_or_remote_state_change_is_discarded(self):
        for local in (True, False):
            with self.subTest(local=local):
                self.release.clear()
                entered = threading.Event()
                original = self.component()

                def slow_inventory(*args):
                    entered.set()
                    self.release.wait(timeout=3)
                    return original

                rod._INVENTORY_REFRESH.clear()
                self.inventory.side_effect = slow_inventory
                rod._start_inventory_refresh("roy", self.settings, None)
                self.assertTrue(entered.wait(timeout=1))
                if local:
                    rod._clear_operations_cache("roy")
                else:
                    self.state["printed_picking_orders"]["synthetic"] = {"printed_at": "new"}
                self.release.set()
                self.join_workers()
                self.assertNotIn("roy", rod._INVENTORY_CACHE)

    def test_manual_refresh_starts_orders_inside_ttl_and_fresh_poll_tracks_running_job(self):
        rod.get_cached_roy_operations_snapshot("roy")
        self.join_workers()
        with patch("roy_operations_dashboard._start_background_operations_refresh") as start:
            rod.get_cached_roy_operations_snapshot("roy", request_refresh=True)
        start.assert_called_once_with("roy", None)
        rod._BACKGROUND_REFRESH["roy"] = {"running": True}
        result = rod.get_cached_roy_operations_snapshot("roy")
        self.assertTrue(result["cache"]["refresh_in_progress"])

    def test_order_failure_preserves_old_data_and_backs_off(self):
        rod.get_cached_roy_operations_snapshot("roy")
        self.join_workers()
        original = copy.deepcopy(rod._CACHE["roy"][1])
        rod._CACHE["roy"] = (time.monotonic() - 10, original)
        self.fetch.side_effect = RuntimeError("synthetic order outage")
        rod._start_background_operations_refresh("roy", None)
        self.join_workers()
        count = self.fetch.call_count
        for _ in range(3):
            result = rod.get_cached_roy_operations_snapshot("roy")
            self.assertEqual(original["generated_at"], result["generated_at"])
            self.assertIn("outage", result["cache"]["last_refresh_error"])
            self.assertFalse(result["cache"]["refresh_in_progress"])
        self.assertEqual(count, self.fetch.call_count)


class RefreshBrowserTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which("node"), "Node.js required")
    def test_completion_polling_single_flight_failure_and_action_queue(self):
        html = build_roy_operations_dashboard_html("roy")
        script = html.split("  <script>\n", 1)[1].split("  </script>", 1)[0]
        variables = script[script.index("    let refreshTimer"):script.index("    let kpiScope")]
        functions = script[script.index("    function scheduleDashboardRefresh"):script.index("    async function markPickupShipped")]
        source = (Path(__file__).parent / "operations_refresh.cjs").read_text(encoding="utf-8")
        result = subprocess.run([shutil.which("node"), "-"], input=source.replace("__REFRESH_CODE__", json.dumps(variables + functions)),
                                encoding="utf-8", text=True, capture_output=True, timeout=20)
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("OPERATIONS_REFRESH_BROWSER_OK", result.stdout)
