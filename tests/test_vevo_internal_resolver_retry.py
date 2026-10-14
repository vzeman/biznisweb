"""Structured resolver retries preserve complete VEVO source membership.

Uses synthetic orders and the seven public field paths/four null positions of
preserved read0209. No customer values or actual order IDs are embedded.
"""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import export_orders as reporting
from gql import Client
from gql.transport.exceptions import TransportQueryError, TransportServerError
from gql.transport.requests import RequestsHTTPTransport
from scripts import reporting_migration_host_gate as host

ROOT = Path(__file__).resolve().parents[1]


def fixture_module(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT/path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fixture = fixture_module("resolver_inventory_fixtures", "tests/test_reporting_order_inventory.py")
host_fixture = fixture_module("resolver_host_fixtures", "tests/test_reporting_migration_host_gate.py")


def partial_error():
    rows = [fixture.order(identity) for identity in range(1, 31)]
    # A conspicuously wrong partial amount proves the failed body is discarded.
    rows[0]["sum"]["raw_value"] = 99999
    for index in range(26, 30):
        rows[index] = None
    paths = [(26, "invoice_address")]
    paths += [(index, field) for index in (27, 28, 29)
              for field in ("status", "invoice_address")]
    errors = [{"message": "Internal server error",
               "extensions": {"category": "internal"},
               "path": ["getOrderList", "data", index, field]}
              for index, field in paths]
    return TransportQueryError("Synthetic resolver failure", errors=errors, data={
        "getOrderList": {"data": rows, "pageInfo": {
            "hasNextPage": True, "nextCursor": 30, "totalRecords": 90}}})


class InternalResolverRetryTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.clock = fixture.Clock()
        for target, value in (("export_orders.time.monotonic", self.clock.monotonic),
                              ("export_orders.time.sleep", self.clock.sleep)):
            mocked = patch(target, value)
            mocked.start()
            self.addCleanup(mocked.stop)
        self.exp = reporting.BizniWebExporter(
            "https://example.invalid/graphql", "synthetic", project_name="vevo",
            order_facts_only=True)
        self.exp.cache_dir = Path(self.folder.name)
        self.exp.prepare_reporting_status_identity = Mock()
        self.exp.cache_days_threshold = 0
        self.api = fixture.MutableAPI([fixture.order(identity) for identity in range(1, 91)])
        self.exp.client = self.api

    def fetch(self):
        return self.exp.fetch_orders(fixture.DAY, fixture.DAY)

    def assert_no_cache(self):
        self.assertEqual([], list(self.exp.cache_dir.iterdir()))

    def test_actual_shaped_partial_is_discarded_and_identical_full_query_recovers(self):
        failure = partial_error()
        before = copy.deepcopy(failure.data)

        def fail_once(api, variables):
            if len(api.calls) == 2:  # The first ascending page, after the anchor.
                raise failure

        self.api.before = fail_once
        rows = self.fetch()
        self.assertEqual(list(range(1, 91)), [int(row["id"]) for row in rows])
        self.assertEqual(self.api.calls[1], self.api.calls[2])
        self.assertNotIn("GetOrdersWithoutPriceElements", self.api.calls[2][0])
        self.assertTrue(all(row["sum"]["raw_value"] == 12.3 for row in rows))
        self.assertTrue(all(row["items"][0]["sum"]["raw_value"] == 10 for row in rows))
        self.assertEqual(before, failure.data)
        self.assertEqual(rows, self.exp.load_from_cache(fixture.DAY))
        cache = json.loads(self.exp.get_cache_filename(fixture.DAY).read_text(encoding="utf-8"))
        self.assertEqual(90, cache["complete_inventory"]["inventory_count"])
        self.assertEqual(90, cache["complete_inventory"]["anchor_max_id"])
        self.assertGreaterEqual(sum(self.clock.waits), 60)
        self.assertIsNone(self.exp._inventory_read_deadline)

    def test_persistent_internal_partial_has_three_attempts_and_no_cache(self):
        failure = partial_error()
        self.api.before = lambda *_: (_ for _ in ()).throw(failure)
        with self.assertRaises(TransportQueryError):
            self.fetch()
        self.assertEqual(3, len(self.api.calls))
        self.assertEqual(self.api.calls[0], self.api.calls[1])
        self.assertEqual(self.api.calls[1], self.api.calls[2])
        self.assert_no_cache()
        self.assertIsNone(self.exp._inventory_read_deadline)

    def test_internal_cooldown_cannot_exceed_remaining_scan_deadline(self):
        failure = partial_error()
        self.api.before = lambda *_: (_ for _ in ()).throw(failure)
        with patch("export_orders.REPORTING_INVENTORY_TIMEOUT_SEC", 59):
            with self.assertRaisesRegex(RuntimeError, "time limit"):
                self.fetch()
        self.assertEqual(1, len(self.api.calls))
        self.assertEqual([], self.clock.waits)
        self.assert_no_cache()
        self.assertIsNone(self.exp._inventory_read_deadline)

    def test_internal_retry_after_cannot_exceed_remaining_scan_deadline(self):
        failure = partial_error()

        def fail(api, _variables):
            api.transport.response_headers = {"Retry-After": "120"}
            raise failure

        self.api.before = fail
        with patch("export_orders.REPORTING_INVENTORY_TIMEOUT_SEC", 60):
            with self.assertRaisesRegex(RuntimeError, "time limit"):
                self.fetch()
        self.assertEqual(1, len(self.api.calls))
        self.assertEqual([], self.clock.waits)
        self.assert_no_cache()

    def test_permanent_codes_509_mixed_and_untyped_partials_remain_fatal(self):
        variants = []
        for code in (509, "FORBIDDEN", "GRAPHQL_VALIDATION_FAILED", "BAD_USER_INPUT"):
            failure = partial_error()
            failure.errors[0]["extensions"]["code"] = code
            variants.append((str(code), failure))
        mixed = partial_error()
        mixed.errors[-1].pop("extensions")
        variants.append(("mixed-unclassified", mixed))
        untyped = TransportQueryError("Internal server error", data=partial_error().data)
        variants.append(("untyped-text-only", untyped))
        for label, failure in variants:
            with self.subTest(label=label):
                self.api.calls.clear()
                self.api.pages.clear()
                self.exp._api_request_count = 0
                self.exp._api_last_request_completed_at = None
                self.api.before = lambda *_: (_ for _ in ()).throw(failure)
                with self.assertRaises(TransportQueryError):
                    self.fetch()
                self.assertEqual(1, len(self.api.calls))
                self.assert_no_cache()

    def test_roy_and_ordinary_vevo_reads_do_not_opt_into_internal_partial(self):
        failure = partial_error()
        self.api.before = lambda *_: (_ for _ in ()).throw(failure)
        with self.assertRaises(TransportQueryError):
            self.exp._execute_graphql(reporting.ORDER_QUERY, variable_values={"params": {
                "limit": 30, "cursor": 0, "sort": "ASC", "order_by": "order_id"}})
        self.assertEqual(1, len(self.api.calls))
        self.api.calls.clear()
        self.api.pages.clear()
        self.exp.project_name = "roy"
        self.exp.complete_order_inventory_enabled = False
        with self.assertRaises(TransportQueryError):
            with self.exp._complete_inventory_read_scope(self.clock.value + 300):
                self.exp._execute_graphql(reporting.ORDER_QUERY, variable_values={"params": {
                    "limit": 30, "cursor": 0, "sort": "ASC", "order_by": "order_id"}})
        self.assertEqual(1, len(self.api.calls))
        self.assert_no_cache()

    def test_price_elements_still_uses_existing_reduced_query_and_detail_policy(self):
        self.api.rows = {identity: fixture.order(identity) for identity in range(1, 4)}
        self.api.full_error = TransportQueryError("Synthetic price_elements resolver failure")
        rows = self.fetch()
        self.assertEqual(3, len(rows))
        self.assertTrue(any("GetOrdersWithoutPriceElements" in text for text, _ in self.api.calls))
        self.assertTrue(any("GetOrderPaymentMetadata" in text for text, _ in self.api.calls))
        self.assertLess(sum(self.clock.waits), 60)
        self.assertTrue(all(isinstance(row["price_elements"], list) for row in rows))

    def test_missing_legacy_scope_attributes_keep_partial_rejection_and_429_retry(self):
        variables = {"params": {"limit": 30, "cursor": 0, "sort": "ASC", "order_by": "order_id"}}
        for attribute in ("project_name", "complete_order_inventory_enabled"):
            with self.subTest(attribute=attribute):
                previous = getattr(self.exp, attribute)
                delattr(self.exp, attribute)
                try:
                    self.api.calls.clear()
                    self.api.pages.clear()
                    self.exp._api_request_count = 0
                    self.exp._api_last_request_completed_at = None
                    failure = partial_error()
                    self.api.before = lambda *_: (_ for _ in ()).throw(failure)
                    with self.assertRaises(TransportQueryError):
                        with self.exp._complete_inventory_read_scope(self.clock.value + 300):
                            self.exp._execute_graphql(reporting.ORDER_QUERY, variable_values=variables)
                    self.assertEqual(1, len(self.api.calls))
                    self.api.calls.clear()
                    self.api.pages.clear()
                    self.exp._api_request_count = 0
                    self.exp._api_last_request_completed_at = None

                    def throttle_once(api, _variables):
                        if len(api.calls) == 1:
                            raise TransportServerError("Synthetic rate limit", code=429)

                    self.api.before = throttle_once
                    with self.exp._complete_inventory_read_scope(self.clock.value + 300):
                        value = self.exp._execute_graphql(reporting.ORDER_QUERY, variable_values=variables)
                    self.assertEqual(30, len(value["getOrderList"]["data"]))
                    self.assertEqual(2, len(self.api.calls))
                    self.assertEqual(self.api.calls[0], self.api.calls[1])
                    self.assert_no_cache()
                finally:
                    setattr(self.exp, attribute, previous)

    def test_internal_recovery_and_failure_restore_owned_transport_state(self):
        self.exp.cache_days_threshold = 99999
        for recovery in (True, False):
            with self.subTest(recovery=recovery):
                transport = RequestsHTTPTransport(url="https://example.invalid/graphql", retries=4, timeout=30)
                transport.connect()  # Local requests.Session only; no network.
                original = [(adapter, adapter.max_retries) for adapter in transport.session.adapters.values()]
                self.api.transport = transport
                self.api.calls.clear()
                self.api.pages.clear()
                self.exp._api_request_count = 0
                self.exp._api_last_request_completed_at = None
                failure = partial_error()

                def fail(api, _variables):
                    self.assertEqual(0, transport.retries)
                    self.assertTrue(all(adapter.max_retries.total == 0 for adapter in transport.session.adapters.values()))
                    self.assertLessEqual(transport.default_timeout, reporting.GRAPHQL_TIMEOUT_SEC)
                    if not recovery or len(api.calls) == 1:
                        raise failure

                self.api.before = fail
                try:
                    if recovery:
                        self.assertEqual(90, len(self.fetch()))
                    else:
                        with self.assertRaises(TransportQueryError):
                            self.fetch()
                    self.assertEqual(4, transport.retries)
                    self.assertEqual(30, transport.default_timeout)
                    self.assertIsNone(self.exp._inventory_read_deadline)
                    for adapter, retries in original:
                        self.assertIs(retries, adapter.max_retries)
                finally:
                    transport.close()

    def test_host_pending_reads_clear_only_after_successful_same_full_page(self):
        import daily_report_runner as runner

        env = {"REPORT_PROJECT": "vevo", "REPORT_SKIP_INVOICES": "true",
               "REPORT_SKIP_CREDITNOTE_STORNO_GUARD": "true", "REPORT_SKIP_EMAIL": "true",
               "REPORT_S3_BUCKET": host.BUCKET, "REPORT_S3_PREFIX": "daily-reports/vevo"}
        for scenario in ("same-page-recovery", "persistent-swallowed", "different-page-after-failure"):
            with self.subTest(scenario=scenario):
                self.api.calls.clear()
                self.api.pages.clear()
                self.exp._api_request_count = 0
                self.exp._api_last_request_completed_at = None
                self.exp.cache_days_threshold = 99999  # Exercise source reads every scenario.
                client = Client(transport=RequestsHTTPTransport(url="https://example.invalid/graphql", retries=0))
                self.exp.client = client
                failure = partial_error()

                def fail(api, variables):
                    if variables["params"]["sort"] == "ASC" and variables["params"]["cursor"] == 0:
                        if scenario != "same-page-recovery" or len(api.calls) == 2:
                            raise failure

                self.api.before = fail

                def execute(_client, query, *args, **kwargs):
                    variables = kwargs.get("variable_values", args[0] if args else None)
                    return self.api.execute(query, variable_values=variables)

                def run():
                    try:
                        self.fetch()
                    except (TransportQueryError, RuntimeError):
                        if scenario == "same-page-recovery":
                            raise
                    if scenario == "different-page-after-failure":
                        Client.execute(client, reporting.ORDER_QUERY, variable_values={"params": {
                            "limit": 30, "cursor": 29, "sort": "ASC", "order_by": "order_id"}})
                    runner.s3_upload_outputs("vevo", {})

                s3 = host_fixture.S3()
                with patch.dict(host.os.environ, env), patch.object(Client, "execute", execute), \
                     patch.object(runner, "main", run), \
                     patch.object(host, "isolate_outputs", return_value={"key": "private", "sha256": "hash"}) as published:
                    if scenario == "same-page-recovery":
                        self.assertEqual("private", host.report_probe(s3, host.PREFIX + "a" * 32 + "/", "a" * 32)["key"])
                        published.assert_called_once()
                        self.assertEqual(self.api.calls[1], self.api.calls[2])
                    else:
                        with self.assertRaisesRegex(RuntimeError, "incomplete"):
                            host.report_probe(s3, host.PREFIX + "a" * 32 + "/", "a" * 32)
                        published.assert_not_called()
                self.assertIsNone(self.exp._inventory_read_deadline)


if __name__ == "__main__":
    unittest.main()
