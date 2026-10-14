"""Synthetic integration of complete source membership with reporting caches."""
import copy
import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from graphql import print_ast
from gql.transport.exceptions import TransportQueryError, TransportServerError
from gql.transport.requests import RequestsHTTPTransport

import export_orders as reporting
from export_orders import BizniWebExporter, ORDER_CACHE_SCHEMA_VERSION, ORDER_QUERY, ORDER_QUERY_WITHOUT_PRICE_ELEMENTS


DAY = datetime(2026, 9, 1)


def order(identity, day=DAY):
    def money(value, net=True):
        return {"value": value, "raw_value": value, "is_net_price": net, "currency": {"code": "EUR"}}
    return {
        "id": str(identity), "order_num": f"synthetic-{identity}",
        "pur_date": day.strftime("%Y-%m-%d 12:00:00"),
        "status": {"id": "4", "name": "Odoslaná"},
        "price_elements": [{"type": "payment", "reference_id": "7", "title": "Dobierka", "price": money(0)}],
        "sum": money(12.3, False),
        "items": [{"item_label": "Synthetic inventory product", "import_code": "SYN-INVENTORY",
                   "quantity": 1, "tax_rate": 23, "price": money(10), "sum": money(10),
                   "sum_with_tax": money(12.3, False)}],
    }


class Clock:
    def __init__(self):
        self.value = 1000.0
        self.waits = []

    def monotonic(self):
        return self.value

    def sleep(self, delay):
        self.waits.append(delay)
        self.value += delay


class MutableAPI:
    def __init__(self, rows):
        self.rows = {int(row["id"]): copy.deepcopy(row) for row in rows}
        self.calls = []
        self.pages = []
        self.before = lambda *_: None
        self.after = lambda *_: None
        self.transport = SimpleNamespace()
        self.full_error = None
        self.detail_error = None

    def execute(self, query, variable_values=None):
        text = print_ast(getattr(query, "document", query))
        self.calls.append((text, copy.deepcopy(variable_values)))
        if "mutation" in text:
            raise AssertionError("Reporting may only read")
        if "getOrderList" not in text:
            if self.detail_error:
                raise self.detail_error
            found = [row for row in self.rows.values() if row["order_num"] == variable_values["order_num"]]
            return {"getOrder": copy.deepcopy(found[0])}
        reduced = "GetOrdersWithoutPriceElements" in text
        if self.full_error and not reduced:
            raise self.full_error
        self.pages.append(copy.deepcopy(variable_values))
        self.before(self, variable_values)
        params = variable_values["params"]
        rows = sorted(self.rows.values(), key=lambda row: int(row["id"]), reverse=params["sort"] == "DESC")
        offset, limit = params.get("cursor", 0), params["limit"]
        page = copy.deepcopy(rows[offset:offset + limit])
        if reduced:
            for row in page:
                row.pop("price_elements", None)
        more = bool(page) and offset + limit <= len(rows)  # Known full-terminal FLOX convention.
        result = {"getOrderList": {"data": page, "pageInfo": {
            "hasNextPage": more, "nextCursor": offset + limit if more else None,
            "totalRecords": len(rows),
        }}}
        self.after(self, variable_values, result)
        return result


class ReportingInventoryTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.clock = Clock()
        for target, value in (("export_orders.time.monotonic", self.clock.monotonic),
                              ("export_orders.time.sleep", self.clock.sleep)):
            mocked = patch(target, value)
            mocked.start()
            self.addCleanup(mocked.stop)
        self.exp = BizniWebExporter("https://example.invalid/graphql", "synthetic", project_name="vevo", order_facts_only=True)
        self.exp.cache_dir = Path(self.folder.name)
        self.exp.prepare_reporting_status_identity = Mock()
        self.exp.cache_days_threshold = 0
        self.api = MutableAPI([order(i) for i in range(1, 91)])
        self.exp.client = self.api

    def fetch(self, start=DAY, end=DAY):
        return self.exp.fetch_orders(start, end)

    def assert_no_cache(self):
        self.assertEqual([], list(self.exp.cache_dir.iterdir()))

    def test_more_than_nine_hundred_same_day_orders_are_complete_with_proven_cache(self):
        self.api.rows = {i: order(i) for i in range(1, 941)}
        rows = self.fetch()
        self.assertEqual(list(range(1, 941)), [int(row["id"]) for row in rows])
        stored = json.loads(self.exp.get_cache_filename(DAY).read_text(encoding="utf-8"))
        self.assertEqual(940, stored["complete_inventory"]["inventory_count"])
        self.assertEqual(940, stored["complete_inventory"]["anchor_max_id"])
        self.assertEqual(rows, self.exp.load_from_cache(DAY))
        self.assertEqual("DESC", self.api.pages[0]["params"]["sort"])
        self.assertTrue(all(call["params"]["order_by"] == "order_id" for call in self.api.pages))

    def test_new_head_after_anchor_is_deferred_even_if_backdated(self):
        def insert(api, variables):
            if len(api.pages) == 2:
                api.rows[91] = order(91)
        self.api.before = insert
        self.assertEqual(list(range(1, 91)), [int(row["id"]) for row in self.fetch()])

    def test_all_vevo_date_entry_points_use_complete_scan_and_prepare_status(self):
        self.api.rows = {i: order(i) for i in range(1, 941)}
        for method in ("fetch_orders_for_month", "fetch_orders_for_period", "_fetch_orders_original"):
            with self.subTest(method=method), patch.object(self.exp, "should_use_cache", return_value=False):
                self.api.pages.clear()
                self.exp.prepare_reporting_status_identity.reset_mock()
                rows = getattr(self.exp, method)(DAY, DAY)
                self.assertEqual(940, len(rows))
                self.assertEqual({"order_id"}, {call["params"]["order_by"] for call in self.api.pages})
                self.exp.prepare_reporting_status_identity.assert_called_once()

    def test_all_vevo_date_entry_points_propagate_page_failure_without_chunk_rescue(self):
        for method in ("fetch_orders_for_month", "fetch_orders_for_period", "_fetch_orders_original"):
            with self.subTest(method=method):
                self.api.pages.clear()
                def fail(api, variables):
                    if len(api.pages) >= 3:
                        raise TransportServerError("synthetic permanent", code=403)
                self.api.before = fail
                with self.assertRaises(TransportServerError):
                    getattr(self.exp, method)(DAY, DAY)
                self.assertEqual(3, len(self.api.pages))
                self.assert_no_cache()

    def test_offset_deletion_repair_consumes_exact_continuity_response(self):
        proof_offsets = []
        def delete_before(api, variables):
            if len(api.pages) == 3:
                del api.rows[1]
        def delete_after(api, variables, result):
            rows = result["getOrderList"]["data"]
            if variables["params"]["limit"] == 30 and rows and rows[0]["id"] == "30":
                proof_offsets.append(variables["params"]["cursor"])
                del api.rows[31]
        self.api.before, self.api.after = delete_before, delete_after
        self.assertEqual(list(range(1, 91)), [int(row["id"]) for row in self.fetch()])
        self.assertEqual([28], proof_offsets)

    def test_full_terminal_and_empty_inventory_are_proven_without_open_empty_read(self):
        for count in (0, 1, 30, 59):
            with self.subTest(count=count):
                self.api.rows = {i: order(i) for i in range(1, count + 1)}
                self.api.pages.clear()
                with patch.object(self.exp, "should_use_cache", return_value=False):
                    self.assertEqual(count, len(self.fetch()))
                self.assertEqual(count, len(self.exp.load_from_cache(DAY)))
                self.assertTrue(all(call["params"]["cursor"] < max(1, count) for call in self.api.pages))

    def test_malformed_pagination_cannot_write_partial_cache(self):
        for case in ("missing_total", "bool_total", "negative_total", "empty_open", "short", "cursor"):
            with self.subTest(case=case):
                def alter(api, variables, result):
                    if variables["params"]["sort"] != "ASC":
                        return
                    data = result["getOrderList"]
                    if case == "missing_total":
                        data["pageInfo"].pop("totalRecords")
                    elif case == "bool_total":
                        data["pageInfo"]["totalRecords"] = True
                    elif case == "negative_total":
                        data["pageInfo"]["totalRecords"] = -1
                    elif case == "empty_open":
                        data["data"] = []
                    elif case == "short":
                        data["data"].pop()
                    else:
                        data["pageInfo"]["nextCursor"] = 31
                self.api.after = alter
                with self.assertRaises(RuntimeError):
                    self.fetch()
                self.assert_no_cache()

    def test_invalid_native_membership_or_date_anywhere_fails_entire_scan(self):
        for changes in ({"id": True}, {"order_num": ""}, {"pur_date": "2025-02-30"},
                        {"pur_date": "2025-01-01 99:00:00"}, {"pur_date": None}):
            with self.subTest(changes=changes):
                self.api.rows[40] = {**order(40, datetime(2025, 1, 1)), **changes}
                with self.assertRaises((RuntimeError, ValueError)):
                    self.fetch()
                self.assert_no_cache()

    def test_invalid_or_duplicate_id_does_not_get_silently_deduplicated(self):
        self.api.rows[40]["order_num"] = self.api.rows[10]["order_num"]
        with self.assertRaisesRegex(RuntimeError, "reused an order number"):
            self.fetch()
        self.assert_no_cache()

    def test_date_filter_follows_complete_id_scan_without_out_of_range_business_validation(self):
        self.api.rows = {i: order(i, datetime(2025, 1, 1)) for i in range(1, 91)}
        self.api.rows[60] = order(60)
        self.api.rows[1]["status"] = None
        checked = []
        def context(row):
            checked.append(row["id"])
            if row["id"] != "60":
                raise AssertionError("Out-of-range business validation")
            return row
        with patch.object(self.exp, "_reporting_order_context", side_effect=context):
            self.assertEqual(["60"], [row["id"] for row in self.fetch()])
        self.assertTrue(checked)
        self.assertEqual({"60"}, set(checked))
        self.assertEqual(90, json.loads(self.exp.get_cache_filename(DAY).read_text())["complete_inventory"]["inventory_count"])

    def test_later_page_failure_propagates_without_partial_or_opposite_scan(self):
        def fail(api, variables):
            if len(api.pages) >= 3:
                raise TransportServerError("synthetic permanent", code=403)
        self.api.before = fail
        with self.assertRaises(TransportServerError):
            self.fetch()
        self.assert_no_cache()
        self.assertIsNone(self.exp._inventory_read_deadline)
        self.assertEqual(["DESC", "ASC", "ASC"], [call["params"]["sort"] for call in self.api.pages])

    def test_business_failure_on_later_requested_day_writes_no_earlier_day(self):
        self.api.rows = {1: order(1), 2: order(2, DAY + timedelta(days=1))}
        def context(row):
            if row["id"] == "2":
                raise ValueError("synthetic status identity rejection")
            return row
        with patch.object(self.exp, "_reporting_order_context", side_effect=context), self.assertRaises(ValueError):
            self.fetch(DAY, DAY + timedelta(days=1))
        self.assert_no_cache()

    def test_supported_price_element_failure_retries_same_page_and_enriches_details(self):
        self.api.rows = {1: order(1), 2: order(2)}
        self.api.full_error = TransportQueryError("synthetic price_elements resolver failure")
        self.assertEqual(2, len(self.fetch()))
        full = [variables for text, variables in self.api.calls if "query GetOrders(" in text]
        reduced = [variables for text, variables in self.api.calls if "GetOrdersWithoutPriceElements" in text]
        self.assertEqual(full, reduced)
        self.assertTrue(all(row["price_elements"] for row in self.exp.load_from_cache(DAY)))

    def test_failed_detail_read_cannot_yield_partial_or_cache(self):
        self.api.full_error = TransportQueryError("synthetic price_elements resolver failure")
        self.api.detail_error = TransportServerError("synthetic unauthorized", code=401)
        with self.assertRaises(TransportServerError):
            self.fetch()
        self.assert_no_cache()

    def test_old_cache_needs_scan_provenance_and_content_tamper_invalidates_it(self):
        cache = self.exp.get_cache_filename(DAY)
        cache.write_text(json.dumps({"schema_version": ORDER_CACHE_SCHEMA_VERSION, "orders": [order(1)]}), encoding="utf-8")
        self.assertIsNone(self.exp.load_from_cache(DAY))
        self.assertEqual(90, len(self.fetch()))
        data = json.loads(cache.read_text(encoding="utf-8"))
        data["orders"].pop()
        cache.write_text(json.dumps(data), encoding="utf-8")
        self.assertIsNone(self.exp.load_from_cache(DAY))

    def test_invalid_marker_and_cross_day_or_project_cache_are_rejected(self):
        self.fetch()
        cache = self.exp.get_cache_filename(DAY)
        original = json.loads(cache.read_text(encoding="utf-8"))
        for changes in ({"project": "roy"}, {"complete": False}, {"anchor_max_id": True},
                        {"inventory_count": 1}, {"pages": 0}, {"date": "2026-09-02"},
                        {"completed_at": "2026-09-01"}):
            with self.subTest(changes=changes):
                changed = copy.deepcopy(original)
                changed["complete_inventory"].update(changes)
                cache.write_text(json.dumps(changed), encoding="utf-8")
                self.assertIsNone(self.exp.load_from_cache(DAY))

    def test_config_flag_alone_cannot_create_proven_cache(self):
        with self.assertRaisesRegex(RuntimeError, "complete inventory evidence"):
            self.exp.save_to_cache_simple(DAY, [order(1)])
        self.assert_no_cache()

    def test_valid_empty_cache_avoids_another_full_scan(self):
        self.api.rows = {}
        self.assertEqual([], self.fetch())
        self.api.execute = Mock(side_effect=AssertionError("Proven empty day is cacheable"))
        self.assertEqual([], self.fetch())

    def test_both_actual_list_queries_include_total_records(self):
        for query in (ORDER_QUERY, ORDER_QUERY_WITHOUT_PRICE_ELEMENTS):
            text = print_ast(getattr(query, "document", query))
            self.assertIn("totalRecords", text)
            self.assertIn("delivery_address", text)
            self.assertIn("raw_value", text)

    def test_default_false_and_roy_settings_preserve_legacy_path(self):
        for project in ("vevo", "roy"):
            exp = BizniWebExporter("https://example.invalid/graphql", "synthetic", project_name=project, order_facts_only=True)
            if project == "vevo":
                exp.complete_order_inventory_enabled = False
            self.assertFalse(exp.complete_order_inventory_enabled)
            exp.prepare_reporting_status_identity = Mock()
            with patch.object(exp, "should_use_cache", return_value=True), \
                    patch.object(exp, "load_from_cache", return_value=[order(1)]), \
                    patch.object(exp, "_filter_by_status", side_effect=lambda rows: rows), \
                    patch.object(exp, "_fetch_complete_order_inventory", side_effect=AssertionError("legacy must not scan")):
                self.assertEqual([order(1)], exp.fetch_orders(DAY, DAY))

    def test_non_boolean_configuration_is_rejected(self):
        for value in ("true", 1, [], None):
            with self.subTest(value=value), patch("export_orders.load_project_settings", return_value={"complete_order_inventory_enabled": value}):
                with self.assertRaisesRegex(ValueError, "must be boolean"):
                    BizniWebExporter("https://example.invalid/graphql", "synthetic", order_facts_only=True)

    def test_committed_vevo_policy_enabled_roy_absent_and_default_false(self):
        self.assertTrue(reporting.load_reporting_order_inventory_policy("vevo"))
        self.assertFalse(reporting.load_reporting_order_inventory_policy("roy"))
        with patch("export_orders.project_dir", return_value=Path(self.folder.name)):
            self.assertFalse(reporting.load_reporting_order_inventory_policy("synthetic"))
        settings = {**copy.deepcopy(self.exp.project_settings), "complete_order_inventory_enabled": False}
        with patch("export_orders.load_project_settings", return_value=settings):
            exp = BizniWebExporter("https://example.invalid/graphql", "synthetic", project_name="vevo", order_facts_only=True)
        self.assertFalse(exp.complete_order_inventory_enabled)

    def test_optional_policy_has_exact_schema_and_boolean_types(self):
        path = Path(self.folder.name) / "order_inventory_policy.json"
        invalid = [{}, [], {"schema_version": True, "complete_order_inventory_enabled": True},
                   {"schema_version": 2, "complete_order_inventory_enabled": True},
                   {"schema_version": 1, "complete_order_inventory_enabled": "true"},
                   {"schema_version": 1, "complete_order_inventory_enabled": True, "extra": True}]
        with patch("export_orders.project_dir", return_value=Path(self.folder.name)):
            for policy in invalid:
                with self.subTest(policy=policy):
                    path.write_text(json.dumps(policy), encoding="utf-8")
                    with self.assertRaises(ValueError):
                        reporting.load_reporting_order_inventory_policy("synthetic")
            path.write_text("{broken", encoding="utf-8")
            with self.assertRaises(ValueError):
                reporting.load_reporting_order_inventory_policy("synthetic")

    def test_2025_and_2026_money_and_eligibility_are_unchanged_on_identical_rows(self):
        self.exp._rebuild_product_expense_indexes({"Synthetic inventory product": 6.5})
        rows = [order(1, datetime(2025, 9, 1)), order(2), order(3)]
        rows[2]["status"] = {"id": "17", "name": "Storno"}
        before = copy.deepcopy(rows)
        self.exp.complete_order_inventory_enabled = False
        legacy = self.exp._filter_by_status(rows, track_excluded=False)
        baseline = [self.exp.flatten_order(row) for row in legacy]
        self.exp.complete_order_inventory_enabled = True
        current = self.exp._filter_by_status(rows, track_excluded=False)
        self.assertEqual(legacy, current)
        self.assertEqual(baseline, [self.exp.flatten_order(row) for row in current])
        self.assertEqual(before, rows)
        self.assertEqual([6.5, 6.5], [entry[0]["total_expense"] for entry in baseline])

    def test_minimum_pacing_and_long_pause_consume_shared_deadline(self):
        self.exp._api_request_count = 100
        self.exp._api_last_request_completed_at = self.clock.value
        with patch("export_orders.REPORTING_INVENTORY_TIMEOUT_SEC", 4), self.assertRaisesRegex(RuntimeError, "time limit"):
            self.fetch()
        self.assertEqual([], self.api.calls)
        self.assert_no_cache()

    def test_retry_after_cannot_outlive_budget_or_reuse_stale_headers(self):
        def unavailable(api, variables):
            api.transport.response_headers = {"Retry-After": "120"}
            raise TransportServerError("synthetic quota", code=429)
        self.api.before = unavailable
        with patch("export_orders.REPORTING_INVENTORY_TIMEOUT_SEC", 60), self.assertRaisesRegex(RuntimeError, "time limit"):
            self.fetch()
        self.assertEqual(1, len(self.api.calls))
        self.assertEqual([], self.clock.waits)
        self.assert_no_cache()

    def test_retryable_transport_failure_has_three_explicit_attempts_only(self):
        self.api.before = lambda *_: (_ for _ in ()).throw(TransportServerError("synthetic unavailable", code=503))
        with self.assertRaises(TransportServerError):
            self.fetch()
        self.assertEqual(3, len(self.api.calls))
        self.assert_no_cache()

    def test_partial_graphql_rows_are_not_retried_or_returned(self):
        self.api.before = lambda *_: (_ for _ in ()).throw(TransportQueryError(
            "synthetic failure", data={"getOrderList": {"data": [order(1)]}},
        ))
        with self.assertRaises(TransportQueryError):
            self.fetch()
        self.assertEqual(1, len(self.api.calls))
        self.assert_no_cache()

    def test_retry_discards_previous_response_headers_and_preserves_pacing(self):
        self.api.rows = {1: order(1)}
        def fail_then_succeed(api, variables):
            self.assertIsNone(api.transport.response_headers)
            if len(api.calls) == 1:
                api.transport.response_headers = {"Retry-After": "2"}
                raise TransportServerError("synthetic throttle", code=429)
        self.api.before = fail_then_succeed
        self.assertEqual(["1"], [row["id"] for row in self.fetch()])
        self.assertEqual(3, len(self.api.calls))  # Retried anchor, then ascending page.
        self.assertEqual([2.0, 0.5, 0.5], self.clock.waits)
        self.assertIsNone(self.exp._inventory_read_deadline)

    def test_scope_restores_new_session_retries_existing_adapters_timeout_and_deadline(self):
        transport = RequestsHTTPTransport(url="https://example.invalid/graphql", retries=4, timeout=30)
        transport.connect()  # Local requests Session setup only; no network.
        self.addCleanup(transport.close)
        self.api.transport = transport
        original_adapters = [(adapter, adapter.max_retries) for adapter in transport.session.adapters.values()]
        previous = self.exp._inventory_read_deadline
        def check(api, variables):
            self.assertEqual(0, transport.retries)
            self.assertTrue(all(adapter.max_retries.total == 0 for adapter in transport.session.adapters.values()))
            self.assertLessEqual(transport.default_timeout, 4)
            raise TransportServerError("synthetic permanent", code=403)
        self.api.before = check
        with patch("export_orders.REPORTING_INVENTORY_TIMEOUT_SEC", 4), self.assertRaises(TransportServerError):
            self.fetch()
        self.assertEqual(4, transport.retries)
        self.assertEqual(30, transport.default_timeout)
        self.assertIs(previous, self.exp._inventory_read_deadline)
        for adapter, retries in original_adapters:
            self.assertIs(retries, adapter.max_retries)

    def test_elapsed_success_response_is_rejected_before_cache(self):
        self.api.after = lambda *_: setattr(self.clock, "value", self.clock.value + 5)
        with patch("export_orders.REPORTING_INVENTORY_TIMEOUT_SEC", 4), self.assertRaisesRegex(RuntimeError, "time limit"):
            self.fetch()
        self.assert_no_cache()


if __name__ == "__main__":
    unittest.main()
