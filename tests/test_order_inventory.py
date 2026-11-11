import unittest
from copy import deepcopy
from unittest.mock import patch

from order_inventory import InventoryScanBudget, PaginationMetadataError, scan_order_inventory


class InventoryScanContractTests(unittest.TestCase):
    def setUp(self):
        self.rows = [{"id": str(i), "order_num": f"synthetic-{i}", "payment_context": {"reference_id": "6"}}
                     for i in range(1, 5)]
        self.requests = []
        self.validated = []

    def read(self, variables, deadline):
        self.requests.append((deepcopy(variables), deadline))
        params = variables["params"]
        rows = sorted(deepcopy(self.rows), key=lambda row: int(row["id"]), reverse=params["sort"] == "DESC")
        offset, limit = params["cursor"], params["limit"]
        more = offset + limit < len(rows)
        return {"getOrderList": {"data": rows[offset:offset + limit], "pageInfo": {
            "hasNextPage": more, "nextCursor": offset + limit if more else None, "totalRecords": len(rows),
        }}}

    def validate(self, row):
        self.assertIn("payment_context", row)
        self.validated.append(row["id"])

    def test_shared_engine_preserves_caller_fields_and_validates_anchor_and_overlaps(self):
        budget = InventoryScanBudget()
        result = scan_order_inventory(self.read, self.validate, budget=budget, page_limit=3)
        self.assertEqual(self.rows, result)
        self.assertEqual(["4", "1", "2", "3", "3", "4"], self.validated)
        self.assertEqual(3, budget.pages)
        self.assertEqual([0, 0, 2], [variables["params"]["cursor"] for variables, _ in self.requests])

    def test_full_and_changed_calls_share_page_and_time_budget(self):
        budget = InventoryScanBudget(max_pages=5)
        with patch("order_inventory.time.monotonic", return_value=100):
            scan_order_inventory(self.read, self.validate, budget=budget, page_limit=3)
        with patch("order_inventory.time.monotonic", return_value=110):
            with self.assertRaisesRegex(RuntimeError, "limit"):
                scan_order_inventory(self.read, self.validate, budget=budget, page_limit=3,
                                     changed_from="2026-01-01 00:00:00")
        self.assertEqual(5, budget.pages)
        self.assertTrue(all(deadline == 1300 for _, deadline in self.requests))
        self.assertNotIn("changed_from", self.requests[3][0])  # Global upper anchor.
        self.assertIn("changed_from", self.requests[4][0])

    def test_validator_failure_and_missing_identity_cannot_return_partial_inventory(self):
        for missing in ("payment_context", "order_num"):
            with self.subTest(missing=missing):
                rows = deepcopy(self.rows)
                self.rows[1].pop(missing)
                with self.assertRaises((AssertionError, RuntimeError)):
                    scan_order_inventory(self.read, self.validate, budget=InventoryScanBudget(), page_limit=3)
                self.rows = rows


class InventoryTerminalMetadataTests(unittest.TestCase):
    def setUp(self):
        self.rows = [{"id": str(i), "order_num": f"synthetic-{i}"} for i in range(1, 60)]
        self.responses = []
        self.requests = []
        self.changed_limit = None
        self.change_response = lambda *_: None

    def read(self, variables, deadline):
        params = variables["params"]
        rows = self.rows
        if "changed_from" in variables and self.changed_limit is not None:
            rows = rows[:self.changed_limit]
        rows = sorted(deepcopy(rows), key=lambda row: int(row["id"]), reverse=params["sort"] == "DESC")
        offset, limit = params["cursor"], params["limit"]
        page = rows[offset:offset + limit]
        # Observed FLOX convention: an exactly full terminal page can retain
        # hasNextPage=true and point nextCursor at the end of the inventory.
        advertised_more = bool(page) and offset + limit <= len(rows)
        result = {"getOrderList": {"data": page, "pageInfo": {
            "hasNextPage": advertised_more,
            "nextCursor": offset + limit if advertised_more else None,
            "totalRecords": len(rows),
        }}}
        self.requests.append(deepcopy(variables))
        self.change_response(variables, result)
        self.responses.append(result)
        return result

    def scan(self, **kwargs):
        return scan_order_inventory(self.read, lambda row: None, budget=InventoryScanBudget(), **kwargs)

    def test_full_terminal_pages_and_single_row_anchor_keep_every_order(self):
        for count in (1, 30, 59, 88, 1074):
            with self.subTest(count=count):
                self.setUp()
                self.rows = [{"id": str(i), "order_num": f"synthetic-{i}"} for i in range(1, count + 1)]
                self.assertEqual(self.rows, self.scan())
                final = self.responses[-1]["getOrderList"]
                if count != 1:
                    self.assertTrue(final["pageInfo"]["hasNextPage"])
                    self.assertEqual(count, final["pageInfo"]["nextCursor"])
                self.assertTrue(all(call["params"]["cursor"] < count for call in self.requests))

    def test_changed_scan_ends_at_full_terminal_page_below_global_upper_id(self):
        self.rows.append({"id": "60", "order_num": "synthetic-60"})
        self.changed_limit = 59
        rows = self.scan(changed_from="2026-01-01 00:00:00")
        self.assertEqual(self.rows[:59], rows)
        self.assertEqual([0, 0, 29], [call["params"]["cursor"] for call in self.requests])
        self.assertEqual(60, self.responses[0]["getOrderList"]["pageInfo"]["totalRecords"])
        self.assertTrue(self.responses[-1]["getOrderList"]["pageInfo"]["hasNextPage"])

    def test_short_and_empty_terminal_inventory_remain_valid(self):
        for count in (0, 1, 24, 58):
            with self.subTest(count=count):
                self.setUp()
                self.rows = self.rows[:count]
                self.assertEqual(self.rows, self.scan())
                self.assertFalse(self.responses[-1]["getOrderList"]["pageInfo"]["hasNextPage"])
        self.setUp()
        self.changed_limit = 0
        self.assertEqual([], self.scan(changed_from="2026-01-01 00:00:00"))

    def test_full_terminal_hint_still_requires_exact_numeric_next_cursor(self):
        for cursor in (None, 0, 58, 60, True, "59"):
            with self.subTest(cursor=cursor):
                self.setUp()
                def change(variables, result):
                    if variables["params"]["cursor"] == 29:
                        result["getOrderList"]["pageInfo"]["nextCursor"] = cursor
                self.change_response = change
                with self.assertRaisesRegex(RuntimeError, "cursor"):
                    self.scan()

    def test_false_before_end_and_short_or_empty_advertised_next_still_fail(self):
        cases = ("false_before_end", "short_nonfinal", "short_terminal", "empty", "understated_count")
        for case in cases:
            with self.subTest(case=case):
                self.setUp()
                def change(variables, result):
                    if variables["params"]["sort"] != "ASC":
                        return
                    payload = result["getOrderList"]
                    if case == "false_before_end":
                        payload["pageInfo"]["hasNextPage"] = False
                    elif case == "short_nonfinal":
                        payload["data"].pop()
                    elif case == "short_terminal":
                        payload["data"].pop()
                        payload["pageInfo"]["totalRecords"] = len(payload["data"])
                        payload["pageInfo"]["nextCursor"] = len(payload["data"])
                    elif case == "empty":
                        payload["data"] = []
                        payload["pageInfo"]["totalRecords"] = 0
                        payload["pageInfo"]["nextCursor"] = 0
                    else:
                        payload["pageInfo"]["totalRecords"] = len(payload["data"]) - 1
                self.change_response = change
                with self.assertRaisesRegex(RuntimeError, "pagination metadata"):
                    self.scan()


class InventoryTerminalCountProofTests(unittest.TestCase):
    def setUp(self):
        self.rows = [{"id": str(i), "order_num": f"private-order-{i}", "value": i * 10} for i in range(1, 49)]
        self.requests = []
        self.responses = []
        self.alter = lambda *_: None
        self.now = 100.0
        for target, value in (("order_inventory.time.monotonic", lambda: self.now),
                              ("order_inventory.time.sleep", lambda _delay: None)):
            mocked = patch(target, value)
            mocked.start()
            self.addCleanup(mocked.stop)

    def read(self, variables, deadline):
        self.requests.append((deepcopy(variables), deadline))
        params = variables["params"]
        rows = sorted(deepcopy(self.rows), key=lambda row: int(row["id"]), reverse=params["sort"] == "DESC")
        offset, limit = params["cursor"], params["limit"]
        page = rows[offset:offset + limit]
        more = offset + len(page) < len(rows)
        result = {"getOrderList": {"data": page, "pageInfo": {
            "hasNextPage": more, "nextCursor": offset + len(page) if more else None,
            "totalRecords": len(rows) + 1,
        }}}
        self.alter(variables, result)
        self.responses.append(result)
        return result

    def scan(self, **kwargs):
        budget = kwargs.pop("budget", InventoryScanBudget())
        reconcile = kwargs.pop("reconcile_terminal_count", True)
        return scan_order_inventory(self.read, lambda row: self.assertIsInstance(row["value"], int),
                                    budget=budget, reconcile_terminal_count=reconcile, **kwargs)

    def test_exact_terminal_proof_returns_every_original_row_and_preserves_raw_metadata(self):
        budget = InventoryScanBudget()
        self.assertEqual(self.rows, self.scan(budget=budget))
        self.assertEqual(6, budget.pages)
        self.assertEqual([(0, 1, "DESC"), (0, 30, "ASC"), (29, 30, "ASC"),
                          (29, 30, "ASC"), (0, 30, "DESC"), (48, 30, "ASC")],
                         [(variables["params"]["cursor"], variables["params"]["limit"], variables["params"]["sort"])
                          for variables, _ in self.requests])
        self.assertTrue(all(response["getOrderList"]["pageInfo"]["totalRecords"] == 49 for response in self.responses))
        self.assertEqual({1300.0}, {deadline for _, deadline in self.requests})

    def test_reporting_retries_precede_one_terminal_proof_without_restarting_scan(self):
        self.assertEqual(self.rows, self.scan(metadata_retries=2))
        self.assertEqual(8, len(self.requests))
        self.assertEqual(1, sum(variables["params"] == {"limit": 1, "order_by": "order_id", "sort": "DESC", "cursor": 0}
                                for variables, _ in self.requests))

    def test_proof_returns_original_financial_rows_not_repeated_or_reverse_rows(self):
        def alter(_variables, result):
            if len(self.requests) in (4, 5):
                for row in result["getOrderList"]["data"]:
                    row["value"] = -999
        self.alter = alter
        self.assertEqual(self.rows, self.scan())

    def test_shared_default_and_incremental_scan_never_apply_terminal_exception(self):
        for kwargs in ({"reconcile_terminal_count": False}, {"changed_from": "2026-11-01 00:00:00"}):
            with self.subTest(kwargs=kwargs):
                self.requests.clear()
                with self.assertRaises(PaginationMetadataError):
                    self.scan(**kwargs)
                self.assertEqual(3, len(self.requests))

    def test_ineligible_terminal_shapes_remain_fatal_without_proof_reads(self):
        for case in ("missing-anchor", "wrong-anchor-number", "wrong-overlap-number", "missing-cursor",
                     "two-extra-records", "short-continuation", "understated-count"):
            with self.subTest(case=case):
                self.requests.clear()
                def alter(variables, result):
                    if variables["params"]["cursor"] != 29:
                        return
                    payload = result["getOrderList"]
                    if case == "missing-anchor":
                        payload["data"].pop()
                        payload["pageInfo"]["totalRecords"] -= 1  # Still exactly one extra, but missing anchor.
                    elif case == "wrong-anchor-number":
                        payload["data"][-1]["order_num"] = "other-anchor"
                    elif case == "wrong-overlap-number":
                        payload["data"][0]["order_num"] = "other-overlap"
                    elif case == "missing-cursor":
                        payload["pageInfo"].pop("nextCursor")
                    elif case == "two-extra-records":
                        payload["pageInfo"]["totalRecords"] += 1
                    elif case == "short-continuation":
                        payload["pageInfo"]["hasNextPage"] = True
                        payload["pageInfo"]["nextCursor"] = 48
                    else:
                        payload["pageInfo"]["totalRecords"] = 47
                self.alter = alter
                with self.assertRaises(PaginationMetadataError):
                    self.scan()
                self.assertEqual(3, len(self.requests))

    def test_first_page_cannot_claim_unproved_overlap(self):
        self.rows = self.rows[:19]
        with self.assertRaises(PaginationMetadataError):
            self.scan()
        self.assertEqual(2, len(self.requests))

    def test_repeat_reverse_and_empty_proof_failures_never_return_inventory(self):
        cases = {
            4: ("pair", "reordered", "duplicate", "total", "missing-cursor", "business"),
            5: ("pair", "reordered", "duplicate", "total", "new-anchor", "short", "cursor", "business"),
            6: ("nonempty", "total", "missing-cursor", "continuation"),
        }
        for stage, changes in cases.items():
            for case in changes:
                with self.subTest(stage=stage, case=case):
                    self.requests.clear()
                    def alter(_variables, result):
                        if len(self.requests) != stage:
                            return
                        payload = result["getOrderList"]
                        if case == "pair":
                            payload["data"][2]["order_num"] = "different-pair"
                        elif case == "reordered":
                            payload["data"][0], payload["data"][1] = payload["data"][1], payload["data"][0]
                        elif case == "duplicate":
                            payload["data"][1] = deepcopy(payload["data"][0])
                        elif case == "total":
                            payload["pageInfo"]["totalRecords"] += 1
                        elif case == "missing-cursor":
                            payload["pageInfo"].pop("nextCursor")
                        elif case == "business":
                            payload["data"][0]["value"] = "invalid"
                        elif case == "new-anchor":
                            payload["data"][0]["id"] = "999"
                        elif case == "short":
                            payload["data"].pop()
                        elif case == "cursor":
                            payload["pageInfo"]["nextCursor"] = 31
                        elif case == "nonempty":
                            payload["data"] = [{"id": "49", "order_num": "new-order", "value": 1}]
                        elif case == "continuation":
                            payload["pageInfo"]["hasNextPage"] = True
                            payload["pageInfo"]["nextCursor"] = 49
                    self.alter = alter
                    with self.assertRaises((RuntimeError, AssertionError)):
                        self.scan()
                    self.assertEqual(stage, len(self.requests))

    def test_each_proof_read_consumes_original_page_and_time_budget(self):
        for max_pages in (3, 4, 5):
            with self.subTest(max_pages=max_pages):
                self.requests.clear()
                with self.assertRaisesRegex(RuntimeError, "limit reached"):
                    self.scan(budget=InventoryScanBudget(max_pages=max_pages))
                self.assertEqual(max_pages, len(self.requests))
        self.requests.clear()
        def expire(_variables, _result):
            if len(self.requests) == 6:
                self.now += 20
        self.alter = expire
        with self.assertRaisesRegex(RuntimeError, "time limit"):
            self.scan(budget=InventoryScanBudget(timeout_seconds=10))
        self.assertEqual(6, len(self.requests))


class InventoryMetadataRecheckTests(unittest.TestCase):
    def setUp(self):
        self.rows = [{"id": str(i), "order_num": f"private-order-{i}"} for i in range(1, 91)]
        self.requests = []
        self.alter = lambda *_: None
        self.now = 100.0
        self.waits = []
        for target, value in (("order_inventory.time.monotonic", lambda: self.now),
                              ("order_inventory.time.sleep", self.sleep)):
            mocked = patch(target, value)
            mocked.start()
            self.addCleanup(mocked.stop)

    def sleep(self, delay):
        self.waits.append(delay)
        self.now += delay

    def read(self, variables, deadline):
        self.requests.append((deepcopy(variables), deadline))
        params = variables["params"]
        rows = sorted(deepcopy(self.rows), key=lambda row: int(row["id"]), reverse=params["sort"] == "DESC")
        offset, limit = params["cursor"], params["limit"]
        page = rows[offset:offset + limit]
        more = offset + len(page) < len(rows)
        result = {"getOrderList": {"data": page, "pageInfo": {
            "hasNextPage": more, "nextCursor": offset + len(page) if more else None,
            "totalRecords": len(rows),
        }}}
        self.alter(variables, result)
        return result

    def scan(self, *, retries=2, budget=None):
        return scan_order_inventory(self.read, lambda _row: None,
                                    budget=budget or InventoryScanBudget(), metadata_retries=retries)

    @staticmethod
    def contradict(result):
        result["getOrderList"]["pageInfo"]["totalRecords"] += 1
        result["getOrderList"]["pageInfo"]["hasNextPage"] = False

    def test_one_metadata_recheck_keeps_exact_request_deadline_and_complete_rows(self):
        def alter(variables, result):
            if len(self.requests) == 3:
                self.contradict(result)
        self.alter = alter
        budget = InventoryScanBudget()
        self.assertEqual(self.rows, self.scan(budget=budget))
        self.assertEqual(self.requests[2], self.requests[3])
        self.assertEqual(len(self.requests), budget.pages)
        self.assertEqual({1300.0}, {deadline for _, deadline in self.requests})
        self.assertEqual([1.0], self.waits)

    def test_persistent_numeric_error_fails_after_three_reads_with_safe_diagnostics(self):
        def alter(variables, result):
            if variables["params"]["sort"] == "ASC":
                self.contradict(result)
                result["getOrderList"]["provider_private"] = "private-token-sentinel"
        self.alter = alter
        with self.assertRaises(PaginationMetadataError) as raised:
            self.scan()
        self.assertEqual(4, len(self.requests))  # One anchor and three same-page reads.
        self.assertEqual([1.0, 2.0], self.waits)
        self.assertEqual(3, raised.exception.diagnostics["attempt"])
        self.assertEqual(["next-flag-count"], raised.exception.diagnostics["reasons"])
        self.assertNotIn("private-", str(raised.exception))
        self.assertEqual({"offset", "limit", "row_count", "has_next", "total_records", "next_cursor",
                          "reasons", "attempt", "rechecks_used"}, set(raised.exception.diagnostics))

    def test_default_shared_scanner_has_no_retries(self):
        self.alter = lambda variables, result: self.contradict(result) if variables["params"]["sort"] == "ASC" else None
        with self.assertRaises(PaginationMetadataError):
            scan_order_inventory(self.read, lambda _row: None, budget=InventoryScanBudget())
        self.assertEqual(2, len(self.requests))
        self.assertEqual([], self.waits)

    def test_deletion_during_recheck_uses_original_continuity_repair(self):
        original = deepcopy(self.rows)
        def alter(variables, result):
            if len(self.requests) == 3:
                self.contradict(result)
                self.rows.pop(0)
        self.alter = alter
        self.assertEqual(original, self.scan())
        offsets = [variables["params"]["cursor"] for variables, _ in self.requests]
        self.assertEqual([0, 0, 29, 29], offsets[:4])
        self.assertIn(28, offsets)  # Repair proves the old previous-ID overlap again.

    def test_failed_page_is_discarded_and_new_head_after_anchor_is_deferred(self):
        def alter(variables, result):
            if len(self.requests) == 3:
                self.contradict(result)
                self.rows = [row for row in self.rows if row["id"] != "31"]
                self.rows.append({"id": "91", "order_num": "private-order-91"})
        self.alter = alter
        self.assertEqual([str(i) for i in range(1, 91) if i != 31], [row["id"] for row in self.scan()])

    def test_retry_budget_cannot_sleep_or_read_past_page_or_time_limits(self):
        for budget in (InventoryScanBudget(max_pages=2), InventoryScanBudget(timeout_seconds=0.5)):
            with self.subTest(budget=budget):
                self.requests.clear()
                self.waits.clear()
                self.alter = lambda variables, result: self.contradict(result) if variables["params"]["sort"] == "ASC" else None
                with self.assertRaisesRegex(RuntimeError, "limit reached"):
                    self.scan(budget=budget)
                self.assertEqual(2, len(self.requests))
                self.assertEqual([], self.waits)

    def test_success_after_sleep_that_overruns_deadline_is_never_read(self):
        self.alter = lambda variables, result: self.contradict(result) if variables["params"]["sort"] == "ASC" else None
        def oversleep(_delay):
            self.now += 5
        with patch("order_inventory.time.sleep", oversleep), self.assertRaisesRegex(RuntimeError, "limit reached"):
            self.scan(budget=InventoryScanBudget(timeout_seconds=3))
        self.assertEqual(2, len(self.requests))

    def test_six_rechecks_cap_applies_across_separate_pages(self):
        self.rows = [{"id": str(i), "order_num": f"private-order-{i}"} for i in range(1, 301)]
        seen = set()
        def alter(variables, result):
            key = (variables["params"]["sort"], variables["params"]["cursor"])
            if key[0] == "ASC" and key not in seen:
                seen.add(key)
                self.contradict(result)
        self.alter = alter
        with self.assertRaises(PaginationMetadataError) as raised:
            self.scan()
        self.assertEqual(6, len(self.waits))
        self.assertEqual(6, raised.exception.diagnostics["rechecks_used"])

    def test_malformed_identity_empty_page_and_invalid_cursor_do_not_retry(self):
        for case in ("identity", "empty", "cursor"):
            with self.subTest(case=case):
                self.requests.clear()
                self.waits.clear()
                def alter(variables, result):
                    if variables["params"]["sort"] != "ASC":
                        return
                    page = result["getOrderList"]
                    page["pageInfo"]["totalRecords"] = 1
                    if case == "identity":
                        page["data"][0]["order_num"] = ""
                    elif case == "empty":
                        page["data"] = []
                    else:
                        page["pageInfo"]["nextCursor"] = "private-token-sentinel"
                self.alter = alter
                with self.assertRaises(RuntimeError) as raised:
                    self.scan()
                self.assertEqual(2, len(self.requests))
                self.assertEqual([], self.waits)
                self.assertNotIn("private-token-sentinel", str(raised.exception))

    def test_retry_configuration_is_strict_and_bounded(self):
        for value in (True, -1, 3, "2", None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.scan(retries=value)
        self.assertEqual([], self.requests)


if __name__ == "__main__":
    unittest.main()
