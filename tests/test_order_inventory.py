import unittest
from copy import deepcopy
from unittest.mock import patch

from order_inventory import InventoryScanBudget, scan_order_inventory


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


if __name__ == "__main__":
    unittest.main()
