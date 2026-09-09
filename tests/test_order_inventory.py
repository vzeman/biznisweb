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


if __name__ == "__main__":
    unittest.main()
