from datetime import datetime, timezone
import json
import unittest
from unittest.mock import Mock

from scripts.audit_order_automation import scan, summarize


def row(number="ORDER-A", **extra):
    return {"order_num": number, "pur_date": "2020-01-01 00:00:00", "blocked": False,
            "status": {"id": "4"}, "invoices": [], "sum": {"value": 10}, **extra}


class ReadOnlyAuditTests(unittest.TestCase):
    def test_all_age_includes_old_eligible_only(self):
        rows = [row(), row("ORDER-B", invoices=[{"id": "I-1"}]),
                row("ORDER-C", blocked=True), row("ORDER-D", sum={"value": 0})]
        counts, candidates = summarize(rows, datetime(2026, 1, 1, tzinfo=timezone.utc))
        self.assertEqual(counts["older_than_7_days"], 1)
        self.assertEqual([r["order_num"] for r in candidates], ["ORDER-A"])

    def test_invalid_total_cannot_enter_backlog(self):
        with self.assertRaises(RuntimeError):
            summarize([row(sum={"value": "NaN"})], datetime.now(timezone.utc))

    def test_regressing_cursor_rejects_partial_scan(self):
        client = Mock()
        payloads = [{"data": {"getOrderList": {"data": [row()], "pageInfo": {
            "hasNextPage": True, "nextCursor": 30, "totalPages": 3}}}},
                    {"data": {"getOrderList": {"data": [row("ORDER-B")], "pageInfo": {
            "hasNextPage": True, "nextCursor": 0, "totalPages": 3}}}}]
        client.post.side_effect = [Mock(status_code=200, content=json.dumps(payload).encode()) for payload in payloads]
        with self.assertRaises(RuntimeError):
            scan(client, "https://example.invalid", "test-token", 4, delay=0)
        self.assertEqual(client.post.call_count, 2)


if __name__ == "__main__":
    unittest.main()
