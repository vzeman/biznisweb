from datetime import datetime, timezone
import unittest

from scripts.seed_invoice_backfill import seed_state


class BackfillSeedTests(unittest.TestCase):
    def test_seed_holds_email_without_overwriting_prior_operations(self):
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        state = {"schema_version": 1, "project": "roy", "lease": None,
                 "orders": {"ORDER-B": {"phase": "create_ambiguous"}}, "last_scan": {}}
        audit = {"project": "roy", "observed_at": now.isoformat(), "complete": True,
                 "candidates": [{"order_num": number, "blocked": False, "invoices": [],
                                 "older_than_7_days": True} for number in ("ORDER-A", "ORDER-B")]}
        result, counts = seed_state(state, audit, "roy", now)
        self.assertEqual(counts["seeded_historical_orders"], 1)
        self.assertEqual(result["orders"]["ORDER-A"]["email_policy"], "hold")
        self.assertEqual(result["orders"]["ORDER-B"], state["orders"]["ORDER-B"])
        self.assertNotIn("ORDER-A", state["orders"])
        again, counts = seed_state(result, audit, "roy", now)
        self.assertEqual(again, result)
        self.assertEqual(counts["seeded_historical_orders"], 0)

    def test_incomplete_or_cross_project_audit_cannot_seed(self):
        for audit in ({"project": "roy", "complete": False}, {"project": "vevo", "complete": True}):
            with self.assertRaises(RuntimeError):
                seed_state({}, audit, "roy", datetime.now(timezone.utc))


if __name__ == "__main__":
    unittest.main()
