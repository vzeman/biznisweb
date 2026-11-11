import unittest
from copy import deepcopy

from scripts.diagnose_reporting_inventory_pages import (
    DiagnosticError, ProviderReader, audit_direction, summary,
)


class DiagnosticAuditTests(unittest.TestCase):
    def setUp(self):
        self.rows = [{"id": str(i), "order_num": f"private-{i}"} for i in range(1, 60)]
        self.calls = []
        self.mutate = lambda params, value: None

    def read(self, params):
        self.calls.append(deepcopy(params))
        rows = sorted(deepcopy(self.rows), key=lambda row: int(row["id"]), reverse=params["sort"] == "DESC")
        start, limit = params["cursor"], params["limit"]
        total = len(rows) + 1  # Observed provider overcount.
        value = {"data": rows[start:start + limit], "pageInfo": {
            "hasNextPage": start + limit < total,
            "nextCursor": start + limit if start + limit < total else None,
            "totalRecords": total,
        }}
        self.mutate(params, value)
        return value

    def test_both_directions_keep_all_identities_and_expose_metadata_anomaly(self):
        pages = []
        ascending = audit_direction(self.read, "ASC", pages)
        descending = audit_direction(self.read, "DESC", pages)
        result = summary(ascending, descending, pages)
        self.assertTrue(result["identity_sequences_match"])
        self.assertEqual(59, result["ascending_count"])
        self.assertEqual(60, result["reported_total_min"])
        self.assertTrue(result["metadata_anomalies"])
        self.assertFalse(result["publication_authorized"])
        self.assertNotIn("private-", str(result))
        self.assertEqual(result["ascending_identity_sha256"], result["descending_reversed_identity_sha256"])

    def test_one_hidden_identity_in_opposite_scan_is_detected(self):
        ascending = audit_direction(self.read, "ASC", [])
        self.rows.pop(20)
        descending = audit_direction(self.read, "DESC", [])
        self.assertFalse(summary(ascending, descending, [])["identity_sequences_match"])

    def test_changed_overlap_cannot_finish(self):
        def mutate(params, value):
            if params["cursor"]:
                value["data"][0]["order_num"] = "changed-private-number"
        self.mutate = mutate
        with self.assertRaisesRegex(DiagnosticError, "^overlap_changed$"):
            audit_direction(self.read, "ASC", [])

    def test_duplicate_or_unsorted_rows_are_rejected(self):
        self.rows[2]["id"] = self.rows[1]["id"]
        with self.assertRaisesRegex(DiagnosticError, "^unordered_page$"):
            audit_direction(self.read, "ASC", [])

    def test_malformed_metadata_fails_without_echoing_private_text(self):
        self.mutate = lambda params, value: value["pageInfo"].update(totalRecords="secret-token")
        with self.assertRaisesRegex(DiagnosticError, "^invalid_metadata$"):
            audit_direction(self.read, "ASC", [])

    def test_empty_inventory_is_only_a_diagnostic_observation(self):
        self.rows = []
        pages = []
        result = summary(audit_direction(self.read, "ASC", pages),
                         audit_direction(self.read, "DESC", pages), pages)
        self.assertTrue(result["identity_sequences_match"])
        self.assertTrue(result["diagnostic_only"])
        self.assertFalse(result["publication_authorized"])
        self.assertEqual(0, result["ascending_count"])


class Response:
    def __init__(self, value=None, status=200, location=None):
        self.status_code = status
        self.headers = {"Location": location} if location else {}
        self.content = b"{}"
        self.value = value or {"data": {"getOrderList": {"data": [], "pageInfo": {}}}}

    def json(self):
        return self.value

    def close(self):
        pass


class FakeSession:
    def __init__(self, responses):
        self.responses, self.calls = list(responses), []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.responses.pop(0)


class DiagnosticReaderTests(unittest.TestCase):
    def reader(self, responses, *, maximum=3, seconds=1200):
        self.now, self.sleeps = 0.0, []
        def sleep(delay):
            self.sleeps.append(delay)
            self.now += delay
        self.session = FakeSession(responses)
        return ProviderReader("https://vevo.sk/api", "private-token", timeout_seconds=seconds,
                              max_pages=maximum, session=self.session, clock=lambda: self.now, sleep=sleep)

    def test_redirect_stays_https_allowlisted_and_keeps_post_body(self):
        reader = self.reader([Response(status=308, location="https://www.vevo.sk/api/"), Response()])
        reader({"cursor": 0, "limit": 30})
        self.assertEqual(2, len(self.session.calls))
        self.assertEqual(self.session.calls[0][1]["json"], self.session.calls[1][1]["json"])
        self.assertGreaterEqual(self.sleeps[0], .5)

    def test_untrusted_redirect_never_receives_credentials(self):
        reader = self.reader([Response(status=308, location="https://attacker.invalid/")])
        with self.assertRaisesRegex(DiagnosticError, "^untrusted_api_url$"):
            reader({"cursor": 0})
        self.assertEqual(1, len(self.session.calls))

    def test_page_budget_is_shared_across_directions(self):
        reader = self.reader([Response()], maximum=1)
        reader({"sort": "ASC"})
        with self.assertRaisesRegex(DiagnosticError, "^page_budget_exhausted$"):
            reader({"sort": "DESC"})
        self.assertEqual(1, len(self.session.calls))

    def test_deadline_does_not_restart_for_next_page(self):
        reader = self.reader([Response()], seconds=1)
        reader({"sort": "ASC"})
        self.now = 1.0
        with self.assertRaisesRegex(DiagnosticError, "^time_budget_exhausted$"):
            reader({"sort": "DESC"})
        self.assertEqual(1, len(self.session.calls))

    def test_hundred_request_pause_is_enforced(self):
        reader = self.reader([Response()])
        reader.requests, reader.last_request = 100, 0
        reader({"cursor": 0})
        self.assertEqual([5.0], self.sleeps)


if __name__ == "__main__":
    unittest.main()
