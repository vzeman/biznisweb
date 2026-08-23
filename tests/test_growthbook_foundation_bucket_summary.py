from __future__ import annotations

import copy
import unittest

from scripts.summarize_growthbook_foundation_bucket import (
    FoundationBucketSummaryError,
    summarize_bucket_listing,
)


class GrowthBookFoundationBucketSummaryTests(unittest.TestCase):
    def test_classifies_only_safe_prefix_categories(self) -> None:
        result = summarize_bucket_listing(
            {
                "IsTruncated": False,
                "KeyCount": 3,
                "Contents": [
                    {"Key": "experiment-events/raw/2026/08/23/private-id.json"},
                    {"Key": "athena-results/query-id.csv"},
                    {"Key": "other/private-name"},
                ],
            }
        )
        self.assertEqual(
            {
                "total": 3,
                "raw_events": 1,
                "athena_results": 1,
                "unexpected": 1,
            },
            result,
        )
        self.assertNotIn("private", str(result))

    def test_accepts_exact_empty_listing(self) -> None:
        self.assertEqual(
            {"total": 0, "raw_events": 0, "athena_results": 0, "unexpected": 0},
            summarize_bucket_listing(
                {"IsTruncated": False, "KeyCount": 0, "Contents": []}
            ),
        )

    def test_rejects_truncation_or_count_mismatch(self) -> None:
        for payload, message in (
            (
                {"IsTruncated": True, "KeyCount": 0, "Contents": []},
                "truncated",
            ),
            (
                {"IsTruncated": False, "KeyCount": 1, "Contents": []},
                "count mismatch",
            ),
        ):
            with self.subTest(message=message):
                with self.assertRaisesRegex(FoundationBucketSummaryError, message):
                    summarize_bucket_listing(payload)

    def test_rejects_boolean_count_and_ambiguous_folder_markers(self) -> None:
        boolean_count = {"IsTruncated": False, "KeyCount": True, "Contents": []}
        with self.assertRaisesRegex(FoundationBucketSummaryError, "key count"):
            summarize_bucket_listing(boolean_count)

        payload = {
            "IsTruncated": False,
            "KeyCount": 2,
            "Contents": [
                {"Key": "experiment-events/raw/"},
                {"Key": "athena-results/"},
            ],
        }
        self.assertEqual(2, summarize_bucket_listing(payload)["unexpected"])

    def test_does_not_modify_input(self) -> None:
        payload = {
            "IsTruncated": False,
            "KeyCount": 1,
            "Contents": [{"Key": "athena-results/query.csv"}],
        }
        before = copy.deepcopy(payload)
        summarize_bucket_listing(payload)
        self.assertEqual(before, payload)


if __name__ == "__main__":
    unittest.main()
