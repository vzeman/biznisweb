from __future__ import annotations

import contextlib
import copy
import io
import json
import pathlib
import tempfile
import unittest

from growthbook_collector.handler import RECEIPT_MARKER
from scripts.summarize_growthbook_receipts import (
    RECEIPT_FAILURE_CODES,
    ReceiptSummaryError,
    main,
    summarize_receipts,
)


FROM_UTC = "2026-08-23T00:00:00Z"
THROUGH_UTC = "2026-08-30T00:00:00Z"
FROM_MS = 1787443200000


def receipt(duplicate: bool) -> str:
    return json.dumps(
        {
            "accepted": True,
            "duplicate": duplicate,
            "marker": RECEIPT_MARKER,
            "schema_version": 1,
        },
        separators=(",", ":"),
        sort_keys=True,
    )


def cloudwatch_payload() -> dict[str, object]:
    return {
        "events": [
            {
                "eventId": "cloudwatch-event-1",
                "timestamp": FROM_MS,
                "message": receipt(False),
                "logStreamName": "not-exported",
            },
            {
                "eventId": "cloudwatch-event-2",
                "timestamp": FROM_MS + 1000,
                "message": receipt(True),
                "logStreamName": "not-exported",
            },
            {
                "eventId": "cloudwatch-event-3",
                "timestamp": FROM_MS + 2000,
                "message": receipt(False),
                "logStreamName": "not-exported",
            },
        ],
        "searchedLogStreams": [],
    }


class GrowthBookReceiptSummaryTests(unittest.TestCase):
    def test_each_receipt_failure_has_a_fixed_identity_free_code(self):
        cases = [(None, 'receipt-export-invalid'), ({'events': None}, 'receipt-export-invalid'),
                 ({'events': [], 'nextToken': 'private-token'}, 'receipt-export-incomplete'),
                 ({'events': [None]}, 'receipt-event-invalid')]
        for field, value, code in (
            ('eventId', '', 'receipt-log-id-missing'),
            ('timestamp', True, 'receipt-timestamp-invalid'),
            ('timestamp', FROM_MS - 1, 'receipt-outside-window'),
            ('message', None, 'receipt-message-invalid'),
            ('message', 'SENSITIVE not JSON', 'receipt-json-invalid'),
            ('message', '[]', 'receipt-object-invalid'),
        ):
            payload = cloudwatch_payload()
            payload['events'][0][field] = value
            cases.append((payload, code))
        duplicated = cloudwatch_payload()
        duplicated['events'][1]['eventId'] = duplicated['events'][0]['eventId']
        cases.append((duplicated, 'receipt-log-id-duplicate'))
        for field, value, code in (
            ('unexpected', 'SENSITIVE', 'receipt-fields-drift'),
            ('schema_version', 2, 'receipt-schema-drift'),
            ('marker', 'SENSITIVE', 'receipt-marker-drift'),
            ('accepted', False, 'receipt-accepted-drift'),
            ('duplicate', 0, 'receipt-duplicate-drift'),
        ):
            payload = cloudwatch_payload()
            marker = json.loads(payload['events'][0]['message'])
            marker[field] = value
            payload['events'][0]['message'] = json.dumps(marker)
            cases.append((payload, code))
        for payload, code in cases:
            saved = copy.deepcopy(payload)
            with self.subTest(code=code), self.assertRaises(ReceiptSummaryError) as caught:
                summarize_receipts(payload, from_utc=FROM_UTC, through_utc=THROUGH_UTC)
            self.assertEqual(code, caught.exception.safe_code)
            self.assertIn(code, RECEIPT_FAILURE_CODES)
            self.assertEqual(saved, payload)

    def test_arbitrary_diagnostic_code_never_becomes_an_output_category(self):
        for invalid in ('SENSITIVE', 'receipt-json-invalid\nSENSITIVE', None, 1, {}, True):
            error = ReceiptSummaryError('SENSITIVE detailed message', safe_code=invalid)
            self.assertEqual('receipt-summary-invalid', error.safe_code)

    def test_exact_concatenated_markers_are_diagnosed_but_never_recovered_or_accepted(self):
        multiple = receipt(False) + receipt(True)
        for message, code in (
            (multiple, 'receipt-json-concatenated-markers'),
            (receipt(False) + '\n ' + receipt(True), 'receipt-json-concatenated-markers'),
            ('SENSITIVE' + multiple, 'receipt-json-invalid'),
            (multiple + 'SENSITIVE', 'receipt-json-invalid'),
            (multiple + '{', 'receipt-json-invalid'),
            (multiple.replace('"accepted":true', '"accepted":false'), 'receipt-json-invalid'),
            (receipt(False) * 65, 'receipt-json-invalid'),
            (multiple + ' ' * 8192, 'receipt-json-invalid'),
        ):
            payload = cloudwatch_payload()
            payload['events'][0]['message'] = message
            with self.subTest(code=code), self.assertRaises(ReceiptSummaryError) as caught:
                summarize_receipts(payload, from_utc=FROM_UTC, through_utc=THROUGH_UTC)
            self.assertEqual(code, caught.exception.safe_code)
            self.assertIn('not valid JSON', str(caught.exception))

    def test_reduces_raw_cloudwatch_events_to_sanitized_counts_only(self) -> None:
        summary = summarize_receipts(
            cloudwatch_payload(), from_utc=FROM_UTC, through_utc=THROUGH_UTC
        )
        self.assertEqual(3, summary["collector_received_event_count"])
        self.assertEqual(2, summary["collector_unique_accepted_event_count"])
        self.assertEqual(1, summary["collector_duplicate_event_count"])
        self.assertFalse(summary["contains_raw_log_events"])
        self.assertFalse(summary["contains_event_or_device_ids"])
        serialized = json.dumps(summary, sort_keys=True)
        for forbidden in (
            "cloudwatch-event-1",
            "logStreamName",
            '"event_id":',
            '"device_id":',
            '"transaction_id":',
        ):
            self.assertNotIn(forbidden, serialized)

    def test_rejects_extra_receipt_fields_duplicate_log_ids_and_out_of_window_events(self) -> None:
        extra = cloudwatch_payload()
        marker = json.loads(extra["events"][0]["message"])
        marker["device_id"] = "forbidden"
        extra["events"][0]["message"] = json.dumps(marker)
        with self.assertRaisesRegex(ReceiptSummaryError, "field set drift"):
            summarize_receipts(extra, from_utc=FROM_UTC, through_utc=THROUGH_UTC)

        duplicated = cloudwatch_payload()
        duplicated["events"][1]["eventId"] = duplicated["events"][0]["eventId"]
        with self.assertRaisesRegex(ReceiptSummaryError, "ID is duplicated"):
            summarize_receipts(
                duplicated, from_utc=FROM_UTC, through_utc=THROUGH_UTC
            )

        outside = cloudwatch_payload()
        outside["events"][0]["timestamp"] = FROM_MS - 1
        with self.assertRaisesRegex(ReceiptSummaryError, "outside the requested window"):
            summarize_receipts(outside, from_utc=FROM_UTC, through_utc=THROUGH_UTC)

    def test_rejects_malformed_marker_and_non_boolean_duplicate_state(self) -> None:
        malformed = cloudwatch_payload()
        malformed["events"][0]["message"] = "not-json"
        with self.assertRaisesRegex(ReceiptSummaryError, "not valid JSON"):
            summarize_receipts(
                malformed, from_utc=FROM_UTC, through_utc=THROUGH_UTC
            )

        wrong_type = cloudwatch_payload()
        marker = json.loads(wrong_type["events"][0]["message"])
        marker["duplicate"] = 0
        wrong_type["events"][0]["message"] = json.dumps(marker)
        with self.assertRaisesRegex(ReceiptSummaryError, "duplicate state drift"):
            summarize_receipts(
                wrong_type, from_utc=FROM_UTC, through_utc=THROUGH_UTC
            )

    def test_rejects_paginated_export_that_is_not_complete(self) -> None:
        paginated = cloudwatch_payload()
        paginated["nextToken"] = "more-events-exist"
        with self.assertRaisesRegex(ReceiptSummaryError, "export is incomplete"):
            summarize_receipts(
                paginated, from_utc=FROM_UTC, through_utc=THROUGH_UTC
            )

    def test_cli_writes_canonical_output_and_can_require_nonempty_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary = pathlib.Path(temporary_directory)
            input_path = temporary / "raw-cloudwatch.json"
            output_path = temporary / "summary.json"
            input_path.write_text(json.dumps(cloudwatch_payload()), encoding="utf-8")
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(
                    0,
                    main(
                        [
                            "--input",
                            str(input_path),
                            "--from-utc",
                            FROM_UTC,
                            "--through-utc",
                            THROUGH_UTC,
                            "--output",
                            str(output_path),
                            "--require-nonempty",
                        ]
                    ),
                )
            raw = output_path.read_bytes()
            self.assertTrue(raw.endswith(b"\n"))
            self.assertEqual(3, json.loads(raw)["collector_received_event_count"])

            input_path.write_text('{"events":[]}', encoding="utf-8")
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(
                    3,
                    main(
                        [
                            "--input",
                            str(input_path),
                            "--from-utc",
                            FROM_UTC,
                            "--through-utc",
                            THROUGH_UTC,
                            "--output",
                            str(output_path),
                            "--require-nonempty",
                        ]
                    ),
                )


if __name__ == "__main__":
    unittest.main()
