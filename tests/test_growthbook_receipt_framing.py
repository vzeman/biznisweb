"""Synthetic characterization of the PRE-FIX emitter; no network or server.

This proves a possible interleaving, not a deployed cause or historical proof.
When a separately reviewed emitter fix lands, replace the pre-fix race test
with a prevention regression; do not keep its barrier inside a logging lock.
"""
from __future__ import annotations

import contextlib
import io
import json
import threading
import unittest
from unittest.mock import patch

from growthbook_collector import handler
from scripts.summarize_growthbook_receipts import ReceiptSummaryError, summarize_receipts


class RecordingStream:
    def __init__(self, *, interleave_bodies=False, fail=None):
        self.fragments = []
        self.flush_count = 0
        self.fail = fail
        self.errors = []
        self.guard = threading.Lock()
        self.bodies = threading.Barrier(2) if interleave_bodies else None

    def write(self, text):
        if self.fail == "write":
            raise OSError("synthetic writer failure")
        with self.guard:
            self.fragments.append(text)
        if self.bodies is not None and text.startswith("{"):
            try:
                self.bodies.wait(timeout=3)
            except threading.BrokenBarrierError:
                with self.guard:
                    self.errors.append("synthetic barrier did not complete")
                raise
        return len(text)

    def flush(self):
        if self.fail == "flush":
            raise OSError("synthetic flush failure")
        with self.guard:
            self.flush_count += 1


def synthetic_payload(messages):
    return {"events": [dict(eventId=f"synthetic-{i}", timestamp=1787443200000 + i,
                            message=message, logStreamName="synthetic-only")
                       for i, message in enumerate(messages)]}


def reduce_synthetic(messages):
    return summarize_receipts(synthetic_payload(messages), from_utc="2026-08-23T00:00:00Z",
                              through_utc="2026-08-24T00:00:00Z")


class GrowthBookReceiptFramingTests(unittest.TestCase):
    def run_emitters(self, stream, flags, *, serialize=False):
        """Join every test-owned thread even if an assertion/emitter fails."""
        begin = threading.Barrier(len(flags) + 1)
        emission_lock = threading.Lock()
        errors = []
        def worker(flag):
            try:
                begin.wait(timeout=3)
                # Test-only prevention candidate, NOT the runtime implementation.
                # Uses the actual emitter unchanged, preserving its error policy.
                with emission_lock if serialize else contextlib.nullcontext():
                    handler._emit_receipt_marker(flag)
            except BaseException as error:
                errors.append(type(error).__name__)
        threads = [threading.Thread(target=worker, args=(flag,), name=f"receipt-fixture-{i}")
                   for i, flag in enumerate(flags)]
        with patch("sys.stdout", stream):
            try:
                for thread in threads:
                    thread.start()
                begin.wait(timeout=3)
                for thread in threads:
                    thread.join(timeout=4)
            finally:
                begin.abort()
                if stream.bodies is not None:
                    stream.bodies.abort()
                for thread in threads:
                    if thread.ident is not None:
                        thread.join(timeout=4)
        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual([], errors)
        self.assertEqual([], stream.errors)

    def test_actual_pre_fix_emitter_writes_body_then_newline_then_flush(self):
        stream = RecordingStream()
        with patch("sys.stdout", stream):
            handler._emit_receipt_marker(False)
        self.assertEqual(2, len(stream.fragments))
        self.assertEqual("\n", stream.fragments[1])
        self.assertEqual(1, stream.flush_count)
        marker = json.loads(stream.fragments[0])
        self.assertEqual({"accepted": True, "duplicate": False,
                          "marker": handler.RECEIPT_MARKER, "schema_version": 1}, marker)

    def test_actual_pre_fix_emitter_can_reproduce_rejected_concatenated_shape(self):
        stream = RecordingStream(interleave_bodies=True)
        self.run_emitters(stream, [False, True])
        self.assertEqual(4, len(stream.fragments))
        self.assertTrue(all(fragment.startswith("{") for fragment in stream.fragments[:2]))
        self.assertEqual(["\n", "\n"], stream.fragments[2:])
        self.assertEqual(2, stream.flush_count)
        message = "".join(stream.fragments).strip()
        with self.assertRaises(ReceiptSummaryError) as caught:
            reduce_synthetic([message])
        self.assertEqual("receipt-json-concatenated-markers", caught.exception.safe_code)

    def test_test_only_full_transaction_serialization_preserves_exact_frames(self):
        stream = RecordingStream()
        flags = [False, True] * 8
        self.run_emitters(stream, flags, serialize=True)
        self.assertEqual(len(flags), stream.flush_count)
        self.assertEqual(2 * len(flags), len(stream.fragments))
        for i in range(0, len(stream.fragments), 2):
            marker = json.loads(stream.fragments[i])
            self.assertEqual(json.dumps(marker, separators=(",", ":"), sort_keys=True), stream.fragments[i])
            self.assertEqual("\n", stream.fragments[i + 1])
        lines = "".join(stream.fragments).splitlines()
        self.assertEqual(len(flags), len(lines))
        summary = reduce_synthetic(lines)
        self.assertEqual(len(flags), summary["collector_received_event_count"])
        self.assertEqual(8, summary["collector_unique_accepted_event_count"])

    def test_test_only_lock_releases_after_contained_write_and_flush_failures(self):
        # The emitter catches logging errors by design; this diagnostic must not
        # make a persisted event retry just because logging fails.
        for failure in ("write", "flush"):
            with self.subTest(failure=failure):
                lock = threading.Lock()
                with lock, patch("sys.stdout", RecordingStream(fail=failure)):
                    self.assertIsNone(handler._emit_receipt_marker(False))
                self.assertTrue(lock.acquire(blocking=False))
                lock.release()
                stream = RecordingStream()
                with lock, patch("sys.stdout", stream):
                    self.assertIsNone(handler._emit_receipt_marker(True))
                self.assertEqual(1, stream.flush_count)
                self.assertTrue(json.loads(stream.fragments[0])["duplicate"])

    def test_reproduction_never_exports_its_synthetic_receipt_messages(self):
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            stream = RecordingStream(interleave_bodies=True)
            self.run_emitters(stream, [False, True])
        self.assertEqual("", stdout.getvalue())
        self.assertEqual("", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
