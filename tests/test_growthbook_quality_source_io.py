from __future__ import annotations

import ast
import copy
import hashlib
import io
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stderr, redirect_stdout
import traceback
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

from reporting_core.experiment_quality_source_io import (
    QualityInputError,
    RAW_SOURCE_PHASES,
    RECEIPTED_ORDER_QUERY,
    read_receipted_order_source,
    read_stable_retained_raw_source,
)
from reporting_core.experiments import ExperimentReceiptWindow, order_completion_receipts
from scripts.build_growthbook_aa_quality_source import build_quality_source
from tests.test_growthbook_aa_quality_source import CONFIG, END, START
from tests.test_growthbook_pipeline import event


FLOOR = START.replace(hour=0) - timedelta(days=1)


class MemoryS3:
    def __init__(self, rows=(), *, page_size=1000):
        self.objects = {}
        for i, row in enumerate(rows):
            key = f"experiment-events/raw/event_date={row['event_date']}/event-{i}.json"
            self.objects[key] = json.dumps(row).encode()
        self.page_size = page_size
        self.list_calls = []
        self.get_calls = []
        self.bodies = []
        self.list_hook = lambda page: page
        self.get_hook = lambda response: response

    def metadata(self, key):
        data = self.objects[key]
        return {"Key": key, "Size": len(data),
                "ETag": '"' + hashlib.md5(data, usedforsecurity=False).hexdigest() + '"',
                "LastModified": END + timedelta(hours=1)}

    def list_objects_v2(self, **request):
        self.list_calls.append(request)
        keys = sorted(k for k in self.objects if k.startswith(request["Prefix"]))
        index = int(request.get("ContinuationToken", 0))
        truncated = index + self.page_size < len(keys)
        result = {"Contents": [self.metadata(k) for k in keys[index:index + self.page_size]],
                  "IsTruncated": truncated}
        if truncated:
            result["NextContinuationToken"] = str(index + self.page_size)
        return self.list_hook(result)

    def get_object(self, **request):
        self.get_calls.append(request)
        metadata = self.metadata(request["Key"])
        if metadata["ETag"] != request["IfMatch"]:
            raise RuntimeError("SENSITIVE raw key precondition failed")
        body = io.BytesIO(self.objects[request["Key"]])
        self.bodies.append(body)
        return self.get_hook({"ETag": metadata["ETag"], "ContentLength": metadata["Size"],
                              "LastModified": metadata["LastModified"], "Body": body})


def read_raw(s3, **kwargs):
    args = dict(bucket="vevo-growthbook-test", context_from_utc=FLOOR, through_utc=END)
    args.update(kwargs)
    return read_stable_retained_raw_source(s3, **args)


def source_order(number="test-order"):
    money = {"value": 20.0, "is_net_price": True, "currency": {"code": "EUR"}}
    return {
        "order_num": number, "status": {"id": 1, "name": "Paid"},
        "price_elements": [],
        "items": [{"item_label": "Synthetic", "ean": "test-ean", "import_code": None,
                   "warehouse_number": None, "quantity": 1, "tax_rate": 23,
                   "price": copy.deepcopy(money), "sum": copy.deepcopy(money),
                   "sum_with_tax": copy.deepcopy(money)}],
        "sum": copy.deepcopy(money),
    }


class RawCoverageTests(unittest.TestCase):
    def test_bounded_concurrent_reads_overlap_but_reduce_in_serial_order(self):
        rows = [event(received_at=START + timedelta(minutes=i)) for i in range(12)]
        baseline = read_raw(MemoryS3(rows, page_size=2))
        memory = MemoryS3(rows, page_size=2)
        first_keys = sorted(memory.objects)[:3]
        barrier, later_closed = threading.Barrier(3), threading.Event()
        lock, closed, workers, pools = threading.Lock(), set(), set(), []
        owner = threading.current_thread()
        phases, stdout, stderr = [], io.StringIO(), io.StringIO()

        class TrackedExecutor(ThreadPoolExecutor):
            def __init__(self, *, max_workers):
                super().__init__(max_workers=max_workers)
                self.limit, self.outstanding, self.peak = max_workers, 0, 0
                pools.append(self)

            def submit(self, fn, *args, **kwargs):
                self.outstanding += 1
                self.peak = max(self.peak, self.outstanding)
                if self.outstanding > self.limit:
                    raise AssertionError('unbounded future submission')
                future = super().submit(fn, *args, **kwargs)
                original_result = future.result

                def result(*args, **kwargs):
                    try:
                        return original_result(*args, **kwargs)
                    finally:
                        self.outstanding -= 1

                future.result = result
                return future

        class Body(io.BytesIO):
            def __init__(self, key):
                super().__init__(memory.objects[key])
                self.key = key

            def read(self, size):
                with lock:
                    workers.add(threading.current_thread())
                if self.key in first_keys:
                    barrier.wait(timeout=5)
                    if self.key == first_keys[0] and not later_closed.wait(5):
                        raise AssertionError('later reads did not finish concurrently')
                return super().read(size)

            def close(self):
                super().close()
                with lock:
                    closed.add(self.key)
                    if set(first_keys[1:]) <= closed:
                        later_closed.set()

        original_get = memory.get_object

        def get(**request):
            response = original_get(**request)
            response['Body'].close()
            response['Body'] = Body(request['Key'])
            memory.bodies.append(response['Body'])
            return response

        memory.get_object = get

        def progress(phase):
            self.assertIs(owner, threading.current_thread())
            phases.append(phase)
            if phase in RAW_SOURCE_PHASES[2:]:
                self.assertTrue(all(body.closed for body in memory.bodies))
                self.assertTrue(all(not worker.is_alive() for worker in workers))

        with patch('reporting_core.experiment_quality_source_io.ThreadPoolExecutor', TrackedExecutor), \
             redirect_stdout(stdout), redirect_stderr(stderr):
            result = read_raw(memory, max_read_workers=3, progress=progress)
        self.assertEqual(baseline, result)
        self.assertEqual(list(RAW_SOURCE_PHASES), phases)
        self.assertEqual(3, pools[0].peak)
        self.assertEqual(0, pools[0].outstanding)
        self.assertEqual(3, len(workers))
        self.assertNotIn(owner, workers)
        self.assertTrue(later_closed.is_set())
        self.assertEqual(set(memory.objects), closed)
        self.assertEqual('', stdout.getvalue() + stderr.getvalue())
        self.assertEqual(sorted(memory.objects), sorted(call['Key'] for call in memory.get_calls))
        self.assertTrue(all(call['IfMatch'] == memory.metadata(call['Key'])['ETag']
                            for call in memory.get_calls))

    def test_concurrent_failure_cancels_queued_and_drains_running_reads(self):
        memory = MemoryS3([event(received_at=START + timedelta(minutes=i)) for i in range(8)])
        started, second_started, cancelled = threading.Event(), threading.Event(), threading.Event()
        phases, futures, workers = [], [], set()
        keys = sorted(memory.objects)
        original_get = memory.get_object

        # One real worker makes queued cancellation deterministic. The source
        # still submits its bounded three-future window and must cancel it on
        # failure before waiting for the already-started second body to close.
        class QueuedExecutor(ThreadPoolExecutor):
            def __init__(self, *, max_workers):
                if max_workers != 3:
                    raise AssertionError('unexpected worker limit')
                super().__init__(max_workers=1)

            def submit(self, fn, *args, **kwargs):
                future = super().submit(fn, *args, **kwargs)
                futures.append(future)
                if len(futures) == 1:
                    original_result = future.result

                    def result(*args, **kwargs):
                        if not second_started.wait(5):
                            raise AssertionError('second read did not start before failure observation')
                        return original_result(*args, **kwargs)

                    future.result = result
                if len(futures) == 3:
                    started.set()
                original_cancel = future.cancel

                def cancel():
                    result = original_cancel()
                    if future is futures[2] and result:
                        cancelled.set()
                    return result

                future.cancel = cancel
                return future

        class FailingBody(io.BytesIO):
            def read(self, size):
                if not started.wait(5):
                    raise AssertionError('bounded work was not submitted')
                raise RuntimeError('SENSITIVE SDK payload')

        class WaitingBody(io.BytesIO):
            def read(self, size):
                second_started.set()
                if not cancelled.wait(5):
                    raise AssertionError('pending work was not cancelled before drain')
                return super().read(size)

        def get(**request):
            workers.add(threading.current_thread())
            response = original_get(**request)
            response['Body'].close()
            body_type = FailingBody if request['Key'] == keys[0] else WaitingBody
            response['Body'] = body_type(memory.objects[request['Key']])
            memory.bodies.append(response['Body'])
            return response

        memory.get_object = get
        with patch('reporting_core.experiment_quality_source_io.ThreadPoolExecutor', QueuedExecutor):
            with self.assertRaises(QualityInputError) as caught:
                read_raw(memory, max_read_workers=3, progress=phases.append)
        self.assertEqual(list(RAW_SOURCE_PHASES[:2]), phases)
        self.assertEqual(3, len(futures))
        self.assertTrue(second_started.is_set())
        self.assertTrue(cancelled.is_set())
        self.assertEqual(keys[:2], [call['Key'] for call in memory.get_calls])
        self.assertTrue(all(future.done() for future in futures))
        self.assertTrue(any(future.cancelled() for future in futures))
        self.assertTrue(all(body.closed for body in memory.bodies))
        self.assertTrue(all(not worker.is_alive() for worker in workers))
        self.assertIsNone(caught.exception.__cause__)
        self.assertNotIn('SENSITIVE', ''.join(traceback.format_exception(caught.exception)))

    def test_worker_bounds_fail_before_io_and_serial_empty_paths_create_no_pool(self):
        for value in (None, True, False, 0, -1, 9, 100000, 1.0, '8'):
            phases, memory = [], MemoryS3()
            with self.subTest(value=value), self.assertRaises(QualityInputError):
                read_raw(memory, max_read_workers=value, progress=phases.append)
            self.assertEqual([], phases)
            self.assertEqual([], memory.list_calls)
        with patch('reporting_core.experiment_quality_source_io.ThreadPoolExecutor') as pool:
            read_raw(MemoryS3([event(received_at=START)]))
            read_raw(MemoryS3(), max_read_workers=8)
            pool.assert_not_called()

    def test_submission_error_drains_prior_worker_before_sanitized_return(self):
        memory = MemoryS3([event(received_at=START + timedelta(minutes=i)) for i in range(4)])
        release, entered = threading.Event(), threading.Event()
        phases, workers, futures = [], [], []
        original_get = memory.get_object

        class FailingExecutor(ThreadPoolExecutor):
            def submit(self, fn, *args, **kwargs):
                if futures:
                    if not entered.wait(5):
                        raise AssertionError('first worker never entered')
                    release.set()
                    raise RuntimeError('SENSITIVE submission value')
                future = super().submit(fn, *args, **kwargs)
                futures.append(future)
                return future

        def get(**request):
            workers.append(threading.current_thread())
            entered.set()
            if not release.wait(5):
                raise AssertionError('submission did not fail')
            return original_get(**request)

        memory.get_object = get
        with patch('reporting_core.experiment_quality_source_io.ThreadPoolExecutor', FailingExecutor):
            with self.assertRaises(QualityInputError) as caught:
                read_raw(memory, max_read_workers=3, progress=phases.append)
        self.assertEqual(list(RAW_SOURCE_PHASES[:2]), phases)
        self.assertEqual(1, len(futures))
        self.assertTrue(futures[0].done())
        self.assertTrue(all(body.closed for body in memory.bodies))
        self.assertTrue(all(not worker.is_alive() for worker in workers))
        self.assertNotIn('SENSITIVE', ''.join(traceback.format_exception(caught.exception)))

    def test_all_worker_limits_preserve_proof_and_strict_failure_checks(self):
        for limit in range(2, 9):
            rows = [event(received_at=START), event(received_at=END)]
            with self.subTest(limit=limit):
                self.assertEqual(read_raw(MemoryS3(rows)), read_raw(MemoryS3(rows), max_read_workers=limit))
                for failure in ('metadata', 'json', 'partition', 'validation', 'inventory'):
                    memory = MemoryS3(rows)
                    if failure == 'metadata':
                        memory.get_hook = lambda response: {**response, 'ETag': '"' + 'f' * 32 + '"'}
                    elif failure == 'json':
                        key = next(iter(memory.objects))
                        memory.objects[key] = b'{"received_at":"bad", "received_at": NaN}'
                    elif failure == 'partition':
                        memory = MemoryS3([event(received_at=START, event_date='2026-08-24')])
                    elif failure == 'validation':
                        memory = MemoryS3([event(received_at=END, email='synthetic@example.invalid')])

                    def progress(phase):
                        if failure == 'inventory' and phase == RAW_SOURCE_PHASES[3]:
                            memory.objects.clear()

                    with self.subTest(failure=failure), self.assertRaises(QualityInputError):
                        read_raw(memory, max_read_workers=limit, progress=progress)
                    self.assertTrue(all(body.closed for body in memory.bodies))

    def test_fixed_substeps_preserve_io_rows_proof_and_default_silence(self):
        for rows in ([], [event(received_at=START + timedelta(minutes=i)) for i in range(3)]):
            baseline, observed = MemoryS3(rows, page_size=1), MemoryS3(rows, page_size=1)
            stdout, stderr, phases = io.StringIO(), io.StringIO(), []
            with redirect_stdout(stdout), redirect_stderr(stderr):
                original = read_raw(baseline)
                result = read_raw(observed, progress=phases.append)
            self.assertEqual(list(RAW_SOURCE_PHASES), phases)
            self.assertEqual(original, result)
            self.assertEqual(baseline.list_calls, observed.list_calls)
            self.assertEqual(baseline.get_calls, observed.get_calls)
            self.assertTrue(all(body.closed for body in baseline.bodies + observed.bodies))
            self.assertEqual('', stdout.getvalue() + stderr.getvalue())

    def test_substeps_identify_real_read_and_validation_failures_safely(self):
        for target in RAW_SOURCE_PHASES:
            with self.subTest(target=target):
                s3, phases = MemoryS3([event(received_at=START)]), []
                original_validate = order_completion_receipts

                def fail_if_current(value):
                    if phases[-1] == target:
                        if isinstance(value, dict) and 'Body' in value:
                            value['Body'].close()
                        raise RuntimeError('SENSITIVE source or SDK value')
                    return value

                s3.list_hook = fail_if_current
                s3.get_hook = fail_if_current

                def validate(rows):
                    fail_if_current(None)
                    return original_validate(rows)

                with patch('reporting_core.experiment_quality_source_io.order_completion_receipts',
                           side_effect=validate):
                    with self.assertRaises(QualityInputError) as caught:
                        read_raw(s3, progress=phases.append)
                self.assertEqual(list(RAW_SOURCE_PHASES[:RAW_SOURCE_PHASES.index(target) + 1]), phases)
                self.assertNotIn('SENSITIVE', ''.join(traceback.format_exception(caught.exception)))
                self.assertIsNone(caught.exception.__cause__)
                self.assertTrue(all(body.closed for body in s3.bodies))

    def test_invalid_bounds_emit_no_substep_and_callback_failure_stays_sanitized(self):
        phases, s3 = [], MemoryS3()
        with self.assertRaises(QualityInputError):
            read_raw(s3, context_from_utc=START, progress=phases.append)
        self.assertEqual([], phases)
        self.assertEqual([], s3.list_calls)
        with self.assertRaises(QualityInputError) as caught:
            read_raw(s3, progress=Mock(side_effect=RuntimeError('SENSITIVE diagnostic injection')))
        self.assertEqual([], s3.list_calls)
        self.assertNotIn('SENSITIVE', ''.join(traceback.format_exception(caught.exception)))

    def test_reads_all_pages_and_empty_days_twice_with_conditional_gets(self):
        rows = [event(received_at=START + timedelta(minutes=i)) for i in range(3)]
        s3 = MemoryS3(rows, page_size=1)
        result = read_raw(s3)
        self.assertEqual(rows, list(result.rows))
        prefixes = [call["Prefix"] for call in s3.list_calls]
        self.assertEqual(2, prefixes.count("experiment-events/raw/event_date=2026-08-24/"))
        self.assertEqual(6, prefixes.count("experiment-events/raw/event_date=2026-08-25/"))
        self.assertEqual(2, prefixes.count("experiment-events/raw/event_date=2026-08-26/"))
        self.assertTrue(all(call["IfMatch"] for call in s3.get_calls))
        self.assertTrue(all(body.closed for body in s3.bodies))
        self.assertEqual(result.sanitized_proof["inventory_before_sha256"],
                         result.sanitized_proof["inventory_after_sha256"])
        self.assertFalse(result.sanitized_proof["historical_retention_proven"])
        self.assertFalse(result.sanitized_proof["context_floor_proven"])

    def test_midnight_through_does_not_read_the_following_partition(self):
        s3 = MemoryS3()
        read_raw(s3, through_utc=END.replace(hour=0))
        self.assertFalse(any("2026-08-26" in call["Prefix"] for call in s3.list_calls))

    def test_later_edge_is_validated_and_left_for_the_existing_window_calculator(self):
        rows = [event(received_at=START), event(received_at=END)]
        raw = read_raw(MemoryS3(rows))
        self.assertEqual(2, len(raw.rows))
        source = build_quality_source(
            raw.rows, [], config=CONFIG,
            window=ExperimentReceiptWindow(FLOOR, START, END), generated_at=END + timedelta(hours=4),
            expected_eligible_devices=1, snapshot_manifest_sha256="a" * 64,
            checkpoint_evidence_sha256="b" * 64, workflow_run_id="123456789", main_commit="c" * 40,
        )
        self.assertEqual(1, source["quality"]["raw_event_count"])
        self.assertEqual(1, source["quality"]["eligible_device_count"])

    def test_same_event_in_distinct_objects_keeps_duplicate_evidence(self):
        row = event(received_at=START)
        self.assertEqual(2, len(read_raw(MemoryS3([row, row])).rows))

    def test_invalid_bounds_fail_before_any_io(self):
        for changes in ({"context_from_utc": START}, {"through_utc": FLOOR},
                        {"context_from_utc": FLOOR.replace(tzinfo=None)},
                        {"through_utc": END.replace(microsecond=1)},
                        {"through_utc": FLOOR + timedelta(days=91)},
                        {"max_objects": True}, {"max_objects": 100001},
                        {"bucket": "bucket/escaped"}):
            with self.subTest(changes=changes):
                s3 = MemoryS3()
                with self.assertRaises(QualityInputError):
                    read_raw(s3, **changes)
                self.assertEqual([], s3.list_calls)

    def test_exact_object_limit_can_finish_but_overflow_never_partially_returns(self):
        rows = [event(received_at=START), event(received_at=START)]
        self.assertEqual(2, len(read_raw(MemoryS3(rows), max_objects=2).rows))
        s3 = MemoryS3(rows)
        with self.assertRaises(QualityInputError):
            read_raw(s3, max_objects=1)
        self.assertEqual([], s3.get_calls)

    def test_byte_limit_precedes_gets(self):
        s3 = MemoryS3([event(received_at=START)])
        with patch("reporting_core.experiment_quality_source_io._MAX_EXTRACT_BYTES", 10):
            with self.assertRaises(QualityInputError):
                read_raw(s3)
        self.assertEqual([], s3.get_calls)

    def test_missing_or_invalid_pagination_proof_fails(self):
        for patcher in (lambda page: {}, lambda page: {"IsTruncated": 0},
                        lambda page: {"IsTruncated": True, "Contents": []},
                        lambda page: {"IsTruncated": False, "NextContinuationToken": "1"}):
            with self.subTest(patcher=patcher):
                s3 = MemoryS3()
                s3.list_hook = patcher
                with self.assertRaises(QualityInputError):
                    read_raw(s3)

    def test_repeated_object_and_cyclic_pagination_fail(self):
        s3 = MemoryS3([event(received_at=START)], page_size=1)
        key = next(iter(s3.objects))
        s3.list_hook = lambda page: {"Contents": [s3.metadata(key), s3.metadata(key)],
                                   "IsTruncated": False}
        with self.assertRaises(QualityInputError):
            read_raw(s3)
        # Exercise a cycle with fresh unique keys, not just duplicate detection.
        rows = [event(received_at=FLOOR + timedelta(hours=i)) for i in range(4)]
        s3 = MemoryS3(rows)
        keys = list(s3.objects)
        index = 0

        def cycle(page):
            nonlocal index
            result = {"Contents": [s3.metadata(keys[index])], "IsTruncated": True,
                      "NextContinuationToken": ["1", "2", "1"][index]}
            index += 1
            return result

        s3.list_hook = cycle
        with self.assertRaises(QualityInputError):
            read_raw(s3)
        self.assertEqual(3, len(s3.list_calls))

    def test_partition_escape_bad_etag_size_and_timestamp_fail(self):
        for field, value in (("Key", "experiment-events/raw/event_date=2026-08-24/nested/x.json"),
                             ("Size", True), ("Size", 16385), ("ETag", None),
                             ("LastModified", datetime(2026, 8, 25))):
            with self.subTest(field=field):
                s3 = MemoryS3([event(received_at=FLOOR)])
                s3.list_hook = lambda page: {**page, "Contents": [{**page["Contents"][0], field: value}]}
                with self.assertRaises(QualityInputError):
                    read_raw(s3)
                self.assertEqual([], s3.get_calls)

    def test_read_metadata_and_body_drift_close_the_body(self):
        for field, value in (("ETag", '"' + "f" * 32 + '"'), ("ContentLength", 1),
                             ("LastModified", END), ("Body", io.BytesIO(b"{}"))):
            with self.subTest(field=field):
                s3 = MemoryS3([event(received_at=START)])
                s3.get_hook = lambda response: {**response, field: value}
                with self.assertRaises(QualityInputError):
                    read_raw(s3)
                if field == "Body":
                    self.assertTrue(value.closed)
                else:
                    self.assertTrue(all(body.closed for body in s3.bodies))

    def test_inventory_mutation_between_passes_fails(self):
        for change in ("add", "delete", "replace"):
            with self.subTest(change=change):
                s3 = MemoryS3([event(received_at=START)])
                original_get = s3.get_object

                def mutate(**request):
                    response = original_get(**request)
                    key = request["Key"]
                    if change == "add":
                        s3.objects[key.replace("event-0", "event-1")] = s3.objects[key]
                    elif change == "delete":
                        del s3.objects[key]
                    else:
                        s3.objects[key] += b" "
                    return response

                s3.get_object = mutate
                with self.assertRaises(QualityInputError):
                    read_raw(s3)

    def test_wrong_receipt_partition_and_unsafe_edge_rows_fail(self):
        for row in (event(received_at=END, email="synthetic@example.invalid"),
                    event(received_at=START, event_date="2026-08-24"),
                    event(received_at=START, received_at_override="unused")):
            with self.subTest(row=row):
                s3 = MemoryS3([row])
                with self.assertRaises(QualityInputError):
                    read_raw(s3)
                self.assertTrue(all(body.closed for body in s3.bodies))

    def test_ambiguous_json_and_nonfinite_constants_fail_and_close_body(self):
        for suffix in (b', "risk_result": "accepted"}', b', "unsafe": NaN}'):
            s3 = MemoryS3([event(received_at=START)])
            key = next(iter(s3.objects))
            s3.objects[key] = s3.objects[key][:-1] + suffix
            with self.assertRaises(QualityInputError):
                read_raw(s3)
            self.assertTrue(all(body.closed for body in s3.bodies))

    def test_proof_and_repr_never_include_row_or_object_identities(self):
        row = event(received_at=START)
        s3 = MemoryS3([row])
        source = read_raw(s3)
        safe = repr(source) + json.dumps(source.sanitized_proof)
        for identity in (row["event_id"], row["device_id"], next(iter(s3.objects))):
            self.assertNotIn(identity, safe)

    def test_sensitive_sdk_exception_is_not_exposed_or_chained(self):
        s3 = Mock()
        s3.list_objects_v2.side_effect = RuntimeError("SENSITIVE payload with identity")
        try:
            read_raw(s3)
        except QualityInputError as exc:
            self.assertIsNone(exc.__cause__)
            self.assertTrue(exc.__suppress_context__)
            self.assertNotIn("SENSITIVE", "".join(traceback.format_exception(exc)))
        else:
            self.fail("read should fail closed")


class ReceiptedOrderCoverageTests(unittest.TestCase):
    def test_queries_only_exact_validated_ids_twice_in_fixed_order(self):
        receipts = {"b": START, "a": START}
        client = Mock(side_effect=lambda query, variable_values:
                      {"getOrder": source_order(variable_values["order_num"])})
        source = read_receipted_order_source(client, completion_receipts=receipts)
        self.assertEqual(["a", "b"], [row["order_num"] for row in source.orders])
        self.assertEqual(["a", "b", "a", "b"],
                         [call.kwargs["variable_values"]["order_num"] for call in client.call_args_list])
        self.assertTrue(all(call.args == (RECEIPTED_ORDER_QUERY,) for call in client.call_args_list))
        self.assertFalse(source.sanitized_proof["atomic_historical_snapshot_proven"])

    def test_empty_receipt_set_makes_no_request(self):
        client = Mock()
        source = read_receipted_order_source(client, completion_receipts={})
        client.assert_not_called()
        self.assertEqual((), source.orders)

    def test_explicit_not_found_is_not_synthetic_or_ignored_in_digest(self):
        missing = read_receipted_order_source(lambda *a, **kw: {"getOrder": None},
                                             completion_receipts={"missing": START})
        empty = read_receipted_order_source(Mock(), completion_receipts={})
        self.assertEqual((), missing.orders)
        self.assertNotEqual(empty.sanitized_proof["responses_before_sha256"],
                            missing.sanitized_proof["responses_before_sha256"])

    def test_invalid_receipt_and_limits_fail_before_io(self):
        for receipts, maximum in (({"bad\" } mutation": START}, 10),
                                  ({"a": START.replace(tzinfo=None)}, 10),
                                  ({"a": START, "b": START}, 1), ({}, True), ({}, 2001)):
            client = Mock()
            with self.assertRaises(QualityInputError):
                read_receipted_order_source(client, completion_receipts=receipts, max_orders=maximum)
            client.assert_not_called()

    def test_errors_partial_responses_wrong_identity_and_pii_fail(self):
        pii = source_order()
        pii["customer"] = {"email": "synthetic@example.invalid"}
        nested = source_order()
        nested["items"][0]["sum"]["customer"] = "unexpected"
        for response in ({}, {"errors": ["private payload"]},
                         {"getOrder": None, "errors": ["not complete"]},
                         {"getOrder": source_order("wrong")},
                         {"getOrder": pii}, {"getOrder": nested}):
            with self.subTest(response=response):
                with self.assertRaises(QualityInputError):
                    read_receipted_order_source(lambda *a, **kw: response,
                                                completion_receipts={"test-order": START})

    def test_missing_requested_field_and_nonfinite_money_fail(self):
        missing = source_order()
        del missing["price_elements"]
        nonfinite = source_order()
        nonfinite["sum"]["value"] = float("nan")
        for order in (missing, nonfinite):
            with self.assertRaises(QualityInputError):
                read_receipted_order_source(lambda *a, **kw: {"getOrder": order},
                                            completion_receipts={"test-order": START})

    def test_response_size_is_bounded(self):
        order = source_order()
        order["items"][0]["item_label"] = "x" * (128 * 1024)
        with self.assertRaises(QualityInputError):
            read_receipted_order_source(lambda *a, **kw: {"getOrder": order},
                                        completion_receipts={"test-order": START})

    def test_second_pass_detects_deletion_creation_and_value_drift(self):
        changed = source_order()
        changed["sum"]["value"] += 1
        for first, second in ((source_order(), None), (None, source_order()),
                              (source_order(), changed)):
            with self.subTest(first=first, second=second):
                client = Mock(side_effect=[{"getOrder": first}, {"getOrder": second}])
                with self.assertRaises(QualityInputError):
                    read_receipted_order_source(client, completion_receipts={"test-order": START})
                self.assertEqual(2, client.call_count)

    def test_client_reusing_mutable_response_cannot_hide_drift(self):
        order = source_order()
        calls = 0

        def query(*args, **kwargs):
            nonlocal calls
            calls += 1
            order["sum"]["value"] = calls
            return {"getOrder": order}

        with self.assertRaises(QualityInputError):
            read_receipted_order_source(query, completion_receipts={"test-order": START})

    def test_proof_and_repr_are_identity_free_and_errors_are_sanitized(self):
        number = "sensitive-synthetic-order"
        source = read_receipted_order_source(lambda *a, **kw: {"getOrder": source_order(number)},
                                            completion_receipts={number: START})
        self.assertNotIn(number, repr(source) + json.dumps(source.sanitized_proof))
        client = Mock(side_effect=RuntimeError(number))
        try:
            read_receipted_order_source(client, completion_receipts={number: START})
        except QualityInputError as exc:
            self.assertNotIn(number, "".join(traceback.format_exception(exc)))
        else:
            self.fail("read should fail closed")

    def test_receipt_selection_excludes_later_partition_edge_before_order_reads(self):
        rows = [event("order_completed", received_at=START, transaction_id="inside"),
                event("order_completed", received_at=END, transaction_id="later")]
        raw = read_raw(MemoryS3(rows))
        receipts = order_completion_receipts(
            row for row in raw.rows
            if datetime.fromisoformat(row["received_at"].replace("Z", "+00:00")) < END
        )
        client = Mock(return_value={"getOrder": None})
        read_receipted_order_source(client, completion_receipts=receipts)
        self.assertTrue(all(call.kwargs["variable_values"] == {"order_num": "inside"}
                            for call in client.call_args_list))

    def test_fixed_query_is_read_only_and_does_not_select_personal_fields(self):
        self.assertTrue(RECEIPTED_ORDER_QUERY.startswith("query "))
        self.assertEqual(1, RECEIPTED_ORDER_QUERY.count("getOrder("))
        for forbidden in ("mutation", "getOrderList", "customer", "address", "email", "phone"):
            self.assertNotIn(forbidden, RECEIPTED_ORDER_QUERY)

    def test_query_parses_and_projects_only_existing_reporting_order_fields(self):
        from graphql import parse
        from reporting_core.experiment_quality_source_io import _ORDER_SHAPE

        # Parse source text without importing the exporter (which loads .env).
        text = (Path(__file__).resolve().parents[1] / "export_orders.py").read_text(encoding="utf-8-sig")
        assignment = next(node for node in ast.parse(text).body
                          if isinstance(node, ast.Assign)
                          and any(isinstance(target, ast.Name) and target.id == "ORDER_QUERY"
                                  for target in node.targets))
        existing = parse(assignment.value.args[0].value).definitions[0].selection_set.selections[0]
        existing_order = existing.selection_set.selections[0]  # getOrderList.data
        operation = parse(RECEIPTED_ORDER_QUERY).definitions[0]
        self.assertEqual("query", operation.operation.value)
        self.assertEqual(1, len(operation.selection_set.selections))

        def check(prior, selected, expected_shape):
            if isinstance(expected_shape, list):
                expected_shape = expected_shape[0]
            old = {node.name.value: node for node in prior.selection_set.selections}
            new = {node.name.value: node for node in selected.selection_set.selections}
            self.assertEqual(set(expected_shape), set(new))
            self.assertTrue(set(new) <= set(old))
            for name, node in new.items():
                if node.selection_set:
                    check(old[name], node, expected_shape[name])
                else:
                    self.assertIsNone(expected_shape[name])

        check(existing_order, operation.selection_set.selections[0], _ORDER_SHAPE)


if __name__ == "__main__":
    unittest.main()
