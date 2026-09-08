from __future__ import annotations

import copy
import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import Mock, patch

from scripts import growthbook_aa_health_log_io as log_io


TASK = "a" * 32
READ_ARGS = {
    "group": "/synthetic/reconciliation",
    "stream": f"ecs/synthetic/{TASK}",
    "task_id": TASK,
    "start_ms": 1_000,
    "end_ms": 10_000,
}


def event(message="synthetic marker", timestamp=2_000, ingestion_time=3_000):
    return {"message": message, "timestamp": timestamp, "ingestionTime": ingestion_time}


def page(token="forward-1", events=()):
    return json.dumps({"events": list(events), "nextForwardToken": token}).encode()


class CompleteHealthLogReadTests(unittest.TestCase):
    def read(self, *pages, **overrides):
        fetch = Mock(side_effect=pages)
        result = log_io.read_complete_log(fetch, **(READ_ARGS | overrides))
        return result, fetch

    def assert_stopped(self, code, *pages, **overrides):
        fetch = Mock(side_effect=pages)
        with self.assertRaisesRegex(log_io.HealthLogError, f"^{code}$"):
            log_io.read_complete_log(fetch, **(READ_ARGS | overrides))
        return fetch

    def test_empty_and_partial_pages_continue_until_explicit_terminal_token(self):
        first, second = event("first"), event("second", 4_000)
        result, fetch = self.read(
            page("a"), page("b", [first]), page("c"),
            page("d", [second]), page("d"),
        )
        self.assertEqual(result, {"events": [first, second]})
        self.assertEqual(fetch.call_count, 5)
        expected = {
            "logGroupName": READ_ARGS["group"], "logStreamName": READ_ARGS["stream"],
            "startTime": 1_000, "endTime": 10_000, "startFromHead": True,
            "limit": 10_000, "unmask": False,
        }
        for index, call in enumerate(fetch.call_args_list):
            request = copy.deepcopy(expected)
            if index:
                request["nextToken"] = ("a", "b", "c", "d")[index - 1]
            self.assertEqual(call.args, (request,))
            self.assertEqual(call.kwargs, {})

    def test_entirely_empty_stream_requires_two_calls(self):
        result, fetch = self.read(page(), page())
        self.assertEqual(result, {"events": []})
        self.assertEqual(fetch.call_count, 2)

    def test_duplicate_rows_are_preserved_in_original_api_order(self):
        repeated = event()
        result, _ = self.read(page("a", [repeated]), page("b", [repeated]), page("b"))
        self.assertEqual(result["events"], [repeated, repeated])

    def test_ingestion_time_after_fixed_read_end_is_allowed(self):
        row = event(ingestion_time=READ_ARGS["end_ms"] + 500_000)
        result, _ = self.read(page(events=[row]), page())
        self.assertEqual(result["events"], [row])

    def test_event_at_inclusive_start_and_before_exclusive_end_is_allowed(self):
        rows = [event(timestamp=1_000), event(timestamp=9_999)]
        result, _ = self.read(page(events=rows), page())
        self.assertEqual(result["events"], rows)

    def test_nonempty_terminal_page_fails_instead_of_deduplicating(self):
        self.assert_stopped("terminal-page-ambiguous", page("a", [event()]), page("a", [event()]))

    def test_token_cycle_fails_without_returning_partial_rows(self):
        fetch = self.assert_stopped("page-token-cycle", page("a"), page("b"), page("a"))
        self.assertEqual(fetch.call_count, 3)

    def test_missing_empty_oversized_and_control_character_tokens_fail(self):
        for token in (None, "", 7, True, "x" * 4097, "secret\nvalue", "\x7f"):
            with self.subTest(token_type=type(token).__name__):
                self.assert_stopped("page-token-invalid", page(token))
        self.assert_stopped("page-token-invalid", b'{"events":[]}')

    def test_invalid_duplicate_key_and_nonfinite_json_fail(self):
        for raw in (
            b"not-json", b"\xff", b'{"events":[],"events":[],"nextForwardToken":"a"}',
            b'{"events":[],"nextForwardToken":"a","unused":NaN}',
            b'{"events":[],"nextForwardToken":"a","unused":Infinity}',
            b'{"events":[],"nextForwardToken":"a","nested":{"x":1,"x":2}}',
        ):
            with self.subTest(raw=raw):
                self.assert_stopped("page-json-invalid", raw)

    def test_invalid_top_level_shape_fails(self):
        for value in (None, [], 0, "text", {}, {"events": None}, {"events": {}}):
            with self.subTest(value=value):
                self.assert_stopped("page-shape-invalid", json.dumps(value).encode())

    def test_invalid_event_metadata_fails(self):
        rows = [None, [], {}, event(message=None), event(message=7)]
        for field, values in (
            ("timestamp", (None, "2000", True, 2_000.0, 999, 10_000)),
            ("ingestionTime", (None, "3000", True, 3_000.0, -1)),
        ):
            rows.extend(event() | {field: value} for value in values)
        missing = event()
        del missing["ingestionTime"]
        rows.extend([missing, event() | {"extra": "not permitted"}])
        for row in rows:
            with self.subTest(row=row):
                self.assert_stopped("event-shape-invalid", page(events=[row]))

    def test_invalid_identities_are_rejected_before_io(self):
        overrides = (
            {"group": ""}, {"group": "a" * 513}, {"group": "wrong:group"},
            {"group": "bad\nname"}, {"group": None},
            {"task_id": "b" * 32}, {"task_id": "A" * 32}, {"task_id": 1},
            {"task_id": "a" * 31}, {"stream": TASK}, {"stream": ""},
            {"stream": f"x:bad/{TASK}"}, {"stream": f"bad*/{TASK}"},
            {"stream": f"bad\x7f/{TASK}"}, {"stream": f"bad\n/{TASK}"},
            {"stream": f"{'a' * 512}/{TASK}"}, {"stream": None},
        )
        for override in overrides:
            with self.subTest(override=override):
                fetch = self.assert_stopped("stream-identity-invalid", **override)
                fetch.assert_not_called()

    def test_invalid_time_windows_are_rejected_before_io(self):
        for start, end in ((True, 10_000), (1_000, True), (1_000.0, 10_000),
                           (-1, 1_000), (1_000, 1_000), (2_000, 1_000),
                           (0, 25 * 60 * 60 * 1_000 + 1)):
            with self.subTest(start=start, end=end):
                fetch = self.assert_stopped("read-window-invalid", start_ms=start, end_ms=end)
                fetch.assert_not_called()

    def test_non_bytes_empty_and_oversized_raw_pages_fail(self):
        with patch.object(log_io, "MAX_PAGE_BYTES", 4):
            for raw in (b"", "text", bytearray(b"abc"), b"12345", None):
                with self.subTest(raw=raw):
                    self.assert_stopped("page-byte-limit", raw)

    def test_total_byte_limit_fails_before_partial_result(self):
        first, second = page("a", [event()]), page("b")
        with patch.object(log_io, "MAX_TOTAL_BYTES", len(first) + len(second) - 1):
            fetch = self.assert_stopped("total-byte-limit", first, second)
            self.assertEqual(fetch.call_count, 2)

    def test_per_page_and_total_event_limits_fail(self):
        self.assert_stopped("event-limit", page(events=[event()] * 10_001))
        with patch.object(log_io, "MAX_EVENTS", 1):
            self.assert_stopped("event-limit", page("a", [event()]), page("b", [event()]))

    def test_page_limit_is_finite_including_empty_pages(self):
        with patch.object(log_io, "MAX_PAGES", 2):
            fetch = self.assert_stopped("page-limit", page("a"), page("b"))
        self.assertEqual(fetch.call_count, 2)

    def test_time_limit_before_io_does_not_call_fetch(self):
        with patch.object(log_io.time, "monotonic", side_effect=[0, log_io.MAX_SECONDS]):
            fetch = self.assert_stopped("read-time-limit")
        fetch.assert_not_called()

    def test_time_limit_after_io_rejects_page_without_return(self):
        with patch.object(log_io.time, "monotonic", side_effect=[0, 0, log_io.MAX_SECONDS]):
            fetch = self.assert_stopped("read-time-limit", page())
        self.assertEqual(fetch.call_count, 1)

    def test_fetch_exception_is_fixed_code_and_emits_no_raw_text(self):
        stdout, stderr = io.StringIO(), io.StringIO()
        fetch = Mock(side_effect=RuntimeError("synthetic sensitive payload"))
        with redirect_stdout(stdout), redirect_stderr(stderr):
            with self.assertRaisesRegex(log_io.HealthLogError, "^page-read-failed$") as raised:
                log_io.read_complete_log(fetch, **READ_ARGS)
        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(stderr.getvalue(), "")
        self.assertIsNone(raised.exception.__cause__)
        self.assertTrue(raised.exception.__suppress_context__)

    def test_fetch_request_mutation_does_not_change_next_page_identity(self):
        requests = []

        def fetch(request):
            requests.append(copy.deepcopy(request))
            request["logStreamName"] = "mutated by synthetic adapter"
            return page()

        self.assertEqual(log_io.read_complete_log(fetch, **READ_ARGS), {"events": []})
        self.assertEqual([row["logStreamName"] for row in requests], [READ_ARGS["stream"]] * 2)


class HealthLogTempCleanupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="vevo-health-log-test-")
        self.addCleanup(self.temp.cleanup)
        self.runner = Path(self.temp.name).resolve()
        self.run_id = "12345"
        self.root = self.runner / f"vevo-aa-infra-{self.run_id}"
        self.root.mkdir()

    def owned_file(self, name="task-logs.json"):
        path = self.root / name
        path.write_bytes(b"synthetic test-owned bytes")
        return path

    def assert_cleanup_stopped(self, code="temp-contents-invalid", **overrides):
        with self.assertRaisesRegex(log_io.HealthLogError, f"^{code}$"):
            log_io.cleanup_temp(**({"root": self.root, "runner_temp": self.runner,
                                    "run_id": self.run_id} | overrides))

    def make_symlink(self, link, target, *, directory=False):
        try:
            link.symlink_to(target, target_is_directory=directory)
        except (OSError, NotImplementedError) as error:
            self.skipTest(f"OS does not permit test-owned symlinks: {type(error).__name__}")

    def test_removes_only_regular_files_and_exact_run_directory(self):
        self.owned_file()
        self.owned_file("selected-task.env")
        adjacent = self.runner / "unrelated.json"
        adjacent.write_bytes(b"unrelated synthetic bytes")
        log_io.cleanup_temp(self.root, self.runner, self.run_id)
        self.assertFalse(self.root.exists())
        self.assertEqual(adjacent.read_bytes(), b"unrelated synthetic bytes")

    def test_absent_exact_directory_is_idempotent(self):
        self.root.rmdir()
        log_io.cleanup_temp(self.root, self.runner, self.run_id)
        log_io.cleanup_temp(self.root, self.runner, self.run_id)
        self.assertFalse(self.root.exists())

    def test_empty_owned_directory_is_removed(self):
        log_io.cleanup_temp(self.root, self.runner, self.run_id)
        self.assertFalse(self.root.exists())

    def test_scope_and_run_id_escapes_fail_before_any_delete(self):
        owned = self.owned_file()
        for override in (
            {"run_id": "0"}, {"run_id": "012345"}, {"run_id": "../12345"},
            {"run_id": 12345}, {"run_id": "9" * 21}, {"run_id": "12345\n"},
            {"root": self.runner}, {"root": self.root / ".."},
            {"root": self.runner / "vevo-aa-infra-54321"},
            {"root": Path("vevo-aa-infra-12345")}, {"runner_temp": Path("relative")},
            {"runner_temp": self.runner / "missing"},
            {"runner_temp": Path(self.runner.anchor)},
        ):
            with self.subTest(override=override):
                self.assert_cleanup_stopped("temp-scope-invalid", **override)
                self.assertTrue(owned.exists())

    def test_wrong_absent_directory_still_requires_exact_scope(self):
        self.root.rmdir()
        self.assert_cleanup_stopped("temp-scope-invalid", root=self.runner / "different")

    def test_run_path_as_regular_file_is_rejected(self):
        self.root.rmdir()
        self.root.write_bytes(b"test-owned file, not directory")
        self.assert_cleanup_stopped("temp-scope-invalid")
        self.assertTrue(self.root.is_file())

    def test_subdirectory_rejected_before_deleting_any_sibling(self):
        owned = self.owned_file("first.json")
        nested = self.root / "nested"
        nested.mkdir()
        child = nested / "keep.txt"
        child.write_bytes(b"keep")
        self.assert_cleanup_stopped()
        self.assertTrue(owned.exists())
        self.assertEqual(child.read_bytes(), b"keep")

    def test_more_than_64_entries_rejected_before_deleting_any_file(self):
        owned = [self.owned_file(f"{i}.json") for i in range(65)]
        self.assert_cleanup_stopped()
        self.assertTrue(all(path.exists() for path in owned))

    def test_file_symlink_rejected_before_deleting_any_sibling_or_target(self):
        owned = self.owned_file("first.json")
        target = self.runner / "outside.txt"
        target.write_bytes(b"outside synthetic target")
        link = self.root / "link.json"
        self.make_symlink(link, target)
        self.assert_cleanup_stopped()
        self.assertTrue(owned.exists())
        self.assertTrue(link.is_symlink())
        self.assertEqual(target.read_bytes(), b"outside synthetic target")

    def test_dangling_symlink_rejected_before_deleting_any_sibling(self):
        owned = self.owned_file("first.json")
        link = self.root / "dangling.json"
        self.make_symlink(link, self.runner / "absent.txt")
        self.assert_cleanup_stopped()
        self.assertTrue(owned.exists())
        self.assertTrue(link.is_symlink())

    def test_root_symlink_escape_rejected_and_target_untouched(self):
        self.root.rmdir()
        target = self.runner / "other-project"
        target.mkdir()
        preserved = target / "keep.json"
        preserved.write_bytes(b"keep")
        self.make_symlink(self.root, target, directory=True)
        self.assert_cleanup_stopped("temp-scope-invalid")
        self.assertTrue(preserved.exists())
        self.assertTrue(self.root.is_symlink())

    def test_hardlink_rejected_before_deleting_any_sibling_or_target(self):
        owned = self.owned_file("first.json")
        target = self.runner / "outside-hardlink.txt"
        target.write_bytes(b"outside synthetic target")
        link = self.root / "hardlink.json"
        try:
            os.link(target, link)
        except (OSError, NotImplementedError) as error:
            self.skipTest(f"OS does not permit test-owned hardlinks: {type(error).__name__}")
        self.assert_cleanup_stopped()
        self.assertTrue(owned.exists())
        self.assertTrue(link.exists())
        self.assertEqual(target.read_bytes(), b"outside synthetic target")


if __name__ == "__main__":
    unittest.main()
