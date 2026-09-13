import math
import unittest
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from types import SimpleNamespace
from unittest.mock import Mock

from gql import gql
from gql.transport.exceptions import TransportQueryError, TransportServerError
from requests import Response

from api_read_backoff import (
    ReadAwareRequestsHTTPTransport, check_read_response_status, prepare_query_read, read_retry_delay,
    retry_after_seconds, wait_before_read,
)


class ApiReadBackoffTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2020, 1, 1, tzinfo=timezone.utc)

    def test_delta_and_case_insensitive_header(self):
        self.assertEqual(75, retry_after_seconds({"rEtRy-AfTeR": " 75 "}))
        self.assertEqual(0, retry_after_seconds({"Retry-After": "0"}))

    def test_http_date_and_past_date(self):
        future = format_datetime(self.now + timedelta(seconds=95), usegmt=True)
        past = format_datetime(self.now - timedelta(seconds=10), usegmt=True)
        self.assertEqual(95, retry_after_seconds({"Retry-After": future}, now=self.now))
        self.assertEqual(0, retry_after_seconds({"Retry-After": past}, now=self.now))

    def test_malformed_headers_fall_back_without_exposing_values(self):
        for value in ("", "-1", "1.5", "nan", "inf", "1e3", "private fixture text", 30, None):
            with self.subTest(value=value):
                self.assertEqual(60, read_retry_delay(TransportServerError("private", code=429),
                                                       attempt=0, response_headers={"Retry-After": value}))
        self.assertIsNone(retry_after_seconds({"Retry-After": "4", "retry-after": "10"}))
        self.assertIsNone(retry_after_seconds("private fixture text"))
        self.assertIsNone(retry_after_seconds({"Retry-After": "Wed, 01 Jan 2020 00:00:10"}))

    def test_extreme_valid_delay_is_not_shortened(self):
        delay = retry_after_seconds({"Retry-After": "9" * 1000})
        self.assertTrue(math.isinf(delay))
        sleep = Mock()
        with self.assertRaisesRegex(RuntimeError, "time limit"):
            wait_before_read(delay, deadline=1200, monotonic=lambda: 0, sleep=sleep)
        sleep.assert_not_called()

    def test_429_fallback_covers_minute_window_on_each_attempt(self):
        for attempt in range(3):
            self.assertEqual(60, read_retry_delay(TransportServerError("private", code=429), attempt=attempt))

    def test_retry_after_is_honored_for_rate_limit_maintenance_and_server_errors(self):
        for code in (429, 501, 503):
            self.assertEqual(75, read_retry_delay(TransportServerError("private", code=code), attempt=0,
                                                  response_headers={"Retry-After": "75"}))
        self.assertEqual(60, read_retry_delay(TransportServerError("private", code=501), attempt=0))

    def test_requests_response_headers_belong_to_current_error(self):
        response = SimpleNamespace(status_code=429, headers={"Retry-After": "85"})
        error = RuntimeError("private")
        error.response = response
        self.assertEqual(85, read_retry_delay(error, attempt=0, response_headers={"Retry-After": "1"}))

    def test_509_quota_never_retries_even_with_header(self):
        self.assertIsNone(read_retry_delay(TransportServerError("private", code=509), attempt=0,
                                           response_headers={"Retry-After": "1"}))
        error = TransportQueryError("private", errors=[{"extensions": {"code": "509"}}])
        self.assertIsNone(read_retry_delay(error, attempt=0))
        self.assertIsNone(read_retry_delay(TransportQueryError("private"), attempt=0, response_status_code=509))

    def test_graphql_structured_rate_limit_and_uncoded_quota(self):
        error = TransportQueryError("private", errors=[{"extensions": {"status_code": 429}}])
        self.assertEqual(60, read_retry_delay(error, attempt=0))
        self.assertEqual(60, read_retry_delay(TransportQueryError("private"), attempt=0, response_status_code=200))

    def test_nontransient_and_partial_graphql_errors_do_not_retry(self):
        for error in (TransportServerError("private", code=403), ValueError("private"), RuntimeError("429"),
                      TransportQueryError("private", data={}), TransportQueryError("private", data={"x": None})):
            self.assertIsNone(read_retry_delay(error, attempt=0, response_headers={"Retry-After": "75"}))

    def test_known_structured_permanent_graphql_failures_are_not_retried(self):
        for code in ("GRAPHQL_PARSE_FAILED", "GRAPHQL_VALIDATION_FAILED", "BAD_USER_INPUT",
                     "OPERATION_RESOLUTION_FAILURE", "UNAUTHENTICATED", "FORBIDDEN"):
            error = TransportQueryError("private", errors=[{"extensions": {"code": code}}])
            self.assertIsNone(read_retry_delay(error, attempt=0, response_headers={"Retry-After": "1"}))
        error = TransportQueryError("private", errors=[{"extensions": {"code": "INTERNAL_SERVER_ERROR"}}])
        self.assertEqual(60, read_retry_delay(error, attempt=0))
        # Messages alone must never be treated as structured permanent codes.
        self.assertEqual(60, read_retry_delay(TransportQueryError("FORBIDDEN"), attempt=0))

    def test_failed_http_status_is_not_a_success_with_data_only_json(self):
        transport = ReadAwareRequestsHTTPTransport(url="https://example.test/api/graphql", retries=0)
        for status in (429, 509, 401, 308):
            response = Response()
            response.status_code = status
            response._content = b'{"data":{"getOrder":{"id":"synthetic"}}}'
            result = transport._prepare_result(response)
            self.assertTrue(result.data)
            with self.assertRaises(TransportServerError) as raised:
                check_read_response_status(transport)
            self.assertEqual(status, raised.exception.code)
        transport.response_status_code = 200
        check_read_response_status(transport)
        check_read_response_status(None)

    def test_retry_metadata_is_invalidated_before_each_query(self):
        transport = SimpleNamespace(response_headers={"Retry-After": "900"}, response_status_code=509)
        client = SimpleNamespace(transport=transport)
        self.assertIs(transport, prepare_query_read(client, gql("query { x }")))
        self.assertIsNone(transport.response_headers)
        self.assertIsNone(transport.response_status_code)
        self.assertEqual(10, read_retry_delay(ConnectionError("private"), attempt=0,
                                             response_headers=transport.response_headers,
                                             response_status_code=transport.response_status_code))

    def test_mutation_and_mixed_document_rejected_before_metadata_changes(self):
        transport = SimpleNamespace(response_headers={"Retry-After": "900"})
        client = SimpleNamespace(transport=transport)
        for query in ("mutation { x }", "query A { x } mutation B { y }"):
            with self.assertRaisesRegex(ValueError, "query"):
                prepare_query_read(client, gql(query))
        self.assertEqual({"Retry-After": "900"}, transport.response_headers)

    def test_gql_transport_retains_current_status_and_headers_before_json_failure(self):
        transport = ReadAwareRequestsHTTPTransport(url="https://example.test/api/graphql", retries=0)
        response = Response()
        response.status_code = 501
        response.headers["Retry-After"] = "75"
        response._content = b"not JSON"
        with self.assertRaises(TransportServerError):
            transport._get_json_result(response)
        self.assertEqual(501, transport.response_status_code)
        self.assertEqual("75", transport.response_headers["Retry-After"])
        self.assertEqual(0, transport.retries)

    def test_gql_json_error_keeps_509_even_when_execution_result_loses_http_status(self):
        transport = ReadAwareRequestsHTTPTransport(url="https://example.test/api/graphql", retries=0)
        response = Response()
        response.status_code = 509
        response._content = b'{"errors":[{"message":"private fixture"}]}'
        result = transport._prepare_result(response)
        self.assertTrue(result.errors)
        self.assertEqual(509, transport.response_status_code)
        error = TransportQueryError("private fixture", errors=result.errors)
        self.assertIsNone(read_retry_delay(error, attempt=0, response_status_code=transport.response_status_code))

    def test_wait_chunks_and_lease_heartbeat(self):
        clock = [0.0]
        waits = []
        progress = []
        def sleep(seconds):
            waits.append(seconds)
            clock[0] += seconds
        wait_before_read(75, deadline=240, monotonic=lambda: clock[0], sleep=sleep,
                         progress_callback=lambda: progress.append(clock[0]))
        self.assertEqual([30, 30, 15], waits)
        self.assertEqual([0, 30, 60, 75], progress)

    def test_insufficient_shared_budget_fails_before_wait_or_heartbeat(self):
        for delay in (60, 5):
            sleep, heartbeat = Mock(), Mock()
            with self.assertRaisesRegex(RuntimeError, "fixture scan time limit"):
                wait_before_read(delay, deadline=1200, monotonic=lambda: 1195,
                                 sleep=sleep, progress_callback=heartbeat, message="fixture scan time limit")
            sleep.assert_not_called()
            heartbeat.assert_not_called()

    def test_slow_heartbeat_and_oversleep_cannot_escape_deadline(self):
        clock = [0.0]
        def progress():
            clock[0] += 100
        sleep = Mock()
        with self.assertRaisesRegex(RuntimeError, "time limit"):
            wait_before_read(60, deadline=120, monotonic=lambda: clock[0], sleep=sleep,
                             progress_callback=progress)
        sleep.assert_not_called()
        clock[0] = 0
        def oversleep(seconds):
            clock[0] += 200
        with self.assertRaisesRegex(RuntimeError, "time limit"):
            wait_before_read(60, deadline=120, monotonic=lambda: clock[0], sleep=oversleep)

    def test_invalid_delay_fails_before_sleep(self):
        for delay in (-1, math.nan):
            sleep = Mock()
            with self.assertRaises(ValueError):
                wait_before_read(delay, deadline=120, monotonic=lambda: 0, sleep=sleep)
            sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
