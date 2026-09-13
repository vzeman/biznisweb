"""Bounded BiznisWeb query backoff; never use this module to replay writes."""
from __future__ import annotations

import math
import re
import time
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

from gql.transport.exceptions import TransportServerError
from gql.transport.requests import RequestsHTTPTransport

READ_BUDGET_SECONDS = 240
MAX_SLEEP_SECONDS = 30
RATE_LIMIT_COOLDOWN_SECONDS = 60
_TRANSIENT_CODES = {429, 500, 501, 502, 503, 504}
# Recognized structured GraphQL codes only; never infer these from messages.
_PERMANENT_GRAPHQL_CODES = {
    "GRAPHQL_PARSE_FAILED", "GRAPHQL_VALIDATION_FAILED", "BAD_USER_INPUT",
    "OPERATION_RESOLUTION_FAILURE", "UNAUTHENTICATED", "FORBIDDEN",
}
_TRANSIENT_ERRORS = {
    "Timeout", "ReadTimeout", "ConnectTimeout", "ConnectionError", "TimeoutError",
    "TransportProtocolError", "TransportQueryError",
}


class ReadAwareRequestsHTTPTransport(RequestsHTTPTransport):
    """Retain HTTP status even when gql turns a JSON error into a query error."""

    def _get_json_result(self, response: Any) -> Any:
        self.response_status_code = response.status_code
        return super()._get_json_result(response)


def prepare_query_read(client: Any, query: Any) -> Any:
    """Validate before a request and invalidate the previous response's headers.

    gql's RequestsHTTPTransport sets response_headers before parsing the current
    response, including server errors. A connection failure sets nothing, so the
    old success/error headers must be cleared before every individual attempt.
    The returned transport is the only transport whose headers may be consulted.
    """
    from graphql import OperationType

    document = getattr(query, "document", query)
    operations = [node for node in document.definitions if hasattr(node, "operation")]
    if not operations or any(node.operation != OperationType.QUERY for node in operations):
        raise ValueError("Read backoff only accepts query operations")
    transport = getattr(client, "transport", None)
    if transport is not None:
        try:
            transport.response_headers = None
            transport.response_status_code = None
        except (AttributeError, TypeError):
            return None  # Never consult a header cache we could not invalidate.
    return transport


def check_read_response_status(transport: Any) -> None:
    """A data-only GraphQL envelope cannot make a failed HTTP request succeed."""
    status = getattr(transport, "response_status_code", None)
    if status is None:
        return  # Injected/other transports can report status on their exception.
    if not isinstance(status, int) or isinstance(status, bool):
        raise RuntimeError("Invalid query HTTP status metadata")
    if not 200 <= status < 300:
        raise TransportServerError("BiznisWeb query returned a non-success HTTP status", code=status)


def _permanent_graphql_failure(error: Exception) -> bool:
    codes = [getattr(error, "code", None)]
    errors = getattr(error, "errors", None)
    if isinstance(errors, list):
        for item in errors:
            if isinstance(item, Mapping):
                codes.append(item.get("code"))
                extensions = item.get("extensions")
                if isinstance(extensions, Mapping):
                    codes.append(extensions.get("code"))
    return any(isinstance(code, str) and code in _PERMANENT_GRAPHQL_CODES for code in codes)


def _header(headers: Any, name: str) -> str | None:
    if not isinstance(headers, Mapping):
        return None
    matches = [value for key, value in headers.items()
               if isinstance(key, str) and key.lower() == name.lower()]
    if len(matches) != 1 or not isinstance(matches[0], str):
        return None
    return matches[0].strip()


def retry_after_seconds(headers: Any, *, now: datetime | None = None) -> float | None:
    """Parse Retry-After delta seconds or an HTTP date without logging headers."""
    value = _header(headers, "Retry-After")
    if not value:
        return None
    if re.fullmatch(r"[0-9]+", value):
        # Even an enormous valid value must fail the caller's time budget,
        # rather than becoming a short fallback or an unbounded sleep.
        return float(value) if len(value) < 300 else math.inf
    try:
        target = parsedate_to_datetime(value)
        if target.tzinfo is None:
            return None
        current = now or datetime.now(timezone.utc)
        if current.tzinfo is None:
            raise ValueError("Retry-After requires an aware current time")
        return max(0.0, (target - current).total_seconds())
    except (TypeError, ValueError, OverflowError):
        return None


def _status_codes(error: Exception, response_status_code: Any = None) -> set[int]:
    values = [response_status_code, getattr(error, "code", None),
              getattr(getattr(error, "response", None), "status_code", None)]
    errors = getattr(error, "errors", None)
    if isinstance(errors, list):
        for item in errors:
            if isinstance(item, Mapping):
                values.append(item.get("code"))
                extensions = item.get("extensions")
                if isinstance(extensions, Mapping):
                    values.extend(extensions.get(name) for name in ("code", "status", "status_code", "httpStatus"))
    return {int(value) for value in values
            if not isinstance(value, bool) and isinstance(value, (int, str))
            and re.fullmatch(r"[4-5][0-9]{2}", str(value))}


def read_retry_delay(error: Exception, *, attempt: int, response_headers: Any = None,
                     response_status_code: Any = None, now: datetime | None = None) -> float | None:
    """Return a safe query-only delay, or None when this failure must stop.

    429 represents the per-minute cost window. 509 is a daily/monthly quota and
    is deliberately not retried. Unclassified GraphQL failures without partial
    data use the same cooldown because FLOX can return quota failures in HTTP 200.
    Raw error messages and response bodies are never inspected or logged.
    """
    codes = _status_codes(error, response_status_code)
    if 509 in codes or getattr(error, "data", None) is not None or _permanent_graphql_failure(error):
        return None
    transient = bool(codes & _TRANSIENT_CODES) or (not codes and type(error).__name__ in _TRANSIENT_ERRORS)
    if not transient:
        return None
    # requests exceptions bind headers directly to this failure. gql instead
    # keeps them on the transport invalidated by prepare_query_read.
    own_headers = getattr(getattr(error, "response", None), "headers", None)
    headers = own_headers if isinstance(own_headers, Mapping) else response_headers
    supplied_delay = retry_after_seconds(headers, now=now)
    if supplied_delay is not None:
        return supplied_delay
    if codes & {429, 501} or type(error).__name__ == "TransportQueryError":
        return RATE_LIMIT_COOLDOWN_SECONDS
    return min(30, 10 * (attempt + 1))


def check_read_deadline(deadline: float, *, delay: float = 0,
                        monotonic: Callable[[], float] = time.monotonic,
                        message: str = "Order API read time limit reached") -> None:
    if monotonic() + delay >= deadline:
        raise RuntimeError(message)


def wait_before_read(delay: float, *, deadline: float,
                     progress_callback: Callable[[], None] | None = None,
                     monotonic: Callable[[], float] = time.monotonic,
                     sleep: Callable[[float], None] = time.sleep,
                     message: str = "Order API read time limit reached") -> None:
    """Wait in short chunks, retaining the caller's complete scan/read budget."""
    if delay < 0 or math.isnan(delay):
        raise ValueError("Invalid query retry delay")
    check_read_deadline(deadline, delay=delay, monotonic=monotonic, message=message)
    remaining = delay
    while remaining > 0:
        if progress_callback:
            progress_callback()
        check_read_deadline(deadline, delay=remaining, monotonic=monotonic, message=message)
        chunk = min(MAX_SLEEP_SECONDS, remaining)
        sleep(chunk)
        remaining -= chunk
        check_read_deadline(deadline, monotonic=monotonic, message=message)
    if progress_callback:
        progress_callback()
    check_read_deadline(deadline, monotonic=monotonic, message=message)
