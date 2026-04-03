#!/usr/bin/env python3
"""
Shared HTTP client helpers for external reporting integrations.
"""

from __future__ import annotations

import os
from typing import Mapping, Optional, Sequence

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DEFAULT_CONNECT_TIMEOUT_SEC = float(os.getenv("REPORT_HTTP_CONNECT_TIMEOUT_SEC", "10"))
DEFAULT_READ_TIMEOUT_SEC = float(os.getenv("REPORT_HTTP_READ_TIMEOUT_SEC", "30"))
DEFAULT_RETRY_TOTAL = max(0, int(os.getenv("REPORT_HTTP_RETRY_TOTAL", "3")))
DEFAULT_RETRY_BACKOFF_SEC = float(os.getenv("REPORT_HTTP_RETRY_BACKOFF_SEC", "0.5"))
DEFAULT_STATUS_FORCE_LIST = (429, 500, 502, 503, 504)
DEFAULT_ALLOWED_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})
DEFAULT_USER_AGENT = os.getenv("REPORT_HTTP_USER_AGENT", "biznisweb-reporting/2026.03")


def resolve_timeout(timeout: Optional[object] = None):
    """Normalize timeout into requests-compatible connect/read tuple."""
    if timeout is None:
        return (DEFAULT_CONNECT_TIMEOUT_SEC, DEFAULT_READ_TIMEOUT_SEC)
    if isinstance(timeout, (int, float)):
        timeout_value = float(timeout)
        return (timeout_value, timeout_value)
    if (
        isinstance(timeout, Sequence)
        and len(timeout) == 2
        and all(isinstance(item, (int, float)) for item in timeout)
    ):
        return (float(timeout[0]), float(timeout[1]))
    raise ValueError("Timeout must be None, a number, or a 2-item numeric sequence.")


class TimeoutRetrySession(requests.Session):
    """Requests session with default timeout + retry policy."""

    def __init__(self, default_timeout=None):
        super().__init__()
        self.default_timeout = resolve_timeout(default_timeout)

    def request(self, method, url, **kwargs):  # type: ignore[override]
        kwargs.setdefault("timeout", self.default_timeout)
        return super().request(method, url, **kwargs)


def build_retry_session(
    *,
    headers: Optional[Mapping[str, str]] = None,
    timeout=None,
    total: int = DEFAULT_RETRY_TOTAL,
    backoff_factor: float = DEFAULT_RETRY_BACKOFF_SEC,
    status_forcelist: Sequence[int] = DEFAULT_STATUS_FORCE_LIST,
    allowed_methods=DEFAULT_ALLOWED_METHODS,
) -> TimeoutRetrySession:
    """Create a requests session with sane retry and timeout defaults."""

    session = TimeoutRetrySession(default_timeout=timeout)
    retry = Retry(
        total=total,
        connect=total,
        read=total,
        status=total,
        backoff_factor=backoff_factor,
        status_forcelist=tuple(status_forcelist),
        allowed_methods=allowed_methods,
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": DEFAULT_USER_AGENT})
    if headers:
        session.headers.update(dict(headers))
    return session

