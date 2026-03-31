#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import py_compile
import sys


ROOT = pathlib.Path(__file__).resolve().parents[1]


def read(rel_path: str) -> str:
    return (ROOT / rel_path).read_text(encoding="utf-8")


def require(text: str, needle: str, message: str) -> None:
    if needle not in text:
        raise AssertionError(message)


def forbid(text: str, needle: str, message: str) -> None:
    if needle in text:
        raise AssertionError(message)


def main() -> int:
    try:
        facebook_ads = read("facebook_ads.py")
        export_orders = read("export_orders.py")
        daily_runner = read("daily_report_runner.py")
        http_client = read("http_client.py")
        weather_client = read("weather_client.py")

        require(
            facebook_ads,
            "headers={'Authorization': f'Bearer {self.access_token}'}",
            "facebook_ads.py must send Meta token via Authorization header.",
        )
        forbid(
            facebook_ads,
            "params = {\"access_token\"",
            "facebook_ads.py must not send Meta token via query string params.",
        )
        require(
            facebook_ads,
            "def _sanitize_url",
            "facebook_ads.py must sanitize logged URLs.",
        )
        require(
            http_client,
            "class TimeoutRetrySession",
            "http_client.py must provide the shared timeout-aware session wrapper.",
        )
        require(
            weather_client,
            "build_retry_session(timeout=self.request_timeout)",
            "weather_client.py must use the shared retry session.",
        )
        require(
            export_orders,
            "source_health",
            "export_orders.py must track source health for partial-data mode.",
        )
        require(
            export_orders,
            "_finalize_source_health",
            "export_orders.py must finalize source-health metadata for each run.",
        )
        require(
            export_orders,
            "\"is_partial\"",
            "export_orders.py must persist partial-data state in source health metadata.",
        )
        require(
            daily_runner,
            "Partial Data",
            "daily_report_runner.py must render partial-data status for generated reports.",
        )

        for rel_path in [
            "http_client.py",
            "facebook_ads.py",
            "google_ads.py",
            "weather_client.py",
            "export_orders.py",
            "daily_report_runner.py",
            "generate_invoices.py",
        ]:
            py_compile.compile(str(ROOT / rel_path), doraise=True)

        print("security_ci.py: OK")
        return 0
    except Exception as exc:  # pragma: no cover - CI failure path
        print(f"security_ci.py: FAIL: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
