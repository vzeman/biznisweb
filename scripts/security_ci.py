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
        html_report_generator = read("html_report_generator.py")
        dashboard_modern = read("dashboard_modern.py")
        http_client = read("http_client.py")
        weather_client = read("weather_client.py")
        read("templates/reporting-client/settings.template.json")
        read("templates/reporting-client/.env.example")
        read("templates/reporting-client/product_expenses.json")
        read("templates/reporting-client/README_CLIENT_SETUP.md")
        read("scripts/reporting_qa_smoke.py")

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
            "_build_attribution_qa",
            "export_orders.py must build attribution QA guardrails before report export.",
        )
        require(
            export_orders,
            "campaign_attribution_summary",
            "export_orders.py must keep campaign attribution summary metadata.",
        )
        require(
            export_orders,
            "_build_geo_qa",
            "export_orders.py must build geo confidence QA metadata before report export.",
        )
        require(
            export_orders,
            "_build_data_assertions_qa",
            "export_orders.py must build data assertion QA metadata before report export.",
        )
        require(
            export_orders,
            "\"qa_failure_count\"",
            "export_orders.py must aggregate QA failure counts into source health metadata.",
        )
        require(
            export_orders,
            "\"qa_warning_count\"",
            "export_orders.py must aggregate QA warning counts into source health metadata.",
        )
        require(
            export_orders,
            "\"null_label_rate_pct\"",
            "export_orders.py must expose null label rate in data assertions QA.",
        )
        require(
            export_orders,
            "_build_margin_stability_qa",
            "export_orders.py must build smoothed margin stability QA before report export.",
        )
        require(
            export_orders,
            "\"is_partial\"",
            "export_orders.py must persist partial-data state in source health metadata.",
        )
        require(
            daily_runner,
            "report_html",
            "daily_report_runner.py must still attach the generated main HTML report artifact.",
        )
        require(
            daily_runner,
            "build_data_quality_summary",
            "daily_report_runner.py must summarize data quality in the email body.",
        )
        require(
            daily_runner,
            "ReportQaFailures",
            "daily_report_runner.py must publish QA failure CloudWatch metrics.",
        )
        require(
            daily_runner,
            "ReportQaWarnings",
            "daily_report_runner.py must publish QA warning CloudWatch metrics.",
        )
        require(
            html_report_generator + dashboard_modern,
            "Partial Data",
            "HTML rendering layer must expose explicit partial-data status for generated reports.",
        )
        require(
            dashboard_modern,
            "Attribution QA guardrails",
            "Modern dashboard must surface attribution QA warnings explicitly.",
        )
        require(
            dashboard_modern,
            "Geo confidence guardrails",
            "Modern dashboard must surface geo confidence guardrails explicitly.",
        )
        require(
            dashboard_modern,
            "Data assertions",
            "Modern dashboard must surface data assertion warnings explicitly.",
        )
        require(
            dashboard_modern,
            "Critical failures",
            "Modern dashboard must expose QA failure counts in data assertions cards.",
        )
        require(
            dashboard_modern,
            "Smoothed fixed-margin alerts",
            "Modern dashboard must surface smoothed fixed-margin alerts explicitly.",
        )
        require(
            dashboard_modern,
            "CM1 / CM2 / CM3 taxonomy",
            "Modern dashboard must surface normalized CM taxonomy explicitly.",
        )
        require(
            dashboard_modern,
            "qa_rows.append",
            "Modern dashboard must keep rendering QA cards alongside source-health cards.",
        )
        require(
            dashboard_modern,
            "hero-alert",
            "Modern dashboard must surface attribution QA warnings in the hero shell before deeper sections.",
        )
        require(
            read(".github/workflows/observability-check.yml"),
            "observability_snapshot.py",
            "Reporting repo must keep an observability workflow baseline.",
        )

        for rel_path in [
            "http_client.py",
            "facebook_ads.py",
            "google_ads.py",
            "weather_client.py",
            "export_orders.py",
            "daily_report_runner.py",
            "generate_invoices.py",
            "scripts/observability_snapshot.py",
            "scripts/scaffold_client.py",
            "scripts/reporting_qa_smoke.py",
        ]:
            py_compile.compile(str(ROOT / rel_path), doraise=True)

        print("security_ci.py: OK")
        return 0
    except Exception as exc:  # pragma: no cover - CI failure path
        print(f"security_ci.py: FAIL: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
