#!/usr/bin/env python3
"""Scheduled monthly credit-note export runner."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate, make_msgid
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from creditnote_export import (
    CreditnoteExportResult,
    parse_date,
    parse_project_list,
    previous_calendar_month,
    run_monthly_creditnote_export,
)
from money_s3_invoice_export import MoneyS3InvoiceExportResult, run_money_s3_invoice_export
from reporting_core import load_project_settings, put_metric, resolve_reporting_defaults


DEFAULT_OWNER_PROJECT = os.getenv("CREDITNOTE_EXPORT_OWNER_PROJECT", "roy").strip() or "roy"
DEFAULT_RECIPIENT = "mil.terem@gmail.com"
MAX_SES_RAW_MESSAGE_BYTES = 9_500_000


@dataclass(frozen=True)
class MonthlyAccountingExportResult:
    creditnote_results: Tuple[CreditnoteExportResult, ...]
    invoice_result: MoneyS3InvoiceExportResult | None

    def __post_init__(self) -> None:
        if not self.creditnote_results:
            raise ValueError("At least one credit-note export is required")
        windows = {(result.date_from, result.date_to) for result in self.creditnote_results}
        if len(windows) != 1:
            raise ValueError("Credit-note exports must use one shared date window")
        projects = tuple(result.projects[0] for result in self.creditnote_results)
        if len(projects) != len(set(projects)):
            raise ValueError("Credit-note exports contain duplicate projects")
        if self.invoice_result is not None:
            if self.invoice_result.projects != projects:
                raise ValueError("Money S3 invoice projects do not match credit-note projects")
            if (self.invoice_result.date_from, self.invoice_result.date_to) != next(iter(windows)):
                raise ValueError("Money S3 invoices do not match the credit-note date window")

    @property
    def projects(self) -> Tuple[str, ...]:
        return tuple(result.projects[0] for result in self.creditnote_results)

    @property
    def date_from(self) -> str:
        return self.creditnote_results[0].date_from

    @property
    def date_to(self) -> str:
        return self.creditnote_results[0].date_to

    @property
    def exported_rows(self) -> int:
        return sum(result.exported_rows for result in self.creditnote_results)

    @property
    def project_counts(self) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for result in self.creditnote_results:
            counts.update(result.project_counts)
        return counts

    @property
    def output_pdfs(self) -> Dict[str, Path]:
        return {
            result.projects[0].upper(): result.output_pdf
            for result in self.creditnote_results
        }

    def as_dict(self) -> Dict[str, Any]:
        return {
            "projects": list(self.projects),
            "date_from": self.date_from,
            "date_to": self.date_to,
            "exported_rows": self.exported_rows,
            "project_counts": self.project_counts,
            "output_pdfs": {project: str(path) for project, path in self.output_pdfs.items()},
            "creditnote_exports": {
                result.projects[0].upper(): result.as_dict()
                for result in self.creditnote_results
            },
            "invoice_export": self.invoice_result.as_dict() if self.invoice_result else None,
        }


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name, "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "y", "on"}


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export monthly BizniWeb credit notes and Money S3 invoices")
    parser.add_argument(
        "--projects",
        default=os.getenv("CREDITNOTE_EXPORT_PROJECTS", "roy,vevo"),
        help="Comma-separated project slugs. Each project gets a separate credit-note PDF.",
    )
    parser.add_argument(
        "--owner-project",
        default=DEFAULT_OWNER_PROJECT,
        help="Project used for metrics/default settings. The export still uses --projects.",
    )
    parser.add_argument(
        "--reference-date",
        default=os.getenv("CREDITNOTE_EXPORT_REFERENCE_DATE", ""),
        help="Reference date in YYYY-MM-DD format. Default: today in --timezone; exports the previous calendar month.",
    )
    parser.add_argument(
        "--from-date",
        default=os.getenv("CREDITNOTE_EXPORT_FROM_DATE", ""),
        help="Explicit credit-note created window start date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--to-date",
        default=os.getenv("CREDITNOTE_EXPORT_TO_DATE", ""),
        help="Explicit credit-note created window end date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--timezone",
        default=os.getenv("REPORT_TIMEZONE", "Europe/Bratislava"),
        help="Timezone used for default reference-date calculation.",
    )
    parser.add_argument(
        "--output-dir",
        default=os.getenv("CREDITNOTE_EXPORT_OUTPUT_DIR", ""),
        help="Optional output directory. Defaults to data/combined_exports.",
    )
    parser.add_argument(
        "--output-tag",
        default=os.getenv("CREDITNOTE_EXPORT_OUTPUT_TAG", ""),
        help="Optional suffix for test artifacts.",
    )
    parser.add_argument(
        "--email-from",
        default=os.getenv("CREDITNOTE_EXPORT_EMAIL_FROM", os.getenv("REPORT_EMAIL_FROM", "")),
        help="SES sender email. Defaults to CREDITNOTE_EXPORT_EMAIL_FROM or REPORT_EMAIL_FROM.",
    )
    parser.add_argument(
        "--email-to",
        default=os.getenv("CREDITNOTE_EXPORT_EMAIL_TO", DEFAULT_RECIPIENT),
        help="Comma-separated recipient list.",
    )
    parser.add_argument(
        "--email-subject",
        default=os.getenv("CREDITNOTE_EXPORT_EMAIL_SUBJECT", ""),
        help="Optional email subject override.",
    )
    parser.add_argument(
        "--skip-email",
        action="store_true",
        default=_env_bool("CREDITNOTE_EXPORT_SKIP_EMAIL", False),
        help="Generate files but do not send SES email.",
    )
    parser.add_argument(
        "--dry-run-email",
        action="store_true",
        default=_env_bool("CREDITNOTE_EXPORT_DRY_RUN_EMAIL", False),
        help="Build the email body and attachments but do not call SES.",
    )
    parser.add_argument(
        "--skip-metrics",
        action="store_true",
        default=_env_bool("CREDITNOTE_EXPORT_SKIP_METRICS", False),
        help="Generate files but do not publish CloudWatch metrics.",
    )
    parser.add_argument(
        "--skip-invoice-export",
        action="store_true",
        default=_env_bool("CREDITNOTE_EXPORT_SKIP_INVOICE_EXPORT", False),
        help="Skip Money S3 invoice XML files. The scheduled accounting run exports them by default.",
    )
    return parser.parse_args(argv)


def resolve_creditnote_export_window(
    timezone_name: str,
    reference_date: str = "",
    from_date: str = "",
    to_date: str = "",
) -> Tuple[str, str]:
    if bool(from_date) != bool(to_date):
        raise ValueError("Use both --from-date and --to-date, or neither.")
    if from_date and to_date:
        start = parse_date(from_date, "from_date")
        end = parse_date(to_date, "to_date")
        if start > end:
            raise ValueError(f"from_date ({start}) cannot be after to_date ({end})")
        return start.isoformat(), end.isoformat()

    if reference_date:
        reference = parse_date(reference_date, "reference_date")
    else:
        reference = datetime.now(ZoneInfo(timezone_name)).date()
    start, end = previous_calendar_month(reference)
    return start.isoformat(), end.isoformat()


AccountingExportInput = CreditnoteExportResult | MonthlyAccountingExportResult


def _creditnote_results(result: AccountingExportInput) -> Tuple[CreditnoteExportResult, ...]:
    if isinstance(result, MonthlyAccountingExportResult):
        return result.creditnote_results
    return (result,)


def _invoice_result(result: AccountingExportInput) -> MoneyS3InvoiceExportResult | None:
    if isinstance(result, MonthlyAccountingExportResult):
        return result.invoice_result
    return None


def build_creditnote_email_subject(result: AccountingExportInput, override: str = "") -> str:
    if override.strip():
        return override.strip()
    projects = "+".join(project.upper() for project in result.projects)
    return f"Uctovne doklady {projects} {result.date_from[:7]}"


def _format_amount(value: Any) -> str:
    try:
        return f"{float(value):,.2f}".replace(",", " ")
    except (TypeError, ValueError):
        return "0.00"


def _format_rate(value: Any) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):.2f}%"
    except (TypeError, ValueError):
        return "-"


def build_creditnote_email_body(result: AccountingExportInput) -> str:
    creditnote_results = _creditnote_results(result)
    invoice_result = _invoice_result(result)
    lines = [
        "Dobry den,",
        "",
        "v prilohe posielam mesacny PDF export dobropisov z BizniWebu.",
        f"Obdobie vytvorenia dobropisov: {result.date_from} az {result.date_to}.",
        f"Faktury su filtrovane podla datumu vystavenia: {result.date_from} az {result.date_to}.",
        f"E-shopy: {', '.join(project.upper() for project in result.projects)}.",
        f"Pocet dobropisov: {result.exported_rows}.",
        "",
        "Dobropisovana suma spolu:",
        "Dobropisy PDF:",
    ]
    if invoice_result is not None:
        lines.insert(3, "V prilohe je aj export faktur vo formate Money S3.")
    for creditnote_result in creditnote_results:
        project = creditnote_result.projects[0].upper()
        if creditnote_result.total_rows:
            totals = ", ".join(
                f"{row.get('Mena') or '-'} {_format_amount(row.get('Suma_s_DPH'))} s DPH"
                for row in creditnote_result.total_rows
            )
        else:
            totals = "bez dobropisov"
        lines.append(f"- {project}: {creditnote_result.exported_rows} ks, {totals}")

    lines.extend(["", "Faktury Money S3:"])
    if invoice_result is None:
        lines.append("- Export faktur bol pre tento beh vypnuty.")
    else:
        for project in invoice_result.projects:
            project_key = project.upper()
            lines.append(f"- {project_key}: {int(invoice_result.invoice_counts.get(project_key) or 0)} faktur")

    lines.extend(
        [
            "",
            "Kontrola vylucenia z reporting revenue:",
        ]
    )
    for creditnote_result in creditnote_results:
        project = creditnote_result.projects[0].upper()
        audit = creditnote_result.reporting_exclusion_summary or {}
        audit_errors = audit.get("audit_errors") or {}
        if audit_errors:
            lines.append(
                f"- {project}: audit nie je kompletne dostupny: "
                + "; ".join(f"{key}: {value}" for key, value in audit_errors.items())
            )
            continue
        line = (
            f"- {project}: v realized revenue ostava {int(audit.get('included_in_revenue') or 0)} "
            f"z {int(audit.get('checked_orders') or 0)} dobropisovanych objednavok"
        )
        missing = int(audit.get("order_not_found") or 0)
        if missing:
            line += f", nenajdene povodne objednavky: {missing}"
        lines.append(line + ".")

    lines.extend(
        [
            "",
            "Prepravcovia podla dobropis rate (dobropisovane objednavky / realized objednavky v reportovanom obdobi):",
        ]
    )
    carrier_rows = [
        row
        for creditnote_result in creditnote_results
        for row in (creditnote_result.carrier_rows or [])
        if int(row.get("Dobropisovane objednavky") or 0) > 0
    ]
    if carrier_rows:
        for row in carrier_rows[:8]:
            lines.append(
                f"- {row.get('Eshop')} / {row.get('Prepravca') or '-'}: "
                f"{int(row.get('Dobropisovane objednavky') or 0)}/"
                f"{int(row.get('Realized objednavky') or 0)} objednavok = "
                f"{_format_rate(row.get('Dobropis rate %'))}, "
                f"dobropisy {int(row.get('Dobropisy') or 0)} ks, "
                f"s DPH {_format_amount(row.get('Suma_s_DPH'))}"
            )
    else:
        lines.append("- Bez dostupnej carrier statistiky alebo bez dobropisov.")

    lines.extend(
        [
            "",
            "Sumar:",
        ]
    )
    summary_rows = [
        row
        for creditnote_result in creditnote_results
        for row in creditnote_result.summary_rows
    ]
    if summary_rows:
        for row in summary_rows:
            lines.append(
                f"- {row.get('Eshop')} {row.get('Mena') or '-'}: "
                f"{int(row.get('Pocet') or 0)} ks, "
                f"s DPH {_format_amount(row.get('Suma_s_DPH'))}"
            )
    else:
        lines.append("- Bez dobropisov v zadanom obdobi.")
    lines.extend(
        [
            "",
            "Tento email bol odoslany automaticky zo systemu BiznisWeb reporting.",
        ]
    )
    return "\n".join(lines)


def send_creditnote_email_ses(
    result: AccountingExportInput,
    subject: str,
    body_text: str,
    email_from: str,
    email_to: str,
    reporting_defaults: Dict[str, Any],
) -> str:
    source = email_from.strip()
    destinations = [email.strip() for email in email_to.split(",") if email.strip()]
    if not source:
        raise ValueError("CREDITNOTE_EXPORT_EMAIL_FROM or REPORT_EMAIL_FROM is required")
    if not destinations:
        raise ValueError("CREDITNOTE_EXPORT_EMAIL_TO has no valid recipients")

    creditnote_results = _creditnote_results(result)
    invoice_result = _invoice_result(result)
    for creditnote_result in creditnote_results:
        if not creditnote_result.output_pdf.exists():
            raise FileNotFoundError(f"Missing creditnote PDF: {creditnote_result.output_pdf}")
    if invoice_result is not None:
        for project in invoice_result.projects:
            project_key = project.upper()
            path = invoice_result.output_files.get(project_key)
            if path is None or not path.exists():
                raise FileNotFoundError(f"Missing Money S3 invoice export for project: {project_key}")

    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = source
    msg["To"] = ", ".join(destinations)
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid(domain="amazonses.com")
    msg["Reply-To"] = source
    msg.attach(MIMEText(body_text, "plain", "utf-8"))

    for creditnote_result in creditnote_results:
        path = creditnote_result.output_pdf
        with path.open("rb") as fh:
            part = MIMEApplication(fh.read(), _subtype="pdf", Name=path.name)
        part["Content-Disposition"] = f'attachment; filename="{path.name}"'
        msg.attach(part)

    if invoice_result is not None:
        for project in invoice_result.projects:
            path = invoice_result.output_files[project.upper()]
            with path.open("rb") as fh:
                part = MIMEApplication(fh.read(), _subtype="xml", Name=path.name)
            part["Content-Disposition"] = f'attachment; filename="{path.name}"'
            msg.attach(part)

    try:
        import boto3  # type: ignore
        from botocore.exceptions import BotoCoreError, ClientError  # type: ignore
    except ImportError as exc:
        raise RuntimeError("boto3 is required for SES email sending. Install dependencies from requirements.txt.") from exc

    region = os.getenv("AWS_REGION", "eu-central-1").strip()
    configuration_set = os.getenv("SES_CONFIGURATION_SET", reporting_defaults.get("ses_configuration_set", "")).strip()
    ses = boto3.client("ses", region_name=region)
    raw_message = msg.as_bytes()
    if len(raw_message) > MAX_SES_RAW_MESSAGE_BYTES:
        raise RuntimeError(
            f"Accounting email is too large for the configured SES safety limit: "
            f"{len(raw_message)} > {MAX_SES_RAW_MESSAGE_BYTES} bytes"
        )
    send_args: Dict[str, Any] = {
        "Source": source,
        "Destinations": destinations,
        "RawMessage": {"Data": raw_message},
    }
    if configuration_set:
        send_args["ConfigurationSetName"] = configuration_set
    try:
        response = ses.send_raw_email(**send_args)
    except (ClientError, BotoCoreError) as exc:
        raise RuntimeError(f"SES send failed: {exc}") from exc
    return response.get("MessageId", "")


def run_creditnote_export_runner(args: argparse.Namespace) -> Dict[str, Any]:
    owner_project = (args.owner_project or DEFAULT_OWNER_PROJECT).strip().lower() or DEFAULT_OWNER_PROJECT
    project_settings = load_project_settings(owner_project)
    reporting_defaults = resolve_reporting_defaults(owner_project, project_settings)
    projects = parse_project_list(args.projects)
    skip_metrics = bool(getattr(args, "skip_metrics", False))
    skip_invoice_export = bool(getattr(args, "skip_invoice_export", False))
    date_from, date_to = resolve_creditnote_export_window(
        timezone_name=args.timezone,
        reference_date=args.reference_date,
        from_date=args.from_date,
        to_date=args.to_date,
    )
    output_dir = Path(args.output_dir) if str(args.output_dir or "").strip() else None
    print(
        "Running monthly accounting export "
        f"projects={','.join(projects)} window={date_from}..{date_to} "
        f"skip_email={args.skip_email} dry_run_email={args.dry_run_email} "
        f"skip_invoice_export={skip_invoice_export}"
    )

    creditnote_results: List[CreditnoteExportResult] = []
    try:
        for project in projects:
            creditnote_results.append(
                run_monthly_creditnote_export(
                    projects=(project,),
                    date_from=date_from,
                    date_to=date_to,
                    output_dir=output_dir,
                    output_tag=args.output_tag,
                )
            )
    except Exception:
        if not skip_metrics:
            put_metric("CreditnoteExportRunFailed", 1, owner_project, reporting_defaults)
        raise

    invoice_result: MoneyS3InvoiceExportResult | None = None
    if skip_invoice_export:
        print("Money S3 invoice export skipped by flag.")
    else:
        try:
            invoice_result = run_money_s3_invoice_export(
                projects=projects,
                date_from=date_from,
                date_to=date_to,
                output_dir=output_dir,
                output_tag=args.output_tag,
            )
        except Exception:
            if not skip_metrics:
                put_metric("InvoiceMoneyS3ExportRunFailed", 1, owner_project, reporting_defaults)
            raise

    result = MonthlyAccountingExportResult(
        creditnote_results=tuple(creditnote_results),
        invoice_result=invoice_result,
    )
    total_gross_credit = sum(
        abs(float(row.get("Suma_s_DPH") or 0.0))
        for creditnote_result in creditnote_results
        for row in creditnote_result.total_rows
    )
    revenue_included_orders = sum(
        int((creditnote_result.reporting_exclusion_summary or {}).get("included_in_revenue") or 0)
        for creditnote_result in creditnote_results
    )
    if skip_metrics:
        print("Monthly accounting export CloudWatch metrics skipped by flag.")
    else:
        put_metric("CreditnoteExportRows", result.exported_rows, owner_project, reporting_defaults)
        put_metric("CreditnoteExportGrossAmount", round(total_gross_credit, 2), owner_project, reporting_defaults)
        put_metric("CreditnoteExportRevenueIncludedOrders", revenue_included_orders, owner_project, reporting_defaults)
        put_metric("CreditnoteExportRunSucceeded", 1, owner_project, reporting_defaults)
        if invoice_result is not None:
            put_metric("InvoiceMoneyS3ExportRows", invoice_result.total_invoices, owner_project, reporting_defaults)
            put_metric("InvoiceMoneyS3ExportRunSucceeded", 1, owner_project, reporting_defaults)

    subject = build_creditnote_email_subject(result, args.email_subject)
    body_text = build_creditnote_email_body(result)
    message_id = ""
    if args.skip_email:
        print("Creditnote export email skipped by flag.")
    elif args.dry_run_email:
        message_id = "dry-run"
        print("Monthly accounting email dry-run; SES send skipped.")
    else:
        try:
            message_id = send_creditnote_email_ses(
                result=result,
                subject=subject,
                body_text=body_text,
                email_from=args.email_from,
                email_to=args.email_to,
                reporting_defaults=reporting_defaults,
            )
        except Exception:
            if not skip_metrics:
                put_metric("CreditnoteExportEmailFailed", 1, owner_project, reporting_defaults)
            raise
        if not skip_metrics:
            put_metric("CreditnoteExportEmailSent", 1, owner_project, reporting_defaults)
        print(f"SES monthly accounting export sent. MessageId={message_id}")

    summary = result.as_dict()
    summary.update(
        {
            "email_skipped": bool(args.skip_email),
            "email_dry_run": bool(args.dry_run_email),
            "email_message_id": message_id,
            "email_to": args.email_to,
            "metrics_skipped": skip_metrics,
            "subject": subject,
            "invoice_export_skipped": skip_invoice_export,
        }
    )
    print("CREDITNOTE_EXPORT_SUMMARY " + json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return summary


def main() -> None:
    load_dotenv(encoding="utf-8-sig")
    try:
        run_creditnote_export_runner(parse_args())
    except Exception as exc:
        message = " ".join(str(exc).splitlines())
        print(f"CREDITNOTE_EXPORT_RUN_FAILED type={type(exc).__name__} message={message}", flush=True)
        raise


if __name__ == "__main__":
    main()
