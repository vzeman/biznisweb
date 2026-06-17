#!/usr/bin/env python3
"""Scheduled monthly credit-note export runner."""

from __future__ import annotations

import argparse
import json
import os
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
from reporting_core import load_project_settings, put_metric, resolve_reporting_defaults


DEFAULT_OWNER_PROJECT = os.getenv("CREDITNOTE_EXPORT_OWNER_PROJECT", "roy").strip() or "roy"
DEFAULT_RECIPIENT = "mil.terem@gmail.com"


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name, "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "y", "on"}


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export monthly BizniWeb credit notes and email one PDF")
    parser.add_argument(
        "--projects",
        default=os.getenv("CREDITNOTE_EXPORT_PROJECTS", "roy,vevo"),
        help="Comma-separated project slugs to include in one PDF.",
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


def build_creditnote_email_subject(result: CreditnoteExportResult, override: str = "") -> str:
    if override.strip():
        return override.strip()
    projects = "+".join(project.upper() for project in result.projects)
    return f"Dobropisy {projects} {result.date_from[:7]}"


def _format_amount(value: Any) -> str:
    try:
        return f"{float(value):,.2f}".replace(",", " ")
    except (TypeError, ValueError):
        return "0.00"


def build_creditnote_email_body(result: CreditnoteExportResult) -> str:
    lines = [
        "Dobry den,",
        "",
        "v prilohe posielam mesacny PDF export dobropisov z BizniWebu.",
        f"Obdobie vytvorenia dobropisov: {result.date_from} az {result.date_to}.",
        f"E-shopy: {', '.join(project.upper() for project in result.projects)}.",
        f"Pocet dobropisov: {result.exported_rows}.",
        "",
        "Sumar:",
    ]
    if result.summary_rows:
        for row in result.summary_rows:
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
    result: CreditnoteExportResult,
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
    if not result.output_pdf.exists():
        raise FileNotFoundError(f"Missing creditnote PDF: {result.output_pdf}")

    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = source
    msg["To"] = ", ".join(destinations)
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid(domain="amazonses.com")
    msg["Reply-To"] = source
    msg.attach(MIMEText(body_text, "plain", "utf-8"))

    with result.output_pdf.open("rb") as fh:
        part = MIMEApplication(fh.read(), _subtype="pdf", Name=result.output_pdf.name)
    part["Content-Disposition"] = f'attachment; filename="{result.output_pdf.name}"'
    msg.attach(part)

    try:
        import boto3  # type: ignore
        from botocore.exceptions import BotoCoreError, ClientError  # type: ignore
    except ImportError as exc:
        raise RuntimeError("boto3 is required for SES email sending. Install dependencies from requirements.txt.") from exc

    region = os.getenv("AWS_REGION", "eu-central-1").strip()
    configuration_set = os.getenv("SES_CONFIGURATION_SET", reporting_defaults.get("ses_configuration_set", "")).strip()
    ses = boto3.client("ses", region_name=region)
    send_args: Dict[str, Any] = {
        "Source": source,
        "Destinations": destinations,
        "RawMessage": {"Data": msg.as_string()},
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
    date_from, date_to = resolve_creditnote_export_window(
        timezone_name=args.timezone,
        reference_date=args.reference_date,
        from_date=args.from_date,
        to_date=args.to_date,
    )
    output_dir = Path(args.output_dir) if str(args.output_dir or "").strip() else None
    print(
        "Running monthly creditnote export "
        f"projects={','.join(projects)} window={date_from}..{date_to} "
        f"skip_email={args.skip_email} dry_run_email={args.dry_run_email}"
    )

    try:
        result = run_monthly_creditnote_export(
            projects=projects,
            date_from=date_from,
            date_to=date_to,
            output_dir=output_dir,
            output_tag=args.output_tag,
        )
    except Exception:
        put_metric("CreditnoteExportRunFailed", 1, owner_project, reporting_defaults)
        raise

    put_metric("CreditnoteExportRows", result.exported_rows, owner_project, reporting_defaults)
    put_metric("CreditnoteExportRunSucceeded", 1, owner_project, reporting_defaults)

    subject = build_creditnote_email_subject(result, args.email_subject)
    body_text = build_creditnote_email_body(result)
    message_id = ""
    if args.skip_email:
        print("Creditnote export email skipped by flag.")
    elif args.dry_run_email:
        message_id = "dry-run"
        print("Creditnote export email dry-run; SES send skipped.")
    else:
        message_id = send_creditnote_email_ses(
            result=result,
            subject=subject,
            body_text=body_text,
            email_from=args.email_from,
            email_to=args.email_to,
            reporting_defaults=reporting_defaults,
        )
        put_metric("CreditnoteExportEmailSent", 1, owner_project, reporting_defaults)
        print(f"SES creditnote export sent. MessageId={message_id}")

    summary = result.as_dict()
    summary.update(
        {
            "email_skipped": bool(args.skip_email),
            "email_dry_run": bool(args.dry_run_email),
            "email_message_id": message_id,
            "email_to": args.email_to,
            "subject": subject,
        }
    )
    print("CREDITNOTE_EXPORT_SUMMARY " + json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return summary


def main() -> None:
    load_dotenv(encoding="utf-8-sig")
    run_creditnote_export_runner(parse_args())


if __name__ == "__main__":
    main()
