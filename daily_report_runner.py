#!/usr/bin/env python3
"""
Daily report automation runner.

Runs export_orders.py for a fixed historical range ending at "yesterday"
in the configured timezone, then sends report links/attachments via AWS SES.
Optional S3 upload is supported.
"""

import argparse
import csv
import mimetypes
import os
import subprocess
import sys
from datetime import datetime, timedelta
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate, make_msgid
from pathlib import Path
from typing import Dict, List
from zoneinfo import ZoneInfo

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name, "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "y", "on"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate and email daily report")
    parser.add_argument(
        "--from-date",
        default=os.getenv("REPORT_FROM_DATE", "2025-05-03"),
        help="Start date in YYYY-MM-DD format (default: REPORT_FROM_DATE or 2025-05-03)",
    )
    parser.add_argument(
        "--to-date",
        default=os.getenv("REPORT_TO_DATE", ""),
        help="End date in YYYY-MM-DD format. If empty, uses yesterday in REPORT_TIMEZONE.",
    )
    parser.add_argument(
        "--timezone",
        default=os.getenv("REPORT_TIMEZONE", "Europe/Bratislava"),
        help="Timezone used for 'yesterday' calculation",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        default=False,
        help="Force fresh fetch from API",
    )
    parser.add_argument(
        "--clear-cache",
        action="store_true",
        default=False,
        help="Clear cache before export",
    )
    parser.add_argument(
        "--skip-export",
        action="store_true",
        help="Skip export step and only send latest files",
    )
    parser.add_argument(
        "--skip-email",
        action="store_true",
        help="Skip email sending",
    )
    return parser.parse_args()


def resolve_to_date(to_date_arg: str, tz_name: str) -> str:
    if to_date_arg:
        datetime.strptime(to_date_arg, "%Y-%m-%d")
        return to_date_arg

    tz = ZoneInfo(tz_name)
    yesterday = (datetime.now(tz).date() - timedelta(days=1)).strftime("%Y-%m-%d")
    return yesterday


def run_export(from_date: str, to_date: str, clear_cache: bool, no_cache: bool) -> None:
    cmd: List[str] = [
        sys.executable,
        str(ROOT_DIR / "export_orders.py"),
        "--from-date",
        from_date,
        "--to-date",
        to_date,
    ]
    if clear_cache:
        cmd.append("--clear-cache")
    if no_cache:
        cmd.append("--no-cache")

    print("Running export:", " ".join(cmd))
    proc = subprocess.run(cmd, cwd=str(ROOT_DIR), check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"export_orders.py failed with exit code {proc.returncode}")


def get_output_paths(from_date: str, to_date: str) -> Dict[str, Path]:
    compact_range = f"{from_date.replace('-', '')}-{to_date.replace('-', '')}"
    return {
        "report_html": DATA_DIR / f"report_{compact_range}.html",
        "email_strategy_html": DATA_DIR / f"email_strategy_{compact_range}.html",
        "export_csv": DATA_DIR / f"export_{compact_range}.csv",
        "aggregate_by_date_csv": DATA_DIR / f"aggregate_by_date_{compact_range}.csv",
        "aggregate_by_month_csv": DATA_DIR / f"aggregate_by_month_{compact_range}.csv",
    }


def s3_upload_outputs(paths: Dict[str, Path]) -> Dict[str, str]:
    bucket = os.getenv("REPORT_S3_BUCKET", "").strip()
    prefix = os.getenv("REPORT_S3_PREFIX", "daily-reports").strip().strip("/")
    region = os.getenv("AWS_REGION", "eu-central-1").strip()

    if not bucket:
        return {}

    try:
        import boto3  # type: ignore
    except ImportError as exc:
        raise RuntimeError("boto3 is required for S3 uploads. Install dependencies from requirements.txt.") from exc

    s3 = boto3.client("s3", region_name=region)
    uploaded_links: Dict[str, str] = {}
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    for key, path in paths.items():
        if not path.exists():
            continue
        object_key = f"{prefix}/{timestamp}/{path.name}"
        content_type, _ = mimetypes.guess_type(path.name)
        extra_args = {"ContentType": content_type} if content_type else {}

        s3.upload_file(str(path), bucket, object_key, ExtraArgs=extra_args)

        expires = int(os.getenv("REPORT_S3_PRESIGN_EXPIRES_SEC", "604800"))
        presigned = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": object_key},
            ExpiresIn=expires,
        )
        uploaded_links[key] = presigned

    return uploaded_links


def build_email_subject() -> str:
    return os.getenv("REPORT_EMAIL_SUBJECT", "Daily Vevo report").strip()


def _to_float(value: str) -> float:
    try:
        return float((value or "").strip())
    except (ValueError, TypeError, AttributeError):
        return 0.0


def _to_int(value: str) -> int:
    try:
        return int(float((value or "").strip()))
    except (ValueError, TypeError, AttributeError):
        return 0


def _fmt_eur(value: float) -> str:
    return f"€{value:,.2f}".replace(",", " ")


def _fmt_pct(value: float) -> str:
    return f"{value:.2f}%"


def build_report_summary(file_paths: Dict[str, Path]) -> str:
    date_csv = file_paths.get("aggregate_by_date_csv")
    if not date_csv or not date_csv.exists():
        return "Súhrn metrík nebol dostupný (chýba aggregate_by_date CSV)."

    rows: List[Dict[str, str]] = []
    with date_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("date"):
                rows.append(row)

    if not rows:
        return "Súhrn metrík nebol dostupný (aggregate_by_date CSV je prázdny)."

    rows = sorted(rows, key=lambda r: r["date"])
    total_revenue = sum(_to_float(r.get("total_revenue", "")) for r in rows)
    total_net_profit = sum(_to_float(r.get("net_profit", "")) for r in rows)
    total_orders = sum(_to_int(r.get("unique_orders", "")) for r in rows)
    total_fb_spend = sum(_to_float(r.get("fb_ads_spend", "")) for r in rows)
    total_google_spend = sum(_to_float(r.get("google_ads_spend", "")) for r in rows)
    total_ads = total_fb_spend + total_google_spend
    total_roas = (total_revenue / total_ads) if total_ads > 0 else 0.0
    aov = (total_revenue / total_orders) if total_orders > 0 else 0.0

    last = rows[-1]
    last_date = last["date"]
    last_orders = _to_int(last.get("unique_orders", ""))
    last_revenue = _to_float(last.get("total_revenue", ""))
    last_profit = _to_float(last.get("net_profit", ""))
    last_fb = _to_float(last.get("fb_ads_spend", ""))

    last_7 = rows[-7:]
    prev_7 = rows[-14:-7] if len(rows) >= 14 else []
    last_7_revenue = sum(_to_float(r.get("total_revenue", "")) for r in last_7)
    last_7_profit = sum(_to_float(r.get("net_profit", "")) for r in last_7)
    last_7_orders = sum(_to_int(r.get("unique_orders", "")) for r in last_7)
    prev_7_revenue = sum(_to_float(r.get("total_revenue", "")) for r in prev_7)
    prev_7_profit = sum(_to_float(r.get("net_profit", "")) for r in prev_7)
    revenue_trend = ((last_7_revenue - prev_7_revenue) / prev_7_revenue * 100) if prev_7_revenue > 0 else 0.0
    profit_trend = ((last_7_profit - prev_7_profit) / prev_7_profit * 100) if prev_7_profit > 0 else 0.0

    return (
        "Kľúčový slovný súhrn:\n"
        f"- Celé obdobie: {total_orders} objednávok, tržby {_fmt_eur(total_revenue)}, "
        f"netto zisk {_fmt_eur(total_net_profit)}, AOV {_fmt_eur(aov)}.\n"
        f"- Reklama: FB {_fmt_eur(total_fb_spend)}, Google {_fmt_eur(total_google_spend)}, "
        f"spolu {_fmt_eur(total_ads)}, ROAS {total_roas:.2f}x.\n"
        f"- Posledný deň ({last_date}): {last_orders} objednávok, tržby {_fmt_eur(last_revenue)}, "
        f"netto zisk {_fmt_eur(last_profit)}, FB spend {_fmt_eur(last_fb)}.\n"
        f"- Posledných 7 dní: {last_7_orders} objednávok, tržby {_fmt_eur(last_7_revenue)}, "
        f"netto zisk {_fmt_eur(last_7_profit)}, trend tržieb {_fmt_pct(revenue_trend)}, "
        f"trend zisku {_fmt_pct(profit_trend)}."
    )


def build_email_body(from_date: str, to_date: str, summary_text: str) -> str:
    return (
        "Dobrý deň,\n\n"
        "v prílohe posielam denný Vevo report v HTML formáte.\n"
        f"Sledované obdobie: {from_date} až {to_date}.\n\n"
        f"{summary_text}\n\n"
        "Tento email bol odoslaný automaticky zo systému Vevo reporting.\n"
    )


def send_email_ses(
    subject: str,
    body_text: str,
    file_paths: Dict[str, Path],
) -> str:
    region = os.getenv("AWS_REGION", "eu-central-1").strip()
    source = os.getenv("REPORT_EMAIL_FROM", "").strip()
    to_raw = os.getenv("REPORT_EMAIL_TO", "").strip()

    if not source:
        raise ValueError("REPORT_EMAIL_FROM is required")
    if not to_raw:
        raise ValueError("REPORT_EMAIL_TO is required")

    destinations = [email.strip() for email in to_raw.split(",") if email.strip()]
    if not destinations:
        raise ValueError("REPORT_EMAIL_TO has no valid recipients")

    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = source
    msg["To"] = ", ".join(destinations)
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid(domain="amazonses.com")
    msg["Reply-To"] = source

    # Keep a short non-empty body to reduce spam score.
    msg.attach(MIMEText(body_text, "plain", "utf-8"))

    def attach_file(path: Path) -> None:
        if path.suffix.lower() in {".html", ".htm"}:
            text = path.read_text(encoding="utf-8", errors="replace")
            part = MIMEText(text, "html", "utf-8")
        else:
            with path.open("rb") as f:
                part = MIMEApplication(f.read(), Name=path.name)
        part["Content-Disposition"] = f'attachment; filename="{path.name}"'
        msg.attach(part)

    if not file_paths["report_html"].exists():
        raise FileNotFoundError(f"Missing HTML report attachment: {file_paths['report_html']}")
    attach_file(file_paths["report_html"])

    try:
        import boto3  # type: ignore
        from botocore.exceptions import BotoCoreError, ClientError  # type: ignore
    except ImportError as exc:
        raise RuntimeError("boto3 is required for SES email sending. Install dependencies from requirements.txt.") from exc

    ses = boto3.client("ses", region_name=region)
    try:
        response = ses.send_raw_email(
            Source=source,
            Destinations=destinations,
            RawMessage={"Data": msg.as_string()},
        )
    except (ClientError, BotoCoreError) as exc:
        raise RuntimeError(f"SES send failed: {exc}") from exc

    return response.get("MessageId", "")


def main() -> None:
    load_dotenv()
    args = parse_args()

    to_date = resolve_to_date(args.to_date, args.timezone)
    from_date = args.from_date

    datetime.strptime(from_date, "%Y-%m-%d")
    datetime.strptime(to_date, "%Y-%m-%d")

    if from_date > to_date:
        raise ValueError(f"from_date ({from_date}) cannot be after to_date ({to_date})")

    use_clear_cache = args.clear_cache or env_bool("REPORT_FORCE_CLEAR_CACHE", False)
    use_no_cache = args.no_cache or env_bool("REPORT_FORCE_NO_CACHE", False)

    if not args.skip_export:
        run_export(
            from_date=from_date,
            to_date=to_date,
            clear_cache=use_clear_cache,
            no_cache=use_no_cache,
        )

    output_paths = get_output_paths(from_date, to_date)
    missing = [str(path) for path in output_paths.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Expected output files not found: {missing}")

    s3_upload_outputs(output_paths)

    if args.skip_email:
        print("Email sending skipped by flag.")
        return

    subject = build_email_subject()
    summary_text = build_report_summary(output_paths)
    body_text = build_email_body(from_date, to_date, summary_text)
    message_id = send_email_ses(
        subject=subject,
        body_text=body_text,
        file_paths=output_paths,
    )
    print(f"SES message sent. MessageId={message_id}")


if __name__ == "__main__":
    main()

