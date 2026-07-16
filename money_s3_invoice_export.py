#!/usr/bin/env python3
"""Monthly BizniWeb Money S3 invoice export helpers."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from email.message import Message
from email.parser import Parser
from pathlib import Path
from typing import Any, Dict, Sequence, Tuple

from dotenv import load_dotenv

from creditnote_export import (
    DEFAULT_CREDITNOTE_PROJECTS,
    ROOT_DIR,
    _login_admin,
    month_slug,
    parse_biznisweb_js_object,
    parse_date,
    parse_project_list,
)
from reporting_core import sanitize_output_tag


MONEY_S3_INVOICE_DATASET = "invoices"
MONEY_S3_FORMAT = "moneys3"


@dataclass(frozen=True)
class MoneyS3InvoiceExportResult:
    projects: Tuple[str, ...]
    date_from: str
    date_to: str
    output_files: Dict[str, Path]
    invoice_counts: Dict[str, int]
    source_filenames: Dict[str, str]

    @property
    def total_invoices(self) -> int:
        return sum(self.invoice_counts.values())

    def as_dict(self) -> Dict[str, Any]:
        return {
            "projects": list(self.projects),
            "date_from": self.date_from,
            "date_to": self.date_to,
            "format": MONEY_S3_FORMAT,
            "date_field": "inv_date",
            "output_files": {project: str(path) for project, path in self.output_files.items()},
            "invoice_counts": self.invoice_counts,
            "source_filenames": self.source_filenames,
            "total_invoices": self.total_invoices,
        }


def biznisweb_php_serialize(value: Any) -> str:
    """Serialize simple values the same way BizniWeb admin mass filters do."""
    if value is None:
        return "N;"
    if isinstance(value, bool):
        return f"b:{1 if value else 0};"
    if isinstance(value, int):
        return f"i:{value};"
    if isinstance(value, float):
        return f"d:{value};"
    if isinstance(value, str):
        return f's:{len(value.encode("utf-8"))}:"{value}";'
    if isinstance(value, (list, tuple)):
        pairs = "".join(
            biznisweb_php_serialize(index) + biznisweb_php_serialize(item)
            for index, item in enumerate(value)
        )
        return f"a:{len(value)}:{{{pairs}}}"
    if isinstance(value, dict):
        pairs = "".join(
            biznisweb_php_serialize(str(key)) + biznisweb_php_serialize(item)
            for key, item in value.items()
        )
        return f"a:{len(value)}:{{{pairs}}}"
    raise TypeError(f"Unsupported BizniWeb serialize type: {type(value).__name__}")


def biznisweb_admin_date(value: date) -> str:
    return f"{value.day}.{value.month}.{value.year}"


def money_s3_invoice_filters(date_from: date, date_to: date) -> Dict[str, str]:
    return {
        "inv_date_from": biznisweb_admin_date(date_from),
        "inv_date_to": biznisweb_admin_date(date_to),
    }


def build_money_s3_invoice_filename(project: str, date_from: date, date_to: date, output_tag: str = "") -> str:
    filename = f"faktury_money_s3_{project}_{month_slug(date_from, date_to)}_issued"
    tag = sanitize_output_tag(output_tag)
    if tag:
        filename = f"{filename}_{tag}"
    return filename


def _filename_from_content_disposition(value: str) -> str:
    if not value:
        return ""
    message: Message = Parser().parsestr(f"Content-Disposition: {value}\n\n")
    filename = message.get_param("filename", header="content-disposition")
    return str(filename or "").strip()


def _validate_money_s3_xml(content: bytes, project: str) -> None:
    stripped = content.lstrip()
    if stripped.startswith(b"<MoneyData"):
        return
    if stripped.startswith(b"<?xml") and b"<MoneyData" in stripped[:300]:
        return
    preview = stripped[:80].decode("utf-8", errors="replace")
    raise RuntimeError(f"Money S3 invoice export for project '{project}' did not return MoneyData XML: {preview!r}")


def download_money_s3_invoice_export(project: str, date_from: date, date_to: date) -> Tuple[bytes, str, int]:
    base_url, session, arf = _login_admin(project)
    mass_filter = biznisweb_php_serialize(money_s3_invoice_filters(date_from, date_to))
    count_response = session.post(
        f"{base_url}/erp/orders/invoices/getListJson",
        data={"start": 0, "limit": 1, "arf": arf, "massfilter": mass_filter},
    )
    count_response.raise_for_status()
    count_payload = parse_biznisweb_js_object(count_response.text)
    try:
        invoice_count = int(count_payload.get("total") or 0)
    except (TypeError, ValueError):
        invoice_count = 0

    response = session.post(
        f"{base_url}/erp/impexp/export/index/{MONEY_S3_INVOICE_DATASET}/{MONEY_S3_FORMAT}",
        data={
            "dataSubset": biznisweb_php_serialize({}),
            "data": MONEY_S3_INVOICE_DATASET,
            "massFilter": mass_filter,
            "arf": arf,
        },
        allow_redirects=True,
    )
    response.raise_for_status()
    _validate_money_s3_xml(response.content, project)
    source_filename = _filename_from_content_disposition(response.headers.get("content-disposition", ""))
    return response.content, source_filename, invoice_count


def run_money_s3_invoice_export(
    projects: Sequence[str] = DEFAULT_CREDITNOTE_PROJECTS,
    date_from: date | str = "",
    date_to: date | str = "",
    output_dir: Path | None = None,
    output_tag: str = "",
) -> MoneyS3InvoiceExportResult:
    root_env = ROOT_DIR / ".env"
    if root_env.exists():
        load_dotenv(dotenv_path=root_env, override=False, encoding="utf-8-sig")

    resolved_projects = parse_project_list(projects)
    if isinstance(date_from, str):
        date_from = parse_date(date_from, "date_from")
    if isinstance(date_to, str):
        date_to = parse_date(date_to, "date_to")
    if date_from > date_to:
        raise ValueError(f"date_from ({date_from}) cannot be after date_to ({date_to})")

    base_output_dir = output_dir or (ROOT_DIR / "data" / "combined_exports")
    base_output_dir.mkdir(parents=True, exist_ok=True)

    output_files: Dict[str, Path] = {}
    invoice_counts: Dict[str, int] = {}
    source_filenames: Dict[str, str] = {}
    for project in resolved_projects:
        content, source_filename, invoice_count = download_money_s3_invoice_export(project, date_from, date_to)
        filename = build_money_s3_invoice_filename(project, date_from, date_to, output_tag=output_tag)
        output_path = base_output_dir / f"{filename}.xml"
        output_path.write_bytes(content)
        output_files[project.upper()] = output_path
        invoice_counts[project.upper()] = invoice_count
        source_filenames[project.upper()] = re.sub(r"[\r\n]+", " ", source_filename).strip()

    return MoneyS3InvoiceExportResult(
        projects=tuple(resolved_projects),
        date_from=date_from.isoformat(),
        date_to=date_to.isoformat(),
        output_files=output_files,
        invoice_counts=invoice_counts,
        source_filenames=source_filenames,
    )
