#!/usr/bin/env python3
"""Monthly BizniWeb credit-note export helpers."""

from __future__ import annotations

import ast
import json
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
from dotenv import load_dotenv

from http_client import build_retry_session, resolve_timeout
from reporting_core import (
    derive_biznisweb_base_url,
    load_project_env,
    load_project_settings,
    project_dir,
    resolve_biznisweb_api_url,
    sanitize_output_tag,
)


ROOT_DIR = Path(__file__).resolve().parent
DEFAULT_CREDITNOTE_PROJECTS = ("roy", "vevo")
DEFAULT_PAGE_LIMIT = 100
WEB_TIMEOUT = resolve_timeout(os.getenv("BIZNISWEB_WEB_TIMEOUT_SEC"))
EXPORT_COLUMNS = [
    "Eshop",
    "Dobropis cislo",
    "Dobropis ID",
    "Vytvorene",
    "Datum vystavenia",
    "Splatnost",
    "Objednavka",
    "Order ID",
    "Faktura",
    "Zakaznik",
    "Email",
    "Stat dorucenia",
    "PSC dorucenia",
    "Mena",
    "Suma bez DPH",
    "Suma s DPH",
    "Suma bez DPH text",
    "Suma s DPH text",
    "Na vratenie",
    "Uz vratene",
    "Datum vratenia",
    "Refund type",
    "Dovod",
    "Storno",
    "Vytvoril",
    "Variabilny symbol",
    "Povodny nakup",
    "Tax OSS",
    "Tax OSS country",
    "Tax excl",
    "Internal note",
]


class _SilentLogger:
    def info(self, *_: Any, **__: Any) -> None:
        pass

    def warning(self, *_: Any, **__: Any) -> None:
        pass

    def error(self, *_: Any, **__: Any) -> None:
        pass


@dataclass(frozen=True)
class CreditnoteExportResult:
    projects: Tuple[str, ...]
    date_from: str
    date_to: str
    output_xlsx: Path
    output_json: Path
    exported_rows: int
    project_counts: Dict[str, int]
    fetch_totals: Dict[str, Dict[str, int]]
    summary_rows: List[Dict[str, Any]]

    def as_dict(self) -> Dict[str, Any]:
        return {
            "projects": list(self.projects),
            "date_from": self.date_from,
            "date_to": self.date_to,
            "output_xlsx": str(self.output_xlsx),
            "output_json": str(self.output_json),
            "exported_rows": self.exported_rows,
            "project_counts": self.project_counts,
            "fetch_totals": self.fetch_totals,
            "summary_rows": self.summary_rows,
        }


def parse_project_list(value: str | Sequence[str] | None) -> Tuple[str, ...]:
    if value is None:
        return DEFAULT_CREDITNOTE_PROJECTS
    if isinstance(value, str):
        raw_items = value.split(",")
    else:
        raw_items = list(value)
    projects = tuple(item.strip().lower() for item in raw_items if str(item).strip())
    return projects or DEFAULT_CREDITNOTE_PROJECTS


def parse_biznisweb_js_object(text: str) -> Dict[str, Any]:
    """Parse BizniWeb admin pseudo-JSON responses without a JavaScript runtime."""
    source = (text or "").strip()
    if not source:
        return {}
    normalized = re.sub(r"\bnull\b", "None", source)
    normalized = re.sub(r"\btrue\b", "True", normalized)
    normalized = re.sub(r"\bfalse\b", "False", normalized)
    try:
        parsed = ast.literal_eval(normalized)
    except (SyntaxError, ValueError):
        # Some admin endpoints use bare top-level keys, e.g. {success: true}.
        quoted = re.sub(r"([{\[,]\s*)([A-Za-z_][A-Za-z0-9_]*)\s*:", r"\1'\2':", normalized)
        parsed = ast.literal_eval(quoted)
    if not isinstance(parsed, dict):
        raise ValueError("BizniWeb response is not an object")
    return parsed


def parse_date(value: str, label: str) -> date:
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"{label} must use YYYY-MM-DD format, got '{value}'") from exc


def previous_calendar_month(reference: date) -> Tuple[date, date]:
    first_this_month = reference.replace(day=1)
    last_previous_month = first_this_month - timedelta(days=1)
    first_previous_month = last_previous_month.replace(day=1)
    return first_previous_month, last_previous_month


def parse_creditnote_datetime(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d.%m.%Y %H:%M:%S", "%d.%m.%Y"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def parse_money(value: Any) -> Tuple[Optional[float], str]:
    text = str(value or "").strip()
    if not text:
        return None, ""
    currency = re.sub(r"[\d\s.,\-+]", "", text).strip()
    normalized = re.sub(r"[^\d,\.\-+]", "", text)
    if not normalized:
        return None, currency
    if "," in normalized and "." in normalized:
        if normalized.rfind(",") > normalized.rfind("."):
            normalized = normalized.replace(".", "").replace(",", ".")
        else:
            normalized = normalized.replace(",", "")
    else:
        normalized = normalized.replace(",", ".")
    try:
        return float(Decimal(normalized)), currency
    except (InvalidOperation, ValueError):
        return None, currency


def signed_credit_amount(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return -abs(float(value))


def first_currency(*values: Any) -> str:
    for value in values:
        _, currency = parse_money(value)
        if currency:
            return currency
    return ""


def month_slug(date_from: date, date_to: date) -> str:
    if date_from.day == 1 and (date_to + timedelta(days=1)).day == 1 and date_from.year == date_to.year:
        return date_from.strftime("%Y-%m")
    return f"{date_from:%Y-%m-%d}_{date_to:%Y-%m-%d}"


def build_export_filename(projects: Sequence[str], date_from: date, date_to: date, output_tag: str = "") -> str:
    project_slug = "_".join(projects)
    filename = f"dobropisy_actual_{project_slug}_{month_slug(date_from, date_to)}_created"
    tag = sanitize_output_tag(output_tag)
    if tag:
        filename = f"{filename}_{tag}"
    return filename


def _extract_arf(text: str) -> str:
    match = re.search(r"[?&]arf=([a-zA-Z0-9]+)", text or "")
    if not match:
        match = re.search(
            r"CsrfToken\s*=\s*function\s*\(\)\s*\{\s*var\s+\w+\s*=\s*'([a-zA-Z0-9]+)'",
            text or "",
        )
    return match.group(1) if match else ""


def _login_admin(project: str):
    if (project_dir(project) / ".env").exists():
        load_project_env(project, logger=_SilentLogger())
    settings = load_project_settings(project)
    api_url = resolve_biznisweb_api_url(project, settings)
    base_url = derive_biznisweb_base_url(api_url)
    prefix = project.upper().replace("-", "_")
    username = os.getenv(f"{prefix}_BIZNISWEB_USERNAME") or os.getenv("BIZNISWEB_USERNAME")
    password = os.getenv(f"{prefix}_BIZNISWEB_PASSWORD") or os.getenv("BIZNISWEB_PASSWORD")
    if not username or not password:
        raise RuntimeError(
            f"BIZNISWEB_USERNAME/BIZNISWEB_PASSWORD missing for project '{project}' "
            f"(or {prefix}_BIZNISWEB_USERNAME/{prefix}_BIZNISWEB_PASSWORD)"
        )

    session = build_retry_session(timeout=WEB_TIMEOUT, allowed_methods=frozenset({"GET", "POST"}))
    login_page = session.get(f"{base_url}/erp/main/login")
    login_page.raise_for_status()
    arf = _extract_arf(login_page.text)
    response = session.post(
        f"{base_url}/admin/login/authenticate/",
        data={"username": username, "password": password, "res": "1890x900", "arf": arf},
        allow_redirects=True,
    )
    response.raise_for_status()
    try:
        payload = response.json()
        arf = payload.get("arf") or arf
    except Exception:
        arf = _extract_arf(response.text) or arf
    return base_url, session, arf


def fetch_project_creditnotes(project: str, page_limit: int = DEFAULT_PAGE_LIMIT) -> Tuple[List[Dict[str, Any]], int]:
    base_url, session, arf = _login_admin(project)
    rows: List[Dict[str, Any]] = []
    reported_total: Optional[int] = None
    start = 0

    while True:
        response = session.post(
            f"{base_url}/erp/orders/creditnotes/getListJson",
            data={"start": start, "limit": page_limit, "arf": arf},
        )
        response.raise_for_status()
        payload = parse_biznisweb_js_object(response.text)
        page_rows = payload.get("rows") or []
        if not isinstance(page_rows, list):
            raise RuntimeError(f"Unexpected creditnote rows payload for project '{project}'")
        if reported_total is None:
            try:
                reported_total = int(payload.get("total") or 0)
            except (TypeError, ValueError):
                reported_total = 0
        rows.extend(page_rows)
        if not page_rows:
            break
        start += page_limit
        if reported_total is not None and start >= reported_total:
            break

    return rows, int(reported_total or len(rows))


def build_creditnote_export_rows(
    project: str,
    raw_rows: Iterable[Dict[str, Any]],
    date_from: date,
    date_to: date,
) -> List[Dict[str, Any]]:
    start_dt = datetime.combine(date_from, time.min)
    end_exclusive = datetime.combine(date_to + timedelta(days=1), time.min)
    export_rows: List[Dict[str, Any]] = []

    for row in raw_rows:
        created_dt = parse_creditnote_datetime(row.get("created"))
        if created_dt is None or not (start_dt <= created_dt < end_exclusive):
            continue
        number = str(row.get("number") or "").strip()
        if not number:
            continue

        net_value, _ = parse_money(row.get("price"))
        gross_value, _ = parse_money(row.get("taxed_price"))
        to_repay_value, _ = parse_money(row.get("to_repay"))
        already_repaid_value, _ = parse_money(row.get("already_repaid"))
        currency = first_currency(row.get("taxed_price"), row.get("currencied_price"), row.get("currencied_to_repay"))

        export_rows.append(
            {
                "Eshop": project.upper(),
                "Dobropis cislo": number,
                "Dobropis ID": row.get("creditnote_id"),
                "Vytvorene": row.get("created"),
                "Datum vystavenia": row.get("issue_date"),
                "Splatnost": row.get("due_date"),
                "Objednavka": row.get("order_num"),
                "Order ID": row.get("order_id"),
                "Faktura": row.get("inv_id"),
                "Zakaznik": row.get("customer"),
                "Email": row.get("email"),
                "Stat dorucenia": row.get("delivery_country"),
                "PSC dorucenia": row.get("delivery_zip"),
                "Mena": currency,
                "Suma bez DPH": signed_credit_amount(net_value),
                "Suma s DPH": signed_credit_amount(gross_value),
                "Suma bez DPH text": row.get("currencied_price"),
                "Suma s DPH text": row.get("taxed_price"),
                "Na vratenie": signed_credit_amount(to_repay_value),
                "Uz vratene": signed_credit_amount(already_repaid_value),
                "Datum vratenia": row.get("repay_date"),
                "Refund type": row.get("refund_type"),
                "Dovod": row.get("reason"),
                "Storno": row.get("storno"),
                "Vytvoril": row.get("created_name"),
                "Variabilny symbol": row.get("var_symb"),
                "Povodny nakup": row.get("buy_date"),
                "Tax OSS": row.get("tax_oss"),
                "Tax OSS country": row.get("tax_oss_country"),
                "Tax excl": row.get("tax_excl"),
                "Internal note": row.get("internal_note"),
            }
        )

    return export_rows


def build_summary_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["Eshop", "Mena", "Pocet", "Suma_bez_DPH", "Suma_s_DPH"])
    return (
        df.groupby(["Eshop", "Mena"], dropna=False)
        .agg(Pocet=("Dobropis cislo", "count"), Suma_bez_DPH=("Suma bez DPH", "sum"), Suma_s_DPH=("Suma s DPH", "sum"))
        .reset_index()
    )


def write_creditnote_workbook(
    export_rows: Sequence[Dict[str, Any]],
    fetch_totals: Dict[str, Dict[str, int]],
    output_xlsx: Path,
    output_json: Path,
    date_from: date,
    date_to: date,
    projects: Sequence[str],
) -> CreditnoteExportResult:
    output_xlsx.parent.mkdir(parents=True, exist_ok=True)
    output_json.parent.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(list(export_rows), columns=EXPORT_COLUMNS)
    if not df.empty:
        df = df.sort_values(["Eshop", "Vytvorene", "Dobropis cislo"]).reset_index(drop=True)
    summary = build_summary_frame(df)
    fetch_summary = pd.DataFrame(
        [
            {"Eshop": project.upper(), **fetch_totals.get(project, {})}
            for project in projects
        ]
    )
    project_counts = {
        project.upper(): int((df["Eshop"] == project.upper()).sum()) if not df.empty else 0
        for project in projects
    }

    source_payload = {
        "date_filter": {
            "created_from": date_from.isoformat(),
            "created_to_inclusive": date_to.isoformat(),
        },
        "source_endpoint": "/erp/orders/creditnotes/getListJson",
        "projects": fetch_totals,
        "rows": list(export_rows),
    }
    output_json.write_text(json.dumps(source_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    with pd.ExcelWriter(output_xlsx, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Dobropisy", index=False)
        summary.to_excel(writer, sheet_name="Sumar", index=False)
        fetch_summary.to_excel(writer, sheet_name="Kontrola_fetch", index=False)
        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            worksheet.freeze_panes = "A2"
            for column_cells in worksheet.columns:
                max_len = 0
                column_letter = column_cells[0].column_letter
                for cell in column_cells[:200]:
                    if cell.value is not None:
                        max_len = max(max_len, len(str(cell.value)))
                worksheet.column_dimensions[column_letter].width = min(max(max_len + 2, 10), 45)
            if sheet_name == "Dobropisy":
                for row in worksheet.iter_rows(min_row=2, min_col=15, max_col=20):
                    for cell in row:
                        if isinstance(cell.value, (int, float)):
                            cell.number_format = "#,##0.00"
            if sheet_name == "Sumar":
                for row in worksheet.iter_rows(min_row=2, min_col=4, max_col=5):
                    for cell in row:
                        if isinstance(cell.value, (int, float)):
                            cell.number_format = "#,##0.00"

    return CreditnoteExportResult(
        projects=tuple(projects),
        date_from=date_from.isoformat(),
        date_to=date_to.isoformat(),
        output_xlsx=output_xlsx,
        output_json=output_json,
        exported_rows=len(df),
        project_counts=project_counts,
        fetch_totals=fetch_totals,
        summary_rows=summary.to_dict(orient="records"),
    )


def run_monthly_creditnote_export(
    projects: Sequence[str] = DEFAULT_CREDITNOTE_PROJECTS,
    date_from: date | str = "",
    date_to: date | str = "",
    output_dir: Optional[Path] = None,
    output_tag: str = "",
) -> CreditnoteExportResult:
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

    export_rows: List[Dict[str, Any]] = []
    fetch_totals: Dict[str, Dict[str, int]] = {}
    for project in resolved_projects:
        raw_rows, reported_total = fetch_project_creditnotes(project)
        project_rows = build_creditnote_export_rows(project, raw_rows, date_from, date_to)
        export_rows.extend(project_rows)
        fetch_totals[project] = {
            "reported_total": reported_total,
            "fetched_rows": len(raw_rows),
            "exported_rows": len(project_rows),
        }

    base_output_dir = output_dir or (ROOT_DIR / "data" / "combined_exports")
    filename = build_export_filename(resolved_projects, date_from, date_to, output_tag=output_tag)
    return write_creditnote_workbook(
        export_rows=export_rows,
        fetch_totals=fetch_totals,
        output_xlsx=base_output_dir / f"{filename}.xlsx",
        output_json=base_output_dir / f"{filename}_source.json",
        date_from=date_from,
        date_to=date_to,
        projects=resolved_projects,
    )
