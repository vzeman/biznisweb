#!/usr/bin/env python3
"""Monthly BizniWeb credit-note export helpers."""

from __future__ import annotations

import ast
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
CURRENCY_ALIASES = {
    "\u00e2\u201a\u00ac": "\u20ac",
    "K\u00e8": "K\u010d",
    "K\u00c4\u008d": "K\u010d",
}
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
    output_pdf: Path
    exported_rows: int
    project_counts: Dict[str, int]
    fetch_totals: Dict[str, Dict[str, int]]
    summary_rows: List[Dict[str, Any]]

    def as_dict(self) -> Dict[str, Any]:
        return {
            "projects": list(self.projects),
            "date_from": self.date_from,
            "date_to": self.date_to,
            "output_pdf": str(self.output_pdf),
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
    raw_currency = re.sub(r"[\d\s.,\-+]", "", text).strip()
    currency = CURRENCY_ALIASES.get(raw_currency, raw_currency)
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


def _pdf_font_candidates() -> Sequence[Path]:
    return (
        ROOT_DIR / "assets" / "fonts" / "DejaVuSans.ttf",
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
        Path("/usr/share/fonts/dejavu/DejaVuSans.ttf"),
        Path("C:/Windows/Fonts/arial.ttf"),
        Path("C:/Windows/Fonts/DejaVuSans.ttf"),
    )


def _pdf_bold_font_candidates(regular_path: Path) -> Sequence[Path]:
    return (
        regular_path.with_name("DejaVuSans-Bold.ttf"),
        regular_path.with_name("arialbd.ttf"),
        ROOT_DIR / "assets" / "fonts" / "DejaVuSans-Bold.ttf",
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"),
        Path("/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf"),
        Path("C:/Windows/Fonts/arialbd.ttf"),
    )


def _register_pdf_fonts() -> Dict[str, str]:
    try:
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise RuntimeError("PDF export requires reportlab. Install dependencies from requirements.txt.") from exc

    registered = set(pdfmetrics.getRegisteredFontNames())
    for path in _pdf_font_candidates():
        if path.exists():
            if "CreditnoteSans" not in registered:
                pdfmetrics.registerFont(TTFont("CreditnoteSans", str(path)))
            bold_font = "CreditnoteSans"
            for bold_path in _pdf_bold_font_candidates(path):
                if bold_path.exists():
                    if "CreditnoteSans-Bold" not in registered:
                        pdfmetrics.registerFont(TTFont("CreditnoteSans-Bold", str(bold_path)))
                    bold_font = "CreditnoteSans-Bold"
                    break
            return {"regular": "CreditnoteSans", "bold": bold_font}
    return {"regular": "Helvetica", "bold": "Helvetica-Bold"}


def _pdf_text(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    text = str(value).strip()
    return text if text else fallback


def _format_pdf_amount(value: Any, currency: Any = "") -> str:
    try:
        amount = float(value)
    except (TypeError, ValueError):
        raw = _pdf_text(value)
        return raw if raw else "-"
    text = f"{amount:,.2f}".replace(",", " ")
    currency_text = _pdf_text(currency)
    return f"{text} {currency_text}".strip()


def _wrap_pdf_text(value: Any, max_width: float, font_name: str, font_size: int, max_lines: int = 2) -> List[str]:
    from reportlab.pdfbase import pdfmetrics

    words: List[str] = []
    for word in _pdf_text(value).split():
        if pdfmetrics.stringWidth(word, font_name, font_size) <= max_width:
            words.append(word)
            continue
        chunk = ""
        for char in word:
            candidate = f"{chunk}{char}"
            if chunk and pdfmetrics.stringWidth(candidate, font_name, font_size) > max_width:
                words.append(chunk)
                chunk = char
            else:
                chunk = candidate
        if chunk:
            words.append(chunk)

    if not words:
        return [""]

    lines: List[str] = []
    current = ""
    for word in words:
        candidate = f"{current} {word}".strip()
        if pdfmetrics.stringWidth(candidate, font_name, font_size) <= max_width:
            current = candidate
            continue
        if current:
            lines.append(current)
        current = word
    if current:
        lines.append(current)

    if len(lines) > max_lines:
        lines = lines[:max_lines]
        suffix = "..."
        while lines[-1] and pdfmetrics.stringWidth(lines[-1] + suffix, font_name, font_size) > max_width:
            lines[-1] = lines[-1][:-1]
        lines[-1] = lines[-1].rstrip() + suffix
    return lines or [""]


def _draw_pdf_text_lines(
    pdf: Any,
    lines: Sequence[str],
    x: float,
    y: float,
    font_name: str,
    font_size: int,
    leading: float,
) -> None:
    pdf.setFont(font_name, font_size)
    for offset, line in enumerate(lines):
        pdf.drawString(x, y - offset * leading, line)


def _draw_creditnote_pdf_footer(pdf: Any, width: float, page_no: int, fonts: Dict[str, str]) -> None:
    from reportlab.lib.units import mm

    pdf.setFont(fonts["regular"], 7)
    pdf.setFillColorRGB(0.35, 0.38, 0.42)
    pdf.drawString(12 * mm, 8 * mm, f"BiznisWeb creditnote export - page {page_no}")
    pdf.drawRightString(width - 12 * mm, 8 * mm, datetime.now().strftime("%Y-%m-%d %H:%M"))
    pdf.setFillColorRGB(0, 0, 0)


def _draw_creditnote_detail_header(pdf: Any, x: float, y: float, columns: Sequence[Tuple[str, float]], fonts: Dict[str, str]) -> None:
    from reportlab.lib import colors
    from reportlab.lib.units import mm

    pdf.setFillColor(colors.HexColor("#eef2f7"))
    pdf.setStrokeColor(colors.HexColor("#cbd5e1"))
    pdf.rect(x, y - 7 * mm, sum(width for _, width in columns), 7 * mm, fill=1, stroke=1)
    pdf.setFillColor(colors.HexColor("#0f172a"))
    pdf.setFont(fonts["bold"], 7)
    current_x = x
    for label, width in columns:
        pdf.drawString(current_x + 1.2 * mm, y - 4.7 * mm, label)
        current_x += width
    pdf.setFillColor(colors.black)


def _draw_creditnote_pdf(
    rows: pd.DataFrame,
    summary: pd.DataFrame,
    fetch_totals: Dict[str, Dict[str, int]],
    output_pdf: Path,
    date_from: date,
    date_to: date,
    projects: Sequence[str],
) -> None:
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.lib.units import mm
        from reportlab.pdfgen import canvas as pdf_canvas
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise RuntimeError("PDF export requires reportlab. Install dependencies from requirements.txt.") from exc

    output_pdf.parent.mkdir(parents=True, exist_ok=True)
    fonts = _register_pdf_fonts()
    pdf = pdf_canvas.Canvas(str(output_pdf), pagesize=landscape(A4), pageCompression=1)
    width, height = landscape(A4)
    margin_x = 12 * mm
    top_y = height - 12 * mm
    page_no = 1

    pdf.setTitle(f"Dobropisy {'+'.join(project.upper() for project in projects)} {month_slug(date_from, date_to)}")
    pdf.setFont(fonts["bold"], 18)
    pdf.drawString(margin_x, top_y, "Mesacny export dobropisov")
    pdf.setFont(fonts["regular"], 10)
    pdf.drawString(margin_x, top_y - 9 * mm, f"Obdobie vytvorenia: {date_from.isoformat()} az {date_to.isoformat()}")
    pdf.drawString(margin_x, top_y - 15 * mm, f"E-shopy: {', '.join(project.upper() for project in projects)}")
    pdf.drawString(margin_x, top_y - 21 * mm, f"Pocet dobropisov: {len(rows)}")
    pdf.drawString(margin_x, top_y - 27 * mm, "Zdroj: BiznisWeb admin credit-note export")

    y = top_y - 42 * mm
    pdf.setFont(fonts["bold"], 12)
    pdf.drawString(margin_x, y, "Sumar")
    y -= 7 * mm
    summary_columns = [("Eshop", 26 * mm), ("Mena", 24 * mm), ("Pocet", 22 * mm), ("Suma bez DPH", 38 * mm), ("Suma s DPH", 38 * mm)]
    _draw_creditnote_detail_header(pdf, margin_x, y, summary_columns, fonts)
    y -= 8 * mm
    pdf.setFont(fonts["regular"], 8)
    if summary.empty:
        pdf.drawString(margin_x, y, "Bez dobropisov v zadanom obdobi.")
        y -= 8 * mm
    else:
        for row in summary.to_dict(orient="records"):
            values = [
                _pdf_text(row.get("Eshop"), "-"),
                _pdf_text(row.get("Mena"), "-"),
                str(int(row.get("Pocet") or 0)),
                _format_pdf_amount(row.get("Suma_bez_DPH"), row.get("Mena")),
                _format_pdf_amount(row.get("Suma_s_DPH"), row.get("Mena")),
            ]
            current_x = margin_x
            for value, (_, col_width) in zip(values, summary_columns):
                pdf.drawString(current_x + 1.2 * mm, y, value)
                current_x += col_width
            y -= 6 * mm

    y -= 7 * mm
    pdf.setFont(fonts["bold"], 12)
    pdf.drawString(margin_x, y, "Kontrola fetch")
    y -= 7 * mm
    fetch_columns = [("Eshop", 26 * mm), ("Reported", 30 * mm), ("Fetched", 30 * mm), ("Exported", 30 * mm)]
    _draw_creditnote_detail_header(pdf, margin_x, y, fetch_columns, fonts)
    y -= 8 * mm
    pdf.setFont(fonts["regular"], 8)
    for project in projects:
        totals = fetch_totals.get(project, {})
        values = [
            project.upper(),
            str(totals.get("reported_total", 0)),
            str(totals.get("fetched_rows", 0)),
            str(totals.get("exported_rows", 0)),
        ]
        current_x = margin_x
        for value, (_, col_width) in zip(values, fetch_columns):
            pdf.drawString(current_x + 1.2 * mm, y, value)
            current_x += col_width
        y -= 6 * mm

    _draw_creditnote_pdf_footer(pdf, width, page_no, fonts)
    pdf.showPage()
    page_no += 1

    detail_columns = [
        ("Eshop", 14 * mm, "Eshop"),
        ("Dobropis", 28 * mm, "Dobropis cislo"),
        ("Vytvorene", 31 * mm, "Vytvorene"),
        ("Vystavene", 24 * mm, "Datum vystavenia"),
        ("Objednavka", 28 * mm, "Objednavka"),
        ("Faktura", 28 * mm, "Faktura"),
        ("Zakaznik", 49 * mm, "Zakaznik"),
        ("Email", 50 * mm, "Email"),
        ("Suma s DPH", 21 * mm, "Suma s DPH"),
    ]
    header_columns = [(label, col_width) for label, col_width, _ in detail_columns]

    def start_detail_page() -> float:
        pdf.setFont(fonts["bold"], 14)
        pdf.drawString(margin_x, top_y, "Detail dobropisov")
        pdf.setFont(fonts["regular"], 8)
        pdf.drawRightString(width - margin_x, top_y, f"{date_from.isoformat()} - {date_to.isoformat()}")
        header_y = top_y - 9 * mm
        _draw_creditnote_detail_header(pdf, margin_x, header_y, header_columns, fonts)
        return header_y - 9 * mm

    y = start_detail_page()
    row_fill = False
    if rows.empty:
        pdf.setFont(fonts["regular"], 10)
        pdf.drawString(margin_x, y, "Bez dobropisov v zadanom obdobi.")
    else:
        for row in rows.to_dict(orient="records"):
            cell_lines: List[List[str]] = []
            for _, col_width, key in detail_columns:
                if key == "Suma s DPH":
                    value = _format_pdf_amount(row.get("Suma s DPH"), row.get("Mena"))
                else:
                    value = row.get(key)
                cell_lines.append(_wrap_pdf_text(value, col_width - 2.4 * mm, fonts["regular"], 7, max_lines=2))
            line_count = max(len(lines) for lines in cell_lines)
            row_height = max(7 * mm, (line_count * 3.7 + 3.5) * mm)
            if y - row_height < 14 * mm:
                _draw_creditnote_pdf_footer(pdf, width, page_no, fonts)
                pdf.showPage()
                page_no += 1
                y = start_detail_page()
                row_fill = False
            if row_fill:
                pdf.setFillColor(colors.HexColor("#f8fafc"))
                pdf.rect(margin_x, y - row_height + 1 * mm, sum(col_width for _, col_width, _ in detail_columns), row_height, fill=1, stroke=0)
                pdf.setFillColor(colors.black)
            pdf.setStrokeColor(colors.HexColor("#e2e8f0"))
            pdf.line(margin_x, y - row_height + 1 * mm, margin_x + sum(col_width for _, col_width, _ in detail_columns), y - row_height + 1 * mm)
            current_x = margin_x
            for lines, (_, col_width, _) in zip(cell_lines, detail_columns):
                _draw_pdf_text_lines(pdf, lines, current_x + 1.2 * mm, y - 3.5 * mm, fonts["regular"], 7, 3.7 * mm)
                current_x += col_width
            y -= row_height
            row_fill = not row_fill

    _draw_creditnote_pdf_footer(pdf, width, page_no, fonts)
    pdf.save()


def write_creditnote_pdf(
    export_rows: Sequence[Dict[str, Any]],
    fetch_totals: Dict[str, Dict[str, int]],
    output_pdf: Path,
    date_from: date,
    date_to: date,
    projects: Sequence[str],
) -> CreditnoteExportResult:
    df = pd.DataFrame(list(export_rows), columns=EXPORT_COLUMNS)
    if not df.empty:
        df = df.sort_values(["Eshop", "Vytvorene", "Dobropis cislo"]).reset_index(drop=True)
    summary = build_summary_frame(df)
    project_counts = {
        project.upper(): int((df["Eshop"] == project.upper()).sum()) if not df.empty else 0
        for project in projects
    }

    _draw_creditnote_pdf(
        rows=df,
        summary=summary,
        fetch_totals=fetch_totals,
        output_pdf=output_pdf,
        date_from=date_from,
        date_to=date_to,
        projects=projects,
    )

    return CreditnoteExportResult(
        projects=tuple(projects),
        date_from=date_from.isoformat(),
        date_to=date_to.isoformat(),
        output_pdf=output_pdf,
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
    return write_creditnote_pdf(
        export_rows=export_rows,
        fetch_totals=fetch_totals,
        output_pdf=base_output_dir / f"{filename}.pdf",
        date_from=date_from,
        date_to=date_to,
        projects=resolved_projects,
    )
