#!/usr/bin/env python3
"""PDF picking-list generation for the ROY operations dashboard."""

from __future__ import annotations

import io
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence


ROOT_DIR = Path(__file__).resolve().parent


def _text(value: Any, fallback: str = "-") -> str:
    text = str(value or "").strip()
    return text if text else fallback


def _font_candidates() -> Sequence[Path]:
    return (
        ROOT_DIR / "assets" / "fonts" / "DejaVuSans.ttf",
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
        Path("/usr/share/fonts/dejavu/DejaVuSans.ttf"),
        Path("C:/Windows/Fonts/arial.ttf"),
        Path("C:/Windows/Fonts/DejaVuSans.ttf"),
    )


def _register_fonts() -> Dict[str, str]:
    try:
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise RuntimeError("PDF export requires reportlab. Install dependencies from requirements.txt.") from exc

    for path in _font_candidates():
        if path.exists():
            pdfmetrics.registerFont(TTFont("BizniswebSans", str(path)))
            return {"regular": "BizniswebSans", "bold": "BizniswebSans"}
    return {"regular": "Helvetica", "bold": "Helvetica-Bold"}


def _wrap_text(text: Any, max_width: float, font_name: str, font_size: int) -> List[str]:
    from reportlab.pdfbase import pdfmetrics

    words = _text(text, "").split()
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
    return lines or [""]


def _draw_wrapped(
    canvas: Any,
    value: Any,
    x: float,
    y: float,
    max_width: float,
    font_name: str,
    font_size: int,
    *,
    leading: float = 11,
    max_lines: int = 3,
) -> float:
    lines = _wrap_text(value, max_width, font_name, font_size)
    if len(lines) > max_lines:
        lines = lines[:max_lines]
        lines[-1] = lines[-1].rstrip(".") + "..."
    canvas.setFont(font_name, font_size)
    for line in lines:
        canvas.drawString(x, y, line)
        y -= leading
    return y


def _draw_meta_row(
    canvas: Any,
    label: str,
    value: Any,
    x: float,
    y: float,
    label_width: float,
    fonts: Dict[str, str],
) -> None:
    canvas.setFont(fonts["bold"], 8)
    canvas.drawString(x, y, label)
    canvas.setFont(fonts["regular"], 9)
    canvas.drawString(x + label_width, y, _text(value))


def _draw_footer(canvas: Any, width: float, page_no: int, fonts: Dict[str, str]) -> None:
    from reportlab.lib.units import mm

    canvas.setFont(fonts["regular"], 8)
    canvas.setFillColorRGB(0.35, 0.39, 0.35)
    canvas.drawString(16 * mm, 10 * mm, f"ROY operations dashboard · strana {page_no}")
    canvas.drawRightString(width - 16 * mm, 10 * mm, datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
    canvas.setFillColorRGB(0, 0, 0)


def _order_items(order: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [item for item in (order.get("items") or []) if isinstance(item, dict)]


def build_roy_picking_lists_pdf(orders: Iterable[Dict[str, Any]]) -> bytes:
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.units import mm
        from reportlab.pdfgen import canvas as pdf_canvas
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise RuntimeError("PDF export requires reportlab. Install dependencies from requirements.txt.") from exc

    fonts = _register_fonts()
    order_rows = [order for order in orders if isinstance(order, dict)]

    buffer = io.BytesIO()
    canvas = pdf_canvas.Canvas(buffer, pagesize=A4, pageCompression=0)
    width, height = A4
    margin_x = 16 * mm
    top_y = height - 16 * mm
    page_no = 0

    def new_page() -> None:
        nonlocal page_no
        if page_no:
            _draw_footer(canvas, width, page_no, fonts)
            canvas.showPage()
        page_no += 1

    if not order_rows:
        new_page()
        canvas.setFont(fonts["bold"], 18)
        canvas.drawString(margin_x, top_y, "Vyskladňovacie listy")
        canvas.setFont(fonts["regular"], 11)
        canvas.drawString(margin_x, top_y - 18 * mm, "Aktuálne nie sú žiadne objednávky na odoslanie.")
        _draw_footer(canvas, width, page_no, fonts)
        canvas.save()
        return buffer.getvalue()

    for index, order in enumerate(order_rows, start=1):
        new_page()
        y = top_y

        canvas.setFont(fonts["bold"], 18)
        canvas.drawString(margin_x, y, "Vyskladňovací list")
        canvas.setFont(fonts["regular"], 10)
        canvas.drawRightString(width - margin_x, y + 1, f"{index}/{len(order_rows)}")
        y -= 10 * mm

        canvas.setStrokeColor(colors.HexColor("#18211b"))
        canvas.setLineWidth(1.1)
        canvas.line(margin_x, y, width - margin_x, y)
        y -= 8 * mm

        left_x = margin_x
        right_x = width / 2 + 8 * mm
        label_width = 26 * mm
        _draw_meta_row(canvas, "Objednávka", order.get("order_num"), left_x, y, label_width, fonts)
        _draw_meta_row(canvas, "Suma", order.get("sum"), right_x, y, label_width, fonts)
        y -= 6 * mm
        _draw_meta_row(canvas, "Dátum", order.get("purchase_at"), left_x, y, label_width, fonts)
        _draw_meta_row(canvas, "Status", order.get("status"), right_x, y, label_width, fonts)
        y -= 6 * mm
        _draw_meta_row(canvas, "Platba", (order.get("payment") or {}).get("title"), left_x, y, label_width, fonts)
        _draw_meta_row(canvas, "Doprava", (order.get("shipping") or {}).get("title"), right_x, y, label_width, fonts)
        y -= 10 * mm

        table_x = margin_x
        col_qty = 16 * mm
        col_product = 92 * mm
        col_code = 30 * mm
        col_ean = 32 * mm
        table_width = col_qty + col_product + col_code + col_ean

        def draw_header() -> None:
            nonlocal y
            canvas.setFillColor(colors.HexColor("#f3f5f1"))
            canvas.rect(table_x, y - 5 * mm, table_width, 7 * mm, fill=1, stroke=0)
            canvas.setFillColor(colors.black)
            canvas.setFont(fonts["bold"], 8)
            canvas.drawString(table_x + 2 * mm, y - 2 * mm, "Ks")
            canvas.drawString(table_x + col_qty + 2 * mm, y - 2 * mm, "Produkt")
            canvas.drawString(table_x + col_qty + col_product + 2 * mm, y - 2 * mm, "Import kód")
            canvas.drawString(table_x + col_qty + col_product + col_code + 2 * mm, y - 2 * mm, "EAN")
            y -= 9 * mm

        draw_header()
        canvas.setStrokeColor(colors.HexColor("#d9ded5"))
        for item in _order_items(order):
            if y < 34 * mm:
                _draw_footer(canvas, width, page_no, fonts)
                canvas.showPage()
                page_no += 1
                y = top_y
                canvas.setFont(fonts["bold"], 14)
                canvas.drawString(margin_x, y, f"Vyskladňovací list · {_text(order.get('order_num'))}")
                y -= 10 * mm
                draw_header()

            product_lines = _wrap_text(item.get("label"), col_product - 4 * mm, fonts["regular"], 8)
            row_height = max(8 * mm, (len(product_lines[:3]) * 4.2 * mm) + 4 * mm)
            canvas.line(table_x, y + 2 * mm, table_x + table_width, y + 2 * mm)
            canvas.setFont(fonts["bold"], 9)
            canvas.drawString(table_x + 2 * mm, y - 2 * mm, _text(item.get("quantity"), "0"))
            _draw_wrapped(canvas, item.get("label"), table_x + col_qty + 2 * mm, y - 1.5 * mm, col_product - 4 * mm, fonts["regular"], 8, leading=4.2 * mm)
            canvas.setFont(fonts["regular"], 8)
            canvas.drawString(table_x + col_qty + col_product + 2 * mm, y - 2 * mm, _text(item.get("import_code")))
            canvas.drawString(table_x + col_qty + col_product + col_code + 2 * mm, y - 2 * mm, _text(item.get("ean")))
            y -= row_height

        y = max(y - 8 * mm, 24 * mm)
        canvas.setFont(fonts["regular"], 9)
        canvas.drawString(margin_x, y, "Skontroloval: ____________________________")
        canvas.drawRightString(width - margin_x, y, "Dátum: __________________")

    _draw_footer(canvas, width, page_no, fonts)
    canvas.save()
    return buffer.getvalue()


def build_roy_picking_lists_filename(orders: Iterable[Dict[str, Any]]) -> str:
    order_rows = [order for order in orders if isinstance(order, dict)]
    stamp = datetime.now().strftime("%Y%m%d-%H%M")
    return f"roy-vyskladnovacie-listy-{len(order_rows)}-{stamp}.pdf"
