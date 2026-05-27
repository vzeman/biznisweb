#!/usr/bin/env python3
"""PDF picking-list generation for the ROY operations dashboard."""

from __future__ import annotations

import io
import unicodedata
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


def _draw_badge(canvas: Any, label: str, x: float, y: float, fonts: Dict[str, str], colors: Any) -> float:
    from reportlab.lib.units import mm
    from reportlab.pdfbase import pdfmetrics

    text_width = pdfmetrics.stringWidth(label, fonts["bold"], 8)
    badge_width = text_width + 8 * mm
    badge_height = 7 * mm
    canvas.setFillColor(colors.HexColor("#f97316"))
    canvas.roundRect(x, y - badge_height + 1.5 * mm, badge_width, badge_height, 2 * mm, fill=1, stroke=0)
    canvas.setFillColor(colors.white)
    canvas.setFont(fonts["bold"], 8)
    canvas.drawString(x + 4 * mm, y - 3.2 * mm, label)
    canvas.setFillColor(colors.black)
    return badge_width


def _fit_font_size(canvas: Any, label: str, font_name: str, max_width: float, start_size: int, min_size: int = 11) -> int:
    size = start_size
    while size > min_size and canvas.stringWidth(label, font_name, size) > max_width:
        size -= 1
    return size


def _draw_attention_banner(
    canvas: Any,
    label: str,
    x: float,
    y_top: float,
    width: float,
    fonts: Dict[str, str],
    colors: Any,
    *,
    fill: str,
    stroke: str,
    text_color: str,
) -> float:
    from reportlab.lib.units import mm

    height = 14 * mm
    canvas.setFillColor(colors.HexColor(fill))
    canvas.setStrokeColor(colors.HexColor(stroke))
    canvas.setLineWidth(1.3)
    canvas.roundRect(x, y_top - height, width, height, 2.5 * mm, fill=1, stroke=1)
    canvas.setFillColor(colors.HexColor(text_color))
    font_size = _fit_font_size(canvas, label, fonts["bold"], width - 10 * mm, 18, min_size=12)
    canvas.setFont(fonts["bold"], font_size)
    canvas.drawCentredString(x + width / 2, y_top - 9 * mm, label)
    canvas.setFillColor(colors.black)
    return y_top - height - 4 * mm


def _draw_order_barcode(canvas: Any, order_num: Any, x_right: float, y_top: float, fonts: Dict[str, str]) -> None:
    from reportlab.graphics.barcode.code128 import Code128
    from reportlab.lib.units import mm

    order_text = _text(order_num, "")
    if not order_text:
        return

    max_width = 58 * mm
    bar_width = 0.32 * mm
    barcode = Code128(order_text, barWidth=bar_width, barHeight=12 * mm, humanReadable=False)
    if barcode.width > max_width:
        barcode = Code128(
            order_text,
            barWidth=bar_width * (max_width / barcode.width),
            barHeight=12 * mm,
            humanReadable=False,
        )

    x = x_right - barcode.width
    y = y_top - barcode.height
    barcode.drawOn(canvas, x, y)
    canvas.setFont(fonts["bold"], 8)
    canvas.drawCentredString(x + barcode.width / 2, y - 3.4 * mm, order_text)


def _draw_labeled_box(
    canvas: Any,
    title: str,
    value: Any,
    x: float,
    y_top: float,
    width: float,
    fonts: Dict[str, str],
    colors: Any,
    *,
    fill: str = "#fff7ed",
    stroke: str = "#fed7aa",
    max_lines: int = 4,
) -> float:
    from reportlab.lib.units import mm

    text = _text(value, "Bez poznámky")
    lines = _wrap_text(text, width - 8 * mm, fonts["regular"], 9)
    if len(lines) > max_lines:
        lines = lines[:max_lines]
        lines[-1] = lines[-1].rstrip(".") + "..."
    height = 10 * mm + max(1, len(lines)) * 4.5 * mm
    bottom = y_top - height

    canvas.setFillColor(colors.HexColor(fill))
    canvas.setStrokeColor(colors.HexColor(stroke))
    canvas.roundRect(x, bottom, width, height, 2.5 * mm, fill=1, stroke=1)
    canvas.setFillColor(colors.HexColor("#7c2d12"))
    canvas.setFont(fonts["bold"], 8)
    canvas.drawString(x + 4 * mm, y_top - 5 * mm, title)
    canvas.setFillColor(colors.black)
    canvas.setFont(fonts["regular"], 9)
    text_y = y_top - 10 * mm
    for line in lines:
        canvas.drawString(x + 4 * mm, text_y, line)
        text_y -= 4.5 * mm
    return bottom - 4 * mm


def _address_lines(address: Any) -> List[str]:
    if not isinstance(address, dict):
        return []
    lines = address.get("lines")
    if isinstance(lines, list):
        return [str(line).strip() for line in lines if str(line or "").strip()]

    fallback_lines = [
        address.get("display_name"),
        address.get("street"),
        " ".join(str(part or "").strip() for part in (address.get("city"), address.get("country")) if str(part or "").strip()),
        address.get("phone"),
        address.get("email"),
    ]
    return [str(line).strip() for line in fallback_lines if str(line or "").strip()]


def _draw_address_block(
    canvas: Any,
    title: str,
    lines: List[str],
    x: float,
    y_top: float,
    width: float,
    height: float,
    fonts: Dict[str, str],
    colors: Any,
) -> None:
    from reportlab.lib.units import mm

    canvas.setFillColor(colors.HexColor("#f8fafc"))
    canvas.setStrokeColor(colors.HexColor("#d8e0d4"))
    canvas.roundRect(x, y_top - height, width, height, 2 * mm, fill=1, stroke=1)
    canvas.setFillColor(colors.HexColor("#334155"))
    canvas.setFont(fonts["bold"], 8)
    canvas.drawString(x + 3 * mm, y_top - 4.5 * mm, title)
    canvas.setFillColor(colors.black)
    y = y_top - 9 * mm
    for line in (lines or ["-"])[:5]:
        y = _draw_wrapped(canvas, line, x + 3 * mm, y, width - 6 * mm, fonts["regular"], 8, leading=3.8 * mm, max_lines=1)


def _address_block_height(lines: List[str]) -> float:
    from reportlab.lib.units import mm

    return 10 * mm + max(2, min(len(lines), 5)) * 3.8 * mm


def _wholesale_info(order: Dict[str, Any]) -> Dict[str, Any]:
    info = order.get("wholesale_pricing") if isinstance(order.get("wholesale_pricing"), dict) else {}
    return {
        "is_wholesale": bool(info.get("is_wholesale")),
        "max_discount_pct": info.get("max_discount_pct"),
        "reason": str(info.get("reason") or "").strip(),
    }


def _normalize_flag_text(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or "").casefold())
    return "".join(ch for ch in text if not unicodedata.combining(ch)).strip()


def _is_personal_pickup_order(order: Dict[str, Any]) -> bool:
    if bool(order.get("personal_pickup")):
        return True
    shipping = order.get("shipping") if isinstance(order.get("shipping"), dict) else {}
    shipping_title = _normalize_flag_text(shipping.get("title"))
    return "osobny odber" in shipping_title


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
        wholesale = _wholesale_info(order)
        is_personal_pickup = _is_personal_pickup_order(order)

        canvas.setFont(fonts["bold"], 18)
        canvas.drawString(margin_x, y, "Vyskladňovací list")
        title_width = canvas.stringWidth("Vyskladňovací list", fonts["bold"], 18)
        if wholesale["is_wholesale"]:
            _draw_badge(canvas, "VEĽKOOBCHOD / VO CENY", margin_x + title_width + 8 * mm, y + 1 * mm, fonts, colors)
        _draw_order_barcode(canvas, order.get("order_num"), width - margin_x, y + 4 * mm, fonts)
        canvas.setFont(fonts["regular"], 8)
        canvas.drawRightString(width - margin_x, y - 16 * mm, f"strana objednávok {index}/{len(order_rows)}")
        y -= 20 * mm

        canvas.setStrokeColor(colors.HexColor("#18211b"))
        canvas.setLineWidth(1.1)
        canvas.line(margin_x, y, width - margin_x, y)
        y -= 8 * mm

        if is_personal_pickup:
            y = _draw_attention_banner(
                canvas,
                "OSOBNÝ ODBER - NEBALIŤ",
                margin_x,
                y,
                width - 2 * margin_x,
                fonts,
                colors,
                fill="#fee2e2",
                stroke="#dc2626",
                text_color="#991b1b",
            )
        if wholesale["is_wholesale"]:
            y = _draw_attention_banner(
                canvas,
                "VEĽKOOBCHODNÁ OBJEDNÁVKA",
                margin_x,
                y,
                width - 2 * margin_x,
                fonts,
                colors,
                fill="#ffedd5",
                stroke="#f97316",
                text_color="#9a3412",
            )

        left_x = margin_x
        right_x = width / 2 + 8 * mm
        label_width = 27 * mm
        customer = order.get("customer") if isinstance(order.get("customer"), dict) else {}
        customer_name = customer.get("display_name") or customer.get("company_name")
        _draw_meta_row(canvas, "Objednávka", order.get("order_num"), left_x, y, label_width, fonts)
        _draw_meta_row(canvas, "Suma", order.get("sum"), right_x, y, label_width, fonts)
        y -= 6 * mm
        _draw_meta_row(canvas, "Dátum", order.get("purchase_at"), left_x, y, label_width, fonts)
        _draw_meta_row(canvas, "Status", order.get("status"), right_x, y, label_width, fonts)
        y -= 6 * mm
        _draw_meta_row(canvas, "Platba", (order.get("payment") or {}).get("title"), left_x, y, label_width, fonts)
        _draw_meta_row(canvas, "Doprava", (order.get("shipping") or {}).get("title"), right_x, y, label_width, fonts)
        y -= 6 * mm
        _draw_meta_row(canvas, "Zákazník", customer_name, left_x, y, label_width, fonts)
        if wholesale["is_wholesale"]:
            wholesale_label = "áno"
            if wholesale.get("max_discount_pct"):
                wholesale_label = f"áno, zľava do {wholesale['max_discount_pct']}%"
            _draw_meta_row(canvas, "VO ceny", wholesale_label, right_x, y, label_width, fonts)
        else:
            _draw_meta_row(canvas, "VO ceny", "nie", right_x, y, label_width, fonts)
        y -= 10 * mm

        y = _draw_labeled_box(
            canvas,
            "Poznámka klienta",
            order.get("customer_note") or "Bez poznámky",
            margin_x,
            y,
            width - 2 * margin_x,
            fonts,
            colors,
        )
        if order.get("internal_note"):
            y = _draw_labeled_box(
                canvas,
                "Interná poznámka",
                order.get("internal_note"),
                margin_x,
                y,
                width - 2 * margin_x,
                fonts,
                colors,
                fill="#f8fafc",
                stroke="#cbd5e1",
                max_lines=3,
            )

        invoice_lines = _address_lines(order.get("invoice_address"))
        delivery_lines = _address_lines(order.get("delivery_address")) or invoice_lines
        if invoice_lines or delivery_lines:
            gap = 6 * mm
            block_width = (width - 2 * margin_x - gap) / 2
            block_height = max(_address_block_height(invoice_lines), _address_block_height(delivery_lines))
            _draw_address_block(canvas, "Fakturačná adresa", invoice_lines, margin_x, y, block_width, block_height, fonts, colors)
            _draw_address_block(
                canvas,
                "Doručovacia adresa",
                delivery_lines,
                margin_x + block_width + gap,
                y,
                block_width,
                block_height,
                fonts,
                colors,
            )
            y -= block_height + 7 * mm

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
                _draw_order_barcode(canvas, order.get("order_num"), width - margin_x, y + 3 * mm, fonts)
                y -= 16 * mm
                draw_header()

            product_lines = _wrap_text(item.get("label"), col_product - 4 * mm, fonts["regular"], 8)
            row_height = max(8 * mm, (len(product_lines[:3]) * 4.2 * mm) + 4 * mm)
            canvas.line(table_x, y + 2 * mm, table_x + table_width, y + 2 * mm)
            canvas.setFont(fonts["bold"], 9)
            canvas.drawString(table_x + 2 * mm, y - 2 * mm, _text(item.get("quantity"), "0"))
            _draw_wrapped(
                canvas,
                item.get("label"),
                table_x + col_qty + 2 * mm,
                y - 1.5 * mm,
                col_product - 4 * mm,
                fonts["regular"],
                8,
                leading=4.2 * mm,
            )
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
