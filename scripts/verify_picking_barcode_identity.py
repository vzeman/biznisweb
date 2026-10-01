#!/usr/bin/env python3
"""Offline barcode round-trip check; no source PDF or business state is changed.

Requires the repository PDF dependencies, PyMuPDF, Pillow and zxing-cpp (3.1.1
was used for the incident audit). Creates controlled PDF bytes only in memory.
This cannot establish the contents of an unavailable historical printout.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import sys

import fitz
from PIL import Image
import zxingcpp

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from roy_picking_lists_pdf import build_roy_picking_lists_pdf  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--order", action="append", required=True)
    args = parser.parse_args()
    if not 1 <= len(args.order) <= 20 or any(not re.fullmatch(r"[0-9]{1,20}", n) for n in args.order):
        parser.error("Supply 1..20 numeric order identifiers")
    checks = []
    for number in args.order:
        row = {"order_num": number, "status": "AUDIT TEST", "sum": "0 EUR", "items": []}
        pdf = build_roy_picking_lists_pdf([row])
        with fitz.open(stream=pdf, filetype="pdf") as document:
            if len(document) != 1 or number not in document[0].get_text():
                raise RuntimeError("PDF text identity mismatch")
            resolutions = {}
            for dpi in (150, 300, 600):
                pixels = document[0].get_pixmap(dpi=dpi, alpha=False)
                with Image.frombytes("RGB", [pixels.width, pixels.height], pixels.samples) as image:
                    decoded = [{"text": b.text, "format": str(b.format)} for b in zxingcpp.read_barcodes(image)]
                if decoded != [{"text": number, "format": "Code 128"}]:
                    raise RuntimeError("Rendered barcode identity mismatch")
                resolutions[str(dpi)] = decoded
        checks.append({"input": number, "dpi": resolutions})
    print(json.dumps({"scope": "controlled reconstruction, not original printout", "checks": checks}))


if __name__ == "__main__":
    main()
