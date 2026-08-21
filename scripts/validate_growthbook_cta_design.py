#!/usr/bin/env python3
from __future__ import annotations

import json
import pathlib
import re
import sys
from typing import Any


ROOT = pathlib.Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "projects" / "vevo" / "growthbook_cta_design.json"
STOREFRONT_PATH = ROOT / "storefront" / "vevo-growthbook" / "vevo-growthbook.js"

EXPECTED_CONTRACT = {
    "schema_version": 1,
    "experiment_id": "vevo-sk-product-cta-color-001",
    "variation_id": "brand_contrast",
    "surface": "slovak_product_detail_add_to_cart_cta",
    "selector": "#product-detail .s1-detailCart .s1-submitCart",
    "class_name": "vevo-gb-cta-brand-contrast",
    "scope": "background_color_only",
    "allowed_css_properties": ["background-color", "background-image", "color"],
    "text_color": "#0f172a",
    "gradient": {
        "angle_degrees": 135,
        "start_color": "#c9a962",
        "end_color": "#b8956f",
    },
    "contrast_standard": "WCAG_2_2_AA_normal_text",
    "minimum_wcag_contrast_ratio": 4.5,
    "unchanged_surfaces": [
        "label",
        "dimensions",
        "layout",
        "placement",
        "product_selector",
        "price",
        "cart_behavior",
        "checkout_behavior",
    ],
}

HEX_COLOR_RE = re.compile(r"^#[0-9a-f]{6}$")
CSS_DECLARATION_RE = re.compile(r'"([a-z-]+):')
FORBIDDEN_BUTTON_MUTATIONS = (
    "button.textContent",
    "button.innerHTML",
    "button.innerText",
    "button.style",
    "button.onclick",
    "button.disabled",
    "button.value",
    "button.formAction",
    'button.setAttribute("style"',
    "button.setAttribute('style'",
    'button.addEventListener("click"',
    "button.addEventListener('click'",
)


class CtaDesignContractError(ValueError):
    pass


def _linear_channel(value: int) -> float:
    channel = value / 255
    if channel <= 0.04045:
        return channel / 12.92
    return ((channel + 0.055) / 1.055) ** 2.4


def relative_luminance(color: str) -> float:
    if not isinstance(color, str) or not HEX_COLOR_RE.fullmatch(color):
        raise CtaDesignContractError(f"invalid lowercase six-digit color: {color!r}")
    red, green, blue = (
        _linear_channel(int(color[index : index + 2], 16)) for index in (1, 3, 5)
    )
    return (0.2126 * red) + (0.7152 * green) + (0.0722 * blue)


def contrast_ratio(first: str, second: str) -> float:
    first_luminance = relative_luminance(first)
    second_luminance = relative_luminance(second)
    lighter = max(first_luminance, second_luminance)
    darker = min(first_luminance, second_luminance)
    return (lighter + 0.05) / (darker + 0.05)


def validate_contract(contract: dict[str, Any], storefront_source: str) -> dict[str, float]:
    if contract != EXPECTED_CONTRACT:
        raise CtaDesignContractError("CTA design contract changed outside the approved v1 shape")
    if not isinstance(storefront_source, str) or not storefront_source:
        raise CtaDesignContractError("storefront source must be non-empty text")

    text_color = contract["text_color"]
    gradient = contract["gradient"]
    ratios = {
        "start": contrast_ratio(text_color, gradient["start_color"]),
        "end": contrast_ratio(text_color, gradient["end_color"]),
    }
    minimum = contract["minimum_wcag_contrast_ratio"]
    failed = sorted(name for name, ratio in ratios.items() if ratio < minimum)
    if failed:
        raise CtaDesignContractError(
            f"CTA contrast is below {minimum}: {', '.join(failed)}"
        )

    style_start = storefront_source.find("style.textContent =")
    style_end = storefront_source.find(
        "root.document.head.appendChild(style);", style_start
    )
    if style_start < 0 or style_end < 0:
        raise CtaDesignContractError("CTA style block is missing")
    style_block = storefront_source[style_start:style_end]

    css_properties = CSS_DECLARATION_RE.findall(style_block)
    if css_properties != contract["allowed_css_properties"]:
        raise CtaDesignContractError(
            "CTA style block may change only background-color, background-image, and color"
        )

    selector = contract["selector"]
    class_name = contract["class_name"]
    required_source_markers = (
        f'var CTA_CLASS = "{class_name}";',
        f'return root.document.querySelector("{selector}");',
        'button.classList.add(CTA_CLASS);',
        'button.classList.remove(CTA_CLASS);',
        f'"background-color:{gradient["start_color"]}!important;"',
        (
            '"background-image:linear-gradient('
            f'{gradient["angle_degrees"]}deg,{gradient["start_color"]} 0%,'
            f'{gradient["end_color"]} 100%)!important;"'
        ),
        f'"color:{text_color}!important;"',
    )
    missing = [marker for marker in required_source_markers if marker not in storefront_source]
    if missing:
        raise CtaDesignContractError(f"CTA source drifted from the design contract: {missing}")

    forbidden = [
        marker for marker in FORBIDDEN_BUTTON_MUTATIONS if marker in storefront_source
    ]
    if forbidden:
        raise CtaDesignContractError(
            f"CTA source contains forbidden behavior/content mutation: {forbidden}"
        )
    if '"content:' in style_block or "'content:" in style_block:
        raise CtaDesignContractError("CTA CSS must not replace the button label")

    return ratios


def validate() -> dict[str, float]:
    contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    if not isinstance(contract, dict):
        raise CtaDesignContractError("CTA design contract must be a JSON object")
    storefront_source = STOREFRONT_PATH.read_text(encoding="utf-8")
    return validate_contract(contract, storefront_source)


def main() -> int:
    try:
        ratios = validate()
        print(
            "validate_growthbook_cta_design.py: OK: "
            f"start={ratios['start']:.4f}:1 end={ratios['end']:.4f}:1"
        )
        return 0
    except (CtaDesignContractError, json.JSONDecodeError, OSError) as exc:
        print(f"validate_growthbook_cta_design.py: FAIL: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
