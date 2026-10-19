"""Reviewed VEVO laundry-bottle identities for the Natural/Premium analysis.

This classifier deliberately never uses EAN or reporting ``H-`` identifiers:
the historical export reuses EANs across both series. Warehouse variant codes
were read back from BiznisWeb on 2026-10-19. Existing project aliases preserve
the historical Natural names; the word ``premium`` in descriptions is not an
identity. Only bottle-only sets are eligible, never mixed gift bundles.
"""

import json
import re
import unicodedata
from functools import lru_cache
from pathlib import Path
from typing import Mapping, TypedDict


class Classification(TypedDict):
    series: str
    size_ml: int | None
    bottles_per_item: int
    basis: str
    identity_note: str


_ROOT = Path(__file__).resolve().parent
_SCENTS = {
    "01": "cotton paradise", "02": "sweet paradise", "03": "lavender kiss",
    "04": "wild africana", "05": "majestic ocean", "06": "royal cotton",
    "07": "ylang absolute", "08": "cotton dream", "09": "pure garden",
}


def _normal(value: object) -> str:
    text = unicodedata.normalize("NFKD", str(value or "").replace("×", "x"))
    return " ".join("".join(c for c in text if not unicodedata.combining(c)).lower().split())


def _single(label: str) -> tuple[str, int, str] | None:
    # Whole-label patterns prevent floor fragrances, samples and mixed bundles
    # from being admitted merely because their names contain a perfume name.
    patterns = (
        r"(?:parfum do prania|parfem (?:na|do) prani) vevo "
        r"(?:(natural|premium) )?no\.\s*(\d{2}) ([a-z ]+)\s*\(?\s*(200|500)\s*ml\)?",
        r"vevo (?:(natural|premium) )?no\.\s*(\d{2}) ([a-z ]+) mosoparfum\s*\(?\s*(200|500)\s*ml\)?",
    )
    for pattern in patterns:
        match = re.fullmatch(pattern, label)
        if match:
            series, number, scent, size = match.groups()
            if _SCENTS.get(number) != scent.strip():
                return None
            if series == "premium" and number not in {"07", "08", "09"}:
                return None
            return ("Premium" if series == "premium" else "Natural", int(size), number)
    return None


@lru_cache(maxsize=1)
def _catalog():
    aliases = json.loads((_ROOT / "product_name_aliases.json").read_text(encoding="utf-8"))
    aliases = {_normal(key): _normal(value) for key, value in aliases.items()}
    bindings = json.loads((_ROOT / "product_cost_identity_bindings.json").read_text(encoding="utf-8"))
    for binding in bindings["bindings"]:
        target = binding.get("canonical_expense_label")
        if target and binding.get("allow_label_only"):
            for label in binding["source_labels"]:
                aliases.setdefault(_normal(label), _normal(target))
    settings = json.loads((_ROOT / "settings.json").read_text(encoding="utf-8"))
    rules, labels = {}, {}
    for rule in settings["product_cost_bundle_rules"]:
        parts = [(_single(_normal(c["expense_key"])), c["quantity"]) for c in rule["components"]]
        spec = None
        if parts and all(part is not None for part, _ in parts):
            series_sizes = {(part[0], part[1]) for part, _ in parts}
            if len(series_sizes) == 1 and all(int(qty) == qty and qty > 0 for _, qty in parts):
                series, size = series_sizes.pop()
                spec = (series, size, sum(int(qty) for _, qty in parts))
        rules[rule["rule_id"]] = spec
        for label in rule["exact_labels"]:
            labels[_normal(label)] = (rule["rule_id"], spec)
    return aliases, rules, labels


def _resolve(label: str, aliases: Mapping[str, str]) -> str:
    seen = set()
    while label in aliases and label not in seen:
        seen.add(label)
        label = aliases[label]
    return label


def _result(series: str, size: int, bottles: int, basis: str, note: str = "") -> Classification:
    return {"series": series, "size_ml": size, "bottles_per_item": bottles,
            "basis": basis, "identity_note": note}


def classify(row: Mapping[str, object], include_pure_sets: bool = True) -> Classification | None:
    """Classify one financial-export row, or return None when outside scope.

    Explicit series/volume conflicts with an observed warehouse variant are
    errors. A historical label without a series can be overridden by a Premium
    warehouse code, with an audit note. Scent-only conflicts are also recorded;
    they do not change either series or bottle volume in this comparison.
    """
    aliases, rules, sets = _catalog()
    original = _normal(row.get("item_label", ""))
    label = _resolve(original, aliases)
    source = str(row.get("expense_source", ""))
    configured = re.search(r"(?:^|:)bundle_components_configured:([^:]+)", source)
    set_identity = sets.get(original) or sets.get(label)
    if configured:
        rule_id = configured.group(1)
        # The export's configured composition outranks an old display title.
        if rule_id not in rules or rules[rule_id] is None:
            return None
        if set_identity and set_identity[1] != rules[rule_id]:
            raise ValueError(f"Conflicting bundle composition: {row.get('item_label')}")
        if not set_identity:
            # A cost source alone cannot turn unrelated merchandise into perfume.
            return None
        set_identity = (rule_id, rules[rule_id])
    if set_identity:
        rule_id, spec = set_identity
        if not include_pure_sets or spec is None:
            return None
        return _result(*spec, basis=f"configured_pure_set:{rule_id}")

    direct = _single(label)
    multiplier = 1
    if direct is None:
        multipack = re.fullmatch(r"([23])\s*x\s*(.+)", label)
        if multipack and include_pure_sets:
            multiplier = int(multipack.group(1))
            direct = _single(multipack.group(2))
            verified_two = label == _normal("2x Parfum do prania Vevo Natural No.07 Ylang Absolute (500ml)")
            inferred = f"bundle_components_inferred:x{multiplier}:" in source
            if not (verified_two or inferred):
                return None
        if direct is None:
            return None
    series, size, scent = direct
    warehouse = _normal(row.get("item_warehouse_number", ""))
    match = re.fullmatch(r"(0[1-9])(200|500)([cp]?)", warehouse)
    note = ""
    basis = "explicit_series_label" if re.search(r"\b(natural|premium)\b", original) else "historical_natural_label"
    if label != original:
        basis += ":catalog_alias"
    if match:
        code_scent, code_size, suffix = match.groups()
        valid = (suffix == "p" and code_scent in {"07", "08", "09"}) or (
            suffix == "c" and code_scent in {"01", "02", "03", "04", "05"}
        ) or (suffix == "" and code_scent in {"06", "07", "08", "09"})
        if valid:
            code_series = "Premium" if suffix == "p" else "Natural"
            explicit = re.search(r"\b(natural|premium)\b", original)
            if explicit and explicit.group(1).title() != code_series:
                raise ValueError(f"Warehouse/series conflict: {row.get('item_label')} / {warehouse}")
            if int(code_size) != size:
                raise ValueError(f"Warehouse/volume conflict: {row.get('item_label')} / {warehouse}")
            if code_series != series:
                note = "Premium warehouse variant overrides historical label without series."
            if code_scent != scent:
                note += (" " if note else "") + "Warehouse scent differs from title; series and volume agree."
            series, basis = code_series, "verified_warehouse_variant"
    if multiplier > 1:
        basis = f"pure_multipack:x{multiplier}:" + basis
    return _result(series, size, multiplier, basis, note)
