"""Validate an explicitly accepted cost model without changing its money values."""
from collections import Counter
from decimal import Decimal, InvalidOperation
import re
from typing import Any, Iterable, Mapping


def _known_nonfallback_source(source: Any) -> bool:
    if not isinstance(source, str):
        return False
    mapped = {"mapped_" + key + suffix for key in (
        "compound_key", "item_label", "legacy_title_hash", "product_identifier", "product_sku"
    ) for suffix in ("", "_normalized")}
    if source in mapped | {"zero_cost_override", "zero_cost_service_override", "zero_margin_override",
                           "zero_revenue_gift_mapped_cost", "zero_revenue_gift_missing_cost",
                           "bundle_component_configured_cost"}:
        return True
    if re.fullmatch(r"(?:authoritative_)?margin_\d+(?:_\d+)?_override", source):
        return True
    if re.fullmatch(r"(?:bundle_components_configured|reporting_bundle_components):[a-zA-Z0-9_-]+", source):
        return True
    for wrapper in ("canonical_alias:", "bundle_component:"):
        if source.startswith(wrapper):
            # Only one instance of each wrapper is emitted by the producer.
            tail = source[len(wrapper):]
            return wrapper not in tail and _known_nonfallback_source(tail)
    match = re.fullmatch(r"bundle_components_inferred:x\d+:(.+)", source)
    return bool(match and match[1] in mapped)


def assess_configured_cost_estimates(
    rows: Iterable[Mapping[str, Any]], *, settings: Mapping[str, Any], margin_pct: Any,
) -> dict:
    policy = settings.get("cost_estimate_publication", {})
    enabled = isinstance(policy, dict) and policy.get("allow_configured_margin_fallback") is True
    errors: Counter = Counter()
    fallback_rows = verified_rows = invalid_rows = 0

    def number(value):
        if isinstance(value, bool) or value is None:
            raise ValueError("missing_or_invalid_number")
        try:
            result = Decimal(str(value))
        except (InvalidOperation, ValueError):
            raise ValueError("missing_or_invalid_number") from None
        if not result.is_finite():
            raise ValueError("nonfinite_number")
        return result

    try:
        margin = number(margin_pct)
        if not 0 <= margin <= 100:
            raise ValueError("invalid_margin")
        token = f"{float(margin):g}".replace(".", "_")
        expected_source = "missing_cost_zero_margin_fallback" if margin == 0 else f"missing_cost_margin_{token}_fallback"
    except ValueError:
        margin, expected_source = None, None

    for row in rows:
        source = row.get("expense_source")
        fallback = isinstance(source, str) and ("missing_cost_" in source.lower() or source.lower() == "fallback_default")
        unknown = not fallback and not _known_nonfallback_source(source)
        if fallback:
            fallback_rows += 1
        # Ordinary missing-cost coverage retains the default strict threshold.
        # Opting into a model adds row validation; it never approves unknown costs.
        if not enabled or not (fallback or unknown):
            continue
        try:
            if unknown:
                raise ValueError("unknown_expense_source")
            if margin is None:
                raise ValueError("invalid_configured_margin")
            bundle_source = "bundle_component:bundle_component_" + expected_source
            if source != expected_source and not (source == bundle_source and row.get("bundle_component_flag") is True):
                raise ValueError("unapproved_expense_source")
            quantity = number(row.get("item_quantity"))
            revenue = number(row.get("item_total_without_tax"))
            discount = number(row.get("item_order_discount_without_tax", 0))
            unit_cost = number(row.get("expense_per_item"))
            total_cost = number(row.get("total_expense"))
            profit = number(row.get("profit_before_ads"))
            if quantity <= 0 or min(revenue, discount, unit_cost, total_cost) < 0:
                raise ValueError("invalid_quantity_or_negative_amount")
            cent = Decimal("0.01")
            noise = Decimal("0.000000001")
            if any(abs(value - value.quantize(cent)) > noise for value in (revenue, discount, total_cost, profit)):
                raise ValueError("noncent_reported_amount")
            # Acquisition cost is estimated before header discounts. Source net
            # can have sub-cent precision; the exported net is rounded to cents.
            rate = 1 - margin / 100
            raw_cost = unit_cost * quantity
            if abs(raw_cost - (revenue + discount) * rate) > Decimal("0.005") * rate + noise:
                raise ValueError("configured_margin_cost_mismatch")
            # Match the producer's native float rounding, including half-cent ties.
            native_cost = Decimal(str(round(float(unit_cost) * float(quantity), 2)))
            native_profit = Decimal(str(round(float(revenue) - float(total_cost), 2)))
            if abs(total_cost - native_cost) > noise:
                raise ValueError("unit_total_cost_mismatch")
            if abs(profit - native_profit) > noise:
                raise ValueError("reported_profit_mismatch")
            verified_rows += 1
        except (ValueError, InvalidOperation, OverflowError) as exc:
            invalid_rows += 1
            reason = str(exc) if isinstance(exc, ValueError) else "invalid_numeric_range"
            errors[reason] += 1
    approved = enabled and fallback_rows > 0 and invalid_rows == 0 and verified_rows == fallback_rows
    return {"enabled": enabled, "approved": approved, "fallback_rows": fallback_rows,
            "verified_rows": verified_rows, "invalid_rows": invalid_rows,
            "invalid_reasons": dict(sorted(errors.items())),
            "basis": "verified_configured_margin_estimate" if approved else "strict_cost_coverage"}
