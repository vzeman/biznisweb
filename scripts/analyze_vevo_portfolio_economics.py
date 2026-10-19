"""Private offline portfolio economics; descriptive scenarios, never forecasts.

Financial scope is 200/500 ml laundry bottles and verified perfume-only sets.
Known mixed-set perfume components count for buyer/basket identity only. Export
line revenue less reporting COGS is merchandise contribution, not net profit.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
from datetime import date, datetime, timedelta
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from projects.vevo.laundry_series_catalog import (  # noqa: E402
    _catalog, _normal, _resolve, _single, classify, classify_retention_components,
)

SERIES = ("Natural", "Premium")
ZERO = Decimal(0)


def decimal(value) -> Decimal:
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("Nonfinite amount")
    return result


def ratio(a, b):
    return a / b if b else None


def empty_total():
    return {"net": ZERO, "gross": ZERO, "cogs": ZERO, "bottles": ZERO,
            "free_bottles": ZERO, "rows": 0, "modeled_cost_rows": 0}


def add(total, row, product=None):
    total["net"] += row["net"]
    total["gross"] += row["gross"]
    total["cogs"] += row["cogs"]
    total["rows"] += 1
    total["modeled_cost_rows"] += any(
        key in row["source"].lower() for key in ("margin", "fallback", "estimate"))
    if product:
        key = "bottles" if row["net"] > 0 else "free_bottles"
        total[key] += row["quantity"] * product["bottles_per_item"]


def finalized(total):
    result = dict(total)
    result["contribution"] = result["net"] - result["cogs"]
    result["contribution_rate"] = ratio(result["contribution"], result["net"])
    if result["bottles"]:
        result["net_per_paid_bottle"] = result["net"] / result["bottles"]
        result["contribution_per_paid_bottle"] = result["contribution"] / result["bottles"]
    return result


def scent(row, product):
    """Resolve singles' scent using the same warehouse-over-label precedence."""
    if product["bottles_per_item"] != 1:
        return None
    if product["basis"] == "verified_warehouse_variant":
        match = re.fullmatch(r"(0[1-9])(200|500)([cp]?)", _normal(row["item_warehouse_number"]))
        if match:
            return match.group(1)
    aliases, _, _ = _catalog()
    spec = _single(_resolve(_normal(row["item_label"]), aliases))
    return spec[2] if spec else None


def prepare(rows, end):
    prepared, seen, orders = [], set(), {}
    for raw in rows:
        key = (raw["order_num"], raw["item_number"])
        if key in seen:
            raise ValueError("Duplicate order/item")
        seen.add(key)
        if raw["realized_revenue"].lower() != "true":
            raise ValueError("Only realized financial exports are supported")
        if decimal(raw["order_credit_net_adjustment"]):
            raise ValueError("Unallocated order credits require line-level attribution")
        dt = datetime.fromisoformat(raw["purchase_date"])
        customer = raw["customer_email"].strip().lower()
        if customer in {"", "none", "nan"} or dt.date() > end:
            raise ValueError("Missing customer or invalid observation date")
        identity = (raw["order_id"], customer, dt)
        if raw["order_num"] in orders and orders[raw["order_num"]] != identity:
            raise ValueError("Conflicting order identity")
        orders[raw["order_num"]] = identity
        quantity, net, gross, cogs = [decimal(raw[field]) for field in (
            "item_quantity", "item_total_without_tax", "item_total_with_tax", "total_expense")]
        if quantity <= 0 or min(net, cogs) < 0 or gross < net:
            raise ValueError("Invalid line amount or quantity")
        if abs(net - cogs - decimal(raw["profit_before_ads"])) > Decimal("0.011"):
            raise ValueError("Export profit does not reconcile")
        product = classify(raw)
        single = classify(raw, include_pure_sets=False)
        components = classify_retention_components(raw) if net > 0 else []
        prepared.append({"date": dt.date(), "customer": customer,
            "order": raw["order_num"], "quantity": quantity, "net": net,
            "gross": gross, "cogs": cogs, "source": raw["expense_source"],
            "product": product, "single": single,
            "scent": scent(raw, single) if single else None,
            "series": {component["series"] for component in components},
            "mixed_set": bool(components) and product is None})
    return prepared


def scenario(removed, replacement, by_size, scent_units, scent_totals):
    """Linear counterfactuals hold prices/mix/costs fixed and save no overhead.

    r is retained *spending* or *bottle quantity*, never automatically customers.
    A scenario delta is r * replacement contribution - removed contribution.
    Break-even r is removed contribution / replacement contribution at r=1.
    """
    lost = removed["contribution"]
    spend_potential = (removed["net"] * replacement["contribution_rate"]
                       if replacement["contribution_rate"] is not None else None)
    quantity_potential, replacement_revenue = ZERO, ZERO
    by_size_detail, missing_sizes = {}, []
    for size, quantity in removed["bottles_by_size"].items():
        target = by_size.get(size)
        if not target or not target["bottles"]:
            if quantity:
                missing_sizes.append(size)
            continue
        unit_net = target["net"] / target["bottles"]
        unit_contribution = target["contribution"] / target["bottles"]
        quantity_potential += quantity * unit_contribution
        replacement_revenue += quantity * unit_net
        by_size_detail[size] = {"removed_paid_bottles": quantity,
            "replacement_net_per_bottle": unit_net,
            "replacement_contribution_per_bottle": unit_contribution}
    if missing_sizes:
        quantity_potential = replacement_revenue = None
    matched_units, unmatched_units, matched_contribution = ZERO, ZERO, ZERO
    for key, quantity in scent_units.items():
        target = scent_totals.get(key)
        if target and target["bottles"]:
            matched_units += quantity
            matched_contribution += quantity * target["contribution"] / target["bottles"]
        else:
            unmatched_units += quantity
    def view(potential):
        return {"replacement_contribution_if_100pct_retained": potential,
            "break_even_retained_fraction": ratio(lost, potential) if potential else None,
            "contribution_change_by_retention_fraction": {
                str(r): decimal(r) * potential - lost if potential is not None else None
                for r in (0, .25, .5, .75, 1)}}
    return {"lost_observed_contribution": lost, "assumed_avoidable_cost_savings": ZERO,
        "same_spending": view(spend_potential),
        "same_bottle_quantity_by_size": {**view(quantity_potential),
            "replacement_net_if_100pct_retained": replacement_revenue,
            "by_size": by_size_detail, "missing_sizes": missing_sizes},
        "same_size_same_scent_singles_only": {
            "matched_removed_paid_bottles": matched_units,
            "unmatched_removed_paid_bottles": unmatched_units,
            "matched_replacement_contribution": matched_contribution,
            "break_even_retained_fraction_for_full_removed_contribution": (
                ratio(lost, matched_contribution) if not unmatched_units
                and matched_units == removed["bottles"] else None)}}


def matched_price_cost_alternatives(single_scent):
    """Same absolute contribution per bottle; no demand elasticity assumption."""
    result = {}
    for key, premium in single_scent["Premium"].items():
        natural = single_scent["Natural"].get(key)
        if not natural or not natural["bottles"]:
            result[key] = {"natural_comparator_available": False}
            continue
        p_net = premium["net"] / premium["bottles"]
        p_cost = premium["cogs"] / premium["bottles"]
        p_contribution = p_net - p_cost
        n_contribution = natural["contribution"] / natural["bottles"]
        gap = n_contribution - p_contribution
        result[key] = {"natural_comparator_available": True,
            "premium_paid_bottles": premium["bottles"],
            "natural_paid_bottles": natural["bottles"],
            "premium_net_per_bottle": p_net, "premium_cogs_per_bottle": p_cost,
            "premium_contribution_per_bottle": p_contribution,
            "natural_contribution_per_bottle": n_contribution,
            "contribution_gap_to_natural": gap,
            "required_premium_net_per_bottle_at_same_cogs": p_net + gap,
            "required_premium_cogs_per_bottle_at_same_net": p_cost - gap,
            "additional_total_contribution_if_premium_volume_unchanged": gap * premium["bottles"]}
    return result


def premium_size_transfer_sensitivity(single_scent):
    result = {}
    for size in ("200", "500"):
        lost = potential = units = ZERO
        unmatched = ZERO
        for key, premium in single_scent["Premium"].items():
            if not key.startswith(size + ":"):
                continue
            units += premium["bottles"]
            lost += premium["contribution"]
            natural = single_scent["Natural"].get(key)
            if not natural or not natural["bottles"]:
                unmatched += premium["bottles"]
            else:
                potential += premium["bottles"] * natural["contribution"] / natural["bottles"]
        result[size] = {"removed_paid_bottles": units, "unmatched_bottles": unmatched,
            "matched_bottles": units - unmatched,
            "missing_coverage_fraction": ratio(unmatched, units),
            "observed_premium_contribution": lost,
            "matched_natural_contribution_at_100pct_transfer": potential,
            "delta_at_100pct_transfer": potential - lost if not unmatched else None,
            "break_even_retained_bottle_fraction": ratio(lost, potential) if not unmatched else None,
            "contribution_change_by_retention_fraction": {
                str(r): decimal(r) * potential - lost if not unmatched else None
                for r in (0, .25, .5, .75, 1)}}
    return result


def period_summary(rows, start, end, lifetime_series):
    selected = [row for row in rows if start <= row["date"] <= end]
    totals = {s: empty_total() for s in SERIES}
    single_size = {s: defaultdict(empty_total) for s in SERIES}
    single_scent = {s: defaultdict(empty_total) for s in SERIES}
    scent_units = {s: Counter() for s in SERIES}
    size_units = {s: Counter() for s in SERIES}
    buyers, orders, order_rows = defaultdict(set), defaultdict(set), defaultdict(list)
    for row in selected:
        order_rows[row["order"]].append(row)
        for series in row["series"]:
            buyers[series].add(row["customer"])
            orders[series].add(row["order"])
        product = row["product"]
        if not product:
            continue
        series = product["series"]
        add(totals[series], row, product)
        if row["net"] > 0:
            size_units[series][str(product["size_ml"])] += row["quantity"] * product["bottles_per_item"]
        single = row["single"]
        if single and row["net"] > 0:
            size = str(single["size_ml"])
            add(single_size[series][size], row, single)
            if row["scent"]:
                key = f"{size}:{row['scent']}"
                add(single_scent[series][key], row, single)
                scent_units[series][key] += row["quantity"]
    financial = {s: finalized(totals[s]) for s in SERIES}
    for s in SERIES:
        financial[s]["bottles_by_size"] = dict(size_units[s])
        financial[s]["retention_scope_buyers"] = len(buyers[s])
        financial[s]["retention_scope_orders"] = len(orders[s])
    single_size = {s: {key: finalized(value) for key, value in single_size[s].items()} for s in SERIES}
    single_scent = {s: {key: finalized(value) for key, value in single_scent[s].items()} for s in SERIES}
    premium_baskets = Counter()
    associated = {key: empty_total() for key in (
        "Premium", "Natural", "other_merchandise_including_samples",
        "unallocated_mixed_sets", "service_and_tip_lines")}
    customer_groups = {name: {"customers": set(), "premium": empty_total(),
                              "all_exported_lines": empty_total()}
                       for name in ("Premium_without_observed_Natural", "Both_observed")}
    for order, items in order_rows.items():
        customer = items[0]["customer"]
        lifetime = lifetime_series[customer]
        if "Premium" in lifetime:
            group = customer_groups["Both_observed" if "Natural" in lifetime else "Premium_without_observed_Natural"]
            group["customers"].add(customer)
            for row in items:
                add(group["all_exported_lines"], row)
                if row["product"] and row["product"]["series"] == "Premium":
                    add(group["premium"], row, row["product"])
        if order not in orders["Premium"]:
            continue
        has_natural = order in orders["Natural"]
        has_other = any(not row["product"] for row in items)
        category = ("Premium_and_Natural" if has_natural else "Premium_only_series")
        category += "_with_other_lines" if has_other else "_no_other_lines"
        premium_baskets[category] += 1
        for row in items:
            if row["product"]:
                category = row["product"]["series"]
            elif row["mixed_set"]:
                category = "unallocated_mixed_sets"
            elif "zero_cost_service_override" in row["source"]:
                category = "service_and_tip_lines"
            else:
                category = "other_merchandise_including_samples"
            add(associated[category], row, row["product"])
    groups = {name: {"customers_with_any_order_in_period": len(value["customers"]),
                     "premium": finalized(value["premium"]),
                     "all_exported_lines": finalized(value["all_exported_lines"])}
              for name, value in customer_groups.items()}
    return {"from": start.isoformat(), "to": end.isoformat(), "financial": financial,
        "paid_single_bottle_size_economics": single_size,
        "paid_single_bottle_size_scent_economics": single_scent,
        "premium_price_cost_to_match_natural_absolute_contribution": matched_price_cost_alternatives(single_scent),
        "premium_size_transfer_sensitivity": premium_size_transfer_sensitivity(single_scent),
        "remove": {s: scenario(financial[s], financial[target], single_size[target],
                    scent_units[s], single_scent[target])
                   for s, target in (("Premium", "Natural"), ("Natural", "Premium"))},
        "premium_baskets": dict(premium_baskets),
        "associated_lines_on_premium_orders": {key: finalized(value) for key, value in associated.items()},
        "customers_classified_by_full_observed_history": groups}


def analyze(rows, end):
    prepared = prepare(rows, end)
    if not prepared:
        raise ValueError("Empty source")
    lifetime = defaultdict(set)
    for row in prepared:
        lifetime[row["customer"]].update(row["series"])
    first = min(row["date"] for row in prepared)
    periods = {"all_time": first, "2026": date(2026, 1, 1),
               "last_180_days": end - timedelta(days=179),
               "last_90_days": end - timedelta(days=89)}
    return {"definitions": {
        "money": "EUR excluding VAT after discounts; contribution = net sales minus reporting COGS, before ads, fulfillment and overhead.",
        "financial_scope": "200/500 ml bottles and verified perfume-only sets. Mixed-set joint prices/costs are excluded.",
        "buyer_scope": "Paid qualifying bottles including verified mixed-set components; samples do not create series membership.",
        "associated_lines": "All exported lines in qualifying Premium orders, explicitly separate from focal perfume scope. Association is not a forecast loss; modeled costs may occur here.",
        "groups": "Premium-only means no observed qualifying Natural bottle in the complete source, not loyalty or no other shop purchase. Period groups use full-history membership.",
        "scenarios": "Static replacement sensitivity, not causal forecasts. No overhead saving assumed. Retention fractions describe spending or bottle quantities, not customer counts; concentration/duration and demand changes are unmodeled.",
        "formula": "Delta contribution = r * replacement contribution at full transfer - removed observed contribution + actually avoidable costs. Break-even r = (removed contribution - actually avoidable costs) / replacement contribution at full transfer.",
        "same_spending": "Transferred removed-series net spending earns target observed aggregate contribution rate.",
        "same_quantity": "Removed paid bottle counts by 200/500 ml are replaced at target same-period paid-single average net prices and unit contribution; does not assume equal doses or wash counts.",
        "same_scent": "Sensitivity uses same 200/500 ml and scent, standalone paid bottles only. Incomplete match is never treated as zero demand."},
        "source_rows": len(prepared),
        "lifetime_buyers": {
            "Natural_only": sum(value == {"Natural"} for value in lifetime.values()),
            "Premium_only": sum(value == {"Premium"} for value in lifetime.values()),
            "Both": sum(value == set(SERIES) for value in lifetime.values())},
        "periods": {name: period_summary(prepared, start, end, lifetime)
                    for name, start in periods.items() if start <= end}}


def serial(value):
    if isinstance(value, Decimal):
        return str(value.quantize(Decimal("0.000001")))
    raise TypeError(type(value).__name__)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--to-date", type=date.fromisoformat, required=True)
    args = parser.parse_args()
    raw = args.input.read_bytes()
    with args.input.open(encoding="utf-8-sig", newline="") as handle:
        result = analyze(list(csv.DictReader(handle)), args.to_date)
    result["source_sha256"] = hashlib.sha256(raw).hexdigest()
    args.output.mkdir(parents=True, exist_ok=True)
    destination = args.output / "portfolio-economics.json"
    destination.write_text(json.dumps(result, ensure_ascii=False, indent=2,
                                      sort_keys=True, default=serial), encoding="utf-8")
    print(json.dumps({"output": str(destination), "source_rows": result["source_rows"],
                      "lifetime_buyers": result["lifetime_buyers"]}, ensure_ascii=True))


if __name__ == "__main__":
    main()
