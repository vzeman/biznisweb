"""Aggregate a private VEVO report export without emitting customer/order identifiers.

The order CSV is an item-grain, realized-order-only export. Monetary item fields
are already EUR, excluding VAT. Currency is a shop proxy, not a landing-domain
observation. This script does not contact production or change source exports.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta
from pathlib import Path


def number(row: dict, key: str) -> float:
    return float(row.get(key) or 0)


def total(rows: list[dict], key: str) -> float:
    return round(sum(number(row, key) for row in rows), 2)


def ratio(numerator: float, denominator: float) -> float | None:
    return round(numerator / denominator, 4) if denominator else None


def read_csv(path: Path) -> list[dict]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def aggregate(rows: list[dict]) -> dict:
    orders = {}
    for row in rows:
        identity = row["order_num"] or row["order_id"]
        if not identity:
            raise ValueError("An order has no stable identity")
        orders.setdefault(identity, row)
    unique = list(orders.values())
    revenue = total(rows, "item_total_without_tax")
    cost = total(rows, "total_expense")
    count = len(unique)
    # Report configuration uses EUR 0.30 packaging + EUR 0.20 net shipping/order.
    variable_fulfillment = round(count * 0.5, 2)
    cm1 = round(revenue - cost - variable_fulfillment, 2)
    new = [row for row in unique if row["is_customer_first_order"].lower() == "true"]
    returning = [row for row in unique if row["is_returning_customer_order"].lower() == "true"]
    missing_cost = [row for row in rows if row["expense_source"] == "missing_cost_margin_35_fallback"]
    margin_override = [row for row in rows if row["expense_source"] == "authoritative_margin_90_override"]
    return {
        "orders": count,
        "net_item_revenue_eur": revenue,
        "aov_net_item_eur": ratio(revenue, count),
        "units": total(rows, "item_quantity"),
        "units_per_order": ratio(total(rows, "item_quantity"), count),
        "product_cost_eur": cost,
        "product_gross_profit_eur": round(revenue - cost, 2),
        "product_gross_margin_pct": ratio(100 * (revenue - cost), revenue),
        "modeled_fulfillment_eur": variable_fulfillment,
        "modeled_cm1_before_ads_fixed_eur": cm1,
        "modeled_cm1_per_order_eur": ratio(cm1, count),
        "modeled_cm1_margin_pct": ratio(100 * cm1, revenue),
        "new_customer_orders": len(new),
        "returning_customer_orders": len(returning),
        "new_customer_revenue_eur": total(new, "order_revenue_net"),
        "returning_customer_revenue_eur": total(returning, "order_revenue_net"),
        "returning_order_share_pct": ratio(100 * len(returning), count),
        "status_orders": dict(Counter(row["status_name"] for row in unique)),
        "payment_orders": dict(Counter(row["payment_title"] for row in unique)),
        "cost_fallback_revenue_eur": total(missing_cost, "item_total_without_tax"),
        "cost_fallback_revenue_share_pct": ratio(100 * total(missing_cost, "item_total_without_tax"), revenue),
        "margin_override_revenue_eur": total(margin_override, "item_total_without_tax"),
        "margin_override_revenue_share_pct": ratio(100 * total(margin_override, "item_total_without_tax"), revenue),
        "deduped_order_revenue_net_eur": total(unique, "order_revenue_net"),
    }


def basket_cohorts(rows: list[dict]) -> dict:
    baskets = defaultdict(list)
    for row in rows:
        baskets[row["order_num"] or row["order_id"]].append(row)
    with_sample = {
        identity for identity, items in baskets.items()
        if any(row["product_sku"] == "H-B8B94A3C" for row in items)
    }
    only_sample = {
        identity for identity, items in baskets.items()
        if all(row["product_sku"] == "H-B8B94A3C" for row in items)
    }
    groups = {
        "first_customer_order": [row for row in rows if row["is_customer_first_order"].lower() == "true"],
        "returning_customer_order": [row for row in rows if row["is_returning_customer_order"].lower() == "true"],
        "contains_essence_sample_set_9x10": [row for row in rows if (row["order_num"] or row["order_id"]) in with_sample],
        "only_essence_sample_set_9x10": [row for row in rows if (row["order_num"] or row["order_id"]) in only_sample],
    }
    return {name: aggregate(items) for name, items in groups.items()}


def retention_cohorts(rows: list[dict], cutoff: str) -> dict:
    """Keep private customer keys in memory; output only aggregate cohorts."""
    baskets = defaultdict(list)
    for row in rows:
        baskets[row["order_num"] or row["order_id"]].append(row)
    customers = defaultdict(list)
    missing_email_orders = 0
    for identity, items in baskets.items():
        first = items[0]
        email = first["customer_email"].strip().lower()
        if not email or email == "nan":
            missing_email_orders += 1
            continue
        customers[email].append({
            "identity": identity,
            "time": datetime.fromisoformat(first["purchase_date"]),
            "shop": first["shop"],
            "first": first["is_customer_first_order"].lower() == "true",
            "items": items,
        })
    end = datetime.combine(date.fromisoformat(cutoff), time.max)
    periods = {
        "SK_acquired_2026-08_mature30d": ("2026-08-01", "2026-08-31"),
        "SK_acquired_2026-09-01_06_mature30d": ("2026-09-01", "2026-09-06"),
        "SK_acquired_2026-09_all_partly_immature": ("2026-09-01", "2026-09-30"),
    }
    results = {
        "methodology": [
            "Normalized customer email is used only in memory; no email, hash or order identifier is persisted.",
            "Acquisition requires the report's is_customer_first_order flag; first purchase is not redefined from the recent window.",
            "The first purchase must use the SK/EUR shop proxy; repeat purchases may use any shop in the archived export.",
            "Thirty-day window includes the first purchase and realized purchases at most 30 exact days later, through the report cutoff.",
            "CM1 deducts product expense and modeled EUR 0.50/order, before ad spend and fixed costs; this is not accounting net profit or LTV.",
            "The full September cohort is partly immature; use the explicit mature cohorts for complete 30-day comparisons.",
            "Sample-first means first basket contains the exact Essence Sample Set 9x10 SKU H-B8B94A3C.",
        ],
        "cutoff_date": cutoff,
        "missing_email_orders_excluded": missing_email_orders,
        "cohorts": {},
    }
    for name, (start, stop) in periods.items():
        selected = []
        for orders in customers.values():
            orders.sort(key=lambda order: (order["time"], order["identity"]))
            first_orders = [order for order in orders if order["first"]]
            if not first_orders:
                continue
            first = first_orders[0]
            first_day = first["time"].date().isoformat()
            if first["shop"] != "SK" or not start <= first_day <= stop:
                continue
            window_end = first["time"] + timedelta(days=30)
            observed = [order for order in orders if first["time"] <= order["time"] <= min(window_end, end)]
            selected.append({
                "first": first,
                "orders": observed,
                "mature": window_end <= end,
                "sample": any(row["product_sku"] == "H-B8B94A3C" for row in first["items"]),
            })
        results["cohorts"][name] = {}
        for segment in ("all", "first_contains_9x10", "first_does_not_contain_9x10"):
            members = [customer for customer in selected if segment == "all" or customer["sample"] == (segment == "first_contains_9x10")]
            first_rows = [row for customer in members for row in customer["first"]["items"]]
            window_rows = [row for customer in members for order in customer["orders"] for row in order["items"]]
            first_totals = aggregate(first_rows)
            window_totals = aggregate(window_rows)
            repeaters = sum(len(customer["orders"]) > 1 for customer in members)
            results["cohorts"][name][segment] = {
                "customers": len(members),
                "mature_30d_customers": sum(customer["mature"] for customer in members),
                "repeat_customers_within_observed_up_to_30d": repeaters,
                "repeat_customer_pct_within_observed_up_to_30d": ratio(100 * repeaters, len(members)),
                "first_purchase_orders": first_totals["orders"],
                "first_purchase_revenue_eur": first_totals["net_item_revenue_eur"],
                "first_purchase_cm1_eur": first_totals["modeled_cm1_before_ads_fixed_eur"],
                "first_purchase_cm1_per_customer_eur": ratio(first_totals["modeled_cm1_before_ads_fixed_eur"], len(members)),
                "observed_up_to_30d_orders": window_totals["orders"],
                "observed_up_to_30d_revenue_eur": window_totals["net_item_revenue_eur"],
                "observed_up_to_30d_cm1_eur": window_totals["modeled_cm1_before_ads_fixed_eur"],
                "observed_up_to_30d_cm1_per_customer_eur": ratio(window_totals["modeled_cm1_before_ads_fixed_eur"], len(members)),
                "observed_up_to_30d_revenue_per_customer_eur": ratio(window_totals["net_item_revenue_eur"], len(members)),
            }
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=Path("data/country-analysis-20261007"))
    parser.add_argument("--output", type=Path, default=Path("data/country-analysis-20261007/aggregates/vevo_country_orders_20261007.json"))
    args = parser.parse_args()
    paths = {name: args.input_dir / name for name in ("orders.csv", "daily.csv", "payload.json")}
    rows = read_csv(paths["orders.csv"])
    daily = read_csv(paths["daily.csv"])
    payload = json.loads(paths["payload.json"].read_text(encoding="utf-8"))
    unrealized = sum(row["realized_revenue"].lower() != "true" for row in rows)
    if unrealized:
        raise ValueError("Expected a realized-only export; review source semantics")
    currencies = {"EUR": "SK", "CZK": "CZ", "HUF": "HU"}
    for row in rows:
        row["shop"] = currencies.get(row["order_currency"], "UNKNOWN")
        row["geo"] = (row["delivery_country"] or row["invoice_country"]).upper() or "UNKNOWN"
        row["geo_with_oss"] = (row["delivery_country"] or row["invoice_country"] or row["oss_country"]).upper() or "UNKNOWN"
        row["date"] = row["purchase_date"][:10]
    periods = {
        "2026-06": ("2026-06-01", "2026-06-30"),
        "2026-07": ("2026-07-01", "2026-07-31"),
        "2026-08": ("2026-08-01", "2026-08-31"),
        "2026-09": ("2026-09-01", "2026-09-30"),
        "2026-10-01_06": ("2026-10-01", "2026-10-06"),
        "2026-08_09": ("2026-08-01", "2026-09-30"),
        "2026-06_07": ("2026-06-01", "2026-07-31"),
        "last60": ("2026-08-08", "2026-10-06"),
        "previous60": ("2026-06-09", "2026-08-07"),
    }
    output = {
        "source_generated_at": payload["generated_at"],
        "source_sha256": {name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in paths.items()},
        "methodology": [
            "Order identities are deduplicated by order_num, falling back to order_id; item rows remain additive.",
            "Revenue is the sum of item_total_without_tax; costs sum total_expense; both are normalized EUR.",
            "Original purchase_date determines the cohort; current realized status determines inclusion.",
            "Shop proxy maps EUR=SK, CZK=CZ, HUF=HU; this does not independently prove checkout domain.",
            "Country evidence uses delivery, then invoice; OSS is reported as a separately validated fallback.",
            "CM1 models EUR 0.30 packaging and EUR 0.20 shipping per realized order; no ad/fixed allocation here.",
            "Cancelled, unpaid and failed checkout denominators are absent; rates cannot be inferred.",
            "No customer or order identity is written to this aggregate output.",
        ],
        "qa": {
            "item_rows": len(rows),
            "realized_orders": aggregate(rows)["orders"],
            "duplicate_item_identity_rows": len(rows) - len({(row["order_num"] or row["order_id"], row["item_number"]) for row in rows}),
            "missing_delivery_country_rows": sum(not row["delivery_country"] for row in rows),
            "unknown_invoice_delivery_country": aggregate([row for row in rows if row["geo"] == "UNKNOWN"]),
            "known_country_currency_mismatch_rows": sum(row["geo"] not in {"UNKNOWN", row["shop"]} for row in rows),
            "oss_completed_country_currency_mismatch_rows": sum(row["geo_with_oss"] != row["shop"] for row in rows),
        },
        "periods": {},
        "weekly": {},
        "products": {},
        "retention_cohorts": retention_cohorts(rows, payload["date_to"][:10]),
    }
    for name, (start, end) in periods.items():
        selected = [row for row in rows if start <= row["date"] <= end]
        day_rows = [row for row in daily if start <= row["date"] <= end]
        aggregate_all = aggregate(selected)
        output["periods"][name] = {
            "date_from": start, "date_to": end,
            "days": (date.fromisoformat(end) - date.fromisoformat(start)).days + 1,
            "currency_shops": {shop: aggregate([row for row in selected if row["shop"] == shop]) for shop in ("SK", "CZ", "HU")},
            "reported_geo": {geo: aggregate([row for row in selected if row["geo"] == geo]) for geo in ("SK", "CZ", "HU", "UNKNOWN")},
            "basket_cohorts": {shop: basket_cohorts([row for row in selected if row["shop"] == shop]) for shop in ("SK", "CZ", "HU")},
            "all": aggregate_all,
            "daily_report": {key: total(day_rows, key) for key in ("total_revenue", "product_expense", "unique_orders", "packaging_cost", "shipping_net_cost", "creditnote_fulfillment_cost", "fb_ads_spend", "google_ads_spend", "cm1_profit", "cm2_profit", "fixed_daily_cost", "net_profit")},
            "reconciliation": {
                "revenue_delta_eur": round(aggregate_all["net_item_revenue_eur"] - total(day_rows, "total_revenue"), 2),
                "product_cost_delta_eur": round(aggregate_all["product_cost_eur"] - total(day_rows, "product_expense"), 2),
                "order_count_delta": aggregate_all["orders"] - total(day_rows, "unique_orders"),
                "cm1_delta_eur": round(aggregate_all["modeled_cm1_before_ads_fixed_eur"] - total(day_rows, "cm1_profit"), 2),
            },
        }
    by_week = defaultdict(list)
    for row in rows:
        if "2026-06-01" <= row["date"] <= "2026-10-06":
            dt = date.fromisoformat(row["date"])
            week = (dt - timedelta(days=dt.weekday())).isoformat()
            by_week[week].append(row)
    for week, week_rows in sorted(by_week.items()):
        output["weekly"][week] = {shop: aggregate([row for row in week_rows if row["shop"] == shop]) for shop in ("SK", "CZ", "HU")}
    for month in ("2026-06", "2026-07", "2026-08", "2026-09", "2026-10"):
        output["products"][month] = {}
        for shop in ("SK", "CZ", "HU"):
            product_rows = defaultdict(list)
            for row in rows:
                if row["date"].startswith(month) and row["shop"] == shop:
                    product_rows[(row["product_sku"], row["item_label"])].append(row)
            products = [{"sku": sku, "label": label, "revenue_eur": total(group, "item_total_without_tax"), "product_cost_eur": total(group, "total_expense"), "units": total(group, "item_quantity"), "orders": len({row["order_num"] or row["order_id"] for row in group})} for (sku, label), group in product_rows.items()]
            output["products"][month][shop] = sorted(products, key=lambda product: product["revenue_eur"], reverse=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote sanitized aggregate: {args.output}")
    print(json.dumps({"qa": output["qa"], "reconciliation": {name: period["reconciliation"] for name, period in output["periods"].items()}}, ensure_ascii=True))


if __name__ == "__main__":
    main()
