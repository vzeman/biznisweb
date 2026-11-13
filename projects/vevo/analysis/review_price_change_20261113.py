"""Read-only, pinned VEVO price review. stdout contains private business aggregates."""

import argparse
import csv
import hashlib
import io
import json
import statistics
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import boto3
from botocore.config import Config

OWNER = "919341186960"
BUCKET = "biznisweb-reporting-artifacts-" + OWNER + "-eu-central-1"
PREFIX = "daily-reports/vevo/20261113T003223Z/"
SOURCES = {
    "dashboard_payload_latest.json":
        "7f583fab48617674375c3da090d3a8ef38189c420884cb019dc1b246b0dd8ea6",
    "export_20250503-20261112.csv":
        "bdb758c3130e3427ed5f4a87d68d12525ff9fbd70e63629d1c1427e903095d74",
}
POST_START = date(2026, 11, 10)
POST_END = date(2026, 11, 12)
MONEY_FIELDS = (
    "revenue", "profit_without_fixed", "profit_with_fixed", "orders",
    "product_cost", "packaging", "shipping", "fixed", "fb_ads",
    "google_ads", "total_ads",
)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def load_source(client, name):
    response = client.get_object(
        Bucket=BUCKET, Key=PREFIX + name, ExpectedBucketOwner=OWNER,
    )
    body = response["Body"]
    try:
        raw = body.read(15_000_001)
    finally:
        body.close()
    require(len(raw) <= 15_000_000, "Source exceeds bounded read")
    require(hashlib.sha256(raw).hexdigest() == SOURCES[name], "Source hash mismatch")
    return raw


def number(row, key):
    return float(row.get(key) or 0)


def expected_dates(start, end):
    return [
        (start + timedelta(days=i)).isoformat()
        for i in range((end - start).days + 1)
    ]


def period(series, start, end, fields):
    dates = expected_dates(start, end)
    require(len(series["dates"]) == len(set(series["dates"])), "Duplicate report date")
    index = {value: i for i, value in enumerate(series["dates"])}
    require(all(value in index for value in dates), "Incomplete comparison calendar")
    return {
        key: sum(float(series[key][index[value]]) for value in dates)
        for key in fields
    }


def enrich(values):
    values = dict(values)
    values["pre_ad_contribution"] = values["profit_without_fixed"] + values["total_ads"]
    values["aov"] = values["revenue"] / values["orders"] if values["orders"] else None
    values["pre_ad_contribution_per_order"] = (
        values["pre_ad_contribution"] / values["orders"] if values["orders"] else None
    )
    values["pre_ad_margin"] = (
        values["pre_ad_contribution"] / values["revenue"] if values["revenue"] else None
    )
    return values


def item_review(rows, start, end, categories):
    selected = [r for r in rows if start.isoformat() <= r["purchase_date_only"] <= end.isoformat()]
    require(all(r["realized_revenue"].lower() == "true" for r in selected),
            "Non-realized row in export")
    orders, skus, groups, fallback = {}, {}, {}, []
    for row in selected:
        order_key = row["order_num"]
        require(bool(order_key), "Missing order identity")
        order = orders.setdefault(order_key, {
            "revenue": number(row, "order_revenue_net"),
            "credit": number(row, "order_credit_net_adjustment"),
            "first": row["is_customer_first_order"].lower() == "true",
            "returning": row["is_returning_customer_order"].lower() == "true",
            "product_contribution": 0,
            "customer_key": row["customer_email"].strip().lower(),
            "basket_categories": set(),
        })
        require(abs(order["revenue"] - number(row, "order_revenue_net")) < 0.001,
                "Inconsistent repeated order revenue")
        order["product_contribution"] += number(row, "profit_before_ads")
        require(not (order["first"] and order["returning"]), "Overlapping customer cohorts")
        sku_key = (row["product_sku"], row["item_label"], row["item_currency"])
        sku = skus.setdefault(sku_key, {
            "quantity": 0, "revenue": 0, "cost": 0, "orders": set(), "unit_prices": set(),
        })
        category = categories[row["item_label"]]  # Reviewed exact labels; no fuzzy fallback.
        if number(row, "item_quantity") > 0:
            order["basket_categories"].add(category)
        group = groups.setdefault(category, {
            "quantity": 0, "revenue": 0, "cost": 0, "orders": set(),
        })
        for aggregate in (sku, group):
            aggregate["quantity"] += number(row, "item_quantity")
            aggregate["revenue"] += number(row, "item_total_without_tax")
            aggregate["cost"] += number(row, "total_expense")
            aggregate["orders"].add(order_key)
        sku["unit_prices"].add(number(row, "item_unit_price_original"))
        if "fallback" in row["expense_source"]:
            fallback.append({
                key: row[key] for key in (
                    "item_label", "item_quantity", "item_total_without_tax",
                    "total_expense", "expense_source",
                )
            })
    sku_output = []
    for (sku_id, label, currency), aggregate in skus.items():
        aggregate["orders"] = len(aggregate["orders"])
        aggregate["unit_prices"] = sorted(aggregate["unit_prices"])
        aggregate["effective_unit_price_eur"] = (
            aggregate["revenue"] / aggregate["quantity"] if aggregate["quantity"] else None
        )
        sku_output.append(dict(sku=sku_id, label=label, currency=currency, **aggregate))
    for aggregate in groups.values():
        aggregate["orders"] = len(aggregate["orders"])
    cohorts = defaultdict(lambda: {"orders": 0, "revenue": 0, "product_contribution": 0})
    for order in orders.values():
        cohort = "first" if order["first"] else "returning" if order["returning"] else "unknown"
        cohorts[cohort]["orders"] += 1
        for key in ("revenue", "product_contribution"):
            cohorts[cohort][key] += order[key]
    # Customer identities stay in memory. Timestamp-tied first orders are one buyer.
    new_buyers = {}
    for order in orders.values():
        if not order["first"] or not order["customer_key"]:
            continue
        buyer = new_buyers.setdefault(order["customer_key"], {
            "orders": 0, "revenue": 0, "product_contribution": 0, "categories": set(),
        })
        buyer["orders"] += 1
        buyer["revenue"] += order["revenue"]
        buyer["product_contribution"] += order["product_contribution"]
        buyer["categories"].update(order["basket_categories"])
    new_baskets = {
        key: {"customers": 0, "revenue": 0, "product_contribution": 0}
        for key in ("full_no_samples", "full_and_samples", "samples_no_full", "other")
    }
    for buyer in new_buyers.values():
        full = bool(buyer["categories"] & {"single_perfume", "perfume_bundle"})
        sample = bool(buyer["categories"] & {"samples", "mixed_discovery_bundle"})
        group = (
            "full_no_samples" if full and not sample else
            "full_and_samples" if full else
            "samples_no_full" if sample else "other"
        )
        new_baskets[group]["customers"] += 1
        for key in ("revenue", "product_contribution"):
            new_baskets[group][key] += buyer[key]
    for group in new_baskets.values():
        group["share_of_new_customers"] = (
            group["customers"] / len(new_buyers) if new_buyers else None
        )
    revenues = sorted((o["revenue"] for o in orders.values()), reverse=True)
    return {
        "orders": len(orders),
        "order_revenue": sum(revenues),
        "line_revenue": sum(number(r, "item_total_without_tax") for r in selected),
        "product_cost": sum(number(r, "total_expense") for r in selected),
        "order_credit_net": sum(o["credit"] for o in orders.values()),
        "median_order_net": statistics.median(revenues) if revenues else None,
        "maximum_order_net": max(revenues) if revenues else None,
        "top_three_order_revenue": sum(revenues[:3]),
        "cohorts": dict(cohorts),
        "new_customer_baskets": {
            "distinct_known_customers": len(new_buyers),
            "customers_with_tied_first_orders": sum(b["orders"] > 1 for b in new_buyers.values()),
            "first_orders_without_email": sum(o["first"] and not o["customer_key"] for o in orders.values()),
            "groups": new_baskets,
        },
        "categories": groups,
        "fallback_lines": fallback,
        "skus": sorted(sku_output, key=lambda r: r["revenue"], reverse=True),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default="codex", help="Existing read-authorized AWS profile")
    args = parser.parse_args()
    client = boto3.Session(profile_name=args.profile, region_name="eu-central-1").client(
        "s3", config=Config(connect_timeout=10, read_timeout=45,
                            retries={"total_max_attempts": 1}),
    )
    payload = json.loads(load_source(client, "dashboard_payload_latest.json"))
    raw_csv = load_source(client, "export_20250503-20261112.csv")
    rows = list(csv.DictReader(io.StringIO(raw_csv.decode("utf-8-sig"))))
    categories = json.loads(Path(__file__).with_name("price_review_categories.json").read_text("utf-8"))
    require(payload["project"] == "vevo" and payload["date_to"] == POST_END.isoformat(),
            "Wrong project or report end")
    health = payload["source_health"]
    require(health["qa_failure_count"] == 0 and not health["is_partial"], "Invalid publication QA")
    dashboard = payload["dashboard"]
    series = dashboard["series"]
    blocks = []
    for weeks in range(9):
        start, end = POST_START - timedelta(weeks=weeks), POST_END - timedelta(weeks=weeks)
        blocks.append({
            "from": start.isoformat(), "to": end.isoformat(),
            **enrich(period(series, start, end, MONEY_FIELDS)),
        })
    # Ratios are calculated from pooled sums, never averaged across days or weeks.
    baseline = enrich({key: sum(block[key] for block in blocks[1:5]) / 4 for key in MONEY_FIELDS})
    details = {}
    countries = {}
    for label, weeks in (("after", 0), ("before", 1)):
        start, end = POST_START - timedelta(weeks=weeks), POST_END - timedelta(weeks=weeks)
        detail = item_review(rows, start, end, categories)
        financials = blocks[weeks]
        require(detail["orders"] == financials["orders"], "Order count reconciliation failed")
        require(abs(detail["order_revenue"] - financials["revenue"]) < 0.03,
                "Order revenue reconciliation failed")
        require(abs(detail["line_revenue"] + detail["order_credit_net"] -
                    detail["order_revenue"]) < 0.03, "Line/credit reconciliation failed")
        # CSV costs omit any separate credit-fulfillment sidecar; expose, do not hide, a difference.
        detail["published_minus_csv_product_cost"] = financials["product_cost"] - detail["product_cost"]
        details[label] = detail
        countries[label] = {}
        for country, data in dashboard["kpis"]["countries"].items():
            trend = data["windows"]["daily"]["trend"]
            values = period(dict(dates=trend["dates"], **trend["metrics"]),
                            start, end, ("revenue", "profit", "orders"))
            countries[label][country] = values
        for country_key, total_key in (("revenue", "revenue"), ("profit", "profit_without_fixed"),
                                       ("orders", "orders")):
            require(abs(sum(v[country_key] for v in countries[label].values()) -
                        financials[total_key]) < 0.03, "Country reconciliation failed")
    result = {
        "generation": PREFIX,
        "source_sha256": SOURCES,
        "quality": {"failures": health["qa_failure_count"], "warnings": health["qa_warning_count"]},
        "blocks_after_then_previous_eight": blocks,
        "previous_four_blocks_mean": baseline,
        "countries": countries,
        "details": details,
        "interpretation": {
            "design": "Observational before/after; not a randomized pricing experiment",
            "excluded": ["2026-11-09 mixed-price transition", "2026-11-13 incomplete day"],
            "quantities": "Parent product units; bundle=one unit; category orders overlap",
            "items_warning": "Dashboard items counts analytical rows, not product units",
            "cost_warning": "SKU contribution excludes packaging, shipping, ads and fixed costs",
            "persistence": "Three complete days cannot establish a lasting causal effect",
        },
    }
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))


if __name__ == "__main__":
    main()
