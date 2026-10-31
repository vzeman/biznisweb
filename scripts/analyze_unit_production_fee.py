"""Read-only scenario calculation from a saved reporting generation.

This utility never downloads, publishes, or changes reporting configuration.
Its JSON output contains private business data and belongs outside public Git.
"""

import argparse
import csv
import hashlib
import json
from collections import defaultdict
from pathlib import Path


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot_dir", type=Path)
    parser.add_argument("--settings", type=Path, required=True)
    parser.add_argument("--monthly-avoided-cost", type=float, required=True)
    parser.add_argument("--per-unit-fee", type=float, required=True)
    parser.add_argument("--cost-in-fixed", action="store_true")
    parser.add_argument("--history-from", required=True, help="YYYY-MM")
    args = parser.parse_args()
    if args.monthly_avoided_cost <= 0 or args.per_unit_fee <= 0:
        parser.error("Both cost parameters must be positive")
    root = args.snapshot_dir
    manifest = read_json(root / "generation.json")
    settings = read_json(args.settings)
    hashes = {
        "generation.json": hashlib.sha256((root / "generation.json").read_bytes()).hexdigest(),
        "project_settings": hashlib.sha256(args.settings.read_bytes()).hexdigest(),
        "analysis_script": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }

    def verified_payload(name):
        raw = (root / name).read_bytes()
        record = manifest["artifacts"][name]
        digest = hashlib.sha256(raw).hexdigest()
        if len(raw) != record["size"] or digest != record["sha256"]:
            raise ValueError(f"Manifest mismatch: {name}")
        hashes[name] = digest
        return json.loads(raw)

    payload = verified_payload("dashboard_payload_30d.json")
    if payload["project"] != manifest["project"]:
        raise ValueError("Project mismatch")
    series = payload["dashboard"]["series"]
    coverage = payload["source_health"]["qa"]["product_expense_coverage"]
    services = [row for row in coverage["source_mix"]
                if row["expense_source"] == "zero_cost_service_override"]
    units = coverage["total_units"] - sum(row["units"] for row in services)
    totals = {key: sum(series[key]) for key in (
        "revenue", "orders", "product_cost", "packaging", "shipping",
        "fixed", "total_ads", "profit_with_fixed")}
    goods_revenue = totals["revenue"] - sum(row["revenue"] for row in services)
    if units <= 0 or goods_revenue <= 0:
        raise ValueError("Positive goods quantity and revenue are required")
    fee = units * args.per_unit_fee
    saving = args.monthly_avoided_cost - fee
    variable = totals["product_cost"] + totals["packaging"] + totals["shipping"]
    before_ads_ratio = (totals["revenue"] - variable - fee) / totals["revenue"]
    after_ads_ratio = before_ads_ratio - totals["total_ads"] / totals["revenue"]
    remaining_fixed = totals["fixed"] - args.monthly_avoided_cost
    result = {
        "generation": manifest["generation_id"],
        "project": payload["project"],
        "date_from": payload["date_from"], "date_to": payload["date_to"],
        "days": len(series["dates"]),
        "monthly_avoided_cost": args.monthly_avoided_cost,
        "per_sale_unit_fee": args.per_unit_fee,
        "avoided_cost_in_report_fixed_confirmed": args.cost_in_fixed,
        "reported_totals": totals,
        "goods_units": units, "excluded_service_units": coverage["total_units"] - units,
        "goods_revenue": goods_revenue, "goods_revenue_per_unit": goods_revenue / units,
        "fee_for_observed_period": fee,
        "saving_against_one_month_cost": saving,
        "crossover_goods_revenue": args.monthly_avoided_cost / args.per_unit_fee * goods_revenue / units,
        "crossover_units": args.monthly_avoided_cost / args.per_unit_fee,
        "model_fixed_assumption": payload["source_health"]["reporting_assumptions"]["fixed_cost"],
        "qa_warnings": payload["source_health"]["qa_warnings"],
    }
    if args.cost_in_fixed:
        if remaining_fixed < 0:
            raise ValueError("Avoided cost exceeds reported fixed overhead")
        result.update({
            "new_profit_for_observed_period": totals["profit_with_fixed"] + saving,
            "remaining_fixed": remaining_fixed,
            "break_even_fixed_ads": (remaining_fixed + totals["total_ads"]) / before_ads_ratio if before_ads_ratio > 0 else None,
            "break_even_proportional_ads": remaining_fixed / after_ads_ratio if after_ads_ratio > 0 else None,
        })

    csv_paths = []
    for pattern in ("aggregate_by_date_product_*.csv", "aggregate_by_month_*.csv"):
        matches = list(root.glob(pattern))
        if len(matches) != 1:
            raise ValueError(f"Expected one CSV for {pattern}, found {len(matches)}")
        csv_paths.append(matches[0])
    product_rows = list(csv.DictReader(csv_paths[0].open(encoding="utf-8-sig")))
    if not product_rows or max(row["date"] for row in product_rows) != payload["date_to"]:
        raise ValueError("Product CSV does not end on the dashboard date")
    service_skus = set(settings["zero_cost_service_product_skus"])
    service_labels = set(settings["zero_cost_service_labels"])
    service_months = defaultdict(lambda: {"units": 0.0, "revenue": 0.0})
    goods_units_in_window = 0.0
    for row in product_rows:
        sku_match = row["product_sku"] in service_skus
        label_match = row["product_name"] in service_labels
        if sku_match != label_match:
            raise ValueError("Service identity requires matching reviewed SKU and label")
        if sku_match:
            month = service_months[row["date"][:7]]
            month["units"] += float(row["total_quantity"])
            month["revenue"] += float(row["total_revenue"])
        elif payload["date_from"] <= row["date"] <= payload["date_to"]:
            goods_units_in_window += float(row["total_quantity"])
    if abs(goods_units_in_window - units) > 0.000001:
        raise ValueError("Product CSV merchandise units disagree with dashboard QA")
    history = []
    for row in csv.DictReader(csv_paths[1].open(encoding="utf-8-sig")):
        if row["month"] < args.history_from:
            continue
        service = service_months[row["month"]]
        quantity = float(row["total_quantity"]) - service["units"]
        history.append({
            "month": row["month"], "goods_units": quantity,
            "goods_revenue": float(row["total_revenue"]) - service["revenue"],
            "report_revenue": float(row["total_revenue"]),
            "saving_against_one_month_cost": args.monthly_avoided_cost - quantity * args.per_unit_fee,
            "reported_profit": float(row["net_profit"]),
        })
    if not history or history[-1]["month"] != payload["date_to"][:7]:
        raise ValueError("Monthly CSV does not cover the dashboard ending month")
    for path in csv_paths:
        hashes[path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    result["monthly_history"] = history
    result["source_sha256"] = hashes
    print(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
