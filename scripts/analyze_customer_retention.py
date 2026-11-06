"""Read-only, offline retention analysis of one published order export.

Only aggregate output is written. Input remains private and outside Git.
Identity is email.strip().lower(); orders are deduplicated by order_num.
Fixed windows use elapsed time and require complete customer follow-up.
"""

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


def pct(numerator, denominator):
    return float(numerator / denominator * 100) if denominator else None


def analyze(csv_path: Path, payload_path: Path):
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    frame = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    end = pd.Timestamp(payload["date_to"]) + pd.Timedelta(days=1)
    start = pd.Timestamp(payload["date_from"])
    required = ["order_num", "customer_email", "purchase_date", "order_revenue_net"]
    assert all(column in frame for column in required)
    assert not frame["order_num"].eq("").any()
    assert frame.groupby("order_num")[required[1:]].nunique().le(1).all().all()
    orders = frame.drop_duplicates("order_num").copy()
    orders["raw_email"] = orders["customer_email"]
    orders["email"] = orders["customer_email"].str.strip().str.lower()
    assert not orders["email"].isin(["", "nan", "none"]).any()
    orders["date"] = pd.to_datetime(orders["purchase_date"], errors="raise")
    assert orders["date"].ge(start).all() and orders["date"].lt(end).all()
    assert not orders.duplicated(["email", "date"]).any()
    orders["revenue"] = pd.to_numeric(orders["order_revenue_net"], errors="raise")
    orders = orders.sort_values(["email", "date", "order_num"])
    orders["sequence"] = orders.groupby("email").cumcount() + 1
    orders["first"] = orders.groupby("email")["date"].transform("min")
    historical = pd.to_datetime(orders["customer_first_purchase_date"], errors="raise")
    assert historical.eq(orders["first"]).all(), "Export has prior history; revise cohort scope"
    orders["repeat"] = orders["sequence"].gt(1)
    assert orders["repeat"].eq(orders["is_returning_customer_order"].eq("True")).all()
    orders["month"] = orders["date"].dt.to_period("M").astype(str)
    orders["market"] = orders["delivery_country"].where(
        orders["delivery_country"].ne(""), orders["invoice_country"]
    ).str.strip().str.lower().replace("", "unknown")
    customers = orders.groupby("email").agg(
        first=("date", "min"), orders=("order_num", "size"), revenue=("revenue", "sum")
    )
    customers["second"] = orders.loc[orders["sequence"].eq(2)].set_index("email")["date"]
    customers["delay_days"] = (customers["second"] - customers["first"]).dt.total_seconds() / 86400
    customers["cohort"] = customers["first"].dt.to_period("M").astype(str)
    customers["market"] = orders.loc[orders["sequence"].eq(1)].set_index("email")["market"]

    def window(group, days):
        eligible = group["first"] + pd.Timedelta(days=days) < end
        converted = eligible & group["delay_days"].le(days)
        return {"eligible": int(eligible.sum()), "repeat": int(converted.sum()),
                "pct": pct(converted.sum(), eligible.sum())}

    monthly = []
    for month, group in orders.groupby("month", sort=True):
        month_start = pd.Period(month).start_time
        prior = group["first"].lt(month_start)
        n = group["email"].nunique()
        monthly.append({
            "month": month, "complete_month": bool(month_start >= start and pd.Period(month).end_time < end),
            "orders": len(group), "customers": n,
            "new_customers": int(group.loc[~prior, "email"].nunique()),
            "prior_month_customers": int(group.loc[prior, "email"].nunique()),
            "prior_month_customer_pct": pct(group.loc[prior, "email"].nunique(), n),
            "customers_with_repeat_order": int(group.loc[group["repeat"], "email"].nunique()),
            "customers_with_repeat_order_pct": pct(group.loc[group["repeat"], "email"].nunique(), n),
            "repeat_orders": int(group["repeat"].sum()), "repeat_order_pct": pct(group["repeat"].sum(), len(group)),
            "revenue": float(group["revenue"].sum()),
            "repeat_revenue_pct": pct(group.loc[group["repeat"], "revenue"].sum(), group["revenue"].sum()),
        })
    cohorts = []
    for month, group in customers.groupby("cohort", sort=True):
        row = {"cohort": month, "customers": len(group),
               "repeat_customers": int(group["orders"].ge(2).sum()),
               "repeat_pct": pct(group["orders"].ge(2).sum(), len(group)),
               "third_order_pct": pct(group["orders"].ge(3).sum(), len(group))}
        for days in [30, 60, 90, 180, 365]:
            result = window(group, days)
            row[f"eligible_{days}d"] = result["eligible"]
            row[f"repeat_{days}d"] = result["repeat"] if result["eligible"] == len(group) else None
            row[f"pct_{days}d"] = result["pct"] if result["eligible"] == len(group) else None
        cohorts.append(row)

    raw_counts = orders.groupby("raw_email").size()
    published = payload["dashboard"]["cohort_summary"]
    assert len(raw_counts) == published["total_customers"]
    assert int(raw_counts.ge(2).sum()) == published["repeat_customers"]
    kpi = payload["dashboard"]["kpis"]["windows"]["all_time"]["metrics"]
    assert len(orders) == kpi["orders"]
    assert abs(orders["revenue"].sum() - kpi["revenue"]) < 0.02
    repeats = orders["repeat"]
    recent = orders[orders["date"].ge(end - pd.Timedelta(days=30))]
    summary = {
        "customers": len(customers), "repeat_customers": int(customers["orders"].ge(2).sum()),
        "repeat_customer_pct": pct(customers["orders"].ge(2).sum(), len(customers)),
        "orders": len(orders), "repeat_orders": int(repeats.sum()),
        "repeat_order_pct": pct(repeats.sum(), len(orders)),
        "net_revenue": float(orders["revenue"].sum()),
        "repeat_order_net_revenue": float(orders.loc[repeats, "revenue"].sum()),
        "repeat_order_revenue_pct": pct(orders.loc[repeats, "revenue"].sum(), orders["revenue"].sum()),
        "median_days_to_second_among_repeaters": float(customers["delay_days"].median()),
        "mean_days_to_second_among_repeaters": float(customers["delay_days"].mean()),
        "recent_30d_orders": len(recent), "recent_30d_repeat_orders": int(recent["repeat"].sum()),
        "recent_30d_repeat_order_pct": pct(recent["repeat"].sum(), len(recent)),
        "customers_3plus": int(customers["orders"].ge(3).sum()),
        "customers_5plus": int(customers["orders"].ge(5).sum()),
        "within_24h_second_orders": int(customers["delay_days"].lt(1).sum()),
        "fixed_windows": {str(days): window(customers, days) for days in [30, 60, 90, 180, 365]},
    }
    markets = []
    for market, group in customers.groupby("market"):
        markets.append({"first_order_market": market, "customers": len(group),
                        "repeat_customers": int(group["orders"].ge(2).sum()),
                        "repeat_pct": pct(group["orders"].ge(2).sum(), len(group)),
                        "window_90d": window(group, 90)})
    cohort_markets = []
    for (month, market), group in customers.groupby(["cohort", "market"]):
        cohort_markets.append({"cohort": month, "first_order_market": market,
                              "customers": len(group), "window_30d": window(group, 30)})
    quarter_comparison = []
    for quarter, group in customers.groupby(customers["first"].dt.to_period("Q")):
        result = window(group, 90)
        quarter_comparison.append({"quarter": str(quarter), "customers": len(group),
                                   "repeat_90d": result["repeat"] if result["eligible"] == len(group) else None,
                                   "pct_90d": result["pct"] if result["eligible"] == len(group) else None})
    assert sum(r["customers"] for r in cohorts) == len(customers)
    assert sum(r["repeat_customers"] for r in cohorts) == summary["repeat_customers"]
    assert sum(r["orders"] for r in monthly) == len(orders)
    assert sum(r["repeat_orders"] for r in monthly) == summary["repeat_orders"]
    return {
        "date_from": payload["date_from"], "date_to": payload["date_to"],
        "generated_at": payload["generated_at"],
        "source_sha256": hashlib.sha256(csv_path.read_bytes()).hexdigest(),
        "payload_sha256": hashlib.sha256(payload_path.read_bytes()).hexdigest(),
        "published_summary": published, "summary": summary, "cohorts": cohorts,
        "monthly": monthly, "markets_by_first_order": markets,
        "cohort_markets": cohort_markets, "quarter_comparison": quarter_comparison,
        "data_quality": {key: payload["source_health"][key] for key in [
            "is_partial", "qa_failure_count", "qa_warning_count", "summary"]},
        "identity_audit": {"raw_unique_emails": len(raw_counts), "normalized_unique_emails": len(customers),
                           "merged_email_groups": int(orders.groupby("email")["raw_email"].nunique().gt(1).sum())},
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", required=True, type=Path)
    parser.add_argument("--payload", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = analyze(args.csv, args.payload)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps(result["summary"], ensure_ascii=False, indent=2))
