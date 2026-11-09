"""Offline descriptive timing analysis; no customer-level paid attribution.

Input is a pinned private publication directory. Output contains aggregates only.
Calendar and acquisition-cohort ledgers are separate; never add them together.
"""
import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd


def ratio(a, b):
    return float(a / b) if b else None


def timing(frame, date):
    frame = frame.copy()
    frame["weekday"] = date.dt.dayofweek
    frame["phase"] = ((date.dt.day - 1) // 7 + 1).clip(upper=5)
    frame["month"] = date.dt.to_period("M").astype(str)
    frame["year"] = date.dt.year
    frame["month_number"] = date.dt.month
    return frame


def run(source):
    manifest = json.loads((source / "generation.json").read_text(encoding="utf-8"))
    payload_path = source / "dashboard_payload_latest.json"
    assert hashlib.sha256(payload_path.read_bytes()).hexdigest() == manifest["artifacts"][payload_path.name]["sha256"]
    for receipt in json.loads((source / "receipts.json").read_text(encoding="utf-8")):
        assert hashlib.sha256((source / Path(receipt["key"]).name).read_bytes()).hexdigest() == receipt["sha256"]
    p = json.loads(payload_path.read_text(encoding="utf-8"))
    assert not p["source_health"]["is_partial"] and not p["source_health"]["qa_failure_count"]
    start, end = pd.Timestamp(p["date_from"]), pd.Timestamp(p["date_to"]) + pd.Timedelta(days=1)
    tag = f"{start:%Y%m%d}-{end - pd.Timedelta(days=1):%Y%m%d}"
    f = pd.read_csv(source / f"export_{tag}.csv", dtype=str, keep_default_na=False)
    d = pd.read_csv(source / f"aggregate_by_date_{tag}.csv")
    d["date"] = pd.to_datetime(d["date"])
    assert list(d["date"]) == list(pd.date_range(start, end, inclusive="left"))
    assert f.groupby("order_num")[["customer_email", "purchase_date", "order_revenue_net"]].nunique().le(1).all().all()
    f["cost"] = pd.to_numeric(f["total_expense"])
    o = f.drop_duplicates("order_num").copy()
    o["email"] = o["customer_email"].str.strip().str.lower()
    assert not o["email"].isin(["", "nan", "none"]).any()
    o["date"] = pd.to_datetime(o["purchase_date"])
    o = o.sort_values(["email", "date", "order_num"])
    assert not o.duplicated(["email", "date"]).any()
    o["first"] = o.groupby("email")["date"].transform("min")
    assert pd.to_datetime(o["customer_first_purchase_date"]).eq(o["first"]).all()
    o["sequence"] = o.groupby("email").cumcount() + 1
    o["revenue"] = pd.to_numeric(o["order_revenue_net"])
    o["cost"] = o["order_num"].map(f.groupby("order_num")["cost"].sum())
    # Same approved reporting estimate as the exported 90-day CM1 model.
    o["cm1"] = o["revenue"] - o["cost"] - 0.5
    o["day"] = o["date"].dt.normalize()
    o["age_days"] = (o["date"] - o["first"]).dt.total_seconds() / 86400
    o["market"] = o["delivery_country"].where(o["delivery_country"].ne(""), o["invoice_country"]).replace("", "unknown")
    c = o[o["sequence"].eq(1)].set_index("email")[["first", "market", "revenue", "cm1"]].copy()
    c = timing(c, c["first"])
    c["day"] = c["first"].dt.normalize()
    c["second_age"] = o[o["sequence"].eq(2)].set_index("email")["age_days"]
    d = timing(d, d["date"])
    d["ads"] = d["fb_ads_spend"] + d["google_ads_spend"]
    d["new"] = d["date"].map(c.groupby("day").size()).fillna(0).astype(int)
    d["complete_month"] = d["date"].dt.to_period("M").dt.start_time.ge(start) & d["date"].dt.to_period("M").dt.end_time.lt(end)
    # Date-ledger totals reconcile; credit-note fulfillment remains in calendar CM1.
    assert len(o) == d["unique_orders"].sum()
    assert abs(o["revenue"].sum() - d["total_revenue"].sum()) < .02
    assert abs(o["cm1"].sum() - d["pre_ad_contribution_profit"].sum() - d["creditnote_fulfillment_cost"].sum()) < .05
    assert c.shape[0] == d["new"].sum()
    per_day = d.set_index("date")
    o["allocated_ads"] = o["day"].map(per_day["ads"] / per_day["unique_orders"].replace(0, np.nan))
    for horizon in [30, 60, 90, 180]:
        window = o[o["age_days"].le(horizon)]
        c[f"eligible_{horizon}"] = c["first"] + pd.Timedelta(days=horizon) < end
        c[f"cm1_{horizon}"] = window.groupby("email")["cm1"].sum()
        c[f"cm2_alloc_{horizon}"] = window.assign(cm2=window["cm1"] - window["allocated_ads"]).groupby("email")["cm2"].sum()
        c[f"revenue_{horizon}"] = window.groupby("email")["revenue"].sum()
    def immediate(frame, dimension):
        rows = []
        for key, g in frame.groupby(dimension, sort=True):
            ads, new, cm1 = g["ads"].sum(), g["new"].sum(), g["pre_ad_contribution_profit"].sum()
            rows.append({"group": str(key), "days": len(g), "ads": float(ads), "orders": int(g["unique_orders"].sum()),
                         "new": int(new), "revenue": float(g["total_revenue"].sum()), "cm1": float(cm1),
                         "after_ads": float(cm1 - ads), "after_fixed": float(g["net_profit"].sum()),
                         "cm1_per_ad": ratio(cm1, ads), "net_revenue_per_ad": ratio(g["total_revenue"].sum(), ads),
                         "cac_blended": ratio(ads, new), "ads_per_day": ratio(ads, len(g)),
                         "after_ads_per_day": ratio(cm1 - ads, len(g))})
        return rows
    calendar = {}
    for name, selected in [("all", d), ("2025", d[d.year.eq(2025)]), ("2026", d[d.year.eq(2026)]),
                           ("recent90", d[d.date.ge(end - pd.Timedelta(days=90))]),
                           ("recent180", d[d.date.ge(end - pd.Timedelta(days=180))]),
                           ("paid_days_ge20", d[d.ads.ge(20)])]:
        calendar[name] = {dim: immediate(selected, dim) for dim in ["weekday", "phase", "month"]}
    cohort_tables = {}
    for horizon in [30, 60, 90, 180]:
        # Whole calendar days with everyone observable through the exact horizon.
        eligible_days = d[d.date.lt(end - pd.Timedelta(days=horizon))]
        cc = c[c.day.isin(eligible_days.date)].copy()
        assert cc[f"eligible_{horizon}"].all()
        tables = {}
        for dim in ["weekday", "phase", "month", "year", "market"]:
            rows = []
            for key, group in cc.groupby(dim, sort=True):
                ads = eligible_days.loc[eligible_days[dim].eq(key), "ads"].sum() if dim != "market" else None
                value = group[f"cm1_{horizon}"].sum()
                rows.append({"group": str(key), "customers": len(group),
                             "repeat": int(group.second_age.le(horizon).sum()),
                             "repeat_pct": 100 * group.second_age.le(horizon).mean(),
                             "cm1_per_customer": float(value / len(group)),
                             "revenue_per_customer": float(group[f"revenue_{horizon}"].mean()),
                             "first_cm1_per_customer": float(group.cm1.mean()),
                             "cm1_per_ad": ratio(value, ads) if ads is not None else None,
                             "cac_blended": ratio(ads, len(group)) if ads is not None else None,
                             "allocated_after_ads_per_customer": float(group[f"cm2_alloc_{horizon}"].mean()),
                             "ads": float(ads) if ads is not None else None,
                             "partial_end_month": bool(group.month.eq((end - pd.Timedelta(days=horizon)).strftime('%Y-%m')).any())})
            tables[dim] = rows
        cohort_tables[str(horizon)] = tables
        full_days = eligible_days[
            eligible_days.date.dt.to_period("M").dt.start_time.ge(start)
            & (eligible_days.date.dt.to_period("M").dt.end_time + pd.Timedelta(days=horizon)).lt(end)
        ]
        full_customers = cc[cc.day.isin(full_days.date)]
        for dim in ["phase", "weekday"]:
            tables[dim + "_complete_months"] = [
                {"group": str(key), "customers": len(group),
                 "repeat_pct": 100 * group.second_age.le(horizon).mean(),
                 "cm1_per_customer": float(group[f"cm1_{horizon}"].mean()),
                 "cm1_per_ad": ratio(group[f"cm1_{horizon}"].sum(), full_days.loc[full_days[dim].eq(key), "ads"].sum())}
                for key, group in full_customers.groupby(dim)
            ]
    # Month-standardized comparisons, full calendar months only. This adjusts
    # the background monthly economics, not unobserved campaigns or causality.
    full = d[d.complete_month].copy()
    months = full.groupby("month")[["pre_ad_contribution_profit", "ads", "new"]].sum()
    full["expected_cm1"] = full.ads * full.month.map(months.pre_ad_contribution_profit / months.ads)
    full["expected_new"] = full.ads * full.month.map(months.new / months.ads)
    stability = {}
    for dim in ["weekday", "phase"]:
        rows = []
        for key, g in full.groupby(dim):
            monthly = g.groupby("month")[["pre_ad_contribution_profit", "expected_cm1", "new", "expected_new", "ads"]].sum()
            # Avoid near-zero budget-month artifacts in consistency counts.
            monthly = monthly[months.loc[monthly.index, "ads"].ge(300) & monthly.ads.gt(0)]
            rows.append({"group": str(key), "cm1_month_adjusted_index": ratio(g.pre_ad_contribution_profit.sum(), g.expected_cm1.sum()),
                         "new_month_adjusted_index": ratio(g.new.sum(), g.expected_new.sum()),
                         "months": len(monthly), "months_cm1_above_average": int(monthly.pre_ad_contribution_profit.gt(monthly.expected_cm1).sum()),
                         "months_new_above_average": int(monthly.new.gt(monthly.expected_new).sum())})
        stability[dim] = rows
    matched = []
    for month, group in full.groupby("month"):
        selected, other = group[group.phase.eq(2)], group[~group.phase.eq(2)]
        left = ratio(selected.pre_ad_contribution_profit.sum(), selected.ads.sum())
        right = ratio(other.pre_ad_contribution_profit.sum(), other.ads.sum())
        matched.append({"month": month, "phase2_ads": float(selected.ads.sum()),
                        "phase2_ratio": left, "other_ratio": right,
                        "eligible_50eur": bool(selected.ads.sum() >= 50 and group.ads.sum() >= 300),
                        "higher": bool(left > right) if left is not None and right is not None else None})
    return {"source_generation": manifest["generation_id"], "from": p["date_from"], "to": p["date_to"],
            "source_receipts": json.loads((source / "receipts.json").read_text(encoding="utf-8")),
            "totals": immediate(d.assign(all="all"), "all")[0], "calendar": calendar,
            "cohorts": cohort_tables, "stability": stability, "matched_phase2_months": matched,
            "zero_order_days": int(d.unique_orders.eq(0).sum()),
            "zero_order_spend": float(d.loc[d.unique_orders.eq(0), "ads"].sum()),
            "meta_spend_share": float(d.fb_ads_spend.sum() / d.ads.sum()),
            "qa_warning_count": p["source_health"]["qa_warning_count"]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = run(args.source)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps(result["totals"], indent=2))
