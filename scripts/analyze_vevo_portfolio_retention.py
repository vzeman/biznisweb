"""Offline sensitivity checks for the Natural/Premium portfolio decision.

This module never exports customer identities. It uses the reviewed item-level
catalogue, retains mixed baskets, and compares fixed follow-up after each
customer's original first purchase of each series. It makes no causal claim.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
from datetime import date, datetime, time, timedelta
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import random
import re
import statistics
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from projects.vevo.laundry_series_catalog import classify, classify_retention_components
from scripts.analyze_vevo_laundry_series import analyze, percent, wilson

SERIES = ("Natural", "Premium")
SHARED_SCENTS = {"07", "08", "09"}


def _scent(row: dict, product: dict | None) -> str | None:
    """Only identify a standalone bottle already admitted by the catalogue."""
    if not product or product["bottles_per_item"] != 1 or "set:" in product["basis"]:
        return None
    if product["basis"] == "verified_warehouse_variant":
        match = re.fullmatch(r"(0[1-9])(?:200|500)[cp]?", row["item_warehouse_number"].lower())
        if match:
            return match.group(1)
    match = re.search(r"\bno\.\s*(0[1-9])\b", row["item_label"].lower())
    return match.group(1) if match else None


def build_sequences(rows: list[dict]) -> list[list[dict]]:
    orders = {}
    for row in rows:
        if Decimal(row["item_total_without_tax"]) <= 0:
            continue
        components = classify_retention_components(row)
        if not components:
            continue
        product = classify(row)
        event = orders.setdefault(row["order_num"], {
            "date": datetime.fromisoformat(row["purchase_date"]),
            "customer": row["customer_email"].strip().lower(),
            "series": set(), "bottles": Counter(), "ml": Counter(),
            "sizes": defaultdict(set), "standalone_scents": defaultdict(set),
        })
        for component in components:
            series = component["series"]
            quantity = float(Decimal(row["item_quantity"])) * component["bottles_per_item"]
            event["series"].add(series)
            event["bottles"][series] += quantity
            event["ml"][series] += quantity * component["size_ml"]
            event["sizes"][series].add(component["size_ml"])
            # A mixed set has no standalone scent identity in this sensitivity.
            event["standalone_scents"][series].add(_scent(row, product))
    customers = defaultdict(list)
    for event in orders.values():
        customers[event["customer"]].append(event)
    return [sorted(events, key=lambda event: event["date"])
            for _, events in sorted(customers.items())]


def entry_records(sequences: list[list[dict]], end: datetime, start: datetime,
                  days: int) -> dict[str, list[dict]]:
    result = {series: [] for series in SERIES}
    for customer_index, seq in enumerate(sequences):
        for series in SERIES:
            index = next((i for i, event in enumerate(seq) if series in event["series"]), None)
            if index is None:
                continue
            entry = seq[index]
            if entry["date"] < start or entry["date"] + timedelta(days=days) > end:
                continue
            follow = [event for event in seq[index + 1:]
                      if event["date"] <= entry["date"] + timedelta(days=days)]
            prior = ("prior_other_series" if index else
                     "first_bottle_both_series" if len(entry["series"]) == 2 else
                     "first_bottle_only_this_series")
            scents = entry["standalone_scents"][series]
            single_shared = (sum(entry["bottles"].values()) == 1
                             and len(scents) == 1 and next(iter(scents)) in SHARED_SCENTS)
            result[series].append({
                "customer_index": customer_index, "same": any(series in event["series"] for event in follow),
                "any": bool(follow), "bottles": entry["bottles"][series],
                "ml": entry["ml"][series], "sizes": entry["sizes"][series],
                "single_shared": single_shared,
                "scent": next(iter(scents)) if single_shared else None,
                "prior": prior,
                "quarter": f"{entry['date'].year}Q{(entry['date'].month - 1) // 3 + 1}",
            })
    return result


def summarize(records: list[dict]) -> dict:
    n = len(records)
    same, any_return = (sum(record[key] for record in records) for key in ("same", "any"))
    return {"customers": n, "same": same, "any": any_return,
            "same_pct": percent(same, n), "any_pct": percent(any_return, n),
            "same_ci95_wilson": wilson(same, n),
            "entry_mean_ml": round(statistics.mean(record["ml"] for record in records), 3) if n else None,
            "entry_median_ml": statistics.median(record["ml"] for record in records) if n else None,
            "entry_mean_bottles": round(statistics.mean(record["bottles"] for record in records), 3) if n else None}


def bootstrap_difference(records: dict[str, list[dict]], draws: int = 5000, seed: int = 20261019) -> dict:
    """Resample whole customers to preserve Natural/Premium cohort overlap.

    The interval is descriptive sampling uncertainty conditional on these
    observational cohorts. It does not address selection, stockouts or dosage.
    """
    membership = defaultdict(lambda: [0, 0, 0, 0])
    for i, series in enumerate(SERIES):
        for record in records[series]:
            membership[record["customer_index"]][2 * i] = 1
            membership[record["customer_index"]][2 * i + 1] = int(record["same"])
    population = list(membership.values())
    original = [summarize(records[series]) for series in SERIES]
    if not population or any(not row["customers"] for row in original):
        return {"difference_pp": None, "ci95_percentile_pp": None, "valid_draws": 0}
    rng, differences = random.Random(seed), []
    for _ in range(draws):
        n_n = n_same = p_n = p_same = 0
        for _ in population:
            record = population[rng.randrange(len(population))]
            n_n += record[0]
            n_same += record[1]
            p_n += record[2]
            p_same += record[3]
        if n_n and p_n:
            differences.append(100 * (n_same / n_n - p_same / p_n))
    differences.sort()
    def quantile(q):
        pos = q * (len(differences) - 1)
        low = int(pos)
        high = min(low + 1, len(differences) - 1)
        return round(differences[low] + (pos - low) * (differences[high] - differences[low]), 4)
    return {"difference_pp": round(original[0]["same_pct"] - original[1]["same_pct"], 4),
            "ci95_percentile_pp": [quantile(.025), quantile(.975)] if differences else None,
            "unique_customers": len(population),
            "overlapping_customers": sum(record[0] and record[2] for record in population),
            "valid_draws": len(differences), "seed": seed,
            "method": "customer_cluster_bootstrap_Natural_minus_Premium"}


def post_both(sequences: list[list[dict]], end: datetime, days: int | None = None) -> dict:
    result = {"customers": 0, "returned": 0, "next": Counter(), "last": Counter()}
    for seq in sequences:
        seen = set()
        for i, event in enumerate(seq):
            seen.update(event["series"])
            if len(seen) == 2:
                break
        else:
            continue
        if days and event["date"] + timedelta(days=days) > end:
            continue
        result["customers"] += 1
        follow = seq[i + 1:]
        if days:
            follow = [later for later in follow if later["date"] <= event["date"] + timedelta(days=days)]
        if follow:
            result["returned"] += 1
            for position, key in ((0, "next"), (-1, "last")):
                series = follow[position]["series"]
                result[key][next(iter(series)) if len(series) == 1 else "Both"] += 1
    result["no_later_purchase"] = result["customers"] - result["returned"]
    return result


def calculate(rows: list[dict], end: date, bootstrap_draws: int = 5000) -> dict:
    # Reuse the main analysis' strict source validation and verify independent
    # event construction against its published denominators and numerators.
    baseline = analyze(rows, end)
    sequences = build_sequences(rows)
    cutoff = datetime.combine(end, time.max)
    start = datetime.fromisoformat(baseline["common_start"])
    windows = {}
    for days in (90, 180):
        records = entry_records(sequences, cutoff, start, days)
        all_metrics = {series: summarize(records[series]) for series in SERIES}
        for series in SERIES:
            expected = baseline["all_buyer_series_windows"][str(days)][series]
            actual = all_metrics[series]
            if (actual["customers"], actual["same"], actual["any"]) != (
                    expected["customers"], expected["same_any"], expected["repeat_any"]):
                raise ValueError("Independent retention reconstruction does not reconcile")
        filters = {
            "one_bottle_of_entered_series": lambda record: record["bottles"] == 1,
            "one_200_of_entered_series": lambda record: record["bottles"] == 1 and record["sizes"] == {200},
            "one_500_of_entered_series": lambda record: record["bottles"] == 1 and record["sizes"] == {500},
            "one_standalone_bottle_total_shared_scents": lambda record: record["single_shared"],
        }
        sensitivities = {}
        for key, predicate in filters.items():
            subset = {series: [record for record in records[series] if predicate(record)] for series in SERIES}
            sensitivities[key] = {series: summarize(subset[series]) for series in SERIES}
            if key == "one_standalone_bottle_total_shared_scents":
                sensitivities[key]["difference"] = bootstrap_difference(subset, bootstrap_draws)
                sensitivities[key]["strata"] = {
                    f"{scent}_{size}": {series: summarize([record for record in subset[series]
                                       if record["scent"] == scent and record["sizes"] == {size}])
                                       for series in SERIES}
                    for scent in sorted(SHARED_SCENTS) for size in (200, 500)}
                sensitivities[key]["by_size"] = {
                    str(size): {series: summarize([record for record in subset[series] if record["sizes"] == {size}])
                                for series in SERIES} for size in (200, 500)}
        windows[str(days)] = {"all": all_metrics,
            "difference": bootstrap_difference(records, bootstrap_draws),
            "sensitivities": sensitivities,
            "prior_exposure": {prior: {series: summarize([record for record in records[series] if record["prior"] == prior])
                                       for series in SERIES}
                               for prior in ("prior_other_series", "first_bottle_both_series", "first_bottle_only_this_series")},
            "entry_quarter": {quarter: {series: summarize([record for record in records[series] if record["quarter"] == quarter])
                                       for series in SERIES}
                              for quarter in sorted({record["quarter"] for cohort in records.values() for record in cohort})}}
    return {"common_start": baseline["common_start"], "end": end.isoformat(), "windows": windows,
            "post_both": {"all_observed": post_both(sequences, cutoff),
                          **{str(days): post_both(sequences, cutoff, days) for days in (90, 180)}},
            "definitions": {
                "entry": "Original first qualifying purchase of each series, never reset at common start; mixed sets count.",
                "same": "Any later order with the entered series inside the elapsed window; other basket items are allowed.",
                "single_shared": "Exactly one paid qualifying perfume bottle in the entire entry order, standalone SKU, scent 07/08/09; other merchandise allowed. Follow-up counts any scent of the same series.",
                "prior": "Prior-other means a qualifying bottle of the other series was purchased in an earlier order; a first mixed order is separate.",
                "post_both": "First order at which both series have ever been purchased starts the clock; only later orders count as choice after trying both.",
                "bootstrap": "Whole customer resampling preserves overlapping cohort membership. Conditional sampling uncertainty only, not causal identification.",
                "limitations": "No randomized assignment, historical stock availability, actual dosage/consumption or satisfaction responses; credited orders absent from realized source. Shared scent/size strata are small. Private customer data stays out of output."
            }}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--bootstrap-draws", type=int, default=5000)
    args = parser.parse_args()
    if args.bootstrap_draws < 1:
        parser.error("--bootstrap-draws must be positive")
    raw = args.input.read_bytes()
    with args.input.open(encoding="utf-8-sig", newline="") as source:
        result = calculate(list(csv.DictReader(source)), args.end, args.bootstrap_draws)
    result["source_sha256"] = hashlib.sha256(raw).hexdigest()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
    print(json.dumps({"output": str(args.output), "source_sha256": result["source_sha256"],
                      "windows": {days: value["all"] for days, value in result["windows"].items()}}))


if __name__ == "__main__":
    main()
