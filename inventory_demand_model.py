"""Robust, order-aware demand signals for inventory replenishment.

The model deliberately keeps physical stock movement separate from recurring
demand. A large order always reduces real stock, but only its robustly capped
quantity is allowed to influence the recurring demand baseline until repeated
orders confirm that the higher demand is persistent.
"""

from __future__ import annotations

import math
from typing import Any, Dict, Iterable, List, Mapping, Optional

import numpy as np
import pandas as pd


ROBUST_DEMAND_COLUMNS = [
    "product_sku",
    "raw_recent_30d_units",
    "adjusted_recent_30d_units",
    "adjusted_recent_90d_units",
    "adjusted_recent_180d_units",
    "robust_baseline_30d_units",
    "unusual_large_order_flag",
    "unusual_large_order_count_30d",
    "unusual_large_order_units_30d",
    "unusual_large_order_adjustment_units_30d",
    "largest_order_units_30d",
    "largest_order_share_30d_pct",
    "largest_unusual_order_num",
    "largest_unusual_order_date",
    "order_count_30d",
    "customer_count_30d",
    "active_weeks_30d",
    "positive_order_count_history",
    "history_observation_days",
    "typical_order_units",
    "outlier_threshold_units",
    "order_rate_per_day",
    "weekly_zero_share_pct",
    "demand_model",
    "demand_confidence",
    "demand_signal_code",
    "demand_signal_label",
    "trend_candidate_flag",
    "trend_growth_ratio",
    "confirmed_repeated_bulk_flag",
]


def _positive_float(value: Any, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(parsed) or parsed <= 0:
        return default
    return parsed


def _positive_int(value: Any, default: int) -> int:
    return max(1, int(round(_positive_float(value, float(default)))))


def _order_size_threshold(
    quantities: np.ndarray,
    *,
    global_threshold: float,
    minimum_history_orders: int,
    iqr_multiplier: float,
    median_multiplier: float,
    sparse_median_multiplier: float,
    minimum_outlier_units: float,
) -> float:
    values = np.asarray(quantities, dtype=float)
    values = values[np.isfinite(values) & (values > 0)]
    if values.size == 0:
        return minimum_outlier_units

    median = float(np.median(values))
    if values.size >= minimum_history_orders:
        q1, q3 = np.quantile(values, [0.25, 0.75])
        iqr = max(float(q3 - q1), 0.0)
        return max(
            float(q3) + (iqr_multiplier * iqr),
            median * median_multiplier,
            minimum_outlier_units,
        )

    if values.size >= 3:
        return max(median * sparse_median_multiplier, minimum_outlier_units)

    # With only two observations, the lower half distinguishes [1, 40] from
    # [40, 40] without pretending that one observation establishes a pattern.
    lower_half = values[values <= median]
    sparse_reference = float(np.median(lower_half)) if lower_half.size else median
    sparse_threshold = max(sparse_reference * sparse_median_multiplier, minimum_outlier_units)
    return min(max(global_threshold, minimum_outlier_units), sparse_threshold) if values.size > 1 else max(
        sparse_threshold,
        minimum_outlier_units,
    )


def _tsb_weekly_forecast(values: np.ndarray, *, size_alpha: float, occurrence_beta: float) -> float:
    series = np.asarray(values, dtype=float)
    series = np.where(np.isfinite(series) & (series > 0), series, 0.0)
    if series.size == 0 or not np.any(series > 0):
        return 0.0

    initial_window = series[: min(8, series.size)]
    occurrence_probability = float(np.mean(initial_window > 0))
    occurrence_probability = max(occurrence_probability, 1.0 / float(series.size))
    positive_values = series[series > 0]
    typical_size = float(np.median(positive_values))

    for value in series:
        occurrence = 1.0 if value > 0 else 0.0
        occurrence_probability = (
            occurrence_beta * occurrence
            + (1.0 - occurrence_beta) * occurrence_probability
        )
        if value > 0:
            typical_size = size_alpha * float(value) + (1.0 - size_alpha) * typical_size

    return max(occurrence_probability * typical_size, 0.0)


def _window_units(
    orders: pd.DataFrame,
    *,
    date_column: str,
    quantity_column: str,
    cutoff: pd.Timestamp,
) -> float:
    return float(orders.loc[orders[date_column] >= cutoff, quantity_column].sum())


def build_robust_demand_summary(
    demand_df: pd.DataFrame,
    demand_anchor: Any,
    config: Optional[Mapping[str, Any]] = None,
) -> pd.DataFrame:
    """Return one robust recurring-demand row per SKU.

    Raw quantities are never modified. ``adjusted_order_units`` is a modeling
    copy that caps an unconfirmed one-off order at a robust positive-order
    threshold. Repeated large orders in multiple weeks are treated as a new
    recurring stream instead of being suppressed indefinitely.
    """

    cfg: Mapping[str, Any] = config or {}
    if demand_df is None or demand_df.empty:
        return pd.DataFrame(columns=ROBUST_DEMAND_COLUMNS)

    lookback_days = _positive_int(cfg.get("lookback_days"), 365)
    minimum_history_orders = _positive_int(cfg.get("minimum_history_orders"), 12)
    iqr_multiplier = _positive_float(cfg.get("iqr_multiplier"), 1.5)
    median_multiplier = _positive_float(cfg.get("median_multiplier"), 3.0)
    sparse_median_multiplier = _positive_float(cfg.get("sparse_median_multiplier"), 4.0)
    minimum_outlier_units = _positive_float(cfg.get("minimum_outlier_units"), 5.0)
    intermittent_zero_week_threshold = min(
        max(_positive_float(cfg.get("intermittent_zero_week_threshold"), 0.5), 0.0),
        1.0,
    )
    tsb_size_alpha = min(max(_positive_float(cfg.get("tsb_size_alpha"), 0.05), 0.001), 1.0)
    tsb_occurrence_beta = min(
        max(_positive_float(cfg.get("tsb_occurrence_beta"), 0.05), 0.001),
        1.0,
    )
    trend_window_days = _positive_int(cfg.get("trend_window_days"), 45)
    trend_confirmation_min_orders = _positive_int(
        cfg.get("trend_confirmation_min_orders"),
        3,
    )
    trend_confirmation_min_active_weeks = _positive_int(
        cfg.get("trend_confirmation_min_active_weeks"),
        2,
    )
    trend_growth_ratio_required = _positive_float(cfg.get("trend_growth_ratio"), 1.5)
    trend_min_units_30d = _positive_float(cfg.get("trend_min_units_30d"), 4.0)

    anchor = pd.to_datetime(demand_anchor, errors="coerce")
    if pd.isna(anchor):
        return pd.DataFrame(columns=ROBUST_DEMAND_COLUMNS)
    anchor = pd.Timestamp(anchor).normalize()

    source = demand_df.copy()
    for column, default in (
        ("product_sku", ""),
        ("order_num", ""),
        ("customer_email", ""),
        ("item_label", ""),
    ):
        if column not in source.columns:
            source[column] = default
    if "purchase_datetime" not in source.columns:
        return pd.DataFrame(columns=ROBUST_DEMAND_COLUMNS)
    if "item_quantity" not in source.columns:
        return pd.DataFrame(columns=ROBUST_DEMAND_COLUMNS)

    source["purchase_datetime"] = pd.to_datetime(source["purchase_datetime"], errors="coerce")
    source["item_quantity"] = pd.to_numeric(source["item_quantity"], errors="coerce").fillna(0.0)
    source["product_sku"] = source["product_sku"].fillna("").astype(str).str.strip()
    source = source.loc[
        source["purchase_datetime"].notna()
        & source["product_sku"].ne("")
        & source["item_quantity"].ne(0)
    ].copy()
    if source.empty:
        return pd.DataFrame(columns=ROBUST_DEMAND_COLUMNS)
    source["_model_order_num"] = source["order_num"].fillna("").astype(str).str.strip()
    missing_order_num = source["_model_order_num"].isin({"", "nan", "none"})
    source.loc[missing_order_num, "_model_order_num"] = source.index[missing_order_num].map(
        lambda row_index: f"__missing_order_{row_index}"
    )

    order_demand = (
        source.groupby(["product_sku", "_model_order_num"], dropna=False)
        .agg(
            product=("item_label", "first"),
            customer=("customer_email", "first"),
            order_datetime=("purchase_datetime", "min"),
            raw_order_units=("item_quantity", "sum"),
        )
        .reset_index()
        .rename(columns={"_model_order_num": "order_num"})
    )
    order_demand["order_datetime"] = pd.to_datetime(order_demand["order_datetime"], errors="coerce")
    order_demand = order_demand.loc[
        order_demand["order_datetime"].notna()
        & (order_demand["raw_order_units"] > 0)
        & (order_demand["order_datetime"].dt.normalize() <= anchor)
    ].copy()
    lookback_cutoff = anchor - pd.Timedelta(days=lookback_days - 1)
    order_demand = order_demand.loc[order_demand["order_datetime"].dt.normalize() >= lookback_cutoff].copy()
    if order_demand.empty:
        return pd.DataFrame(columns=ROBUST_DEMAND_COLUMNS)

    global_quantities = order_demand["raw_order_units"].to_numpy(dtype=float)
    global_median = float(np.median(global_quantities))
    global_q1, global_q3 = np.quantile(global_quantities, [0.25, 0.75])
    global_threshold = max(
        float(global_q3) + iqr_multiplier * max(float(global_q3 - global_q1), 0.0),
        global_median * median_multiplier,
        minimum_outlier_units,
    )

    recent_30d_cutoff = anchor - pd.Timedelta(days=29)
    recent_90d_cutoff = anchor - pd.Timedelta(days=89)
    recent_180d_cutoff = anchor - pd.Timedelta(days=179)
    trend_cutoff = anchor - pd.Timedelta(days=trend_window_days - 1)
    prior_trend_cutoff = anchor - pd.Timedelta(days=119)
    rows: List[Dict[str, Any]] = []

    for sku, sku_orders in order_demand.groupby("product_sku", sort=False):
        sku_orders = sku_orders.sort_values("order_datetime").copy()
        quantities = sku_orders["raw_order_units"].to_numpy(dtype=float)
        threshold = _order_size_threshold(
            quantities,
            global_threshold=global_threshold,
            minimum_history_orders=minimum_history_orders,
            iqr_multiplier=iqr_multiplier,
            median_multiplier=median_multiplier,
            sparse_median_multiplier=sparse_median_multiplier,
            minimum_outlier_units=minimum_outlier_units,
        )
        sku_orders["outlier_candidate"] = sku_orders["raw_order_units"] > (threshold + 1e-9)
        sku_orders["week_start"] = sku_orders["order_datetime"].dt.to_period("W").dt.start_time

        trend_orders = sku_orders.loc[sku_orders["order_datetime"].dt.normalize() >= trend_cutoff]
        recent_outlier_orders = trend_orders.loc[trend_orders["outlier_candidate"]]
        confirmed_repeated_bulk = bool(
            recent_outlier_orders["order_num"].nunique() >= trend_confirmation_min_orders
            and recent_outlier_orders["week_start"].nunique() >= trend_confirmation_min_active_weeks
        )
        confirmed_bulk_mask = (
            sku_orders["outlier_candidate"]
            & (sku_orders["order_datetime"].dt.normalize() >= recent_90d_cutoff)
            & confirmed_repeated_bulk
        )
        sku_orders["adjusted_order_units"] = np.where(
            sku_orders["outlier_candidate"] & ~confirmed_bulk_mask,
            np.minimum(sku_orders["raw_order_units"], threshold),
            sku_orders["raw_order_units"],
        )
        sku_orders["unconfirmed_outlier"] = sku_orders["outlier_candidate"] & ~confirmed_bulk_mask

        recent_30 = sku_orders.loc[
            sku_orders["order_datetime"].dt.normalize() >= recent_30d_cutoff
        ].copy()
        raw_recent_30d_units = float(recent_30["raw_order_units"].sum())
        adjusted_recent_30d_units = float(recent_30["adjusted_order_units"].sum())
        adjusted_recent_90d_units = _window_units(
            sku_orders,
            date_column="order_datetime",
            quantity_column="adjusted_order_units",
            cutoff=recent_90d_cutoff,
        )
        adjusted_recent_180d_units = _window_units(
            sku_orders,
            date_column="order_datetime",
            quantity_column="adjusted_order_units",
            cutoff=recent_180d_cutoff,
        )

        first_order_date = pd.Timestamp(sku_orders["order_datetime"].min()).normalize()
        observation_start = max(first_order_date, lookback_cutoff)
        observation_days = max((anchor - observation_start).days + 1, 1)
        weekly_start = observation_start.to_period("W").start_time
        weekly_end = anchor.to_period("W").start_time
        weekly_index = pd.date_range(weekly_start, weekly_end, freq="W-MON")
        weekly_adjusted = (
            sku_orders.groupby("week_start")["adjusted_order_units"]
            .sum()
            .reindex(weekly_index, fill_value=0.0)
        )
        weekly_zero_share = float(np.mean(weekly_adjusted.to_numpy(dtype=float) <= 0)) if len(weekly_adjusted) else 1.0
        intermittent = weekly_zero_share >= intermittent_zero_week_threshold

        window_days_30 = min(observation_days, 30)
        window_days_90 = min(observation_days, 90)
        window_days_180 = min(observation_days, 180)
        monthly_rates = np.asarray(
            [
                adjusted_recent_30d_units * 30.0 / max(window_days_30, 1),
                adjusted_recent_90d_units * 30.0 / max(window_days_90, 1),
                adjusted_recent_180d_units * 30.0 / max(window_days_180, 1),
            ],
            dtype=float,
        )
        unconfirmed_recent_anomaly = bool(recent_30["unconfirmed_outlier"].any())
        weights = np.asarray(
            [0.15, 0.40, 0.45] if unconfirmed_recent_anomaly else [0.50, 0.30, 0.20],
            dtype=float,
        )
        robust_multi_horizon = float(np.average(monthly_rates, weights=weights))
        tsb_weekly = _tsb_weekly_forecast(
            weekly_adjusted.to_numpy(dtype=float),
            size_alpha=tsb_size_alpha,
            occurrence_beta=tsb_occurrence_beta,
        )
        tsb_30d = tsb_weekly * (30.0 / 7.0)

        positive_adjusted_sizes = sku_orders.loc[
            sku_orders["adjusted_order_units"] > 0,
            "adjusted_order_units",
        ]
        typical_order_units = (
            float(positive_adjusted_sizes.median())
            if not positive_adjusted_sizes.empty
            else 0.0
        )
        if confirmed_repeated_bulk:
            robust_baseline_30d_units = robust_multi_horizon
            demand_model = "confirmed_repeated_bulk"
        elif intermittent:
            robust_baseline_30d_units = tsb_30d
            demand_model = "tsb_intermittent"
        else:
            robust_baseline_30d_units = robust_multi_horizon
            demand_model = "robust_multi_horizon"

        prior_trend_orders = sku_orders.loc[
            (sku_orders["order_datetime"].dt.normalize() >= prior_trend_cutoff)
            & (sku_orders["order_datetime"].dt.normalize() < recent_30d_cutoff)
        ]
        prior_monthly_units = float(prior_trend_orders["adjusted_order_units"].sum()) * (30.0 / 90.0)
        trend_reference = max(prior_monthly_units, typical_order_units, 1e-9)
        trend_growth_ratio = adjusted_recent_30d_units / trend_reference
        order_count_30d = int(recent_30["order_num"].nunique())
        active_weeks_30d = int(recent_30["week_start"].nunique())
        trend_candidate = bool(
            not unconfirmed_recent_anomaly
            and adjusted_recent_30d_units >= trend_min_units_30d
            and order_count_30d >= trend_confirmation_min_orders
            and active_weeks_30d >= trend_confirmation_min_active_weeks
            and trend_growth_ratio >= trend_growth_ratio_required
        )

        unusual_orders = recent_30.loc[recent_30["unconfirmed_outlier"]].copy()
        unusual_units = float(
            (unusual_orders["raw_order_units"] - unusual_orders["adjusted_order_units"]).sum()
        )
        largest_order_units = float(recent_30["raw_order_units"].max()) if not recent_30.empty else 0.0
        largest_order_share = (
            largest_order_units / raw_recent_30d_units * 100.0
            if raw_recent_30d_units > 0
            else 0.0
        )
        largest_unusual = (
            unusual_orders.sort_values("raw_order_units", ascending=False).iloc[0]
            if not unusual_orders.empty
            else None
        )
        if unconfirmed_recent_anomaly:
            demand_signal_code = "one_off_large_order"
            demand_signal_label = "Unusual large order; recurring demand protected"
        elif trend_candidate:
            demand_signal_code = "demand_acceleration_candidate"
            demand_signal_label = "Demand acceleration awaiting persistence check"
        elif intermittent:
            demand_signal_code = "intermittent_baseline"
            demand_signal_label = "Intermittent demand (TSB baseline)"
        else:
            demand_signal_code = "stable_baseline"
            demand_signal_label = "Robust multi-horizon baseline"

        if len(sku_orders) >= minimum_history_orders and not unconfirmed_recent_anomaly:
            confidence = "High" if not intermittent else "Medium"
        elif len(sku_orders) >= 4:
            confidence = "Medium" if not unconfirmed_recent_anomaly else "Low"
        else:
            confidence = "Low"

        order_rate_per_day = (
            robust_baseline_30d_units / 30.0 / typical_order_units
            if typical_order_units > 0
            else 0.0
        )
        rows.append(
            {
                "product_sku": str(sku),
                "raw_recent_30d_units": round(raw_recent_30d_units, 3),
                "adjusted_recent_30d_units": round(adjusted_recent_30d_units, 3),
                "adjusted_recent_90d_units": round(adjusted_recent_90d_units, 3),
                "adjusted_recent_180d_units": round(adjusted_recent_180d_units, 3),
                "robust_baseline_30d_units": round(max(robust_baseline_30d_units, 0.0), 3),
                "unusual_large_order_flag": unconfirmed_recent_anomaly,
                "unusual_large_order_count_30d": int(len(unusual_orders)),
                "unusual_large_order_units_30d": round(max(unusual_units, 0.0), 3),
                "unusual_large_order_adjustment_units_30d": round(max(unusual_units, 0.0), 3),
                "largest_order_units_30d": round(largest_order_units, 3),
                "largest_order_share_30d_pct": round(largest_order_share, 1),
                "largest_unusual_order_num": (
                    str(largest_unusual.get("order_num") or "")
                    if largest_unusual is not None
                    else None
                ),
                "largest_unusual_order_date": (
                    pd.Timestamp(largest_unusual.get("order_datetime")).strftime("%Y-%m-%d")
                    if largest_unusual is not None
                    else None
                ),
                "order_count_30d": order_count_30d,
                "customer_count_30d": int(
                    recent_30["customer"]
                    .fillna("")
                    .astype(str)
                    .str.strip()
                    .replace({"nan": "", "none": ""})
                    .loc[lambda values: values.ne("")]
                    .nunique()
                ),
                "active_weeks_30d": active_weeks_30d,
                "positive_order_count_history": int(len(sku_orders)),
                "history_observation_days": int(observation_days),
                "typical_order_units": round(typical_order_units, 3),
                "outlier_threshold_units": round(threshold, 3),
                "order_rate_per_day": round(max(order_rate_per_day, 0.0), 6),
                "weekly_zero_share_pct": round(weekly_zero_share * 100.0, 1),
                "demand_model": demand_model,
                "demand_confidence": confidence,
                "demand_signal_code": demand_signal_code,
                "demand_signal_label": demand_signal_label,
                "trend_candidate_flag": trend_candidate,
                "trend_growth_ratio": round(trend_growth_ratio, 3),
                "confirmed_repeated_bulk_flag": confirmed_repeated_bulk,
            }
        )

    return pd.DataFrame(rows, columns=ROBUST_DEMAND_COLUMNS)


def poisson_tail_probability(expected_events: Any, required_events: Any) -> float:
    """Return P(Poisson(expected_events) >= required_events) without SciPy."""

    try:
        lam = max(float(expected_events), 0.0)
        required = int(math.ceil(float(required_events)))
    except (TypeError, ValueError):
        return 0.0
    if required <= 0:
        return 1.0
    if lam <= 0:
        return 0.0
    if lam > 100:
        z_score = (required - 0.5 - lam) / math.sqrt(2.0 * lam)
        return min(max(0.5 * math.erfc(z_score), 0.0), 1.0)

    probability = math.exp(-lam)
    cumulative = probability
    for event_count in range(1, required):
        probability *= lam / float(event_count)
        cumulative += probability
    return min(max(1.0 - cumulative, 0.0), 1.0)


def update_m_of_n_signal_history(
    previous_rows: Iterable[Mapping[str, Any]],
    current_candidates: Mapping[str, Any],
    *,
    check_date: Any,
    window_runs: int = 3,
    required_runs: int = 2,
) -> pd.DataFrame:
    """Persist a stable M-of-N trend signal with same-day idempotency."""

    window = max(int(window_runs), 1)
    required = min(max(int(required_runs), 1), window)
    normalized_date = pd.to_datetime(check_date, errors="coerce")
    if pd.isna(normalized_date):
        raise ValueError("check_date must be a valid date")
    date_text = pd.Timestamp(normalized_date).strftime("%Y-%m-%d")

    previous_by_sku: Dict[str, Mapping[str, Any]] = {}
    for row in previous_rows or []:
        if not isinstance(row, Mapping):
            continue
        sku = str(row.get("sku") or "").strip()
        if sku:
            previous_by_sku[sku] = row

    rows: List[Dict[str, Any]] = []
    for raw_sku, raw_candidate in current_candidates.items():
        sku = str(raw_sku or "").strip()
        if not sku:
            continue
        candidate = bool(raw_candidate)
        previous = previous_by_sku.get(sku, {})
        checks_text = str(previous.get("trend_candidate_checks") or "")
        checks = [character == "1" for character in checks_text if character in {"0", "1"}]
        last_check_date = str(previous.get("last_check_date") or "").strip()
        if last_check_date == date_text and checks:
            checks[-1] = candidate
        else:
            checks.append(candidate)
        checks = checks[-window:]
        confirmation_count = sum(1 for value in checks if value)
        rows.append(
            {
                "sku": sku,
                "last_check_date": date_text,
                "trend_candidate_checks": "".join("1" if value else "0" for value in checks),
                "trend_confirmation_count": confirmation_count,
                "trend_confirmed_flag": confirmation_count >= required,
                "trend_persistence_window": window,
                "trend_persistence_required": required,
            }
        )
    return pd.DataFrame(rows)
