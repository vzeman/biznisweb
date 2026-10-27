"""Pure country executive decks built from canonical order and measured daily facts.

This module never fetches data, reallocates advertising, or changes the company
calculation. Amounts outside the displayed markets remain a visible residual.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
import math
import re
from typing import Any, Dict, Optional

import pandas as pd

from .cfo_kpis import build_cfo_kpi_payload


COUNTRY_LABELS = {
    "sk": ("Slovakia", "Slovensko"),
    "cz": ("Czechia", "Česko"),
    "hu": ("Hungary", "Maďarsko"),
}
_PROVIDER_BASES = {
    "facebook_ads": "meta_country_breakdown_daily",
    "google_ads": "google_user_location_daily",
}
_AD_DEPENDENT_METRICS = frozenset({
    "profit", "cac", "roas", "post_ad_margin", "company_margin_with_fixed",
})


def _money(value: Any) -> Decimal:
    if isinstance(value, bool):
        raise ValueError("Boolean advertising amount")
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("Invalid advertising amount") from exc
    if not amount.is_finite() or amount < 0:
        raise ValueError("Invalid advertising amount")
    return amount


def _daily_ad_source(source: Any, basis: str, dates: list[str]) -> Dict[str, Any]:
    """Validate the additive source without substituting the old period totals."""
    empty = {"status": "unavailable", "basis": basis, "daily": {}, "totals": {}}
    if not isinstance(source, dict) or source.get("status") != "ok":
        return {**empty, "reason": "source_unavailable"}
    try:
        if source.get("basis") != basis or source.get("currency") != "EUR":
            raise ValueError("basis_or_currency")
        if source.get("date_from") != dates[0] or source.get("date_to") != dates[-1]:
            raise ValueError("date_range")
        raw_daily = source["daily_spend_by_country"]
        if not isinstance(raw_daily, dict) or set(raw_daily) != set(dates):
            raise ValueError("daily_shape")
        daily: Dict[str, Dict[str, float]] = {}
        totals: Dict[str, Decimal] = {}
        for day, values in raw_daily.items():
            if day not in dates or not isinstance(values, dict):
                raise ValueError("daily_scope")
            daily[day] = {}
            for country, value in values.items():
                if not isinstance(country, str) or not (
                    re.fullmatch("[a-z]{2}", country) or country in {"unknown", "unallocated", "other"}
                ):
                    raise ValueError("country_key")
                amount = _money(value)
                daily[day][country] = float(amount)
                totals[country] = totals.get(country, Decimal(0)) + amount
        reported = source["spend_by_country"]
        if not isinstance(reported, dict):
            raise ValueError("total_shape")
        reported = {key: _money(value) for key, value in reported.items()}
        keys = set(reported) | set(totals)
        # These fields describe the same daily response, unlike the independent
        # existing period-level country query. Compare native totals, not an
        # expanded financial tolerance or a rounded/rescaled allocation.
        if any(float(reported.get(key, Decimal(0))) != float(totals.get(key, Decimal(0))) for key in keys):
            raise ValueError("daily_country_total")
        total = sum(totals.values(), Decimal(0))
        if float(_money(source["total_spend"])) != float(total):
            raise ValueError("daily_total")
    except (KeyError, TypeError, ValueError):
        return {**empty, "reason": "invalid_daily_source"}
    return {
        "status": "ok", "basis": basis, "daily": daily,
        "totals": {key: float(value) for key, value in totals.items()},
        "total_spend": float(total),
        "country_coverage_status": source.get("country_coverage_status"),
        "unallocated_spend": source.get("unallocated_spend"),
        "reported_unknown_spend": source.get("reported_unknown_spend"),
    }


def _mask_ad_dependent_values(payload: Dict[str, Any]) -> None:
    for window in payload.get("windows", {}).values():
        for key in _AD_DEPENDENT_METRICS:
            window["metrics"][key] = None
        window.get("secondary_metrics", {})["company_margin_with_fixed"] = None
        for key, values in window.get("trend", {}).get("metrics", {}).items():
            if key in _AD_DEPENDENT_METRICS:
                window["trend"]["metrics"][key] = [None] * len(values)
    for comparisons in payload.get("comparisons", {}).values():
        for key, values in comparisons.items():
            if key in _AD_DEPENDENT_METRICS:
                comparisons[key] = dict.fromkeys(values)


def _period_delta(source: Dict[str, Any], legacy: Any) -> Optional[Dict[str, Any]]:
    if source["status"] != "ok" or not isinstance(legacy, dict) or legacy.get("status") != "ok":
        return None
    period = legacy.get("spend_by_country")
    if not isinstance(period, dict):
        return None
    try:
        keys = set(source["totals"]) | set(period)
        deltas = {
            key: float(Decimal(str(source["totals"].get(key, 0))) - _money(period.get(key, 0)))
            for key in sorted(keys)
        }
        total = float(Decimal(str(source["total_spend"])) - _money(legacy["total_spend"]))
    except (KeyError, ValueError):
        return None
    return {"daily_minus_period_by_country_eur": deltas, "daily_minus_period_total_eur": total}


def build_country_cfo_payload(
    date_agg: pd.DataFrame,
    order_frame: pd.DataFrame,
    daily_ads: Dict[str, Any],
    *,
    fulfillment_rows: Optional[pd.DataFrame] = None,
    period_country_ads: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Use the unchanged CFO window logic for SK/CZ/HU on one shared calendar.

    ``order_frame`` has one canonical financial row per order, including already
    credit-adjusted net revenue and the existing allocated shared overhead.
    ``fulfillment_rows`` comes from the same selected excluded-order events used
    in the company daily calculation; it contains no additional financial orders.
    """
    if date_agg is None or date_agg.empty:
        return {"countries": {}, "country_order": list(COUNTRY_LABELS), "country_metadata": {}}
    company = date_agg.copy(deep=True)
    company["date"] = pd.to_datetime(company["date"]).dt.date
    company = company.sort_values("date").reset_index(drop=True)
    if company["date"].duplicated().any():
        raise ValueError("Country CFO calendar contains duplicate dates")
    complete_dates = pd.date_range(company["date"].min(), company["date"].max(), freq="D").date
    company = company.set_index("date").reindex(complete_dates).rename_axis("date").reset_index()
    calendar = company["date"].tolist()
    date_keys = [value.isoformat() for value in calendar]
    sources = {
        provider: _daily_ad_source((daily_ads or {}).get(provider), basis, date_keys)
        for provider, basis in _PROVIDER_BASES.items()
    }
    unavailable = [key for key, value in sources.items() if value["status"] != "ok"]
    provider_coverage = {
        key: {
            "status": value.get("country_coverage_status") or (
                "partial" if any(value["totals"].get(code, 0) > 0 for code in ("unknown", "unallocated")) else "complete"
            ) if value["status"] == "ok" else "unavailable",
            "unknown_spend_eur": value["totals"].get("unknown", 0.0) if value["status"] == "ok" else None,
            "unallocated_spend_eur": value["totals"].get("unallocated", 0.0) if value["status"] == "ok" else None,
        }
        for key, value in sources.items()
    }

    orders = order_frame.copy(deep=True) if isinstance(order_frame, pd.DataFrame) else pd.DataFrame()
    if orders.empty:
        orders = pd.DataFrame(columns=["order_num", "purchase_date", "country", "customer_email"])
    orders = orders.drop_duplicates().copy()
    if orders["order_num"].duplicated().any():
        raise ValueError("Country CFO requires one canonical row per order")
    orders["date"] = pd.to_datetime(orders["purchase_date"], errors="raise").dt.date
    if any(value not in calendar for value in orders["date"]):
        raise ValueError("Country CFO order falls outside its calendar")
    orders["country"] = orders["country"].fillna("unknown").astype(str).str.lower().str.strip()
    for name in ("order_revenue_net", "product_cost", "packaging_cost", "shipping_net_cost",
                 "allocated_fixed_overhead", "order_credit_net_adjustment", "total_items_in_order"):
        if name not in orders:
            orders[name] = 0.0
        orders[name] = pd.to_numeric(orders[name], errors="raise").fillna(0.0)
        if not all(math.isfinite(float(value)) for value in orders[name]):
            raise ValueError("Country CFO contains a nonfinite financial value")

    # Preserve global customer acquisition history before filtering a market.
    emails = orders["customer_email"].fillna("").astype(str).str.strip().str.lower()
    first_dates = orders.assign(_email=emails).groupby("_email")["date"].min()
    if "customer_first_purchase_date" not in orders:
        orders["customer_first_purchase_date"] = None
    history = pd.to_datetime(orders["customer_first_purchase_date"], errors="coerce")
    orders["customer_first_purchase_date"] = [
        (value.date() if not pd.isna(value) else first_dates[email]).isoformat()
        for email, value in zip(emails, history)
    ]

    fulfillment = fulfillment_rows.copy(deep=True) if isinstance(fulfillment_rows, pd.DataFrame) else pd.DataFrame()
    if not fulfillment.empty:
        fulfillment["date"] = pd.to_datetime(fulfillment["date"]).dt.date
        if any(value not in calendar for value in fulfillment["date"]):
            raise ValueError("Country CFO fulfillment falls outside its calendar")
        fulfillment["country"] = fulfillment["country"].fillna("unknown").astype(str).str.lower().str.strip()

    frames: Dict[str, pd.DataFrame] = {}
    countries: Dict[str, Any] = {}
    for country, (label_en, label_sk) in COUNTRY_LABELS.items():
        scoped = orders[orders["country"] == country].copy()
        frame = pd.DataFrame({"date": calendar})
        grouped = scoped.groupby("date")
        mappings = {
            "total_revenue": "order_revenue_net", "product_expense": "product_cost",
            "packaging_cost": "packaging_cost", "shipping_net_cost": "shipping_net_cost",
            "fixed_daily_cost": "allocated_fixed_overhead",
            "revenue_credit_adjustment": "order_credit_net_adjustment",
            "total_quantity": "total_items_in_order",
        }
        for target, original in mappings.items():
            frame[target] = frame["date"].map(grouped[original].sum()).fillna(0.0)
        frame["unique_orders"] = frame["date"].map(grouped["order_num"].nunique()).fillna(0).astype(int)
        if not fulfillment.empty:
            credits = fulfillment[fulfillment["country"] == country].groupby("date")
            for target, original in (("packaging_cost", "creditnote_packaging_cost"),
                                     ("shipping_net_cost", "creditnote_shipping_net_cost")):
                frame[target] += frame["date"].map(credits[original].sum()).fillna(0.0)
        for provider, column in (("facebook_ads", "fb_ads_spend"), ("google_ads", "google_ads_spend")):
            frame[column] = [sources[provider]["daily"].get(day, {}).get(country, 0.0) for day in date_keys]
        frame["pre_ad_contribution_profit"] = (
            frame["total_revenue"] - frame["product_expense"] - frame["packaging_cost"] - frame["shipping_net_cost"]
        )
        frame["pre_ad_contribution_margin_pct"] = [
            (profit / revenue * 100.0) if revenue > 0 else 0.0
            for profit, revenue in zip(frame["pre_ad_contribution_profit"], frame["total_revenue"])
        ]
        frame["contribution_profit"] = frame["pre_ad_contribution_profit"] - frame["fb_ads_spend"] - frame["google_ads_spend"]
        payload = build_cfo_kpi_payload(frame, scoped, fixed_daily_cost_eur=0.0)
        for metric in payload["metric_defs"]:
            if metric["key"] == "company_margin_with_fixed":
                metric["label"] = "Company Margin (allocated shared fixed costs)"
        payload.update(country=country, label_en=label_en, label_sk=label_sk,
                       status="unavailable" if unavailable else "ok")
        payload["metadata"] = {
            "market_basis": "delivery_country_with_invoice_fallback",
            "fixed_cost_basis": "existing_daily_order_share",
            "customer_basis": "global_first_purchase_date",
            "date_from": date_keys[0], "date_to": date_keys[-1],
            "calendar_complete": True, "calendar_days": len(calendar),
            "unavailable_ad_sources": unavailable,
            "advertising_reallocated": False,
            "creditnote_fulfillment_included": fulfillment_rows is not None,
            "creditnote_fulfillment_source": "same_selected_company_events" if fulfillment_rows is not None else "company_residual_only",
            "provider_country_coverage": provider_coverage,
        }
        if unavailable:
            _mask_ad_dependent_values(payload)
        countries[country] = payload
        frames[country] = frame

    residuals = {}
    for column in ("total_revenue", "product_expense", "packaging_cost", "shipping_net_cost",
                   "unique_orders", "fixed_daily_cost", "fb_ads_spend", "google_ads_spend"):
        global_sum = float(pd.to_numeric(company.get(column, pd.Series(dtype=float)), errors="raise").fillna(0).sum())
        scoped_sum = sum(float(frame[column].sum()) for frame in frames.values())
        provider = {"fb_ads_spend": "facebook_ads", "google_ads_spend": "google_ads"}.get(column)
        residuals[column] = None if provider in unavailable else global_sum - scoped_sum
    return {
        "countries": countries,
        "country_order": list(COUNTRY_LABELS),
        "country_metadata": {
            "basis": "canonical_order_financials_and_measured_daily_country_ads",
            "date_from": date_keys[0], "date_to": date_keys[-1],
            "calendar_complete": True, "calendar_days": len(calendar),
            "unavailable_ad_sources": unavailable,
            "provider_status": {key: value["status"] for key, value in sources.items()},
            "provider_country_coverage": provider_coverage,
            "provider_daily_totals": {key: value.get("totals", {}) for key, value in sources.items()},
            "daily_vs_period": {
                key: _period_delta(value, (period_country_ads or {}).get(key))
                for key, value in sources.items()
            },
            "company_minus_displayed_countries": residuals,
            "residual_basis": "other_or_unknown_markets_unallocated_ads_and_orderless_day_fixed_costs",
            "residual_reallocated": False,
        },
    }
