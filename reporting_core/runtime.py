#!/usr/bin/env python3
"""
Reusable runtime settings loader/applicator for per-project reporting behavior.
"""

from __future__ import annotations

import copy
import json
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from .config import project_dir, resolve_biznisweb_api_url, resolve_project_env_value, resolve_reporting_defaults


@dataclass(frozen=True)
class ProjectRuntime:
    project_name: str
    api_url: str
    api_token: str
    item_price_values_are_net: bool
    expense_match_mode: str
    product_name_aliases: Dict[str, str]
    packaging_cost_per_order: float
    shipping_subsidy_per_order: float
    fixed_monthly_cost: float
    fixed_daily_cost: float
    currency_rates_to_eur: Dict[str, float]
    product_expenses: Dict[str, float]
    missing_cost_margin_pct: float
    zero_margin_brands: List[str]
    zero_cost_brands: List[str]
    zero_cost_label_patterns: List[str]
    authoritative_margin_override_skus: Dict[str, float]
    margin_override_skus: Dict[str, float]
    margin_override_brands: Dict[str, float]
    margin_override_label_patterns: Dict[str, float]
    margin_15_brands: List[str]
    margin_15_label_patterns: List[str]
    exclude_zero_price_label_patterns: List[str]
    excluded_order_statuses: List[str]
    manual_fb_ads_total: Optional[float]
    manual_google_ads_total: Optional[float]
    prefer_manual_ads_totals: bool
    weather: Dict[str, Any]
    reporting_defaults: Dict[str, Any]

    @property
    def shipping_net_per_order(self) -> float:
        """Canonical shipping semantics.

        Positive value = net shipping cost to the business.
        Negative value = shipping profit / shipping margin retained by the business.
        """
        return float(self.shipping_subsidy_per_order)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "project_name": self.project_name,
            "api_url": self.api_url,
            "api_token": self.api_token,
            "item_price_values_are_net": self.item_price_values_are_net,
            "expense_match_mode": self.expense_match_mode,
            "product_name_aliases": dict(self.product_name_aliases),
            "packaging_cost_per_order": self.packaging_cost_per_order,
            "shipping_subsidy_per_order": self.shipping_subsidy_per_order,
            "shipping_net_per_order": self.shipping_net_per_order,
            "fixed_monthly_cost": self.fixed_monthly_cost,
            "fixed_daily_cost": self.fixed_daily_cost,
            "currency_rates_to_eur": dict(self.currency_rates_to_eur),
            "product_expenses": dict(self.product_expenses),
            "missing_cost_margin_pct": self.missing_cost_margin_pct,
            "zero_margin_brands": list(self.zero_margin_brands),
            "zero_cost_brands": list(self.zero_cost_brands),
            "zero_cost_label_patterns": list(self.zero_cost_label_patterns),
            "authoritative_margin_override_skus": dict(self.authoritative_margin_override_skus),
            "margin_override_skus": dict(self.margin_override_skus),
            "margin_override_brands": dict(self.margin_override_brands),
            "margin_override_label_patterns": dict(self.margin_override_label_patterns),
            "margin_15_brands": list(self.margin_15_brands),
            "margin_15_label_patterns": list(self.margin_15_label_patterns),
            "exclude_zero_price_label_patterns": list(self.exclude_zero_price_label_patterns),
            "excluded_order_statuses": list(self.excluded_order_statuses),
            "manual_fb_ads_total": self.manual_fb_ads_total,
            "manual_google_ads_total": self.manual_google_ads_total,
            "prefer_manual_ads_totals": self.prefer_manual_ads_totals,
            "weather": copy.deepcopy(self.weather),
            "reporting_defaults": dict(self.reporting_defaults),
        }


def _coerce_margin_override_map(raw: Any) -> Dict[str, float]:
    if not raw:
        return {}

    items = raw.items() if isinstance(raw, dict) else []
    result: Dict[str, float] = {}
    for key, value in items:
        key_text = str(key or "").strip()
        if not key_text:
            continue
        try:
            margin_pct = float(str(value).strip().rstrip("%"))
        except (TypeError, ValueError):
            continue
        if 0.0 <= margin_pct < 100.0:
            result[key_text] = margin_pct
    return result


def _coerce_authoritative_margin_override_map(raw: Any) -> Dict[str, float]:
    """Strictly validate explicit policies that intentionally replace mapped costs."""
    if raw in (None, {}):
        return {}
    if not isinstance(raw, dict):
        raise ValueError("authoritative_margin_override_skus must be an object")

    result: Dict[str, float] = {}
    for key, value in raw.items():
        key_text = str(key or "").strip()
        if not key_text or key_text.lower() in {"nan", "none", "null"}:
            raise ValueError("authoritative_margin_override_skus contains an empty SKU")
        if key_text.endswith(".0") and key_text[:-2].isdigit():
            key_text = key_text[:-2]
        normalized_key = key_text.upper()
        if normalized_key in result:
            raise ValueError(
                f"authoritative_margin_override_skus contains duplicate normalized SKU {normalized_key!r}"
            )
        try:
            margin_pct = float(str(value).strip().rstrip("%"))
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"authoritative_margin_override_skus[{key_text!r}] must be a number from 0 up to, but not including, 100"
            ) from exc
        if not math.isfinite(margin_pct) or not 0.0 <= margin_pct < 100.0:
            raise ValueError(
                f"authoritative_margin_override_skus[{key_text!r}] must be from 0 up to, but not including, 100"
            )
        result[normalized_key] = margin_pct
    return result


def _coerce_margin_pct(raw: Any, *, setting_name: str, default: float = 0.0) -> float:
    if raw in (None, ""):
        return float(default)
    try:
        margin_pct = float(str(raw).strip().rstrip("%"))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{setting_name} must be a number from 0 up to, but not including, 100") from exc
    if not 0.0 <= margin_pct < 100.0:
        raise ValueError(f"{setting_name} must be from 0 up to, but not including, 100")
    return margin_pct


def load_project_runtime(
    project_name: str,
    *,
    settings: Dict[str, Any],
    legacy_product_expenses: Optional[Dict[str, float]] = None,
    default_currency_rates: Optional[Dict[str, float]] = None,
    default_packaging_cost_per_order: float,
    default_shipping_subsidy_per_order: float,
    default_fixed_monthly_cost: float,
    default_fixed_daily_cost: float,
    default_weather_timezone: str = "Europe/Bratislava",
) -> ProjectRuntime:
    project_path = project_dir(project_name)
    product_expenses = dict(legacy_product_expenses or {}) if project_name == "vevo" else {}
    product_name_aliases: Dict[str, str] = {}
    product_expenses_file = settings.get("product_expenses_file", "product_expenses.json")
    product_expenses_path = project_path / product_expenses_file
    if product_expenses_path.exists():
        with open(product_expenses_path, "r", encoding="utf-8") as f:
            raw_map = json.load(f) or {}
            product_expenses = {str(k): float(v) for k, v in raw_map.items()}
    product_name_aliases_file = settings.get("product_name_aliases_file")
    if product_name_aliases_file:
        product_name_aliases_path = project_path / str(product_name_aliases_file)
        if product_name_aliases_path.exists():
            with open(product_name_aliases_path, "r", encoding="utf-8") as f:
                raw_aliases = json.load(f) or {}
                product_name_aliases = {
                    str(k): str(v)
                    for k, v in raw_aliases.items()
                    if str(k).strip() and str(v).strip()
                }

    raw_weather = settings.get("weather", {}) or {}
    normalized_locations = []
    for location in raw_weather.get("locations", []) or []:
        try:
            normalized_locations.append({
                "name": str(location.get("name", "Location")).strip() or "Location",
                "latitude": float(location["latitude"]),
                "longitude": float(location["longitude"]),
                "weight": float(location.get("weight", 1.0)),
            })
        except (KeyError, TypeError, ValueError):
            continue
    weather_settings = {
        "enabled": bool(raw_weather.get("enabled", False) and normalized_locations),
        "timezone": str(raw_weather.get("timezone", default_weather_timezone)).strip() or default_weather_timezone,
        "locations": normalized_locations,
    }

    return ProjectRuntime(
        project_name=project_name,
        api_url=resolve_biznisweb_api_url(project_name, settings),
        api_token=resolve_project_env_value(project_name, "BIZNISWEB_API_TOKEN"),
        item_price_values_are_net=bool(settings.get("item_price_values_are_net", False)),
        expense_match_mode=str(settings.get("expense_match_mode", "identifier_first")).strip().lower() or "identifier_first",
        product_name_aliases=dict(product_name_aliases),
        packaging_cost_per_order=float(settings.get("packaging_cost_per_order", default_packaging_cost_per_order)),
        shipping_subsidy_per_order=float(
            settings.get(
                "shipping_net_per_order",
                settings.get("shipping_subsidy_per_order", default_shipping_subsidy_per_order),
            )
        ),
        fixed_monthly_cost=float(settings.get("fixed_monthly_cost", default_fixed_monthly_cost)),
        fixed_daily_cost=float(settings.get("fixed_daily_cost", default_fixed_daily_cost)),
        currency_rates_to_eur={
            str(k).upper(): float(v)
            for k, v in dict(settings.get("currency_rates_to_eur", default_currency_rates or {})).items()
        },
        product_expenses={str(k): float(v) for k, v in product_expenses.items()},
        missing_cost_margin_pct=_coerce_margin_pct(
            settings.get("missing_cost_margin_pct"),
            setting_name="missing_cost_margin_pct",
        ),
        zero_margin_brands=[str(v).strip() for v in settings.get("zero_margin_brands", []) if str(v).strip()],
        zero_cost_brands=[str(v).strip() for v in settings.get("zero_cost_brands", []) if str(v).strip()],
        zero_cost_label_patterns=[str(v).strip() for v in settings.get("zero_cost_label_patterns", []) if str(v).strip()],
        authoritative_margin_override_skus=_coerce_authoritative_margin_override_map(
            settings.get("authoritative_margin_override_skus", {})
        ),
        margin_override_skus=_coerce_margin_override_map(settings.get("margin_override_skus", {})),
        margin_override_brands=_coerce_margin_override_map(settings.get("margin_override_brands", {})),
        margin_override_label_patterns=_coerce_margin_override_map(settings.get("margin_override_label_patterns", {})),
        margin_15_brands=[str(v).strip() for v in settings.get("margin_15_brands", []) if str(v).strip()],
        margin_15_label_patterns=[str(v).strip() for v in settings.get("margin_15_label_patterns", []) if str(v).strip()],
        exclude_zero_price_label_patterns=[
            str(v).strip() for v in settings.get("exclude_zero_price_label_patterns", []) if str(v).strip()
        ],
        excluded_order_statuses=[
            str(v).strip() for v in settings.get("excluded_order_statuses", []) if str(v).strip()
        ],
        manual_fb_ads_total=(
            float(settings.get("manual_fb_ads_total"))
            if settings.get("manual_fb_ads_total") is not None
            else None
        ),
        manual_google_ads_total=(
            float(settings.get("manual_google_ads_total"))
            if settings.get("manual_google_ads_total") is not None
            else None
        ),
        prefer_manual_ads_totals=bool(settings.get("prefer_manual_ads_totals", False)),
        weather=weather_settings,
        reporting_defaults=resolve_reporting_defaults(project_name, settings),
    )


def apply_project_runtime(runtime: ProjectRuntime, target_globals: Dict[str, Any]) -> None:
    target_globals["ITEM_PRICE_VALUES_ARE_NET"] = bool(runtime.item_price_values_are_net)
    target_globals["EXPENSE_MATCH_MODE"] = str(runtime.expense_match_mode).strip().lower() or "identifier_first"
    target_globals["PRODUCT_NAME_ALIASES"] = dict(runtime.product_name_aliases)
    target_globals["PACKAGING_COST_PER_ORDER"] = float(runtime.packaging_cost_per_order)
    target_globals["SHIPPING_SUBSIDY_PER_ORDER"] = float(runtime.shipping_subsidy_per_order)
    target_globals["SHIPPING_NET_PER_ORDER"] = float(runtime.shipping_net_per_order)
    target_globals["FIXED_MONTHLY_COST"] = float(runtime.fixed_monthly_cost)
    target_globals["FIXED_DAILY_COST"] = float(runtime.fixed_daily_cost)
    target_globals["CURRENCY_RATES_TO_EUR"] = dict(runtime.currency_rates_to_eur)
    target_globals["PRODUCT_EXPENSES"] = dict(runtime.product_expenses)
    target_globals["MISSING_COST_MARGIN_PCT"] = float(runtime.missing_cost_margin_pct)
    target_globals["ZERO_MARGIN_BRANDS"] = [str(v).strip().lower() for v in runtime.zero_margin_brands if str(v).strip()]
    target_globals["ZERO_COST_BRANDS"] = [str(v).strip().lower() for v in runtime.zero_cost_brands if str(v).strip()]
    target_globals["ZERO_COST_LABEL_PATTERNS"] = [str(v).strip() for v in runtime.zero_cost_label_patterns if str(v).strip()]
    target_globals["AUTHORITATIVE_MARGIN_OVERRIDE_SKUS"] = dict(runtime.authoritative_margin_override_skus)
    target_globals["MARGIN_OVERRIDE_SKUS"] = dict(runtime.margin_override_skus)
    target_globals["MARGIN_OVERRIDE_BRANDS"] = dict(runtime.margin_override_brands)
    target_globals["MARGIN_OVERRIDE_LABEL_PATTERNS"] = dict(runtime.margin_override_label_patterns)
    target_globals["MARGIN_15_BRANDS"] = [str(v).strip().lower() for v in runtime.margin_15_brands if str(v).strip()]
    target_globals["MARGIN_15_LABEL_PATTERNS"] = [str(v).strip() for v in runtime.margin_15_label_patterns if str(v).strip()]
    target_globals["EXCLUDE_ZERO_PRICE_LABEL_PATTERNS"] = [
        str(v).strip() for v in runtime.exclude_zero_price_label_patterns if str(v).strip()
    ]
    target_globals["EXCLUDED_ORDER_STATUSES"] = [
        str(v).strip() for v in runtime.excluded_order_statuses if str(v).strip()
    ]
    target_globals["MANUAL_FB_ADS_TOTAL"] = runtime.manual_fb_ads_total
    target_globals["MANUAL_GOOGLE_ADS_TOTAL"] = runtime.manual_google_ads_total
    target_globals["PREFER_MANUAL_ADS_TOTALS"] = bool(runtime.prefer_manual_ads_totals)
    target_globals["WEATHER_SETTINGS"] = copy.deepcopy(runtime.weather)
    target_globals["ENABLE_EMAIL_STRATEGY_REPORT"] = bool(runtime.reporting_defaults.get("enable_email_strategy_report", False))
