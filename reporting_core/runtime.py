#!/usr/bin/env python3
"""
Reusable runtime settings loader/applicator for per-project reporting behavior.
"""

from __future__ import annotations

import copy
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from .config import project_dir, resolve_biznisweb_api_url, resolve_reporting_defaults


@dataclass(frozen=True)
class ProjectRuntime:
    project_name: str
    api_url: str
    api_token: str
    packaging_cost_per_order: float
    shipping_subsidy_per_order: float
    fixed_monthly_cost: float
    currency_rates_to_eur: Dict[str, float]
    product_expenses: Dict[str, float]
    zero_margin_brands: List[str]
    zero_cost_brands: List[str]
    zero_cost_label_patterns: List[str]
    margin_15_brands: List[str]
    margin_15_label_patterns: List[str]
    exclude_zero_price_label_patterns: List[str]
    manual_fb_ads_total: Optional[float]
    manual_google_ads_total: Optional[float]
    weather: Dict[str, Any]
    reporting_defaults: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "project_name": self.project_name,
            "api_url": self.api_url,
            "api_token": self.api_token,
            "packaging_cost_per_order": self.packaging_cost_per_order,
            "shipping_subsidy_per_order": self.shipping_subsidy_per_order,
            "fixed_monthly_cost": self.fixed_monthly_cost,
            "currency_rates_to_eur": dict(self.currency_rates_to_eur),
            "product_expenses": dict(self.product_expenses),
            "zero_margin_brands": list(self.zero_margin_brands),
            "zero_cost_brands": list(self.zero_cost_brands),
            "zero_cost_label_patterns": list(self.zero_cost_label_patterns),
            "margin_15_brands": list(self.margin_15_brands),
            "margin_15_label_patterns": list(self.margin_15_label_patterns),
            "exclude_zero_price_label_patterns": list(self.exclude_zero_price_label_patterns),
            "manual_fb_ads_total": self.manual_fb_ads_total,
            "manual_google_ads_total": self.manual_google_ads_total,
            "weather": copy.deepcopy(self.weather),
            "reporting_defaults": dict(self.reporting_defaults),
        }


def load_project_runtime(
    project_name: str,
    *,
    settings: Dict[str, Any],
    legacy_product_expenses: Optional[Dict[str, float]] = None,
    default_currency_rates: Optional[Dict[str, float]] = None,
    default_packaging_cost_per_order: float,
    default_shipping_subsidy_per_order: float,
    default_fixed_monthly_cost: float,
    default_weather_timezone: str = "Europe/Bratislava",
) -> ProjectRuntime:
    project_path = project_dir(project_name)
    product_expenses = dict(legacy_product_expenses or {}) if project_name == "vevo" else {}
    product_expenses_file = settings.get("product_expenses_file", "product_expenses.json")
    product_expenses_path = project_path / product_expenses_file
    if product_expenses_path.exists():
        with open(product_expenses_path, "r", encoding="utf-8") as f:
            raw_map = json.load(f) or {}
            product_expenses = {str(k): float(v) for k, v in raw_map.items()}

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
        api_token=os.getenv("BIZNISWEB_API_TOKEN", ""),
        packaging_cost_per_order=float(settings.get("packaging_cost_per_order", default_packaging_cost_per_order)),
        shipping_subsidy_per_order=float(settings.get("shipping_subsidy_per_order", default_shipping_subsidy_per_order)),
        fixed_monthly_cost=float(settings.get("fixed_monthly_cost", default_fixed_monthly_cost)),
        currency_rates_to_eur={
            str(k).upper(): float(v)
            for k, v in dict(settings.get("currency_rates_to_eur", default_currency_rates or {})).items()
        },
        product_expenses={str(k): float(v) for k, v in product_expenses.items()},
        zero_margin_brands=[str(v).strip() for v in settings.get("zero_margin_brands", []) if str(v).strip()],
        zero_cost_brands=[str(v).strip() for v in settings.get("zero_cost_brands", []) if str(v).strip()],
        zero_cost_label_patterns=[str(v).strip() for v in settings.get("zero_cost_label_patterns", []) if str(v).strip()],
        margin_15_brands=[str(v).strip() for v in settings.get("margin_15_brands", []) if str(v).strip()],
        margin_15_label_patterns=[str(v).strip() for v in settings.get("margin_15_label_patterns", []) if str(v).strip()],
        exclude_zero_price_label_patterns=[
            str(v).strip() for v in settings.get("exclude_zero_price_label_patterns", []) if str(v).strip()
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
        weather=weather_settings,
        reporting_defaults=resolve_reporting_defaults(project_name, settings),
    )


def apply_project_runtime(runtime: ProjectRuntime, target_globals: Dict[str, Any]) -> None:
    target_globals["PACKAGING_COST_PER_ORDER"] = float(runtime.packaging_cost_per_order)
    target_globals["SHIPPING_SUBSIDY_PER_ORDER"] = float(runtime.shipping_subsidy_per_order)
    target_globals["FIXED_MONTHLY_COST"] = float(runtime.fixed_monthly_cost)
    target_globals["CURRENCY_RATES_TO_EUR"] = dict(runtime.currency_rates_to_eur)
    target_globals["PRODUCT_EXPENSES"] = dict(runtime.product_expenses)
    target_globals["ZERO_MARGIN_BRANDS"] = [str(v).strip().lower() for v in runtime.zero_margin_brands if str(v).strip()]
    target_globals["ZERO_COST_BRANDS"] = [str(v).strip().lower() for v in runtime.zero_cost_brands if str(v).strip()]
    target_globals["ZERO_COST_LABEL_PATTERNS"] = [str(v).strip() for v in runtime.zero_cost_label_patterns if str(v).strip()]
    target_globals["MARGIN_15_BRANDS"] = [str(v).strip().lower() for v in runtime.margin_15_brands if str(v).strip()]
    target_globals["MARGIN_15_LABEL_PATTERNS"] = [str(v).strip() for v in runtime.margin_15_label_patterns if str(v).strip()]
    target_globals["EXCLUDE_ZERO_PRICE_LABEL_PATTERNS"] = [
        str(v).strip() for v in runtime.exclude_zero_price_label_patterns if str(v).strip()
    ]
    target_globals["MANUAL_FB_ADS_TOTAL"] = runtime.manual_fb_ads_total
    target_globals["MANUAL_GOOGLE_ADS_TOTAL"] = runtime.manual_google_ads_total
    target_globals["WEATHER_SETTINGS"] = copy.deepcopy(runtime.weather)
    target_globals["ENABLE_EMAIL_STRATEGY_REPORT"] = bool(runtime.reporting_defaults.get("enable_email_strategy_report", False))
