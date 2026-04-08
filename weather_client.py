#!/usr/bin/env python3
"""
Historical weather client with local cache for reporting analytics.
"""

from __future__ import annotations

import json
import os
from calendar import monthrange
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pandas as pd

from http_client import build_retry_session, resolve_timeout


class WeatherClient:
    """Fetch historical daily weather data and cache it per month/location."""

    BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
    DAILY_FIELDS = [
        "weather_code",
        "temperature_2m_mean",
        "temperature_2m_max",
        "temperature_2m_min",
        "precipitation_sum",
        "precipitation_hours",
        "wind_speed_10m_max",
    ]

    def __init__(self, cache_dir: Path, timezone: str = "Europe/Bratislava") -> None:
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.timezone = timezone
        self.request_timeout = resolve_timeout(os.getenv("WEATHER_API_TIMEOUT_SEC"))
        self.session = build_retry_session(timeout=self.request_timeout)

    def get_daily_weather(
        self,
        date_from: datetime,
        date_to: datetime,
        locations: List[Dict[str, Any]],
    ) -> pd.DataFrame:
        """Return weighted daily weather data for the selected period."""
        valid_locations = []
        for raw_location in locations or []:
            try:
                valid_locations.append(
                    {
                        "name": str(raw_location.get("name", "Location")).strip() or "Location",
                        "latitude": float(raw_location["latitude"]),
                        "longitude": float(raw_location["longitude"]),
                        "weight": float(raw_location.get("weight", 1.0)),
                    }
                )
            except (KeyError, TypeError, ValueError):
                continue

        if not valid_locations:
            return pd.DataFrame()

        archive_available_to = self._archive_available_to()
        effective_date_from = date_from.date()
        effective_date_to = min(date_to.date(), archive_available_to)
        if effective_date_from > effective_date_to:
            return pd.DataFrame()

        location_frames = []
        for location in valid_locations:
            location_df = self._load_location_weather(
                date_from=effective_date_from,
                date_to=effective_date_to,
                location=location,
                archive_available_to=archive_available_to,
            )
            if location_df.empty:
                continue

            location_df["location_name"] = location["name"]
            location_df["weight"] = location["weight"]
            location_frames.append(location_df)

        if not location_frames:
            return pd.DataFrame()

        combined = pd.concat(location_frames, ignore_index=True)
        numeric_columns = [
            "temperature_2m_mean",
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "precipitation_hours",
            "wind_speed_10m_max",
        ]

        weighted_rows: List[Dict[str, Any]] = []
        for day, group in combined.groupby("date", sort=True):
            total_weight = group["weight"].sum() or 1.0
            row: Dict[str, Any] = {"date": day}
            for column in numeric_columns:
                weighted_value = (group[column] * group["weight"]).sum() / total_weight
                row[column] = round(float(weighted_value), 3)

            primary_idx = group["weight"].astype(float).idxmax()
            row["weather_code"] = int(group.loc[primary_idx, "weather_code"])
            row["location_count"] = int(len(group))
            weighted_rows.append(row)

        return pd.DataFrame(weighted_rows).sort_values("date").reset_index(drop=True)

    def _load_location_weather(
        self,
        date_from: date,
        date_to: date,
        location: Dict[str, Any],
        archive_available_to: date,
    ) -> pd.DataFrame:
        monthly_rows: List[pd.DataFrame] = []
        for year, month in self._month_windows(date_from, date_to):
            payload = self._load_cached_or_fetch(location, year, month, archive_available_to)
            daily = (payload or {}).get("daily") or {}
            dates = daily.get("time") or []
            if not dates:
                continue

            month_df = pd.DataFrame(
                {
                    "date": pd.to_datetime(dates).date,
                    "weather_code": daily.get("weather_code", []),
                    "temperature_2m_mean": daily.get("temperature_2m_mean", []),
                    "temperature_2m_max": daily.get("temperature_2m_max", []),
                    "temperature_2m_min": daily.get("temperature_2m_min", []),
                    "precipitation_sum": daily.get("precipitation_sum", []),
                    "precipitation_hours": daily.get("precipitation_hours", []),
                    "wind_speed_10m_max": daily.get("wind_speed_10m_max", []),
                }
            )
            monthly_rows.append(month_df)

        if not monthly_rows:
            return pd.DataFrame()

        location_df = pd.concat(monthly_rows, ignore_index=True)
        location_df = location_df[
            (location_df["date"] >= date_from) & (location_df["date"] <= date_to)
        ].copy()

        numeric_columns = [
            "weather_code",
            "temperature_2m_mean",
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "precipitation_hours",
            "wind_speed_10m_max",
        ]
        for column in numeric_columns:
            location_df[column] = pd.to_numeric(location_df[column], errors="coerce")

        location_df["weather_code"] = location_df["weather_code"].fillna(0).astype(int)
        return location_df

    def _load_cached_or_fetch(
        self,
        location: Dict[str, Any],
        year: int,
        month: int,
        archive_available_to: date,
    ) -> Dict[str, Any]:
        cache_path = self._cache_path(location, year, month, archive_available_to)
        if cache_path.exists():
            with open(cache_path, "r", encoding="utf-8") as file:
                return json.load(file)

        payload = self._fetch_month(location, year, month, archive_available_to)
        if not payload:
            return {}
        with open(cache_path, "w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2)
        return payload

    def _fetch_month(
        self,
        location: Dict[str, Any],
        year: int,
        month: int,
        archive_available_to: date,
    ) -> Dict[str, Any]:
        month_start = date(year, month, 1)
        month_end = date(year, month, monthrange(year, month)[1])
        effective_end = min(month_end, archive_available_to)
        if effective_end < month_start:
            return {}

        start_date = month_start.strftime("%Y-%m-%d")
        end_date = effective_end.strftime("%Y-%m-%d")

        response = self.session.get(
            self.BASE_URL,
            params={
                "latitude": location["latitude"],
                "longitude": location["longitude"],
                "start_date": start_date,
                "end_date": end_date,
                "daily": ",".join(self.DAILY_FIELDS),
                "timezone": self.timezone,
            },
        )
        response.raise_for_status()
        return response.json()

    def _cache_path(
        self,
        location: Dict[str, Any],
        year: int,
        month: int,
        archive_available_to: date,
    ) -> Path:
        lat = f"{location['latitude']:.4f}".replace(".", "_")
        lon = f"{location['longitude']:.4f}".replace(".", "_")
        safe_name = str(location.get("name", "location")).strip().lower().replace(" ", "_")
        month_end = date(year, month, monthrange(year, month)[1])
        if archive_available_to >= month_end:
            suffix = f"{year}_{month:02d}"
        else:
            effective_end = min(month_end, archive_available_to)
            suffix = f"{year}_{month:02d}_through_{effective_end.strftime('%Y%m%d')}"
        return self.cache_dir / f"{safe_name}_{lat}_{lon}_{suffix}.json"

    @staticmethod
    def _archive_available_to() -> date:
        return (datetime.now(UTC).date() - timedelta(days=1))

    @staticmethod
    def _month_windows(date_from: date, date_to: date) -> Iterable[tuple[int, int]]:
        current = date(date_from.year, date_from.month, 1)
        end_marker = date(date_to.year, date_to.month, 1)
        while current <= end_marker:
            yield current.year, current.month
            if current.month == 12:
                current = date(current.year + 1, 1, 1)
            else:
                current = date(current.year, current.month + 1, 1)
