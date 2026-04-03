#!/usr/bin/env python3
"""
Shared artifact/output contracts for reporting runs.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Dict, Optional

from .config import project_data_dir


@dataclass(frozen=True)
class ReportingArtifactSet:
    project_name: str
    from_date: str
    to_date: str
    output_tag: str
    data_dir: Path
    report_html: Path
    email_strategy_html: Path
    export_csv: Path
    aggregate_by_date_csv: Path
    aggregate_by_month_csv: Path
    aggregate_by_date_product_csv: Path
    data_quality_json: Path
    weather_impact_csv: Path

    def as_dict(self) -> Dict[str, Path]:
        return {
            "report_html": self.report_html,
            "email_strategy_html": self.email_strategy_html,
            "export_csv": self.export_csv,
            "aggregate_by_date_csv": self.aggregate_by_date_csv,
            "aggregate_by_month_csv": self.aggregate_by_month_csv,
            "aggregate_by_date_product_csv": self.aggregate_by_date_product_csv,
            "data_quality_json": self.data_quality_json,
            "weather_impact_csv": self.weather_impact_csv,
        }

    def required_daily_runner_outputs(self) -> Dict[str, Path]:
        return {
            "report_html": self.report_html,
            "export_csv": self.export_csv,
            "aggregate_by_date_csv": self.aggregate_by_date_csv,
            "aggregate_by_month_csv": self.aggregate_by_month_csv,
        }


def sanitize_output_tag(output_tag: Optional[str]) -> str:
    normalized = re.sub(r"[^A-Za-z0-9_-]+", "_", str(output_tag or "").strip())
    normalized = normalized.strip("._-")
    return normalized


def apply_output_tag(path: Path, output_tag: Optional[str]) -> Path:
    tag = sanitize_output_tag(output_tag)
    if not tag:
        return path
    return path.with_name(f"{path.stem}__{tag}{path.suffix}")


def build_artifact_set(project: str, from_date: str, to_date: str, output_tag: Optional[str] = None) -> ReportingArtifactSet:
    compact_range = f"{from_date.replace('-', '')}-{to_date.replace('-', '')}"
    data_dir = project_data_dir(project)
    tag = sanitize_output_tag(output_tag)
    return ReportingArtifactSet(
        project_name=project,
        from_date=from_date,
        to_date=to_date,
        output_tag=tag,
        data_dir=data_dir,
        report_html=apply_output_tag(data_dir / f"report_{compact_range}.html", tag),
        email_strategy_html=apply_output_tag(data_dir / f"email_strategy_{compact_range}.html", tag),
        export_csv=apply_output_tag(data_dir / f"export_{compact_range}.csv", tag),
        aggregate_by_date_csv=apply_output_tag(data_dir / f"aggregate_by_date_{compact_range}.csv", tag),
        aggregate_by_month_csv=apply_output_tag(data_dir / f"aggregate_by_month_{compact_range}.csv", tag),
        aggregate_by_date_product_csv=apply_output_tag(data_dir / f"aggregate_by_date_product_{compact_range}.csv", tag),
        data_quality_json=apply_output_tag(data_dir / f"data_quality_{compact_range}.json", tag),
        weather_impact_csv=apply_output_tag(data_dir / f"weather_impact_{compact_range}.csv", tag),
    )
