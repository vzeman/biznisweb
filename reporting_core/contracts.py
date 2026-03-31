#!/usr/bin/env python3
"""
Shared artifact/output contracts for reporting runs.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict

from .config import project_data_dir


@dataclass(frozen=True)
class ReportingArtifactSet:
    project_name: str
    from_date: str
    to_date: str
    data_dir: Path
    report_html: Path
    email_strategy_html: Path
    export_csv: Path
    aggregate_by_date_csv: Path
    aggregate_by_month_csv: Path
    aggregate_by_date_product_csv: Path
    data_quality_json: Path
    weather_impact_csv: Path
    cfo_graph_html: Path

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
            "cfo_graph_html": self.cfo_graph_html,
        }

    def required_daily_runner_outputs(self) -> Dict[str, Path]:
        return {
            "report_html": self.report_html,
            "export_csv": self.export_csv,
            "aggregate_by_date_csv": self.aggregate_by_date_csv,
            "aggregate_by_month_csv": self.aggregate_by_month_csv,
        }


def build_artifact_set(project: str, from_date: str, to_date: str) -> ReportingArtifactSet:
    compact_range = f"{from_date.replace('-', '')}-{to_date.replace('-', '')}"
    data_dir = project_data_dir(project)
    return ReportingArtifactSet(
        project_name=project,
        from_date=from_date,
        to_date=to_date,
        data_dir=data_dir,
        report_html=data_dir / f"report_{compact_range}.html",
        email_strategy_html=data_dir / f"email_strategy_{compact_range}.html",
        export_csv=data_dir / f"export_{compact_range}.csv",
        aggregate_by_date_csv=data_dir / f"aggregate_by_date_{compact_range}.csv",
        aggregate_by_month_csv=data_dir / f"aggregate_by_month_{compact_range}.csv",
        aggregate_by_date_product_csv=data_dir / f"aggregate_by_date_product_{compact_range}.csv",
        data_quality_json=data_dir / f"data_quality_{compact_range}.json",
        weather_impact_csv=data_dir / f"weather_impact_{compact_range}.csv",
        cfo_graph_html=data_dir / f"cfo_graphs_{compact_range}.html",
    )
