"""Isolated pre-checkpoint scenarios, independent of the live A/A lifecycle."""

from __future__ import annotations

import json
from pathlib import Path

from scripts.validate_growthbook_aa_measurement_window import (
    expected_measurement_window,
    validate_measurement_window,
)


ROOT = Path(__file__).resolve().parents[1] / "projects" / "vevo"


def load(name: str) -> dict[str, object]:
    return json.loads((ROOT / name).read_text(encoding="utf-8"))


def initial_snapshot() -> dict[str, object]:
    snapshot = load("growthbook_aa_snapshot.json")
    activation = load("growthbook_production_aa_activation.json")
    acceptance = load("growthbook_aa_acceptance.json")
    reconciliation = load("growthbook_production_reconciliation_deploy_evidence.json")
    window = expected_measurement_window(activation, acceptance, reconciliation)
    snapshot["measurement_window"] = window
    snapshot["snapshot_build_allowed"] = False
    for component_name in ("automated_evidence", "manual_qa_evidence"):
        snapshot[component_name].update(
            producer_allowed=False,
            window_status="frozen_waiting_for_completion",
            from_utc=window["from_utc"],
            through_utc=None,
            status="not_recorded",
            run_id=None,
            main_commit=None,
            sha256=None,
        )
    snapshot["automated_evidence"].update(
        quality_report_status="not_recorded",
        quality_report_key=None,
        quality_report_sha256=None,
    )
    snapshot["manual_qa_evidence"].update(
        observation_status="not_recorded", observation_sha256=None
    )
    validate_measurement_window(snapshot, activation, acceptance, reconciliation)
    return snapshot
