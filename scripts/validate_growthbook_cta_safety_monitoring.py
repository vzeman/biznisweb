#!/usr/bin/env python3
"""Validate the checked-in VEVO CTA safety-only monitoring contract."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

try:
    from scripts.evaluate_growthbook_cta_safety import (
        CtaSafetyEvaluationError,
        validate_contract,
    )
except ModuleNotFoundError:  # Direct execution from scripts/.
    from evaluate_growthbook_cta_safety import (  # type: ignore
        CtaSafetyEvaluationError,
        validate_contract,
    )


ROOT = Path(__file__).resolve().parents[1]
VEVO = ROOT / "projects" / "vevo"
CONTRACT_PATH = VEVO / "growthbook_cta_safety_monitoring.json"
DECISION_PATH = VEVO / "growthbook_cta_decision_contract.json"


def main() -> int:
    try:
        contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        validate_contract(contract)
        expected = contract["source_bindings"]["decision_contract"]["sha256"]
        actual = hashlib.sha256(DECISION_PATH.read_bytes()).hexdigest()
        if actual != expected:
            raise CtaSafetyEvaluationError(
                "CTA safety checked-in decision contract SHA-256 drift"
            )
    except (OSError, json.JSONDecodeError, CtaSafetyEvaluationError, KeyError) as exc:
        print(f"validate_growthbook_cta_safety_monitoring.py: FAIL: {exc}")
        return 2
    print(
        "VEVO_CTA_SAFETY_CONTRACT_OK:waiting=true:primary=false:"
        "winner=false:automatic=false"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
