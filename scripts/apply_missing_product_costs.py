#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from reporting_core import load_project_settings, project_dir  # noqa: E402


Summary = Dict[str, Any]


def parse_purchase_cost(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip().replace("\u00a0", "").replace(" ", "").replace("€", "")
    if not text:
        return None
    if "," in text and "." not in text:
        text = text.replace(",", ".")
    try:
        parsed = float(text)
    except ValueError as exc:
        raise ValueError(f"Invalid purchase_cost_net value: {value!r}") from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise ValueError(f"purchase_cost_net must be a finite non-negative number: {value!r}")
    return round(parsed, 6)


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "null"}:
        return ""
    return text


def compose_key(*parts: Any) -> str:
    values = [clean_text(part) for part in parts]
    if not values or any(not value for value in values):
        return ""
    return "||".join(values)


def resolve_expense_key(row: Dict[str, Any]) -> str:
    explicit_key = clean_text(row.get("expense_key") or row.get("suggested_expense_key"))
    if explicit_key:
        return explicit_key

    label = clean_text(row.get("item_label"))
    for identifier_column in ("item_import_code", "item_ean", "item_warehouse_number", "product_sku"):
        candidate = compose_key(label, row.get(identifier_column))
        if candidate:
            return candidate
    return clean_text(row.get("product_sku")) or label


def load_csv_rows(csv_path: Path) -> List[Dict[str, Any]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def load_existing_expenses(expenses_path: Path) -> Dict[str, float]:
    if not expenses_path.exists():
        return {}
    with expenses_path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle) or {}
    return {str(key): float(value) for key, value in raw.items()}


def apply_missing_cost_rows(
    existing: Dict[str, float],
    rows: Iterable[Dict[str, Any]],
    *,
    allow_overwrite: bool = False,
) -> Tuple[Dict[str, float], Summary]:
    updated = dict(existing)
    summary: Summary = {
        "applied": 0,
        "skipped_empty_cost": 0,
        "skipped_missing_key": 0,
        "skipped_existing_same": 0,
        "skipped_existing_conflict": 0,
        "invalid_rows": [],
        "conflicts": [],
    }

    for index, row in enumerate(rows, start=2):
        try:
            cost = parse_purchase_cost(row.get("purchase_cost_net") or row.get("new_purchase_cost_net"))
        except ValueError as exc:
            summary["invalid_rows"].append(f"row {index}: {exc}")
            continue

        if cost is None:
            summary["skipped_empty_cost"] += 1
            continue

        key = resolve_expense_key(row)
        if not key:
            summary["skipped_missing_key"] += 1
            continue

        existing_cost = updated.get(key)
        if existing_cost is not None and not allow_overwrite:
            if round(float(existing_cost), 6) == cost:
                summary["skipped_existing_same"] += 1
            else:
                summary["skipped_existing_conflict"] += 1
                summary["conflicts"].append(
                    f"{key}: existing={float(existing_cost):.6f}, csv={cost:.6f}"
                )
            continue

        updated[key] = cost
        summary["applied"] += 1

    return updated, summary


def resolve_product_expenses_path(project: str) -> Path:
    settings = load_project_settings(project)
    product_expenses_file = clean_text(settings.get("product_expenses_file")) or "product_expenses.json"
    return project_dir(project) / product_expenses_file


def write_expenses(expenses_path: Path, expenses: Dict[str, float]) -> None:
    expenses_path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(sorted(expenses.items(), key=lambda item: item[0].lower()))
    expenses_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def print_summary(summary: Summary, *, dry_run: bool, expenses_path: Path) -> None:
    mode = "DRY RUN" if dry_run else "APPLIED"
    print(f"{mode}: {summary['applied']} product cost mapping(s) for {expenses_path}")
    print(f"Skipped empty purchase_cost_net: {summary['skipped_empty_cost']}")
    print(f"Skipped missing key: {summary['skipped_missing_key']}")
    print(f"Skipped existing same value: {summary['skipped_existing_same']}")
    print(f"Skipped existing conflicts: {summary['skipped_existing_conflict']}")
    if summary["invalid_rows"]:
        print("Invalid rows:")
        for message in summary["invalid_rows"]:
            print(f"- {message}")
    if summary["conflicts"]:
        print("Conflicts; re-run with --allow-overwrite only if the CSV value is intentional:")
        for message in summary["conflicts"]:
            print(f"- {message}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Apply purchase costs from a missing_product_costs CSV into projects/<project>/product_expenses.json."
    )
    parser.add_argument("--project", required=True, help="Reporting project name, e.g. vevo or roy")
    parser.add_argument("--csv", required=True, help="Path to missing_product_costs CSV generated by the report")
    parser.add_argument("--dry-run", action="store_true", help="Validate and print the summary without writing JSON")
    parser.add_argument(
        "--allow-overwrite",
        action="store_true",
        help="Allow CSV values to overwrite existing product_expenses keys",
    )
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing CSV: {csv_path}")

    expenses_path = resolve_product_expenses_path(args.project)
    rows = load_csv_rows(csv_path)
    existing = load_existing_expenses(expenses_path)
    updated, summary = apply_missing_cost_rows(
        existing,
        rows,
        allow_overwrite=bool(args.allow_overwrite),
    )

    print_summary(summary, dry_run=bool(args.dry_run), expenses_path=expenses_path)
    if summary["invalid_rows"]:
        return 2
    if bool(summary["conflicts"]) and not args.allow_overwrite:
        return 3
    if not args.dry_run:
        write_expenses(expenses_path, updated)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
