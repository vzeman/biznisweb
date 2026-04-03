#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from reporting_core.config import PROJECTS_DIR, build_project_context


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def latest_match(directory: Path, pattern: str) -> Optional[Path]:
    matches = sorted(directory.glob(pattern), key=lambda item: item.stat().st_mtime, reverse=True)
    return matches[0] if matches else None


def load_json(path: Optional[Path]) -> Optional[Dict[str, Any]]:
    if not path or not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def serialize_file(path: Optional[Path]) -> Optional[Dict[str, Any]]:
    if not path or not path.exists():
        return None
    stat = path.stat()
    return {
        "path": str(path.relative_to(ROOT)),
        "modified_utc": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "size_bytes": stat.st_size,
    }


def discover_projects() -> List[str]:
    return sorted(path.parent.name for path in PROJECTS_DIR.glob("*/settings.json"))


def build_project_snapshot(project_name: str) -> Dict[str, Any]:
    context = build_project_context(project_name)
    data_dir = context.data_dir
    latest_report = latest_match(data_dir, "report_*.html")
    latest_export = latest_match(data_dir, "export_*.csv")
    latest_cfo = latest_match(data_dir, "cfo_graphs_*.html")
    latest_quality = latest_match(data_dir, "data_quality_*.json")
    latest_weather = latest_match(data_dir, "weather_impact_*.csv")
    quality_payload = load_json(latest_quality) or {}
    source_states = {}
    for source_key, payload in (quality_payload.get("sources") or {}).items():
        source_states[source_key] = {
            "status": payload.get("status"),
            "healthy": payload.get("healthy"),
            "message": payload.get("message"),
        }
    return {
        "project": project_name,
        "display_name": context.reporting_defaults.get("display_name", project_name),
        "data_dir": str(data_dir.relative_to(ROOT)),
        "latest_artifacts": {
            "report_html": serialize_file(latest_report),
            "export_csv": serialize_file(latest_export),
            "cfo_html": serialize_file(latest_cfo),
            "data_quality_json": serialize_file(latest_quality),
            "weather_impact_csv": serialize_file(latest_weather),
        },
        "latest_source_health": {
            "overall_status": quality_payload.get("overall_status"),
            "is_partial": quality_payload.get("is_partial"),
            "summary": quality_payload.get("summary"),
            "partial_sources": quality_payload.get("partial_sources") or [],
            "sources": source_states,
        },
    }


def build_snapshot(projects: List[str], ci_mode: bool = False) -> Dict[str, Any]:
    return {
        "generated_at_utc": iso_utc_now(),
        "mode": "ci" if ci_mode else "local",
        "project_count": len(projects),
        "projects": [build_project_snapshot(project_name) for project_name in projects],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Emit reporting observability snapshot.")
    parser.add_argument("--project", action="append", dest="projects", help="Specific project slug(s) to include.")
    parser.add_argument("--ci", action="store_true", help="CI mode: fail if no project templates/settings exist.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    parser.add_argument("--output", type=Path, help="Optional path to write the snapshot JSON.")
    args = parser.parse_args()

    projects = sorted(set(args.projects or discover_projects()))
    if args.ci and not projects:
        raise SystemExit("No projects discovered under projects/*/settings.json.")

    payload = build_snapshot(projects, ci_mode=args.ci)
    serialized = json.dumps(payload, ensure_ascii=False, indent=2 if args.pretty else None)

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(serialized + ("\n" if args.pretty else ""), encoding="utf-8")
    else:
        print(serialized)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
