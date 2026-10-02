#!/usr/bin/env python3
"""Read-only exact subject scan for VEVO batch 56 candidates."""

from __future__ import annotations

import importlib.util
import json
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
MCP_PATH = ROOT / "content/VEVO_CONTENT/tools/biznisweb_vevo_content_mcp.py"
OUT = ROOT / "content/VEVO_CONTENT/exports/batch-56-admin-candidate-scan-2026-10-02.json"

SECTIONS = {
    "blog": "765",
    "faq": "774",
    "glossary": "1905",
}

SUBJECTS = {
    "mercerized cotton": ["mercerovana bavlna", "mercerized cotton", "mercerised cotton", "mercerizacia"],
    "sanforized cotton": ["sanforizovana bavlna", "sanforized cotton", "sanforised cotton", "predzrazena bavlna"],
    "easy-care cotton": ["nekrciva bavlna", "easy care bavlna", "easy care cotton", "non iron cotton", "wrinkle resistant cotton"],
    "biopolished cotton": ["biolestena bavlna", "bio polished cotton", "biopolished cotton", "enzymova uprava bavlny"],
    "garment dyed": ["garment dyed", "garment dye", "farbene v hotovom odeve", "farbeneho hotoveho odevu"],
    "pigment dyed": ["pigmentovo farbene", "pigment dyed", "pigment dye"],
    "stonewashed denim": ["stonewashed", "stone washed", "enzyme washed denim", "enzymovo prany denim"],
    "peach finish": ["peach finish", "peached fabric", "peach skin", "bruseny povrch"],
    "calendered fabric": ["kalandrovana tkanina", "calendered fabric", "calender finish", "kalandrovanie"],
    "chintz": ["chintz", "cinc", "glazovana bavlna"],
    "ikat": ["ikat"],
    "batik": ["batika", "batik"],
    "devore": ["devore", "devore textil", "burnout fabric", "burn out fabric"],
    "flocked textile": ["flockovany textil", "flocked fabric", "flock fabric"],
    "plisse": ["plise", "plisse", "permanent pleats", "krcena uprava"],
    "smocking": ["smocking", "smokovanie", "smokovane riasenie"],
    "broderie anglaise": ["broderie anglaise", "eyelet fabric", "dierkovana vysivka"],
    "brocade": ["brokat", "brocade"],
}


def normalize(value: object) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(char for char in text if not unicodedata.combining(char))
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()


def contains_phrase(haystack: str, alias: str) -> bool:
    phrase = normalize(alias)
    return bool(phrase) and re.search(rf"(?:^| ){re.escape(phrase)}(?: |$)", haystack) is not None


def load_mcp():
    spec = importlib.util.spec_from_file_location("vevo_content_mcp", MCP_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load repo-local MCP module: {MCP_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> None:
    mcp = load_mcp()
    sections: dict[str, object] = {}
    total_matches = 0
    for name, block_id in SECTIONS.items():
        rows = mcp.admin_list_news_posts(block_id, limit=mcp.DUPLICATE_SCAN_LIMIT)
        matches: list[dict[str, object]] = []
        for row in rows:
            haystack = normalize(f"{row.get('title', '')} {row.get('link', '')}")
            matched_subjects = [
                subject
                for subject, aliases in SUBJECTS.items()
                if any(contains_phrase(haystack, alias) for alias in aliases)
            ]
            if matched_subjects:
                matches.append(
                    {
                        "id": row.get("id") or row.get("post_id") or row.get("news_id"),
                        "title": row.get("title"),
                        "link": row.get("link"),
                        "active": row.get("active"),
                        "subjects": matched_subjects,
                    }
                )
        total_matches += len(matches)
        sections[name] = {
            "block_id": block_id,
            "record_count": len(rows),
            "match_count": len(matches),
            "matches": matches,
        }

    report = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "account": "vevo.flox.sk",
        "public_domain": "www.vevo.sk",
        "language_id": "1",
        "page_id": "309",
        "candidate_subject_count": len(SUBJECTS),
        "total_match_count": total_matches,
        "sections": sections,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
