#!/usr/bin/env python3
"""Verify batch 55 exact title, slug and active state in VEVO admin."""

from __future__ import annotations

import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
MCP_PATH = ROOT / "content/VEVO_CONTENT/tools/biznisweb_vevo_content_mcp.py"
ARTICLES_PATH = ROOT / "content/VEVO_CONTENT/imports/batch-55-2026-08-28-articles.json"
OUT = ROOT / "content/VEVO_CONTENT/exports/batch-55-2026-08-28-admin-readback.json"
BLOCK_ID = "765"


def load_mcp():
    spec = importlib.util.spec_from_file_location("vevo_content_mcp", MCP_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load repo-local MCP module: {MCP_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> None:
    mcp = load_mcp()
    articles = json.loads(ARTICLES_PATH.read_text(encoding="utf-8"))
    rows = mcp.admin_list_news_posts(BLOCK_ID, limit=mcp.DUPLICATE_SCAN_LIMIT)
    checks = []
    for article in articles:
        matches = [
            row
            for row in rows
            if str(row.get("title") or "").strip() == article["title"]
            and str(row.get("link") or "").strip() == article["link"]
        ]
        checks.append(
            {
                "title": article["title"],
                "slug": article["link"],
                "match_count": len(matches),
                "post_id": (matches[0].get("news_id") if len(matches) == 1 else None),
                "active": (matches[0].get("active") if len(matches) == 1 else None),
                "ok": len(matches) == 1 and str(matches[0].get("active") or "") == "1",
            }
        )
    report = {
        "verified_at": datetime.now(timezone.utc).isoformat(),
        "account": "vevo.flox.sk",
        "public_domain": "www.vevo.sk",
        "language_id": "1",
        "page_id": "309",
        "block_id": BLOCK_ID,
        "admin_catalog_count": len(rows),
        "article_count": len(checks),
        "all_ok": all(check["ok"] for check in checks),
        "checks": checks,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if not report["all_ok"]:
        raise SystemExit("Batch 55 admin readback failed")


if __name__ == "__main__":
    main()
