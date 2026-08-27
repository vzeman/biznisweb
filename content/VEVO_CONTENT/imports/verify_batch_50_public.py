#!/usr/bin/env python3
"""Independently verify VEVO batch 50 on the public website."""

from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import verify_batch_45_public as checks


ARTICLES_JSON = Path("content/VEVO_CONTENT/imports/batch-50-2026-08-27-articles.json")
OUT_JSON = Path("content/VEVO_CONTENT/exports/batch-50-2026-08-27-publication-verify.json")


def main() -> None:
    articles = json.loads(ARTICLES_JSON.read_text(encoding="utf-8"))
    results = [checks.verify_article(article) for article in articles]
    link_urls = sorted(
        {
            urljoin(checks.BASE_URL, href) if href.startswith("/") else href
            for article in articles
            for href in re.findall(r'href="([^"]+)"', str(article["long"]))
        }
    )
    with ThreadPoolExecutor(max_workers=6) as executor:
        link_checks = list(executor.map(checks.verify_link, link_urls))

    output = {
        "batch": "batch-50",
        "verified_at": datetime.now(timezone.utc).isoformat(),
        "article_count": len(results),
        "all_ok": all(item["ok"] for item in results)
        and all(item["ok"] for item in link_checks),
        "articles": results,
        "link_checks": link_checks,
    }
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(
        json.dumps(output, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "article_count": output["article_count"],
                "all_ok": output["all_ok"],
                "links_checked": len(link_checks),
                "report": str(OUT_JSON),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    if not output["all_ok"]:
        print(
            json.dumps(
                {
                    "failed_articles": [item for item in results if not item["ok"]],
                    "failed_links": [item for item in link_checks if not item["ok"]],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        raise SystemExit("Batch 50 public verification failed")


if __name__ == "__main__":
    main()
