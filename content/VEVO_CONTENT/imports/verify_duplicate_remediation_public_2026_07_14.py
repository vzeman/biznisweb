#!/usr/bin/env python3
"""Verify every remediated VEVO duplicate directly on public article pages."""

from __future__ import annotations

import hashlib
import json
import re
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup


PROJECT = Path(__file__).resolve().parents[1]
TOOLS = PROJECT / "tools"
sys.path.insert(0, str(TOOLS))

import vevo_public_content_guard as public_guard


BASE_URL = "https://www.vevo.sk"
ARTICLE_FILES = (
    PROJECT / "imports" / "exact-duplicate-remediation-2026-07-14-articles.json",
    PROJECT / "imports" / "semantic-duplicate-remediation-2026-07-14-articles.json",
)
REPORT = (
    PROJECT
    / "exports"
    / "duplicate-remediation-public-verify-2026-07-14.json"
)
LEGACY_HIDDEN_SLUG = "111111111111111111"


def normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFC", value or "")
    return re.sub(r"\s+", " ", normalized).strip()


def visible_text(markup: str) -> str:
    return normalize_text(
        BeautifulSoup(markup or "", "html.parser").get_text(" ", strip=True)
    )


def load_articles() -> list[dict[str, Any]]:
    articles: dict[str, dict[str, Any]] = {}
    for source in ARTICLE_FILES:
        payload = json.loads(source.read_text(encoding="utf-8"))
        for article in payload["articles"]:
            post_id = str(article["post_id"])
            if post_id in articles:
                raise RuntimeError(f"duplicate verification post id: {post_id}")
            articles[post_id] = {**article, "source_file": str(source)}
    return list(articles.values())


def verify_article(
    session: requests.Session, article: dict[str, Any]
) -> dict[str, Any]:
    url = f"{BASE_URL}/n/{article['link']}"
    response = session.get(url, timeout=45, allow_redirects=False)
    failures = []
    title = ""
    body_html = ""
    actual_text = ""
    expected_text = visible_text(article["long"])
    one_character_paragraphs: list[str] = []
    marker_match = re.search(
        r"VEVO-(?:EXACT|SEMANTIC)-DUPLICATE-REPAIR-20260714-\d+",
        article["long"],
    )
    marker = marker_match.group(0) if marker_match else ""

    if response.status_code != 200:
        failures.append(f"public status {response.status_code} != 200")
    else:
        soup = BeautifulSoup(response.text, "html.parser")
        heading = soup.select_one("h1")
        body = soup.select_one(".s1-newsDetail-cont .userHTMLContent")
        title = normalize_text(heading.get_text(" ", strip=True) if heading else "")
        body_html = str(body) if body else ""
        actual_text = visible_text(body_html)
        expected_title = normalize_text(article["title"])
        if title != expected_title:
            failures.append(f"title mismatch: {title!r} != {expected_title!r}")
        if not body:
            failures.append("public article body container is missing")
        else:
            if marker not in body_html:
                failures.append("remediation marker is missing")
            if actual_text != expected_text:
                failures.append("public visible article text differs from prepared body")
            one_character_paragraphs = [
                normalize_text(paragraph.get_text(" ", strip=True))
                for paragraph in body.find_all("p")
                if 0 < len(normalize_text(paragraph.get_text(" ", strip=True))) <= 2
            ]
            if one_character_paragraphs:
                failures.append(
                    f"one-character paragraph damage: {one_character_paragraphs[:5]}"
                )
            category_links = {
                anchor.get("href")
                for anchor in body.find_all("a", href=True)
                if str(anchor.get("href") or "").startswith("/c/")
            }
            product_links = {
                anchor.get("href")
                for anchor in body.find_all("a", href=True)
                if str(anchor.get("href") or "").startswith("/p-")
            }
            if not category_links:
                failures.append("category link is missing")
            if not product_links:
                failures.append("product link is missing")
            if len(body.find_all("table")) < int(
                article.get("metrics", {}).get("table_count", 1)
            ):
                failures.append("one or more prepared tables are missing")
            public_hits = public_guard.find_hits(
                {"title": title, "short": "", "long": body_html}
            )
            if public_hits:
                failures.append(f"forbidden public wording: {public_hits}")

    return {
        "post_id": str(article["post_id"]),
        "url": url,
        "slug": article["link"],
        "status_code": response.status_code,
        "redirect_location": response.headers.get("Location"),
        "title": title,
        "marker": marker,
        "expected_visible_sha256": hashlib.sha256(
            expected_text.encode("utf-8")
        ).hexdigest(),
        "actual_visible_sha256": hashlib.sha256(
            actual_text.encode("utf-8")
        ).hexdigest(),
        "visible_word_count": len(actual_text.split()),
        "one_character_paragraph_count": len(one_character_paragraphs),
        "ok": not failures,
        "failures": failures,
    }


def main() -> int:
    articles = load_articles()
    session = requests.Session()
    session.headers.update(
        {"User-Agent": "Codex VEVO duplicate remediation public verifier"}
    )
    results = [verify_article(session, article) for article in articles]
    hidden_url = f"{BASE_URL}/n/{LEGACY_HIDDEN_SLUG}"
    hidden_response = session.get(hidden_url, timeout=45, allow_redirects=False)
    hidden_check = {
        "post_id": "1520",
        "url": hidden_url,
        "expected_status_code": 404,
        "status_code": hidden_response.status_code,
        "ok": hidden_response.status_code == 404,
    }
    all_ok = all(item["ok"] for item in results) and hidden_check["ok"]
    report = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "article_count": len(results),
        "public_ok_count": sum(1 for item in results if item["ok"]),
        "legacy_hidden_check": hidden_check,
        "articles": results,
        "all_ok": all_ok,
    }
    REPORT.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "article_count": report["article_count"],
                "public_ok_count": report["public_ok_count"],
                "legacy_hidden_status": hidden_response.status_code,
                "report": str(REPORT),
                "all_ok": report["all_ok"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
