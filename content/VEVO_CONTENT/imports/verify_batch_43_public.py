#!/usr/bin/env python3
"""Independently verify VEVO batch 43 on the public website."""

from __future__ import annotations

import html
import json
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests


BASE_URL = "https://www.vevo.sk"
ARTICLES_JSON = Path("content/VEVO_CONTENT/imports/batch-43-2026-08-14-articles.json")
OUT_JSON = Path(
    "content/VEVO_CONTENT/exports/batch-43-2026-08-14-publication-verify.json"
)
USER_AGENT = "Codex VEVO batch 43 independent public verification"
PARAGRAPH_RE = re.compile(r"(?is)<p\b[^>]*>(.*?)</p>")
TAG_RE = re.compile(r"<[^>]+>")
WORD_RE = re.compile(r"[^\W_]+(?:[-'][^\W_]+)?", re.UNICODE)
INTERNAL_TERMS_RE = re.compile(
    r"\b(?:CTA|SEO|longtail|long[ -]?tail|keyword|fan[ -]?out)\b",
    re.IGNORECASE,
)
FIXED_PRICE_RE = re.compile(r"\b\d{1,4}(?:[.,]\d{2})?\s*(?:EUR|\u20ac)\b", re.IGNORECASE)


def fetch(url: str) -> tuple[requests.Response, str]:
    response = requests.get(
        url,
        headers={"User-Agent": USER_AGENT},
        timeout=45,
        allow_redirects=True,
    )
    return response, response.content.decode("utf-8", errors="replace")


def mojibake_variant(value: str) -> str:
    return value.encode("utf-8").decode("cp1250", errors="replace")


def visible_text(markup: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(TAG_RE.sub(" ", markup))).strip()


def article_segment(page_source: str) -> str:
    quick_answer = "R\u00fdchla odpove\u010f"
    starts = [
        page_source.find(marker)
        for marker in (quick_answer, mojibake_variant(quick_answer))
    ]
    starts = [position for position in starts if position >= 0]
    if not starts:
        return page_source
    start = min(starts)
    end_candidates = [
        page_source.find(marker, start)
        for marker in (
            "Diskusia",
            mojibake_variant("Diskusia"),
            "s1-newsDetail-footer",
            "s1-newsNavigation",
            "s1-socialShare",
            "</main>",
        )
    ]
    end_candidates = [position for position in end_candidates if position >= 0]
    end = min(end_candidates) if end_candidates else min(len(page_source), start + 320_000)
    return page_source[start:end]


def short_paragraph_metrics(markup: str) -> tuple[int, int]:
    total = 0
    current_run = 0
    max_run = 0
    for raw in PARAGRAPH_RE.findall(markup):
        if len(visible_text(raw)) <= 2:
            total += 1
            current_run += 1
            max_run = max(max_run, current_run)
        else:
            current_run = 0
    return total, max_run


def expected_links(article: dict[str, object], prefix: str) -> list[str]:
    return sorted(
        {
            href
            for href in re.findall(r'href="([^"]+)"', str(article["long"]))
            if href.startswith(prefix)
        }
    )


def text_variant_present(value: str, page: str) -> bool:
    return value in html.unescape(page) or mojibake_variant(value) in html.unescape(page)


def verify_article(article: dict[str, object]) -> dict[str, object]:
    slug = str(article["link"])
    title = str(article["title"])
    expected_url = f"{BASE_URL}/n/{slug}"
    response, page = fetch(expected_url)
    segment = article_segment(page)
    rendered_text = visible_text(segment)
    product_links = expected_links(article, "/p-")
    category_links = expected_links(article, "/c/")
    external_links = sorted(
        {
            href
            for href in re.findall(r'href="([^"]+)"', str(article["long"]))
            if href.startswith("https://") and "vevo.sk" not in href
        }
    )
    short_count, short_run = short_paragraph_metrics(segment)
    final_path = urlparse(response.url).path.rstrip("/")
    expected_path = f"/n/{slug}"
    checks: dict[str, object] = {
        "title": title,
        "slug": slug,
        "url": expected_url,
        "status": response.status_code,
        "final_url": response.url,
        "clean_url_ok": final_path == expected_path,
        "title_present": text_variant_present(title, page),
        "quick_answer_present": text_variant_present("R\u00fdchla odpove\u010f", segment),
        "visible_word_count": len(WORD_RE.findall(rendered_text)),
        "h2_count": len(re.findall(r"<h2\b", segment, re.IGNORECASE)),
        "table_count": len(re.findall(r"<table\b", segment, re.IGNORECASE)),
        "responsive_table_count": len(
            re.findall(
                r'<div\b[^>]*style="[^"]*overflow-x:\s*auto[^"]*"[^>]*>\s*'
                r'<table\b[^>]*style="[^"]*min-width:',
                segment,
                re.IGNORECASE,
            )
        ),
        "styled_block_count": len(
            re.findall(r"<div\b[^>]*\bstyle=", segment, re.IGNORECASE)
        ),
        "action_button_count": len(
            re.findall(
                r'<a\b[^>]*style="[^"]*display:\s*inline-block[^"]*padding:'
                r'[^"]*"[^>]*href="/(?:p-|c/)',
                segment,
                re.IGNORECASE,
            )
        ),
        "product_links_present": bool(product_links)
        and all(href in segment for href in product_links),
        "category_links_present": bool(category_links)
        and all(href in segment for href in category_links),
        "external_sources_present": bool(external_links)
        and all(href in segment for href in external_links),
        "bad_escaped_html": bool(
            re.search(r"&lt;/?(?:p|h2|h3|div|table|a)\b", segment, re.IGNORECASE)
        ),
        "malformed_href_count": len(re.findall(r'href=(?!["\'])', segment)),
        "short_paragraph_count": short_count,
        "max_short_paragraph_run": short_run,
        "internal_wording_found": sorted(set(INTERNAL_TERMS_RE.findall(rendered_text))),
        "fixed_price_found": bool(FIXED_PRICE_RE.search(rendered_text)),
        "product_links": product_links,
        "category_links": category_links,
        "external_links": external_links,
        "body_length": len(segment),
    }
    checks["ok"] = all(
        [
            checks["status"] == 200,
            checks["clean_url_ok"],
            checks["title_present"],
            checks["quick_answer_present"],
            checks["visible_word_count"] >= 2500,
            checks["h2_count"] >= 24,
            checks["table_count"] >= 2,
            checks["responsive_table_count"] == checks["table_count"],
            checks["styled_block_count"] >= 10,
            checks["action_button_count"] >= 2,
            checks["product_links_present"],
            checks["category_links_present"],
            checks["external_sources_present"],
            not checks["bad_escaped_html"],
            checks["malformed_href_count"] == 0,
            checks["max_short_paragraph_run"] < 3,
            not checks["internal_wording_found"],
            not checks["fixed_price_found"],
        ]
    )
    return checks


def verify_link(url: str) -> dict[str, object]:
    try:
        response, _ = fetch(url)
        host = urlparse(url).netloc.lower()
        iso_automation_block = host == "www.iso.org" and response.status_code == 403
        return {
            "url": url,
            "status": response.status_code,
            "final_url": response.url,
            "allowed_iso_automation_block": iso_automation_block,
            "ok": response.status_code == 200 or iso_automation_block,
        }
    except requests.RequestException as exc:
        return {
            "url": url,
            "status": None,
            "final_url": None,
            "allowed_iso_automation_block": False,
            "ok": False,
            "error": str(exc),
        }


def main() -> None:
    articles = json.loads(ARTICLES_JSON.read_text(encoding="utf-8"))
    results = [verify_article(article) for article in articles]
    link_urls = sorted(
        {
            urljoin(BASE_URL, href) if href.startswith("/") else href
            for article in articles
            for href in re.findall(r'href="([^"]+)"', str(article["long"]))
        }
    )
    with ThreadPoolExecutor(max_workers=6) as executor:
        link_checks = list(executor.map(verify_link, link_urls))

    output = {
        "batch": "batch-43",
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
        raise SystemExit("Batch 43 public verification failed")


if __name__ == "__main__":
    main()
