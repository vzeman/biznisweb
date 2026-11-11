import html
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests


BASE_URL = "https://www.vevo.sk"
ARTICLES_JSON = Path("content/VEVO_CONTENT/imports/batch-41-2026-07-21-articles.json")
OUT_JSON = Path("content/VEVO_CONTENT/exports/batch-41-2026-07-21-publication-verify.json")
USER_AGENT = "Codex VEVO batch 41 public verification"
PARAGRAPH_RE = re.compile(r"(?is)<p\b[^>]*>(.*?)</p>")
TAG_RE = re.compile(r"<[^>]+>")
WORD_RE = re.compile(r"[^\W_]+(?:[-'][^\W_]+)?", re.UNICODE)


def fetch(url):
    response = requests.get(
        url,
        headers={"User-Agent": USER_AGENT},
        timeout=45,
        allow_redirects=True,
    )
    return response, response.content.decode("utf-8", errors="replace")


def visible_text(markup):
    without_tags = TAG_RE.sub(" ", markup)
    return re.sub(r"\s+", " ", html.unescape(without_tags)).strip()


def article_segment(page_source):
    starts = [
        page_source.find("<strong>Rýchla odpoveď:"),
        page_source.find("Rýchla odpoveď"),
    ]
    starts = [position for position in starts if position >= 0]
    if not starts:
        return page_source
    start = min(starts)
    ends = [
        page_source.find("Diskusia", start),
        page_source.find("Pridať komentár", start),
        page_source.find("s1-newsNavigation", start),
        page_source.find("s1-socialShare", start),
        page_source.find("</main>", start),
    ]
    ends = [position for position in ends if position >= 0]
    end = min(ends) if ends else min(len(page_source), start + 240_000)
    return page_source[start:end]


def short_paragraph_metrics(markup):
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


def expected_links(article, prefix):
    return sorted(
        {
            href
            for href in re.findall(r'href="([^"]+)"', article["long"])
            if href.startswith(prefix)
        }
    )


def main():
    articles = json.loads(ARTICLES_JSON.read_text(encoding="utf-8"))
    results = []

    for article in articles:
        expected_url = f'{BASE_URL}/n/{article["link"]}'
        response, page = fetch(expected_url)
        segment = article_segment(page)
        product_links = expected_links(article, "/p-")
        category_links = expected_links(article, "/c/")
        external_links = sorted(
            {
                href
                for href in re.findall(r'href="([^"]+)"', article["long"])
                if href.startswith("https://") and "vevo.sk" not in href
            }
        )
        short_count, short_run = short_paragraph_metrics(segment)
        final_path = urlparse(response.url).path.rstrip("/")
        expected_path = f'/n/{article["link"]}'
        checks = {
            "title": article["title"],
            "slug": article["link"],
            "url": expected_url,
            "status": response.status_code,
            "final_url": response.url,
            "clean_url_ok": final_path == expected_path,
            "title_present": article["title"] in html.unescape(page),
            "quick_answer_present": "Rýchla odpoveď" in segment,
            "visible_word_count": len(WORD_RE.findall(visible_text(segment))),
            "h2_count": len(re.findall(r"<h2\b", segment, re.IGNORECASE)),
            "table_count": len(re.findall(r"<table\b", segment, re.IGNORECASE)),
            "styled_block_count": len(
                re.findall(r"<div\b[^>]*\bstyle=", segment, re.IGNORECASE)
            ),
            "product_links_present": all(href in segment for href in product_links),
            "category_links_present": all(href in segment for href in category_links),
            "external_sources_present": all(href in segment for href in external_links),
            "bad_escaped_html": bool(
                re.search(r"&lt;/?(?:p|h2|h3|div|table|a)\b", segment, re.IGNORECASE)
            ),
            "malformed_href_count": len(re.findall(r'href=(?!["\'])', segment)),
            "short_paragraph_count": short_count,
            "max_short_paragraph_run": short_run,
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
                checks["visible_word_count"] >= 1700,
                checks["h2_count"] >= 16,
                checks["table_count"] >= 2,
                checks["styled_block_count"] >= 8,
                checks["product_links_present"],
                checks["category_links_present"],
                checks["external_sources_present"],
                not checks["bad_escaped_html"],
                checks["malformed_href_count"] == 0,
                checks["max_short_paragraph_run"] < 3,
            ]
        )
        results.append(checks)

    link_urls = sorted(
        {
            urljoin(BASE_URL, href) if href.startswith("/") else href
            for article in articles
            for href in re.findall(r'href="([^"]+)"', article["long"])
        }
    )
    link_checks = []
    for url in link_urls:
        response, _ = fetch(url)
        link_checks.append(
            {
                "url": url,
                "status": response.status_code,
                "final_url": response.url,
                "ok": response.status_code == 200,
            }
        )

    output = {
        "batch": "batch-41",
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
        failed_articles = [
            {"slug": item["slug"], "checks": item}
            for item in results
            if not item["ok"]
        ]
        failed_links = [item for item in link_checks if not item["ok"]]
        print(
            json.dumps(
                {"failed_articles": failed_articles, "failed_links": failed_links},
                ensure_ascii=False,
                indent=2,
            )
        )
        raise SystemExit("Batch 41 public verification failed")


if __name__ == "__main__":
    main()
