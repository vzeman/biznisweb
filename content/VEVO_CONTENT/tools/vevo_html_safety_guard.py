import argparse
import html
import json
import re
from pathlib import Path


P_RE = re.compile(r"(?is)<p\b[^>]*>(.*?)</p>")
TAG_RE = re.compile(r"<[^>]+>")
HREF_RE = re.compile(r"href\s*=\s*([\"'])(.*?)\1", re.IGNORECASE | re.DOTALL)
PRICE_RE = re.compile(r"(?:\bCena\s*:|\b\d{1,4}[,.]\d{2}\s*(?:€|EUR))", re.IGNORECASE)
CLEAN_SLUG_RE = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")


def visible_text(markup):
    return html.unescape(re.sub(r"\s+", " ", TAG_RE.sub(" ", markup))).strip()


def load_articles(path):
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("articles", "updates"):
            if isinstance(data.get(key), list):
                return data[key]
    raise SystemExit(f"Unsupported article JSON structure: {path}")


def short_paragraph_metrics(markup):
    max_run = 0
    current_run = 0
    total_short = 0
    for raw in P_RE.findall(markup):
        text = visible_text(raw)
        if len(text) <= 2:
            total_short += 1
            current_run += 1
            max_run = max(max_run, current_run)
        else:
            current_run = 0
    return max_run, total_short


def analyze(article):
    title = str(article.get("title") or "")
    slug = str(article.get("link") or article.get("slug") or "").strip().strip("/")
    long = str(article.get("long") or "")
    short = str(article.get("short") or "")
    failures = []

    if not title or not short or not long:
        failures.append("missing title, short, or long content")
    if not CLEAN_SLUG_RE.fullmatch(slug):
        failures.append("slug is not clean lowercase ASCII")
    if re.fullmatch(r"1{2,}", slug):
        failures.append("slug is a repeated-1 placeholder")
    if re.search(r"(?is)<script\b|<iframe\b|\son\w+\s*=|javascript\s*:", long):
        failures.append("unsafe script, iframe, event handler, or javascript URL")
    if re.search(r"&lt;\s*(?:p|div|h2|table)\b", long, re.IGNORECASE):
        failures.append("escaped HTML detected")
    if PRICE_RE.search(visible_text(long)):
        failures.append("fixed product price detected")
    if len(re.findall(r"<div\b[^>]*\bstyle=", long, re.IGNORECASE)) < 4:
        failures.append("fewer than 4 inline-styled content blocks")
    if not re.search(r'href=["\'](?:https://www\.vevo\.sk)?/p-', long, re.IGNORECASE):
        failures.append("missing product link")
    if not re.search(r'href=["\'](?:https://www\.vevo\.sk)?/c/', long, re.IGNORECASE):
        failures.append("missing category link")

    malformed_links = []
    for _, href in HREF_RE.findall(long):
        stripped = html.unescape(href).strip()
        if (
            not stripped
            or stripped.startswith(('"', "'"))
            or stripped.endswith(('"', "'"))
            or "%22" in stripped.lower()
            or "&quot;" in href.lower()
        ):
            malformed_links.append(href)
    if malformed_links:
        failures.append(f"malformed href values: {malformed_links[:3]}")

    max_short_run, short_count = short_paragraph_metrics(long)
    if max_short_run:
        failures.append(f"short paragraph run detected: {max_short_run}")

    return {
        "title": title,
        "slug": slug,
        "body_length": len(long),
        "styled_block_count": len(re.findall(r"<div\b[^>]*\bstyle=", long, re.IGNORECASE)),
        "table_count": len(re.findall(r"<table\b", long, re.IGNORECASE)),
        "product_link_count": len(re.findall(r'href=["\'](?:https://www\.vevo\.sk)?/p-', long, re.IGNORECASE)),
        "category_link_count": len(re.findall(r'href=["\'](?:https://www\.vevo\.sk)?/c/', long, re.IGNORECASE)),
        "short_paragraph_count": short_count,
        "max_short_paragraph_run": max_short_run,
        "ok": not failures,
        "failures": failures,
    }


def main():
    parser = argparse.ArgumentParser(description="Validate VEVO rich HTML and slug safety before publication.")
    parser.add_argument("paths", nargs="+", type=Path)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()

    articles = []
    for path in args.paths:
        for article in load_articles(path):
            result = analyze(article)
            result["source_file"] = str(path)
            articles.append(result)

    report = {
        "article_count": len(articles),
        "failure_count": sum(1 for article in articles if not article["ok"]),
        "all_ok": all(article["ok"] for article in articles),
        "articles": articles,
    }
    output = json.dumps(report, ensure_ascii=False, indent=2)
    print(output)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(output + "\n", encoding="utf-8")
    if not report["all_ok"]:
        raise SystemExit("VEVO HTML safety guard failed")


if __name__ == "__main__":
    main()
