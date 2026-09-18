import argparse
import html
import json
import re
from pathlib import Path


TAG_RE = re.compile(r"<[^>]+>")
WORD_RE = re.compile(r"[^\W_]+(?:[-'][^\W_]+)?", re.UNICODE)
P_RE = re.compile(r"(?is)<p\b[^>]*>(.*?)</p>")


def visible_text(markup):
    text = re.sub(r"(?is)<script.*?</script>|<style.*?</style>", " ", markup)
    text = TAG_RE.sub(" ", text)
    return html.unescape(re.sub(r"\s+", " ", text)).strip()


def word_count(markup):
    return len(WORD_RE.findall(visible_text(markup)))


def max_short_paragraph_run(markup):
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


def article_metrics(article):
    long = article.get("long") or ""
    max_short_run, short_paragraph_count = max_short_paragraph_run(long)
    return {
        "title": article.get("title", ""),
        "slug": article.get("link") or article.get("slug") or "",
        "words": word_count(long),
        "h2_count": len(re.findall(r"<h2\b", long, re.IGNORECASE)),
        "table_count": len(re.findall(r"<table\b", long, re.IGNORECASE)),
        "styled_block_count": len(re.findall(r"<div[^>]+style=", long, re.IGNORECASE)),
        "product_link_count": len(re.findall(r'href="(?:https://www\.vevo\.sk)?/p-', long)),
        "category_link_count": len(re.findall(r'href="(?:https://www\.vevo\.sk)?/c/', long)),
        "faq_question_count": len(re.findall(r"<h3\b", long, re.IGNORECASE)),
        "max_short_paragraph_run": max_short_run,
        "short_paragraph_count": short_paragraph_count,
    }


def load_articles(path):
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("articles", "updates"):
            if isinstance(data.get(key), list):
                return data[key]
    raise SystemExit(f"Unsupported article JSON structure: {path}")


def check_article(metrics, args):
    failures = []
    if metrics["words"] < args.min_words:
        failures.append(f"words {metrics['words']} < {args.min_words}")
    if metrics["h2_count"] < args.min_h2:
        failures.append(f"h2 {metrics['h2_count']} < {args.min_h2}")
    if metrics["table_count"] < args.min_tables:
        failures.append(f"tables {metrics['table_count']} < {args.min_tables}")
    if metrics["styled_block_count"] < args.min_styled_blocks:
        failures.append(f"styled blocks {metrics['styled_block_count']} < {args.min_styled_blocks}")
    if metrics["product_link_count"] < args.min_product_links:
        failures.append(f"product links {metrics['product_link_count']} < {args.min_product_links}")
    if metrics["category_link_count"] < args.min_category_links:
        failures.append(f"category links {metrics['category_link_count']} < {args.min_category_links}")
    if metrics["faq_question_count"] < args.min_faq_questions:
        failures.append(f"FAQ questions {metrics['faq_question_count']} < {args.min_faq_questions}")
    if metrics["max_short_paragraph_run"] >= args.max_short_paragraph_run:
        failures.append(
            f"short paragraph run {metrics['max_short_paragraph_run']} >= {args.max_short_paragraph_run}"
        )
    if metrics["short_paragraph_count"] > args.max_short_paragraph_count:
        failures.append(
            f"short paragraph count {metrics['short_paragraph_count']} > {args.max_short_paragraph_count}"
        )
    return failures


def main():
    parser = argparse.ArgumentParser(description="Check depth and structure of new VEVO article batches.")
    parser.add_argument("paths", nargs="+", type=Path, help="Article JSON files to check.")
    parser.add_argument("--report", type=Path, help="Optional JSON report path.")
    parser.add_argument("--min-words", type=int, default=1500)
    parser.add_argument("--min-h2", type=int, default=8)
    parser.add_argument("--min-tables", type=int, default=2)
    parser.add_argument("--min-styled-blocks", type=int, default=4)
    parser.add_argument("--min-product-links", type=int, default=1)
    parser.add_argument("--min-category-links", type=int, default=1)
    parser.add_argument("--min-faq-questions", type=int, default=3)
    parser.add_argument("--max-short-paragraph-run", type=int, default=8)
    parser.add_argument("--max-short-paragraph-count", type=int, default=20)
    args = parser.parse_args()

    failures = []
    all_metrics = []
    for path in args.paths:
        for index, article in enumerate(load_articles(path), start=1):
            metrics = article_metrics(article)
            metrics["source_file"] = str(path)
            metrics["index"] = index
            article_failures = check_article(metrics, args)
            metrics["ok"] = not article_failures
            metrics["failures"] = article_failures
            all_metrics.append(metrics)
            if article_failures:
                failures.append(metrics)

    result = {
        "article_count": len(all_metrics),
        "failure_count": len(failures),
        "articles": all_metrics,
    }
    rendered = json.dumps(result, ensure_ascii=False, indent=2)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    if failures:
        raise SystemExit("VEVO article depth guard failed")


if __name__ == "__main__":
    main()
