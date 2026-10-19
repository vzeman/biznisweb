import argparse
import html
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
IMPORTS_DIR = ROOT / "content" / "VEVO_CONTENT" / "imports"
EXPORTS_DIR = ROOT / "content" / "VEVO_CONTENT" / "exports"

TAG_RE = re.compile(r"<[^>]+>")
WORD_RE = re.compile(r"[A-Za-zÀ-ž0-9]+(?:[-'][A-Za-zÀ-ž0-9]+)?")
FORBIDDEN_PUBLIC_RE = re.compile(
    r"longtail|long-tail|long tail|"
    r"\bkeyword(?:s)?\b|\bSEO\b|search\s+intent|sub[- ]?query|fan[- ]?out|fanout|"
    r"cielene\s+pokr[yý]vame|cielene\s+odpoved[aá]",
    re.IGNORECASE,
)
CTA_RE = re.compile(r"\bCTA\b")


def visible_text(markup):
    text = re.sub(r"(?is)<script.*?</script>|<style.*?</style>", " ", markup or "")
    text = TAG_RE.sub(" ", text)
    return html.unescape(re.sub(r"\s+", " ", text)).strip()


def word_count(markup):
    return len(WORD_RE.findall(visible_text(markup)))


def load_json(path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def load_mappings():
    mappings = {}
    for path in sorted(EXPORTS_DIR.glob("batch-*-mapping.json")):
        data = load_json(path)
        if isinstance(data, list):
            posts = data
        elif isinstance(data, dict):
            posts = data.get("posts") or data.get("articles") or data.get("records") or []
        else:
            posts = []
        for post in posts:
            if not isinstance(post, dict):
                continue
            slug = post.get("slug") or post.get("link")
            post_id = post.get("id") or post.get("post_id") or post.get("news_id")
            if slug and post_id:
                mappings[slug] = {
                    "post_id": str(post_id),
                    "url": post.get("url") or f"https://www.vevo.sk/n/{slug}",
                    "mapping_file": str(path.relative_to(ROOT)),
                }
    return mappings


def article_list(root):
    if isinstance(root, list):
        return root
    if isinstance(root, dict):
        for key in ("articles", "updates"):
            if isinstance(root.get(key), list):
                return root[key]
    return []


def slug_for(article):
    slug = article.get("link") or article.get("slug")
    if not slug and isinstance(article.get("url"), str) and "/n/" in article["url"]:
        slug = article["url"].rstrip("/").rsplit("/n/", 1)[-1]
    return slug or ""


def source_priority(path):
    name = path.name
    if "quality-update" in name or name.startswith("update-"):
        return 30
    if name.endswith("-articles.json"):
        return 20
    return 10


def iter_sources():
    for path in sorted(IMPORTS_DIR.glob("*.json")):
        root = load_json(path)
        articles = article_list(root)
        if not articles:
            continue
        for index, article in enumerate(articles):
            if isinstance(article, dict) and any(key in article for key in ("title", "short", "long")):
                yield path, index, article


def metrics_for(path, index, article, mapping):
    long = article.get("long") or ""
    title = article.get("title") or ""
    slug = slug_for(article)
    hrefs = re.findall(r'href="([^"]+)"', long)
    forbidden = sorted(
        {match.group(0) for match in FORBIDDEN_PUBLIC_RE.finditer(long)}
        | {match.group(0) for match in CTA_RE.finditer(long)}
    )
    metrics = {
        "title": title,
        "slug": slug,
        "post_id": mapping.get("post_id"),
        "url": article.get("url") or mapping.get("url"),
        "source_file": str(path.relative_to(ROOT)),
        "source_index": index,
        "source_priority": source_priority(path),
        "words": word_count(long),
        "h2_count": len(re.findall(r"<h2\b", long, re.IGNORECASE)),
        "h3_count": len(re.findall(r"<h3\b", long, re.IGNORECASE)),
        "table_count": len(re.findall(r"<table\b", long, re.IGNORECASE)),
        "styled_block_count": len(re.findall(r"<div[^>]+style=", long, re.IGNORECASE)),
        "product_link_count": sum(1 for href in hrefs if href.startswith("/p-")),
        "category_link_count": sum(1 for href in hrefs if href.startswith("/c/")),
        "article_link_count": sum(1 for href in hrefs if href.startswith("/n/")),
        "external_link_count": sum(1 for href in hrefs if href.startswith("http")),
        "has_quick_answer": "Rýchla odpoveď" in long or "Rychla odpoved" in long,
        "has_fixed_price": bool(re.search(r"(\d+[,.]\d{1,2}\s*€|Cena:)", long)),
        "forbidden_public_terms": forbidden,
    }
    metrics["cluster"] = cluster_for(metrics)
    metrics["commercial_priority"] = commercial_priority(metrics)
    metrics["tier"] = classify(metrics)
    metrics["recommended_action"] = recommended_action(metrics)
    return metrics


def cluster_for(metrics):
    haystack = f"{metrics.get('title', '')} {metrics.get('slug', '')}".lower()
    if any(term in haystack for term in ("prack", "praci-gel", "praci gel", "davkov", "oplach", "odstred", "program", "bubon")):
        return "core_laundry_process"
    if any(term in haystack for term in ("zapach", "vona", "vôňa", "parfum", "zatuch", "smrd")):
        return "odor_fragrance"
    if any(term in haystack for term in ("uterak", "uterák", "obliec", "postel", "plachta", "deka", "matrac")):
        return "bedding_towels"
    if any(
        term in haystack
        for term in (
            "polyester",
            "bavlna",
            "viskoz",
            "modal",
            "lyocell",
            "softshell",
            "fleece",
            "vlna",
            "akryl",
            "elastan",
            "polyamid",
            "material",
            "materiál",
        )
    ):
        return "materials_textiles"
    if any(term in haystack for term in ("dets", "body", "pyzam", "podbrad", "skol", "škôl")):
        return "kids_household"
    if any(term in haystack for term in ("makeup", "ruz", "rúž", "krem", "maskar", "lak", "parfumovy-flak")):
        return "cosmetics_stains"
    if any(term in haystack for term in ("olej", "maslo", "vajick", "caj", "kava", "vino", "ovoc", "omack", "kari", "kurkum")):
        return "food_stains"
    return "specific_stains_and_care"


def commercial_priority(metrics):
    base = {
        "core_laundry_process": 100,
        "odor_fragrance": 92,
        "bedding_towels": 88,
        "materials_textiles": 82,
        "kids_household": 74,
        "cosmetics_stains": 66,
        "food_stains": 58,
        "specific_stains_and_care": 50,
    }.get(metrics["cluster"], 50)
    if metrics["product_link_count"] == 0 or metrics["category_link_count"] == 0:
        base += 6
    if metrics["words"] < 800:
        base += 4
    return base


def classify(metrics):
    if metrics["forbidden_public_terms"]:
        return "fix_public_terms"
    if metrics["words"] < 800:
        return "major_expand"
    if metrics["words"] < 1200:
        return "medium_expand"
    if metrics["words"] < 1500:
        return "light_expand"
    if metrics["table_count"] < 2 or metrics["product_link_count"] < 1 or metrics["category_link_count"] < 1:
        return "structure_polish"
    return "watch"


def recommended_action(metrics):
    tier = metrics["tier"]
    if tier == "major_expand":
        return "Add 800-1200 words, diagnosis table, prevention section, product card, category card, FAQ."
    if tier == "medium_expand":
        return "Add 500-800 words, second practical table, prevention/caution section, stronger sales blocks."
    if tier == "light_expand":
        return "Add 300-500 words and one missing practical block without changing existing core text."
    if tier == "structure_polish":
        return "Keep text mostly unchanged; add missing table or product/category card."
    if tier == "fix_public_terms":
        return "Fix internal wording before any other retrofit."
    return "No urgent expansion; revisit only when cluster needs consolidation."


def build_inventory():
    mappings = load_mappings()
    by_slug = {}
    for path, index, article in iter_sources():
        slug = slug_for(article)
        if not slug:
            continue
        mapping = mappings.get(slug, {})
        item = metrics_for(path, index, article, mapping)
        current = by_slug.get(slug)
        if current is None or item["source_priority"] >= current["source_priority"]:
            by_slug[slug] = item
    articles = sorted(
        by_slug.values(),
        key=lambda item: (-item["commercial_priority"], tier_order(item["tier"]), item["words"], item["slug"]),
    )
    return articles


def tier_order(tier):
    order = {
        "fix_public_terms": 0,
        "major_expand": 1,
        "medium_expand": 2,
        "light_expand": 3,
        "structure_polish": 4,
        "watch": 5,
    }
    return order.get(tier, 9)


def markdown_report(report):
    lines = [
        "# VEVO Conservative Retrofit Priority",
        "",
        f"Generated: {report['generated_at']}",
        f"Article count: {report['article_count']}",
        "",
        "## Summary",
        "",
    ]
    for tier, count in report["tiers"].items():
        lines.append(f"- {tier}: {count}")
    lines.extend(["", "## Clusters", ""])
    for cluster, count in report["clusters"].items():
        lines.append(f"- {cluster}: {count}")
    lines.extend(
        [
            "",
            "## First Waves",
            "",
            "Use 2 to 3 articles per wave. Keep URL, title, short description, publish date, and existing core content stable.",
            "",
        ]
    )
    for wave_index, start in enumerate(range(0, min(30, len(report["articles"])), 3), start=1):
        lines.append(f"### Wave {wave_index}")
        lines.append("")
        for item in report["articles"][start : start + 3]:
            lines.append(
                f"- `priority {item['commercial_priority']}` `{item['cluster']}` `{item['tier']}` `{item['words']} words` [{item['title']}]({item.get('url') or item['slug']})"
            )
            lines.append(f"  - Action: {item['recommended_action']}")
            lines.append(f"  - Source: `{item['source_file']}`")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def main():
    parser = argparse.ArgumentParser(description="Inventory VEVO articles for conservative expansion retrofit.")
    parser.add_argument("--out", type=Path, default=EXPORTS_DIR / "retrofit-inventory-latest.json")
    parser.add_argument("--markdown", type=Path)
    args = parser.parse_args()

    articles = build_inventory()
    report = {
        "project": "VEVO_CONTENT",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "article_count": len(articles),
        "tiers": dict(Counter(item["tier"] for item in articles)),
        "clusters": dict(Counter(item["cluster"] for item in articles)),
        "articles": articles,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if args.markdown:
        args.markdown.parent.mkdir(parents=True, exist_ok=True)
        args.markdown.write_text(markdown_report(report), encoding="utf-8")
    print(
        json.dumps(
            {
                "article_count": report["article_count"],
                "tiers": report["tiers"],
                "clusters": report["clusters"],
                "out": str(args.out),
                "markdown": str(args.markdown) if args.markdown else None,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
