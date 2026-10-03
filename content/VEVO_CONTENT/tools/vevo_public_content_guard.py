import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
IMPORTS_DIR = ROOT / "content" / "VEVO_CONTENT" / "imports"
EXPORTS_DIR = ROOT / "content" / "VEVO_CONTENT" / "exports"
DEFAULT_REPORT = EXPORTS_DIR / "internal-public-terms-audit-latest.json"

PUBLIC_FIELDS = ("title", "short", "long")

FORBIDDEN_RE = re.compile(
    r"longtail|long-tail|long tail|"
    r"\bkeyword(?:s)?\b|kľúčov\w*\s+slov\w*|klucov\w*\s+slov\w*|"
    r"\bSEO\b|seo\s+z[aá]mer|search\s+intent|"
    r"sub[- ]?quer(?:y|ies)|sub[- ]?query|fan[- ]?out|fanout|"
    r"cielene\s+pokr[yý]vame|cielene\s+odpoved[aá]|cielime\s+aj\s+longtail",
    re.IGNORECASE,
)
CTA_RE = re.compile(r"\bCTA\b")


def _replace_paragraphs(html):
    replacements = [
        (
            re.compile(
                r"<p>\s*Longtail sekcie pokrývajú výrazy ako\s+(.*?)\.\s+(.*?)</p>",
                re.IGNORECASE | re.DOTALL,
            ),
            r"<p>V článku nájdete aj praktické situácie, ktoré sa pri tejto téme často riešia: \1. \2</p>",
        ),
        (
            re.compile(
                r"<p>\s*Longtail výrazy:\s*(.*?)\.\s+(.*?)</p>",
                re.IGNORECASE | re.DOTALL,
            ),
            r"<p>V článku nájdete aj súvisiace praktické situácie: \1. \2</p>",
        ),
        (
            re.compile(
                r"<p>\s*V článku cielime aj longtail výrazy ako\s+<strong>(.*?)</strong>\.\s+(.*?)</p>",
                re.IGNORECASE | re.DOTALL,
            ),
            r"<p>V článku nájdete aj praktické otázky z praxe: <strong>\1</strong>. \2</p>",
        ),
        (
            re.compile(
                r"<p>\s*V článku cielene pokrývame aj longtail výrazy:\s*<strong>(.*?)</strong>\.\s+(.*?)</p>",
                re.IGNORECASE | re.DOTALL,
            ),
            r"<p>Okrem hlavnej odpovede rozoberáme aj praktické situácie z domácnosti: <strong>\1</strong>. \2</p>",
        ),
        (
            re.compile(
                r"<p>\s*Článok cielene odpovedá aj na praktické longtail otázky, ktoré ľudia často riešia samostatne\.\s*</p>",
                re.IGNORECASE,
            ),
            "<p>Nižšie nájdete praktické odpovede aj na otázky, ktoré ľudia pri praní často riešia samostatne.</p>",
        ),
    ]
    for pattern, replacement in replacements:
        html = pattern.sub(replacement, html)
    return html


def sanitize_text(value):
    if not isinstance(value, str):
        return value

    text = value
    text = re.sub(
        r"\n?<!--\s*VEVO[^>]*(?:fanout|sales block|expert expansion|quality)[^>]*-->\n?",
        "\n",
        text,
        flags=re.IGNORECASE,
    )
    text = _replace_paragraphs(text)
    text = re.sub(r"\bLongtail otázky\b", "Praktické otázky", text)
    text = re.sub(r"\blongtail otázky\b", "praktické otázky", text)
    text = re.sub(r"\bLongtail výrazy\b", "Praktické situácie", text)
    text = re.sub(r"\blongtail výrazy\b", "praktické situácie", text)
    text = re.sub(r"\bLongtail sekcie\b", "Praktické časti", text)
    text = re.sub(r"\blongtail sekcie\b", "praktické časti", text)
    text = re.sub(r"\bLongtail\b", "Praktické", text)
    text = re.sub(r"\blongtail\b", "praktické", text)
    text = re.sub(r"V článku cielene pokrývame aj praktické výrazy:", "Okrem hlavnej odpovede rozoberáme aj praktické situácie:", text)
    text = re.sub(r"Článok cielene odpovedá", "Článok odpovedá", text)
    text = re.sub(r"Cieľom je vysvetliť", "Dôležité je vysvetliť", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip() if value.strip() == value else text


def load_mappings():
    mappings = {}
    for path in sorted(EXPORTS_DIR.glob("batch-*-mapping.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if isinstance(data, list):
            posts = data
        elif isinstance(data, dict):
            posts = data.get("posts") or data.get("articles") or data.get("records") or []
        else:
            posts = []
        for post in posts:
            slug = post.get("slug") or post.get("link")
            post_id = post.get("id") or post.get("post_id") or post.get("news_id")
            if slug and post_id:
                mappings[slug] = {
                    "post_id": str(post_id),
                    "url": post.get("url") or f"https://www.vevo.sk/n/{slug}",
                    "mapping_file": str(path.relative_to(ROOT)),
                }
    return mappings


def article_priority(path):
    name = path.name
    if "quality-update" in name:
        return 20
    if name.endswith("-articles.json"):
        return 10
    return 0


def iter_article_json(paths):
    for path in paths:
        try:
            root = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise SystemExit(f"Invalid JSON in {path}: {exc}") from exc
        if isinstance(root, list):
            articles = root
        elif isinstance(root, dict) and isinstance(root.get("updates"), list):
            articles = root["updates"]
        elif isinstance(root, dict) and isinstance(root.get("articles"), list):
            articles = root["articles"]
        else:
            continue
        if not all(isinstance(item, dict) for item in articles):
            continue
        if not any(any(field in item for field in PUBLIC_FIELDS) for item in articles):
            continue
        yield path, root, articles


def find_hits(article):
    hits = {}
    for field in PUBLIC_FIELDS:
        value = article.get(field)
        if not isinstance(value, str):
            continue
        found = {match.group(0) for match in FORBIDDEN_RE.finditer(value)}
        found.update(match.group(0) for match in CTA_RE.finditer(value))
        found = sorted(found, key=str.lower)
        if found:
            hits[field] = found
    return hits


def forbidden_terms(value):
    if not isinstance(value, str):
        return []
    found = {match.group(0) for match in FORBIDDEN_RE.finditer(value)}
    found.update(match.group(0) for match in CTA_RE.finditer(value))
    return sorted(found, key=str.lower)


def process(paths, fix=False):
    mappings = load_mappings()
    changed_articles = []
    remaining = []
    files_changed = []

    for path, root, articles in iter_article_json(paths):
        file_changed = False
        for index, article in enumerate(articles):
            before = {field: article.get(field) for field in PUBLIC_FIELDS}
            fields_changed = []
            for field in PUBLIC_FIELDS:
                cleaned = sanitize_text(article.get(field))
                if cleaned != article.get(field):
                    article[field] = cleaned
                    fields_changed.append(field)
            after_hits = find_hits(article)
            slug = article.get("link") or article.get("slug")
            if not slug and isinstance(article.get("url"), str) and "/n/" in article["url"]:
                slug = article["url"].rstrip("/").rsplit("/n/", 1)[-1]
            title = article.get("title", "")
            if fields_changed:
                file_changed = True
                mapping = mappings.get(slug, {})
                changed_articles.append(
                    {
                        "source_file": str(path.relative_to(ROOT)),
                        "source_priority": article_priority(path),
                        "index": index,
                        "title": title,
                        "slug": slug,
                        "post_id": str(article.get("post_id") or mapping.get("post_id") or ""),
                        "url": article.get("url") or mapping.get("url"),
                        "fields_changed": fields_changed,
                    }
                )
            if after_hits:
                remaining.append(
                    {
                        "source_file": str(path.relative_to(ROOT)),
                        "index": index,
                        "title": title,
                        "slug": slug,
                        "hits": after_hits,
                    }
                )
            if not fix:
                for field, value in before.items():
                    article[field] = value

        if fix and file_changed:
            path.write_text(json.dumps(root, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            files_changed.append(str(path.relative_to(ROOT)))

    # Deduplicate by slug for live update; the quality-update file is the source for batch 26.
    live_candidates = {}
    for item in changed_articles:
        slug = item.get("slug")
        if not slug:
            continue
        current = live_candidates.get(slug)
        if current is None or item["source_priority"] >= current["source_priority"]:
            live_candidates[slug] = item

    report = {
        "project": "VEVO_CONTENT",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "fix_applied": fix,
        "files_changed": files_changed,
        "changed_article_count": len(changed_articles),
        "live_candidate_count": len(live_candidates),
        "changed_articles": changed_articles,
        "live_candidates": sorted(live_candidates.values(), key=lambda item: (item["source_file"], item["index"])),
        "remaining_hits": remaining,
        "remaining_hit_count": len(remaining),
    }
    return report


def default_paths():
    return sorted(IMPORTS_DIR.glob("*.json"))


def main():
    parser = argparse.ArgumentParser(description="Guard VEVO public article text against internal SEO/workflow wording.")
    parser.add_argument("--fix", action="store_true", help="Rewrite article JSON files with public-safe phrasing.")
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT, help="Write JSON audit report.")
    parser.add_argument("paths", nargs="*", type=Path, help="Specific JSON files to scan. Defaults to all VEVO import JSON files.")
    args = parser.parse_args()

    paths = args.paths or default_paths()
    report = process(paths, fix=args.fix)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "fix_applied": args.fix,
                "files_changed": len(report["files_changed"]),
                "changed_article_count": report["changed_article_count"],
                "live_candidate_count": report["live_candidate_count"],
                "remaining_hit_count": report["remaining_hit_count"],
                "report": str(args.report),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    if report["remaining_hits"]:
        raise SystemExit("Public article text still contains internal wording")


if __name__ == "__main__":
    main()
