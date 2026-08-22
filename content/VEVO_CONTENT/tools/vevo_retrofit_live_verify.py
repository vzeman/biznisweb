import argparse
import html
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import requests

from vevo_public_content_guard import forbidden_terms


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_REPORT = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-live-verification-latest.json"
BASE_URL = "https://www.vevo.sk"
INTERNAL_HREF_RE = re.compile(r'href="(/(?:n|c|p|casto-kladene-dotazy)[^"#?]*)"', re.IGNORECASE)


def load_articles(path):
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict) and isinstance(data.get("updates"), list):
        return data["updates"]
    if isinstance(data, list):
        return data
    raise SystemExit(f"Unsupported retrofit file structure: {path}")


def article_hrefs(article):
    long = article.get("long") or ""
    return sorted(set(INTERNAL_HREF_RE.findall(long)))


def check_link(session, href):
    url = urljoin(BASE_URL, href)
    try:
        response = session.get(url, timeout=20, allow_redirects=True)
    except requests.RequestException as exc:
        return {"href": href, "url": url, "ok": False, "error": str(exc)}
    return {
        "href": href,
        "url": url,
        "status": response.status_code,
        "final_url": response.url,
        "ok": response.status_code < 400,
    }


def verify_article(session, article, required_fragments):
    url = article.get("url") or urljoin(BASE_URL, f"/n/{article.get('slug') or article.get('link')}")
    response = session.get(url, timeout=30, headers={"Cache-Control": "no-cache"})
    page = html.unescape(response.text)
    source_hrefs = article_hrefs(article)
    product_hrefs = [href for href in source_hrefs if href.startswith("/p-")]
    category_hrefs = [href for href in source_hrefs if href.startswith("/c/")]
    missing_hrefs = [href for href in product_hrefs + category_hrefs if href not in page]
    missing_fragments = [fragment for fragment in required_fragments if fragment not in page]
    title = article.get("title") or ""
    title_present = not title or title in page
    hits = forbidden_terms(page)

    ok = (
        response.status_code == 200
        and title_present
        and not missing_fragments
        and not missing_hrefs
        and not hits
        and bool(product_hrefs)
        and bool(category_hrefs)
    )
    return {
        "post_id": str(article.get("post_id") or ""),
        "slug": article.get("slug") or article.get("link") or "",
        "url": url,
        "status": response.status_code,
        "final_url": response.url,
        "title_present": title_present,
        "required_fragments": required_fragments,
        "missing_fragments": missing_fragments,
        "product_hrefs": product_hrefs,
        "category_hrefs": category_hrefs,
        "missing_product_or_category_hrefs": missing_hrefs,
        "forbidden_hits": hits,
        "ok": ok,
    }


def main():
    parser = argparse.ArgumentParser(description="Verify published VEVO retrofit articles.")
    parser.add_argument("wave_file", type=Path, help="Retrofit wave JSON with updates.")
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument(
        "--required-fragment",
        action="append",
        default=[],
        help="Fragment that must appear on every live article page. Can be passed more than once.",
    )
    parser.add_argument("--skip-link-check", action="store_true")
    args = parser.parse_args()

    articles = load_articles(args.wave_file)
    session = requests.Session()
    article_results = [verify_article(session, article, args.required_fragment) for article in articles]

    link_results = []
    if not args.skip_link_check:
        hrefs = sorted({href for article in articles for href in article_hrefs(article)})
        link_results = [check_link(session, href) for href in hrefs]

    all_ok = all(item["ok"] for item in article_results) and all(item["ok"] for item in link_results)
    report = {
        "project": "VEVO_CONTENT",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "wave_file": str(args.wave_file),
        "article_count": len(article_results),
        "link_count": len(link_results),
        "all_ok": all_ok,
        "articles": article_results,
        "links": link_results,
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"all_ok": all_ok, "articles": len(article_results), "links": len(link_results), "report": str(args.report)}, ensure_ascii=False, indent=2))
    if not all_ok:
        raise SystemExit("VEVO retrofit live verification failed")


if __name__ == "__main__":
    main()
