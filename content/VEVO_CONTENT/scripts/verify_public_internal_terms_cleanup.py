import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path

import requests


ROOT = Path(__file__).resolve().parents[3]
GUARD_PATH = ROOT / "content" / "VEVO_CONTENT" / "tools" / "vevo_public_content_guard.py"
SOURCE_REPORT = ROOT / "content" / "VEVO_CONTENT" / "exports" / "internal-public-terms-cleanup-2026-06-16-source.json"
OUT_JSON = ROOT / "content" / "VEVO_CONTENT" / "exports" / "internal-public-terms-cleanup-2026-06-16-live-verification.json"
BASE_URL = "https://www.vevo.sk"
SITEMAP_CACHE = None


def load_guard():
    spec = importlib.util.spec_from_file_location("vevo_public_content_guard", GUARD_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


GUARD = load_guard()


def fetch(url):
    response = requests.get(
        url,
        headers={"User-Agent": "Codex VEVO internal wording cleanup verification"},
        timeout=45,
        allow_redirects=True,
    )
    return response, response.content.decode("utf-8", errors="replace")


def sitemap_urls():
    global SITEMAP_CACHE
    if SITEMAP_CACHE is not None:
        return SITEMAP_CACHE
    response, text = fetch(f"{BASE_URL}/sitemap.xml")
    if response.status_code != 200:
        SITEMAP_CACHE = set()
    else:
        SITEMAP_CACHE = set(part.strip() for part in text.split("<loc>") if "</loc>" in part)
        SITEMAP_CACHE = {part.split("</loc>", 1)[0] for part in SITEMAP_CACHE}
    return SITEMAP_CACHE


def alternate_public_urls(url):
    if "/n/" not in url:
        return []
    base, slug = url.rsplit("/n/", 1)
    slug = slug.strip("/")
    variants = {
        slug.replace("makk", "maekk"),
        slug.replace("mak", "maek"),
    }
    variants.discard(slug)
    known_urls = sitemap_urls()
    return [f"{base}/n/{variant}" for variant in sorted(variants) if f"{base}/n/{variant}" in known_urls]


def article_segment(page_source):
    starts = [
        "<strong>Rýchla odpoveď:",
        "Rýchla odpoveď",
        '<div class="s1-newsDetail"',
        "<article",
    ]
    start = -1
    for marker in starts:
        start = page_source.find(marker)
        if start != -1:
            break
    if start == -1:
        return page_source

    end_candidates = [
        page_source.find("Diskusia", start),
        page_source.find("Pridať komentár", start),
        page_source.find("s1-newsNavigation", start),
        page_source.find("s1-socialShare", start),
        page_source.find("vevo-sticky-cta", start),
        page_source.find("</main>", start),
    ]
    end_candidates = [item for item in end_candidates if item != -1]
    end = min(end_candidates) if end_candidates else start + 90000
    return page_source[start:end]


def urls_to_verify():
    mappings = GUARD.load_mappings()
    urls = {
        slug: {"slug": slug, "post_id": data.get("post_id"), "url": data.get("url")}
        for slug, data in mappings.items()
        if data.get("url")
    }
    if SOURCE_REPORT.exists():
        report = json.loads(SOURCE_REPORT.read_text(encoding="utf-8"))
        for item in report.get("live_candidates", []):
            if item.get("slug") and item.get("url"):
                urls[item["slug"]] = {
                    "slug": item["slug"],
                    "post_id": item.get("post_id"),
                    "url": item["url"],
                    "cleanup_candidate": True,
                }
    return sorted(urls.values(), key=lambda item: item["slug"])


def main():
    results = []
    for item in urls_to_verify():
        requested_url = item["url"]
        response, page = fetch(requested_url)
        resolved_from_sitemap = None
        if response.status_code == 404:
            for alternate_url in alternate_public_urls(requested_url):
                alternate_response, alternate_page = fetch(alternate_url)
                if alternate_response.status_code == 200:
                    response, page = alternate_response, alternate_page
                    resolved_from_sitemap = alternate_url
                    break
        segment = article_segment(page)
        hits = GUARD.forbidden_terms(segment)
        results.append(
            {
                "slug": item["slug"],
                "post_id": item.get("post_id"),
                "url": requested_url,
                "resolved_from_sitemap": resolved_from_sitemap,
                "status": response.status_code,
                "final_url": response.url,
                "cleanup_candidate": bool(item.get("cleanup_candidate")),
                "forbidden_hits": hits,
                "reachable": response.status_code == 200,
                "public_text_ok": not hits,
                "ok": response.status_code == 200 and not hits,
            }
        )

    output = {
        "project": "VEVO_CONTENT",
        "verified_at": datetime.now(timezone.utc).isoformat(),
        "url_count": len(results),
        "cleanup_candidate_count": sum(1 for item in results if item["cleanup_candidate"]),
        "all_public_text_ok": all(item["public_text_ok"] for item in results),
        "all_reachable": all(item["reachable"] for item in results),
        "unreachable_count": sum(1 for item in results if not item["reachable"]),
        "all_ok": all(item["ok"] for item in results),
        "results": results,
    }
    OUT_JSON.write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "url_count": output["url_count"],
                "cleanup_candidate_count": output["cleanup_candidate_count"],
                "all_public_text_ok": output["all_public_text_ok"],
                "all_reachable": output["all_reachable"],
                "unreachable_count": output["unreachable_count"],
                "all_ok": output["all_ok"],
                "verification": str(OUT_JSON),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    if not output["all_public_text_ok"]:
        failed = [item for item in results if not item["public_text_ok"]]
        raise SystemExit(f"Live public wording verification failed for {len(failed)} URLs")


if __name__ == "__main__":
    main()
