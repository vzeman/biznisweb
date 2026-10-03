import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import requests


BASE_URL = "https://www.vevo.sk"
ARTICLES_JSON = Path("content/VEVO_CONTENT/imports/batch-27-2026-06-16-articles.json")
MAPPING_JSON = Path("content/VEVO_CONTENT/exports/batch-27-2026-06-16-mapping.json")
OUT_JSON = Path("content/VEVO_CONTENT/exports/batch-27-2026-06-16-verification.json")


def fetch(url):
    response = requests.get(
        url,
        headers={"User-Agent": "Codex VEVO batch 27 public verification"},
        timeout=45,
        allow_redirects=True,
    )
    text = response.content.decode("utf-8", errors="replace")
    return response, text


def article_segment(page_source):
    start = page_source.find("<strong>Rýchla odpoveď:")
    if start == -1:
        start = page_source.find("Rýchla odpoveď")
    if start == -1:
        return page_source
    end_candidates = [
        page_source.find("Diskusia", start),
        page_source.find("Pridať komentár", start),
        page_source.find("s1-newsNavigation", start),
        page_source.find("s1-socialShare", start),
        page_source.find("</main>", start),
    ]
    end_candidates = [item for item in end_candidates if item != -1]
    end = min(end_candidates) if end_candidates else start + 70000
    return page_source[start:end]


def check_links(articles):
    hrefs = sorted({href for article in articles for href in re.findall(r'href="([^"]+)"', article["long"])})
    checks = []
    for href in hrefs:
        if href.startswith("/"):
            url = urljoin(BASE_URL, href)
        elif href.startswith("http"):
            url = href
        else:
            continue
        response, _ = fetch(url)
        checks.append(
            {
                "href": href,
                "url": url,
                "status": response.status_code,
                "final_url": response.url,
                "ok": response.status_code == 200,
            }
        )
    return checks


def main():
    articles = json.loads(ARTICLES_JSON.read_text(encoding="utf-8"))
    mapping = json.loads(MAPPING_JSON.read_text(encoding="utf-8"))
    posts_by_slug = {post["slug"]: post for post in mapping["posts"]}

    results = []
    for article in articles:
        post = posts_by_slug[article["link"]]
        response, page = fetch(post["url"])
        segment = article_segment(page)
        date_match = re.search(r'datePublished[^>]+content="([^"]+)"', page)
        expected_date = f'{article["date_posted"]}T{article["time_posted"]}'
        product_hrefs = sorted({href for href in re.findall(r'href="([^"]+)"', article["long"]) if href.startswith("/p-")})
        category_hrefs = sorted({href for href in re.findall(r'href="([^"]+)"', article["long"]) if href.startswith("/c/")})
        checks = {
            "post_id": post["id"],
            "url": post["url"],
            "status": response.status_code,
            "final_url": response.url,
            "title_present": article["title"] in page,
            "date_published": date_match.group(1) if date_match else None,
            "date_matches_expected": bool(date_match and date_match.group(1) == expected_date),
            "quick_answer_present": "Rýchla odpoveď" in segment,
            "fanout_heading_present": "Čo v článku nájdete" in segment,
            "diagnostic_table_present": "border-collapse: collapse" in segment,
            "product_block_present": "Kedy dáva zmysel:" in segment and "Kedy najprv riešiť príčinu:" in segment,
            "styled_cards_present": all(marker in segment for marker in ["#f7fbff", "#fffaf5", "#f7fbf8"]),
            "product_hrefs_present": all(href in segment for href in product_hrefs),
            "category_hrefs_present": all(href in segment for href in category_hrefs),
            "no_internal_jargon_in_article_source": not re.search(r"\bCTA\b", segment, re.IGNORECASE),
            "no_fixed_prices_in_article_source": "Cena:" not in segment
            and not re.search(r"\d+(?:[,.]\d{1,2})?\s*€", segment),
            "no_escaped_quote_artifacts": '\\"' not in segment and "&quot;" not in segment,
            "source_malformed_href_count": len(re.findall(r'href=(?!")', segment)),
            "expected_article_length": len(article["long"]),
            "product_hrefs": product_hrefs,
            "category_hrefs": category_hrefs,
        }
        checks["ok"] = (
            checks["status"] == 200
            and checks["title_present"]
            and checks["date_matches_expected"]
            and checks["quick_answer_present"]
            and checks["fanout_heading_present"]
            and checks["diagnostic_table_present"]
            and checks["product_block_present"]
            and checks["styled_cards_present"]
            and checks["product_hrefs_present"]
            and checks["category_hrefs_present"]
            and checks["no_internal_jargon_in_article_source"]
            and checks["no_fixed_prices_in_article_source"]
            and checks["no_escaped_quote_artifacts"]
            and checks["source_malformed_href_count"] == 0
        )
        results.append(checks)

    link_checks = check_links(articles)
    output = {
        "batch": "batch-27",
        "verified_at": datetime.now(timezone.utc).isoformat(),
        "post_count": len(results),
        "all_ok": all(item["ok"] for item in results) and all(item["ok"] for item in link_checks),
        "results": results,
        "link_checks": link_checks,
        "links_checked": len(link_checks),
    }
    OUT_JSON.write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "post_count": output["post_count"],
                "all_ok": output["all_ok"],
                "links_checked": output["links_checked"],
                "verification": str(OUT_JSON),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    if not output["all_ok"]:
        raise SystemExit("Batch 27 public verification failed")


if __name__ == "__main__":
    main()
