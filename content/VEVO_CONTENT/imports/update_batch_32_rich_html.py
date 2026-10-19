import argparse
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup


ROOT = Path(__file__).resolve().parents[3]
VEVO_ROOT = ROOT / "content" / "VEVO_CONTENT"
ARTICLES_JSON = VEVO_ROOT / "imports" / "batch-32-2026-07-05-articles.json"
ID_MAP_JSON = VEVO_ROOT / "exports" / "batch-32-2026-07-05-public-id-map.json"
RESULTS_JSON = VEVO_ROOT / "exports" / "batch-32-2026-07-05-rich-html-results.json"
CONFIG_PATH = Path.home() / ".codex" / "config.toml"
BASE_URL = "https://www.vevo.sk"
FORCE_UPDATE_ALL = True

REQUIRED_CATEGORY = "/c/vevo-home-care/upratovanie/cistiace-prostriedky/cistic-do-robotickeho-vysavaca"
REQUIRED_PRODUCT = "/p-1635/vevo-cistic-podlah-pre-vsetky-vysavace-ylang-absolute"
FORBIDDEN_PUBLIC_RE = re.compile(
    r"\bCTA\b|longtail|long tail|keyword|sub-query|fan-out|cielene pokr",
    re.IGNORECASE,
)
P_RE = re.compile(r"(?is)<p\b[^>]*>(.*?)</p>")


def load_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def mcp_url():
    config = CONFIG_PATH.read_text(encoding="utf-8")
    match = re.search(r'(?s)\[mcp_servers\.biznisweb-vevo\]\s*url\s*=\s*"([^"]+)"', config)
    if not match:
        raise SystemExit("biznisweb-vevo MCP URL not found in ~/.codex/config.toml")
    return match.group(1)


def parse_sse_json(text):
    for line in text.splitlines():
        if line.startswith("data: "):
            return json.loads(line[6:])
    raise ValueError(f"No JSON data line in MCP response: {text[:500]}")


def call_update(endpoint, payload, request_id, attempts=4):
    body = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "tools/call",
        "params": {
            "name": "biznisweb-update_news_post",
            "arguments": payload,
        },
    }
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            response = requests.post(
                endpoint,
                json=body,
                headers={"Accept": "application/json, text/event-stream"},
                timeout=120,
            )
            response.raise_for_status()
            parsed = parse_sse_json(response.text)
            if "error" in parsed:
                raise RuntimeError(json.dumps(parsed["error"], ensure_ascii=False))
            result = parsed.get("result", {})
            for item in result.get("content", []):
                if item.get("type") != "text":
                    continue
                try:
                    inner = json.loads(item.get("text", ""))
                except json.JSONDecodeError:
                    continue
                if inner.get("error"):
                    raise RuntimeError(inner["error"])
                if inner.get("success") is False:
                    raise RuntimeError(json.dumps(inner, ensure_ascii=False))
            return parsed
        except Exception as exc:
            last_exc = exc
            if attempt == attempts:
                raise
            time.sleep(2 * attempt)
    raise last_exc


def fetch_html(url):
    response = requests.get(
        f"{url}?_codex_verify={int(time.time() * 1000)}",
        headers={"User-Agent": "Codex VEVO batch 32 verifier"},
        timeout=45,
    )
    return response.status_code, response.text


def main_article_inner(html):
    soup = BeautifulSoup(html, "html.parser")
    divs = soup.select("div.userHTMLContent")
    if not divs:
        return ""
    div = max(divs, key=lambda node: len(str(node)))
    return "".join(str(child) for child in div.contents).strip()


def visible_text(markup):
    soup = BeautifulSoup(markup, "html.parser")
    return " ".join(soup.get_text(" ", strip=True).split())


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


def link_count(markup, target):
    soup = BeautifulSoup(markup, "html.parser")
    targets = {target.rstrip("/")}
    if target.startswith("/"):
        targets.add(f"{BASE_URL}{target}".rstrip("/"))
    elif target.startswith(BASE_URL):
        targets.add(target.removeprefix(BASE_URL).rstrip("/"))
    return sum(1 for anchor in soup.find_all("a") if (anchor.get("href") or "").rstrip("/") in targets)


def verify_article(article, post_id):
    url = f"{BASE_URL}/n/{article['link']}"
    status, html = fetch_html(url)
    inner = main_article_inner(html) if status == 200 else ""
    max_short_run, short_paragraph_count = max_short_paragraph_run(inner)
    required_category_links = link_count(inner, REQUIRED_CATEGORY)
    required_product_links = link_count(inner, REQUIRED_PRODUCT)
    return {
        "post_id": post_id,
        "title": article["title"],
        "slug": article["link"],
        "url": url,
        "status": status,
        "body_length": len(inner),
        "has_title": article["title"] in html,
        "has_quick_answer": "Rýchla odpoveď" in inner,
        "has_required_category": required_category_links >= 1,
        "has_required_product": required_product_links >= 1,
        "required_category_link_count": required_category_links,
        "required_product_link_count": required_product_links,
        "has_styled_blocks": "border: 1px" in inner and "border-radius" in inner,
        "has_escaped_quotes": '\\"' in inner or "&lt;p&gt;" in inner,
        "has_forbidden_public_terms": bool(FORBIDDEN_PUBLIC_RE.search(inner)),
        "max_short_paragraph_run": max_short_run,
        "short_paragraph_count": short_paragraph_count,
        "ok": (
            status == 200
            and len(inner) > 1000
            and article["title"] in html
            and "Rýchla odpoveď" in inner
            and required_category_links >= 1
            and required_product_links >= 1
            and "border: 1px" in inner
            and "border-radius" in inner
            and '\\"' not in inner
            and "&lt;p&gt;" not in inner
            and not FORBIDDEN_PUBLIC_RE.search(inner)
            and max_short_run < 8
            and short_paragraph_count <= 20
        ),
    }


def main():
    parser = argparse.ArgumentParser(description="Legacy batch 32 live rich-HTML repair.")
    parser.add_argument(
        "--execute-live",
        action="store_true",
        help="Explicitly allow updates to existing live VEVO posts.",
    )
    args = parser.parse_args()
    if not args.execute_live:
        raise SystemExit("Refusing live VEVO updates without --execute-live")

    articles = load_json(ARTICLES_JSON)
    id_records = load_json(ID_MAP_JSON)
    ids_by_slug = {record["link"]: record.get("post_id") for record in id_records}
    endpoint = mcp_url()

    records = []
    for index, article in enumerate(articles, start=1):
        post_id = ids_by_slug.get(article["link"])
        if not post_id:
            raise SystemExit(f"Missing post_id for {article['link']}")
        before = verify_article(article, post_id)
        record = {
            "post_id": post_id,
            "slug": article["link"],
            "title": article["title"],
            "before": before,
        }
        if before["ok"] and not FORCE_UPDATE_ALL:
            record["skipped"] = "already_ok"
            record["after"] = before
            record["ok"] = True
            records.append(record)
            continue

        if FORBIDDEN_PUBLIC_RE.search(article["long"]):
            raise SystemExit(f"Forbidden public/internal wording in source long HTML: {article['link']}")
        if REQUIRED_CATEGORY not in article["long"] or REQUIRED_PRODUCT not in article["long"]:
            raise SystemExit(f"Missing required robot vacuum links in source long HTML: {article['link']}")

        payload = {
            "post_id": str(post_id),
            "title": article["title"],
            "short": article["short"],
            "long": article["long"],
            "visible": True,
        }
        result = call_update(endpoint, payload, f"batch-32-rich-html-{index}")
        time.sleep(1.2)
        after = verify_article(article, post_id)
        if not after["ok"]:
            time.sleep(4)
            after = verify_article(article, post_id)

        record["mcp_result"] = result.get("result", {})
        record["after"] = after
        record["ok"] = after["ok"]
        records.append(record)

    report = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "batch": "batch-32-robot-vacuum",
        "source_articles": str(ARTICLES_JSON.relative_to(ROOT)),
        "source_id_map": str(ID_MAP_JSON.relative_to(ROOT)),
        "force_update_all": FORCE_UPDATE_ALL,
        "record_count": len(records),
        "updated_count": sum(1 for record in records if not record.get("skipped")),
        "ok_count": sum(1 for record in records if record.get("ok")),
        "all_ok": all(record.get("ok") for record in records),
        "records": records,
    }
    RESULTS_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "record_count": report["record_count"],
                "updated_count": report["updated_count"],
                "ok_count": report["ok_count"],
                "all_ok": report["all_ok"],
                "failed": [
                    {
                        "post_id": record["post_id"],
                        "slug": record["slug"],
                        "after": record.get("after"),
                    }
                    for record in records
                    if not record.get("ok")
                ],
                "results": str(RESULTS_JSON),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
