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
REPORT_PATH = VEVO_ROOT / "exports" / "laundry-perfume-link-insert-2026-06-29.json"
RECOVERY_REPORT_PATH = VEVO_ROOT / "exports" / "laundry-perfume-link-recovery-2026-06-29.json"
CONFIG_PATH = Path.home() / ".codex" / "config.toml"
TARGET_URL = "https://www.vevo.sk/c/vevo-fragrance/parfum-do-prania"
TARGET_ANCHOR = "parfumy do prania"


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


def call_update(endpoint, payload, request_id):
    body = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "tools/call",
        "params": {"name": "biznisweb-update_news_post", "arguments": payload},
    }
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


def fetch(url):
    try:
        response = requests.get(url, headers={"User-Agent": "Codex VEVO recovery"}, timeout=30)
        return response.status_code, response.text
    except requests.RequestException as exc:
        return None, str(exc)


def main_article_inner(html):
    soup = BeautifulSoup(html, "html.parser")
    divs = soup.select("div.userHTMLContent")
    if not divs:
        return ""
    div = max(divs, key=lambda node: len(str(node)))
    return "".join(str(child) for child in div.contents).strip()


def verify(url):
    status, html = fetch(url)
    inner = main_article_inner(html) if status == 200 else ""
    soup = BeautifulSoup(inner, "html.parser")
    links = [
        a for a in soup.find_all("a")
        if (a.get("href") or "").rstrip("/") == TARGET_URL.rstrip("/")
        and a.get_text(" ", strip=True).lower() == TARGET_ANCHOR
    ]
    return {
        "status": status,
        "body_length": len(inner),
        "target_link_count": len(links),
        "escaped_html": "&lt;p&gt;" in inner,
        "ok": status == 200 and len(inner) > 1000 and len(links) >= 1 and "&lt;p&gt;" not in inner,
    }


def source_articles_by_slug(report):
    by_file = {}
    result = {}
    for record in report["records"]:
        source_file = record.get("source_file")
        slug = record.get("slug")
        if not source_file or not slug:
            continue
        path = ROOT / source_file
        if path not in by_file:
            by_file[path] = load_json(path)
        root = by_file[path]
        articles = root if isinstance(root, list) else root.get("articles") or root.get("updates") or []
        for article in articles:
            if isinstance(article, dict) and (article.get("link") or article.get("slug")) == slug:
                result[slug] = article
                break
    return result


def run(execute_live=False):
    if not execute_live:
        raise SystemExit("Refusing live VEVO recovery without --execute-live")

    report = load_json(REPORT_PATH)
    endpoint = mcp_url()
    source_by_slug = source_articles_by_slug(report)
    records = []
    targets = [record for record in report["records"] if record.get("mcp_update_ok")]
    for index, record in enumerate(targets, start=1):
        before = verify(record["url"])
        article = source_by_slug.get(record["slug"])
        item = {
            "post_id": record["post_id"],
            "slug": record["slug"],
            "url": record["url"],
            "before": before,
        }
        if before.get("ok"):
            item["skipped"] = "already_ok"
            item["after"] = before
            item["ok"] = True
            records.append(item)
            continue
        if not article:
            item["error"] = "source article not found"
            item["after"] = before
            records.append(item)
            continue
        payload = {
            "post_id": record["post_id"],
            "title": article.get("title", ""),
            "short": article.get("short", ""),
            "long": article.get("long", ""),
            "visible": True,
        }
        if isinstance(article.get("position"), int):
            payload["position"] = article["position"]
        try:
            call_update(endpoint, payload, f"laundry-perfume-recovery-{index}")
            time.sleep(1.2)
            after = verify(record["url"])
            if not after.get("ok"):
                time.sleep(4)
                after = verify(record["url"])
            item["update_ok"] = True
            item["after"] = after
            item["ok"] = after.get("ok") is True
        except Exception as exc:
            item["error"] = f"{type(exc).__name__}: {exc}"
            item["after"] = verify(record["url"])
            item["ok"] = False
        records.append(item)

    recovery = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_report": str(REPORT_PATH.relative_to(ROOT)),
        "record_count": len(records),
        "recovered_count": sum(1 for record in records if record.get("ok") or record.get("skipped") == "already_ok"),
        "all_ok": all(record.get("ok") or record.get("skipped") == "already_ok" for record in records),
        "records": records,
    }
    RECOVERY_REPORT_PATH.write_text(json.dumps(recovery, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "record_count": recovery["record_count"],
        "recovered_count": recovery["recovered_count"],
        "all_ok": recovery["all_ok"],
        "failed": [record for record in records if not (record.get("ok") or record.get("skipped") == "already_ok")],
        "report": str(RECOVERY_REPORT_PATH),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Legacy live recovery for laundry perfume links.")
    parser.add_argument(
        "--execute-live",
        action="store_true",
        help="Explicitly allow updates to existing live VEVO posts.",
    )
    run(execute_live=parser.parse_args().execute_live)
