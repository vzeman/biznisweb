import argparse
import json
import re
import sys
import time
import tomllib
from pathlib import Path
from urllib.parse import urljoin

import requests


BASE = "https://www.vevo.sk"
ROOT = Path(__file__).resolve().parents[3]
ARTICLE_JSON = ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-35-2026-07-08-articles.json"
OUT = ROOT / "content" / "VEVO_CONTENT" / "exports" / "batch-35-2026-07-08-publication.json"
MCP_CONFIG = Path.home() / ".codex" / "config.toml"


def mcp_url_from_config():
    if not MCP_CONFIG.exists():
        raise RuntimeError(f"Missing Codex config: {MCP_CONFIG}")
    data = tomllib.loads(MCP_CONFIG.read_text(encoding="utf-8"))
    try:
        return data["mcp_servers"]["biznisweb-vevo"]["url"]
    except KeyError as exc:
        raise RuntimeError("Missing mcp_servers.biznisweb-vevo.url in Codex config") from exc


def parse_sse_response(response):
    response.raise_for_status()
    payloads = []
    for raw_line in response.text.splitlines():
        line = raw_line.strip()
        if not line.startswith("data:"):
            continue
        data = line[5:].strip()
        if data:
            payloads.append(json.loads(data))
    if not payloads:
        raise RuntimeError(f"No JSON-RPC data in MCP response: {response.text[:500]}")
    payload = payloads[-1]
    if "error" in payload:
        raise RuntimeError(json.dumps(payload["error"], ensure_ascii=False))
    return payload.get("result")


def mcp_call(url, method, params=None, request_id=1):
    headers = {"Accept": "application/json, text/event-stream", "Content-Type": "application/json"}
    payload = {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params or {}}
    response = requests.post(url, headers=headers, json=payload, timeout=120)
    return parse_sse_response(response)


def tool_call(url, name, arguments, request_id):
    return mcp_call(url, "tools/call", {"name": name, "arguments": arguments}, request_id=request_id)


def extract_text_result(result):
    if not isinstance(result, dict):
        return result
    content = result.get("content")
    if isinstance(content, list) and content:
        first = content[0]
        if isinstance(first, dict) and first.get("type") == "text":
            text = first.get("text", "")
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return text
    return result


def load_existing_report():
    if not OUT.exists():
        return {"block_id": "765", "posts": []}
    return json.loads(OUT.read_text(encoding="utf-8"))


def summarize_tool_result(parsed):
    if not isinstance(parsed, dict):
        return {"raw": str(parsed)[:500]}
    news_post = parsed.get("news_post") if isinstance(parsed.get("news_post"), dict) else {}
    return {
        "success": parsed.get("success"),
        "message": parsed.get("message"),
        "news_post": {
            "id": news_post.get("id") or news_post.get("post_id"),
            "title": news_post.get("title"),
            "link": news_post.get("link"),
            "visible": news_post.get("visible"),
        },
    }


def save_report(report):
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def public_check(url, title):
    response = requests.get(url, timeout=30, allow_redirects=True)
    text = response.text
    return {
        "url": url,
        "status": response.status_code,
        "ok": response.status_code == 200 and title in text and "<table" in text and "style=" in text,
        "title_found": title in text,
        "table_found": "<table" in text,
        "style_found": "style=" in text,
        "length": len(text),
    }


def slug_from_link(article):
    return article["link"].strip("/")


def main():
    parser = argparse.ArgumentParser(description="Publish VEVO batch 35 through the configured MCP endpoint.")
    parser.add_argument("--publish", action="store_true", help="Actually create/update live posts.")
    parser.add_argument(
        "--allow-unsafe-mcp-publish",
        action="store_true",
        help="Escape hatch only. Remote VEVO MCP news tools do not expose/preserve link slugs.",
    )
    parser.add_argument("--block-id", default="765")
    args = parser.parse_args()

    articles = json.loads(ARTICLE_JSON.read_text(encoding="utf-8"))
    report = load_existing_report()
    report.setdefault("block_id", args.block_id)
    report.setdefault("posts", [])
    by_title = {row["title"]: row for row in report["posts"]}

    if not args.publish:
        print(json.dumps({"dry_run": True, "article_count": len(articles), "existing_report_posts": len(by_title)}, ensure_ascii=False, indent=2))
        return
    if not args.allow_unsafe_mcp_publish:
        raise SystemExit(
            "Refusing VEVO news publish through remote MCP: add/update news tools do not expose or preserve "
            "link/slug and were observed creating /n/111... URLs. Use the admin/XLS clean-URL workflow or "
            "upgrade the MCP to support link/date fields before publishing."
        )

    mcp_url = mcp_url_from_config()
    mcp_call(
        mcp_url,
        "initialize",
        {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "vevo-batch-35-publisher", "version": "1.0"},
        },
        request_id=1,
    )

    request_id = 10
    for index, article in enumerate(articles, start=1):
        title = article["title"]
        expected_url = urljoin(BASE, f"/n/{slug_from_link(article)}")
        row = by_title.get(title)
        if row and row.get("post_id"):
            print(f"SKIP existing mapping {row['post_id']} {title}", flush=True)
            continue
        result = tool_call(
            mcp_url,
            "biznisweb-add_news_post",
            {
                "block_id": args.block_id,
                "title": title,
                "short": article["short"],
                "long": article["long"],
                "visible": False,
                "position": index,
            },
            request_id=request_id,
        )
        request_id += 1
        parsed = extract_text_result(result)
        news_post = parsed.get("news_post", parsed) if isinstance(parsed, dict) else {}
        post_id = str(news_post.get("id") or news_post.get("post_id") or "")
        if not post_id:
            raise RuntimeError(f"Missing post_id for {title}: {parsed}")
        row = {
            "title": title,
            "slug": slug_from_link(article),
            "expected_url": expected_url,
            "post_id": post_id,
            "created_visible": False,
            "create_result": summarize_tool_result(parsed),
        }
        report["posts"].append(row)
        by_title[title] = row
        save_report(report)
        print(f"CREATED hidden {post_id} {title}", flush=True)
        time.sleep(0.4)

    for row in report["posts"]:
        if row.get("published"):
            continue
        result = tool_call(
            mcp_url,
            "biznisweb-update_news_post",
            {"post_id": row["post_id"], "visible": True},
            request_id=request_id,
        )
        request_id += 1
        row["publish_result"] = summarize_tool_result(extract_text_result(result))
        row["published"] = True
        save_report(report)
        print(f"PUBLISHED {row['post_id']} {row['title']}", flush=True)
        time.sleep(0.4)

    checks = []
    for row in report["posts"]:
        check = public_check(row["expected_url"], row["title"])
        row["public_check"] = check
        checks.append(check)
        save_report(report)
        print(f"VERIFY {check['status']} {check['ok']} {row['expected_url']}", flush=True)

    report["record_count"] = len(report["posts"])
    report["ok_count"] = sum(1 for row in report["posts"] if row.get("public_check", {}).get("ok"))
    report["all_ok"] = report["ok_count"] == report["record_count"] == len(articles)
    report["url_shape_ok"] = all(re.match(r"^https://www\.vevo\.sk/n/[a-z0-9-]+$", row["expected_url"]) for row in report["posts"])
    save_report(report)
    print(json.dumps({"record_count": report["record_count"], "ok_count": report["ok_count"], "all_ok": report["all_ok"], "url_shape_ok": report["url_shape_ok"]}, ensure_ascii=False, indent=2))
    if not report["all_ok"] or not report["url_shape_ok"]:
        raise SystemExit("Batch 35 publication verification failed")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
