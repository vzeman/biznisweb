import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import requests


ARTICLES_JSON = Path("content/VEVO_CONTENT/imports/batch-27-2026-06-16-articles.json")
MAPPING_JSON = Path("content/VEVO_CONTENT/exports/batch-27-2026-06-16-mapping.json")
MCP_RESULTS_JSON = Path("content/VEVO_CONTENT/exports/batch-27-2026-06-16-mcp-results.json")
CONFIG_PATH = Path.home() / ".codex" / "config.toml"
BASE_URL = "https://www.vevo.sk"
BLOCK_ID = "765"
POST_IDS = ["2260", "2261", "2262", "2263", "2264"]


def mcp_url():
    config = CONFIG_PATH.read_text(encoding="utf-8")
    match = re.search(
        r'(?s)\[mcp_servers\.biznisweb-vevo\]\s*url\s*=\s*"([^"]+)"',
        config,
    )
    if not match:
        raise SystemExit("biznisweb-vevo MCP URL not found in ~/.codex/config.toml")
    return match.group(1)


def parse_sse_json(text):
    for line in text.splitlines():
        if line.startswith("data: "):
            return json.loads(line[6:])
    raise ValueError(f"No JSON data line in MCP response: {text[:500]}")


def call_update(url, payload, request_id):
    body = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "tools/call",
        "params": {
            "name": "biznisweb-update_news_post",
            "arguments": payload,
        },
    }
    response = requests.post(
        url,
        json=body,
        headers={"Accept": "application/json, text/event-stream"},
        timeout=90,
    )
    response.raise_for_status()
    parsed = parse_sse_json(response.text)
    if "error" in parsed:
        raise RuntimeError(json.dumps(parsed["error"], ensure_ascii=False))
    return parsed


def main():
    parser = argparse.ArgumentParser(description="Legacy batch 27 live rich-HTML repair.")
    parser.add_argument(
        "--execute-live",
        action="store_true",
        help="Explicitly allow updates to existing live VEVO posts.",
    )
    args = parser.parse_args()
    if not args.execute_live:
        raise SystemExit("Refusing live VEVO updates without --execute-live")

    articles = json.loads(ARTICLES_JSON.read_text(encoding="utf-8"))
    if len(articles) != len(POST_IDS):
        raise SystemExit(f"Article count {len(articles)} does not match post IDs {len(POST_IDS)}")

    mapping_posts = []
    updates = []
    endpoint = mcp_url()

    for index, (article, post_id) in enumerate(zip(articles, POST_IDS), start=1):
        if re.search(r"\bCTA\b", article["long"], re.IGNORECASE):
            raise SystemExit(f"Forbidden customer-facing CTA acronym in {article['link']}")
        if "Cena:" in article["long"] or "€" in article["long"]:
            raise SystemExit(f"Fixed price marker in {article['link']}")

        payload = {
            "post_id": post_id,
            "title": article["title"],
            "short": article["short"],
            "long": article["long"],
            "visible": True,
        }
        result = call_update(endpoint, payload, index)
        url = f"{BASE_URL}/n/{article['link']}"
        updates.append(
            {
                "post_id": post_id,
                "title": article["title"],
                "slug": article["link"],
                "url": url,
                "long_length": len(article["long"]),
                "mcp_result": result.get("result", {}),
            }
        )
        mapping_posts.append(
            {
                "id": post_id,
                "title": article["title"],
                "slug": article["link"],
                "date_posted": article["date_posted"],
                "time_posted": article["time_posted"],
                "url": url,
            }
        )

    created_at = datetime.now(timezone.utc).isoformat()
    MAPPING_JSON.write_text(
        json.dumps(
            {
                "batch": "batch-27",
                "created_at": created_at,
                "block_id": BLOCK_ID,
                "posts": mapping_posts,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    MCP_RESULTS_JSON.write_text(
        json.dumps(
            {
                "batch": "batch-27",
                "created_at": created_at,
                "purpose": "Normalize rich HTML for VEVO batch 27 after admin creation with clean URLs and dates.",
                "updates": updates,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "updated": len(updates),
                "mapping": str(MAPPING_JSON),
                "results": str(MCP_RESULTS_JSON),
                "ids": POST_IDS,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
