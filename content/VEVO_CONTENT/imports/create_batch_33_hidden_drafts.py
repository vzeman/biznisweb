import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import requests


ROOT = Path(__file__).resolve().parents[3]
VEVO_ROOT = ROOT / "content" / "VEVO_CONTENT"
ARTICLES_JSON = VEVO_ROOT / "imports" / "batch-33-2026-07-06-articles.json"
MAPPING_JSON = VEVO_ROOT / "exports" / "batch-33-2026-07-06-hidden-drafts.json"
CONFIG_PATH = Path.home() / ".codex" / "config.toml"
BLOCK_ID = "765"

SEED_MAPPING = {
    "Ako umyť podlahu bez šmúh: laminát, vinyl, dlažba a mopovanie v praxi": {
        "post_id": "2291",
        "title": "Ako umyť podlahu bez šmúh: laminát, vinyl, dlažba a mopovanie v praxi",
        "slug": "ako-umyt-podlahu-bez-smuh-laminat-vinyl-dlazba-a-mopovanie-v-praxi",
        "target_date": "2025-09-19",
        "target_time": "08:00:00",
        "visible": False,
        "created_by": "manual_tool_call_before_importer",
    }
}


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
    raise RuntimeError(f"No JSON data line in MCP response: {text[:500]}")


def parse_created_post(result):
    payload = None
    for item in result.get("content", []):
        if item.get("type") != "text":
            continue
        try:
            payload = json.loads(item.get("text", ""))
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Malformed create response text: {item.get('text', '')[:500]}") from exc
        break
    if not payload:
        raise RuntimeError("Missing text payload in create response")
    if payload.get("success") is not True:
        raise RuntimeError(json.dumps(payload, ensure_ascii=False))
    post_id = payload.get("news_post", {}).get("id")
    if not post_id:
        raise RuntimeError(f"Missing news_post.id in create response: {json.dumps(payload, ensure_ascii=False)[:800]}")
    return payload


def call_add(endpoint, article, request_id):
    body = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "tools/call",
        "params": {
            "name": "biznisweb-add_news_post",
            "arguments": {
                "block_id": BLOCK_ID,
                "title": article["title"],
                "short": article["short"],
                "long": article["long"],
                "visible": False,
            },
        },
    }
    response = requests.post(
        endpoint,
        json=body,
        headers={"Accept": "application/json, text/event-stream"},
        timeout=180,
    )
    response.raise_for_status()
    parsed = parse_sse_json(response.text)
    if "error" in parsed:
        raise RuntimeError(json.dumps(parsed["error"], ensure_ascii=False))
    return parse_created_post(parsed.get("result", {}))


def load_mapping():
    if MAPPING_JSON.exists():
        data = json.loads(MAPPING_JSON.read_text(encoding="utf-8"))
    else:
        data = {"created_at": datetime.now(timezone.utc).isoformat(), "block_id": BLOCK_ID, "drafts": []}
    by_title = {draft["title"]: draft for draft in data.get("drafts", [])}
    for title, draft in SEED_MAPPING.items():
        if title not in by_title:
            data.setdefault("drafts", []).append(draft)
            by_title[title] = draft
    return data, by_title


def save_mapping(data):
    MAPPING_JSON.parent.mkdir(parents=True, exist_ok=True)
    data["updated_at"] = datetime.now(timezone.utc).isoformat()
    MAPPING_JSON.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="Legacy batch 33 hidden-draft creator.")
    parser.add_argument(
        "--execute-live",
        action="store_true",
        help="Explicitly allow hidden draft creation in the live VEVO admin.",
    )
    args = parser.parse_args()
    if not args.execute_live:
        raise SystemExit("Refusing live VEVO draft creation without --execute-live")

    articles = json.loads(ARTICLES_JSON.read_text(encoding="utf-8"))
    data, by_title = load_mapping()
    endpoint = mcp_url()
    created = []
    skipped = []
    save_mapping(data)

    for index, article in enumerate(articles, start=1):
        if article["title"] in by_title:
            skipped.append({"title": article["title"], "post_id": by_title[article["title"]]["post_id"]})
            continue
        payload = call_add(endpoint, article, request_id=f"batch33-hidden-{index}")
        draft = {
            "post_id": str(payload["news_post"]["id"]),
            "title": article["title"],
            "slug": article["link"],
            "target_date": article["date_posted"],
            "target_time": article["time_posted"],
            "visible": False,
            "created_by": "create_batch_33_hidden_drafts.py",
        }
        data.setdefault("drafts", []).append(draft)
        by_title[article["title"]] = draft
        created.append(draft)
        save_mapping(data)

    output = {
        "article_count": len(articles),
        "created_count": len(created),
        "skipped_count": len(skipped),
        "created": created,
        "skipped": skipped,
        "mapping": str(MAPPING_JSON),
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
