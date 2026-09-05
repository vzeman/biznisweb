import argparse
import importlib.util
import json
import re
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

import requests


ROOT = Path(__file__).resolve().parents[3]
GUARD_PATH = ROOT / "content" / "VEVO_CONTENT" / "tools" / "vevo_public_content_guard.py"
CONFIG_PATH = Path.home() / ".codex" / "config.toml"
SOURCE_REPORT = ROOT / "content" / "VEVO_CONTENT" / "exports" / "internal-public-terms-cleanup-2026-06-16-source.json"
MCP_RESULTS = ROOT / "content" / "VEVO_CONTENT" / "exports" / "internal-public-terms-cleanup-2026-06-16-mcp-results.json"


def load_guard():
    spec = importlib.util.spec_from_file_location("vevo_public_content_guard", GUARD_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


GUARD = load_guard()


def git_output(args):
    result = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return result.stdout


def changed_import_json_files():
    output = git_output(["diff", "--name-only", "--", "content/VEVO_CONTENT/imports"])
    paths = []
    for line in output.splitlines():
        if line.endswith(".json"):
            paths.append(ROOT / line)
    return sorted(paths)


def read_head_json(path):
    rel = path.relative_to(ROOT).as_posix()
    result = subprocess.run(
        ["git", "show", f"HEAD:{rel}"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        return None
    return json.loads(result.stdout)


def article_list(root):
    if isinstance(root, list):
        return root
    if isinstance(root, dict) and isinstance(root.get("updates"), list):
        return root["updates"]
    if isinstance(root, dict) and isinstance(root.get("articles"), list):
        return root["articles"]
    return []


def slug_for(article):
    slug = article.get("link") or article.get("slug")
    if not slug and isinstance(article.get("url"), str) and "/n/" in article["url"]:
        slug = article["url"].rstrip("/").rsplit("/n/", 1)[-1]
    return slug


def article_key(article, index):
    return slug_for(article) or article.get("title") or f"index:{index}"


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


def call_update(endpoint, payload, request_id):
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
        endpoint,
        json=body,
        headers={"Accept": "application/json, text/event-stream"},
        timeout=120,
    )
    response.raise_for_status()
    parsed = parse_sse_json(response.text)
    if "error" in parsed:
        raise RuntimeError(json.dumps(parsed["error"], ensure_ascii=False))
    tool_result = parsed.get("result", {})
    for item in tool_result.get("content", []):
        if item.get("type") != "text":
            continue
        try:
            text_payload = json.loads(item.get("text", ""))
        except json.JSONDecodeError:
            continue
        if text_payload.get("error"):
            raise RuntimeError(text_payload["error"])
        if text_payload.get("success") is False:
            raise RuntimeError(json.dumps(text_payload, ensure_ascii=False))
    return parsed


def changed_articles_from_git():
    mappings = GUARD.load_mappings()
    candidates = []
    for path in changed_import_json_files():
        current_root = json.loads(path.read_text(encoding="utf-8"))
        previous_root = read_head_json(path)
        if previous_root is None:
            continue
        current_articles = article_list(current_root)
        previous_articles = {
            article_key(article, index): article
            for index, article in enumerate(article_list(previous_root))
            if isinstance(article, dict)
        }
        for index, article in enumerate(current_articles):
            if not isinstance(article, dict):
                continue
            key = article_key(article, index)
            previous = previous_articles.get(key, {})
            changed_fields = [
                field
                for field in GUARD.PUBLIC_FIELDS
                if isinstance(article.get(field), str) and article.get(field) != previous.get(field)
            ]
            if not changed_fields:
                continue
            hits = GUARD.find_hits(article)
            if hits:
                raise SystemExit(
                    f"Refusing to update live post because public text still contains internal wording: {key} {hits}"
                )
            slug = slug_for(article)
            mapping = mappings.get(slug, {})
            post_id = str(article.get("post_id") or mapping.get("post_id") or "")
            url = article.get("url") or mapping.get("url")
            candidates.append(
                {
                    "source_file": str(path.relative_to(ROOT)),
                    "source_priority": GUARD.article_priority(path),
                    "index": index,
                    "title": article.get("title", ""),
                    "slug": slug,
                    "post_id": post_id,
                    "url": url,
                    "fields_changed": changed_fields,
                    "article": article,
                }
            )

    by_slug = {}
    for item in candidates:
        slug = item.get("slug")
        if not slug:
            continue
        current = by_slug.get(slug)
        if current is None or item["source_priority"] >= current["source_priority"]:
            by_slug[slug] = item
    return sorted(by_slug.values(), key=lambda item: (item["source_file"], item["index"]))


def public_report(candidates, dry_run):
    return {
        "project": "VEVO_CONTENT",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "purpose": "Remove internal SEO/workflow wording from public VEVO article fields.",
        "dry_run": dry_run,
        "live_candidate_count": len(candidates),
        "live_candidates": [
            {
                key: item.get(key)
                for key in ("source_file", "source_priority", "index", "title", "slug", "post_id", "url", "fields_changed")
            }
            for item in candidates
        ],
    }


def main():
    parser = argparse.ArgumentParser(description="Update VEVO live articles after internal public wording cleanup.")
    parser.add_argument("--dry-run", action="store_true", help="Build reports without calling BiznisWeb MCP (default).")
    parser.add_argument(
        "--execute-live",
        action="store_true",
        help="Explicitly allow updates to existing live VEVO posts.",
    )
    parser.add_argument("--sleep", type=float, default=0.15, help="Pause between MCP updates.")
    parser.add_argument("--slugs", nargs="*", help="Limit update to specific article slugs.")
    parser.add_argument("--mcp-results", type=Path, default=MCP_RESULTS, help="Where to write MCP update results.")
    args = parser.parse_args()
    if args.dry_run and args.execute_live:
        parser.error("Use either --dry-run or --execute-live, not both.")
    dry_run = not args.execute_live

    candidates = changed_articles_from_git()
    if args.slugs:
        wanted = set(args.slugs)
        candidates = [item for item in candidates if item.get("slug") in wanted]
    missing = [item for item in candidates if not item.get("post_id")]
    if missing:
        raise SystemExit(f"Missing post IDs for {len(missing)} cleanup candidates")

    SOURCE_REPORT.write_text(json.dumps(public_report(candidates, dry_run), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    updates = []
    endpoint = None if dry_run else mcp_url()
    for request_id, item in enumerate(candidates, start=1):
        article = item["article"]
        payload = {
            "post_id": item["post_id"],
            "title": article["title"],
            "short": article["short"],
            "long": article["long"],
            "visible": bool(article.get("active", True)),
        }
        result = {"dry_run": True}
        if not dry_run:
            result = call_update(endpoint, payload, request_id)
            time.sleep(args.sleep)
        updates.append(
            {
                "post_id": item["post_id"],
                "title": item["title"],
                "slug": item["slug"],
                "url": item["url"],
                "source_file": item["source_file"],
                "fields_changed": item["fields_changed"],
                "long_length": len(article["long"]),
                "mcp_result": result.get("result", result),
            }
        )

    args.mcp_results.write_text(
        json.dumps(
            {
                "project": "VEVO_CONTENT",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "purpose": "Update live VEVO articles after removing internal wording from public article text.",
                "dry_run": dry_run,
                "updated_count": len(updates),
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
                "dry_run": dry_run,
                "candidate_count": len(candidates),
                "updated_count": len(updates),
                "source_report": str(SOURCE_REPORT),
                "mcp_results": str(args.mcp_results),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
