#!/usr/bin/env python3
"""Expand and differentiate confirmed VEVO semantic-overlap article groups."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


PROJECT = Path(__file__).resolve().parents[1]
TOOLS = PROJECT / "tools"
IMPORTS = PROJECT / "imports"
sys.path.insert(0, str(TOOLS))
sys.path.insert(0, str(IMPORTS))

import biznisweb_vevo_content_mcp as content_mcp
import semantic_duplicate_configs_2026_07_14 as config_source
import semantic_duplicate_deep_dives_2026_07_14 as deep_dive_source
import vevo_article_depth_guard as depth_guard
import vevo_html_safety_guard as html_guard
import vevo_public_content_guard as public_guard
import vevo_rich_expansion_renderer as renderer


MARKER_PREFIX = "VEVO-SEMANTIC-DUPLICATE-REPAIR-20260714"
PREPARED = IMPORTS / "semantic-duplicate-remediation-2026-07-14-articles.json"
BACKUP = PROJECT / "exports" / "semantic-duplicate-remediation-2026-07-14-backup.json"
REPORT = PROJECT / "exports" / "semantic-duplicate-remediation-2026-07-14-results.json"

CONFIG_IDS = {config["post_id"] for config in config_source.CONFIGS}
if CONFIG_IDS != set(deep_dive_source.DEEP_DIVES):
    raise RuntimeError("semantic remediation config and deep-dive ids differ")
CONFIGS = [
    {**config, "deep_dive": deep_dive_source.DEEP_DIVES[config["post_id"]]}
    for config in config_source.CONFIGS
]


def validate_config(
    config: dict[str, Any],
    existing: dict[str, Any],
    original_long: str | None = None,
) -> dict[str, Any]:
    post_id = config["post_id"]
    existing_title = renderer.normalize_admin_unicode(
        str(existing.get("title") or "")
    ).strip()
    if existing_title != config["title"]:
        raise RuntimeError(
            f"post {post_id} title drift: {existing.get('title')!r} != {config['title']!r}"
        )
    if str(existing.get("link") or "").strip() != config["slug"]:
        raise RuntimeError(
            f"post {post_id} slug drift: {existing.get('link')!r} != {config['slug']!r}"
        )
    if str(existing.get("active") or "0") != "1":
        raise RuntimeError(f"post {post_id} is not public")

    marker = f"{MARKER_PREFIX}-{post_id}"
    if original_long is not None and marker not in str(existing.get("long") or ""):
        raise RuntimeError(
            f"post {post_id} cannot rebuild: current live body lacks remediation marker"
        )
    base_long = original_long if original_long is not None else str(existing.get("long") or "")
    long_body, already_applied = renderer.append_preserving_original(
        config,
        base_long,
        marker,
    )
    if original_long is not None:
        already_applied = False
    article = {
        "post_id": post_id,
        "block_id": str(existing.get("block_id") or ""),
        "title": config["title"],
        "short": config["short"],
        "long": long_body,
        "link": config["slug"],
        "description": config["description"],
        "active": True,
    }
    html_result = html_guard.analyze(article)
    public_hits = public_guard.find_hits(article)
    metrics = depth_guard.article_metrics(article)
    failures = list(html_result["failures"])
    if public_hits:
        failures.append(f"forbidden public wording: {public_hits}")
    if metrics["words"] < 1500:
        failures.append(f"visible word count {metrics['words']} < 1500")
    if metrics["h2_count"] < 12:
        failures.append(f"h2 count {metrics['h2_count']} < 12")
    if metrics["table_count"] < 2:
        failures.append(f"table count {metrics['table_count']} < 2")
    if metrics["styled_block_count"] < 6:
        failures.append(f"styled blocks {metrics['styled_block_count']} < 6")
    if metrics["faq_question_count"] < 5:
        failures.append(f"FAQ count {metrics['faq_question_count']} < 5")
    if failures:
        raise RuntimeError(
            f"post {post_id} failed content guards: "
            + json.dumps(failures, ensure_ascii=False)
        )
    return {
        **article,
        "already_applied": already_applied,
        "metrics": metrics,
        "html_safety": html_result,
    }


def fetch_and_prepare(
    rebuild_from_backup: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    snapshot = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "posts": {
            config["post_id"]: content_mcp.admin_get_news_post(config["post_id"])
            for config in CONFIGS
        },
    }
    backup_posts: dict[str, Any] = {}
    if rebuild_from_backup:
        if not BACKUP.exists():
            raise RuntimeError(f"semantic remediation backup is missing: {BACKUP}")
        backup_posts = json.loads(BACKUP.read_text(encoding="utf-8"))["posts"]
        if CONFIG_IDS != set(backup_posts):
            raise RuntimeError("semantic remediation backup post ids differ from config")
    prepared = [
        validate_config(
            config,
            snapshot["posts"][config["post_id"]],
            (
                renderer.normalize_admin_unicode(
                    str(backup_posts[config["post_id"]].get("long") or "")
                )
                if rebuild_from_backup
                else None
            ),
        )
        for config in CONFIGS
    ]
    if len({item["post_id"] for item in prepared}) != len(prepared):
        raise RuntimeError("semantic remediation contains duplicate post ids")
    return prepared, snapshot


def new_links(prepared: list[dict[str, Any]]) -> list[str]:
    links = set()
    boundary = "<h2>Ďalší pôvodný prehľad témy</h2>"
    for article in prepared:
        marker_end = article["long"].find(boundary)
        expansion = article["long"][:marker_end] if marker_end >= 0 else article["long"]
        for href in re.findall(r'href=["\']([^"\']+)["\']', expansion):
            links.add(requests.compat.urljoin("https://www.vevo.sk", href))
    return sorted(links)


def check_links(urls: list[str]) -> list[dict[str, Any]]:
    results = []
    failures = []
    for url in urls:
        response = requests.get(
            url,
            headers={"User-Agent": "Codex VEVO semantic duplicate remediation"},
            timeout=45,
            allow_redirects=True,
        )
        result = {
            "url": url,
            "status_code": response.status_code,
            "final_url": response.url,
            "ok": response.status_code < 400,
        }
        results.append(result)
        if not result["ok"]:
            failures.append(result)
    if failures:
        raise RuntimeError(
            "link preflight failed: " + json.dumps(failures, ensure_ascii=False)
        )
    return results


def write_prepared(
    prepared: list[dict[str, Any]],
    snapshot: dict[str, Any],
    links: list[dict[str, Any]],
) -> None:
    PREPARED.write_text(
        json.dumps(
            {
                "created_at": datetime.now(timezone.utc).isoformat(),
                "articles": prepared,
                "link_preflight": links,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    if not BACKUP.exists():
        BACKUP.write_text(
            json.dumps(snapshot, ensure_ascii=True, indent=2) + "\n",
            encoding="utf-8",
        )


def execute(
    prepared: list[dict[str, Any]], link_results: list[dict[str, Any]]
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "mode": "execute-live",
        "updates": [],
        "link_preflight": link_results,
        "all_ok": False,
    }
    REPORT.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    try:
        for article in prepared:
            if article["already_applied"]:
                post = content_mcp.admin_get_news_post(article["post_id"])
                public_status = content_mcp.public_status_for_slug(article["link"])
                post_title = renderer.normalize_admin_unicode(
                    str(post.get("title") or "")
                ).strip()
                result = {
                    "post_id": article["post_id"],
                    "title": post_title,
                    "slug": post.get("link"),
                    "action": "already-applied",
                    "public_status": public_status,
                    "metrics": article["metrics"],
                    "ok": (
                        post_title == article["title"]
                        and str(post.get("link") or "").strip() == article["link"]
                        and public_status.get("status_code") == 200
                    ),
                }
            else:
                updated = content_mcp.tool_update_news_post(
                    {
                        "post_id": article["post_id"],
                        "short": article["short"],
                        "long": article["long"],
                        "description": article["description"],
                        "active": True,
                        "confirm_visible": True,
                    }
                )
                post = updated["news_post"]
                post_title = renderer.normalize_admin_unicode(
                    str(post.get("title") or "")
                ).strip()
                result = {
                    "post_id": article["post_id"],
                    "title": post_title,
                    "slug": post.get("link"),
                    "action": "updated",
                    "public_status": updated["public_status"],
                    "metrics": article["metrics"],
                    "ok": (
                        post_title == article["title"]
                        and str(post.get("link") or "").strip() == article["link"]
                        and updated["public_status"].get("status_code") == 200
                    ),
                }
            report["updates"].append(result)
            REPORT.write_text(
                json.dumps(report, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
        report["all_ok"] = all(item["ok"] for item in report["updates"])
    finally:
        REPORT.write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute-live",
        action="store_true",
        help="Apply all validated semantic-overlap expansions through the local MCP helper.",
    )
    parser.add_argument(
        "--rebuild-from-backup",
        action="store_true",
        help="Rebuild each expansion from the immutable pre-remediation body backup.",
    )
    args = parser.parse_args()

    prepared, snapshot = fetch_and_prepare(args.rebuild_from_backup)
    link_results = check_links(new_links(prepared))
    write_prepared(prepared, snapshot, link_results)
    summary = {
        "article_count": len(prepared),
        "min_words": min(item["metrics"]["words"] for item in prepared),
        "max_words": max(item["metrics"]["words"] for item in prepared),
        "link_count": len(link_results),
        "prepared_file": str(PREPARED),
        "backup_file": str(BACKUP),
        "rebuilt_from_backup": args.rebuild_from_backup,
    }
    if not args.execute_live:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    report = execute(prepared, link_results)
    print(
        json.dumps(
            {**summary, "results_file": str(REPORT), "all_ok": report["all_ok"]},
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if report["all_ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
