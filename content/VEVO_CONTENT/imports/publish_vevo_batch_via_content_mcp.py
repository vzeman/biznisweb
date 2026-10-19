#!/usr/bin/env python3
"""Safely publish a prepared VEVO batch through the slug-aware local MCP server."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


ROOT = Path(__file__).resolve().parents[3]
VEVO_ROOT = ROOT / "content" / "VEVO_CONTENT"
DEFAULT_ARTICLES = VEVO_ROOT / "imports" / "batch-37-2026-07-14-articles.json"
DEFAULT_REPORT = VEVO_ROOT / "exports" / "batch-37-2026-07-14-mcp-publication.json"
DEFAULT_SMOKE_REPORT = VEVO_ROOT / "exports" / "vevo-mcp-slug-smoke-2026-07-14.json"
DEFAULT_MCP_SCRIPT = VEVO_ROOT / "tools" / "biznisweb_vevo_content_mcp.py"
BASE_URL = "https://www.vevo.sk"
SLUG_RE = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")
ONE_CHAR_PARAGRAPHS_RE = re.compile(r"(?:<p[^>]*>\s*[^<\s]\s*</p>\s*){3,}", re.IGNORECASE)


def now_iso() -> str:
    return dt.datetime.now().astimezone().isoformat(timespec="seconds")


def save_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


class StdioMcpClient:
    def __init__(self, server_script: Path) -> None:
        self.server_script = server_script.resolve()
        self.process: Optional[subprocess.Popen[str]] = None
        self.request_id = 0

    def __enter__(self) -> "StdioMcpClient":
        if not self.server_script.is_file():
            raise FileNotFoundError(f"VEVO MCP server not found: {self.server_script}")
        env = os.environ.copy()
        env["PYTHONUTF8"] = "1"
        self.process = subprocess.Popen(
            [sys.executable, "-X", "utf8", str(self.server_script)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            env=env,
        )
        self.request(
            "initialize",
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "vevo-content-publisher", "version": "1.0"},
            },
        )
        return self

    def __exit__(self, _exc_type: Any, _exc: Any, _traceback: Any) -> None:
        if not self.process:
            return
        if self.process.stdin:
            self.process.stdin.close()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.terminate()
            self.process.wait(timeout=5)

    def request(self, method: str, params: Optional[Dict[str, Any]] = None) -> Any:
        if not self.process or not self.process.stdin or not self.process.stdout:
            raise RuntimeError("MCP process is not running")
        self.request_id += 1
        request_id = self.request_id
        request = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params or {},
        }
        self.process.stdin.write(json.dumps(request, ensure_ascii=False) + "\n")
        self.process.stdin.flush()
        while True:
            line = self.process.stdout.readline()
            if not line:
                stderr = self.process.stderr.read() if self.process.stderr else ""
                raise RuntimeError(f"VEVO MCP server stopped unexpectedly: {stderr[-2000:]}")
            response = json.loads(line)
            if response.get("id") != request_id:
                continue
            if response.get("error"):
                error = response["error"]
                raise RuntimeError(f"{method} failed: {error.get('message') or error}")
            return response.get("result")

    def call_tool(self, name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        result = self.request(
            "tools/call",
            {"name": name, "arguments": arguments},
        )
        content = result.get("content") if isinstance(result, dict) else None
        if not isinstance(content, list) or not content:
            raise RuntimeError(f"Tool {name} returned no content")
        text = content[0].get("text") if isinstance(content[0], dict) else None
        if not isinstance(text, str):
            raise RuntimeError(f"Tool {name} returned an unexpected payload")
        parsed = json.loads(text)
        if not isinstance(parsed, dict):
            raise RuntimeError(f"Tool {name} did not return an object")
        return parsed


def validate_slug(value: Any) -> str:
    slug = str(value or "").strip().strip("/")
    if not SLUG_RE.fullmatch(slug):
        raise ValueError(f"Invalid clean slug: {value!r}")
    if re.fullmatch(r"1{2,}", slug):
        raise ValueError(f"Repeated-1 placeholder slug is forbidden: {slug}")
    return slug


def validate_article(article: Dict[str, Any]) -> None:
    for field in ("title", "short", "long", "link", "title_tag", "description"):
        if not str(article.get(field) or "").strip():
            raise ValueError(f"Article is missing {field}: {article.get('title')!r}")
    validate_slug(article["link"])
    long_html = str(article["long"])
    required_fragments = ("<p", "<h2", "<table", "style=", "/p-", "/c/")
    missing = [fragment for fragment in required_fragments if fragment not in long_html]
    if missing:
        raise ValueError(f"Rich HTML requirements missing for {article['title']}: {missing}")
    if "&lt;p" in long_html or "&lt;div" in long_html:
        raise ValueError(f"Escaped HTML detected for {article['title']}")
    if ONE_CHAR_PARAGRAPHS_RE.search(long_html):
        raise ValueError(f"One-character paragraph damage detected for {article['title']}")


def article_tool_args(
    article: Dict[str, Any],
    block_id: str,
    *,
    visible: bool,
) -> Dict[str, Any]:
    commenting = article.get("commenting", "none")
    if isinstance(commenting, bool):
        commenting = "public" if commenting else "none"
    payload: Dict[str, Any] = {
        "block_id": block_id,
        "title": article["title"],
        "short": article["short"],
        "long": article["long"],
        "link": validate_slug(article["link"]),
        "visible": visible,
        "date_posted": article.get("date_posted") or dt.date.today().isoformat(),
        "time_posted": article.get("time_posted") or dt.datetime.now().strftime("%H:%M:%S"),
        "commenting": str(commenting),
        "title_tag": article.get("title_tag") or article["title"],
        "keywords": article.get("keywords") or "",
        "description": article.get("description") or "",
        "image": article.get("image") or "",
        "image_title": article.get("image_title") or "",
        "image_alt": article.get("image_alt") or "",
    }
    if visible:
        payload["confirm_visible"] = True
    return payload


def public_status(slug: str) -> Dict[str, Any]:
    url = f"{BASE_URL}/n/{slug}"
    response = requests.get(
        url,
        headers={"User-Agent": "Codex VEVO MCP publication verifier"},
        timeout=30,
        allow_redirects=False,
    )
    return {
        "url": url,
        "status_code": response.status_code,
        "location": response.headers.get("location"),
        "body_length": len(response.text),
    }


def compact_tool_result(result: Dict[str, Any]) -> Dict[str, Any]:
    post = result.get("news_post") if isinstance(result.get("news_post"), dict) else {}
    return {
        "post_id": str(result.get("post_id") or post.get("news_id") or post.get("id") or ""),
        "title": post.get("title"),
        "link": post.get("link"),
        "active": post.get("active"),
        "short_length": len(str(post.get("short") or "")),
        "long_length": len(str(post.get("long") or "")),
        "public_status": result.get("public_status"),
    }


def list_catalog(client: StdioMcpClient, block_id: str) -> List[Dict[str, Any]]:
    result = client.call_tool(
        "vevo_list_news_posts",
        {"block_id": block_id, "limit": 2000, "summary_only": True},
    )
    rows = result.get("rows")
    if not isinstance(rows, list):
        raise RuntimeError("VEVO MCP catalog scan did not return rows")
    return rows


def exact_catalog_matches(
    rows: List[Dict[str, Any]], title: str, slug: str
) -> List[Dict[str, Any]]:
    title_key = title.strip().casefold()
    return [
        row
        for row in rows
        if str(row.get("title") or "").strip().casefold() == title_key
        or str(row.get("link") or "").strip() == slug
    ]


def verify_post_details(post: Dict[str, Any], title: str, slug: str) -> None:
    failures = []
    if str(post.get("title") or "").strip() != title.strip():
        failures.append("title")
    if str(post.get("link") or "").strip() != slug:
        failures.append("link")
    long_html = str(post.get("long") or "")
    if any(fragment not in long_html for fragment in ("<p", "<h2", "<table", "style=")):
        failures.append("rich_html")
    if "&lt;p" in long_html or ONE_CHAR_PARAGRAPHS_RE.search(long_html):
        failures.append("html_integrity")
    if failures:
        raise RuntimeError(f"Admin readback failed for {slug}: {failures}")


def smoke_article(stamp: str) -> Dict[str, Any]:
    marker = f"VEVO_MCP_SMOKE_{stamp}"
    return {
        "title": f"Codex VEVO MCP smoke {stamp}",
        "short": f"<p><strong>Skrytý API test:</strong> Overenie slugu a HTML. {marker}</p>",
        "long": (
            f"<p><strong>Skrytý API test:</strong> Tento záznam overuje presný slug a bohaté HTML. {marker}</p>"
            '<div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 22px 0; background: #f7fbf8;">'
            '<h2 style="margin-top: 0;">Kontrolný blok</h2><p>Test musí zostať skrytý a po overení sa zmaže.</p></div>'
            '<h2>Kontrolná tabuľka</h2><table style="width: 100%; border-collapse: collapse; margin: 20px 0;">'
            '<tbody><tr><td style="border: 1px solid #e5e5e5; padding: 10px;">Slug</td>'
            '<td style="border: 1px solid #e5e5e5; padding: 10px;">Presný</td></tr></tbody></table>'
        ),
        "link": f"codex-vevo-content-mcp-smoke-{stamp.lower()}",
        "date_posted": dt.date.today().isoformat(),
        "time_posted": dt.datetime.now().strftime("%H:%M:%S"),
        "commenting": False,
        "title_tag": f"Codex VEVO MCP smoke {stamp}",
        "description": "Dočasný skrytý záznam na overenie bezpečného VEVO MCP publikovania.",
        "marker": marker,
    }


def run_smoke(client: StdioMcpClient, block_id: str, report_path: Path) -> None:
    stamp = dt.datetime.now().strftime("%Y%m%d%H%M%S")
    article = smoke_article(stamp)
    report: Dict[str, Any] = {
        "started_at": now_iso(),
        "mode": "hidden-create-readback-delete",
        "block_id": block_id,
        "title": article["title"],
        "slug": article["link"],
        "post_id": None,
        "deleted": False,
        "all_ok": False,
    }
    save_json(report_path, report)
    try:
        before_rows = list_catalog(client, block_id)
        if exact_catalog_matches(before_rows, article["title"], article["link"]):
            raise RuntimeError("Disposable smoke title or slug already exists")
        before_public = public_status(article["link"])
        report["public_before"] = before_public
        save_json(report_path, report)
        if before_public["status_code"] != 404:
            raise RuntimeError(f"Smoke slug is not free: {before_public}")

        created = client.call_tool(
            "vevo_add_news_post",
            article_tool_args(article, block_id, visible=False),
        )
        report["create"] = compact_tool_result(created)
        report["post_id"] = report["create"]["post_id"]
        save_json(report_path, report)
        if not report["post_id"]:
            raise RuntimeError("Smoke create did not return post_id")

        details_result = client.call_tool(
            "vevo_get_news_post",
            {"post_id": report["post_id"]},
        )
        details = details_result.get("news_post") or {}
        verify_post_details(details, article["title"], article["link"])
        if article["marker"] not in str(details.get("long") or ""):
            raise RuntimeError("Smoke HTML marker is missing from admin readback")
        report["readback"] = {
            "title": details.get("title"),
            "link": details.get("link"),
            "active": details.get("active"),
            "short_length": len(str(details.get("short") or "")),
            "long_length": len(str(details.get("long") or "")),
            "marker_found": True,
        }
        report["public_hidden"] = public_status(article["link"])
        save_json(report_path, report)
        if report["readback"]["active"] != "0" or report["public_hidden"]["status_code"] != 404:
            raise RuntimeError("Smoke post was not kept hidden")

        client.call_tool(
            "vevo_delete_news_post",
            {"post_id": report["post_id"], "confirm_delete": True},
        )
        report["deleted"] = True
        after_rows = list_catalog(client, block_id)
        report["admin_match_count_after_delete"] = len(
            exact_catalog_matches(after_rows, article["title"], article["link"])
        )
        report["public_after_delete"] = public_status(article["link"])
        report["all_ok"] = (
            report["admin_match_count_after_delete"] == 0
            and report["public_after_delete"]["status_code"] == 404
        )
        report["completed_at"] = now_iso()
        save_json(report_path, report)
        if not report["all_ok"]:
            raise RuntimeError("Smoke cleanup verification failed")
    except Exception as exc:
        report["error"] = str(exc)
        if not report.get("deleted"):
            try:
                rows = list_catalog(client, block_id)
                matches = exact_catalog_matches(rows, article["title"], article["link"])
                if len(matches) == 1:
                    cleanup_id = str(matches[0].get("news_id") or "")
                    if cleanup_id:
                        client.call_tool(
                            "vevo_delete_news_post",
                            {"post_id": cleanup_id, "confirm_delete": True},
                        )
                        report["cleanup_deleted_post_id"] = cleanup_id
                        report["deleted"] = True
            except Exception as cleanup_exc:
                report["cleanup_error"] = str(cleanup_exc)
        report["completed_at"] = now_iso()
        save_json(report_path, report)
        raise


def load_publication_report(path: Path, article_file: Path, block_id: str) -> Dict[str, Any]:
    if path.exists():
        report = load_json(path)
        if not isinstance(report, dict):
            raise RuntimeError(f"Invalid publication report: {path}")
        return report
    return {
        "started_at": now_iso(),
        "mode": "slug-safe-mcp-publication",
        "article_file": str(article_file.resolve()),
        "block_id": block_id,
        "posts": [],
        "status": "started",
        "all_ok": False,
    }


def status_after_preflight(report: Dict[str, Any]) -> str:
    return "complete" if report.get("all_ok") else "preflight_passed"


def run_batch(
    client: StdioMcpClient,
    article_file: Path,
    report_path: Path,
    block_id: str,
    publish: bool,
) -> None:
    articles = load_json(article_file)
    if not isinstance(articles, list) or not articles:
        raise RuntimeError(f"No articles found in {article_file}")
    for article in articles:
        validate_article(article)
    slugs = [validate_slug(article["link"]) for article in articles]
    titles = [str(article["title"]).strip().casefold() for article in articles]
    if len(set(slugs)) != len(slugs) or len(set(titles)) != len(titles):
        raise RuntimeError("Batch contains duplicate titles or slugs")

    report = load_publication_report(report_path, article_file, block_id)
    report["last_run_at"] = now_iso()
    report["last_run_mode"] = "publish" if publish else "preflight"
    report.setdefault("posts", [])
    mapped = {str(row.get("slug") or ""): row for row in report["posts"]}
    try:
        catalog = list_catalog(client, block_id)
        report["admin_catalog_count"] = len(catalog)
        preflight: List[Dict[str, Any]] = []
        for article in articles:
            slug = validate_slug(article["link"])
            existing_mapping = mapped.get(slug)
            matches = exact_catalog_matches(catalog, article["title"], slug)
            status = public_status(slug)
            check = {
                "title": article["title"],
                "slug": slug,
                "admin_match_count": len(matches),
                "public_status": status,
                "mapped_post_id": (existing_mapping or {}).get("post_id"),
            }
            preflight.append(check)
            if existing_mapping:
                if len(matches) != 1:
                    raise RuntimeError(f"Mapped post {slug} does not have one exact admin match")
                if str(matches[0].get("news_id") or "") != str(existing_mapping.get("post_id") or ""):
                    raise RuntimeError(f"Mapped post id changed for {slug}")
            elif matches:
                raise RuntimeError(f"Unmapped duplicate candidate exists for {slug}: {matches[:2]}")
            elif status["status_code"] != 404:
                raise RuntimeError(f"Public slug is not free for {slug}: {status}")
        report["preflight"] = preflight

        if not publish:
            report["last_preflight_at"] = now_iso()
            report["last_preflight_ok"] = True
            report["status"] = status_after_preflight(report)
            save_json(report_path, report)
            print(
                json.dumps(
                    {
                        "dry_run": True,
                        "article_count": len(articles),
                        "admin_catalog_count": len(catalog),
                        "preflight_passed": True,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return

        report["publish_requested"] = True
        report["status"] = "publishing"
        save_json(report_path, report)

        for article in articles:
            slug = validate_slug(article["link"])
            row = mapped.get(slug)
            details: Dict[str, Any] = {}
            if row:
                details_result = client.call_tool(
                    "vevo_get_news_post",
                    {"post_id": str(row["post_id"])},
                )
                details = details_result.get("news_post") or {}
                verify_post_details(details, article["title"], slug)
            else:
                created = client.call_tool(
                    "vevo_add_news_post",
                    article_tool_args(article, block_id, visible=False),
                )
                create_summary = compact_tool_result(created)
                if not create_summary["post_id"] or create_summary["active"] != "0":
                    raise RuntimeError(f"Hidden create validation failed for {slug}: {create_summary}")
                row = {
                    "title": article["title"],
                    "slug": slug,
                    "url": f"{BASE_URL}/n/{slug}",
                    "post_id": create_summary["post_id"],
                    "create": create_summary,
                    "state": "hidden_created",
                }
                report["posts"].append(row)
                mapped[slug] = row
                report["status"] = f"hidden_created:{slug}"
                save_json(report_path, report)
                details = created.get("news_post") or {}
                verify_post_details(details, article["title"], slug)

            if str(details.get("active") or "") != "1":
                published = client.call_tool(
                    "vevo_update_news_post",
                    {
                        "post_id": str(row["post_id"]),
                        **article_tool_args(article, block_id, visible=True),
                    },
                )
                publish_summary = compact_tool_result(published)
                if publish_summary["active"] != "1":
                    raise RuntimeError(f"Publish readback is not active for {slug}")
                row["publish"] = publish_summary
                row["state"] = "published"
                report["status"] = f"published:{slug}"
                save_json(report_path, report)

            status = public_status(slug)
            row["public_status"] = status
            row["state"] = "public_verified" if status["status_code"] == 200 else "publish_failed"
            save_json(report_path, report)
            if status["status_code"] != 200:
                raise RuntimeError(f"Public URL did not return 200 for {slug}: {status}")

        report["record_count"] = len(report["posts"])
        report["public_ok_count"] = sum(
            1
            for row in report["posts"]
            if row.get("public_status", {}).get("status_code") == 200
        )
        report["all_ok"] = report["record_count"] == report["public_ok_count"] == len(articles)
        report["status"] = "complete" if report["all_ok"] else "incomplete"
        report["completed_at"] = now_iso()
        save_json(report_path, report)
        if not report["all_ok"]:
            raise RuntimeError("Batch publication report is incomplete")
        print(
            json.dumps(
                {
                    "record_count": report["record_count"],
                    "public_ok_count": report["public_ok_count"],
                    "all_ok": report["all_ok"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    except Exception as exc:
        report["status"] = "failed"
        report["last_error"] = str(exc)
        report["failed_at"] = now_iso()
        save_json(report_path, report)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Preflight, smoke-test, or publish a prepared VEVO batch through the slug-safe content MCP."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--smoke", action="store_true", help="Create, verify, and delete one hidden test post.")
    mode.add_argument("--publish", action="store_true", help="Publish the prepared batch after all preflight gates.")
    parser.add_argument("--block-id", default="765")
    parser.add_argument("--articles", type=Path, default=DEFAULT_ARTICLES)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--smoke-report", type=Path, default=DEFAULT_SMOKE_REPORT)
    parser.add_argument(
        "--mcp-script",
        type=Path,
        default=Path(os.environ.get("VEVO_CONTENT_MCP_SCRIPT", str(DEFAULT_MCP_SCRIPT))),
    )
    args = parser.parse_args()

    with StdioMcpClient(args.mcp_script) as client:
        if args.smoke:
            run_smoke(client, args.block_id, args.smoke_report)
            print(json.dumps(load_json(args.smoke_report), ensure_ascii=False, indent=2))
            return 0
        run_batch(client, args.articles, args.report, args.block_id, args.publish)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
