#!/usr/bin/env python3
"""Local MCP server for VEVO Blog BiznisWeb content operations.

This server reads untracked VEVO credentials and exposes narrowly scoped,
slug-safe tools for Codex. News-post writes use the BiznisWeb admin endpoints
so the exact link, metadata, and rich HTML can be preserved. New posts default
to hidden drafts.
"""

from __future__ import annotations

import ast
import datetime as dt
import html.parser
import json
import os
import re
import sys
import time
import traceback
import urllib.parse
import urllib.request
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


SERVER_NAME = "biznisweb-vevo-content"
SERVER_VERSION = "0.3.3"
CONTENT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LEGACY_REPORTING_REPO = (
    Path.home() / "Desktop" / "biznisweb-creditnote-carrier-audit"
)
PUBLIC_BLOG_URL = "https://www.vevo.sk/blog"
PUBLIC_BASE_URL = "https://www.vevo.sk"
CLEAN_SLUG_RE = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")
DUPLICATE_SCAN_LIMIT = 2000

VEVO_BLOG_PAGES = [
    {"page_id": "309", "title": "Blog", "role": "blog"},
]


class PublicBlogParser(html.parser.HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.news_block_ids: List[str] = []
        self.in_h1 = False
        self.h1_parts: List[str] = []
        self.links: List[Dict[str, str]] = []
        self._current_href: Optional[str] = None
        self._current_text: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[tuple[str, Optional[str]]]) -> None:
        attrs_dict = {key: value or "" for key, value in attrs}
        if tag == "div":
            value = attrs_dict.get("id", "")
            if re.fullmatch(r"block-\d+", value):
                class_value = attrs_dict.get("class", "")
                if "blockNews" in class_value or "blocknews" in class_value.lower():
                    self.news_block_ids.append(value.replace("block-", "", 1))
        if tag == "h1":
            self.in_h1 = True
        if tag == "a":
            href = attrs_dict.get("href")
            if href:
                self._current_href = href
                self._current_text = []

    def handle_endtag(self, tag: str) -> None:
        if tag == "h1":
            self.in_h1 = False
        if tag == "a" and self._current_href:
            text = " ".join("".join(self._current_text).split())
            href = self._current_href
            if text and (
                href.startswith("/blog")
                or href.startswith("/n/")
                or href.startswith("https://www.vevo.sk/blog")
                or href.startswith("https://www.vevo.sk/n/")
            ):
                self.links.append({"text": text, "href": urllib.parse.urljoin(PUBLIC_BLOG_URL, href)})
            self._current_href = None
            self._current_text = []

    def handle_data(self, data: str) -> None:
        if self.in_h1:
            self.h1_parts.append(data)
        if self._current_href:
            self._current_text.append(data)


def load_env_file(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"VEVO env file not found: {path}")
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def resolve_env_file() -> Path:
    explicit = os.environ.get("VEVO_CONTENT_ENV_FILE", "").strip()
    if explicit:
        path = Path(explicit).expanduser()
        if not path.is_file():
            raise FileNotFoundError(f"VEVO_CONTENT_ENV_FILE does not exist: {path}")
        return path

    legacy_repo = Path(
        os.environ.get(
            "BIZNISWEB_VEVO_REPO",
            str(DEFAULT_LEGACY_REPORTING_REPO),
        )
    ).expanduser()
    candidates = [
        CONTENT_ROOT / ".env",
        legacy_repo / "projects" / "vevo" / ".env",
    ]
    for path in candidates:
        if path.is_file():
            return path
    rendered = ", ".join(str(path) for path in candidates)
    raise FileNotFoundError(f"VEVO credentials file not found; checked: {rendered}")


def ensure_vevo_env(*, require_api: bool = False) -> Dict[str, str]:
    env_file = resolve_env_file()
    load_env_file(env_file)
    api_url = os.environ.get("BIZNISWEB_API_URL", "").strip()
    token = os.environ.get("BIZNISWEB_API_TOKEN", "").strip()
    admin_url = os.environ.get("BIZNISWEB_ADMIN_BASE_URL", "").strip()
    if not admin_url and api_url:
        parsed_api_url = urllib.parse.urlparse(api_url)
        if parsed_api_url.scheme and parsed_api_url.netloc:
            admin_url = f"{parsed_api_url.scheme}://{parsed_api_url.netloc}"
    username = os.environ.get("BIZNISWEB_USERNAME", "").strip()
    password = os.environ.get("BIZNISWEB_PASSWORD", "").strip()
    if not admin_url:
        raise RuntimeError("VEVO BIZNISWEB_ADMIN_BASE_URL or BIZNISWEB_API_URL is missing")
    if require_api and (not api_url or not token):
        raise RuntimeError("VEVO BIZNISWEB_API_URL/BIZNISWEB_API_TOKEN are missing")
    return {
        "env_file": str(env_file),
        "api_url": api_url,
        "token": token,
        "admin_base_url": admin_url.rstrip("/"),
        "username_present": str(bool(username)).lower(),
        "password_present": str(bool(password)).lower(),
    }


def post_graphql(query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    env = ensure_vevo_env(require_api=True)
    parsed_api_url = urllib.parse.urlparse(env["api_url"])
    if parsed_api_url.netloc.lower() == "vevo.flox.sk":
        target_url = urllib.parse.urlunparse(parsed_api_url._replace(netloc="www.vevo.sk"))
    else:
        target_url = env["api_url"]
    headers = {
        "Content-Type": "application/json",
        "BW-API-Key": f"Token {env['token']}",
    }
    response = requests.post(
        target_url,
        headers=headers,
        json={"query": query, "variables": variables or {}},
        timeout=45,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"GraphQL HTTP {response.status_code}: {response.text[:1000]}")
    data = response.json()
    if data.get("errors"):
        raise RuntimeError(json.dumps(data["errors"], ensure_ascii=False))
    return data.get("data") or {}


def admin_object_to_python(source: str) -> str:
    result: List[str] = []
    index = 0
    quote: Optional[str] = None
    escaped = False
    while index < len(source):
        char = source[index]
        if quote:
            result.append(char)
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            index += 1
            continue
        if char in {"'", '"'}:
            quote = char
            result.append(char)
            index += 1
            continue
        if char.isalpha() or char == "_":
            end = index + 1
            while end < len(source) and (source[end].isalnum() or source[end] == "_"):
                end += 1
            token = source[index:end]
            lookahead = end
            while lookahead < len(source) and source[lookahead].isspace():
                lookahead += 1
            previous = next((item for item in reversed(result) if not item.isspace()), "")
            if lookahead < len(source) and source[lookahead] == ":" and previous in "{[,":
                result.append(repr(token))
            elif token == "null":
                result.append("None")
            elif token == "true":
                result.append("True")
            elif token == "false":
                result.append("False")
            else:
                result.append(token)
            index = end
            continue
        result.append(char)
        index += 1
    return "".join(result)


def parse_admin_object(text: str) -> Dict[str, Any]:
    source = (text or "").strip()
    if not source:
        return {}
    source = source.replace(r"\/", "/")
    try:
        parsed = json.loads(source)
    except json.JSONDecodeError:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", SyntaxWarning)
            parsed = ast.literal_eval(admin_object_to_python(source))
    if not isinstance(parsed, dict):
        raise ValueError("BiznisWeb admin response is not an object")
    return parsed


def extract_arf(text: str) -> str:
    match = re.search(r"[?&]arf=([a-zA-Z0-9]+)", text or "")
    if not match:
        match = re.search(
            r"CsrfToken\s*=\s*function\s*\(\)\s*\{\s*var\s+\w+\s*=\s*'([a-zA-Z0-9]+)'",
            text or "",
        )
    return match.group(1) if match else ""


def admin_base_url() -> str:
    env = ensure_vevo_env()
    parsed_admin_url = urllib.parse.urlparse(env["admin_base_url"])
    if not parsed_admin_url.scheme or not parsed_admin_url.netloc:
        raise RuntimeError(
            f"Unexpected BIZNISWEB_ADMIN_BASE_URL: {env['admin_base_url']}"
        )
    return env["admin_base_url"]


def admin_login() -> tuple[str, requests.Session, str]:
    ensure_vevo_env()
    username = os.environ.get("BIZNISWEB_USERNAME", "").strip()
    password = os.environ.get("BIZNISWEB_PASSWORD", "").strip()
    if not username or not password:
        raise RuntimeError("VEVO BIZNISWEB_USERNAME/BIZNISWEB_PASSWORD are missing")
    base_url = admin_base_url()
    session = requests.Session()
    session.headers.update({"User-Agent": "Codex VEVO Content MCP"})
    login_page = session.get(f"{base_url}/erp/main/login", timeout=30)
    login_page.raise_for_status()
    arf = extract_arf(login_page.text)
    response = session.post(
        f"{base_url}/admin/login/authenticate/",
        data={"username": username, "password": password, "res": "1890x900", "arf": arf},
        timeout=30,
        allow_redirects=True,
    )
    response.raise_for_status()
    pages = session.get(f"{base_url}/erp/main/pages", timeout=30)
    pages.raise_for_status()
    arf = extract_arf(pages.text) or arf
    if not arf:
        raise RuntimeError("Could not resolve BiznisWeb admin CSRF token")
    return base_url, session, arf


def admin_post(path: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    base_url, session, arf = admin_login()
    payload = dict(data or {})
    payload.setdefault("arf", arf)
    response = session.post(f"{base_url}{path}", data=payload, timeout=45)
    response.raise_for_status()
    return parse_admin_object(response.text)


def admin_list_blocks(page_id: str) -> List[Dict[str, Any]]:
    data = admin_post(f"/erp/pages/blocks/getListJson/{page_id}", {"start": 0, "limit": 200})
    return data.get("rows") or []


def admin_list_news_posts(block_id: str, limit: int = 200) -> List[Dict[str, Any]]:
    data = admin_post(f"/erp/pages/news/getListJson/{block_id}", {"start": 0, "limit": limit})
    return data.get("rows") or []


def admin_get_news_post(post_id: str) -> Dict[str, Any]:
    data = admin_post(f"/erp/pages/news/getDetails/{post_id}", {})
    if not data.get("success"):
        raise RuntimeError(json.dumps(data, ensure_ascii=False))
    post = data.get("data") or {}
    if not post:
        raise RuntimeError(f"No news post detail returned for {post_id}")
    return post


def admin_delete_news_post(post_id: str) -> Dict[str, Any]:
    base_url, session, arf = admin_login()
    response = session.post(f"{base_url}/erp/pages/news/delete/{post_id}?arf={urllib.parse.quote(arf)}", timeout=30)
    response.raise_for_status()
    return parse_admin_object(response.text)


def public_status_for_slug(slug: str) -> Dict[str, Any]:
    if not slug:
        return {"checked": False}
    url = f"{PUBLIC_BASE_URL}/n/{slug}"
    response = requests.get(
        url,
        headers={"User-Agent": "Codex VEVO Content MCP"},
        timeout=30,
        allow_redirects=False,
    )
    return {
        "checked": True,
        "url": url,
        "status_code": response.status_code,
        "location": response.headers.get("location"),
    }


def active_to_admin(value: Any) -> str:
    if isinstance(value, str):
        return "1" if value.strip().lower() in {"1", "true", "yes", "on", "active", "visible"} else "0"
    return "1" if bool(value) else "0"


def normalize_admin_unicode(value: str) -> str:
    return value.encode("utf-16", "surrogatepass").decode("utf-16", "replace")


def now_posted() -> tuple[str, str]:
    now = dt.datetime.now()
    return now.strftime("%Y-%m-%d"), now.strftime("%H:%M:%S")


def validate_slug(value: Any) -> str:
    slug = str(value or "").strip().strip("/")
    if not slug:
        raise ValueError("link is required for slug-safe VEVO publishing")
    if not CLEAN_SLUG_RE.fullmatch(slug):
        raise ValueError("link must be a clean lowercase ASCII slug")
    if re.fullmatch(r"1{2,}", slug):
        raise ValueError("repeated-1 placeholder slugs are forbidden")
    return slug


def resolve_payload_slug(
    args: Dict[str, Any], source: Dict[str, Any], active: str
) -> str:
    explicit_link = args.get("link") is not None
    value = args.get("link") if explicit_link else source.get("link")
    try:
        return validate_slug(value)
    except ValueError:
        legacy_slug = str(value or "").strip().strip("/")
        if (
            source
            and not explicit_link
            and active == "0"
            and re.fullmatch(r"1{2,}", legacy_slug)
        ):
            return legacy_slug
        raise


def post_id_from_record(record: Dict[str, Any]) -> str:
    return str(record.get("news_id") or record.get("id") or "").strip()


def assert_post_readback(post: Dict[str, Any], payload: Dict[str, Any]) -> None:
    failures = []
    for field in ("title", "link"):
        actual = normalize_admin_unicode(str(post.get(field) or "")).strip()
        expected = normalize_admin_unicode(str(payload.get(field) or "")).strip()
        if actual != expected:
            failures.append(
                {
                    "field": field,
                    "expected": expected,
                    "actual": actual,
                }
            )
    for field in ("short", "long"):
        expected = normalize_admin_unicode(str(payload.get(field) or ""))
        actual = normalize_admin_unicode(str(post.get(field) or ""))
        if expected and (len(actual) < max(20, int(len(expected) * 0.9))):
            failures.append(
                {
                    "field": field,
                    "expected_length": len(expected),
                    "actual_length": len(actual),
                }
            )
    if failures:
        raise RuntimeError(
            "VEVO admin readback does not match the requested post: "
            + json.dumps(failures, ensure_ascii=False)
        )


def wait_for_public_status(slug: str, expected: int, attempts: int = 6) -> Dict[str, Any]:
    status = public_status_for_slug(slug)
    for _ in range(max(0, attempts - 1)):
        if status.get("status_code") == expected:
            return status
        time.sleep(1.5)
        status = public_status_for_slug(slug)
    return status


def news_payload(args: Dict[str, Any], existing: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    posted_date, posted_time = now_posted()
    source = dict(existing or {})
    active_source = source.get("active", False)
    active = active_to_admin(args.get("visible", args.get("active", active_source)))
    payload: Dict[str, Any] = {
        "news_id": source.get("news_id", ""),
        "block_id": str(args.get("block_id") or source.get("block_id") or "").strip(),
        "title": str(args.get("title") if args.get("title") is not None else source.get("title") or "").strip(),
        "active": active,
        "date_posted": str(args.get("date_posted") or source.get("date_posted") or posted_date),
        "time_posted": str(args.get("time_posted") or source.get("time_posted") or posted_time),
        "date_from": str(args.get("date_from") if args.get("date_from") is not None else source.get("date_from") or ""),
        "time_from": str(args.get("time_from") if args.get("time_from") is not None else source.get("time_from") or ""),
        "date_until": str(args.get("date_until") if args.get("date_until") is not None else source.get("date_until") or ""),
        "time_until": str(args.get("time_until") if args.get("time_until") is not None else source.get("time_until") or ""),
        "commenting": str(args.get("commenting") or source.get("commenting") or "none"),
        "short": str(args.get("short") if args.get("short") is not None else source.get("short") or ""),
        "long": str(args.get("long") if args.get("long") is not None else source.get("long") or ""),
        "title_tag": str(args.get("title_tag") if args.get("title_tag") is not None else source.get("title_tag") or ""),
        "link": resolve_payload_slug(args, source, active),
        "keywords": str(args.get("keywords") if args.get("keywords") is not None else source.get("keywords") or ""),
        "description": str(args.get("description") if args.get("description") is not None else source.get("description") or ""),
        "image": str(args.get("image") if args.get("image") is not None else source.get("image") or ""),
        "image_title": str(args.get("image_title") if args.get("image_title") is not None else source.get("image_title") or ""),
        "image_alt": str(args.get("image_alt") if args.get("image_alt") is not None else source.get("image_alt") or ""),
    }
    payload = {
        key: normalize_admin_unicode(value) if isinstance(value, str) else value
        for key, value in payload.items()
    }
    if not payload["title"]:
        raise ValueError("title is required")
    if not payload["block_id"] and not payload["news_id"]:
        raise ValueError("block_id is required")
    if payload["active"] == "1" and not bool(args.get("confirm_visible", False)):
        raise ValueError("active/visible=true requires confirm_visible=true")
    return payload


def fetch_public_blog() -> Dict[str, Any]:
    request = urllib.request.Request(
        PUBLIC_BLOG_URL,
        headers={"User-Agent": "Codex VEVO Content MCP smoke"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        body = response.read().decode("utf-8", "replace")
    parser = PublicBlogParser()
    parser.feed(body)
    article_links = [link for link in parser.links if "/n/" in link["href"]]
    return {
        "url": PUBLIC_BLOG_URL,
        "h1": " ".join("".join(parser.h1_parts).split()),
        "public_news_block_ids": sorted(set(parser.news_block_ids), key=int),
        "article_count_on_first_page": len({link["href"] for link in article_links}),
        "article_samples": article_links[:10],
    }


def tool_smoke(_args: Dict[str, Any]) -> Dict[str, Any]:
    env = ensure_vevo_env()
    api_available = bool(env["api_url"] and env["token"])
    language_data: Dict[str, Any] = {}
    product_data: Dict[str, Any] = {}
    if api_available:
        language_data = post_graphql(
            "query { listLanguageVersions { id code name visible system_lang } }"
        )
        product_data = post_graphql(
            """
            query($lang_code: CountryCodeAlpha2!, $params: ProductParams) {
              getProductList(lang_code: $lang_code, params: $params) {
                data { id title active import_code }
              }
            }
            """,
            {"lang_code": "SK", "params": {"limit": 3}},
        )
    blocks = admin_list_blocks("309")
    news_blocks = [block for block in blocks if str(block.get("object")) == "news"]
    public_blog = fetch_public_blog()
    languages = language_data.get("listLanguageVersions") or []
    return {
        "admin_host": urllib.parse.urlparse(env["admin_base_url"]).netloc,
        "api_host": urllib.parse.urlparse(env["api_url"]).netloc if env["api_url"] else None,
        "api_token_present": bool(env["token"]),
        "env_file": env["env_file"],
        "admin_username_present": env["username_present"] == "true",
        "admin_password_present": env["password_present"] == "true",
        "language_sk": next((item for item in languages if str(item.get("code", "")).lower() == "sk"), None),
        "product_samples": (product_data.get("getProductList") or {}).get("data") or [],
        "blog_page_id": "309",
        "admin_news_blocks": news_blocks,
        "public_blog": public_blog,
    }


def tool_list_languages(_args: Dict[str, Any]) -> Dict[str, Any]:
    return post_graphql("query { listLanguageVersions { id code name visible system_lang timezone } }")


def tool_public_blog(_args: Dict[str, Any]) -> Dict[str, Any]:
    return fetch_public_blog()


def tool_vevo_blog_blocks(_args: Dict[str, Any]) -> Dict[str, Any]:
    pages: List[Dict[str, Any]] = []
    for page in VEVO_BLOG_PAGES:
        blocks = admin_list_blocks(page["page_id"])
        news_blocks = [
            {
                "block_id": str(block.get("block_id")),
                "box": block.get("box"),
                "object": block.get("object"),
                "contents": block.get("contents"),
            }
            for block in blocks
            if str(block.get("object")) == "news"
        ]
        pages.append({**page, "news_blocks": news_blocks, "all_blocks_count": len(blocks)})
    return {"pages": pages}


def tool_list_news_posts(args: Dict[str, Any]) -> Dict[str, Any]:
    block_id = str(args.get("block_id") or "").strip()
    if not block_id:
        raise ValueError("block_id is required")
    limit = int(args.get("limit") or 200)
    if limit < 1 or limit > DUPLICATE_SCAN_LIMIT:
        raise ValueError(f"limit must be between 1 and {DUPLICATE_SCAN_LIMIT}")
    rows = admin_list_news_posts(block_id, limit=limit)
    if bool(args.get("summary_only", False)):
        fields = (
            "news_id",
            "block_id",
            "active",
            "position",
            "title",
            "title_tag",
            "link",
            "date_posted",
            "time_posted",
            "url",
        )
        rows = [{field: row.get(field) for field in fields} for row in rows]
    return {"block_id": block_id, "total": len(rows), "rows": rows}


def tool_get_news_post(args: Dict[str, Any]) -> Dict[str, Any]:
    post_id = str(args.get("post_id") or "").strip()
    if not post_id:
        raise ValueError("post_id is required")
    return {
        "post_id": post_id,
        "news_post": admin_get_news_post(post_id),
    }


def unique_post_matches(rows: List[Dict[str, Any]], payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    title = str(payload.get("title") or "").strip().casefold()
    link = str(payload.get("link") or "").strip()
    matches = []
    for row in rows:
        row_title = str(row.get("title") or "").strip().casefold()
        row_link = str(row.get("link") or "").strip()
        if title and row_title == title:
            matches.append(row)
            continue
        if link and row_link == link:
            matches.append(row)
    return matches


def blocking_update_matches(
    rows: List[Dict[str, Any]], payload: Dict[str, Any], post_id: str
) -> List[Dict[str, Any]]:
    if payload.get("active") != "1":
        return []
    requested_link = str(payload.get("link") or "").strip()
    return [
        row
        for row in unique_post_matches(rows, payload)
        if post_id_from_record(row) != post_id
        and (
            active_to_admin(row.get("active")) == "1"
            or str(row.get("link") or "").strip() == requested_link
        )
    ]


def tool_add_news_post(args: Dict[str, Any]) -> Dict[str, Any]:
    payload = news_payload(args)
    if payload["active"] == "1":
        raise ValueError("New VEVO posts must be created hidden and published by a separate update")
    public_before = public_status_for_slug(payload["link"])
    if public_before.get("status_code") != 404:
        raise RuntimeError(
            "Public slug is not free; refusing create: "
            + json.dumps(public_before, ensure_ascii=False)
        )
    before_rows = admin_list_news_posts(payload["block_id"], limit=DUPLICATE_SCAN_LIMIT)
    duplicates = unique_post_matches(before_rows, payload)
    if duplicates:
        raise RuntimeError(
            "Duplicate candidate exists; refusing create until existing post is reviewed: "
            + json.dumps(duplicates[:3], ensure_ascii=False)
        )

    result = admin_post("/erp/pages/news/addcheck/", payload)
    if not result.get("success"):
        raise RuntimeError(json.dumps(result, ensure_ascii=False))

    after_rows = admin_list_news_posts(payload["block_id"], limit=DUPLICATE_SCAN_LIMIT)
    created = unique_post_matches(after_rows, payload)
    if len(created) != 1:
        raise RuntimeError(
            "Create succeeded but exact created post id could not be resolved safely: "
            + json.dumps({"matches": created, "response": result}, ensure_ascii=False)
        )
    post = created[0]
    post_id = post_id_from_record(post)
    if not post_id:
        raise RuntimeError(
            "Create succeeded but the created post id is missing: "
            + json.dumps(post, ensure_ascii=False)
        )
    details = admin_get_news_post(post_id)
    assert_post_readback(details, payload)
    public_status = wait_for_public_status(payload["link"], expected=404)
    if public_status.get("status_code") != 404:
        raise RuntimeError(
            "Hidden VEVO draft unexpectedly resolves publicly: "
            + json.dumps(
                {"post_id": post_id, "public_status": public_status},
                ensure_ascii=False,
            )
        )
    return {
        "news_post": details,
        "post_id": post_id,
        "active_requested": False,
        "admin_response": result,
        "public_before": public_before,
        "public_status": public_status,
    }


def tool_update_news_post(args: Dict[str, Any]) -> Dict[str, Any]:
    post_id = str(args.get("post_id") or "").strip()
    if not post_id:
        raise ValueError("post_id is required")
    existing = admin_get_news_post(post_id)
    payload = news_payload(args, existing=existing)
    payload["news_id"] = post_id
    block_rows = admin_list_news_posts(payload["block_id"], limit=DUPLICATE_SCAN_LIMIT)
    duplicates = blocking_update_matches(block_rows, payload, post_id)
    if duplicates:
        raise RuntimeError(
            "Another post already has the requested title or slug; refusing update: "
            + json.dumps(duplicates[:3], ensure_ascii=False)
        )
    result = admin_post("/erp/pages/news/editcheck/", payload)
    if not result.get("success"):
        raise RuntimeError(json.dumps(result, ensure_ascii=False))
    post = admin_get_news_post(post_id)
    assert_post_readback(post, payload)
    expected_status = 200 if payload["active"] == "1" else 404
    public_status = wait_for_public_status(payload["link"], expected=expected_status)
    if public_status.get("status_code") != expected_status:
        raise RuntimeError(
            "VEVO public status did not reach the requested visibility: "
            + json.dumps(
                {
                    "post_id": post_id,
                    "expected_status": expected_status,
                    "public_status": public_status,
                },
                ensure_ascii=False,
            )
        )
    return {
        "news_post": post,
        "post_id": post_id,
        "admin_response": result,
        "public_status": public_status,
    }


def tool_delete_news_post(args: Dict[str, Any]) -> Dict[str, Any]:
    post_id = str(args.get("post_id") or "").strip()
    if not post_id:
        raise ValueError("post_id is required")
    if not bool(args.get("confirm_delete", False)):
        raise ValueError("delete requires confirm_delete=true")
    result = admin_delete_news_post(post_id)
    if not result.get("success"):
        raise RuntimeError(json.dumps(result, ensure_ascii=False))
    return {"deleted": True, "post_id": post_id, "admin_response": result}


NEWS_FIELDS_SCHEMA = {
    "block_id": {"type": "string"},
    "title": {"type": "string"},
    "short": {"type": "string"},
    "long": {"type": "string"},
    "link": {"type": "string"},
    "active": {"type": "boolean"},
    "visible": {"type": "boolean"},
    "confirm_visible": {"type": "boolean"},
    "date_posted": {"type": "string"},
    "time_posted": {"type": "string"},
    "date_from": {"type": "string"},
    "time_from": {"type": "string"},
    "date_until": {"type": "string"},
    "time_until": {"type": "string"},
    "commenting": {"type": "string"},
    "title_tag": {"type": "string"},
    "keywords": {"type": "string"},
    "description": {"type": "string"},
    "image": {"type": "string"},
    "image_title": {"type": "string"},
    "image_alt": {"type": "string"},
}


TOOLS = {
    "vevo_smoke": {
        "description": "Read-only VEVO access smoke: API credentials, admin Blog block discovery, product samples, and public Blog discovery.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        "handler": tool_smoke,
    },
    "vevo_list_languages": {
        "description": "List VEVO BiznisWeb language versions. Read-only.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        "handler": tool_list_languages,
    },
    "vevo_public_blog": {
        "description": "Scrape the public VEVO Blog page and return public news block ids and article samples. Read-only.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        "handler": tool_public_blog,
    },
    "vevo_blog_blocks": {
        "description": "List the VEVO admin Blog page/news block mapping. Read-only.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        "handler": tool_vevo_blog_blocks,
    },
    "vevo_list_news_posts": {
        "description": "List VEVO BiznisWeb news posts in a specific admin news block. Read-only.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "block_id": {"type": "string"},
                "limit": {"type": "integer"},
                "summary_only": {"type": "boolean"},
            },
            "required": ["block_id"],
            "additionalProperties": False,
        },
        "handler": tool_list_news_posts,
    },
    "vevo_get_news_post": {
        "description": "Read one VEVO BiznisWeb news post from the admin by post_id. Read-only.",
        "inputSchema": {
            "type": "object",
            "properties": {"post_id": {"type": "string"}},
            "required": ["post_id"],
            "additionalProperties": False,
        },
        "handler": tool_get_news_post,
    },
    "vevo_add_news_post": {
        "description": "Create a slug-safe VEVO Blog post as a hidden draft through admin endpoints. Direct visible create is forbidden.",
        "inputSchema": {
            "type": "object",
            "properties": NEWS_FIELDS_SCHEMA,
            "required": ["block_id", "title", "short", "long", "link"],
            "additionalProperties": False,
        },
        "handler": tool_add_news_post,
    },
    "vevo_update_news_post": {
        "description": "Update an existing VEVO Blog post by post_id with admin readback. Publishing requires confirm_visible=true.",
        "inputSchema": {
            "type": "object",
            "properties": {"post_id": {"type": "string"}, **NEWS_FIELDS_SCHEMA},
            "required": ["post_id"],
            "additionalProperties": False,
        },
        "handler": tool_update_news_post,
    },
    "vevo_delete_news_post": {
        "description": "Delete a VEVO Blog post by post_id. Requires confirm_delete=true.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "post_id": {"type": "string"},
                "confirm_delete": {"type": "boolean"},
            },
            "required": ["post_id", "confirm_delete"],
            "additionalProperties": False,
        },
        "handler": tool_delete_news_post,
    },
}


def send_response(message_id: Any, result: Any = None, error: Optional[Dict[str, Any]] = None) -> None:
    response: Dict[str, Any] = {"jsonrpc": "2.0", "id": message_id}
    if error is not None:
        response["error"] = error
    else:
        response["result"] = result
    sys.stdout.write(json.dumps(response, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def tool_descriptor(name: str, spec: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": name,
        "description": spec["description"],
        "inputSchema": spec["inputSchema"],
    }


def handle_request(message: Dict[str, Any]) -> None:
    method = message.get("method")
    message_id = message.get("id")
    params = message.get("params") or {}
    if method == "initialize":
        send_response(
            message_id,
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
            },
        )
        return
    if method == "tools/list":
        send_response(message_id, {"tools": [tool_descriptor(name, spec) for name, spec in TOOLS.items()]})
        return
    if method == "tools/call":
        name = params.get("name")
        args = params.get("arguments") or {}
        if name not in TOOLS:
            send_response(message_id, error={"code": -32602, "message": f"Unknown tool: {name}"})
            return
        try:
            result = TOOLS[name]["handler"](args)
            send_response(
                message_id,
                {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(result, ensure_ascii=False, indent=2),
                        }
                    ]
                },
            )
        except Exception as exc:
            send_response(
                message_id,
                error={
                    "code": -32000,
                    "message": str(exc),
                    "data": traceback.format_exc(limit=5),
                },
            )
        return
    if method in {"notifications/initialized", "initialized"}:
        return
    if method == "ping":
        send_response(message_id, {})
        return
    if message_id is not None:
        send_response(message_id, error={"code": -32601, "message": f"Method not found: {method}"})


def main() -> int:
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            message = json.loads(line)
            handle_request(message)
        except Exception as exc:
            send_response(None, error={"code": -32700, "message": str(exc)})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
