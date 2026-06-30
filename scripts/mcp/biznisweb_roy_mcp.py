#!/usr/bin/env python3
"""Local MCP server for ROY/noze.sk BiznisWeb content operations.

This server reads the existing ROY runtime credentials from the reporting
repository and exposes narrowly scoped tools for Codex. Public GraphQL is kept
for read checks; news-post writes use the BiznisWeb admin endpoints because the
ROY GraphQL token rejects news mutations without a partner token/package.
"""

from __future__ import annotations

import ast
import datetime as dt
import html.parser
import json
import os
import re
import sys
import traceback
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


SERVER_NAME = "biznisweb-roy"
SERVER_VERSION = "0.3.0"
DEFAULT_REPO = Path(r"C:\Users\Patrik jankech\Desktop\biznisweb-creditnote-carrier-audit")
PUBLIC_MAGAZINE_URL = "https://www.noze.sk/ostry-magazin"

NOZE_MAGAZINE_PAGES = [
    {"page_id": "823", "title": "Ostr\u00fd magaz\u00edn", "role": "overview"},
    {"page_id": "824", "title": "No\u017ee recenzie", "role": "category"},
    {"page_id": "825", "title": "Rady a tipy o no\u017eoch", "role": "category"},
    {"page_id": "826", "title": "Typy \u010depel\u00ed no\u017eov", "role": "category"},
    {"page_id": "827", "title": "Materi\u00e1ly rukov\u00e4t\u00ed no\u017eov", "role": "category"},
    {"page_id": "828", "title": "Materi\u00e1ly \u010depel\u00ed no\u017eov", "role": "category"},
    {"page_id": "829", "title": "\u00dadr\u017eba a br\u00fasenie no\u017eov", "role": "category"},
    {"page_id": "830", "title": "Zna\u010dky a v\u00fdrobcovia", "role": "category"},
    {"page_id": "831", "title": "No\u017ee pod\u013ea pou\u017eitia", "role": "category"},
]


class PublicMagazineParser(html.parser.HTMLParser):
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
                href.startswith("/ostry-magazin")
                or href.startswith("/n/")
                or href.startswith("https://www.noze.sk/ostry-magazin")
                or href.startswith("https://www.noze.sk/n/")
            ):
                self.links.append({"text": text, "href": urllib.parse.urljoin(PUBLIC_MAGAZINE_URL, href)})
            self._current_href = None
            self._current_text = []

    def handle_data(self, data: str) -> None:
        if self.in_h1:
            self.h1_parts.append(data)
        if self._current_href:
            self._current_text.append(data)


def load_env_file(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"ROY env file not found: {path}")
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ[key] = value


def ensure_roy_env() -> Dict[str, str]:
    repo = Path(os.environ.get("BIZNISWEB_ROY_REPO", str(DEFAULT_REPO)))
    load_env_file(repo / "projects" / "roy" / ".env")
    api_url = os.environ.get("BIZNISWEB_API_URL", "").strip()
    token = os.environ.get("BIZNISWEB_API_TOKEN", "").strip()
    username = os.environ.get("BIZNISWEB_USERNAME", "").strip()
    password = os.environ.get("BIZNISWEB_PASSWORD", "").strip()
    if not api_url or not token:
        raise RuntimeError("ROY BIZNISWEB_API_URL/BIZNISWEB_API_TOKEN are missing")
    return {
        "repo": str(repo),
        "api_url": api_url,
        "token": token,
        "username_present": str(bool(username)).lower(),
        "password_present": str(bool(password)).lower(),
    }


def post_graphql(query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    env = ensure_roy_env()
    parsed_api_url = urllib.parse.urlparse(env["api_url"])
    if parsed_api_url.netloc.lower() == "roy.flox.sk":
        target_url = urllib.parse.urlunparse(parsed_api_url._replace(netloc="www.roy.sk"))
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


def parse_admin_object(text: str) -> Dict[str, Any]:
    source = (text or "").strip()
    if not source:
        return {}
    source = source.replace(r"\/", "/")
    normalized = re.sub(r"\bnull\b", "None", source)
    normalized = re.sub(r"\btrue\b", "True", normalized)
    normalized = re.sub(r"\bfalse\b", "False", normalized)
    try:
        parsed = ast.literal_eval(normalized)
    except (SyntaxError, ValueError):
        quoted = re.sub(r"([{\[,]\s*)([A-Za-z_][A-Za-z0-9_]*)\s*:", r"\1'\2':", normalized)
        parsed = ast.literal_eval(quoted)
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
    env = ensure_roy_env()
    parsed_api_url = urllib.parse.urlparse(env["api_url"])
    if not parsed_api_url.scheme or not parsed_api_url.netloc:
        raise RuntimeError(f"Unexpected BIZNISWEB_API_URL: {env['api_url']}")
    return f"{parsed_api_url.scheme}://{parsed_api_url.netloc}"


def admin_login() -> tuple[str, requests.Session, str]:
    ensure_roy_env()
    username = os.environ.get("BIZNISWEB_USERNAME", "").strip()
    password = os.environ.get("BIZNISWEB_PASSWORD", "").strip()
    if not username or not password:
        raise RuntimeError("ROY BIZNISWEB_USERNAME/BIZNISWEB_PASSWORD are missing")
    base_url = admin_base_url()
    session = requests.Session()
    session.headers.update({"User-Agent": "Codex ROY MCP"})
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
    url = f"https://www.noze.sk/n/{slug}"
    response = requests.get(url, headers={"User-Agent": "Codex ROY MCP"}, timeout=30, allow_redirects=False)
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


def now_posted() -> tuple[str, str]:
    now = dt.datetime.now()
    return now.strftime("%Y-%m-%d"), now.strftime("%H:%M:%S")


def news_payload(args: Dict[str, Any], existing: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    posted_date, posted_time = now_posted()
    source = dict(existing or {})
    active_source = source.get("active", False)
    payload: Dict[str, Any] = {
        "news_id": source.get("news_id", ""),
        "block_id": str(args.get("block_id") or source.get("block_id") or "").strip(),
        "title": str(args.get("title") if args.get("title") is not None else source.get("title") or "").strip(),
        "active": active_to_admin(args.get("visible", args.get("active", active_source))),
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
        "link": str(args.get("link") if args.get("link") is not None else source.get("link") or "").strip(),
        "keywords": str(args.get("keywords") if args.get("keywords") is not None else source.get("keywords") or ""),
        "description": str(args.get("description") if args.get("description") is not None else source.get("description") or ""),
        "image": str(args.get("image") if args.get("image") is not None else source.get("image") or ""),
        "image_title": str(args.get("image_title") if args.get("image_title") is not None else source.get("image_title") or ""),
        "image_alt": str(args.get("image_alt") if args.get("image_alt") is not None else source.get("image_alt") or ""),
    }
    if not payload["title"]:
        raise ValueError("title is required")
    if not payload["block_id"] and not payload["news_id"]:
        raise ValueError("block_id is required")
    if payload["active"] == "1" and not bool(args.get("confirm_visible", False)):
        raise ValueError("active/visible=true requires confirm_visible=true")
    return payload


def fetch_public_magazine() -> Dict[str, Any]:
    request = urllib.request.Request(PUBLIC_MAGAZINE_URL, headers={"User-Agent": "Codex ROY MCP smoke"})
    with urllib.request.urlopen(request, timeout=30) as response:
        body = response.read().decode("utf-8", "replace")
    parser = PublicMagazineParser()
    parser.feed(body)
    article_links = [link for link in parser.links if "/n/" in link["href"]]
    category_links = [link for link in parser.links if "/ostry-magazin/" in link["href"]]
    return {
        "url": PUBLIC_MAGAZINE_URL,
        "h1": " ".join("".join(parser.h1_parts).split()),
        "public_news_block_ids": sorted(set(parser.news_block_ids), key=int),
        "article_count_on_first_page": len({link["href"] for link in article_links}),
        "article_samples": article_links[:10],
        "category_links": category_links[:20],
    }


def tool_smoke(_args: Dict[str, Any]) -> Dict[str, Any]:
    env = ensure_roy_env()
    language_data = post_graphql("query { listLanguageVersions { id code name visible system_lang } }")
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
    public_magazine = fetch_public_magazine()
    languages = language_data.get("listLanguageVersions") or []
    return {
        "api_host": urllib.parse.urlparse(env["api_url"]).netloc,
        "token_present": True,
        "admin_username_present": env["username_present"] == "true",
        "admin_password_present": env["password_present"] == "true",
        "language_af": next((item for item in languages if str(item.get("code", "")).lower() == "af"), None),
        "product_samples": (product_data.get("getProductList") or {}).get("data") or [],
        "public_magazine": public_magazine,
    }


def tool_list_languages(_args: Dict[str, Any]) -> Dict[str, Any]:
    return post_graphql("query { listLanguageVersions { id code name visible system_lang timezone } }")


def tool_public_magazine(_args: Dict[str, Any]) -> Dict[str, Any]:
    return fetch_public_magazine()


def tool_noze_magazine_blocks(_args: Dict[str, Any]) -> Dict[str, Any]:
    pages: List[Dict[str, Any]] = []
    for page in NOZE_MAGAZINE_PAGES:
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
    rows = admin_list_news_posts(block_id, limit=limit)
    return {"block_id": block_id, "total": len(rows), "rows": rows}


def unique_post_matches(rows: List[Dict[str, Any]], payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    title = payload.get("title")
    link = payload.get("link")
    matches = []
    for row in rows:
        if title and row.get("title") == title:
            matches.append(row)
            continue
        if link and row.get("link") == link:
            matches.append(row)
    return matches


def tool_add_news_post(args: Dict[str, Any]) -> Dict[str, Any]:
    payload = news_payload(args)
    before_rows = admin_list_news_posts(payload["block_id"])
    duplicates = unique_post_matches(before_rows, payload)
    if duplicates:
        raise RuntimeError(
            "Duplicate candidate exists; refusing create until existing post is reviewed: "
            + json.dumps(duplicates[:3], ensure_ascii=False)
        )

    result = admin_post("/erp/pages/news/addcheck/", payload)
    if not result.get("success"):
        raise RuntimeError(json.dumps(result, ensure_ascii=False))

    after_rows = admin_list_news_posts(payload["block_id"])
    created = unique_post_matches(after_rows, payload)
    if len(created) != 1:
        raise RuntimeError(
            "Create succeeded but exact created post id could not be resolved safely: "
            + json.dumps({"matches": created, "response": result}, ensure_ascii=False)
        )
    post = created[0]
    return {
        "news_post": post,
        "active_requested": payload["active"] == "1",
        "admin_response": result,
        "public_status": public_status_for_slug(str(post.get("link") or payload.get("link") or "")),
    }


def tool_update_news_post(args: Dict[str, Any]) -> Dict[str, Any]:
    post_id = str(args.get("post_id") or "").strip()
    if not post_id:
        raise ValueError("post_id is required")
    existing = admin_get_news_post(post_id)
    payload = news_payload(args, existing=existing)
    payload["news_id"] = post_id
    result = admin_post("/erp/pages/news/editcheck/", payload)
    if not result.get("success"):
        raise RuntimeError(json.dumps(result, ensure_ascii=False))
    post = admin_get_news_post(post_id)
    return {
        "news_post": post,
        "admin_response": result,
        "public_status": public_status_for_slug(str(post.get("link") or payload.get("link") or "")),
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
    "roy_smoke": {
        "description": "Read-only ROY/noze.sk access smoke: GraphQL credentials, AF language, product samples, public magazine discovery.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        "handler": tool_smoke,
    },
    "roy_list_languages": {
        "description": "List ROY BiznisWeb language versions. Read-only.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        "handler": tool_list_languages,
    },
    "noze_public_magazine": {
        "description": "Scrape the public noze.sk Ostry magazin page and return public news block ids, categories and article samples. Read-only.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        "handler": tool_public_magazine,
    },
    "noze_magazine_blocks": {
        "description": "List admin page/news block mapping for noze.sk Ostry magazin categories. Read-only.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        "handler": tool_noze_magazine_blocks,
    },
    "roy_list_news_posts": {
        "description": "List ROY/noze.sk BiznisWeb news posts in a specific admin news block. Read-only.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "block_id": {"type": "string"},
                "limit": {"type": "integer"},
            },
            "required": ["block_id"],
            "additionalProperties": False,
        },
        "handler": tool_list_news_posts,
    },
    "roy_add_news_post": {
        "description": "Create a ROY/noze.sk BiznisWeb news post through admin endpoints. Defaults to hidden draft; active/visible=true requires confirm_visible=true.",
        "inputSchema": {
            "type": "object",
            "properties": NEWS_FIELDS_SCHEMA,
            "required": ["block_id", "title"],
            "additionalProperties": False,
        },
        "handler": tool_add_news_post,
    },
    "roy_update_news_post": {
        "description": "Update a ROY/noze.sk BiznisWeb news post through admin endpoints by post_id. Publishing requires confirm_visible=true.",
        "inputSchema": {
            "type": "object",
            "properties": {"post_id": {"type": "string"}, **NEWS_FIELDS_SCHEMA},
            "required": ["post_id"],
            "additionalProperties": False,
        },
        "handler": tool_update_news_post,
    },
    "roy_delete_news_post": {
        "description": "Delete a ROY/noze.sk BiznisWeb news post by post_id. Requires confirm_delete=true.",
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
