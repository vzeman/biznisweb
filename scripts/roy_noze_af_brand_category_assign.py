#!/usr/bin/env python3
"""
Assign active noze.sk AF products to the Značky parent category and matching brand categories.

The ROY GraphQL API exposes product reads, but product-category assignment is
only writable through the ROY admin product form. This script preserves every
currently checked category and only adds missing AF brand category IDs.
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
import time
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import quote, urlparse

import requests


ROOT = Path(__file__).resolve().parents[1]
PROJECT_ENV = ROOT / "projects" / "roy" / ".env"
DEFAULT_OUTPUT = ROOT / "projects" / "roy" / "exports" / "noze-af-brand-category-assign-2026-06-11.json"

AF_LANG_CODE = "AF"
AF_ROOT_CATEGORY_ID = "4635"
BRANDS_PARENT_CATEGORY_ID = "6419"
PRODUCT_FORM_ROOTS = "variantStore_quanStore_quanInactives_variantCharges_alterStore_discussion_attributes"


GET_PRODUCTS_QUERY_WITH_PRODUCER = """
query NozeAfProductsWithProducer($lang: CountryCodeAlpha2!, $params: ProductParams) {
  getProductList(lang_code: $lang, params: $params) {
    data {
      id
      title
      import_code
      active
      link
      producer { id name }
    }
    pageInfo { hasNextPage nextCursor pageIndex totalPages }
  }
}
"""


GET_PRODUCTS_QUERY_BASIC = """
query NozeAfProductsBasic($lang: CountryCodeAlpha2!, $params: ProductParams) {
  getProductList(lang_code: $lang, params: $params) {
    data {
      id
      title
      import_code
      active
      link
    }
    pageInfo { hasNextPage nextCursor pageIndex totalPages }
  }
}
"""


GET_PRODUCT_QUERY = """
query NozeAfProduct($id: ID!) {
  getProduct(product_id: $id, lang_code: "AF") {
    id
    title
    import_code
    active
    link
    producer { id name }
    assigned_categories { id title }
  }
}
"""


@dataclass(frozen=True)
class BrandTarget:
    key: str
    category_id: str
    title: str
    aliases: tuple[str, ...]


BRAND_TARGETS: tuple[BrandTarget, ...] = (
    BrandTarget("opinel", "6420", "Nože Opinel", ("opinel",)),
    BrandTarget("morakniv", "6421", "Nože Morakniv", ("morakniv", "mora")),
    BrandTarget("walther", "6422", "Nože Walther", ("walther",)),
    BrandTarget("kizlyar", "6423", "Nože Kizlyar", ("kizlyar",)),
    BrandTarget("higonokami", "6424", "Nože Higonokami", ("higonokami",)),
    BrandTarget("ganzo", "6425", "Nože Ganzo", ("ganzo",)),
    BrandTarget("ruike", "6426", "Nože Ruike", ("ruike",)),
    BrandTarget("helle", "6427", "Nože Helle", ("helle",)),
    BrandTarget("cold steel", "6428", "Nože Cold Steel", ("cold steel",)),
    BrandTarget("civivi", "6429", "Nože Civivi", ("civivi",)),
    BrandTarget("victorinox", "6430", "Nože Victorinox", ("victorinox",)),
    BrandTarget("bestech", "6431", "Nože Bestech", ("bestech",)),
    BrandTarget("mikov", "6432", "Nože Mikov", ("mikov",)),
    BrandTarget("boker", "6433", "Nože Boker", ("boker",)),
    BrandTarget("joker", "6434", "Nože Joker", ("joker",)),
    BrandTarget("kanetsune", "6435", "Nože Kanetsune", ("kanetsune",)),
)


BRAND_CATEGORY_IDS = {target.category_id for target in BRAND_TARGETS}
EXCLUDED_PRODUCT_IDS = {"3326"}
EXCLUDED_TITLE_BRAND_ALIASES = ("nitecore",)


@dataclass
class ProductRecord:
    id: str
    title: str
    import_code: str
    active: bool
    link: str
    producer_id: str
    producer_name: str
    brand: BrandTarget
    match_source: str
    current_category_ids: List[str] = field(default_factory=list)
    missing_category_ids: List[str] = field(default_factory=list)
    admin_checked_category_ids: List[str] = field(default_factory=list)
    admin_verified_after_ids: List[str] = field(default_factory=list)
    applied: bool = False
    skipped: bool = False
    verified_after_ids: List[str] = field(default_factory=list)
    error: str = ""


def configure_stdout() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")


def load_env(path: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    if not path.exists():
        raise FileNotFoundError(f"Missing env file: {path}")
    for raw in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def derive_admin_base_url(api_url: str) -> str:
    parsed = urlparse(api_url)
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    return "https://roy.flox.sk"


def strip_tags(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", value or "")).strip()


def normalize(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", strip_tags(value or ""))
    asciiish = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", " ", asciiish.lower()).strip()


def contains_alias(haystack: str, alias: str) -> bool:
    pattern = r"(?<![a-z0-9])" + re.escape(alias) + r"(?![a-z0-9])"
    return re.search(pattern, haystack) is not None


def matching_targets(value: str) -> List[BrandTarget]:
    normalized = normalize(value)
    if not normalized:
        return []
    return [
        target
        for target in BRAND_TARGETS
        if any(contains_alias(normalized, alias) for alias in target.aliases)
    ]


def quote_js_object_keys(value: str) -> str:
    output: List[str] = []
    index = 0
    quote_char: Optional[str] = None
    escaped = False
    while index < len(value):
        char = value[index]
        if quote_char:
            output.append(char)
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote_char:
                quote_char = None
            index += 1
            continue
        if char in ("'", '"'):
            quote_char = char
            output.append(char)
            index += 1
            continue
        if char in ("{", ","):
            output.append(char)
            index += 1
            while index < len(value) and value[index].isspace():
                output.append(value[index])
                index += 1
            key_start = index
            if index < len(value) and (value[index].isalpha() or value[index] == "_"):
                index += 1
                while index < len(value) and (value[index].isalnum() or value[index] == "_"):
                    index += 1
                key_end = index
                while index < len(value) and value[index].isspace():
                    index += 1
                if index < len(value) and value[index] == ":":
                    output.append(f'"{value[key_start:key_end]}"')
                    output.append(value[key_end:index])
                    output.append(":")
                    index += 1
                    continue
                index = key_start
            continue
        output.append(char)
        index += 1
    return "".join(output)


def replace_js_literals(value: str) -> str:
    output: List[str] = []
    index = 0
    quote_char: Optional[str] = None
    escaped = False
    replacements = {"true": "True", "false": "False", "null": "None"}
    while index < len(value):
        char = value[index]
        if quote_char:
            output.append(char)
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote_char:
                quote_char = None
            index += 1
            continue
        if char in ("'", '"'):
            quote_char = char
            output.append(char)
            index += 1
            continue
        replaced = False
        for source, target in replacements.items():
            end = index + len(source)
            if (
                value.startswith(source, index)
                and (index == 0 or not value[index - 1].isalnum())
                and (end == len(value) or not value[end].isalnum())
            ):
                output.append(target)
                index = end
                replaced = True
                break
        if replaced:
            continue
        output.append(char)
        index += 1
    return "".join(output)


def parse_admin_response(text: str) -> Any:
    body = text.strip()
    if not body:
        return {}
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        pass
    normalized = replace_js_literals(quote_js_object_keys(body))
    try:
        return ast.literal_eval(normalized)
    except (SyntaxError, ValueError) as exc:
        raise RuntimeError(f"Could not parse admin response: {body[:300]}") from exc


def js_serialized_length(value: str) -> int:
    encoded = quote(value, safe="-_.!~*'()")
    return len(re.sub(r"%[0-9A-Fa-f]{2}", "x", encoded))


def php_serialize(value: Any) -> str:
    if value is None:
        return "N;"
    if isinstance(value, bool):
        return f"b:{1 if value else 0};"
    if isinstance(value, int):
        return f"i:{value};"
    if isinstance(value, float):
        return f"d:{value};"
    if isinstance(value, str):
        return f's:{js_serialized_length(value)}:"{value}";'
    if isinstance(value, list):
        body = "".join(php_serialize(index) + php_serialize(str(item)) for index, item in enumerate(value))
        return f"a:{len(value)}:{{{body}}}"
    if isinstance(value, dict):
        body = ""
        for key, item in value.items():
            serialized_key = int(key) if str(key).isdigit() else str(key)
            body += php_serialize(serialized_key) + php_serialize(item)
        return f"a:{len(value)}:{{{body}}}"
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def sort_ids(values: Iterable[str]) -> List[str]:
    return sorted({str(value) for value in values}, key=lambda item: int(item) if item.isdigit() else item)


class BiznisWebGraphqlClient:
    def __init__(self, api_url: str, api_token: str) -> None:
        self.api_url = api_url
        self.product_list_fallback_pages: List[int] = []
        self.session = requests.Session()
        self.session.headers.update({
            "BW-API-Key": f"Token {api_token}",
            "User-Agent": "biznisweb-reporting/roy-noze-af-brand-category-assign",
        })

    def graphql(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        last_error = ""
        max_attempts = 8
        for attempt in range(1, max_attempts + 1):
            response = self.session.post(
                self.api_url,
                json={"query": query, "variables": variables or {}},
                timeout=(10, 60),
            )
            try:
                payload = response.json()
            except ValueError:
                last_error = json.dumps({
                    "status": response.status_code,
                    "body": response.text[:500],
                }, ensure_ascii=False)
                if attempt < max_attempts and "NozeAfProductsWithProducer" not in query:
                    time.sleep(min(15.0, 2.0 * attempt))
                    continue
                raise RuntimeError(last_error)
            if response.status_code < 400 and not payload.get("errors"):
                return payload["data"]

            last_error = json.dumps({
                "status": response.status_code,
                "errors": payload.get("errors"),
            }, ensure_ascii=False)
            retryable = response.status_code >= 500 or any(
                ((error.get("extensions") or {}).get("category") == "internal")
                for error in payload.get("errors") or []
            )
            if retryable and attempt < max_attempts and "NozeAfProductsWithProducer" not in query:
                time.sleep(min(15.0, 2.0 * attempt))
                continue
            raise RuntimeError(last_error)
        raise RuntimeError(last_error)

    def get_product(self, product_id: str) -> Optional[Dict[str, Any]]:
        data = self.graphql(GET_PRODUCT_QUERY, {"id": str(product_id)})
        return data.get("getProduct")

    def list_products(self, include_inactive: bool = False) -> List[Dict[str, Any]]:
        products: List[Dict[str, Any]] = []
        cursor: Optional[int] = None
        while True:
            params: Dict[str, Any] = {"limit": 30}
            if cursor is not None:
                params["cursor"] = cursor
            try:
                data = self.graphql(GET_PRODUCTS_QUERY_WITH_PRODUCER, {"lang": AF_LANG_CODE, "params": params})
            except RuntimeError:
                page_index = int(cursor or 0)
                self.product_list_fallback_pages.append(page_index)
                data = self.graphql(GET_PRODUCTS_QUERY_BASIC, {"lang": AF_LANG_CODE, "params": params})
            block = data["getProductList"]
            for product in block.get("data") or []:
                if include_inactive or product.get("active"):
                    products.append(product)
            page = block.get("pageInfo") or {}
            if not page.get("hasNextPage"):
                break
            cursor = page.get("nextCursor")
            if cursor is None:
                break
            time.sleep(0.25)
        return products

    def get_product_categories(self, product_id: str) -> List[Dict[str, str]]:
        product = self.get_product(product_id) or {}
        return product.get("assigned_categories") or []


class BiznisWebAdminClient:
    def __init__(self, base_url: str, username: str, password: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.arf_token = ""
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "biznisweb-reporting/roy-noze-af-brand-category-assign-admin",
        })

    @staticmethod
    def extract_arf(*values: str) -> str:
        for value in values:
            match = re.search(r"[?&]arf=([a-zA-Z0-9]+)", value or "")
            if match:
                return match.group(1)
            match = re.search(
                r"var\s+CsrfToken\s*=\s*function\s*\(\)\s*\{\s*var\s+\w+\s*=\s*'([a-zA-Z0-9]+)'",
                value or "",
            )
            if match:
                return match.group(1)
        return ""

    def login(self) -> None:
        login_page = self.session.get(f"{self.base_url}/erp/main/login", timeout=(10, 30))
        login_page.raise_for_status()
        login_arf = self.extract_arf(login_page.url, login_page.text)
        login_response = self.session.post(
            f"{self.base_url}/admin/login/authenticate/",
            data={
                "username": self.username,
                "password": self.password,
                "res": "1440x900",
                "arf": login_arf,
            },
            allow_redirects=True,
            timeout=(10, 30),
        )
        login_response.raise_for_status()
        payload = parse_admin_response(login_response.text)
        if not payload.get("success"):
            raise RuntimeError(f"Admin login failed: {str(payload)[:300]}")

        redirect = payload.get("redirect") or payload.get("url") or "/erp/"
        redirect_url = redirect if str(redirect).startswith("http") else f"{self.base_url}{redirect}"
        dashboard = self.session.get(redirect_url, allow_redirects=True, timeout=(10, 30))
        dashboard.raise_for_status()
        self.arf_token = str(payload.get("arf") or "") or self.extract_arf(dashboard.url, dashboard.text)
        if not self.arf_token:
            raise RuntimeError("Admin login succeeded, but no ARF token was found.")

    def get_product_form(self, product_id: str, active_category_id: str) -> Dict[str, str]:
        response = self.session.get(
            f"{self.base_url}/erp/products/main/init/{product_id}/{active_category_id}/{PRODUCT_FORM_ROOTS}",
            headers={"X-Requested-With": "XMLHttpRequest"},
            timeout=(10, 60),
        )
        if response.status_code >= 400:
            raise RuntimeError(f"Product form load failed for {product_id}: HTTP {response.status_code}")
        payload = parse_admin_response(response.text)
        if not payload.get("success") or not isinstance(payload.get("data"), dict):
            raise RuntimeError(f"Product form load failed for {product_id}: {str(payload)[:300]}")
        data = payload["data"]
        if str(data.get("product_id")) != str(product_id):
            raise RuntimeError(f"Product form mismatch: expected {product_id}, got {data.get('product_id')}")
        return {key: "" if value is None else str(value) for key, value in data.items()}

    def get_checked_category_ids(self, product_id: str) -> List[str]:
        response = self.session.get(
            f"{self.base_url}/erp/products/main/categoryTree/{product_id}",
            headers={"X-Requested-With": "XMLHttpRequest"},
            timeout=(10, 90),
        )
        if response.status_code >= 400:
            raise RuntimeError(f"Category tree load failed for {product_id}: HTTP {response.status_code}")
        nodes = parse_admin_response(response.text)
        if not isinstance(nodes, list):
            raise RuntimeError(f"Unexpected category tree response for {product_id}: {str(nodes)[:300]}")
        checked: List[str] = []

        def walk(items: Iterable[Dict[str, Any]]) -> None:
            for item in items:
                if item.get("checked"):
                    checked.append(str(item["id"]))
                children = item.get("children") or []
                if children:
                    walk(children)

        walk(nodes)
        return sort_ids(checked)

    def add_product_categories(
        self,
        product_id: str,
        target_category_ids: Iterable[str],
        active_category_id: str,
    ) -> tuple[List[str], List[str]]:
        before = self.get_checked_category_ids(product_id)
        target_ids = sort_ids(target_category_ids)
        after_ids = sort_ids(before + target_ids)
        if before == after_ids:
            return before, before

        form = self.get_product_form(product_id, active_category_id)
        form["product_id"] = str(product_id)
        form["nodes"] = php_serialize(after_ids)
        form["nodes_origin"] = php_serialize(before)
        response = self.session.post(
            f"{self.base_url}/erp/products/main/editcheck/{active_category_id}?arf={self.arf_token}",
            data=form,
            headers={"X-Requested-With": "XMLHttpRequest"},
            timeout=(10, 90),
        )
        if response.status_code >= 400:
            raise RuntimeError(f"Product save failed for {product_id}: HTTP {response.status_code}")
        payload = parse_admin_response(response.text)
        if not payload.get("success"):
            raise RuntimeError(f"Product save failed for {product_id}: {str(payload)[:300]}")

        verified = self.get_checked_category_ids(product_id)
        hidden_under_checked_brands = BRAND_CATEGORY_IDS if BRANDS_PARENT_CATEGORY_ID in verified else set()
        removed_existing = sorted(set(before) - set(verified) - hidden_under_checked_brands, key=int)
        if removed_existing:
            raise RuntimeError(f"Product {product_id} lost existing categories after save: {removed_existing}")
        return before, verified


def match_brand(product: Dict[str, Any]) -> Optional[tuple[BrandTarget, str]]:
    if str(product.get("id") or "") in EXCLUDED_PRODUCT_IDS:
        return None
    producer = product.get("producer") or {}
    producer_name = producer.get("name") or ""
    normalized_title = normalize(product.get("title") or "")
    if any(contains_alias(normalized_title, alias) for alias in EXCLUDED_TITLE_BRAND_ALIASES):
        return None
    producer_matches = matching_targets(producer_name)
    title_matches = matching_targets(product.get("title") or "")

    if producer_matches:
        return producer_matches[0], "producer"
    if producer_name.strip():
        return None

    if len(title_matches) == 1:
        return title_matches[0], "title_without_producer"
    return None


def build_records(products: Iterable[Dict[str, Any]]) -> List[ProductRecord]:
    records: List[ProductRecord] = []
    for product in products:
        matched = match_brand(product)
        if not matched:
            continue
        brand, source = matched
        producer = product.get("producer") or {}
        records.append(ProductRecord(
            id=str(product["id"]),
            title=strip_tags(product.get("title") or ""),
            import_code=str(product.get("import_code") or ""),
            active=bool(product.get("active")),
            link=product.get("link") or "",
            producer_id=str(producer.get("id") or ""),
            producer_name=producer.get("name") or "",
            brand=brand,
            match_source=source,
        ))
    return records


def record_to_json(record: ProductRecord) -> Dict[str, Any]:
    return {
        "id": record.id,
        "title": record.title,
        "import_code": record.import_code,
        "active": record.active,
        "link": record.link,
        "producer_id": record.producer_id,
        "producer_name": record.producer_name,
        "brand_key": record.brand.key,
        "brand_category_id": record.brand.category_id,
        "brand_category_title": record.brand.title,
        "target_category_ids": [BRANDS_PARENT_CATEGORY_ID, record.brand.category_id],
        "match_source": record.match_source,
        "current_category_ids": record.current_category_ids,
        "missing_category_ids": record.missing_category_ids,
        "admin_checked_category_ids": record.admin_checked_category_ids,
        "admin_verified_after_ids": record.admin_verified_after_ids,
        "applied": record.applied,
        "skipped": record.skipped,
        "verified_after_ids": record.verified_after_ids,
        "error": record.error,
    }


def summarize(records: Iterable[ProductRecord]) -> Dict[str, Any]:
    items = list(records)
    by_brand: Dict[str, Dict[str, int]] = {}
    by_source: Dict[str, int] = {}
    for record in items:
        brand = by_brand.setdefault(record.brand.key, {
            "candidates": 0,
            "applied": 0,
            "skipped": 0,
            "errors": 0,
        })
        brand["candidates"] += 1
        brand["applied"] += 1 if record.applied else 0
        brand["skipped"] += 1 if record.skipped else 0
        brand["errors"] += 1 if record.error else 0
        by_source[record.match_source] = by_source.get(record.match_source, 0) + 1
    return {
        "candidate_count": len(items),
        "applied_count": sum(1 for item in items if item.applied),
        "skipped_count": sum(1 for item in items if item.skipped),
        "error_count": sum(1 for item in items if item.error),
        "needs_update_count": sum(1 for item in items if item.missing_category_ids),
        "by_brand": by_brand,
        "by_match_source": by_source,
    }


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    configure_stdout()
    parser = argparse.ArgumentParser(description="Assign noze.sk AF products to matching brand categories.")
    parser.add_argument("--apply", action="store_true", help="Apply category additions through ROY admin.")
    parser.add_argument("--admin-check", action="store_true", help="Load admin category trees during dry-run.")
    parser.add_argument("--include-inactive", action="store_true", help="Include inactive AF products.")
    parser.add_argument("--offset", type=int, default=0, help="Skip the first N matched products before processing.")
    parser.add_argument("--limit", type=int, default=0, help="Process only the first N matched products.")
    parser.add_argument("--product-id", action="append", default=[], help="Process only this product ID. Can be used more than once.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--admin-base-url", default="")
    parser.add_argument("--active-category-id", default=AF_ROOT_CATEGORY_ID)
    parser.add_argument("--progress-every", type=int, default=25)
    args = parser.parse_args()

    env = load_env(PROJECT_ENV)
    api_url = env.get("BIZNISWEB_API_URL") or "https://roy.flox.sk/api/graphql"
    api_token = env.get("BIZNISWEB_API_TOKEN")
    if not api_token:
        raise RuntimeError("BIZNISWEB_API_TOKEN missing in projects/roy/.env")

    graphql = BiznisWebGraphqlClient(api_url, api_token)
    if args.product_id:
        products = []
        for product_id in args.product_id:
            product = graphql.get_product(str(product_id))
            if product and (args.include_inactive or product.get("active")):
                products.append(product)
    else:
        products = graphql.list_products(include_inactive=args.include_inactive)
    records = build_records(products)
    if args.offset:
        records = records[args.offset :]
    if args.limit:
        records = records[: args.limit]

    admin: Optional[BiznisWebAdminClient] = None
    if args.apply or args.admin_check:
        username = env.get("BIZNISWEB_USERNAME")
        password = env.get("BIZNISWEB_PASSWORD")
        if not username or not password:
            raise RuntimeError("BIZNISWEB_USERNAME/BIZNISWEB_PASSWORD missing in projects/roy/.env")
        admin = BiznisWebAdminClient(args.admin_base_url or derive_admin_base_url(api_url), username, password)
        admin.login()

    for index, record in enumerate(records, start=1):
        target_ids = [BRANDS_PARENT_CATEGORY_ID, record.brand.category_id]
        try:
            if not admin:
                record.missing_category_ids = target_ids
            else:
                record.admin_checked_category_ids = admin.get_checked_category_ids(record.id)
                before_graphql_ids = sort_ids(category["id"] for category in graphql.get_product_categories(record.id))
                record.current_category_ids = before_graphql_ids
                record.missing_category_ids = sorted(set(target_ids) - set(before_graphql_ids), key=int)

                if not args.apply:
                    record.skipped = not record.missing_category_ids
                elif not record.missing_category_ids:
                    record.verified_after_ids = before_graphql_ids
                    record.admin_verified_after_ids = record.admin_checked_category_ids
                    record.skipped = True
                else:
                    before_admin, verified_admin = admin.add_product_categories(
                        record.id,
                        target_ids,
                        args.active_category_id,
                    )
                    record.admin_checked_category_ids = before_admin
                    record.admin_verified_after_ids = verified_admin
                    after_graphql_ids = sort_ids(category["id"] for category in graphql.get_product_categories(record.id))
                    record.verified_after_ids = after_graphql_ids
                    missing_after = sorted(set(target_ids) - set(after_graphql_ids), key=int)
                    if missing_after:
                        raise RuntimeError(f"Product {record.id} missing target categories after save: {missing_after}")
                    record.applied = True
        except Exception as exc:
            record.error = str(exc)[:800]

        if args.progress_every and index % args.progress_every == 0:
            print(
                f"progress={index}/{len(records)} applied={sum(1 for item in records if item.applied)} "
                f"errors={sum(1 for item in records if item.error)} product_id={record.id}"
            )
            sys.stdout.flush()
        time.sleep(0.12 if not args.apply else 0.2)

    sample_verified: List[Dict[str, Any]] = []
    if args.apply:
        for record in records[:20]:
            if record.error:
                continue
            try:
                categories = graphql.get_product_categories(record.id)
                sample_verified.append({
                    "product_id": record.id,
                    "brand_category_id": record.brand.category_id,
                    "graphql_category_ids": [category["id"] for category in categories],
                    "has_parent": any(category["id"] == BRANDS_PARENT_CATEGORY_ID for category in categories),
                    "has_brand": any(category["id"] == record.brand.category_id for category in categories),
                })
            except Exception as exc:
                sample_verified.append({"product_id": record.id, "error": str(exc)[:500]})

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": "apply" if args.apply else "dry-run-admin-check" if args.admin_check else "dry-run",
        "target": {
            "project": "roy",
            "language_code": AF_LANG_CODE.lower(),
            "active_only": not args.include_inactive,
            "active_category_id_for_admin_save": args.active_category_id,
            "brand_parent_category_id": BRANDS_PARENT_CATEGORY_ID,
            "brand_categories": [
                {
                    "key": target.key,
                    "category_id": target.category_id,
                    "title": target.title,
                    "aliases": list(target.aliases),
                }
                for target in BRAND_TARGETS
            ],
        },
        "product_count_seen": len(products),
        "offset": args.offset,
        "limit": args.limit,
        "product_list_fallback_pages": graphql.product_list_fallback_pages,
        "summary": summarize(records),
        "records": [record_to_json(record) for record in records],
        "graphql_sample_verification": sample_verified,
    }
    write_json(args.output, payload)

    summary = payload["summary"]
    print(f"mode={payload['mode']}")
    print(f"product_count_seen={len(products)}")
    print(f"candidate_count={summary['candidate_count']}")
    print(f"needs_update_count={summary['needs_update_count']}")
    print(f"applied_count={summary['applied_count']}")
    print(f"skipped_count={summary['skipped_count']}")
    print(f"error_count={summary['error_count']}")
    print(f"output={args.output}")
    for key, item in sorted(summary["by_brand"].items()):
        print(
            f"- {key}: candidates={item['candidates']} applied={item['applied']} "
            f"skipped={item['skipped']} errors={item['errors']}"
        )
    return 1 if summary["error_count"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
