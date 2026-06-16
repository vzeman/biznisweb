#!/usr/bin/env python3
"""
Export, generate, apply, and verify short SEO content for roy.sk SK knife categories.

The roy.sk shop is managed through the ROY BiznisWeb GraphQL endpoint. This
script targets the Slovak Noze vsetky tree and avoids cross-shop mentions in
generated content.
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse

import requests


ROOT = Path(__file__).resolve().parents[1]
PROJECT_ENV = ROOT / "projects" / "roy" / ".env"
DEFAULT_OUTPUT = ROOT / "projects" / "roy" / "exports" / "roy-sk-category-seo-2026-06-16.json"

SK_LANG_ID = "1"
SK_LANG_CODE = "sk"
ROY_NOZE_ROOT_CATEGORY_ID = "6437"


GET_CATEGORY_QUERY = """
query CategoryForSeoWithProducts($id: ID!) {
  getCategory(category_id: $id, productListParams: {limit: 8}) {
    id
    title
    menu_title
    link
    intro
    bottom
    seo_title
    seo_description
    language { id code name system_lang visible }
    children_categories {
      id
      title
      link
      language { id code name }
    }
    products {
      data {
        id
        title
        active
        import_code
        link
      }
      pageInfo { hasNextPage nextCursor pageIndex totalPages }
    }
  }
}
"""


GET_CATEGORY_QUERY_NO_PRODUCTS = """
query CategoryForSeo($id: ID!) {
  getCategory(category_id: $id) {
    id
    title
    menu_title
    link
    intro
    bottom
    seo_title
    seo_description
    language { id code name system_lang visible }
    children_categories {
      id
      title
      link
      language { id code name }
    }
  }
}
"""


UPDATE_CATEGORY_MUTATION = """
mutation UpdateCategorySeo($id: ID!, $langId: ID!, $data: CategoryInput) {
  updateCategory(category_id: $id, lang_id: $langId, data: $data) {
    id
    title
    link
    intro
    bottom
    seo_title
    seo_description
    language { id code name }
  }
}
"""


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


def compact_space(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def strip_tags(value: Optional[str]) -> str:
    if not value:
        return ""
    return compact_space(re.sub(r"<[^>]+>", " ", value))


def trim_to(value: str, limit: int) -> str:
    value = compact_space(value)
    if len(value) <= limit:
        return value
    cut = value[: limit + 1].rsplit(" ", 1)[0].rstrip(" ,.;:-")
    return cut if len(cut) >= max(20, limit - 25) else value[:limit].rstrip(" ,.;:-")


def roy_url(link: str) -> str:
    if not link:
        return ""
    parsed = urlparse(link)
    path = parsed.path
    return f"https://www.roy.sk{path}"


def derive_admin_base_url(api_url: str) -> str:
    parsed = urlparse(api_url)
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    return "https://roy.flox.sk"


def quote_js_object_keys(value: str) -> str:
    output: List[str] = []
    index = 0
    quote: Optional[str] = None
    escaped = False
    while index < len(value):
        char = value[index]
        if quote:
            output.append(char)
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            index += 1
            continue
        if char in ("'", '"'):
            quote = char
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
    quote: Optional[str] = None
    escaped = False
    replacements = {"true": "True", "false": "False", "null": "None"}
    while index < len(value):
        char = value[index]
        if quote:
            output.append(char)
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            index += 1
            continue
        if char in ("'", '"'):
            quote = char
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


def parse_admin_response(text: str) -> Dict[str, Any]:
    body = text.strip()
    if not body:
        return {}
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        parsed = None
    if isinstance(parsed, dict):
        return parsed

    normalized = replace_js_literals(quote_js_object_keys(body))
    try:
        value = ast.literal_eval(normalized)
    except (SyntaxError, ValueError) as exc:
        raise RuntimeError(f"Could not parse admin response: {body[:300]}") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"Unexpected admin response type: {type(value).__name__}")
    return value


class BiznisWebClient:
    def __init__(self, api_url: str, api_token: str) -> None:
        self.api_url = api_url
        self.session = requests.Session()
        self.session.headers.update({
            "BW-API-Key": f"Token {api_token}",
            "User-Agent": "biznisweb-reporting/roy-sk-category-seo",
        })

    def graphql(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
        retry_internal: bool = True,
    ) -> Dict[str, Any]:
        last_error = ""
        for attempt in range(1, 6):
            response = self.session.post(
                self.api_url,
                json={"query": query, "variables": variables or {}},
                timeout=(10, 45),
            )
            try:
                payload = response.json()
            except ValueError:
                last_error = json.dumps({
                    "status": response.status_code,
                    "body": response.text[:500],
                }, ensure_ascii=False)
                if attempt < 5:
                    time.sleep(1.5 * attempt)
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
            if retryable and retry_internal and attempt < 5:
                time.sleep(1.5 * attempt)
                continue
            raise RuntimeError(last_error)
        raise RuntimeError(last_error)

    def get_category(self, category_id: str) -> Dict[str, Any]:
        try:
            data = self.graphql(GET_CATEGORY_QUERY, {"id": str(category_id)}, retry_internal=False)
        except RuntimeError:
            data = self.graphql(GET_CATEGORY_QUERY_NO_PRODUCTS, {"id": str(category_id)})
        category = data.get("getCategory")
        if not category:
            raise RuntimeError(f"Category not found: {category_id}")
        return category

    def update_category(self, category_id: str, payload: Dict[str, str]) -> Dict[str, Any]:
        data = self.graphql(
            UPDATE_CATEGORY_MUTATION,
            {"id": str(category_id), "langId": SK_LANG_ID, "data": payload},
        )
        return data["updateCategory"]


class BiznisWebAdminClient:
    def __init__(self, base_url: str, username: str, password: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.arf_token = ""
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "biznisweb-reporting/roy-sk-category-seo-admin",
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
        arf_token = self.extract_arf(login_page.url, login_page.text)
        login_response = self.session.post(
            f"{self.base_url}/admin/login/authenticate/",
            data={
                "username": self.username,
                "password": self.password,
                "res": "1440x900",
                "arf": arf_token,
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
        self.arf_token = (
            str(payload.get("arf") or "")
            or self.extract_arf(dashboard.url, dashboard.text)
        )
        if not self.arf_token:
            raise RuntimeError("Admin login succeeded, but no ARF token was found for category saves.")

    def load_category_form(self, category_id: str) -> Dict[str, Any]:
        response = self.session.get(
            f"{self.base_url}/erp/products/categories/init/{category_id}/attributes_filter_mirrors",
            headers={"X-Requested-With": "XMLHttpRequest"},
            timeout=(10, 45),
        )
        if response.status_code >= 400:
            raise RuntimeError(f"Admin category load failed for {category_id}: HTTP {response.status_code}")
        payload = parse_admin_response(response.text)
        if not payload.get("success") or not isinstance(payload.get("data"), dict):
            raise RuntimeError(f"Admin category load failed for {category_id}: {str(payload)[:300]}")
        data = payload["data"]
        if str(data.get("category_id")) != str(category_id):
            raise RuntimeError(f"Admin category load mismatch: expected {category_id}, got {data.get('category_id')}")
        return data

    def update_category(self, category_id: str, seo_payload: Dict[str, str]) -> Dict[str, str]:
        current = self.load_category_form(category_id)
        form_data = {key: "" if value is None else str(value) for key, value in current.items()}
        form_data.update({
            "category_id": str(category_id),
            "text": seo_payload["intro"],
            "bottom_text": seo_payload["bottom"],
            "title_tag": seo_payload["seo_title"],
            "description": seo_payload["seo_description"],
        })
        response = self.session.post(
            f"{self.base_url}/erp/products/categories/editcheck/?arf={self.arf_token}",
            data=form_data,
            headers={"X-Requested-With": "XMLHttpRequest"},
            timeout=(10, 45),
        )
        if response.status_code >= 400:
            raise RuntimeError(f"Admin category save failed for {category_id}: HTTP {response.status_code}")
        payload = parse_admin_response(response.text)
        if not payload.get("success"):
            raise RuntimeError(f"Admin category save failed for {category_id}: {str(payload)[:300]}")

        verified = self.load_category_form(category_id)
        return {
            "intro": strip_tags(str(verified.get("text") or "")),
            "bottom": strip_tags(str(verified.get("bottom_text") or "")),
            "seo_title": compact_space(str(verified.get("title_tag") or "")),
            "seo_description": compact_space(str(verified.get("description") or "")),
        }


@dataclass
class CategoryNode:
    id: str
    title: str
    link: str
    path_titles: List[str]
    current_intro: str = ""
    current_bottom: str = ""
    current_seo_title: str = ""
    current_seo_description: str = ""
    product_titles: List[str] = field(default_factory=list)
    children_ids: List[str] = field(default_factory=list)

    @property
    def path(self) -> str:
        return " > ".join(self.path_titles)

    @property
    def public_url(self) -> str:
        return roy_url(self.link)


def category_context(title: str, path: str, products: List[str]) -> Dict[str, str]:
    hay = f"{title} {path}".lower()
    product_hint = ""
    if products:
        names = [compact_space(p) for p in products if compact_space(p)]
        if names:
            product_hint = " Nájdete tu napríklad " + ", ".join(names[:3]) + "."

    if "brúsen" in hay or "brus" in hay or "ocieľ" in hay or "ociel" in hay:
        return {
            "noun": "brúsne pomôcky a príslušenstvo na údržbu nožov",
            "benefit": "udržiavanie ostria v dobrom stave doma, v dielni aj v teréne",
            "choice": "zrnitosti, typu ostria a spôsobu používania",
            "detail": product_hint or " Vyberajte podľa typu čepele, požadovanej jemnosti a skúseností s brúsením.",
        }
    if "kuchyn" in hay:
        return {
            "noun": "kuchynské nože a doplnky",
            "benefit": "krájanie, porciovanie a každodennú prípravu jedla",
            "choice": "tvaru čepele, materiálu rukoväte a štýlu práce v kuchyni",
            "detail": product_hint or " Hodí sa porovnať dĺžku čepele, vyváženie a nároky na údržbu.",
        }
    if "rezb" in hay or "lyžič" in hay or "drevorezb" in hay:
        return {
            "noun": "rezbárske nože a náradie",
            "benefit": "presnú prácu s drevom, detailné rezy a tvorbu tvarov",
            "choice": "typu výbrusu, tvaru čepele a veľkosti projektu",
            "detail": product_hint or " Pri výbere sledujte pohodlie úchopu, kontrolu rezu a vhodnosť na mäkké alebo tvrdšie drevo.",
        }
    if "sek" in hay or "mačet" in hay or "záhrad" in hay or "zahrad" in hay:
        return {
            "noun": "sekery, mačety a záhradné nástroje",
            "benefit": "prácu okolo domu, v záhrade, kempe aj lese",
            "choice": "dĺžky, hmotnosti, účelu a materiálu rukoväte",
            "detail": product_hint or " Vyberajte podľa toho, či riešite štiepanie, čistenie porastu alebo univerzálne outdoor použitie.",
        }
    if "sprej" in hay or "medve" in hay or "maco" in hay:
        return {
            "noun": "obranné spreje a výbavu proti medveďom",
            "benefit": "bezpečnejší pohyb v prírode, lese a horskom teréne",
            "choice": "objemu, dosahu, spôsobu nosenia a typu aktivity",
            "detail": product_hint or " Pri výbere sledujte najmä rýchlu dostupnosť, praktické puzdro a vhodnosť pre turistiku či poľovníctvo.",
        }
    if "značk" in hay or "znack" in hay:
        return {
            "noun": "nože podľa značiek a výrobcov",
            "benefit": "porovnanie sortimentu podľa značky, štýlu a dostupných modelov",
            "choice": "značky, typu noža, použitia a parametrov",
            "detail": product_hint or " Začnite značkou, ktorú poznáte, alebo porovnajte modely podľa účelu a konštrukcie.",
        }
    if "príslušen" in hay or "prislusen" in hay or "puzdr" in hay or "klip" in hay:
        return {
            "noun": "príslušenstvo k nožom a doplnky na používanie",
            "benefit": "praktické doplnenie, nosenie, uloženie alebo údržbu noža",
            "choice": "kompatibility, veľkosti, materiálu a spôsobu používania",
            "detail": product_hint or " Skontrolujte kompatibilitu s konkrétnym nožom, rozmery a spôsob uchytenia.",
        }
    if "výroba" in hay or "vyroba" in hay or "čepeľ" in hay or "cepel" in hay or "rukov" in hay or "materi" in hay:
        return {
            "noun": "materiály a príslušenstvo na výrobu nožov",
            "benefit": "stavbu, úpravu alebo servis vlastného noža",
            "choice": "materiálu, rozmerov, kompatibility a plánovaného použitia",
            "detail": product_hint or " Zamerajte sa na parametre ocele, rukoväťové materiály a diely, ktoré spolu konštrukčne sedia.",
        }
    if "outdoor" in hay or "bushcraft" in hay or "survival" in hay or "taktick" in hay:
        return {
            "noun": "outdoorové a taktické nože",
            "benefit": "turistiku, kempovanie, bushcraft a praktické úlohy v teréne",
            "choice": "konštrukcie, ocele, puzdra a bezpečnosti nosenia",
            "detail": product_hint or " Porovnajte pevnosť čepele, istotu úchopu a to, či potrebujete univerzálny alebo špecializovaný nôž.",
        }
    if "loveck" in hay or "poľovní" in hay or "polov" in hay or "hubár" in hay or "hubar" in hay:
        return {
            "noun": "lovecké, poľovnícke a hubárske nože",
            "benefit": "prácu v lese, pri love, zbere húb a spracovaní úlovku",
            "choice": "tvaru čepele, bezpečného puzdra a odolnosti materiálov",
            "detail": product_hint or " Praktický výber zohľadní jednoduché čistenie, istý úchop a nosenie v teréne.",
        }
    if "edc" in hay or "bežné použitie" in hay or "bezne pouzitie" in hay or "zatvár" in hay or "zatvar" in hay:
        return {
            "noun": "EDC a zatváracie nože",
            "benefit": "každodenné nosenie, drobné práce a rýchlu dostupnosť",
            "choice": "veľkosti, poistky, klipu, ocele a ergonómie",
            "detail": product_hint or " Pri každodennom nošení je dôležitá nízka hmotnosť, bezpečné zatváranie a pohodlné ovládanie.",
        }
    if "motýl" in hay or "motyl" in hay or "karambit" in hay or "vrhac" in hay or "na krk" in hay:
        return {
            "noun": "špeciálne typy nožov",
            "benefit": "zberateľské, tréningové alebo špecifické použitie",
            "choice": "konštrukcie, bezpečnosti, ovládania a účelu",
            "detail": product_hint or " Skontrolujte rozmery, spôsob nosenia a to, či ide o praktický, tréningový alebo zberateľský model.",
        }
    if "oceľ" in hay or "ocel" in hay or "damask" in hay or "nerez" in hay or "uhlík" in hay or "uhlik" in hay:
        return {
            "noun": "nože podľa typu ocele",
            "benefit": "porovnanie čepelí podľa držania ostria, húževnatosti a údržby",
            "choice": "ocele, tvrdosti, odolnosti voči korózii a použitia",
            "detail": product_hint or " Pri porovnaní ocele sledujte, či preferujete ľahšiu údržbu, agresívnejší rez alebo vyššiu odolnosť.",
        }
    return {
        "noun": "nože a príslušenstvo",
        "benefit": "praktické použitie doma, v práci, kuchyni aj prírode",
        "choice": "účelu, veľkosti, materiálu a spôsobu nosenia",
        "detail": product_hint or " Porovnajte parametre, konštrukciu, materiály a vyberte kategóriu podľa reálneho použitia.",
    }


def build_seo_title(public_title: str, path: str) -> str:
    hay = f"{public_title} {path}".lower()
    title_has_knife = "nož" in public_title.lower() or "noz" in public_title.lower()
    if "brúsen" in hay or "brus" in hay or "ocieľ" in hay or "ociel" in hay:
        title = public_title if title_has_knife else f"{public_title} nožov"
    elif "kuchyn" in hay:
        title = public_title
    elif "rezb" in hay or "lyžič" in hay or "drevorezb" in hay:
        title = f"{public_title} na prácu s drevom"
    elif "sek" in hay or "mačet" in hay or "záhrad" in hay or "zahrad" in hay:
        title = f"{public_title} do terénu a záhrady"
    elif "sprej" in hay or "medve" in hay:
        title = f"{public_title} do prírody"
    elif "značk" in hay or "znack" in hay:
        title = public_title if "nože" in public_title.lower() else "Nože podľa značky"
    elif "príslušen" in hay or "prislusen" in hay or "puzdr" in hay:
        title = public_title if title_has_knife else f"{public_title} k nožom"
    elif "výroba" in hay or "vyroba" in hay or "čepeľ" in hay or "cepel" in hay:
        title = public_title if title_has_knife else f"{public_title} na výrobu nožov"
    elif "outdoor" in hay or "bushcraft" in hay or "survival" in hay or "taktick" in hay:
        title = public_title if title_has_knife else f"{public_title} nože"
    elif "loveck" in hay or "poľovní" in hay or "polov" in hay or "hubár" in hay or "hubar" in hay:
        title = public_title
    elif "edc" in hay or "zatvár" in hay or "zatvar" in hay:
        title = public_title
    elif "oceľ" in hay or "ocel" in hay or "damask" in hay or "nerez" in hay:
        title = f"{public_title} podľa ocele"
    elif "nož" in hay or "noz" in hay:
        title = public_title
    else:
        title = f"{public_title} - nože a výbava"
    return trim_to(title, 60)


def build_seo(node: CategoryNode) -> Dict[str, str]:
    title = compact_space(node.title)
    public_title = {
        "Nože všetky": "Všetky nože",
        "Nože kategórie": "Nože podľa kategórie",
    }.get(title, title)
    context = category_context(title, node.path, node.product_titles)
    intro = (
        f"Kategória {public_title} je zameraná na {context['benefit']}. "
        f"Vyberať môžete podľa {context['choice']}."
    )
    bottom = (
        f"V kategórii {public_title} nájdete {context['noun']} pre zákazníkov, ktorí chcú vyberať podľa reálneho použitia, "
        f"kvality spracovania a praktických parametrov. {context['detail']} "
        "Pri výbere porovnajte účel, rozmery, materiál, bezpečnosť používania a nároky na údržbu."
    )
    seo_title = build_seo_title(public_title, node.path)
    seo_base = f"{public_title}: výber podľa {context['choice']}."
    seo_tail = " Porovnajte praktické parametre, spracovanie a vhodné použitie."
    if len(seo_base + seo_tail) <= 155:
        seo_description = seo_base + seo_tail
    elif len(seo_base) <= 155:
        seo_description = seo_base
    else:
        seo_description = trim_to(
            f"{public_title}: prehľadný výber podľa použitia, materiálu a parametrov.",
            155,
        )
    return {
        "intro": trim_to(intro, 320),
        "bottom": trim_to(bottom, 520),
        "seo_title": seo_title,
        "seo_description": seo_description,
    }


def node_from_category(category: Dict[str, Any], path_titles: List[str]) -> CategoryNode:
    products = category.get("products") or {}
    product_titles = [
        compact_space(product.get("title") or "")
        for product in products.get("data") or []
        if product.get("active") and compact_space(product.get("title") or "")
    ]
    return CategoryNode(
        id=str(category["id"]),
        title=compact_space(category.get("title") or ""),
        link=category.get("link") or "",
        path_titles=path_titles,
        current_intro=strip_tags(category.get("intro")),
        current_bottom=strip_tags(category.get("bottom")),
        current_seo_title=compact_space(category.get("seo_title") or ""),
        current_seo_description=compact_space(category.get("seo_description") or ""),
        product_titles=product_titles,
        children_ids=[str(child["id"]) for child in category.get("children_categories") or []],
    )


def traverse_categories(client: BiznisWebClient, root_id: str) -> List[CategoryNode]:
    queue: List[tuple[str, List[str]]] = [(root_id, [])]
    seen: set[str] = set()
    nodes: List[CategoryNode] = []
    while queue:
        category_id, parent_path = queue.pop(0)
        if category_id in seen:
            continue
        seen.add(category_id)
        category = client.get_category(category_id)
        language = category.get("language") or {}
        if str(language.get("id")) != SK_LANG_ID or str(language.get("code")).lower() != SK_LANG_CODE:
            raise RuntimeError(f"Unexpected language for category {category_id}: {language}")
        title = compact_space(category.get("title") or "")
        path = parent_path + [title]
        node = node_from_category(category, path)
        if not node.link or "/c/" not in node.link:
            raise RuntimeError(f"Refusing category without public /c/ link: {node.id} {node.title} {node.link}")
        nodes.append(node)
        for child in category.get("children_categories") or []:
            child_lang = (child.get("language") or {}).get("code")
            if str(child_lang).lower() == SK_LANG_CODE:
                queue.append((str(child["id"]), path))
        time.sleep(0.05)
    return nodes


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def public_verify(urls: Iterable[str], limit: int = 8) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    session = requests.Session()
    for url in list(urls)[:limit]:
        try:
            response = session.get(url, timeout=(10, 30), allow_redirects=True)
            text = response.text[:200_000]
            results.append({
                "url": url,
                "status": response.status_code,
                "final_url": response.url,
                "has_roy_domain": urlparse(response.url).netloc.endswith("roy.sk"),
                "has_category_content_marker": any(marker in text for marker in ("category", "s1-product", "product")),
            })
        except Exception as exc:
            results.append({"url": url, "error": str(exc)[:300]})
        time.sleep(0.2)
    return results


def main() -> int:
    configure_stdout()
    parser = argparse.ArgumentParser(description="Generate/apply roy.sk SK category SEO content.")
    apply_group = parser.add_mutually_exclusive_group()
    apply_group.add_argument("--apply", action="store_true", help="Apply updates through GraphQL updateCategory.")
    apply_group.add_argument("--apply-admin", action="store_true", help="Apply updates through the ROY admin category form.")
    parser.add_argument("--root-category-id", default=ROY_NOZE_ROOT_CATEGORY_ID)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--verify-public", action="store_true")
    parser.add_argument("--verify-limit", type=int, default=12)
    parser.add_argument("--limit", type=int, default=0, help="Process only the first N traversed categories.")
    parser.add_argument("--admin-base-url", default="", help="Admin base URL; defaults to the GraphQL host.")
    args = parser.parse_args()

    env = load_env(PROJECT_ENV)
    api_url = env.get("BIZNISWEB_API_URL") or "https://roy.flox.sk/api/graphql"
    api_token = env.get("BIZNISWEB_API_TOKEN")
    if not api_token:
        raise RuntimeError("BIZNISWEB_API_TOKEN missing in projects/roy/.env")

    client = BiznisWebClient(api_url, api_token)
    nodes = traverse_categories(client, str(args.root_category_id))
    if args.limit:
        nodes = nodes[: args.limit]
    admin_base_url = args.admin_base_url or derive_admin_base_url(api_url)
    admin_client: Optional[BiznisWebAdminClient] = None
    if args.apply_admin:
        username = env.get("BIZNISWEB_USERNAME")
        password = env.get("BIZNISWEB_PASSWORD")
        if not username or not password:
            raise RuntimeError("BIZNISWEB_USERNAME/BIZNISWEB_PASSWORD missing in projects/roy/.env")
        admin_client = BiznisWebAdminClient(admin_base_url, username, password)
        admin_client.login()
    records: List[Dict[str, Any]] = []

    for index, node in enumerate(nodes, start=1):
        proposed = build_seo(node)
        record = {
            "id": node.id,
            "title": node.title,
            "path": node.path,
            "api_link": node.link,
            "public_url": node.public_url,
            "product_samples": node.product_titles[:8],
            "before": {
                "intro": node.current_intro,
                "bottom": node.current_bottom,
                "seo_title": node.current_seo_title,
                "seo_description": node.current_seo_description,
            },
            "proposed": proposed,
            "applied": False,
            "verified_after": None,
        }
        if args.apply or args.apply_admin:
            try:
                if args.apply_admin:
                    if admin_client is None:
                        raise RuntimeError("Admin client is not initialized.")
                    updated = admin_client.update_category(node.id, proposed)
                else:
                    updated = client.update_category(node.id, proposed)
                record["applied"] = True
                record["verified_after"] = {
                    "intro": strip_tags(updated.get("intro")),
                    "bottom": strip_tags(updated.get("bottom")),
                    "seo_title": compact_space(updated.get("seo_title") or ""),
                    "seo_description": compact_space(updated.get("seo_description") or ""),
                }
            except Exception as exc:
                record["apply_error"] = str(exc)[:800]
            if index % 25 == 0:
                print(f"apply_progress={index}/{len(nodes)} category_id={node.id} applied={record['applied']}")
                sys.stdout.flush()
            time.sleep(0.08)
        records.append(record)

    public_checks: List[Dict[str, Any]] = []
    if args.verify_public:
        public_checks = public_verify((record["public_url"] for record in records), limit=args.verify_limit)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "project": "roy",
        "target": {
            "admin_api_url": api_url,
            "graphql_api_url": api_url,
            "admin_base_url": admin_base_url if args.apply_admin else None,
            "language_id": SK_LANG_ID,
            "language_code": SK_LANG_CODE,
            "language_name": "Slovakia",
            "public_domain": "https://www.roy.sk",
            "root_category_id": str(args.root_category_id),
        },
        "mode": "apply-admin" if args.apply_admin else "apply" if args.apply else "dry-run",
        "category_count": len(records),
        "categories": records,
        "public_checks": public_checks,
    }
    write_json(args.output, payload)

    print(f"mode={payload['mode']}")
    print(f"api_url={api_url}")
    print(f"target_language={SK_LANG_CODE.upper()} lang_id={SK_LANG_ID}")
    print(f"root_category_id={args.root_category_id}")
    print(f"category_count={len(records)}")
    print(f"output={args.output}")
    for record in records[:12]:
        print(f"- {record['id']} {record['title']} -> {record['public_url']}")
    if len(records) > 12:
        print(f"... {len(records) - 12} more")
    if public_checks:
        ok = sum(1 for item in public_checks if item.get("status") == 200 and item.get("has_roy_domain"))
        print(f"public_checks_ok={ok}/{len(public_checks)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
