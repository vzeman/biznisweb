import argparse
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup


ROOT = Path(__file__).resolve().parents[3]
VEVO_ROOT = ROOT / "content" / "VEVO_CONTENT"
IMPORTS_DIR = VEVO_ROOT / "imports"
EXPORTS_DIR = VEVO_ROOT / "exports"
CONFIG_PATH = Path.home() / ".codex" / "config.toml"
TARGET_URL = "https://www.vevo.sk/c/vevo-fragrance/parfum-do-prania"
TARGET_ANCHOR = "parfumy do prania"
REPORT_PATH = EXPORTS_DIR / "laundry-perfume-link-insert-2026-06-29.json"


def load_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


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
    raise ValueError(f"No JSON data line in MCP response: {text[:500]}")


def call_update(endpoint, payload, request_id):
    body = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "tools/call",
        "params": {"name": "biznisweb-update_news_post", "arguments": payload},
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
    result = parsed.get("result", {})
    for item in result.get("content", []):
        if item.get("type") != "text":
            continue
        try:
            inner = json.loads(item.get("text", ""))
        except json.JSONDecodeError:
            continue
        if inner.get("error"):
            raise RuntimeError(inner["error"])
        if inner.get("success") is False:
            raise RuntimeError(json.dumps(inner, ensure_ascii=False))
    return parsed


def load_mappings():
    mappings = {}
    for path in sorted(EXPORTS_DIR.glob("batch-*-mapping.json")):
        data = load_json(path)
        posts = data if isinstance(data, list) else data.get("posts") or data.get("articles") or []
        for post in posts:
            slug = post.get("slug") or post.get("link")
            post_id = post.get("id") or post.get("post_id") or post.get("news_id")
            if not slug or not post_id:
                continue
            mappings[slug] = {
                "post_id": str(post_id),
                "url": post.get("url") or f"https://www.vevo.sk/n/{slug}",
                "mapping_file": str(path.relative_to(ROOT)),
            }
    return mappings


def article_list(root):
    if isinstance(root, list):
        return root
    if isinstance(root, dict) and isinstance(root.get("articles"), list):
        return root["articles"]
    if isinstance(root, dict) and isinstance(root.get("updates"), list):
        return root["updates"]
    return []


def category_sentence(title, slug, body):
    text = f"{title} {slug} {body[:1200]}".lower()
    link = f'<a href="{TARGET_URL}">{TARGET_ANCHOR}</a>'
    if any(word in text for word in ["uterák", "uterak", "osuš", "osus", "župan", "zupan", "obliečk", "oblieck", "posteľ", "postel", "pyžam", "pyzam"]):
        return (
            f"<p>Pri savých domácich textíliách najprv riešte čistotu, oplach a sušenie; "
            f"až potom má zmysel jemne doladiť vôňu cez {link}, aby vôňa neprekrývala vlhkosť.</p>"
        )
    if any(word in text for word in ["zápach", "zapach", "vôň", "vona", "osviež", "osviez", "pot"]):
        return (
            f"<p>Keď je zdroj zápachu vyriešený a textil je naozaj čistý, finálnu sviežosť v praní "
            f"môžu doplniť aj {link}; používajte ich skôr ako posledný krok, nie ako náhradu prania.</p>"
        )
    if any(word in text for word in ["dres", "šport", "sport", "softshell", "membrán", "membran", "polyester", "funkč", "funkc", "elastan"]):
        return (
            f"<p>Pri funkčných a športových textíliách najprv odstráňte pot a zvyšky pracieho prostriedku; "
            f"ak chcete po vypraní jemnú vôňu, môžu pomôcť {link} s miernym dávkovaním podľa typu materiálu.</p>"
        )
    if any(word in text for word in ["bavlna", "vlna", "ľan", "lan", "viskóz", "viskoz", "akryl", "nylon", "modal", "lyocell", "materiál", "material"]):
        return (
            f"<p>Pri výbere postupu je vždy dôležitejší materiál a štítok než samotná vôňa; "
            f"ak je textil dobre vypraný, jemný voňavý finiš môžu pridať {link}.</p>"
        )
    return (
        f"<p>Pri škvrnách najprv odstráňte samotné znečistenie a skontrolujte výsledok pred sušením; "
        f"keď je textil čistý, vôňu môžete doladiť cez {link}.</p>"
    )


def insertion_index(long):
    candidates = []
    for marker in [
        '<div style="border: 1px solid #dbe5de;',
        '<h2>Súvisiace návody',
        '<h2>Súvisiace články',
        "<h2>FAQ",
        "<h2>Najčastejšie otázky",
    ]:
        idx = long.find(marker)
        if idx > 1200:
            candidates.append(idx)
    return min(candidates) if candidates else len(long)


def insert_sentence(long, sentence):
    if "parfum-do-prania" in long:
        return long, False
    idx = insertion_index(long)
    return long[:idx].rstrip() + "\n" + sentence + "\n" + long[idx:].lstrip(), True


def fetch_public(url):
    response = requests.get(url, headers={"User-Agent": "Codex VEVO link insert"}, timeout=30)
    return response.status_code, response.text


def main_article_inner(html):
    soup = BeautifulSoup(html, "html.parser")
    divs = soup.select("div.userHTMLContent")
    if not divs:
        return ""
    div = max(divs, key=lambda node: len(str(node)))
    return "".join(str(child) for child in div.contents).strip()


def verify_public(url):
    status, html = fetch_public(url)
    inner = main_article_inner(html) if status == 200 else ""
    soup = BeautifulSoup(inner, "html.parser")
    links = [
        a for a in soup.find_all("a")
        if (a.get("href") or "").rstrip("/") == TARGET_URL.rstrip("/")
        and a.get_text(" ", strip=True).lower() == TARGET_ANCHOR
    ]
    return {
        "status": status,
        "body_length": len(inner),
        "target_link_count": len(links),
        "escaped_html": "&lt;p&gt;" in inner,
        "ok": status == 200 and len(inner) > 1000 and len(links) >= 1 and "&lt;p&gt;" not in inner,
    }


def article_score(article):
    title = article.get("title", "")
    slug = article.get("link") or article.get("slug") or ""
    body = article.get("long") or ""
    text = f"{title} {slug} {body[:1200]}".lower()
    score = 0
    for keyword in [
        "vôň", "vona", "zápach", "zapach", "pranie", "prať", "prat", "bielize",
        "oblečen", "obliečk", "uterák", "zupan", "župan", "posteľ", "postel",
        "textil", "sveter", "tričko", "dres", "polyester", "bavlna",
    ]:
        if keyword in text:
            score += 1
    return score


def collect_candidates(limit=None):
    mappings = load_mappings()
    candidates = []
    loaded_files = {}
    for path in sorted(IMPORTS_DIR.glob("batch-*-articles.json")):
        root = load_json(path)
        articles = article_list(root)
        loaded_files[path] = root
        for index, article in enumerate(articles):
            if not isinstance(article, dict):
                continue
            slug = article.get("link") or article.get("slug")
            long = article.get("long") or ""
            if not slug or slug not in mappings or TARGET_URL in long:
                continue
            candidates.append({
                "score": article_score(article),
                "source_file": path,
                "index": index,
                "article": article,
                "slug": slug,
                "post_id": mappings[slug]["post_id"],
                "url": mappings[slug]["url"],
                "mapping_file": mappings[slug]["mapping_file"],
            })
    candidates.sort(key=lambda item: (-item["score"], int(item["post_id"]) if item["post_id"].isdigit() else 999999))
    return (candidates[:limit] if limit else candidates), loaded_files


def run(limit, update_live, write_local, sleep_seconds, allow_url_sensitive_live_update=False):
    if update_live and not allow_url_sensitive_live_update:
        raise SystemExit(
            "Live updates are disabled for this one-off script. The BiznisWeb news "
            "update endpoint can rewrite or hide article URLs when used for existing "
            "posts. Use this script only for dry-run/local report generation, then "
            "apply live changes through the guarded admin/import workflow."
        )
    endpoint = mcp_url() if update_live else None
    selected, loaded_files = collect_candidates()
    updated_by_file = {path: False for path in loaded_files}
    report = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "target_url": TARGET_URL,
        "target_anchor": TARGET_ANCHOR,
        "limit": limit,
        "update_live": update_live,
        "write_local": write_local,
        "updated_count": 0,
        "records": [],
    }

    for request_index, item in enumerate(selected, start=1):
        if report["updated_count"] >= limit:
            break
        article = item["article"]
        sentence = category_sentence(article.get("title", ""), item["slug"], article.get("long") or "")

        try:
            status, public_html = fetch_public(item["url"])
            if status != 200:
                raise RuntimeError(f"public status {status}")
            public_inner = main_article_inner(public_html)
            if not public_inner:
                raise RuntimeError("public article body not found")
            updated_public, inserted_public = insert_sentence(public_inner, sentence)
            if not inserted_public:
                verify = verify_public(item["url"])
                report["records"].append({**item_summary(item, sentence), "skipped": "already_has_link", "verify": verify})
                continue

            updated_local = None
            inserted_local = False
            if write_local:
                updated_local, inserted_local = insert_sentence(article.get("long") or "", sentence)

            mcp_ok = False
            if update_live:
                call_update(endpoint, {"post_id": item["post_id"], "long": updated_public}, f"laundry-perfume-link-{request_index}")
                mcp_ok = True
                time.sleep(sleep_seconds)

            if write_local and inserted_local:
                article["long"] = updated_local
                updated_by_file[item["source_file"]] = True

            verify = verify_public(item["url"]) if update_live else {"ok": True, "dry_run": True}
            ok = verify.get("ok") is True
            if ok:
                report["updated_count"] += 1
            report["records"].append({
                **item_summary(item, sentence),
                "inserted_live": inserted_public,
                "inserted_local": inserted_local,
                "mcp_update_ok": mcp_ok,
                "verify": verify,
                "ok": ok,
            })
        except Exception as exc:
            report["records"].append({**item_summary(item, sentence), "error": f"{type(exc).__name__}: {exc}", "ok": False})

    if write_local:
        for path, changed in updated_by_file.items():
            if changed:
                write_json(path, loaded_files[path])

    report["all_ok"] = report["updated_count"] >= limit and all(record.get("ok") or record.get("skipped") for record in report["records"])
    REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return report


def item_summary(item, sentence):
    return {
        "post_id": item["post_id"],
        "title": item["article"].get("title", ""),
        "slug": item["slug"],
        "url": item["url"],
        "source_file": str(item["source_file"].relative_to(ROOT)),
        "mapping_file": item["mapping_file"],
        "sentence": sentence,
    }


def main():
    parser = argparse.ArgumentParser(description="Insert a contextual VEVO laundry perfume category sentence into existing articles.")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--update-live", action="store_true")
    parser.add_argument("--write-local", action="store_true")
    parser.add_argument("--sleep", type=float, default=1.2)
    parser.add_argument(
        "--allow-url-sensitive-live-update",
        action="store_true",
        help="Explicit escape hatch for the original one-off live updater. Prefer the guarded admin/import workflow.",
    )
    args = parser.parse_args()
    report = run(args.limit, args.update_live, args.write_local, args.sleep, args.allow_url_sensitive_live_update)
    print(json.dumps({
        "updated_count": report["updated_count"],
        "record_count": len(report["records"]),
        "all_ok": report["all_ok"],
        "report": str(REPORT_PATH),
        "failed": [record for record in report["records"] if not record.get("ok") and not record.get("skipped")],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
