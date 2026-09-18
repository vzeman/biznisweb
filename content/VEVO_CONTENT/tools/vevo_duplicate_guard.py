import argparse
import json
import re
import sys
import unicodedata
import urllib.parse
import xml.etree.ElementTree as ET
from html.parser import HTMLParser
from pathlib import Path

import requests


PUBLIC = "https://www.vevo.sk"
RSS_URL = f"{PUBLIC}/e/rss/news"
FAQ_URL = f"{PUBLIC}/casto-kladene-dotazy"
ROOT = Path(__file__).resolve().parents[3]
IMPORTS_DIR = ROOT / "content" / "VEVO_CONTENT" / "imports"


CLUSTERS = {
    "wardrobe_storage": {
        "terms": [
            "satnik",
            "skrin",
            "skrina",
            "skrine",
            "skrini",
            "bieliznik",
            "bielizniku",
            "ulozny",
            "ulozne",
            "box",
            "police",
        ],
        "intent_terms": [
            "prevonat",
            "vona",
            "zatuchnutie",
            "zatuchnuty",
            "zapach",
            "vycistit",
            "triedenie",
            "ulozenie",
        ],
        "note": "Wardrobe/storage topics overlap easily. Require a distinct intent: gentle scent, musty-smell removal, seasonal sorting, bedding cabinet, or organizer cleaning.",
    },
    "bedding_sleep": {
        "terms": ["obliecky", "postelne", "postelna", "plachta", "vankus", "paplon", "spalna", "matrac"],
        "intent_terms": ["prat", "prevonat", "zapach", "vona", "alergia", "chranic"],
        "note": "Bedding topics need clear separation by textile type and user problem.",
    },
    "bathroom_towels": {
        "terms": ["uterak", "uteraky", "kupeln", "predlozka", "rohoz", "zupan", "frote"],
        "intent_terms": ["zapach", "savost", "prat", "vlhkost", "zatuchnutie"],
        "note": "Bathroom/towel topics should be separated by textile and problem.",
    },
    "bags_travel_sport": {
        "terms": ["taska", "batoh", "kufor", "organizery", "sport", "fitko", "turisticky"],
        "intent_terms": ["zapach", "vycistit", "prat", "pot", "vlhkost"],
        "note": "Bag/travel/sport topics overlap when they only say odor removal; specify object and use case.",
    },
    "fragrance_general": {
        "terms": ["vona", "prevonat", "parfum", "aroma", "interierovy"],
        "intent_terms": ["byt", "spalna", "satnik", "kupelna", "obyvacka", "postelna", "boli", "hlava"],
        "note": "Fragrance topics need room/use-case separation and cannot be generic scent articles.",
    },
}


CANONICAL_INTENTS = {
    "laundry_symbols": {
        "severity": "block",
        "note": (
            "Laundry-symbol head terms are canonical topics. Do not create a new article when "
            "VEVO already has a guide for symboly prania, pracie symboly, praci stitok, "
            "or vysvetlivky na pranie; expand or consolidate the canonical URL instead."
        ),
    },
}


STOPWORDS = {
    "ako",
    "a",
    "aj",
    "bez",
    "do",
    "na",
    "od",
    "po",
    "pre",
    "pred",
    "pri",
    "s",
    "so",
    "v",
    "vo",
    "z",
    "zo",
    "aby",
    "ktore",
    "ktory",
    "alebo",
    "doma",
}


ACTION_GROUPS = {
    "clean": {"cistit", "vycistit", "umyt", "umyvat"},
    "launder": {"prat", "vyprat"},
    "remove": {"odstranit", "zbavit"},
    "dry": {"susit", "ususit"},
    "scent": {"prevonat", "prevoňat", "vonat"},
    "choose": {"vybrat", "vyberat"},
    "store": {"skladovat", "ulozit", "organizovat", "zorganizovat"},
    "maintain": {"udrziavat", "udrzat", "predist"},
    "use": {"pouzivat", "pouzit", "davkovat"},
}


GENERIC_PROBLEM_TERMS = {
    "bakterie",
    "bezpecne",
    "bezpecny",
    "bezpecna",
    "chyby",
    "domaca",
    "farba",
    "kompletny",
    "mastnota",
    "mastne",
    "mastny",
    "navod",
    "pach",
    "pachy",
    "pokrcenie",
    "prach",
    "problem",
    "problemy",
    "skvrna",
    "skvrny",
    "sprievodca",
    "spravne",
    "stolovy",
    "susenie",
    "textilne",
    "latkove",
    "kuchynske",
    "kupelnove",
    "udrzba",
    "vona",
    "zapach",
}

GENERIC_PROBLEM_ROOTS = (
    "bakter",
    "bezpec",
    "mastn",
    "map",
    "pach",
    "pokrc",
    "prach",
    "skvr",
    "smuh",
    "sus",
    "udrz",
    "zapach",
)


class NewsLinkParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.current_href = None
        self.current_text = []
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "a":
            return
        href = dict(attrs).get("href")
        if href and "/n/" in href:
            self.current_href = urllib.parse.urljoin(PUBLIC, href)
            self.current_text = []

    def handle_data(self, data):
        if self.current_href:
            self.current_text.append(data)

    def handle_endtag(self, tag):
        if tag.lower() != "a" or not self.current_href:
            return
        title = re.sub(r"\s+", " ", "".join(self.current_text)).strip()
        if title and norm(title) not in {"citajte viac", "viac"}:
            self.links.append((title, self.current_href))
        self.current_href = None
        self.current_text = []


def norm(value):
    value = unicodedata.normalize("NFKD", value.lower())
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def slugify(value):
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = re.sub(r"[^a-zA-Z0-9]+", "-", value.lower())
    return re.sub(r"-+", "-", value).strip("-")


def tokens(value):
    return {token for token in norm(value).split() if token not in STOPWORDS and len(token) > 2}


def jaccard(left, right):
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def row_from_title_link(title, link, source):
    slug = urllib.parse.urlparse(link).path.rstrip("/").rsplit("/", 1)[-1]
    return {
        "title": title,
        "link": link,
        "slug": slug,
        "tokens": sorted(tokens(title)),
        "sources": [source],
    }


def fetch_rss_existing():
    response = requests.get(RSS_URL, timeout=40)
    response.raise_for_status()
    root = ET.fromstring(response.content)
    rows = []
    for item in root.findall("./channel/item"):
        title = item.findtext("title") or ""
        link = item.findtext("link") or ""
        if title and link:
            rows.append(row_from_title_link(title, link, "rss"))
    return rows


def fetch_faq_existing():
    response = requests.get(FAQ_URL, timeout=40)
    response.raise_for_status()
    parser = NewsLinkParser()
    parser.feed(response.text)
    return [row_from_title_link(title, link, "faq") for title, link in parser.links]


def article_lists(data):
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("articles", "updates"):
            if isinstance(data.get(key), list):
                return data[key]
    return []


def load_local_existing(exclude_batch=None):
    rows = []
    for path in sorted(IMPORTS_DIR.glob("*.json")):
        match = re.match(r"batch-(\d+)-", path.name)
        if exclude_batch is not None and match and int(match.group(1)) == exclude_batch:
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        for article in article_lists(data):
            if not isinstance(article, dict):
                continue
            title = str(article.get("title") or "").strip()
            slug = str(article.get("link") or article.get("slug") or "").strip().strip("/")
            if not title or not slug:
                continue
            if "/n/" in slug:
                slug = urllib.parse.urlparse(slug).path.rstrip("/").rsplit("/", 1)[-1]
            link = f"{PUBLIC}/n/{slug}"
            row = row_from_title_link(title, link, f"local:{path.name}")
            rows.append(row)
    return rows


def merge_existing(rows):
    merged = {}
    for row in rows:
        key = row.get("slug") or f"title:{norm(row.get('title', ''))}"
        current = merged.get(key)
        if current is None:
            merged[key] = row
            continue
        current["sources"] = sorted(set(current.get("sources", [])) | set(row.get("sources", [])))
        if len(row.get("title", "")) > len(current.get("title", "")):
            current["title"] = row["title"]
            current["tokens"] = row["tokens"]
    return list(merged.values())


def fetch_existing(exclude_batch=None):
    return merge_existing(
        fetch_rss_existing()
        + fetch_faq_existing()
        + load_local_existing(exclude_batch=exclude_batch)
    )


def action_groups(value):
    value_tokens = set(norm(value).split())
    return {name for name, terms in ACTION_GROUPS.items() if value_tokens & terms}


def anchor_tokens(value):
    action_terms = set().union(*ACTION_GROUPS.values())
    return [
        token
        for token in norm(value).split()
        if token not in STOPWORDS
        and token not in action_terms
        and token not in GENERIC_PROBLEM_TERMS
        and not token.startswith(GENERIC_PROBLEM_ROOTS)
        and len(token) > 2
    ]


def intent_overlap(left, right):
    left_groups = action_groups(left)
    right_groups = action_groups(right)
    same_actions = sorted(left_groups & right_groups)
    left_anchors = anchor_tokens(left)
    right_anchors = anchor_tokens(right)
    shared_anchors = sorted(set(left_anchors[:3]) & set(right_anchors[:3]))
    return {
        "same_actions": same_actions,
        "shared_anchors": shared_anchors,
        "same_head": bool(same_actions and shared_anchors),
    }


def catalog_health(rows):
    by_title = {}
    for row in rows:
        by_title.setdefault(norm(row["title"]), []).append(row)
    duplicate_titles = []
    for group in by_title.values():
        live = [row for row in group if "rss" in row.get("sources", [])]
        if len(live) > 1:
            duplicate_titles.append(
                {
                    "title": live[0]["title"],
                    "records": [
                        {"link": row["link"], "slug": row["slug"]}
                        for row in live
                    ],
                }
            )
    bad_slugs = [
        {"title": row["title"], "link": row["link"], "slug": row["slug"]}
        for row in rows
        if "rss" in row.get("sources", [])
        and (
            re.fullmatch(r"1{2,}", row["slug"])
            or not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", row["slug"])
        )
    ]
    return {
        "duplicate_title_group_count": len(duplicate_titles),
        "duplicate_titles": duplicate_titles,
        "bad_slug_count": len(bad_slugs),
        "bad_slugs": bad_slugs,
    }


def cluster_hits(title):
    normalized = norm(title)
    title_tokens = set(normalized.split())
    hits = []
    for name, config in CLUSTERS.items():
        term_hit = sorted(set(config["terms"]) & title_tokens)
        intent_hit = sorted(set(config["intent_terms"]) & title_tokens)
        if term_hit:
            hits.append({"cluster": name, "terms": term_hit, "intent_terms": intent_hit, "note": config["note"]})
    return hits


def canonical_intents(title):
    normalized = norm(title)
    title_tokens = set(normalized.split())
    hits = []

    symbol_terms = {"symbol", "symboly", "symbolov", "symboloch", "znacky", "znaciek", "vysvetlivky"}
    laundry_context_terms = {
        "prania",
        "pranie",
        "pracie",
        "praci",
        "stitok",
        "stitku",
        "stitkom",
        "obleceni",
        "oblecenie",
    }
    care_symbol_terms = {"vanicka", "trojuholnik", "stvorec", "zehlicka", "susicka", "bielenie"}

    has_symbol_phrase = bool(symbol_terms & title_tokens) or "pracie symboly" in normalized
    has_laundry_context = bool(laundry_context_terms & title_tokens)
    has_care_symbol_list = len(care_symbol_terms & title_tokens) >= 2

    if (has_symbol_phrase and has_laundry_context) or (has_care_symbol_list and "prania" in title_tokens):
        hits.append("laundry_symbols")

    return hits


def analyze(candidates, existing, similarity_threshold):
    existing_by_slug = {row["slug"]: row for row in existing}
    existing_by_title = {norm(row["title"]): row for row in existing}
    existing_by_intent = {}
    for row in existing:
        for intent in canonical_intents(row["title"]):
            existing_by_intent.setdefault(intent, []).append(row)
    results = []
    for candidate in candidates:
        candidate = candidate.strip()
        if not candidate:
            continue
        candidate_slug = slugify(candidate)
        candidate_tokens = tokens(candidate)
        issues = []
        exact_slug = existing_by_slug.get(candidate_slug)
        exact_title = existing_by_title.get(norm(candidate))
        if exact_slug:
            issues.append({"type": "exact_slug", "severity": "block", "match": exact_slug})
        if exact_title and (not exact_slug or exact_title["link"] != exact_slug.get("link")):
            issues.append({"type": "exact_title", "severity": "block", "match": exact_title})
        for intent in canonical_intents(candidate):
            matches = existing_by_intent.get(intent, [])
            if matches:
                issues.append({
                    "type": "canonical_intent_duplicate",
                    "severity": CANONICAL_INTENTS[intent]["severity"],
                    "intent": intent,
                    "note": CANONICAL_INTENTS[intent]["note"],
                    "matches": [
                        {"title": match["title"], "link": match["link"], "slug": match["slug"]}
                        for match in matches[:8]
                    ],
                })
        scored = []
        intent_matches = []
        for row in existing:
            score = jaccard(candidate_tokens, set(row["tokens"]))
            if score >= similarity_threshold:
                scored.append({"score": round(score, 3), "title": row["title"], "link": row["link"], "slug": row["slug"], "sources": row.get("sources", [])})
            overlap = intent_overlap(candidate, row["title"])
            if overlap["same_head"]:
                intent_matches.append(
                    {
                        "title": row["title"],
                        "link": row["link"],
                        "slug": row["slug"],
                        "same_actions": overlap["same_actions"],
                        "shared_anchors": overlap["shared_anchors"],
                        "sources": row.get("sources", []),
                    }
                )
        scored.sort(key=lambda item: item["score"], reverse=True)
        if intent_matches:
            issues.append(
                {
                    "type": "same_head_intent",
                    "severity": "block",
                    "note": "Existing content uses the same action and subject. Expand or consolidate the existing URL instead of creating another broad article.",
                    "matches": intent_matches[:8],
                }
            )
        hits = cluster_hits(candidate)
        for hit in hits:
            same_cluster = []
            cluster_terms = set(CLUSTERS[hit["cluster"]]["terms"])
            for row in existing:
                row_terms = set(row["tokens"])
                if cluster_terms & row_terms:
                    score = jaccard(candidate_tokens, row_terms)
                    if score >= 0.16:
                        same_cluster.append({"score": round(score, 3), "title": row["title"], "link": row["link"]})
            same_cluster.sort(key=lambda item: item["score"], reverse=True)
            if same_cluster[:5]:
                issues.append({
                    "type": "cluster_overlap",
                    "severity": "review",
                    "cluster": hit["cluster"],
                    "note": hit["note"],
                    "matches": same_cluster[:5],
                })
        if scored:
            issues.append({"type": "similar_title", "severity": "review", "matches": scored[:8]})
        status = "block" if any(issue["severity"] == "block" for issue in issues) else ("review" if issues else "ok")
        results.append({
            "title": candidate,
            "slug": candidate_slug,
            "status": status,
            "clusters": hits,
            "issues": issues,
        })

    for index, result in enumerate(results):
        left_tokens = tokens(result["title"])
        for other in results[index + 1 :]:
            score = jaccard(left_tokens, tokens(other["title"]))
            overlap = intent_overlap(result["title"], other["title"])
            if result["slug"] == other["slug"] or norm(result["title"]) == norm(other["title"]):
                severity = "block"
                issue_type = "candidate_exact_duplicate"
            elif overlap["same_head"] or score >= similarity_threshold:
                severity = "review"
                issue_type = "candidate_batch_overlap"
            else:
                continue
            for target, match in ((result, other), (other, result)):
                target["issues"].append(
                    {
                        "type": issue_type,
                        "severity": severity,
                        "matches": [{"title": match["title"], "slug": match["slug"], "score": round(score, 3)}],
                    }
                )
                target["status"] = "block" if severity == "block" else ("review" if target["status"] == "ok" else target["status"])
    return results


def read_candidates(args):
    candidates = []
    candidates.extend(args.title or [])
    if args.file:
        with open(args.file, encoding="utf-8") as handle:
            candidates.extend(line.strip() for line in handle if line.strip() and not line.lstrip().startswith("#"))
    if not candidates and not sys.stdin.isatty():
        candidates.extend(line.strip() for line in sys.stdin if line.strip())
    return candidates


def main():
    parser = argparse.ArgumentParser(description="Guard VEVO article batches against duplicate titles, slugs, and overlapping content clusters.")
    parser.add_argument("--title", action="append", help="Candidate title. Can be repeated.")
    parser.add_argument("--file", help="UTF-8 text file with one candidate title per line.")
    parser.add_argument("--similarity-threshold", type=float, default=0.28)
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    parser.add_argument("--report", type=Path, help="Write the machine-readable report to this JSON file.")
    args = parser.parse_args()

    candidates = read_candidates(args)
    if not candidates:
        parser.error("Provide --title, --file, or candidate titles on stdin.")

    exclude_batch = None
    if args.file:
        match = re.search(r"batch-(\d+)-", Path(args.file).name)
        if match:
            exclude_batch = int(match.group(1))

    existing = fetch_existing(exclude_batch=exclude_batch)
    results = analyze(candidates, existing, args.similarity_threshold)
    health = catalog_health(existing)
    report = {"existing_count": len(existing), "excluded_batch": exclude_batch, "catalog_health": health, "results": results}

    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(f"Existing RSS articles checked: {len(existing)}")
        for result in results:
            print(f"\n[{result['status'].upper()}] {result['title']}")
            print(f"slug: {result['slug']}")
            for issue in result["issues"]:
                print(f"- {issue['severity']}: {issue['type']}")
                if "match" in issue:
                    print(f"  match: {issue['match']['title']} | {issue['match']['link']}")
                for match in issue.get("matches", [])[:5]:
                    prefix = f"{match['score']}: " if "score" in match else ""
                    print(f"  {prefix}{match['title']} | {match['link']}")
                if issue.get("note"):
                    print(f"  note: {issue['note']}")

    if any(result["status"] == "block" for result in results):
        sys.exit(2)
    if any(result["status"] == "review" for result in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
