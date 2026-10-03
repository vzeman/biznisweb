#!/usr/bin/env python3
"""Audit duplicate VEVO articles across the glossary, FAQ, and Blog blocks."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
from collections import Counter
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

import biznisweb_vevo_content_mcp as content_mcp
import vevo_duplicate_guard as duplicate_guard


PROJECT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = PROJECT / "exports" / "vevo-cross-section-duplicate-audit-2026-07-14.json"

CONTENT_SECTIONS = (
    {
        "role": "glossary",
        "title": "Slovnik pojmov o prani a vonach | Encyklopedia voni",
        "page_id": "805",
        "block_id": "1905",
        "public_url": "https://www.vevo.sk/slovnik-pojmov-o-prani-a-vonach-encyklopedia-voni",
    },
    {
        "role": "faq",
        "title": "Casto kladene otazky o prani a vonach",
        "page_id": "313",
        "block_id": "774",
        "public_url": "https://www.vevo.sk/casto-kladene-dotazy",
    },
    {
        "role": "blog",
        "title": "Blog",
        "page_id": "309",
        "block_id": "765",
        "public_url": "https://www.vevo.sk/blog",
    },
)

GENERIC_TITLE_TERMS = duplicate_guard.STOPWORDS | {
    "caste",
    "casto",
    "cistenie",
    "cistenim",
    "kladene",
    "kompletny",
    "navod",
    "odpovede",
    "otazky",
    "prakticky",
    "sprievodca",
    "sprievodcom",
    "spravne",
    "tipy",
    "udrzba",
    "udrzbou",
    "vysvetlene",
}

CONTENT_STOPWORDS = GENERIC_TITLE_TERMS | {
    "ale",
    "ani",
    "bolo",
    "budete",
    "by",
    "cez",
    "ich",
    "ked",
    "ktora",
    "moze",
    "mozete",
    "nie",
    "podla",
    "potom",
    "preto",
    "pretoze",
    "tak",
    "tento",
    "toto",
    "treba",
    "uz",
    "viac",
}


class VisibleTextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.ignored_depth = 0

    def handle_starttag(self, tag: str, _attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() in {"script", "style"}:
            self.ignored_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in {"script", "style"} and self.ignored_depth:
            self.ignored_depth -= 1

    def handle_data(self, data: str) -> None:
        if not self.ignored_depth:
            self.parts.append(data)


def visible_text(markup: str) -> str:
    parser = VisibleTextParser()
    parser.feed(markup or "")
    return re.sub(r"\s+", " ", " ".join(parser.parts)).strip()


def title_tokens(value: str) -> set[str]:
    return {
        token
        for token in duplicate_guard.norm(value).split()
        if len(token) > 2 and token not in GENERIC_TITLE_TERMS
    }


def content_tokens(value: str) -> list[str]:
    return [
        token
        for token in duplicate_guard.norm(visible_text(value)).split()
        if len(token) > 2 and token not in CONTENT_STOPWORDS
    ]


def overlap_coefficient(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / min(len(left), len(right))


def cosine_similarity(left: list[str], right: list[str]) -> float:
    left_counts = Counter(left)
    right_counts = Counter(right)
    if not left_counts or not right_counts:
        return 0.0
    dot = sum(value * right_counts.get(token, 0) for token, value in left_counts.items())
    left_norm = math.sqrt(sum(value * value for value in left_counts.values()))
    right_norm = math.sqrt(sum(value * value for value in right_counts.values()))
    return dot / (left_norm * right_norm) if left_norm and right_norm else 0.0


def five_word_shingles(tokens: list[str]) -> set[tuple[str, ...]]:
    return {tuple(tokens[index : index + 5]) for index in range(max(0, len(tokens) - 4))}


def record_summary(record: dict[str, Any]) -> dict[str, Any]:
    body = str(record.get("long") or "")
    words = content_tokens(body)
    slug = str(record.get("link") or "").strip()
    return {
        "post_id": str(record.get("news_id") or record.get("id") or ""),
        "block_id": str(record.get("block_id") or record.get("_block_id") or ""),
        "section": str(record.get("_section") or ""),
        "active": str(record.get("active") or "0") == "1",
        "title": str(record.get("title") or "").strip(),
        "slug": slug,
        "public_url": f"https://www.vevo.sk/n/{slug}" if slug else None,
        "views": int(record.get("views") or 0),
        "date_posted": record.get("date_posted"),
        "html_length": len(body),
        "visible_word_count": len(visible_text(body).split()),
        "content_token_count": len(words),
        "content_sha256": hashlib.sha256(duplicate_guard.norm(visible_text(body)).encode("utf-8")).hexdigest(),
    }


def exact_groups(records: list[dict[str, Any]], key) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        value = key(record)
        if value:
            grouped.setdefault(value, []).append(record)
    groups = []
    for value, members in grouped.items():
        if len(members) < 2:
            continue
        summaries = [record_summary(member) for member in members]
        groups.append(
            {
                "key": value,
                "record_count": len(summaries),
                "active_count": sum(1 for item in summaries if item["active"]),
                "records": summaries,
            }
        )
    return sorted(groups, key=lambda item: (-item["active_count"], -item["record_count"], item["key"]))


def candidate_pairs(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    active = [record for record in records if str(record.get("active") or "0") == "1"]
    pairs: list[dict[str, Any]] = []
    token_cache = {id(record): title_tokens(str(record.get("title") or "")) for record in active}
    content_cache: dict[int, list[str]] = {}

    for index, left in enumerate(active):
        left_title = str(left.get("title") or "")
        left_norm = duplicate_guard.norm(left_title)
        left_tokens = token_cache[id(left)]
        if len(left_tokens) < 2:
            continue
        for right in active[index + 1 :]:
            right_title = str(right.get("title") or "")
            right_norm = duplicate_guard.norm(right_title)
            if left_norm == right_norm:
                continue
            right_tokens = token_cache[id(right)]
            if len(right_tokens) < 2:
                continue

            overlap = overlap_coefficient(left_tokens, right_tokens)
            jaccard = duplicate_guard.jaccard(left_tokens, right_tokens)
            contains = min(len(left_norm), len(right_norm)) >= 18 and (
                left_norm in right_norm or right_norm in left_norm
            )
            if not ((overlap >= 0.67 and jaccard >= 0.4) or contains):
                continue

            left_content = content_cache.setdefault(id(left), content_tokens(str(left.get("long") or "")))
            right_content = content_cache.setdefault(id(right), content_tokens(str(right.get("long") or "")))
            cosine = cosine_similarity(left_content, right_content)
            left_shingles = five_word_shingles(left_content)
            right_shingles = five_word_shingles(right_content)
            shingle_jaccard = duplicate_guard.jaccard(left_shingles, right_shingles)
            title_score = 0.65 * overlap + 0.35 * jaccard

            if title_score >= 0.9 or (title_score >= 0.8 and cosine >= 0.55):
                review_priority = "high"
            elif title_score >= 0.72:
                review_priority = "medium"
            else:
                review_priority = "low"

            pairs.append(
                {
                    "review_priority": review_priority,
                    "same_section": str(left.get("_section")) == str(right.get("_section")),
                    "title_score": round(title_score, 3),
                    "title_overlap": round(overlap, 3),
                    "title_jaccard": round(jaccard, 3),
                    "content_cosine": round(cosine, 3),
                    "five_word_shingle_jaccard": round(shingle_jaccard, 3),
                    "left": record_summary(left),
                    "right": record_summary(right),
                }
            )

    priority = {"high": 0, "medium": 1, "low": 2}
    return sorted(
        pairs,
        key=lambda item: (
            priority[item["review_priority"]],
            -item["title_score"],
            -item["content_cosine"],
            item["left"]["post_id"],
        ),
    )


def build_report(records_by_section: list[tuple[dict[str, str], list[dict[str, Any]]]]) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    sections = []
    for section, rows in records_by_section:
        decorated = [
            {**row, "_section": section["role"], "_block_id": section["block_id"]}
            for row in rows
        ]
        records.extend(decorated)
        sections.append(
            {
                **section,
                "record_count": len(decorated),
                "active_count": sum(1 for row in decorated if str(row.get("active") or "0") == "1"),
                "hidden_count": sum(1 for row in decorated if str(row.get("active") or "0") != "1"),
            }
        )

    title_groups = exact_groups(records, lambda row: duplicate_guard.norm(str(row.get("title") or "")))
    content_groups = exact_groups(
        records,
        lambda row: hashlib.sha256(
            duplicate_guard.norm(visible_text(str(row.get("long") or ""))).encode("utf-8")
        ).hexdigest()
        if str(row.get("long") or "").strip()
        else "",
    )
    pairs = candidate_pairs(records)

    return {
        "project": "VEVO_CONTENT",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "scope": "glossary_faq_blog",
        "sections": sections,
        "totals": {
            "record_count": len(records),
            "active_count": sum(1 for row in records if str(row.get("active") or "0") == "1"),
            "hidden_count": sum(1 for row in records if str(row.get("active") or "0") != "1"),
            "exact_title_group_count": len(title_groups),
            "public_exact_title_group_count": sum(1 for group in title_groups if group["active_count"] > 1),
            "exact_content_group_count": len(content_groups),
            "public_exact_content_group_count": sum(1 for group in content_groups if group["active_count"] > 1),
            "near_title_pair_count": len(pairs),
            "high_priority_pair_count": sum(1 for pair in pairs if pair["review_priority"] == "high"),
        },
        "exact_title_groups": title_groups,
        "exact_content_groups": content_groups,
        "near_title_pairs": pairs,
        "inventory": [record_summary(record) for record in records],
    }


def fetch_report() -> dict[str, Any]:
    records_by_section = []
    for section in CONTENT_SECTIONS:
        rows = content_mcp.admin_list_news_posts(section["block_id"], limit=content_mcp.DUPLICATE_SCAN_LIMIT)
        records_by_section.append((section, rows))
    return build_report(records_by_section)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    report = fetch_report()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report["totals"], ensure_ascii=False, indent=2))
    print(f"report={args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
