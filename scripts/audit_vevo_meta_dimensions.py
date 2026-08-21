from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping
from urllib.parse import parse_qsl, urlparse

import requests

from facebook_ads import FacebookAdsClient


MARKER = "VEVO_META_DIMENSION_AUDIT_OK:"
FAIL_MARKER = "VEVO_META_DIMENSION_AUDIT_FAIL:"
START_MARKER = "VEVO_META_DIMENSION_AUDIT_START:schema=1"
FORBIDDEN_QUERY_KEYS = frozenset({"fbclid", "_fbp", "_fbc"})
DIMENSIONS = (
    "utm_source",
    "utm_medium",
    "utm_id",
    "utm_content",
    "meta_adset_id",
    "meta_placement",
)
PLACEMENTS = frozenset(
    {
        "facebook_feed",
        "facebook_marketplace",
        "facebook_reels",
        "facebook_stories",
        "facebook_video_feeds",
        "instagram_explore",
        "instagram_feed",
        "instagram_profile_feed",
        "instagram_reels",
        "instagram_stories",
        "messenger_inbox",
        "threads_feed",
        "audience_network",
    }
)


class MetaDimensionAuditError(RuntimeError):
    pass


def _safe_get_json(
    client: FacebookAdsClient,
    url: str,
    params: Mapping[str, Any],
    context: str,
) -> dict[str, Any]:
    try:
        return client._get_json(url, dict(params), context)
    except requests.exceptions.RequestException:
        raise MetaDimensionAuditError(f"Meta Graph read failed during {context}") from None


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except InvalidOperation as exc:
        raise MetaDimensionAuditError("Meta insights returned a non-numeric amount") from exc


def _integer(value: Any) -> int:
    try:
        return int(Decimal(str(value or "0")))
    except (InvalidOperation, ValueError) as exc:
        raise MetaDimensionAuditError("Meta insights returned a non-integer count") from exc


def _paged(client: FacebookAdsClient, url: str, params: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    request_params = dict(params)
    for _ in range(100):
        payload = _safe_get_json(client, url, request_params, "delivery insights")
        page = payload.get("data") or []
        if not isinstance(page, list) or any(not isinstance(row, dict) for row in page):
            raise MetaDimensionAuditError("Meta audit response shape changed")
        rows.extend(page)
        after = ((payload.get("paging") or {}).get("cursors") or {}).get("after")
        if not after:
            return rows
        request_params["after"] = after
    raise MetaDimensionAuditError("Meta audit pagination exceeded the fixed ceiling")


def _iter_strings(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for child in value.values():
            yield from _iter_strings(child)
    elif isinstance(value, list):
        for child in value:
            yield from _iter_strings(child)


def _query_values(creative: Mapping[str, Any]) -> dict[str, set[str]]:
    values: dict[str, set[str]] = {}

    def add_query(raw: str) -> None:
        text = raw.strip()
        if not text:
            return
        query = urlparse(text).query if text.lower().startswith(("http://", "https://")) else text.lstrip("?")
        for key, value in parse_qsl(query, keep_blank_values=True):
            values.setdefault(key.strip().lower(), set()).add(value.strip())

    url_tags = creative.get("url_tags")
    if isinstance(url_tags, str):
        add_query(url_tags)
    for candidate in _iter_strings(
        {
            "object_story_spec": creative.get("object_story_spec"),
            "asset_feed_spec": creative.get("asset_feed_spec"),
        }
    ):
        if candidate.lower().startswith(("http://", "https://")):
            add_query(candidate)
    return values


def _accepted_dimension(field: str, values: set[str], identity: Mapping[str, str]) -> bool:
    if field == "utm_source":
        return bool(values & {"meta", "facebook", "instagram"})
    if field == "utm_medium":
        return bool(values & {"paid_social", "social", "cpc"})
    if field == "utm_id":
        return bool(values & {"{{campaign.id}}", identity["campaign_id"]})
    if field == "utm_content":
        return bool(values & {"{{ad.id}}", identity["ad_id"]})
    if field == "meta_adset_id":
        return bool(values & {"{{adset.id}}", identity["adset_id"]})
    if field == "meta_placement":
        return "{{placement}}" in values or bool(values & PLACEMENTS)
    raise MetaDimensionAuditError(f"unknown dimension contract field: {field}")


def _recommended_dimension(field: str, values: set[str]) -> bool:
    expected = {
        "utm_source": "meta",
        "utm_medium": "paid_social",
        "utm_id": "{{campaign.id}}",
        "utm_content": "{{ad.id}}",
        "meta_adset_id": "{{adset.id}}",
        "meta_placement": "{{placement}}",
    }
    return expected[field] in values


def _coverage_entry(ads: int, clicks: int, spend: Decimal, total_clicks: int, total_spend: Decimal) -> dict[str, Any]:
    return {
        "ads": ads,
        "clicks": clicks,
        "spend_eur": float(spend.quantize(Decimal("0.01"))),
        "click_coverage_pct": round(100 * clicks / total_clicks, 2) if total_clicks else None,
        "spend_coverage_pct": round(float(100 * spend / total_spend), 2) if total_spend else None,
    }


def build_audit_summary(
    insights: list[dict[str, Any]],
    ads: Mapping[str, Mapping[str, Any]],
    *,
    since: str,
    until: str,
    api_version: str,
) -> dict[str, Any]:
    if not insights:
        raise MetaDimensionAuditError("Meta returned no ad-level delivery rows for the audit window")

    totals = {field: {"ads": 0, "clicks": 0, "spend": Decimal("0")} for field in DIMENSIONS}
    accepted_all = {"ads": 0, "clicks": 0, "spend": Decimal("0")}
    recommended_all = {"ads": 0, "clicks": 0, "spend": Decimal("0")}
    total_clicks = 0
    total_spend = Decimal("0")
    configured_ads = 0
    forbidden_parameter_ads = 0
    campaign_ids: set[str] = set()
    adset_ids: set[str] = set()
    seen_ad_ids: set[str] = set()

    for row in insights:
        ad_id = str(row.get("ad_id") or "")
        campaign_id = str(row.get("campaign_id") or "")
        adset_id = str(row.get("adset_id") or "")
        if not (ad_id.isdigit() and campaign_id.isdigit() and adset_id.isdigit()):
            raise MetaDimensionAuditError("Meta insights returned an invalid ad identity")
        if ad_id in seen_ad_ids:
            raise MetaDimensionAuditError("Meta insights returned duplicate ad delivery rows")
        seen_ad_ids.add(ad_id)
        clicks = _integer(row.get("clicks"))
        spend = _decimal(row.get("spend"))
        total_clicks += clicks
        total_spend += spend
        campaign_ids.add(campaign_id)
        adset_ids.add(adset_id)

        ad = ads.get(ad_id)
        for field, expected in (
            ("id", ad_id),
            ("campaign_id", campaign_id),
            ("adset_id", adset_id),
        ):
            actual = str((ad or {}).get(field) or "")
            if actual and actual != expected:
                raise MetaDimensionAuditError("Meta creative identity differs from delivery identity")
        creative = (ad or {}).get("creative") or {}
        if not isinstance(creative, dict):
            creative = {}
        if creative:
            configured_ads += 1
        query_values = _query_values(creative)
        if set(query_values) & FORBIDDEN_QUERY_KEYS:
            forbidden_parameter_ads += 1
        identity = {"ad_id": ad_id, "campaign_id": campaign_id, "adset_id": adset_id}
        accepted = {
            field: _accepted_dimension(field, query_values.get(field, set()), identity)
            for field in DIMENSIONS
        }
        recommended = {
            field: _recommended_dimension(field, query_values.get(field, set()))
            for field in DIMENSIONS
        }
        for field, present in accepted.items():
            if present:
                totals[field]["ads"] += 1
                totals[field]["clicks"] += clicks
                totals[field]["spend"] += spend
        if all(accepted.values()):
            accepted_all["ads"] += 1
            accepted_all["clicks"] += clicks
            accepted_all["spend"] += spend
        if all(recommended.values()):
            recommended_all["ads"] += 1
            recommended_all["clicks"] += clicks
            recommended_all["spend"] += spend

    summary = {
        "schema_version": 1,
        "api_version": api_version,
        "window": {"since": since, "until": until, "complete_utc_days": 30},
        "traffic_ads": len(insights),
        "traffic_campaigns": len(campaign_ids),
        "traffic_adsets": len(adset_ids),
        "ads_with_creative_configuration": configured_ads,
        "total_clicks": total_clicks,
        "total_spend_eur": float(total_spend.quantize(Decimal("0.01"))),
        "collector_compatible_coverage": {
            field: _coverage_entry(
                values["ads"], values["clicks"], values["spend"], total_clicks, total_spend
            )
            for field, values in totals.items()
        },
        "collector_compatible_all_dimensions": _coverage_entry(
            accepted_all["ads"], accepted_all["clicks"], accepted_all["spend"], total_clicks, total_spend
        ),
        "recommended_macro_contract_all_dimensions": _coverage_entry(
            recommended_all["ads"],
            recommended_all["clicks"],
            recommended_all["spend"],
            total_clicks,
            total_spend,
        ),
        "forbidden_click_identifier_parameter_ads": forbidden_parameter_ads,
    }
    forbidden_output_keys = {
        "ad_id", "ad_ids", "adset_id", "adset_ids", "campaign_id", "campaign_ids",
        "url", "urls", "name", "names",
    }
    if forbidden_output_keys & set(summary):
        raise MetaDimensionAuditError("sanitized Meta audit summary contains a forbidden key")
    serialized = json.dumps(summary, sort_keys=True).lower()
    if any(marker in serialized for marker in ("http://", "https://", "fbclid=", "_fbp=", "_fbc=")):
        raise MetaDimensionAuditError("sanitized Meta audit summary contains a forbidden value")
    return summary


def run_audit(client: FacebookAdsClient) -> dict[str, Any]:
    if not client.is_configured:
        raise MetaDimensionAuditError("Meta Ads credentials are not configured in the managed runtime")
    today = datetime.now(timezone.utc).date()
    since = (today - timedelta(days=30)).isoformat()
    until = (today - timedelta(days=1)).isoformat()
    insights = _paged(
        client,
        f"{client.base_url}/{client.ad_account_id}/insights",
        {
            "fields": "ad_id,campaign_id,adset_id,spend,impressions,clicks",
            "level": "ad",
            "time_range": json.dumps({"since": since, "until": until}, separators=(",", ":")),
            "limit": 500,
        },
    )
    ad_ids = sorted({str(row.get("ad_id") or "") for row in insights})
    ads: dict[str, Mapping[str, Any]] = {}
    for offset in range(0, len(ad_ids), 50):
        batch = ad_ids[offset : offset + 50]
        payload = _safe_get_json(
            client,
            f"{client.base_url}/",
            {
                "ids": ",".join(batch),
                "fields": (
                    "id,campaign_id,adset_id,effective_status,"
                    "creative{id,url_tags,object_story_spec,asset_feed_spec}"
                ),
            },
            "creative configuration",
        )
        ads.update({str(key): value for key, value in payload.items() if isinstance(value, dict)})
    return build_audit_summary(
        insights,
        ads,
        since=since,
        until=until,
        api_version=client.api_version,
    )


def main() -> int:
    print(START_MARKER, flush=True)
    try:
        summary = run_audit(FacebookAdsClient())
        print(MARKER + json.dumps(summary, sort_keys=True, separators=(",", ":")), flush=True)
        return 0
    except MetaDimensionAuditError as exc:
        print(FAIL_MARKER + str(exc), file=sys.stderr, flush=True)
        return 1
    except Exception:
        print(FAIL_MARKER + "unexpected_internal_error", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
