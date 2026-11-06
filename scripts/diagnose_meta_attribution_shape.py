#!/usr/bin/env python3
"""Read a bounded Meta Insights request and emit only response field names.

The diagnostic intentionally excludes tokens, account/campaign identities, names,
metric values and raw provider responses. Campaign identity is represented only by
a one-way SHA-256 prefix so multiple response shapes can be distinguished safely.
"""
from __future__ import annotations

import argparse
from datetime import date
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


API_VERSION = "v21.0"
BASE_URL = f"https://graph.facebook.com/{API_VERSION}"
PURCHASE_ACTION = "offsite_conversion.fb_pixel_purchase"
ADD_TO_CART_ACTION = "offsite_conversion.fb_pixel_add_to_cart"
PURCHASE_WINDOW = "7d_click"
MAX_SECRET_BYTES = 64 * 1024
MAX_RESPONSE_BYTES = 8 * 1024 * 1024


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise ValueError(reason)


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _bounded_json(response: Any) -> dict[str, Any]:
    raw = response.read(MAX_RESPONSE_BYTES + 1)
    _require(len(raw) <= MAX_RESPONSE_BYTES, "meta-response-too-large")
    payload = json.loads(raw.decode("utf-8"))
    _require(isinstance(payload, dict), "meta-response-not-object")
    return payload


def _api_json(
    path: str,
    params: dict[str, str | int],
    token: str,
    *,
    opener: Callable[..., Any] = urlopen,
) -> dict[str, Any]:
    _require(re.fullmatch(r"/[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*", path) is not None, "meta-path-invalid")
    request = Request(
        f"{BASE_URL}{path}?{urlencode(params)}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    )
    try:
        with opener(request, timeout=45) as response:
            return _bounded_json(response)
    except HTTPError as exc:
        status = int(exc.code)
        code = subcode = error_type = None
        try:
            payload = _bounded_json(exc)
            error = payload.get("error") if isinstance(payload.get("error"), dict) else {}
            code = error.get("code")
            subcode = error.get("error_subcode")
            error_type = error.get("type")
        except (ValueError, json.JSONDecodeError, UnicodeDecodeError):
            pass
        raise RuntimeError(
            f"Meta API HTTP failure status={status} code={code} subcode={subcode} type={error_type}"
        ) from None
    except URLError:
        raise RuntimeError("Meta API transport failure") from None


def _campaigns(account_id: str, token: str, *, opener: Callable[..., Any]) -> list[dict[str, Any]]:
    query: dict[str, str | int] = {"fields": "id,name,status,objective", "limit": 500}
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for _ in range(100):
        payload = _api_json(f"/{account_id}/campaigns", query, token, opener=opener)
        data = payload.get("data")
        _require(isinstance(data, list) and all(isinstance(row, dict) for row in data), "meta-campaign-list-invalid")
        rows.extend(data)
        paging = payload.get("paging") if isinstance(payload.get("paging"), dict) else {}
        if not paging.get("next"):
            return rows
        cursors = paging.get("cursors") if isinstance(paging.get("cursors"), dict) else {}
        cursor = str(cursors.get("after") or "")
        _require(bool(cursor) and cursor not in seen, "meta-campaign-pagination-invalid")
        seen.add(cursor)
        query["after"] = cursor
    raise ValueError("meta-campaign-pagination-limit")


def _field_names(value: Any, reason: str) -> list[str]:
    _require(isinstance(value, dict), reason)
    return sorted(key for key in value if isinstance(key, str))


def _classified_shapes(rows: Any, classes: dict[str, str], reason: str) -> list[dict[str, Any]]:
    if rows is None:
        return []
    _require(isinstance(rows, list) and all(isinstance(row, dict) for row in rows), reason)
    result = []
    for row in rows:
        action_class = classes.get(str(row.get("action_type") or ""))
        if action_class:
            result.append({"class": action_class, "fields": _field_names(row, reason)})
    return result


def collect_shape(
    *,
    project: str,
    account_id: str,
    token: str,
    date_from: str,
    date_to: str,
    opener: Callable[..., Any] = urlopen,
) -> dict[str, Any]:
    start, end = date.fromisoformat(date_from), date.fromisoformat(date_to)
    _require(start <= end and (end - start).days <= 30, "meta-diagnostic-date-window-invalid")
    _require(project in {"roy", "vevo"}, "meta-diagnostic-project-invalid")
    _require(bool(token), "meta-token-missing")
    normalized_account = account_id if account_id.startswith("act_") else f"act_{account_id}"
    _require(re.fullmatch(r"act_[0-9]+", normalized_account) is not None, "meta-account-id-invalid")

    insights_params: dict[str, str | int] = {
        "fields": (
            "spend,impressions,clicks,reach,cpc,cpm,ctr,frequency,unique_clicks,"
            "cost_per_unique_click,actions,conversions,cost_per_action_type,conversion_values"
        ),
        "time_range": json.dumps({"since": date_from, "until": date_to}, separators=(",", ":")),
        "level": "campaign",
        "action_attribution_windows": json.dumps([PURCHASE_WINDOW]),
        "action_report_time": "impression",
    }
    request_contract = {
        "api_version": API_VERSION,
        "fields": insights_params["fields"].split(","),
        "date_from": date_from,
        "date_to": date_to,
        "level": "campaign",
        "action_attribution_windows": [PURCHASE_WINDOW],
        "action_report_time": "impression",
    }
    shapes = []
    campaigns = _campaigns(normalized_account, token, opener=opener)
    for campaign in campaigns:
        campaign_id = str(campaign.get("id") or "")
        _require(re.fullmatch(r"[0-9]+", campaign_id) is not None, "meta-campaign-id-invalid")
        payload = _api_json(f"/{campaign_id}/insights", insights_params, token, opener=opener)
        data = payload.get("data")
        _require(isinstance(data, list) and len(data) <= 1, "meta-campaign-insights-invalid")
        paging = payload.get("paging") if isinstance(payload.get("paging"), dict) else {}
        _require(not paging.get("next"), "meta-campaign-insights-paginated")
        item: dict[str, Any] = {
            "campaign_hash": hashlib.sha256(campaign_id.encode("ascii")).hexdigest()[:16],
            "has_data": bool(data),
            "insight_fields": [],
            "action_shapes": [],
            "cost_per_action_shapes": [],
        }
        if data:
            row = data[0]
            _require(isinstance(row, dict), "meta-campaign-insight-row-invalid")
            item["insight_fields"] = _field_names(row, "meta-campaign-insight-row-invalid")
            item["action_shapes"] = _classified_shapes(
                row.get("actions"),
                {PURCHASE_ACTION: "purchase", ADD_TO_CART_ACTION: "add_to_cart"},
                "meta-actions-invalid",
            )
            item["cost_per_action_shapes"] = _classified_shapes(
                row.get("cost_per_action_type"),
                {PURCHASE_ACTION: "purchase"},
                "meta-cost-per-action-invalid",
            )
        shapes.append(item)

    result = {
        "schema_version": 1,
        "project": project,
        "request_contract": request_contract,
        "request_contract_sha256": _digest(request_contract),
        "campaigns_seen": len(campaigns),
        "campaign_shapes": shapes,
    }
    result["shape_sha256"] = _digest(result)
    return result


def _load_secret(path: Path) -> dict[str, Any]:
    raw = path.read_bytes()
    _require(0 < len(raw) <= MAX_SECRET_BYTES, "meta-secret-size-invalid")
    secret = json.loads(raw.decode("utf-8"))
    _require(isinstance(secret, dict), "meta-secret-not-object")
    return secret


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True, choices=("roy", "vevo"))
    parser.add_argument("--secret-file", required=True, type=Path)
    parser.add_argument("--date-from", required=True)
    parser.add_argument("--date-to", required=True)
    args = parser.parse_args()
    secret = _load_secret(args.secret_file)
    result = collect_shape(
        project=args.project,
        account_id=str(secret.get("FACEBOOK_AD_ACCOUNT_ID") or ""),
        token=str(secret.get("FACEBOOK_ACCESS_TOKEN") or ""),
        date_from=args.date_from,
        date_to=args.date_to,
    )
    print("META_ATTRIBUTION_SHAPE_DIAGNOSTIC:" + json.dumps(result, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
