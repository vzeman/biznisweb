"""Pure independent country-card verification; never grants publication approval.

Native Meta files use their manifest basenames (NNNN-response.json and matching
NNNN-receipt.json) below --meta-directory. Google input is a daily source object
or the preflight providers.google_ads.daily wrapper. Optional financial CSV is
the full item export; repeated canonical revenue/credit fields are counted once.
No provider, SDK, production calculator, or country-builder code is imported.
"""
from __future__ import annotations

import argparse
import csv
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import re
from typing import Any
from urllib.parse import parse_qsl, urlparse


COUNTRIES = ("sk", "cz", "hu")
WINDOWS = {"daily": 1, "weekly": 7, "monthly": 30, "all_time": None}
METRICS = {"revenue", "profit", "orders", "aov", "cac", "roas", "pre_ad_contribution_margin",
           "post_ad_margin", "company_margin_with_fixed"}
DEPENDENT = {"profit", "cac", "roas", "post_ad_margin", "company_margin_with_fixed"}
ACCOUNT = "act_1131531703632101"
ORIGIN = "https://graph.facebook.com"
MONEY_EPSILON = Decimal("0.000001")  # Floating serialization identity only; no source reallocation.


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def decode(raw: str | bytes) -> Any:
    def unique(pairs):
        if len(pairs) != len(dict(pairs)):
            raise ValueError("duplicate_json_key")
        return dict(pairs)
    return json.loads(raw, object_pairs_hook=unique,
                      parse_constant=lambda _value: (_ for _ in ()).throw(ValueError("nonfinite_json")))


def load_payload(path: Path) -> dict:
    raw = path.read_text(encoding="utf-8-sig")
    if raw.lstrip().startswith("<"):
        match = re.search(r'<script\s+id=["\']report-dashboard-json["\']\s+type=["\']application/json["\']>(.*?)</script>', raw, re.S | re.I)
        if not match:
            raise ValueError("embedded_payload_missing")
        raw = match.group(1)
    value = decode(raw)
    if not isinstance(value, dict):
        raise ValueError("payload_not_object")
    return value


def number(value: Any) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise ValueError("not_a_number")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError("not_a_number") from exc
    if not result.is_finite():
        raise ValueError("nonfinite_number")
    return result


def iso(value: Any) -> str:
    if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
        raise ValueError("invalid_iso_date")
    return value


def days(first: str, last: str) -> list[str]:
    start, end = date.fromisoformat(iso(first)), date.fromisoformat(iso(last))
    if start > end:
        raise ValueError("reversed_dates")
    return [(start + timedelta(days=offset)).isoformat() for offset in range((end - start).days + 1)]


def window_dates(first: str, last: str, key: str) -> list[str]:
    count = WINDOWS[key]
    start = first if count is None else max(first, (date.fromisoformat(last) - timedelta(days=count - 1)).isoformat())
    return days(start, last)


def _meta_paging_cursor(value: Any, params: dict, direction: str, cursors: dict) -> str:
    """Check native link metadata independently; request receipts stay on v21.

    Meta may advertise its upgraded v24 endpoint in a response to a v21 request.
    The provider follows only the cursor using its original request URL.
    """
    if not isinstance(value, str):
        raise ValueError("meta_daily_cursor")
    endpoint = urlparse(value)
    pairs = parse_qsl(endpoint.query, keep_blank_values=True)
    cursor_key = "after" if direction == "next" else "before"
    cursor = cursors.get(cursor_key)
    base = {key: item for key, item in params.items() if key != "after"}
    if (endpoint.scheme != "https" or endpoint.netloc != "graph.facebook.com" or endpoint.fragment or endpoint.params
            or endpoint.path not in {f"/{version}/{ACCOUNT}/insights" for version in ("v21.0", "v24.0")}
            or len(pairs) != len(dict(pairs)) or set(dict(pairs)) != set(base) | {cursor_key}
            or not isinstance(cursor, str) or not cursor):
        raise ValueError("meta_daily_cursor")
    query = dict(pairs)
    if query.pop(cursor_key) != cursor:
        raise ValueError("meta_daily_cursor")
    query["time_range"] = decode(query["time_range"])
    if isinstance(base["time_range"], str):
        base["time_range"] = decode(base["time_range"])
    if query != base:
        raise ValueError("meta_daily_cursor_scope")
    return cursor


def _native_meta(manifest: dict, directory: Path) -> dict:
    """Reconstruct exact complete per-query native responses, not report values."""
    if (manifest.get("project") != "vevo" or manifest.get("complete") is not True
            or manifest.get("currency") != "EUR" or manifest.get("meta_account_id") != ACCOUNT):
        raise ValueError("meta_manifest_identity")
    release = manifest.get("release_id", "")
    if not re.fullmatch("[a-f0-9]{32}", release):
        raise ValueError("meta_release_identity")
    mode = manifest.get("capture_mode")
    prefix = (f"data/vevo/reporting/runtime/probes/{release}/artifacts/source-inputs/meta/" if mode == "probe"
              else f"daily-reports/vevo/source-inputs/{release}/meta/" if mode == "live" else "")
    if not prefix:
        raise ValueError("meta_capture_mode")
    first, last = iso(manifest["report_from_date"]), iso(manifest["report_to_date"])
    expected = {(first, last)} | {(max(first, (date.fromisoformat(last) - timedelta(days=n - 1)).isoformat()), last) for n in (7, 30, 90)}
    records = manifest["native_responses"]
    if not isinstance(records, list) or not 1 <= len(records) <= 300:
        raise ValueError("meta_response_inventory")
    objects, total_size = {}, 0
    for index, record in enumerate(records, 1):
        name = f"{index:04d}-response.json"
        if (record.get("key") != prefix + name or record.get("origin") != ORIGIN
                or record.get("method") != "GET" or record.get("status") != 200
                or record.get("headers_or_credentials_persisted") is not False
                or not re.fullmatch("[a-f0-9]{64}", record.get("sha256", ""))
                or not re.fullmatch("[a-f0-9]{64}", record.get("original_response_sha256", ""))):
            raise ValueError("meta_response_receipt")
        target = directory / name
        if target.is_symlink() or not target.resolve().is_relative_to(directory.resolve()):
            raise ValueError("meta_response_path")
        raw = target.read_bytes()
        total_size += len(raw)
        if not 0 < len(raw) <= 8 * 1024 * 1024 or total_size > 128 * 1024 * 1024:
            raise ValueError("meta_response_size")
        if record.get("size") != len(raw) or record["sha256"] != sha(raw):
            raise ValueError("meta_response_hash")
        receipt = (directory / f"{index:04d}-receipt.json").read_bytes()
        canonical = json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        if receipt != canonical:
            raise ValueError("meta_receipt_hash")
        objects[index] = decode(raw)
    groups, reconstructed, consumed = manifest["query_groups"], {}, set()
    if not isinstance(groups, dict) or any(group.get("complete") is not True for group in groups.values()):
        raise ValueError("meta_group_incomplete")
    for group in groups.values():
        if group.get("kind") != "country_daily":
            continue
        query = group["query"]
        scope = (query["time_range"]["since"], query["time_range"]["until"])
        if (scope not in expected or scope in reconstructed or query.get("fields") != "spend"
                or query.get("level") != "account" or query.get("breakdowns") != "country"
                or str(query.get("time_increment")) != "1"):
            raise ValueError("meta_daily_query")
        identity_number = group["currency_metadata_page"]
        if type(identity_number) is not int or identity_number not in objects:
            raise ValueError("meta_currency_receipt")
        identity, identity_record = objects[identity_number], records[identity_number - 1]
        if (identity_record.get("kind") != "identity" or identity_record.get("path") != f"/v21.0/{ACCOUNT}"
                or identity.get("currency") != "EUR" or identity.get("id") not in (ACCOUNT, ACCOUNT[4:])):
            raise ValueError("meta_currency_receipt")
        calendar = dict.fromkeys(days(*scope))
        daily = {day: {} for day in calendar}
        seen, seen_cursors, after = set(), set(), None
        page_numbers = group["pages"]
        if not isinstance(page_numbers, list) or not page_numbers:
            raise ValueError("meta_daily_pages")
        for index in page_numbers:
            if type(index) is not int or index not in objects or index in consumed:
                raise ValueError("meta_daily_page_identity")
            consumed.add(index)
            record, body = records[index - 1], objects[index]
            params = record["public_params"]
            limits = decode(params["time_range"]) if isinstance(params["time_range"], str) else params["time_range"]
            if (record.get("kind") != "country_daily" or record.get("path") != f"/v21.0/{ACCOUNT}/insights"
                    or limits != {"since": scope[0], "until": scope[1]}
                    or params.get("fields") != "spend" or params.get("level") != "account"
                    or params.get("breakdowns") != "country" or str(params.get("time_increment")) != "1"
                    or str(params.get("limit")) != "500"
                    or not set(params) <= {"fields", "level", "time_range", "breakdowns", "time_increment", "limit", "after"}
                    or params.get("after") != after or not isinstance(body.get("data"), list)):
                raise ValueError("meta_daily_page_scope")
            for row in body["data"]:
                if not isinstance(row, dict) or set(row) != {"date_start", "date_stop", "country", "spend"}:
                    raise ValueError("meta_daily_row_shape")
                day, country = iso(row["date_start"]), row["country"]
                if (day not in daily or row.get("date_stop") != day or not isinstance(country, str)
                        or not (country == "unknown" or re.fullmatch("[A-Za-z]{2}", country))):
                    raise ValueError("meta_daily_row_scope")
                country = country.lower()
                if (day, country) in seen:
                    raise ValueError("meta_daily_duplicate")
                seen.add((day, country))
                amount = number(row["spend"])
                if amount < 0:
                    raise ValueError("meta_daily_negative")
                daily[day][country] = amount
            paging = body.get("paging") or {}
            if not isinstance(paging, dict) or not set(paging) <= {"next", "previous", "cursors"}:
                raise ValueError("meta_daily_cursor")
            cursors = paging.get("cursors") or {}
            if not isinstance(cursors, dict):
                raise ValueError("meta_daily_cursor")
            if "previous" in paging:
                _meta_paging_cursor(paging["previous"], params, "previous", cursors)
            if "next" in paging:
                after = _meta_paging_cursor(paging["next"], params, "next", cursors)
                if not body["data"] or after in seen_cursors:
                    raise ValueError("meta_daily_cursor")
                seen_cursors.add(after)
            else:
                after = None
        if after is not None:
            raise ValueError("meta_daily_unfinished")
        reconstructed[scope] = daily
    if set(reconstructed) != expected:
        raise ValueError("meta_four_daily_ranges_missing")
    return reconstructed


def _google_daily(source: dict) -> dict:
    if "providers" in source:
        source = source["providers"]["google_ads"]["daily"]
    if (source.get("status") != "ok" or source.get("currency") != "EUR"
            or source.get("basis") != "google_user_location_daily"
            or not re.fullmatch("[0-9]+", str(source.get("customer_id", "")))):
        raise ValueError("google_daily_identity")
    calendar = days(source["date_from"], source["date_to"])
    raw = source["daily_spend_by_country"]
    if set(raw) != set(calendar) or set(source["daily_account_spend"]) != set(calendar):
        raise ValueError("google_daily_calendar")
    result, total = {}, Decimal(0)
    for day in calendar:
        values = {country: number(value) for country, value in raw[day].items()}
        if any(value < 0 for value in values.values()) or any(not (re.fullmatch("[a-z]{2}", key) or key in {"unknown", "unallocated"}) for key in values):
            raise ValueError("google_daily_amount")
        account = number(source["daily_account_spend"][day])
        if sum(values.values(), Decimal(0)) != account:
            raise ValueError("google_daily_conservation")
        result[day] = values
        total += account
    if total != number(source["total_spend"]) or total != number(source["account_total_spend"]):
        raise ValueError("google_total_conservation")
    return result


def _country(value: Any) -> str:
    text = str(value or "").strip().lower()
    return {"slovakia": "sk", "slovensko": "sk", "czech republic": "cz", "czechia": "cz",
            "česko": "cz", "cesko": "cz", "hungary": "hu", "maďarsko": "hu", "madarsko": "hu"}.get(text, text)


def _orders(rows: list[dict]) -> list[dict]:
    orders = {}
    for row in rows:
        key = str(row["order_num"]).strip()
        if not key:
            raise ValueError("financial_order_identity")
        candidate = {
            "date": iso(str(row["purchase_date"])[:10]),
            "country": _country(row.get("delivery_country") or row.get("invoice_country")),
            "revenue": number(row["order_revenue_net"]),
            "credit": number(row.get("order_credit_net_adjustment") or 0),
            "email": str(row.get("customer_email") or "").strip().lower(),
            "first": str(row.get("customer_first_purchase_date") or "")[:10],
        }
        if key in orders and candidate != orders[key]:
            raise ValueError("financial_order_fields_conflict")
        orders[key] = candidate
    history = {}
    for order in orders.values():
        first = iso(order["first"]) if order["first"].lower() not in {"", "nan", "none", "nat"} else order["date"]
        if order["email"] not in history or first < history[order["email"]]:
            history[order["email"]] = first
    for order in orders.values():
        order["first"] = history[order["email"]]
    return list(orders.values())


def verify_country_cfo_payload(payload: dict, *, meta_manifest: dict | None = None,
                               meta_directory: Path | None = None, google_daily_source: dict | None = None,
                               financial_rows: list[dict] | None = None, global_daily: list[dict] | None = None) -> dict:
    errors, checks = [], {}
    def require(condition, code):
        if not condition:
            errors.append(code)
        return bool(condition)
    def equal(actual, expected, code):
        try:
            return require(abs(number(actual) - number(expected)) <= MONEY_EPSILON, code)
        except ValueError:
            return require(False, code)
    dashboard = payload.get("dashboard", payload)
    kpis = dashboard.get("kpis") or {}
    metadata = kpis.get("country_metadata") or {}
    first = payload.get("date_from") or metadata.get("date_from")
    last = payload.get("date_to") or metadata.get("date_to")
    try:
        calendar = days(first, last)
        countries = kpis["countries"]
        require(payload.get("project") in (None, "vevo"), "project_identity")
        require(kpis.get("country_order") == list(COUNTRIES) and set(countries) == set(COUNTRIES), "country_inventory")
        require(len(kpis.get("metric_defs", [])) == 9 and {item.get("key") for item in kpis.get("metric_defs", [])} == METRICS, "global_metric_inventory")
        require(set(kpis.get("windows", {})) == set(WINDOWS), "global_window_inventory")
        for country in COUNTRIES:
            scoped = countries[country]
            require(scoped.get("country") == country and scoped.get("status") == "ok", country + ":unusable_source")
            require([item.get("key") for item in scoped.get("metric_defs", [])] == [item.get("key") for item in kpis.get("metric_defs", [])]
                    and {item.get("key") for item in scoped.get("metric_defs", [])} == METRICS, country + ":metric_inventory")
            require(set(scoped.get("windows", {})) == set(WINDOWS), country + ":window_inventory")
            info = scoped.get("metadata") or {}
            require(info.get("date_from") == first and info.get("date_to") == last
                    and info.get("calendar_complete") is True and info.get("calendar_days") == len(calendar), country + ":calendar_identity")
            for window in WINDOWS:
                card = scoped["windows"][window]
                metrics = card["metrics"]
                prefix = country + ":" + window + ":"
                require(set(metrics) == METRICS, prefix + "metric_keys")
                for key, value in metrics.items():
                    if value is None:
                        require(key == "cac" or (scoped.get("status") == "unavailable" and key in DEPENDENT), prefix + "unexpected_na_" + key)
                    else:
                        try:
                            number(value)
                        except ValueError:
                            require(False, prefix + "nonfinite_" + key)
                revenue, count = number(metrics["revenue"]), number(metrics["orders"])
                require(count >= 0 and count == count.to_integral_value(), prefix + "order_count")
                equal(metrics["aov"], revenue / count if count > 0 else 0, prefix + "aov")
                if metrics.get("profit") is not None:
                    profit = number(metrics["profit"])
                    absolute = number(card["secondary_metrics"]["company_margin_with_fixed"])
                    equal(metrics["post_ad_margin"], profit / revenue * 100 if revenue > 0 else 0, prefix + "post_ad_margin")
                    equal(metrics["company_margin_with_fixed"], absolute / revenue * 100 if revenue > 0 else 0, prefix + "company_margin")
                    require(profit - absolute >= -MONEY_EPSILON, prefix + "negative_fixed")
                trend, global_trend = card["trend"], kpis["windows"][window]["trend"]
                require(trend.get("dates") == global_trend.get("dates") and all(day in calendar for day in trend.get("dates", [])), prefix + "trend_dates")
                require(set(trend.get("metrics", {})) == METRICS, prefix + "trend_metric_inventory")
                for key, series in trend.get("metrics", {}).items():
                    require(len(series) == len(trend.get("dates", [])), prefix + "trend_length_" + key)
                    for value in series:
                        if value is not None:
                            number(value)
                        else:
                            require(key == "cac" or (scoped.get("status") == "unavailable" and key in DEPENDENT), prefix + "trend_na_" + key)
                comparisons = scoped.get("comparisons", {}).get(window, {})
                require(set(comparisons) == METRICS, prefix + "comparison_inventory")
                for key, values in comparisons.items():
                    require(set(values) == set(kpis.get("comparisons", {}).get(window, {}).get(key, {})), prefix + "comparison_keys_" + key)
                    for value in values.values():
                        if value is not None:
                            number(value)
        global_all = kpis["windows"]["all_time"]
        global_fixed = number(global_all["metrics"]["profit"]) - number(global_all["secondary_metrics"]["company_margin_with_fixed"])
        displayed_fixed = sum((number(countries[c]["windows"]["all_time"]["metrics"]["profit"])
                              - number(countries[c]["windows"]["all_time"]["secondary_metrics"]["company_margin_with_fixed"]) for c in COUNTRIES), Decimal(0))
        equal(displayed_fixed + number(metadata["company_minus_displayed_countries"]["fixed_daily_cost"]), global_fixed, "shared_fixed_conservation")
        for metric, residual in (("revenue", "total_revenue"), ("orders", "unique_orders")):
            displayed = sum((number(countries[c]["windows"]["all_time"]["metrics"][metric]) for c in COUNTRIES), Decimal(0))
            equal(displayed + number(metadata["company_minus_displayed_countries"][residual]), global_all["metrics"][metric], "country_" + metric + "_conservation")
        checks["structure_and_literal_identities"] = not errors
    except (AttributeError, KeyError, TypeError, ValueError, InvalidOperation):
        require(False, "invalid_card_structure_or_number")
        return {"passed": False, "checks": checks, "errors": errors, "financial_approval": False, "publication_approval": False}

    native, google, orders = None, None, None
    if meta_manifest is not None or meta_directory is not None:
        try:
            native = _native_meta(meta_manifest, Path(meta_directory))
            require((first, last) in native, "meta_payload_range_missing")
            checks["native_meta_four_ranges"] = True
        except (AttributeError, OSError, KeyError, TypeError, ValueError, InvalidOperation):
            require(False, "native_meta_source_invalid")
            checks["native_meta_four_ranges"] = False
    if google_daily_source is not None:
        try:
            google = _google_daily(google_daily_source)
            require(set(calendar) <= set(google), "google_payload_range_missing")
            checks["google_daily_source"] = True
        except (AttributeError, KeyError, TypeError, ValueError, InvalidOperation):
            require(False, "google_daily_source_invalid")
            checks["google_daily_source"] = False
    revenue_mode = None
    if financial_rows is not None:
        try:
            orders = _orders(financial_rows)
            scoped_orders = [order for order in orders if first <= order["date"] <= last]
            if number(global_all["metrics"]["orders"]) != len(scoped_orders):
                raise ValueError("financial_csv_global_orders")
            raw_revenue = sum((order["revenue"] for order in scoped_orders), Decimal(0))
            credit = sum((order["credit"] for order in scoped_orders), Decimal(0))
            expected = number(global_all["metrics"]["revenue"])
            if abs(raw_revenue - expected) <= MONEY_EPSILON:
                revenue_mode = "already_adjusted_canonical_order_revenue"
            elif abs(raw_revenue + credit - expected) <= MONEY_EPSILON:
                revenue_mode = "apply_order_credit_once"
            else:
                raise ValueError("financial_csv_global_revenue")
            checks["financial_csv_revenue_basis"] = True
        except (KeyError, TypeError, ValueError, InvalidOperation):
            require(False, "financial_csv_source_invalid")
            checks["financial_csv_revenue_basis"] = False
            orders = None
    daily_by_date = None
    if global_daily is not None:
        try:
            daily_by_date = {iso(str(row["date"])[:10]): row for row in global_daily}
            if len(daily_by_date) != len(global_daily) or not set(calendar) <= set(daily_by_date):
                raise ValueError("global_daily_calendar")
            checks["global_daily_calendar"] = True
        except (KeyError, TypeError, ValueError):
            require(False, "global_daily_source_invalid")
            daily_by_date = None
    source_checks = 0
    profit_identity_checks = 0
    source_errors_before = len(errors)
    if native is not None and (first, last) in native and google is not None and set(calendar) <= set(google):
        for country in COUNTRIES:
            for window in WINDOWS:
                scope = window_dates(first, last, window)
                card = countries[country]["windows"][window]
                metrics = card["metrics"]
                prefix = country + ":" + window + ":source_"
                ads = sum((native[(first, last)][day].get(country, Decimal(0)) + google[day].get(country, Decimal(0)) for day in scope), Decimal(0))
                revenue = number(metrics["revenue"])
                equal(metrics["roas"], min(revenue / ads, Decimal(15)) if ads > 0 else 0, prefix + "mer")
                if revenue > 0:
                    pre_ad = revenue * number(metrics["pre_ad_contribution_margin"]) / 100
                    equal(metrics["profit"], pre_ad - ads, prefix + "post_ad_profit")
                    profit_identity_checks += 1
                if orders is not None:
                    selected = [order for order in orders if order["country"] == country and order["date"] in scope]
                    actual_revenue = sum((order["revenue"] + (order["credit"] if revenue_mode == "apply_order_credit_once" else 0) for order in selected), Decimal(0))
                    equal(metrics["revenue"], actual_revenue, prefix + "csv_revenue")
                    equal(metrics["orders"], len(selected), prefix + "csv_orders")
                    equal(card["secondary_metrics"].get("revenue_credit_adjustment", 0), sum((order["credit"] for order in selected), Decimal(0)), prefix + "credit_once")
                    acquired = {(order["date"], order["email"]) for order in selected if order["email"] not in ("", "nan") and order["first"] == order["date"]}
                    if acquired and ads > 0:
                        equal(metrics["cac"], ads / len(acquired), prefix + "cac")
                    else:
                        require(metrics["cac"] is None, prefix + "cac_na")
                    if daily_by_date is not None:
                        allocated = Decimal(0)
                        for day in scope:
                            company_orders = number(daily_by_date[day]["unique_orders"])
                            count = sum(order["date"] == day for order in selected)
                            if company_orders > 0:
                                allocated += number(daily_by_date[day]["fixed_daily_cost"]) * count / company_orders
                        inferred = number(metrics["profit"]) - number(card["secondary_metrics"]["company_margin_with_fixed"])
                        equal(inferred, allocated, prefix + "shared_fixed")
                source_checks += 1
    checks["source_country_windows_checked"] = source_checks
    checks["post_ad_profit_identities_with_positive_revenue"] = profit_identity_checks
    clean_sources = source_checks == 12 and len(errors) == source_errors_before
    checks["all_ad_sources_verified"] = clean_sources
    checks["customer_cac_from_financial_csv"] = clean_sources and orders is not None
    checks["allocated_fixed_from_daily_source"] = clean_sources and orders is not None and daily_by_date is not None
    return {
        "schema_version": 1, "read_only": True, "passed": not errors, "checks": checks,
        "errors": errors, "date_from": first, "date_to": last,
        "financial_csv_revenue_mode": revenue_mode,
        "creditnote_fulfillment_source_independently_verified": False,
        "financial_approval": False, "publication_approval": False,
        "source_scope": "Provided source files verified; this helper does not attest their execution or private-cloud provenance.",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--payload", type=Path, required=True)
    parser.add_argument("--meta-manifest", type=Path)
    parser.add_argument("--meta-directory", type=Path)
    parser.add_argument("--google-daily-source", type=Path)
    parser.add_argument("--financial-csv", type=Path)
    parser.add_argument("--global-daily", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    def read_rows(path):
        with path.open(encoding="utf-8-sig", newline="") as handle:
            return list(csv.DictReader(handle))
    paths = [args.payload, args.meta_manifest, args.google_daily_source, args.financial_csv, args.global_daily]
    inputs = {str(path): sha(path.read_bytes()) for path in paths if path is not None}
    result = verify_country_cfo_payload(
        load_payload(args.payload),
        meta_manifest=decode(args.meta_manifest.read_bytes()) if args.meta_manifest else None,
        meta_directory=args.meta_directory,
        google_daily_source=decode(args.google_daily_source.read_bytes()) if args.google_daily_source else None,
        financial_rows=read_rows(args.financial_csv) if args.financial_csv else None,
        global_daily=read_rows(args.global_daily) if args.global_daily else None,
    )
    result["inputs_sha256"] = inputs
    result["verification_script_sha256"] = sha(Path(__file__).read_bytes())
    with args.output.open("x", encoding="utf-8") as handle:
        json.dump(result, handle, indent=2, allow_nan=False)
        handle.write("\n")
    print(json.dumps({"passed": result["passed"], "output_sha256": sha(args.output.read_bytes()), "financial_approval": False}))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
