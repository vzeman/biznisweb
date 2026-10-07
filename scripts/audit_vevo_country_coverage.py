"""Read-only VEVO country/status coverage audit; no order or service mutations.

Credentials are fetched into memory from Secrets Manager. Only minimal order
facts are saved under ignored data/, and public evidence contains aggregates.
The classification mirrors current reporting settings and reviewed status IDs.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_EVEN
import hashlib
import json
from pathlib import Path
import re
import sys
import time
from types import SimpleNamespace
import unicodedata

import boto3
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from order_status_identity import bind_catalogue, bind_status_identity, canonical_order  # noqa: E402

ENDPOINT = "https://vevo.flox.sk/api/graphql"
SECRET_ARN = "arn:aws:secretsmanager:eu-central-1:919341186960:secret:vevo/reporting/runtime-env-ygoPma"
PAYMENT_FIELDS = "price_elements { type title reference_id }"
ORDER_FIELDS = """
order_num pur_date status { id name } invoice_address { country }
sum { value currency { code } }
items { quantity tax_rate price { value currency { code } }
sum { value currency { code } } sum_with_tax { value currency { code } } }
"""
CATALOGUE_QUERY = """query CountryAuditStatuses($lang_code: CountryCodeAlpha2!) {
listOrderStatuses(lang_code: $lang_code, only_active: true) { id name } }"""
PAYMENT_QUERY = """query CountryAuditPayment($order_num: String!) {
getOrder(order_num: $order_num) { order_num price_elements { type title reference_id } } }"""


class AuditError(Exception):
    """Fixed, credential-free diagnostics only."""


def normalized(value):
    text = "".join(c for c in unicodedata.normalize("NFKD", str(value or "")) if not unicodedata.combining(c)).lower()
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def dec(value):
    result = Decimal(str(value or 0))
    if not result.is_finite():
        raise AuditError("non_finite_money")
    return result


def money(value):
    return float(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN))


def payment_info(order):
    return next((row for row in order.get("price_elements") or []
                 if normalized(row.get("type")) == "payment"), {})


def decision(order, settings):
    """Mirror export_orders._realized_revenue_decision on canonical status."""
    if order.get("status_identity_unbound"):
        return False, "unbound_status"
    status = normalized((order.get("status") or {}).get("name"))
    if status in {normalized(x) for x in settings["paid_statuses"]}:
        return True, "paid_status"
    prepaid_statuses = {normalized(x) for x in settings["prepaid_fulfilled_statuses"]}
    cod_statuses = {normalized(x) for x in settings["cod_statuses"]}
    if not isinstance(order.get("price_elements"), list):
        if status in prepaid_statuses | cod_statuses:
            raise AuditError("payment_metadata_incomplete")
    payment = payment_info(order)
    reference = str(payment.get("reference_id") or "")
    title = normalized(payment.get("title"))
    prepaid = reference in settings["prepaid_payment_ids"] or any(
        normalized(x) in title for x in settings["prepaid_payment_patterns"])
    cod = reference in settings["cod_payment_ids"] or any(
        normalized(x) in title for x in settings["cod_payment_patterns"])
    if status in prepaid_statuses and prepaid:
        return True, "prepaid_fulfilled_status"
    if status in cod_statuses:
        return (True, "cod_status_and_payment") if cod else (False, "cod_status_without_cod_payment")
    return False, "missing_status" if not status else "non_realized_status"


def item_net_eur(order, rates):
    """Use explicit net item sums, as reporting does, then VAT fallback."""
    total = Decimal(0)
    order_currency = ((order.get("sum") or {}).get("currency") or {}).get("code") or "EUR"
    for item in order.get("items") or []:
        net, gross, price = item.get("sum") or {}, item.get("sum_with_tax") or {}, item.get("price") or {}
        currency = next((x.get("currency", {}).get("code") for x in (net, gross, price)
                         if (x.get("currency") or {}).get("code")), order_currency)
        if currency not in rates:
            raise AuditError("unknown_currency")
        if net.get("value") is not None:
            value = dec(net["value"])
        elif gross.get("value") is not None:
            value = dec(gross["value"]) / (1 + dec(item.get("tax_rate")) / 100)
        else:
            value = dec(price.get("value")) * dec(item.get("quantity"))
        total += (value * dec(rates[currency])).quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN)
    return total


class Reader:
    def __init__(self, token):
        self.session = requests.Session()
        self.session.headers.update({"BW-API-Key": "Token " + token, "Content-Type": "application/json"})
        self.last_request = 0.0
        self.request_count = 0
        self.endpoint = ENDPOINT

    def query(self, query, variables):
        if not query.lstrip().startswith("query ") or "mutation" in query.lower():
            raise AuditError("read_only_query_gate")
        for attempt in range(4):
            time.sleep(max(0, 0.6 - (time.monotonic() - self.last_request)))
            self.last_request = time.monotonic()
            self.request_count += 1
            if self.request_count % 100 == 0:
                time.sleep(5)
            response = self.session.post(self.endpoint, json={"query": query, "variables": variables}, timeout=(10, 45), allow_redirects=False)
            if response.status_code in {307, 308}:
                target = response.headers.get("Location")
                if self.endpoint != ENDPOINT or target != "https://www.vevo.sk/api/graphql":
                    raise AuditError("unexpected_redirect")
                self.endpoint = target
                continue
            if response.status_code in {429, 500, 502, 503, 504} and attempt < 3:
                time.sleep(2 ** (attempt + 1))
                continue
            if response.status_code != 200:
                raise AuditError("graphql_http_" + str(response.status_code))
            try:
                payload = response.json()
            except ValueError:
                raise AuditError("graphql_non_json") from None
            if payload.get("errors"):
                error_text = str(payload["errors"]).lower()
                is_rate = any(term in error_text for term in ("rate limit", "too many", "429"))
                if is_rate and attempt < 3:
                    time.sleep(10 * (attempt + 1))
                    continue
                if "price_elements" in error_text:
                    raise AuditError("price_elements_unavailable")
                kind = "rate_limit" if is_rate else "internal" if "internal" in error_text else "other"
                raise AuditError("graphql_error_" + kind)
            if not isinstance(payload.get("data"), dict):
                raise AuditError("graphql_data_missing")
            return payload["data"]
        raise AuditError("graphql_retries_exhausted")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-date", default="2026-08-01")
    parser.add_argument("--to-date", default="2026-10-06")
    parser.add_argument("--profile", default="codex")
    parser.add_argument("--resume", action="store_true", help="Resume same-date, same-settings private checkpoint")
    args = parser.parse_args()
    start, end = date.fromisoformat(args.from_date), date.fromisoformat(args.to_date)
    if start > end or (end - start).days > 100:
        raise AuditError("date_bounds_invalid")
    settings_path = ROOT / "projects/vevo/settings.json"
    settings_bytes = settings_path.read_bytes()
    settings = json.loads(settings_bytes)
    secret = boto3.Session(profile_name=args.profile).client("secretsmanager", region_name="eu-central-1").get_secret_value(SecretId=SECRET_ARN)
    runtime = json.loads(secret["SecretString"])
    url = runtime.get("BIZNISWEB_API_URL") or runtime.get("VEVO_BIZNISWEB_API_URL") or settings["biznisweb_api_url"]
    token = runtime.get("BIZNISWEB_API_TOKEN") or runtime.get("VEVO_BIZNISWEB_API_TOKEN")
    if url != ENDPOINT or settings["biznisweb_api_url"] != ENDPOINT or not isinstance(token, str) or not token:
        raise AuditError("vevo_credential_target_invalid")
    reader = Reader(token)
    client = SimpleNamespace(transport=SimpleNamespace(url=ENDPOINT))
    bind_status_identity(client, "vevo", settings)
    catalogue = reader.query(CATALOGUE_QUERY, {"lang_code": "SK"})["listOrderStatuses"]
    bind_catalogue(client, catalogue)
    private_dir = ROOT / "data/country-analysis-20261007"
    private_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_path = private_dir / "coverage_checkpoint.json"
    settings_hash = hashlib.sha256(settings_bytes).hexdigest()
    cursor, seen_cursors, seen_orders, facts = None, set(), set(), []
    reached_boundary = False
    oldest_seen = None
    fallback_pages = 0
    previous_date = None
    if args.resume:
        saved = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        if (saved["from_date"], saved["to_date"], saved["settings_sha256"]) != (str(start), str(end), settings_hash):
            raise AuditError("checkpoint_scope_mismatch")
        facts, cursor = saved["facts"], saved["next_cursor"]
        seen_orders = {row["order_num"] for row in facts}
        seen_cursors = set(saved["seen_cursors"])
        oldest_seen = date.fromisoformat(saved["oldest_seen"])
        previous_date = oldest_seen
    for page_index in range(300):
        params = {"limit": 30, "order_by": "pur_date", "sort": "DESC"}
        if cursor is not None:
            params["cursor"] = cursor
        query = "query CountryAuditOrders($params: OrderParams) { getOrderList(params: $params) { data { " + ORDER_FIELDS + PAYMENT_FIELDS + " } pageInfo { hasNextPage nextCursor } } }"
        try:
            block = reader.query(query, {"params": params})["getOrderList"]
        except AuditError as exc:
            if str(exc) not in {"price_elements_unavailable", "graphql_error_internal"}:
                raise
            fallback_pages += 1
            block = reader.query(query.replace(PAYMENT_FIELDS, ""), {"params": params})["getOrderList"]
        orders = block.get("data") or []
        if not orders and block.get("pageInfo", {}).get("hasNextPage"):
            raise AuditError("empty_nonterminal_page")
        page_dates = []
        for order in orders:
            order_date = date.fromisoformat(str(order.get("pur_date", ""))[:10])
            if previous_date is not None and order_date > previous_date:
                raise AuditError("descending_date_order_violated")
            previous_date = order_date
            page_dates.append(order_date)
            oldest_seen = min(oldest_seen, order_date) if oldest_seen else order_date
            if not start <= order_date <= end:
                continue
            order_num = str(order.get("order_num") or "")
            if not order_num or order_num in seen_orders:
                raise AuditError("duplicate_or_missing_order_key")
            seen_orders.add(order_num)
            if not isinstance(order.get("price_elements"), list):
                payment_order = reader.query(PAYMENT_QUERY, {"order_num": order_num}).get("getOrder") or {}
                if str(payment_order.get("order_num")) != order_num:
                    raise AuditError("payment_join_mismatch")
                order["price_elements"] = payment_order.get("price_elements")
                if not isinstance(order["price_elements"], list):
                    raise AuditError("payment_metadata_incomplete")
            canonical = canonical_order(client, order, inventory=True)
            included, reason = decision(canonical, settings["realized_revenue"])
            payment = payment_info(order)
            country = str((order.get("invoice_address") or {}).get("country") or "unknown").upper()
            facts.append({"order_num": order_num, "date": order_date.isoformat(), "month": order_date.strftime("%Y-%m"),
                          "country": country, "currency": ((order.get("sum") or {}).get("currency") or {}).get("code"),
                          "status_id": str((order.get("status") or {}).get("id")), "status_name": (order.get("status") or {}).get("name"),
                          "canonical_status": (canonical.get("status") or {}).get("name"),
                          "payment_id": str(payment.get("reference_id") or ""), "payment_title": payment.get("title") or "",
                          "included": included, "reason": reason, "item_net_eur": money(item_net_eur(order, settings["currency_rates_to_eur"]))})
        print(json.dumps({"page": page_index + 1, "orders_in_window": len(facts), "oldest_date": oldest_seen.isoformat() if oldest_seen else None}), flush=True)
        page_info = block.get("pageInfo") or {}
        if (page_dates and min(page_dates) < start) or not page_info.get("hasNextPage"):
            reached_boundary = True
            break
        cursor = page_info.get("nextCursor")
        if cursor is None or str(cursor) in seen_cursors:
            raise AuditError("pagination_cursor_invalid")
        seen_cursors.add(str(cursor))
        checkpoint_path.write_text(json.dumps({"from_date": str(start), "to_date": str(end),
            "settings_sha256": settings_hash, "next_cursor": cursor, "seen_cursors": sorted(seen_cursors),
            "oldest_seen": str(oldest_seen), "facts": facts}, ensure_ascii=False), encoding="utf-8")
    if not reached_boundary:
        raise AuditError("pagination_bound_exhausted")
    groups = defaultdict(lambda: {"orders": 0, "item_net_eur": Decimal(0)})
    dimensions = ("month", "country", "currency", "status_id", "status_name", "payment_id", "payment_title", "included", "reason")
    for row in facts:
        target = groups[tuple(row[key] for key in dimensions)]
        target["orders"] += 1
        target["item_net_eur"] += dec(row["item_net_eur"])
    summary = {"generated_at": datetime.now(timezone.utc).isoformat(), "endpoint": reader.endpoint,
               "from_date": start.isoformat(), "to_date": end.isoformat(), "complete_boundary": reached_boundary,
               "oldest_seen": oldest_seen.isoformat() if oldest_seen else None, "requests_completion_pass": reader.request_count,
               "resumed_from_checkpoint": args.resume,
               "fallback_pages": fallback_pages, "order_count": len(facts), "settings_sha256": hashlib.sha256(settings_bytes).hexdigest(),
               "country_basis": "invoice country (minimal projection of existing reporting API query)",
               "money_basis": "net merchandise EUR using current reporting static currency rates; Decimal half-even cents per line; not collected cash",
               "rounding_caveat": "Reporting uses binary-float round per line; Decimal half-even may differ by a cent at rounding ties. Use archived export for included revenue comparisons.",
               "rows": [{**dict(zip(dimensions, key)), "orders": val["orders"], "item_net_eur": money(val["item_net_eur"])}
                        for key, val in sorted(groups.items(), key=lambda item: str(item[0]))]}
    (private_dir / "biznisweb_order_facts.json").write_text(json.dumps(facts, ensure_ascii=False, indent=2), encoding="utf-8")
    docs = private_dir / "aggregates"
    docs.mkdir(exist_ok=True)
    (docs / "vevo_country_order_coverage_20261007.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"audit": "complete", "orders": len(facts), "requests": reader.request_count}), flush=True)


if __name__ == "__main__":
    try:
        main()
    except AuditError as exc:
        print("VEVO_COUNTRY_AUDIT_FAILED:" + str(exc), file=sys.stderr)
        raise SystemExit(2) from None
    except Exception:
        print("VEVO_COUNTRY_AUDIT_FAILED:unexpected_sanitized", file=sys.stderr)
        raise SystemExit(2) from None
