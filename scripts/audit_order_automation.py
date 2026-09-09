#!/usr/bin/env python3
"""Read-only, all-history invoice backlog audit; private output is git-ignored.

Use the existing runtime secret directly; never print credentials or customer
data. A partial/unstable scan is an error, never an empty or complete backlog.
This script cannot issue invoices, send mail or change order statuses.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import time

import boto3
import requests


QUERY = """
query AuditInvoiceBacklog($status: Int!, $params: OrderParams) {
  getOrderList(status: $status, include_blocking: true, params: $params) {
    data { order_num pur_date blocked status { id name }
           invoices { id } sum { value currency { code } } }
    pageInfo { hasNextPage nextCursor totalPages }
  }
}
"""


def scan(client, url, token, status_id, *, delay=1, max_pages=5000):
    cursor = None
    cursors = set()
    orders = {}
    pages = 0
    total_pages = None
    while pages < max_pages:
        params = {"limit": 30, "order_by": "pur_date", "sort": "DESC"}
        if cursor is not None:
            params["cursor"] = cursor
        payload = None
        for attempt in range(4):
            response = client.post(url, json={"query": QUERY, "variables": {
                "status": status_id, "params": params,
            }}, headers={"BW-API-Key": "Token " + token}, timeout=45)
            if response.status_code in (429, 502, 503, 504) and attempt < 3:
                time.sleep(2 ** (attempt + 1))
                continue
            response.raise_for_status()
            payload = json.loads(response.content)
            break
        if not isinstance(payload, dict) or payload.get("errors"):
            raise RuntimeError("Audit query failed; no complete result")
        block = (payload.get("data") or {}).get("getOrderList")
        if not isinstance(block, dict) or not isinstance(block.get("data"), list):
            raise RuntimeError("Audit page missing")
        page = block.get("pageInfo")
        if not isinstance(page, dict) or not isinstance(page.get("hasNextPage"), bool):
            raise RuntimeError("Audit pagination incomplete")
        if total_pages is not None and page.get("totalPages") != total_pages:
            raise RuntimeError("Collection changed during audit; rerun from the beginning")
        total_pages = page.get("totalPages")
        for row in block["data"]:
            if "invoices" in row and row["invoices"] is None:
                row["invoices"] = []
            number = str(row.get("order_num") or "")
            if not number or number in orders or not isinstance(row.get("invoices"), list):
                raise RuntimeError("Audit has missing or repeated order evidence")
            if str((row.get("status") or {}).get("id")) != str(status_id):
                raise RuntimeError("Audit status changed during scan")
            orders[number] = row
        pages += 1
        if pages % 10 == 0:
            print(json.dumps({"pages": pages, "orders": len(orders)}), flush=True)
        if not page["hasNextPage"]:
            return list(orders.values()), pages
        next_cursor = page.get("nextCursor")
        if not block["data"] or next_cursor in (None, "") or str(next_cursor) in cursors:
            raise RuntimeError("Audit cursor did not advance")
        cursors.add(str(next_cursor))
        cursor = next_cursor
        time.sleep(delay)
    raise RuntimeError("Audit reached page limit")


def summarize(rows, now):
    result = {"orders": len(rows), "with_invoice": 0, "blocked_without_invoice": 0,
              "zero_without_invoice": 0, "candidates": 0, "older_than_7_days": 0}
    candidates = []
    for row in rows:
        if row["invoices"]:
            result["with_invoice"] += 1
            continue
        if row.get("blocked") is not False:
            result["blocked_without_invoice"] += 1
            continue
        total = float((row.get("sum") or {})["value"])
        if total <= 0:
            result["zero_without_invoice"] += 1
            continue
        purchased = datetime.fromisoformat(row["pur_date"].replace("Z", "+00:00"))
        if purchased.tzinfo is None:
            from zoneinfo import ZoneInfo
            purchased = purchased.replace(tzinfo=ZoneInfo("Europe/Bratislava"))
        older = purchased < now - timedelta(days=7)
        result["candidates"] += 1
        result["older_than_7_days"] += int(older)
        candidates.append({**row, "older_than_7_days": older})
    return result, candidates


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", choices=("roy", "vevo"), required=True)
    parser.add_argument("--profile", default=None)
    parser.add_argument("--region", default="eu-central-1")
    args = parser.parse_args()
    aws_session = boto3.Session(profile_name=args.profile, region_name=args.region)
    secret = json.loads(aws_session.client("secretsmanager").get_secret_value(
        SecretId=f"{args.project}/reporting/runtime-env")["SecretString"])
    settings = json.loads((Path(__file__).resolve().parents[1] / "projects" / args.project / "settings.json").read_text(encoding="utf-8"))
    client = requests.Session()
    url, token = secret["BIZNISWEB_API_URL"], secret["BIZNISWEB_API_TOKEN"]
    response = client.post(url, json={"query": "query($lang:CountryCodeAlpha2!){listOrderStatuses(lang_code:$lang){id name}}",
        "variables": {"lang": "SK"}}, headers={"BW-API-Key": "Token " + token}, timeout=45)
    response.raise_for_status()
    payload = json.loads(response.content)
    if payload.get("errors"):
        raise RuntimeError("Status discovery failed")
    statuses = payload["data"]["listOrderStatuses"]
    eligible = settings["invoice_generation"]["eligible_statuses"]
    ids = [int(row["id"]) for row in statuses if row["name"] in eligible]
    if len(ids) != len(eligible):
        raise RuntimeError("Eligible status identity is ambiguous")
    all_rows, pages = [], 0
    for status_id in ids:
        rows, count = scan(client, url, token, status_id)
        all_rows.extend(rows)
        pages += count
    now = datetime.now(timezone.utc)
    summary, candidates = summarize(all_rows, now)
    result = {"project": args.project, "observed_at": now.isoformat(), "complete": True,
              "pages": pages, "summary": summary, "candidates": candidates}
    destination = Path(__file__).resolve().parents[1] / "data" / args.project / "order-automation" / "backlog-audit.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(result, ensure_ascii=True, indent=2), encoding="utf-8")
    print(json.dumps({"project": args.project, "complete": True, "pages": pages, **summary}), flush=True)


if __name__ == "__main__":
    main()
