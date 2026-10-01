#!/usr/bin/env python3
"""Read-only ROY fulfillment evidence collector. Private output stays in data/roy.

No API mutation, print request, invoice creation, email or S3 write is performed.
Credentials are read directly into memory from the established runtime secret.
"""
from __future__ import annotations

import argparse
import copy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import time

import boto3
from graphql import print_ast
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from order_status_safety import ORDER_SAFETY_QUERY, assess_payment_evidence  # noqa: E402
from roy_operations_dashboard import (  # noqa: E402
    _is_fulfillable_order, resolve_roy_operations_settings,
)

BUCKET = "biznisweb-reporting-artifacts-919341186960-eu-central-1"
SERVICE = "arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2"
ORDER_FIELDS = """
id order_num pur_date last_change blocked status { id name }
price_elements { type title reference_id }
sum { value is_net_price currency { code } }
invoices { id invoice_num paid pay_date payments { id } }
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--order", action="append", required=True)
    parser.add_argument("--since", required=True, help="ISO date, inclusive")
    parser.add_argument("--max-pages", type=int, default=30)
    args = parser.parse_args()
    since = datetime.strptime(args.since, "%Y-%m-%d").date()
    if not 1 <= len(args.order) <= 20 or any(not re.fullmatch(r"[0-9]{1,20}", n) for n in args.order):
        parser.error("Supply 1..20 numeric order numbers")
    if not 1 <= args.max_pages <= 100:
        parser.error("max-pages must be 1..100")
    session = boto3.Session(profile_name=args.profile, region_name="eu-central-1")
    if session.client("sts").get_caller_identity()["Account"] != "919341186960":
        raise RuntimeError("Wrong AWS account")
    service = session.client("apprunner").describe_service(ServiceArn=SERVICE)["Service"]
    image = service["SourceConfiguration"]["ImageRepository"]
    result = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "service": {"arn": SERVICE, "status": service["Status"], "image": image["ImageIdentifier"],
                    "command": image["ImageConfiguration"].get("StartCommand"), "instance_ip": "App Runner managed"},
        "scope": {"orders": args.order, "since": args.since}, "sources": {},
    }
    s3 = session.client("s3")
    result["bucket_versioning"] = s3.get_bucket_versioning(Bucket=BUCKET).get("Status", "not_enabled")
    sources = {}
    for key in ("daily-reports/roy-sk/operations/state.json", "daily-reports/roy-sk/operations/live_snapshot.json",
                "data/roy/order-automation/state.json"):
        response = s3.get_object(Bucket=BUCKET, Key=key)
        body = response["Body"].read()
        sources[key] = json.loads(body)
        result["sources"][key] = {"etag": response["ETag"], "modified": response["LastModified"].isoformat(),
                                   "sha256": hashlib.sha256(body).hexdigest()}
    state = sources["daily-reports/roy-sk/operations/state.json"]
    result["print_acknowledgements"] = {
        n: {k: v for k, v in (state.get("printed_picking_orders", {}).get(n) or {}).items() if k != "customer"}
        for n in args.order
    }
    result["print_batches"] = [b for b in state.get("picking_print_batches", []) if set(args.order) & set(b.get("order_nums", []))]
    snapshot = sources["daily-reports/roy-sk/operations/live_snapshot.json"]
    result["snapshot_at"] = snapshot.get("generated_at")
    result["snapshot_orders"] = [
        {k: o.get(k) for k in ("id", "order_num", "status", "status_id", "payment", "fulfillment_reason",
                               "picking_printed", "picking_printed_at")}
        for o in (snapshot.get("orders") or {}).get("orders", []) if str(o.get("order_num")) in args.order
    ]
    journal = sources["data/roy/order-automation/state.json"]
    journal_fields = ("order_num", "order_id", "phase", "attempted_at", "updated_at", "invoice_num", "invoice_id",
                      "native_preinvoice_key", "preinvoice_id", "email_state", "status_observation_source",
                      "status_observed_at", "verified_fulfillment_status")
    result["invoice_journal"] = {
        n: {k: v for k, v in (journal.get("orders", {}).get(n) or {}).items() if k in journal_fields}
        for n in args.order
    }
    token = json.loads(session.client("secretsmanager").get_secret_value(
        SecretId="roy/reporting/runtime-env")["SecretString"])["BIZNISWEB_API_TOKEN"]
    http = requests.Session()
    http.headers.update({"BW-API-Key": "Token " + token})

    def query(document: str, variables: dict) -> dict:
        if not document.lstrip().startswith("query "):
            raise ValueError("Only explicit GraphQL queries permitted")
        response = http.post("https://roy.flox.sk/api/graphql", json={"query": document, "variables": variables}, timeout=45)
        response.raise_for_status()
        payload = response.json()
        if payload.get("errors") or not isinstance(payload.get("data"), dict):
            raise RuntimeError("Read failed; do not treat as complete evidence")
        time.sleep(0.6)
        return payload["data"]

    settings = resolve_roy_operations_settings(json.loads((ROOT / "projects/roy/settings.json").read_text(encoding="utf-8")))
    result["statuses"] = query("query Statuses($lang: CountryCodeAlpha2!) { listOrderStatuses(lang_code: $lang, only_active: true) { id name } }", {"lang": "SK"})["listOrderStatuses"]
    status_by_id = {str(row["id"]): row for row in result["statuses"]}
    result["orders"] = []
    document = print_ast(getattr(ORDER_SAFETY_QUERY, "document", ORDER_SAFETY_QUERY))
    for number in args.order:
        order = query(document, {"order_num": number})["getOrder"]
        if not order or order["order_num"] != number:
            raise RuntimeError("Order identity mismatch")
        assessment = assess_payment_evidence(order)
        variants = {}
        for status_id in ("1", "67", "4", "17"):
            variant = copy.deepcopy(order)
            variant["status"] = status_by_id[status_id]
            variants[status_id] = _is_fulfillable_order(variant, settings)
        result["orders"].append({"order": order, "payment_assessment": vars(assessment),
                                 "current_eligibility": _is_fulfillable_order(order, settings),
                                 "reconstructed_status_variants_not_historical_snapshots": variants})
    rows, seen, cursors, cursor = [], set(), set(), None
    scan_query = "query AuditWindow($params: OrderParams) { getOrderList(include_blocking: true, params: $params) { data { " + ORDER_FIELDS + " } pageInfo { hasNextPage nextCursor } } }"
    for page_number in range(1, args.max_pages + 1):
        params = {"limit": 30, "order_by": "order_id", "sort": "DESC"}
        if cursor is not None:
            params["cursor"] = cursor
        block = query(scan_query, {"params": params})["getOrderList"]
        batch = block["data"]
        if not isinstance(batch, list):
            raise RuntimeError("Missing scan rows")
        for row in batch:
            key = str(row["id"])
            if key in seen:
                raise RuntimeError("Concurrent pagination shift; incomplete audit")
            if rows and int(key) >= int(rows[-1]["id"]):
                raise RuntimeError("Order inventory is not descending by ID")
            seen.add(key)
            rows.append(row)
        page = block["pageInfo"]
        # This is a bounded recent-ID window, not an all-history completeness claim.
        if not page["hasNextPage"] or (batch and all(datetime.fromisoformat(r["pur_date"]).date() < since for r in batch)):
            break
        cursor = page["nextCursor"]
        if cursor is None or str(cursor) in cursors:
            raise RuntimeError("Pagination did not advance")
        cursors.add(str(cursor))
        time.sleep(1.4)
    else:
        raise RuntimeError("Page limit reached; incomplete audit")
    selected = [r for r in rows if datetime.fromisoformat(r["pur_date"]).date() >= since]
    for row in selected:
        row["dashboard_eligibility"] = _is_fulfillable_order(row, settings)
    result["recent_window"] = {"pages": page_number, "read_orders": len(rows), "selected_orders": len(selected),
                               "limitation": "Recent descending-ID window; no all-history completeness claim", "orders": selected}
    output_dir = ROOT / "data/roy/order-automation/audits" / datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_dir.mkdir(parents=True, exist_ok=True)
    output = output_dir / ("fulfillment-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + ".json")
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(output), "sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
                      "read_orders": len(rows), "selected_orders": len(selected)}))


if __name__ == "__main__":
    main()
