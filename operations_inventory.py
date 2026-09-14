"""Inventory for shops whose ordinary report does not publish operations data.

Read the realized-sales export from the exact current report generation and reuse
the existing demand model with live catalogue stock. No report, email, order or
catalogue writes; the ordinary scheduled report remains independently deployed.
"""
from __future__ import annotations

import copy
import hashlib
import io
import json
import os
import re
import threading
import time
from contextlib import contextmanager
from datetime import date

_LOCK = threading.Lock()
_MODEL_LOCK = threading.Lock()
_CACHE: dict[str, tuple[str, float, dict]] = {}
_CATALOGUE_CACHE: dict[str, tuple[float, object]] = {}
MAX_EXPORT_BYTES = 64 * 1024 * 1024
MAX_PAYLOAD_BYTES = 12 * 1024 * 1024
CACHE_SECONDS = 900


def _read(s3, bucket: str, key: str, maximum: int) -> bytes:
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    try:
        if response["ContentLength"] > maximum:
            raise ValueError("Operations source exceeds size limit")
        data = body.read(maximum + 1)
    finally:
        body.close()
    if len(data) > maximum:
        raise ValueError("Operations source exceeds size limit")
    return data


def read_inventory_export(s3, project: str, settings: dict, report_payload: dict) -> tuple[bytes, dict]:
    """No prefix listing, guessed latest file or cross-generation mixing."""
    artifacts = settings["live_dashboard_artifacts"]
    bucket, prefix = artifacts["s3_bucket"], artifacts["s3_prefix"].strip("/")
    if project != "vevo" or prefix != "daily-reports/vevo" or report_payload.get("project") != project:
        raise ValueError("Operations inventory source project mismatch")
    manifest_key = f"{prefix}/latest/generation.json"
    manifest_bytes = _read(s3, bucket, manifest_key, 65536)
    manifest = json.loads(manifest_bytes)
    generation = manifest.get("generation_id", "")
    if (manifest.get("project") != project or manifest.get("schema_version") != 1
            or not re.fullmatch(r"\d{8}T\d{6}Z", generation)):
        raise ValueError("Invalid inventory source generation")
    item = manifest["artifacts"]["dashboard_payload_latest.json"]
    if item["key"] != f"{prefix}/{generation}/dashboard_payload_latest.json":
        raise ValueError("Inventory source escaped its generation")
    payload_bytes = _read(s3, bucket, item["key"], MAX_PAYLOAD_BYTES)
    if len(payload_bytes) != item["size"] or hashlib.sha256(payload_bytes).hexdigest() != item["sha256"]:
        raise ValueError("Inventory report integrity mismatch")
    payload = json.loads(payload_bytes)
    if any(payload.get(key) != report_payload.get(key) for key in ("project", "generated_at", "date_from", "date_to")):
        raise ValueError("Report changed during inventory refresh; retry with its new generation")
    health = payload.get("source_health") or {}
    if health.get("is_partial") or health.get("qa_failure_count", 0):
        raise ValueError("Incomplete report cannot supply inventory recommendations")
    start, end = date.fromisoformat(payload["date_from"]), date.fromisoformat(payload["date_to"])
    if start > end:
        raise ValueError("Invalid inventory report dates")
    key = f"{prefix}/{generation}/export_{start:%Y%m%d}-{end:%Y%m%d}.csv"
    export = _read(s3, bucket, key, MAX_EXPORT_BYTES)
    if _read(s3, bucket, manifest_key, 65536) != manifest_bytes:
        raise ValueError("Report generation changed during inventory read")
    return export, {"generation": generation, "export_key": key,
                    "export_sha256": hashlib.sha256(export).hexdigest(),
                    "date_from": start.isoformat(), "date_to": end.isoformat()}


@contextmanager
def configured_model(project: str, settings: dict):
    """The legacy model uses module constants; isolate and restore its runtime."""
    import export_orders as model
    from reporting_core import apply_project_runtime, load_project_runtime
    from roy_operations_dashboard import assert_operations_project

    assert_operations_project(project)
    with _MODEL_LOCK:
        runtime = load_project_runtime(
            project, settings=settings, legacy_product_expenses=model.LEGACY_VEVO_PRODUCT_EXPENSES,
            default_currency_rates=model.CURRENCY_RATES_TO_EUR,
            default_packaging_cost_per_order=model.PACKAGING_COST_PER_ORDER,
            default_shipping_subsidy_per_order=model.SHIPPING_SUBSIDY_PER_ORDER,
            default_fixed_monthly_cost=model.FIXED_MONTHLY_COST,
            default_fixed_daily_cost=model.FIXED_DAILY_COST,
        )
        updates: dict = {}
        apply_project_runtime(runtime, updates)
        missing = object()
        previous = {key: getattr(model, key, missing) for key in updates}
        vars(model).update(updates)
        try:
            yield model
        finally:
            for key, value in previous.items():
                if value is missing:
                    vars(model).pop(key, None)
                else:
                    setattr(model, key, value)


def build_inventory_analytics(project: str, settings: dict, export: bytes) -> dict:
    import pandas as pd
    from roy_operations_dashboard import _build_client

    frame = pd.read_csv(io.BytesIO(export), low_memory=False,
                        dtype={"product_sku": str, "order_num": str, "item_import_code": str})
    required = {"order_num", "purchase_date", "product_sku", "item_quantity", "realized_revenue"}
    if not required.issubset(frame.columns) or frame.empty:
        raise ValueError("Inventory sales export is empty or incompatible")
    realized = frame["realized_revenue"].astype(str).str.lower().isin({"true", "1", "1.0"})
    if not realized.all():
        raise ValueError("Inventory export contains non-realized orders")
    # Facts-only initialization has no ad clients, environment changes or file
    # creation. Its sole attached client is the current shop's read-only API.
    with configured_model(project, settings) as model:
        exporter = model.BizniWebExporter(settings["biznisweb_api_url"], "", project_name=project,
                                         order_facts_only=True)
        exporter.client = _build_client(project, settings)
        result = exporter.analyze_roy_product_demand_analytics(frame, operations_inventory=True)
        catalogue = exporter._product_inventory_snapshot_cache.get("SK")
        if catalogue is not None:
            _CATALOGUE_CACHE[project] = (time.monotonic(), catalogue.copy(deep=True))
    if result["summary"].get("inventory_status") == "error":
        raise RuntimeError("VEVO inventory source is unavailable; no stock totals published")
    # JSON roundtrip removes pandas/numpy types and non-finite optional values.
    serialized = {key: json.loads(value.to_json(orient="records", date_format="iso"))
                  if isinstance(value, pd.DataFrame) else value for key, value in result.items()}
    return json.loads(json.dumps(serialized, default=lambda value: value.item()))


def fetch_catalogue_stock(project: str, settings: dict, targets: list[dict]) -> tuple[dict, dict]:
    """Small complete catalogue, exact reporting SKU; no fuzzy product matching."""
    from roy_operations_dashboard import _build_client, _state_now_iso
    with configured_model(project, settings) as model:
        cached = _CATALOGUE_CACHE.get(project)
        if cached and time.monotonic() - cached[0] < 45:
            frame = cached[1]
        else:
            exporter = model.BizniWebExporter(settings["biznisweb_api_url"], "", project_name=project, order_facts_only=True)
            exporter.client = _build_client(project, settings)
            frame = exporter.fetch_product_inventory_snapshot("SK")
            _CATALOGUE_CACHE[project] = (time.monotonic(), frame.copy(deep=True))
        wanted = {row["sku"] for row in targets}
        stocks = {}
        for sku, rows in frame.groupby("reporting_sku"):
            if sku not in wanted:
                continue
            raw = float(rows["available_quantity_raw"].sum())
            quantity = float(rows["quantity_raw"].sum())
            stocks[sku] = {"sku": sku, "available_quantity_raw": raw,
                           "quantity_raw": quantity, "available_quantity": max(0, raw),
                           "quantity": max(0, quantity), "active": bool(rows["active"].any()),
                           "matched_product_id": ",".join(sorted(set(rows["product_id"].astype(str))))}
    return stocks, {"enabled": True, "source": "biznisweb_catalogue_exact_reporting_sku",
                    "target_count": len(wanted), "matched_count": len(stocks),
                    "unmatched_count": len(wanted) - len(stocks), "error_count": 0,
                    "checked_at": _state_now_iso()}


def enrich_operations_inventory(project: str, payload: dict, settings: dict) -> dict:
    from roy_operations_dashboard import assert_operations_project
    assert_operations_project(project)
    if project != "vevo" or payload.get("project") != project:
        raise ValueError("Operations inventory is not configured for this project")
    fingerprint = json.dumps({key: payload.get(key) for key in ("project", "generated_at", "date_from", "date_to")}, sort_keys=True)
    with _LOCK:
        cached = _CACHE.get(project)
        if cached and cached[0] == fingerprint and time.monotonic() - cached[1] < CACHE_SECONDS:
            inventory = cached[2]
        else:
            import boto3
            s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "eu-central-1"))
            export, source = read_inventory_export(s3, project, settings, payload)
            inventory = build_inventory_analytics(project, settings, export)
            inventory["source"] = source
            _CACHE[project] = (fingerprint, time.monotonic(), inventory)
    result = copy.deepcopy(payload)
    result.setdefault("dashboard", {})["roy_product_demand"] = copy.deepcopy(inventory)
    result["dashboard"]["roy_operations_inventory"] = copy.deepcopy(inventory)
    return result
