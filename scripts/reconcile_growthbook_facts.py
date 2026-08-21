#!/usr/bin/env python3
"""Build PII-free GrowthBook facts from S3 events and BiznisWeb orders.

The command is dry-run by default.  Publishing requires both ``--publish`` and
``GROWTHBOOK_FACT_PUBLISH_ENABLED=true``.  It creates no AWS infrastructure,
changes no BiznisWeb record and logs no order/customer payload.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import boto3

import export_orders as reporting
from reporting_core.config import load_project_env, load_project_settings
from reporting_core.experiment_io import load_raw_experiment_events
from reporting_core.experiment_orders import build_biznisweb_authoritative_orders
from reporting_core.experiments import (
    ExperimentDataError,
    build_experiment_facts,
    load_experiment_build_config,
    order_completion_receipts,
    publish_experiment_facts,
)
from reporting_core.runtime import apply_project_runtime, load_project_runtime


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("expected YYYY-MM-DD") from exc


def _enabled(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes"}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reconcile anonymous experiment events with authoritative BiznisWeb order value."
    )
    parser.add_argument("--project", required=True, help="Project directory name, for example vevo")
    parser.add_argument("--event-from", required=True, type=_date, help="First raw receipt partition")
    parser.add_argument("--event-through", required=True, type=_date, help="Last raw receipt partition")
    parser.add_argument(
        "--bucket",
        default=os.getenv("GROWTHBOOK_EVENT_BUCKET", ""),
        help="Experiment-only S3 bucket; defaults to GROWTHBOOK_EVENT_BUCKET",
    )
    parser.add_argument(
        "--region",
        default=os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "eu-central-1",
        help="AWS region for the existing experiment bucket",
    )
    parser.add_argument(
        "--config",
        type=Path,
        help="Experiment reporting contract; defaults to projects/<project>/growthbook_reporting.json",
    )
    parser.add_argument(
        "--max-partitions",
        type=int,
        default=90,
        help="Fail closed when the inclusive raw partition window exceeds this bound",
    )
    parser.add_argument(
        "--max-raw-events",
        type=int,
        default=100_000,
        help="Fail closed before reconciliation when the raw object count exceeds this bound",
    )
    parser.add_argument(
        "--publish",
        action="store_true",
        help="Write curated facts; also requires GROWTHBOOK_FACT_PUBLISH_ENABLED=true",
    )
    return parser


def run(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    project = str(args.project or "").strip().lower()
    if not project or project not in {path.name.lower() for path in (REPO_ROOT / "projects").iterdir() if path.is_dir()}:
        raise ExperimentDataError("unknown reporting project")
    if not args.bucket:
        raise ExperimentDataError("GROWTHBOOK_EVENT_BUCKET is required")
    if type(args.max_partitions) is not int or not 1 <= args.max_partitions <= 90:
        raise ExperimentDataError("max partitions must be between 1 and 90")
    if type(args.max_raw_events) is not int or not 1 <= args.max_raw_events <= 100_000:
        raise ExperimentDataError("max raw events must be between 1 and 100000")
    partition_count = (args.event_through - args.event_from).days + 1
    if partition_count < 1 or partition_count > args.max_partitions:
        raise ExperimentDataError("event partition window exceeds the configured limit")
    if args.publish and not _enabled(os.getenv("GROWTHBOOK_FACT_PUBLISH_ENABLED")):
        raise ExperimentDataError(
            "publishing is disabled; set GROWTHBOOK_FACT_PUBLISH_ENABLED=true only in the reviewed runtime"
        )

    project_settings = load_project_settings(project)
    load_project_env(project)
    runtime = load_project_runtime(
        project,
        settings=project_settings,
        legacy_product_expenses=reporting.LEGACY_VEVO_PRODUCT_EXPENSES,
        default_currency_rates=reporting.CURRENCY_RATES_TO_EUR,
        default_packaging_cost_per_order=reporting.PACKAGING_COST_PER_ORDER,
        default_shipping_subsidy_per_order=reporting.SHIPPING_SUBSIDY_PER_ORDER,
        default_fixed_monthly_cost=reporting.FIXED_MONTHLY_COST,
        default_fixed_daily_cost=reporting.FIXED_DAILY_COST,
    )
    apply_project_runtime(runtime, reporting.__dict__)
    if not runtime.api_token:
        raise ExperimentDataError("BiznisWeb API credentials are unavailable for the selected project")

    config_path = args.config or REPO_ROOT / "projects" / project / "growthbook_reporting.json"
    config = load_experiment_build_config(config_path)
    s3 = boto3.client("s3", region_name=args.region)
    raw_events = load_raw_experiment_events(
        s3,
        bucket=args.bucket,
        start_date=args.event_from,
        end_date=args.event_through,
        max_window_days=args.max_partitions,
        max_objects=args.max_raw_events,
    )
    receipts = order_completion_receipts(raw_events)

    exporter = reporting.BizniWebExporter(
        runtime.api_url,
        runtime.api_token,
        project_name=project,
        output_tag="growthbook-reconcile",
        enable_period_bundle=False,
    )
    source_orders = []
    if receipts:
        receipt_dates = [value.date() for value in receipts.values()]
        # One-day guard on both sides handles UTC/local-midnight boundaries
        # without turning reconciliation into a broad order-history fetch.
        order_from = min(receipt_dates) - timedelta(days=1)
        order_through = max(receipt_dates) + timedelta(days=1)
        realized_orders = exporter.fetch_orders(
            datetime.combine(order_from, time.min),
            datetime.combine(order_through, time.min),
        )
        source_orders = [*realized_orders, *(exporter.excluded_status_orders or [])]

    generated_at = datetime.now(timezone.utc)
    authoritative_orders = build_biznisweb_authoritative_orders(
        exporter,
        source_orders,
        completion_receipts=receipts,
        generated_at=generated_at,
        maturity_checkpoint_days=config.maturity_checkpoint_days,
        packaging_cost_eur=reporting.PACKAGING_COST_PER_ORDER,
        shipping_net_cost_eur=reporting.SHIPPING_NET_PER_ORDER,
    )
    bundle = build_experiment_facts(
        raw_events,
        authoritative_orders,
        config=config,
        generated_at=generated_at,
    )

    summary = {
        "mode": "publish" if args.publish else "dry-run",
        "event_partitions": partition_count,
        "raw_events": len(raw_events),
        "completed_transaction_ids": len(receipts),
        "authoritative_orders": len(authoritative_orders),
        "device_facts": len(bundle.device_facts),
        "performance_facts": len(bundle.performance_facts),
        "quality_reports": len(bundle.quality_reports),
    }
    if args.publish:
        summary["published"] = publish_experiment_facts(
            s3,
            bucket=args.bucket,
            bundle=bundle,
        )
    print(json.dumps(summary, separators=(",", ":"), sort_keys=True))
    return 0


def main() -> None:
    try:
        raise SystemExit(run())
    except ExperimentDataError as exc:
        print(f"GrowthBook reconciliation stopped: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
