#!/usr/bin/env python3
"""
Shared CloudWatch metric publishing for reporting and invoice runners.
"""

import os
from typing import Any, Dict


def automation_metric_defaults(reporting_defaults: Dict[str, Any], dry_run: bool) -> Dict[str, Any]:
    """Keep diagnostics out of the production automation health series."""
    return {**reporting_defaults, "metric_run_mode": "dry-run" if dry_run else "live"}


def put_metric(metric_name: str, value: float, project: str, reporting_defaults: Dict[str, Any], unit: str = "Count") -> None:
    """Publish a custom CloudWatch metric for job observability."""
    region = os.getenv("AWS_REGION", "eu-central-1").strip()
    try:
        import boto3  # type: ignore
        from botocore.exceptions import BotoCoreError, ClientError  # type: ignore
    except ImportError:
        print("WARN: boto3 not available, skipping CloudWatch metric publishing.")
        return

    try:
        dimensions = [{"Name": "Project", "Value": project}]
        run_mode = reporting_defaults.get("metric_run_mode")
        if run_mode is not None:
            if run_mode not in {"live", "dry-run"}:
                raise ValueError("Unsupported automation metric run mode")
            dimensions.append({"Name": "RunMode", "Value": run_mode})
        cloudwatch = boto3.client("cloudwatch", region_name=region)
        cloudwatch.put_metric_data(
            Namespace=reporting_defaults["cloudwatch_namespace"],
            MetricData=[
                {
                    "MetricName": metric_name,
                    "Dimensions": dimensions,
                    "Value": float(value),
                    "Unit": unit,
                }
            ],
        )
    except (ClientError, BotoCoreError) as exc:
        print(f"WARN: failed to publish CloudWatch metric {metric_name}: {exc}")
