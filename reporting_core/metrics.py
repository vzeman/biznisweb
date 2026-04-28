#!/usr/bin/env python3
"""
Shared CloudWatch metric publishing for reporting and invoice runners.
"""

import os
from typing import Any, Dict


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
        cloudwatch = boto3.client("cloudwatch", region_name=region)
        cloudwatch.put_metric_data(
            Namespace=reporting_defaults["cloudwatch_namespace"],
            MetricData=[
                {
                    "MetricName": metric_name,
                    "Dimensions": [{"Name": "Project", "Value": project}],
                    "Value": float(value),
                    "Unit": unit,
                }
            ],
        )
    except (ClientError, BotoCoreError) as exc:
        print(f"WARN: failed to publish CloudWatch metric {metric_name}: {exc}")
