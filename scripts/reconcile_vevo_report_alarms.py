"""Bind the two existing VEVO report alarms to the actual metric namespace.

Read-only unless --apply. Preserve thresholds, recipients and every other field.
Run after the compatible report release, with its exact current definition ARN.
"""
from __future__ import annotations

import argparse
from contextlib import closing
import copy
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from reporting_core.config import resolve_reporting_defaults
from scripts.record_verified_manual_settlement import ACCOUNT, REGION, require_source

EXPECTED = {"vevo-reporting-run-failed": "ReportRunFailed",
            "vevo-reporting-missing-email-heartbeat": "ReportEmailSent"}


def corrected_config(alarm, fields, namespace):
    name = alarm.get("AlarmName")
    if (name not in EXPECTED or alarm.get("MetricName") != EXPECTED[name]
            or alarm.get("Dimensions") != [{"Name": "Project", "Value": "vevo"}]
            or alarm.get("Namespace") not in {"VevoReporting", namespace}
            or alarm.get("ActionsEnabled") is not True or not alarm.get("AlarmActions")
            or alarm.get("Metrics") or alarm.get("TreatMissingData") not in {"breaching", "notBreaching"}):
        raise ValueError("report_alarm_identity_or_configuration_changed")
    before = {key: copy.deepcopy(value) for key, value in alarm.items() if key in fields}
    after = {**copy.deepcopy(before), "Namespace": namespace}
    return before, after


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default="codex")
    parser.add_argument("--expected-report-definition", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    source = require_source(root)
    prefix = f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/vevo-reporting-daily:"
    if not args.expected_report_definition.startswith(prefix) or not args.expected_report_definition[len(prefix):].isdigit():
        raise ValueError("invalid_report_definition")
    settings = json.loads((root / "projects/vevo/settings.json").read_text(encoding="utf-8"))
    namespace = resolve_reporting_defaults("vevo", settings)["cloudwatch_namespace"]
    import boto3
    from botocore.config import Config
    session = boto3.Session(profile_name=args.profile, region_name=REGION)
    config = Config(connect_timeout=10, read_timeout=30, retries={"total_max_attempts": 1})
    with closing(session.client("sts", config=config)) as sts:
        if sts.get_caller_identity()["Account"] != ACCOUNT:
            raise ValueError("report_alarm_account_changed")
    results = []
    with closing(session.client("scheduler", config=config)) as scheduler, closing(session.client("cloudwatch", config=config)) as cw:
        fields = set(cw.meta.service_model.operation_model("PutMetricAlarm").input_shape.members)
        for name in EXPECTED:
            schedule = scheduler.get_schedule(Name="vevo-daily-report-email")
            if (schedule["State"] != "ENABLED" or schedule["Target"]["EcsParameters"]["TaskDefinitionArn"]
                    != args.expected_report_definition):
                raise ValueError("report_schedule_not_at_verified_release")
            alarms = cw.describe_alarms(AlarmNames=[name])["MetricAlarms"]
            if len(alarms) != 1:
                raise ValueError("report_alarm_missing")
            before, after = corrected_config(alarms[0], fields, namespace)
            changed = before != after
            if args.apply and changed:
                fresh = cw.describe_alarms(AlarmNames=[name])["MetricAlarms"]
                if len(fresh) != 1 or corrected_config(fresh[0], fields, namespace)[0] != before:
                    raise ValueError("report_alarm_concurrent_change")
                cw.put_metric_alarm(**after)
                readback = cw.describe_alarms(AlarmNames=[name])["MetricAlarms"]
                if len(readback) != 1 or corrected_config(readback[0], fields, namespace)[0] != after:
                    raise ValueError("report_alarm_readback_mismatch")
            results.append({"name": name, "before": before, "after": after, "changed": changed})
    print(json.dumps({"ok": True, "applied": args.apply, "source": source, "alarms": results}, sort_keys=True))


if __name__ == "__main__":
    main()
