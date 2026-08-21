#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


EXPECTED_CREATE_RESOURCES = {
    "CollectorApi": "AWS::ApiGatewayV2::Api",
    "CollectorApiAccessLogs": "AWS::Logs::LogGroup",
    "CollectorApiIntegration": "AWS::ApiGatewayV2::Integration",
    "CollectorApiStage": "AWS::ApiGatewayV2::Stage",
    "CollectorCluster": "AWS::ECS::Cluster",
    "CollectorContainerLogs": "AWS::Logs::LogGroup",
    "CollectorExecutionRole": "AWS::IAM::Role",
    "CollectorHealthyHostAlarm": "AWS::CloudWatch::Alarm",
    "CollectorListener": "AWS::ElasticLoadBalancingV2::Listener",
    "CollectorLoadBalancer": "AWS::ElasticLoadBalancingV2::LoadBalancer",
    "CollectorLoadBalancerIngress": "AWS::EC2::SecurityGroupIngress",
    "CollectorLoadBalancerSecurityGroup": "AWS::EC2::SecurityGroup",
    "CollectorService": "AWS::ECS::Service",
    "CollectorServiceIngress": "AWS::EC2::SecurityGroupIngress",
    "CollectorServiceSecurityGroup": "AWS::EC2::SecurityGroup",
    "CollectorTarget5xxAlarm": "AWS::CloudWatch::Alarm",
    "CollectorTargetGroup": "AWS::ElasticLoadBalancingV2::TargetGroup",
    "CollectorTaskDefinition": "AWS::ECS::TaskDefinition",
    "CollectorTaskRole": "AWS::IAM::Role",
    "CollectorVpcLink": "AWS::ApiGatewayV2::VpcLink",
    "CollectorVpcLinkSecurityGroup": "AWS::EC2::SecurityGroup",
    "CuratedExperimentFactsTable": "AWS::Glue::Table",
    "CuratedPerformanceFactsTable": "AWS::Glue::Table",
    "ExperimentCatalogDatabase": "AWS::Glue::Database",
    "ExperimentDataBucket": "AWS::S3::Bucket",
    "ExperimentDataBucketPolicy": "AWS::S3::BucketPolicy",
    "GrowthBookAthenaWorkGroup": "AWS::Athena::WorkGroup",
    "GrowthBookReadOnlyPolicy": "AWS::IAM::ManagedPolicy",
    "RawExperimentEventsTable": "AWS::Glue::Table",
    "ReportingAthenaWorkGroup": "AWS::Athena::WorkGroup",
    "ReportingExperimentDataPolicy": "AWS::IAM::ManagedPolicy",
}
CANDIDATE_UPDATE_RESOURCES = {
    "CollectorService": "AWS::ECS::Service",
    "CollectorTaskDefinition": "AWS::ECS::TaskDefinition",
}
ACTIVATION_ALLOWED_LOGICAL_IDS = {"CollectorPostRoute"}
DEACTIVATION_ALLOWED_LOGICAL_IDS = {"CollectorPostRoute"}


def _load(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise AssertionError("change set must be a JSON object")
    return payload


def with_change_set_type(
    payload: dict[str, Any], expected_change_set_type: str
) -> dict[str, Any]:
    if expected_change_set_type not in {"CREATE", "UPDATE"}:
        raise AssertionError(f"unsupported expected change set type: {expected_change_set_type}")
    api_value = payload.get("ChangeSetType")
    if api_value is not None and str(api_value) != expected_change_set_type:
        raise AssertionError(
            f"change set type mismatch: API={api_value} expected={expected_change_set_type}"
        )
    normalized = dict(payload)
    normalized["ChangeSetType"] = expected_change_set_type
    return normalized


def validate(payload: dict[str, Any], phase: str) -> list[dict[str, str]]:
    status = str(payload.get("Status") or "")
    if status != "CREATE_COMPLETE":
        raise AssertionError(f"change set is not ready: {status or 'missing'}")
    raw_changes = payload.get("Changes")
    if not isinstance(raw_changes, list) or not raw_changes:
        raise AssertionError("change set contains no resource changes")

    change_set_type = str(payload.get("ChangeSetType") or "")
    if change_set_type not in {"CREATE", "UPDATE"}:
        raise AssertionError(f"change set type is missing or unsupported: {change_set_type or 'missing'}")

    normalized: list[dict[str, str]] = []
    for wrapper in raw_changes:
        if not isinstance(wrapper, dict) or not isinstance(wrapper.get("ResourceChange"), dict):
            raise AssertionError("change set contains a malformed resource change")
        change = wrapper["ResourceChange"]
        action = str(change.get("Action") or "")
        logical_id = str(change.get("LogicalResourceId") or "")
        replacement = str(change.get("Replacement") or "False")
        resource_type = str(change.get("ResourceType") or "")
        if not logical_id or not resource_type:
            raise AssertionError("change set resource identity is incomplete")
        if action not in {"Add", "Modify", "Remove"}:
            raise AssertionError(f"destructive change rejected: {action} {logical_id}")
        if action == "Remove" and phase != "deactivate":
            raise AssertionError(f"destructive change rejected: {action} {logical_id}")
        normalized.append(
            {
                "action": action,
                "logical_id": logical_id,
                "replacement": replacement,
                "resource_type": resource_type,
            }
        )

    if phase == "production-foundation":
        if change_set_type != "CREATE":
            raise AssertionError("Production foundation must be a CREATE change set")
        resources = {row["logical_id"]: row["resource_type"] for row in normalized}
        if len(resources) != len(normalized):
            raise AssertionError("Production foundation repeats a logical resource")
        if resources != EXPECTED_CREATE_RESOURCES:
            missing = sorted(EXPECTED_CREATE_RESOURCES.keys() - resources.keys())
            extra = sorted(resources.keys() - EXPECTED_CREATE_RESOURCES.keys())
            wrong_type = sorted(
                logical_id
                for logical_id in resources.keys() & EXPECTED_CREATE_RESOURCES.keys()
                if resources[logical_id] != EXPECTED_CREATE_RESOURCES[logical_id]
            )
            raise AssertionError(
                "Production foundation CREATE resource contract mismatch: "
                f"missing={missing} extra={extra} wrong_type={wrong_type}"
            )
        if any(row["action"] != "Add" or row["replacement"] != "False" for row in normalized):
            raise AssertionError(
                "Production foundation must contain only non-replacement Add changes"
            )
    elif phase == "activate":
        if change_set_type != "UPDATE":
            raise AssertionError("route activation must be an UPDATE change set")
        logical_ids = {row["logical_id"] for row in normalized}
        if not logical_ids.issubset(ACTIVATION_ALLOWED_LOGICAL_IDS):
            raise AssertionError(
                f"route activation contains unrelated changes: {sorted(logical_ids)}"
            )
        if normalized != [
            {
                "action": "Add",
                "logical_id": "CollectorPostRoute",
                "replacement": "False",
                "resource_type": "AWS::ApiGatewayV2::Route",
            }
        ]:
            raise AssertionError(f"unexpected route activation change set: {normalized}")
    elif phase == "deactivate":
        if change_set_type != "UPDATE":
            raise AssertionError("route deactivation must be an UPDATE change set")
        logical_ids = {row["logical_id"] for row in normalized}
        if not logical_ids.issubset(DEACTIVATION_ALLOWED_LOGICAL_IDS):
            raise AssertionError(
                f"route deactivation contains unrelated changes: {sorted(logical_ids)}"
            )
        if normalized != [
            {
                "action": "Remove",
                "logical_id": "CollectorPostRoute",
                "replacement": "False",
                "resource_type": "AWS::ApiGatewayV2::Route",
            }
        ]:
            raise AssertionError(f"unexpected route deactivation change set: {normalized}")
    elif phase == "candidate":
        resources = {row["logical_id"]: row["resource_type"] for row in normalized}
        if len(resources) != len(normalized):
            raise AssertionError("candidate change set repeats a logical resource")
        if change_set_type == "CREATE":
            if resources != EXPECTED_CREATE_RESOURCES:
                missing = sorted(EXPECTED_CREATE_RESOURCES.keys() - resources.keys())
                extra = sorted(resources.keys() - EXPECTED_CREATE_RESOURCES.keys())
                wrong_type = sorted(
                    logical_id
                    for logical_id in resources.keys() & EXPECTED_CREATE_RESOURCES.keys()
                    if resources[logical_id] != EXPECTED_CREATE_RESOURCES[logical_id]
                )
                raise AssertionError(
                    "candidate CREATE resource contract mismatch: "
                    f"missing={missing} extra={extra} wrong_type={wrong_type}"
                )
            if any(row["action"] != "Add" or row["replacement"] != "False" for row in normalized):
                raise AssertionError("candidate CREATE must contain only non-replacement Add changes")
        else:
            if not resources or not set(resources).issubset(CANDIDATE_UPDATE_RESOURCES):
                raise AssertionError(
                    f"candidate UPDATE contains unrelated changes: {sorted(resources)}"
                )
            for logical_id, resource_type in resources.items():
                if resource_type != CANDIDATE_UPDATE_RESOURCES[logical_id]:
                    raise AssertionError(f"candidate UPDATE resource type mismatch: {logical_id}")
            for row in normalized:
                if row["logical_id"] == "CollectorTaskDefinition":
                    if row["action"] != "Modify" or row["replacement"] != "True":
                        raise AssertionError("task definition update must be a reviewed replacement")
                elif row["action"] != "Modify" or row["replacement"] != "False":
                    raise AssertionError(f"unexpected candidate UPDATE action: {row}")
    else:
        raise AssertionError(f"unsupported validation phase: {phase}")

    return normalized


def main() -> int:
    parser = argparse.ArgumentParser(description="Fail closed on unsafe GrowthBook change sets")
    parser.add_argument("--change-set", type=Path, required=True)
    parser.add_argument("--change-set-type", choices=("CREATE", "UPDATE"), required=True)
    parser.add_argument(
        "--phase",
        choices=("candidate", "activate", "deactivate", "production-foundation"),
        required=True,
    )
    args = parser.parse_args()
    payload = with_change_set_type(_load(args.change_set), args.change_set_type)
    normalized = validate(payload, args.phase)
    print(json.dumps(normalized, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
