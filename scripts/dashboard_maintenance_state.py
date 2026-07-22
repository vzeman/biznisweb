#!/usr/bin/env python3
"""CAS-safe controller for the live-dashboard maintenance lease in S3."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from live_dashboard_maintenance import (  # noqa: E402
    DEFAULT_LEASE_SECONDS,
    DEFAULT_MAINTENANCE_MESSAGE,
    DEFAULT_MAX_LIFETIME_SECONDS,
    MaintenanceConflict,
    build_active_maintenance_state,
    build_inactive_maintenance_state,
    normalize_maintenance_state,
    public_maintenance_status,
)


MAX_CAS_ATTEMPTS = 5
AWS_CLI_TIMEOUT_SECONDS = 45
_NOT_FOUND_MARKERS = ("nosuchkey", "not found", "status code: 404")
_CONFLICT_MARKERS = (
    "preconditionfailed",
    "conditionalrequestconflict",
    "status code: 409",
    "status code: 412",
)


class AwsCliError(RuntimeError):
    def __init__(self, message: str, *, output: str = "") -> None:
        super().__init__(message)
        self.output = output


def _run_aws(arguments: list[str]) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            [
                "aws",
                "--cli-connect-timeout",
                "10",
                "--cli-read-timeout",
                "30",
                *arguments,
            ],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=AWS_CLI_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        raise AwsCliError(
            f"AWS CLI command timed out after {AWS_CLI_TIMEOUT_SECONDS} seconds: "
            f"{' '.join(arguments[:2])}",
            output=str(exc),
        ) from exc
    if result.returncode != 0:
        output = "\n".join(part for part in (result.stdout, result.stderr) if part).strip()
        raise AwsCliError(f"AWS CLI command failed: {' '.join(arguments[:2])}", output=output)
    return result


def _is_error(error: AwsCliError, markers: tuple[str, ...]) -> bool:
    output = error.output.casefold()
    return any(marker in output for marker in markers)


def read_s3_state(*, bucket: str, key: str, region: str) -> Tuple[Optional[Dict[str, Any]], str]:
    with tempfile.TemporaryDirectory(prefix="dashboard-maintenance-read-") as directory:
        body_path = Path(directory) / "maintenance.json"
        try:
            response = _run_aws(
                [
                    "s3api",
                    "get-object",
                    "--bucket",
                    bucket,
                    "--key",
                    key,
                    "--region",
                    region,
                    str(body_path),
                ]
            )
        except AwsCliError as exc:
            if _is_error(exc, _NOT_FOUND_MARKERS):
                return None, ""
            raise
        metadata = json.loads(response.stdout or "{}")
        raw = json.loads(body_path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("Maintenance object must contain a JSON object.")
        return raw, str(metadata.get("ETag") or "").strip()


def conditional_put_s3_state(
    state: Dict[str, Any],
    *,
    bucket: str,
    key: str,
    region: str,
    expected_etag: str,
) -> None:
    with tempfile.TemporaryDirectory(prefix="dashboard-maintenance-write-") as directory:
        body_path = Path(directory) / "maintenance.json"
        body_path.write_text(
            json.dumps(state, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        arguments = [
            "s3api",
            "put-object",
            "--bucket",
            bucket,
            "--key",
            key,
            "--body",
            str(body_path),
            "--content-type",
            "application/json; charset=utf-8",
            "--server-side-encryption",
            "AES256",
            "--region",
            region,
        ]
        if expected_etag:
            arguments.extend(["--if-match", expected_etag])
        else:
            arguments.extend(["--if-none-match", "*"])
        _run_aws(arguments)


def mutate_s3_state(
    builder: Callable[[Optional[Dict[str, Any]]], Dict[str, Any]],
    verifier: Callable[[Dict[str, Any]], bool],
    *,
    bucket: str,
    key: str,
    region: str,
) -> Dict[str, Any]:
    last_conflict: Optional[Exception] = None
    for _attempt in range(1, MAX_CAS_ATTEMPTS + 1):
        current, etag = read_s3_state(bucket=bucket, key=key, region=region)
        desired = builder(current)
        try:
            conditional_put_s3_state(
                desired,
                bucket=bucket,
                key=key,
                region=region,
                expected_etag=etag,
            )
        except AwsCliError as exc:
            if _is_error(exc, _CONFLICT_MARKERS + _NOT_FOUND_MARKERS):
                last_conflict = exc
                continue
            raise
        saved, _saved_etag = read_s3_state(bucket=bucket, key=key, region=region)
        if saved is None or not verifier(saved):
            last_conflict = MaintenanceConflict("Maintenance state changed before readback verification.")
            continue
        return saved
    raise MaintenanceConflict(
        f"Maintenance state did not converge after {MAX_CAS_ATTEMPTS} CAS attempts."
    ) from last_conflict


def _common_parser(subparser: argparse.ArgumentParser) -> None:
    subparser.add_argument("--project", required=True)
    subparser.add_argument("--bucket", required=True)
    subparser.add_argument("--key", required=True)
    subparser.add_argument("--region", required=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    status = commands.add_parser("status")
    _common_parser(status)

    start = commands.add_parser("start")
    _common_parser(start)
    start.add_argument("--operation-id", required=True)
    start.add_argument("--reason-code", default="deployment")
    start.add_argument("--message", default=DEFAULT_MAINTENANCE_MESSAGE)
    start.add_argument("--phase", default="starting")
    start.add_argument("--actor", default="github-actions")
    start.add_argument("--ttl-seconds", type=int, default=DEFAULT_LEASE_SECONDS)
    start.add_argument("--max-lifetime-seconds", type=int, default=DEFAULT_MAX_LIFETIME_SECONDS)
    start.add_argument("--source-sha", default="")
    start.add_argument("--image-digest", default="")

    renew = commands.add_parser("renew")
    _common_parser(renew)
    renew.add_argument("--operation-id", required=True)
    renew.add_argument("--phase", default="deploying")
    renew.add_argument("--actor", default="github-actions")
    renew.add_argument("--ttl-seconds", type=int, default=DEFAULT_LEASE_SECONDS)

    stop = commands.add_parser("stop")
    _common_parser(stop)
    stop.add_argument("--operation-id", required=True)
    stop.add_argument("--outcome", default="complete")
    stop.add_argument("--actor", default="github-actions")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    location = {"bucket": args.bucket, "key": args.key, "region": args.region}
    if args.command == "status":
        raw, _etag = read_s3_state(**location)
        print(json.dumps(public_maintenance_status(raw or {}, project=args.project), ensure_ascii=False))
        return

    if args.command == "start":
        saved = mutate_s3_state(
            lambda current: build_active_maintenance_state(
                current or {},
                project=args.project,
                operation_id=args.operation_id,
                reason_code=args.reason_code,
                message=args.message,
                phase=args.phase,
                actor=args.actor,
                ttl_seconds=args.ttl_seconds,
                max_lifetime_seconds=args.max_lifetime_seconds,
                source_sha=args.source_sha,
                image_digest=args.image_digest,
            ),
            lambda raw: (
                (status := normalize_maintenance_state(raw, project=args.project))["active"]
                and status["operation_id"] == args.operation_id
            ),
            **location,
        )
    elif args.command == "renew":
        def renew_builder(current: Optional[Dict[str, Any]]) -> Dict[str, Any]:
            status = normalize_maintenance_state(current or {}, project=args.project)
            if not status["active"] or status["operation_id"] != args.operation_id:
                raise MaintenanceConflict("Maintenance lease is inactive, expired, or owned by another operation.")
            return build_active_maintenance_state(
                current or {},
                project=args.project,
                operation_id=args.operation_id,
                reason_code=status["reason_code"],
                message=status["message"],
                phase=args.phase,
                actor=args.actor,
                ttl_seconds=args.ttl_seconds,
                source_sha=status.get("source_sha") or "",
                image_digest=status.get("image_digest") or "",
            )

        saved = mutate_s3_state(
            renew_builder,
            lambda raw: (
                (status := normalize_maintenance_state(raw, project=args.project))["active"]
                and status["operation_id"] == args.operation_id
            ),
            **location,
        )
    else:
        saved = mutate_s3_state(
            lambda current: build_inactive_maintenance_state(
                current or {},
                project=args.project,
                operation_id=args.operation_id,
                outcome=args.outcome,
                actor=args.actor,
            ),
            lambda raw: (
                not (status := normalize_maintenance_state(raw, project=args.project))["active"]
                and status["operation_id"] == args.operation_id
            ),
            **location,
        )

    print(json.dumps(public_maintenance_status(saved, project=args.project), ensure_ascii=False))


if __name__ == "__main__":
    main()
