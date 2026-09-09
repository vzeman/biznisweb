"""Resolve explicit project storage without inventing a bucket destination."""

import os
import re
from typing import Any, Mapping


def resolve_report_s3_location(
    project: str, settings: Mapping[str, Any] | None = None,
    environ: Mapping[str, Any] | None = None, *, required: bool = False,
) -> tuple[str, str]:
    """Prefer nonempty runtime values, then the project's recorded destination.

    A settings destination must be established from the project's real runtime;
    deployment verifies it against the scheduled reporting task before use.
    This helper has no account-wide or other-project bucket fallback.
    """
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", project):
        raise ValueError("Invalid reporting storage project")
    env = os.environ if environ is None else environ
    artifacts = (settings or {}).get("live_dashboard_artifacts") or {}
    if not isinstance(artifacts, Mapping):
        raise ValueError("Invalid reporting storage settings")
    project_key = project.upper().replace("-", "_")

    def value(name: str, setting: str) -> str:
        for candidate in (env.get(f"{project_key}_{name}"), env.get(f"{name}_{project_key}"),
                          env.get(name), artifacts.get(setting)):
            if candidate is not None and str(candidate).strip():
                return str(candidate).strip()
        return ""

    bucket = value("REPORT_S3_BUCKET", "s3_bucket")
    prefix = value("REPORT_S3_PREFIX", "s3_prefix").strip("/") or f"daily-reports/{project}"
    if bucket and not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
        raise ValueError("Invalid reporting storage bucket")
    if required and not bucket:
        raise ValueError("A configured reporting storage bucket is required")
    if any(part in {"", ".", ".."} for part in prefix.split("/")):
        raise ValueError("Invalid reporting storage prefix")
    return bucket, prefix
