"""Complete, bounded reads of the already-identified reconciliation log.

The CLI is restricted to the managed exact-main GitHub health workflow. Raw
messages stay on that runner, never in the canonical evidence or diagnostics.
Importing this module creates no client and performs no I/O.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import time
from datetime import UTC, datetime, time as local_time, timedelta
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ".github/workflows/monitor-vevo-growthbook-production-aa-infra.yml"
MAX_PAGES = 100
MAX_EVENTS = 50_000
MAX_PAGE_BYTES = 8 * 1024 * 1024  # Includes JSON escaping of a <=1 MiB AWS page.
MAX_TOTAL_BYTES = 32 * 1024 * 1024
MAX_SECONDS = 300
SAFE_CODES = frozenset({
    "stream-identity-invalid", "read-window-invalid", "read-time-limit",
    "page-read-failed", "page-byte-limit", "total-byte-limit", "page-json-invalid",
    "page-shape-invalid", "page-token-invalid", "event-limit", "event-shape-invalid",
    "terminal-page-ambiguous", "page-token-cycle", "page-limit", "temp-scope-invalid",
    "temp-contents-invalid", "temp-contents-changed", "temp-cleanup-failed",
    "managed-boundary-invalid", "temp-output-exists",
})


class HealthLogError(ValueError):
    """Only fixed, local codes may be emitted by the CLI."""


def require(condition: bool, code: str) -> None:
    if not condition:
        raise HealthLogError(code)


def _object(pairs: list) -> dict:
    result = {}
    for key, value in pairs:
        require(key not in result, "page-json-invalid")
        result[key] = value
    return result


def _constant(value: str) -> None:
    raise HealthLogError("page-json-invalid")


def read_complete_log(fetch_page, *, group: str, stream: str, task_id: str,
                      start_ms: int, end_ms: int) -> dict:
    """Return all events, in API order, only after explicit completion.

    Empty/partial pages are not completion. No event is deduplicated or repaired.
    An ambiguous nonempty terminal page is rejected instead of silently dropping
    it. The caller's fixed interval covers the exact task from its due boundary
    through the once-captured read time, not merely the task-selection window.
    """
    require(isinstance(group, str) and re.fullmatch(r"[.\-_/#A-Za-z0-9]{1,512}", group)
            is not None, "stream-identity-invalid")
    require(isinstance(task_id, str) and re.fullmatch(r"[a-f0-9]{32}", task_id)
            is not None, "stream-identity-invalid")
    require(isinstance(stream, str) and 1 <= len(stream) <= 512
            and not any(ord(c) < 32 or ord(c) == 127 or c in ":*" for c in stream)
            and stream.endswith("/" + task_id), "stream-identity-invalid")
    require(type(start_ms) is int and type(end_ms) is int
            and 0 <= start_ms < end_ms and end_ms - start_ms <= 25 * 60 * 60 * 1000,
            "read-window-invalid")
    base = dict(logGroupName=group, logStreamName=stream, startTime=start_ms,
                endTime=end_ms, startFromHead=True, limit=10000, unmask=False)
    events, seen = [], set()
    token = None
    total_bytes = 0
    began = time.monotonic()
    for _ in range(MAX_PAGES):
        require(time.monotonic() - began < MAX_SECONDS, "read-time-limit")
        request = dict(base)
        if token is not None:
            request["nextToken"] = token
        try:
            raw = fetch_page(request)
        except Exception:
            raise HealthLogError("page-read-failed") from None
        require(time.monotonic() - began < MAX_SECONDS, "read-time-limit")
        require(type(raw) is bytes and 0 < len(raw) <= MAX_PAGE_BYTES, "page-byte-limit")
        total_bytes += len(raw)
        require(total_bytes <= MAX_TOTAL_BYTES, "total-byte-limit")
        try:
            page = json.loads(raw, object_pairs_hook=_object, parse_constant=_constant)
        except (ValueError, UnicodeError, RecursionError):
            raise HealthLogError("page-json-invalid") from None
        require(type(page) is dict and type(page.get("events")) is list,
                "page-shape-invalid")
        forward = page.get("nextForwardToken")
        require(type(forward) is str and 0 < len(forward) <= 4096
                and not any(ord(c) < 32 or ord(c) == 127 for c in forward),
                "page-token-invalid")
        rows = page["events"]
        require(len(rows) <= 10000 and len(events) + len(rows) <= MAX_EVENTS,
                "event-limit")
        for row in rows:
            require(type(row) is dict and set(row) == {"message", "timestamp", "ingestionTime"}
                    and type(row["message"]) is str
                    and type(row["timestamp"]) is int
                    and start_ms <= row["timestamp"] < end_ms
                    and type(row["ingestionTime"]) is int and row["ingestionTime"] >= 0,
                    "event-shape-invalid")
        if forward == token:
            require(not rows, "terminal-page-ambiguous")
            return {"events": events}
        require(forward not in seen, "page-token-cycle")
        seen.add(forward)
        events.extend(rows)
        token = forward
    raise HealthLogError("page-limit")


def _plain_path(path: Path) -> bool:
    return (not path.is_symlink() and not getattr(path, "is_junction", lambda: False)()
            and path.resolve() == path)


def temp_scope(root: Path, runner_temp: Path, run_id: str) -> Path:
    require(type(run_id) is str and re.fullmatch(r"[1-9][0-9]{0,19}", run_id)
            is not None, "temp-scope-invalid")
    require(runner_temp.is_absolute() and runner_temp.is_dir()
            and _plain_path(runner_temp) and runner_temp != Path(runner_temp.anchor),
            "temp-scope-invalid")
    require(root.is_absolute() and root == runner_temp / f"vevo-aa-infra-{run_id}"
            and _plain_path(root), "temp-scope-invalid")
    require(not root.exists() or root.is_dir(), "temp-scope-invalid")
    return root


def cleanup_temp(root: Path, runner_temp: Path, run_id: str) -> None:
    """Delete only prevalidated regular files in one exact run-owned directory."""
    root = temp_scope(root, runner_temp, run_id)
    if not root.exists():
        return
    children = list(root.iterdir())
    require(len(children) <= 64, "temp-contents-invalid")
    # Preflight ALL entries before the first deletion. No recursive delete.
    identities = []
    for child in children:
        info = child.lstat()
        require(child.parent == root and _plain_path(child) and stat.S_ISREG(info.st_mode)
                and info.st_nlink == 1, "temp-contents-invalid")
        identities.append((child, info.st_dev, info.st_ino))
    for child, device, inode in identities:
        info = child.lstat()
        require(_plain_path(root) and _plain_path(child) and stat.S_ISREG(info.st_mode)
                and info.st_nlink == 1 and (info.st_dev, info.st_ino) == (device, inode),
                "temp-contents-changed")
        child.unlink()
    root.rmdir()
    require(not root.exists(), "temp-cleanup-failed")


def managed_scope(env: dict) -> Path:
    require(env.get("GITHUB_ACTIONS") == "true" and env.get("RUNNER_ENVIRONMENT") == "github-hosted"
            and env.get("GITHUB_REPOSITORY") == "vzeman/biznisweb"
            and env.get("GITHUB_REF") == "refs/heads/main"
            and env.get("GITHUB_WORKFLOW_REF") == f"vzeman/biznisweb/{WORKFLOW}@refs/heads/main"
            and env.get("RUN_INFRA_HEALTH") == "true", "managed-boundary-invalid")
    require(Path(env.get("GITHUB_WORKSPACE", "")) == ROOT and Path.cwd().resolve() == ROOT,
            "managed-boundary-invalid")
    return temp_scope(Path(env.get("TEMP_HEALTH_DIR", "")),
                      Path(env.get("RUNNER_TEMP", "")), env.get("GITHUB_RUN_ID", ""))


def read_window(due_text: str, now: datetime) -> tuple[int, int]:
    zone = ZoneInfo("Europe/Bratislava")
    local = now.astimezone(zone)
    date = local.date() - timedelta(days=local.time() < local_time(3, 45))
    due = datetime.combine(date, local_time(3, 45), zone)
    require(due_text == due.isoformat(timespec="seconds") and now >= due,
            "read-window-invalid")
    return int(due.timestamp() * 1000), int(now.timestamp() * 1000)


def cli_fetch(request: dict) -> bytes:
    # CLI output cannot leak SDK messages, raw responses or tokens to a log.
    result = subprocess.run(
        ["aws", "logs", "get-log-events", "--cli-input-json", json.dumps(request),
         "--region", "eu-central-1", "--output", "json", "--no-paginate",
         "--no-cli-pager", "--cli-connect-timeout", "10", "--cli-read-timeout", "20"],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, check=False, timeout=35,
    )
    require(result.returncode == 0, "page-read-failed")
    return result.stdout


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cleanup-only", action="store_true")
    args = parser.parse_args(argv)
    try:
        env = dict(os.environ)
        root = managed_scope(env)
        if args.cleanup_only:
            cleanup_temp(root, Path(env["RUNNER_TEMP"]), env["GITHUB_RUN_ID"])
            print("PRODUCTION_AA_INFRA_CLEANUP_OK:raw-retained=false")
            return 0
        require(root.is_dir() and env.get("HEALTH_PHASE") == "natural_reconciliation_verified"
                and env.get("AWS_REGION") == "eu-central-1", "managed-boundary-invalid")
        head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT,
                              stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                              check=True, timeout=10).stdout.decode().strip()
        require(re.fullmatch(r"[a-f0-9]{40}", head) is not None
                and head == env.get("GITHUB_SHA"), "managed-boundary-invalid")
        start_ms, end_ms = read_window(env.get("CHECKED_DUE_LOCAL", ""), datetime.now(UTC))
        output = root / "task-logs.json"
        require(not output.exists() and not output.is_symlink(), "temp-output-exists")
        payload = read_complete_log(
            cli_fetch, group=env.get("RECONCILIATION_LOG_GROUP", ""),
            stream=env.get("RECONCILIATION_LOG_STREAM", ""),
            task_id=env.get("RECONCILIATION_TASK_ID", ""), start_ms=start_ms, end_ms=end_ms,
        )
        with output.open("x", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=True, separators=(",", ":"))
        print("PRODUCTION_AA_INFRA_LOG_READ_OK:complete=true:raw-emitted=false")
        return 0
    except Exception as error:
        # Never format an exception: CLI/JSON/OS errors may contain raw data.
        code = "unclassified-error"
        if (type(error) is HealthLogError and len(error.args) == 1
                and type(error.args[0]) is str and error.args[0] in SAFE_CODES):
            code = error.args[0]
        print(f"PRODUCTION_AA_INFRA_LOG_IO_STOPPED:code={code}:raw-emitted=false")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
