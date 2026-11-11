"""Read-only, independent bidirectional order-identity audit; never authorizes publication.

Only IDs and order numbers are collected, in an ignored private output file. No
business module is imported and no provider mutation is available in this tool.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from urllib.parse import urljoin, urlparse

ACCOUNT = "919341186960"
REGION = "eu-central-1"
HOSTS = frozenset({"vevo.sk", "www.vevo.sk", "vevo.flox.sk"})
QUERY = """query InventoryIdentityAudit($params: OrderParams) {
  getOrderList(params: $params) {
    data { id order_num }
    pageInfo { hasNextPage nextCursor totalRecords }
  }
}"""


class DiagnosticError(RuntimeError):
    """Fixed safe reason code; never include response or credential text."""


def require(condition, code):
    if not condition:
        raise DiagnosticError(code)


def identity(row):
    require(isinstance(row, dict), "invalid_row")
    raw, number = row.get("id"), row.get("order_num")
    require(type(raw) in (str, int) and str(raw).isascii()
            and str(raw).isdigit() and int(raw) > 0, "invalid_id")
    require(isinstance(number, str) and bool(number.strip()) and len(number) <= 256,
            "invalid_order_number")
    return int(raw), number


def digest(rows):
    return hashlib.sha256(json.dumps(rows, separators=(",", ":"),
                                    ensure_ascii=False).encode()).hexdigest()


def audit_direction(read, direction, pages, *, limit=30):
    """Independent overlapping enumeration; metadata is evidence, never authority.

    This is a diagnostic algorithm, not the production inventory scanner. A
    successful return means only that visible ordered rows were enumerated.
    """
    require(direction in {"ASC", "DESC"} and 2 <= limit <= 30, "invalid_scan_arguments")
    rows, seen_ids, seen_numbers = [], set(), set()
    offset = 0
    while True:
        value = read({"cursor": offset, "limit": limit, "order_by": "order_id", "sort": direction})
        require(isinstance(value, dict) and isinstance(value.get("data"), list), "invalid_page")
        page, info = value["data"], value.get("pageInfo")
        require(isinstance(info, dict) and type(info.get("totalRecords")) is int
                and info["totalRecords"] >= 0 and type(info.get("hasNextPage")) is bool,
                "invalid_metadata")
        cursor = info.get("nextCursor")
        require(cursor is None or type(cursor) is int and cursor >= 0, "invalid_next_cursor")
        require(len(page) <= limit, "oversized_page")
        pairs = [identity(row) for row in page]
        ids = [pair[0] for pair in pairs]
        require(all(a < b if direction == "ASC" else a > b
                    for a, b in zip(ids, ids[1:])), "unordered_page")
        end = offset + len(page)
        full_terminal_hint = info["hasNextPage"] and len(page) == limit and end == info["totalRecords"]
        consistent = (info["hasNextPage"] == (end < info["totalRecords"]) or full_terminal_hint)
        consistent = consistent and end <= info["totalRecords"]
        consistent = consistent and not (info["hasNextPage"] and len(page) != limit)
        consistent = consistent and (not info["hasNextPage"] or cursor == end)
        pages.append({"direction": direction, "offset": offset, "limit": limit,
                      "returned": len(page), "totalRecords": info["totalRecords"],
                      "hasNextPage": info["hasNextPage"], "nextCursor": cursor,
                      "metadata_consistent": consistent})
        if offset:
            require(bool(pairs) and pairs[0] == rows[-1], "overlap_changed")
            pairs = pairs[1:]
        for pair in pairs:
            require(pair[0] not in seen_ids and pair[1] not in seen_numbers, "duplicate_identity")
            seen_ids.add(pair[0])
            seen_numbers.add(pair[1])
            rows.append(pair)
        if len(page) < limit:
            break
        offset += len(page) - 1
    return rows


class ProviderReader:
    def __init__(self, url, token, *, timeout_seconds, max_pages, session,
                 clock=time.monotonic, sleep=time.sleep, on_progress=None):
        self.url, self.token, self.session = url, token, session
        self.clock, self.sleep = clock, sleep
        self.deadline = clock() + timeout_seconds
        self.max_pages, self.pages, self.requests = max_pages, 0, 0
        self.last_request = None
        self.on_progress = on_progress

    def _pace(self):
        delay = max(0.0, .5 - (self.clock() - self.last_request)) if self.last_request is not None else 0
        if self.requests and self.requests % 100 == 0:
            delay = max(delay, 5.0)
        require(self.clock() + delay < self.deadline, "time_budget_exhausted")
        if delay:
            self.sleep(delay)
        require(self.clock() < self.deadline, "time_budget_exhausted")

    def __call__(self, params):
        require(self.pages < self.max_pages, "page_budget_exhausted")
        self.pages += 1
        for _ in range(3):
            parsed = urlparse(self.url)
            require(parsed.scheme == "https" and parsed.hostname in HOSTS
                    and parsed.username is None and parsed.password is None
                    and parsed.port in (None, 443), "untrusted_api_url")
            self._pace()
            self.last_request = self.clock()
            self.requests += 1
            remaining = self.deadline - self.clock()
            require(remaining > 0, "time_budget_exhausted")
            response = self.session.post(
                self.url, json={"query": QUERY, "variables": {"params": params}},
                headers={"BW-API-Key": "Token " + self.token},
                timeout=min(45, remaining), allow_redirects=False,
            )
            require(self.clock() <= self.deadline, "time_budget_exhausted")
            if response.status_code in (307, 308):
                destination = response.headers.get("Location")
                require(isinstance(destination, str) and bool(destination), "invalid_redirect")
                self.url = urljoin(self.url, destination)
                response.close()
                continue
            require(response.status_code == 200, "provider_http_failure")
            require(len(response.content) <= 262144, "response_size_exceeded")
            value = response.json()
            response.close()
            require(isinstance(value, dict) and not value.get("errors"), "provider_graphql_failure")
            require(isinstance(value.get("data"), dict), "invalid_response")
            if self.on_progress and self.pages % 50 == 0:
                self.on_progress(self.pages)
            return value["data"].get("getOrderList")
        raise DiagnosticError("redirect_limit_exceeded")


def summary(ascending, descending, pages):
    totals = [page["totalRecords"] for page in pages]
    anomalies = [page for page in pages if not page["metadata_consistent"]]
    matching = ascending == list(reversed(descending))
    return {"diagnostic_only": True, "publication_authorized": False,
            "ascending_count": len(ascending), "descending_count": len(descending),
            "identity_sequences_match": matching,
            "ascending_identity_sha256": digest(ascending),
            "descending_reversed_identity_sha256": digest(list(reversed(descending))),
            "reported_total_min": min(totals) if totals else None,
            "reported_total_max": max(totals) if totals else None,
            "page_count": len(pages), "metadata_anomalies": anomalies}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True, choices=("vevo",))
    parser.add_argument("--profile", default="codex")
    parser.add_argument("--output", required=True)
    parser.add_argument("--timeout-seconds", type=int, default=1200)
    parser.add_argument("--max-pages", type=int, default=1000)
    args = parser.parse_args(argv)
    require(1 <= args.timeout_seconds <= 1200 and 1 <= args.max_pages <= 1000, "invalid_limits")
    root = Path(__file__).resolve().parents[1]
    output = (root / args.output).resolve()
    require(output.is_relative_to((root / "data").resolve()) and not output.exists(), "invalid_private_output")
    require(subprocess.run(["git", "check-ignore", "-q", "--", str(output)], cwd=root).returncode == 0,
            "output_not_ignored")
    source = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    script = Path(__file__).read_bytes()
    require(script == subprocess.check_output(
        ["git", "show", source + ":scripts/diagnose_reporting_inventory_pages.py"], cwd=root),
        "diagnostic_source_not_committed")
    evidence = {"schema_version": 1, "diagnostic_only": True, "publication_authorized": False,
                "source_commit": source, "script_sha256": hashlib.sha256(script).hexdigest(),
                "process": {"pid": os.getpid(), "parent_pid": os.getppid(), "executable": sys.executable,
                            "command": list(getattr(sys, "orig_argv", [sys.executable, *sys.argv])), "listener_port": None},
                "pages": [], "ascending": [], "descending": []}
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as handle:
        json.dump(evidence, handle, indent=2)
    print(json.dumps({"event": "audit_started", "source": source, "process": evidence["process"]}), flush=True)
    try:
        import boto3
        import requests
        session = boto3.Session(profile_name=args.profile, region_name=REGION)
        require(session.client("sts").get_caller_identity()["Account"] == ACCOUNT, "unexpected_aws_account")
        ecs = session.client("ecs")
        for state in ("RUNNING", "PENDING"):
            require(not ecs.list_tasks(cluster="vevo-reporting-cluster", family="vevo-reporting-daily",
                                       desiredStatus=state)["taskArns"], "concurrent_report_task")
        secret = json.loads(session.client("secretsmanager").get_secret_value(
            SecretId="vevo/reporting/runtime-env")["SecretString"])
        with requests.Session() as http:
            reader = ProviderReader(secret["BIZNISWEB_API_URL"], secret["BIZNISWEB_API_TOKEN"],
                                    timeout_seconds=args.timeout_seconds, max_pages=args.max_pages,
                                    session=http, on_progress=lambda count: print(json.dumps(
                                        {"event": "audit_progress", "pages": count}), flush=True))
            for direction in ("ASC", "DESC"):
                evidence[direction.lower() + "ending"] = audit_direction(reader, direction, evidence["pages"])
                output.write_text(json.dumps(evidence, indent=2), encoding="utf-8")
        result = summary(evidence["ascending"], evidence["descending"], evidence["pages"])
        evidence["summary"] = result
        require(result["identity_sequences_match"], "bidirectional_identity_mismatch")
        evidence["status"] = "observation_complete"
        print(json.dumps(result), flush=True)
    except Exception as exc:
        reason = str(exc) if isinstance(exc, DiagnosticError) else "diagnostic_failed_" + type(exc).__name__
        evidence["status"] = "observation_failed"
        evidence["failure_code"] = reason
        print(json.dumps({"event": "audit_failed", "reason": reason}), flush=True)
        return 1
    finally:
        output.write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except DiagnosticError as exc:
        print(json.dumps({"event": "audit_failed", "reason": str(exc)}), flush=True)
        raise SystemExit(1) from None
