#!/usr/bin/env python3
"""Finite stdlib-only probe injected into an unchanged pinned reporting image."""
import argparse
import ast
import hashlib
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
import subprocess
import threading
from types import SimpleNamespace
from typing import List, Optional
from urllib.request import urlopen

MARKER = "REPORTING_GUARD_IDENTITY_OK"
SKIP = "REPORT_SKIP_CREDITNOTE_STORNO_GUARD"


def verify_skip_reader(tree, environ, project):
    """Run only the hash-verified argument parser, never import the business module."""
    functions = [node for node in tree.body if isinstance(node, ast.FunctionDef)
                 and node.name in {"env_bool", "parse_args"}]
    if {node.name for node in functions} != {"env_bool", "parse_args"} or len(functions) != 2:
        raise RuntimeError("report-probe-parser-missing")
    namespace = {"argparse": argparse, "os": SimpleNamespace(getenv=environ.get),
                 "DEFAULT_PROJECT": project, "Optional": Optional, "List": List}
    exec(compile(ast.Module(body=functions, type_ignores=[]), "verified-report-parser", "exec"), namespace)
    if namespace["parse_args"]([]).skip_creditnote_storno_guard is not True:
        raise RuntimeError("report-probe-skip-parser-failed")
    branches = [node for node in ast.walk(tree) if isinstance(node, ast.If)
                and isinstance(node.test, ast.Attribute) and node.test.attr == "skip_creditnote_storno_guard"
                and isinstance(node.test.value, ast.Name) and node.test.value.id == "args"]
    def guard_calls(nodes):
        return [node for root in nodes for node in ast.walk(root) if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name) and node.func.id == "maybe_run_creditnote_storno_guard"]
    if len(branches) != 1 or guard_calls(branches[0].body) or len(guard_calls(branches[0].orelse)) != 1:
        raise RuntimeError("report-probe-skip-branch-invalid")


def probe_payload(task, project, revision, source, expected_hash, environ, cwd):
    family = f"{project}-reporting-daily"
    if (project not in {"roy", "vevo"} or task.get("Family") != family
            or str(task.get("Revision")) != str(revision) or cwd != "/app"):
        raise RuntimeError("report-probe-identity-mismatch")
    if hashlib.sha256(source).hexdigest() != expected_hash:
        raise RuntimeError("report-probe-source-mismatch")
    tree = ast.parse(source)
    constants = {node.value for node in ast.walk(tree) if isinstance(node, ast.Constant) and isinstance(node.value, str)}
    if SKIP not in constants or "--skip-creditnote-storno-guard" not in constants:
        raise RuntimeError("report-probe-skip-reader-missing")
    if environ.get(SKIP, "").strip().lower() != "true" or environ.get("REPORT_PROJECT") != project:
        raise RuntimeError("report-probe-skip-override-missing")
    verify_skip_reader(tree, environ, project)
    ips = sorted({ip for container in task.get("Containers", []) for network in container.get("Networks", [])
                  for ip in network.get("IPv4Addresses", [])})
    if len(ips) != 1 or not task.get("TaskARN"):
        raise RuntimeError("report-probe-network-identity-missing")
    return {"marker": MARKER, "project": project, "service": family, "revision": str(revision),
            "task": task["TaskARN"], "private_ip": ips[0], "instance_id": "N/A:FARGATE", "path": cwd,
            "source_sha256": expected_hash, "inline_guard_skipped": True, "business_runner_started": False}


def localhost_marker(payload):
    body = json.dumps(payload, sort_keys=True).encode()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/marker.json":
                self.send_error(404)
                return
            self.send_response(200)
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    worker = threading.Thread(target=server.serve_forever)
    worker.start()
    try:
        result = subprocess.run(["curl", "--fail", "--silent", "--show-error", "--max-time", "10",
                                 f"http://127.0.0.1:{server.server_port}/marker.json"],
                                capture_output=True, check=True, timeout=15)
        if json.loads(result.stdout) != payload:
            raise RuntimeError("report-probe-localhost-mismatch")
    finally:
        server.shutdown()
        worker.join(timeout=10)
        server.server_close()
    if worker.is_alive():
        raise RuntimeError("report-probe-server-not-stopped")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", choices=("roy", "vevo"), required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--source-sha256", required=True)
    args = parser.parse_args()
    with urlopen(os.environ["ECS_CONTAINER_METADATA_URI_V4"] + "/task", timeout=5) as response:
        task = json.load(response)
    payload = probe_payload(task, args.project, args.revision, Path("/app/daily_report_runner.py").read_bytes(),
                            args.source_sha256, os.environ, os.getcwd())
    localhost_marker(payload)
    print(MARKER + " " + json.dumps(payload, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
