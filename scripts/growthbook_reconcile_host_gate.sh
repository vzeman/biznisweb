#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

PROJECT="${REPORT_PROJECT:?REPORT_PROJECT missing}"
ENVIRONMENT="${GROWTHBOOK_ENVIRONMENT:?GROWTHBOOK_ENVIRONMENT missing}"
VERSION="${GROWTHBOOK_RECONCILE_VERSION:?GROWTHBOOK_RECONCILE_VERSION missing}"

if [[ "${PROJECT}" != "vevo" ]]; then
  echo "GrowthBook reconciliation host gate is VEVO-only, got ${PROJECT}" >&2
  exit 1
fi
if [[ "${ENVIRONMENT}" != "preview" && "${ENVIRONMENT}" != "production" ]]; then
  echo "GrowthBook reconciliation host gate requires preview or production, got ${ENVIRONMENT}" >&2
  exit 1
fi
if [[ "${REPO_ROOT}" != "/app" ]]; then
  echo "GrowthBook reconciliation host gate requires /app, got ${REPO_ROOT}" >&2
  exit 1
fi
if [[ ! -f "scripts/reconcile_growthbook_facts.py" ]]; then
  echo "Versioned GrowthBook reconciler is missing from the image" >&2
  exit 1
fi

python - "${PROJECT}" "${ENVIRONMENT}" "${VERSION}" <<'PY'
import json
import sys
import threading
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.request import urlopen

project, environment, version = sys.argv[1:]


class QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self, _format, *_args):
        return


with TemporaryDirectory(prefix="vevo-growthbook-host-gate-", dir="/tmp") as directory:
    root = Path(directory)
    health = {
        "ok": True,
        "environment": environment,
        "project": project,
        "service": f"vevo-growthbook-{environment}-reconciliation",
    }
    marker = {
        "marker": "GROWTHBOOK_RECONCILE_HOST_OK",
        "environment": environment,
        "project": project,
        "runtime_path": "/app",
        "version": version,
    }
    (root / "health").write_text(json.dumps(health, sort_keys=True), encoding="utf-8")
    (root / "marker.json").write_text(json.dumps(marker, sort_keys=True), encoding="utf-8")

    def handler(*args, **kwargs):
        return QuietHandler(*args, directory=str(root), **kwargs)

    server = ThreadingHTTPServer(("127.0.0.1", 8000), handler)
    thread = threading.Thread(target=server.serve_forever, name="growthbook-host-gate")
    thread.start()
    try:
        with urlopen("http://127.0.0.1:8000/health", timeout=5) as response:
            actual_health = json.loads(response.read().decode("utf-8"))
        with urlopen("http://127.0.0.1:8000/marker.json", timeout=5) as response:
            actual_marker = json.loads(response.read().decode("utf-8"))
        if actual_health != health:
            raise SystemExit("localhost health payload mismatch")
        if actual_marker != marker:
            raise SystemExit("localhost marker payload mismatch")
        print(f"GROWTHBOOK_RECONCILE_LOCALHOST_HEALTH_OK:{environment}:{version}")
        print(f"GROWTHBOOK_RECONCILE_LOCALHOST_MARKER_OK:/app:{version}")
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
        if thread.is_alive():
            raise SystemExit("localhost host-gate thread did not stop")
PY
